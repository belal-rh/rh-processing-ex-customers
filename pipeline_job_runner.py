# pipeline_job_runner.py
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterable

import requests
from openai import OpenAI

from config import AppConfig, TrelloConfig, HubSpotConfig, OpenAIConfig
from jobs import JOB_STORE, ContactState
from job_io import contact_dir, write_json, write_text
from utils_csv import read_csv_rows, normalize_email
from hubspot_client import HubSpotClient
from openai_assistant_client import OpenAIAssistantClient
from rate_limit import TokenBucketLimiter, RateLimitConfig, compute_backoff


# ----------------------------
# Helpers
# ----------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ms_to_iso(ms: str | int | None) -> str:
    if ms is None or ms == "":
        return ""
    try:
        val = int(ms)
        dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return str(ms)

def _safe_json_loads(s: str) -> dict[str, Any] | None:
    try:
        o = json.loads(s)
        return o if isinstance(o, dict) else None
    except Exception:
        return None


# ----------------------------
# Trello fetch (rate-limited)
# ----------------------------

class TrelloFetcher:
    def __init__(self, cfg: TrelloConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=5.0, burst=5))

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.cfg.api_base}{path}"
        base_params = {"key": self.cfg.api_key, "token": self.cfg.api_token}
        if params:
            base_params.update(params)

        last_err: Exception | None = None
        for attempt in range(8):
            self.limiter.acquire(1.0)
            try:
                resp = self.session.get(url, params=base_params, timeout=self.cfg.timeout_seconds)

                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            time.sleep(float(ra))
                        except Exception:
                            time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))
                    else:
                        time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))
                    continue

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_err = e
                time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))

        raise RuntimeError(f"Trello GET failed: {path}. Last error: {last_err}")

    def fetch_card_bundle(self, trello_short_id: str) -> dict[str, Any]:
        """
        trello_short_id = shortLink / card id used in https://trello.com/c/<id>
        """
        card = self._get(
            f"/cards/{trello_short_id}",
            params={"fields": "name,desc,dateLastActivity,url,idShort"},
        )

        actions = self._get(
            f"/cards/{trello_short_id}/actions",
            params={"filter": "commentCard", "limit": 1000, "fields": "type,date,data"},
        )

        checklists = self._get(
            f"/cards/{trello_short_id}/checklists",
            params={"fields": "name", "checkItems": "all", "checkItem_fields": "name,state,pos"},
        )

        return {"card": card, "actions": actions, "checklists": checklists}

    def build_trello_text(self, bundle: dict[str, Any]) -> str:
        card = bundle.get("card", {}) or {}
        actions = bundle.get("actions", []) or []
        checklists = bundle.get("checklists", []) or []

        parts: list[str] = []
        parts.append("TRELLO_CARD:")
        parts.append(f"- Name: {card.get('name','')}")
        parts.append(f"- URL: {card.get('url','')}")
        parts.append(f"- LastActivity: {card.get('dateLastActivity','')}")
        desc = (card.get("desc") or "").strip()
        if desc:
            parts.append("")
            parts.append("TRELLO_DESC:")
            parts.append(desc)

        # Comments (timestamped)
        if actions:
            parts.append("")
            parts.append("TRELLO_COMMENTS (timestamped):")
            # actions already include 'date' ISO
            for a in actions:
                dt = a.get("date", "")
                data = a.get("data", {}) or {}
                txt = ((data.get("text") or "") if isinstance(data, dict) else "") or ""
                txt = txt.strip()
                if txt:
                    parts.append(f"- [{dt}] {txt}")

        # Checklists
        if checklists:
            parts.append("")
            parts.append("TRELLO_CHECKLISTS:")
            for cl in checklists:
                cl_name = (cl.get("name") or "").strip()
                parts.append(f"- Checklist: {cl_name}")
                items = cl.get("checkItems", []) or []
                for it in items:
                    name = (it.get("name") or "").strip()
                    state = (it.get("state") or "").strip()
                    if name:
                        parts.append(f"  - [{state}] {name}")

        return "\n".join(parts).strip()


# ----------------------------
# HubSpot write (notes + associations)
# ----------------------------

class HubSpotWriteClient:
    def __init__(self, cfg: HubSpotConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.cfg.private_app_token}",
            "Content-Type": "application/json",
        })
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=2.0, burst=2))

        if not getattr(cfg, "note_to_contact_type_id", 0) or not getattr(cfg, "note_to_deal_type_id", 0):
            raise RuntimeError(
                "Missing HubSpot association type IDs. Set HS_ASSOC_NOTE_TO_CONTACT_TYPE_ID and HS_ASSOC_NOTE_TO_DEAL_TYPE_ID in .env"
            )

    def _request(self, method: str, path: str, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.cfg.api_base}{path}"
        last_err: Exception | None = None

        for attempt in range(self.cfg.max_retries):
            self.limiter.acquire(1.0)
            try:
                resp = self.session.request(method, url, json=json_body, timeout=self.cfg.timeout_seconds)

                if resp.status_code == 429:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            time.sleep(float(ra))
                        except Exception:
                            time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                    else:
                        time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                    continue

                resp.raise_for_status()
                return resp.json() if resp.text else {}

            except Exception as e:
                last_err = e
                time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))

        raise RuntimeError(f"HubSpot write failed: {method} {path}. Last error: {last_err}")

    def create_note_html(self, html_body: str) -> str:
        data = self._request(
            "POST",
            "/crm/v3/objects/notes",
            json_body={"properties": {"hs_note_body": html_body}},
        )
        note_id = str(data.get("id", "") or "")
        if not note_id:
            raise RuntimeError("Failed to create note: missing id")
        return note_id

    def associate_note_to_contact(self, note_id: str, contact_id: str) -> None:
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/contacts/{contact_id}/{self.cfg.note_to_contact_type_id}",
            json_body=None,
        )

    def associate_note_to_deal(self, note_id: str, deal_id: str) -> None:
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/deals/{deal_id}/{self.cfg.note_to_deal_type_id}",
            json_body=None,
        )


# ----------------------------
# Step2 HubSpot fetch (per contact)
# ----------------------------

def _build_hubspot_text(notes: list[dict[str, Any]], calls: list[dict[str, Any]]) -> str:
    parts: list[str] = []

    if notes:
        parts.append("HUBSPOT_NOTES (timestamped):")
        for n in notes:
            ts = n.get("timestamp", "")
            body = n.get("body", "")
            if body:
                parts.append(f"- [{ts}] {body}".strip())
        parts.append("")

    if calls:
        parts.append("HUBSPOT_CALLS (timestamped + outcome):")
        for c in calls:
            ts = c.get("timestamp", "")
            outc = c.get("outcome", "")
            body = c.get("body", "")
            parts.append(f"- [{ts}] OUTCOME={outc} | {body}".strip())
        parts.append("")

    return "\n".join(parts).strip()


def fetch_hubspot_bundle(hs_client: HubSpotClient, contact_id: str) -> dict[str, Any]:
    note_ids = hs_client.list_associated_object_ids(contact_id, "notes")
    call_ids = hs_client.list_associated_object_ids(contact_id, "calls")
    deal_ids = hs_client.list_associated_object_ids(contact_id, "deals")

    note_props = ["hs_note_body", "hs_timestamp", "hs_createdate"]
    call_props = ["hs_call_body", "hs_call_outcome", "hs_timestamp", "hs_createdate"]

    notes_raw = hs_client.batch_read_objects("notes", note_ids, note_props) if note_ids else []
    calls_raw = hs_client.batch_read_objects("calls", call_ids, call_props) if call_ids else []

    notes_norm: list[dict[str, str]] = []
    for n in notes_raw:
        props = n.get("properties", {}) or {}
        ts = props.get("hs_timestamp") or props.get("hs_createdate")
        notes_norm.append({
            "timestamp": _ms_to_iso(ts),
            "body": (props.get("hs_note_body") or "").strip(),
            "id": str(n.get("id", "")),
        })
    notes_norm.sort(key=lambda x: x.get("timestamp", ""))

    calls_norm: list[dict[str, str]] = []
    for c in calls_raw:
        props = c.get("properties", {}) or {}
        ts = props.get("hs_timestamp") or props.get("hs_createdate")
        calls_norm.append({
            "timestamp": _ms_to_iso(ts),
            "outcome": (props.get("hs_call_outcome") or "").strip(),
            "body": (props.get("hs_call_body") or "").strip(),
            "id": str(c.get("id", "")),
        })
    calls_norm.sort(key=lambda x: x.get("timestamp", ""))

    hubspot_text = _build_hubspot_text(notes_norm, calls_norm)

    return {
        "hubspot_contact_id": contact_id,
        "deal_ids": deal_ids,
        "notes": notes_norm,
        "calls": calls_norm,
        "hubspot_text": hubspot_text,
    }


# ----------------------------
# AI (Step3) + HTML Render (Step4)
# ----------------------------

HTML_RENDER_SYSTEM_PROMPT = """
Du bist ein Formatter für HubSpot-Notizen.
Du erhältst ausschließlich JSON im vorgegebenen Schema und gibst ausschließlich HTML zurück.

Regeln:
- Nutze nur diese HTML-Tags: <b>, <i>, <u>, <br>, <ul>, <li>, <p>, <hr>.
- Keine Styles, keine Scripts, keine externen Ressourcen.
- Mache klare Abschnitte mit <b>Überschrift</b> und <hr>.
- Schreibe prägnant, scannbar, ohne Füllwörter.
- Wenn Felder leer sind: Abschnitt weglassen oder "Unbekannt" (max. 1 Zeile).
- Sprache: Deutsch. Anrede: Du/Dich/Dir/Deine großgeschrieben.
- Gib wirklich NUR HTML aus (keine Markdown-Fences, keine Erklärungen).
""".strip()


def render_html_from_json(openai_key: str, model: str, payload: dict[str, Any], max_retries: int = 4) -> str:
    client = OpenAI(api_key=openai_key)
    user_input = "Wandle dieses JSON in eine HubSpot-Notiz im HTML-Format um:\n\n" + json.dumps(payload, ensure_ascii=False)

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": HTML_RENDER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
            )
            html = getattr(resp, "output_text", None)
            if not html or not str(html).strip():
                raise RuntimeError("Empty HTML output")
            return str(html).strip()
        except Exception as e:
            last_err = e
            time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))
    raise RuntimeError(f"HTML render failed. Last error: {last_err}")


# ----------------------------
# Main job runner
# ----------------------------

def run_pipeline_job(
    job_id: str,
    app_cfg: AppConfig,
    trello_cfg: TrelloConfig,
    hs_cfg: HubSpotConfig,
    oa_cfg: OpenAIConfig,
    csv1_path: str,
    csv2_path: str,
    delim1: str,
    delim2: str,
    mapping: dict[str, str],
    extra_user_prompt_step3: str,
    render_model: str,
) -> None:
    """
    Runs step1..step4 per contact and writes per-contact artifacts into:
      output/jobs/<job_id>/contacts/<contact_id>/...
    """
    job_dir = JOB_STORE.job_dir(job_id)
    JOB_STORE.set_status(job_id, "running")

    # Load CSVs
    csv1 = read_csv_rows(csv1_path, delimiter=delim1)
    csv2 = read_csv_rows(csv2_path, delimiter=delim2)

    csv1_email_col = mapping["csv1_email_col"]
    csv1_hubspot_id_col = mapping["csv1_hubspot_id_col"]
    csv2_email_col = mapping["csv2_email_col"]
    csv2_trello_id_col = mapping["csv2_trello_id_col"]

    email_to_trello: dict[str, list[str]] = {}
    for r in csv2:
        em = normalize_email(r.get(csv2_email_col, ""))
        tid = (r.get(csv2_trello_id_col, "") or "").strip()
        if em and tid:
            email_to_trello.setdefault(em, []).append(tid)

    # Clients
    trello = TrelloFetcher(trello_cfg)
    hs_read = HubSpotClient(hs_cfg)
    asst = OpenAIAssistantClient(oa_cfg)

    total_contacts = 0
    for r in csv1:
        em = normalize_email(r.get(csv1_email_col, ""))
        hs_id = (r.get(csv1_hubspot_id_col, "") or "").strip()
        if em and hs_id:
            total_contacts += 1

    JOB_STORE.set_progress(job_id, total=total_contacts, done=0, errors=0, duplicates=0)

    done = 0
    errors = 0
    duplicates = 0

    for r in csv1:
        email = normalize_email(r.get(csv1_email_col, ""))
        contact_id = (r.get(csv1_hubspot_id_col, "") or "").strip()
        if not email or not contact_id:
            continue

        # initial state
        JOB_STORE.upsert_contact(
            job_id,
            contact_id,
            ContactState(email=email, hubspot_contact_id=contact_id, status="running", step="step1", last_message="Matching Trello-ID…"),
        )

        cdir = contact_dir(job_dir, contact_id)
        write_json(os.path.join(cdir, "meta.json"), {"email": email, "hubspot_contact_id": contact_id, "started_at": _utc_now_iso()})

        trello_ids = email_to_trello.get(email, [])
        # de-dupe but preserve order
        seen = set()
        uniq = []
        for tid in trello_ids:
            if tid not in seen:
                seen.add(tid)
                uniq.append(tid)

        # Step1 decision
        if len(uniq) == 0:
            JOB_STORE.update_contact(job_id, contact_id, status="error", step="step1", last_message="Kein Trello-Match", error="no_trello_match")
            write_json(os.path.join(cdir, "step1_match.json"), {"status": "no_match", "trello_ids": []})
            errors += 1
            done += 1
            JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)
            continue

        if len(uniq) > 1:
            duplicates += 1
            JOB_STORE.update_contact(
                job_id,
                contact_id,
                step="step1",
                last_message=f"{len(uniq)} Trello-IDs gefunden – verarbeite alle",
            )
            write_json(os.path.join(cdir, "step1_match.json"), {
                "status": "multi",
                "trello_ids": uniq,
                "links": [f"https://trello.com/c/{tid}" for tid in uniq],
            })
            JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)
        else:
            write_json(os.path.join(cdir, "step1_match.json"), {
                "status": "single",
                "trello_ids": uniq,
                "links": [f"https://trello.com/c/{tid}" for tid in uniq],
            })

        # Keep trello_id for UI convenience (first id), but we process ALL ids for content
        trello_id = uniq[0]
        JOB_STORE.update_contact(job_id, contact_id, trello_id=trello_id, step="step1", last_message=f"Trello-IDs matched: {', '.join(uniq)}")

        # Step1 fetch Trello bundle
                # Step1 fetch Trello bundles (ALL matched ids) and merge into one trello_text
        try:
            bundles: list[dict[str, Any]] = []
            trello_blocks: list[str] = []
            per_card_errors: list[dict[str, Any]] = []

            for tid in uniq:
                try:
                    b = trello.fetch_card_bundle(tid)
                    bundles.append(b)
                    txt = trello.build_trello_text(b)

                    # Make it super clear for the LLM where each card starts/ends
                    trello_blocks.append(
                        f"===== TRELLO CARD START: {tid} =====\n{txt}\n===== TRELLO CARD END: {tid} ====="
                    )
                except Exception as e_card:
                    per_card_errors.append({"trello_id": tid, "error": str(e_card)})

            if not trello_blocks:
                raise RuntimeError(f"Trello fetch failed for all cards. errors={per_card_errors}")

            trello_text = "\n\n".join(trello_blocks)

            write_json(os.path.join(cdir, "step1_trello_cards.json"), bundles)
            write_json(os.path.join(cdir, "step1_trello_errors.json"), {"errors": per_card_errors})
            write_text(os.path.join(cdir, "step1_trello_text.txt"), trello_text)

            # keep legacy filename too (optional, but helps old UI paths)
            write_json(os.path.join(cdir, "step1_trello.json"), {"cards": bundles, "errors": per_card_errors, "trello_ids": uniq})

            JOB_STORE.update_contact(job_id, contact_id, step="step2", last_message="HubSpot Notes/Calls/Deals laden…")
        except Exception as e:
            JOB_STORE.update_contact(job_id, contact_id, status="error", step="step1", last_message="Trello fetch error", error=str(e))
            write_json(os.path.join(cdir, "step1_error.json"), {"error": str(e)})
            errors += 1
            done += 1
            JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)
            continue

        # Step2 fetch HubSpot bundle
        try:
            hs_bundle = fetch_hubspot_bundle(hs_read, contact_id)
            write_json(os.path.join(cdir, "step2_hubspot.json"), hs_bundle)
            write_text(os.path.join(cdir, "step2_hubspot_text.txt"), hs_bundle.get("hubspot_text", ""))

            merged_context = (trello_text + "\n\n" + (hs_bundle.get("hubspot_text") or "")).strip()
            write_text(os.path.join(cdir, "step2_merged_context.txt"), merged_context)

            JOB_STORE.update_contact(job_id, contact_id, step="step3", last_message="Assistant JSON Analyse…")
        except Exception as e:
            JOB_STORE.update_contact(job_id, contact_id, status="error", step="step2", last_message="HubSpot fetch error", error=str(e))
            write_json(os.path.join(cdir, "step2_error.json"), {"error": str(e)})
            errors += 1
            done += 1
            JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)
            continue

        # Step3: Assistant JSON
        try:
            raw = asst.summarize_with_assistant(
                merged_context_text=merged_context,
                extra_user_prompt=extra_user_prompt_step3,
            )
            parsed = _safe_json_loads(raw)
            write_text(os.path.join(cdir, "step3_raw.txt"), raw)
            if not parsed:
                raise RuntimeError("Assistant output is not valid JSON")
            write_json(os.path.join(cdir, "step3_ai.json"), parsed)

            JOB_STORE.update_contact(job_id, contact_id, step="step4", last_message="HTML Render (für HubSpot)…")
        except Exception as e:
            JOB_STORE.update_contact(job_id, contact_id, status="error", step="step3", last_message="Assistant error", error=str(e))
            write_json(os.path.join(cdir, "step3_error.json"), {"error": str(e)})
            errors += 1
            done += 1
            JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)
            continue

        # Step4: HTML render
        try:
            html = render_html_from_json(
                openai_key=oa_cfg.api_key,
                model=render_model,
                payload=parsed,
                max_retries=oa_cfg.max_retries,
            )
            write_text(os.path.join(cdir, "step4_note.html"), html)
            write_json(os.path.join(cdir, "step4_status.json"), {"rendered_at": _utc_now_iso(), "model": render_model})

            JOB_STORE.update_contact(job_id, contact_id, status="done", step="step4", last_message="Fertig (bereit für Review)")
        except Exception as e:
            JOB_STORE.update_contact(job_id, contact_id, status="error", step="step4", last_message="HTML render error", error=str(e))
            write_json(os.path.join(cdir, "step4_error.json"), {"error": str(e)})
            errors += 1

        done += 1
        JOB_STORE.set_progress(job_id, done=done, errors=errors, duplicates=duplicates)

    JOB_STORE.set_status(job_id, "done")


def set_verified(job_id: str, contact_id: str, verified: bool) -> None:
    job_dir = JOB_STORE.job_dir(job_id)
    cdir = contact_dir(job_dir, contact_id)
    write_json(os.path.join(cdir, "verified.json"), {"verified": bool(verified), "ts": _utc_now_iso()})
    JOB_STORE.update_contact(job_id, contact_id, verified=bool(verified), last_message=("Verified" if verified else "Unverified"))


def push_verified_to_hubspot(
    job_id: str,
    hs_cfg: HubSpotConfig,
    also_associate_deals: bool = True,
) -> dict[str, Any]:
    """
    For each verified contact:
      - create note (HTML) AND associate to contact/deals
    """
    job_dir = JOB_STORE.job_dir(job_id)
    
    # Config Werte sicherstellen
    n_c_id = getattr(hs_cfg, "note_to_contact_type_id", 0)
    n_d_id = getattr(hs_cfg, "note_to_deal_type_id", 0)
    
    writer = HubSpotWriteClient(hs_cfg, note_to_contact_type_id=n_c_id, note_to_deal_type_id=n_d_id)

    snapshot = JOB_STORE.get_snapshot(job_id)
    contacts = snapshot.get("contacts", {})

    results = {"created": 0, "errors": 0, "details": []}

    for contact_id, st in contacts.items():
        if not st.get("verified"):
            continue
        if st.get("status") != "done":
            continue

        cdir = contact_dir(job_dir, contact_id)
        html_path = os.path.join(cdir, "step4_note.html")
        hs_path = os.path.join(cdir, "step2_hubspot.json")

        try:
            html = ""
            if os.path.exists(html_path):
                with open(html_path, "r", encoding="utf-8") as f:
                    html = f.read().strip()
            if not html:
                raise RuntimeError("Missing step4_note.html")

            deal_ids: list[str] = []
            if also_associate_deals and os.path.exists(hs_path):
                with open(hs_path, "r", encoding="utf-8") as f:
                    hs_bundle = json.load(f)
                # Flexible Deals Erkennung
                if isinstance(hs_bundle.get("deal_ids"), list):
                    deal_ids = [str(x) for x in hs_bundle.get("deal_ids") if str(x).strip()]
                elif isinstance(hs_bundle.get("deals"), list): # Fallback falls du Struktur änderst
                     deal_ids = [str(d["id"]) for d in hs_bundle["deals"] if d.get("id")]

            JOB_STORE.update_contact(job_id, contact_id, step="write", last_message="Schreibe Note nach HubSpot…")

            # --- NEUER AUFRUF ---
            # Alles in einem Request
            note_id = writer.create_note_html_with_associations(
                html_body=html,
                contact_id=contact_id,
                deal_ids=deal_ids
            )

            write_json(os.path.join(cdir, "hubspot_write_result.json"), {
                "note_id": note_id,
                "contact_id": contact_id,
                "deal_ids": deal_ids,
                "written_at": _utc_now_iso(),
            })

            JOB_STORE.update_contact(job_id, contact_id, step="write", last_message=f"Note geschrieben (note_id={note_id})")
            results["created"] += 1
            results["details"].append({"contact_id": contact_id, "note_id": note_id, "deal_count": len(deal_ids)})

        except Exception as e:
            results["errors"] += 1
            results["details"].append({"contact_id": contact_id, "error": str(e)})
            write_json(os.path.join(cdir, "hubspot_write_error.json"), {"error": str(e), "ts": _utc_now_iso()})
            JOB_STORE.update_contact(job_id, contact_id, step="write", last_message="HubSpot write error", error=str(e))

    return results