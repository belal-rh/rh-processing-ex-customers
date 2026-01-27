# step4_render_hubspot_html.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from openai import OpenAI

from config import AppConfig, OpenAIConfig, load_config
from utils_csv import read_csv_rows, write_csv_rows


# --- OPTIMIERTER SYSTEM PROMPT (ENTWURF 1 - FAKTEN BASIERT) ---
HTML_RENDER_SYSTEM_PROMPT = """
Du bist ein Analyst für Vertriebsdaten. Deine Aufgabe ist es, komplexe Kundenhistorien in eine extrem kompakte "15-Sekunden-Übersicht" für Sales-Mitarbeiter zu verwandeln.

Input: Ein JSON-Objekt mit Kundeninformationen, Trello-Karten, Kommentaren und HubSpot-Calls.
Output: Ein HTML-Snippet (nur Body), das in HubSpot Notes gerendert wird.

Strenge Formatierungsvorgaben (HubSpot kompatibel):
1. Nutze NUR diese Tags: <b>, <i>, <u>, <br>, <ul>, <li>, <p>, <hr>.
2. KEINE Markdown-Codeblöcke (```html), keine <html> oder <body> Tags.

Inhaltliche Struktur (Zwingend einhalten):

<b>Betreuungszeitraum & Fokus</b>
[Startdatum] – [Enddatum] ([Paket/Dienstleistung])

<b>Initiale Herausforderung</b>
[Kurzer Satz: Was war das Problem zu Beginn?]

<b>Erreichte Erfolge</b>
<ul>
    <li>[Erfolg 1 (mit Zahlen wenn möglich)]</li>
    <li>[Erfolg 2]</li>
</ul>

<b>Herausforderungen zum Ende</b>
[Woran hakt es aktuell? Was wurde nicht gelöst?]

<b>Grund für Nicht-Verlängerung / Risiko</b>
[Warum verlängert der Kunde nicht? Oder was ist das größte Risiko (z.B. Liquidität, Mindset)?]

<b>Persönliche Situation & Fakten</b>
[Faktische persönliche Details aus dem Gesprächsverlauf: Genannte Urlaubsplanung, familiäre Situation, explizit geäußerte Wünsche (z.B. "will mehr Zeit für X"), Hobbys oder gesundheitliche Aspekte. 
WICHTIG: Keine psychologischen Interpretationen oder DISG-Typen (kein "Typ Gelb")! Nur Fakten.]

<hr>
<i>Datenbasis: Letzte 90 Tage</i>

Regeln für die KI:
- Fasse dich extrem kurz. Stichpunkte statt Romane.
- Wenn ein Punkt (z.B. Persönliches) nicht in den Daten ist, schreibe "Keine privaten Details bekannt".
- Interpretiere "Grund für nicht weitermachen", wenn es nicht explizit steht (suche nach Liquiditätsengpass, Unzufriedenheit, fehlender Umsetzung).
- Sprache: Deutsch.
""".strip()

HTML_RENDER_USER_PROMPT_PREFIX = "Erstelle die 15-Sekunden-Sales-Übersicht aus diesem JSON:\n\n"


@dataclass(frozen=True)
class Step4Input:
    step3_review_csv_path: str  # output/step3_final_review.csv
    delimiter: str = ","


@dataclass(frozen=True)
class Step4Outputs:
    output_csv_path: str
    failed_csv_path: str


def _safe_json_loads(s: str) -> dict[str, Any] | None:
    try:
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _render_html_via_responses(
    client: OpenAI,
    model: str,
    json_payload: dict[str, Any],
    max_retries: int = 4,
    backoff_base_seconds: float = 0.8,
) -> str:
    """
    Uses Chat Completions API to convert JSON -> HubSpot-compatible HTML.
    """
    user_input = HTML_RENDER_USER_PROMPT_PREFIX + json.dumps(json_payload, ensure_ascii=False)

    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": HTML_RENDER_SYSTEM_PROMPT},
                    {"role": "user", "content": user_input},
                ],
                temperature=0.3, # Niedrigere Temperature für konsistenteres Format
            )

            html = resp.choices[0].message.content
            if not html or not str(html).strip():
                raise RuntimeError("Empty output_text from model")
            
            # Clean potential markdown block markers if model ignores instructions
            cleaned_html = str(html).strip().replace("```html", "").replace("```", "")
            
            return cleaned_html

        except Exception as e:
            last_err = e
            time.sleep(backoff_base_seconds * (2 ** attempt))

    raise RuntimeError(f"HTML render failed after retries. Last error: {last_err}")


def run_step4_render_hubspot_html(
    app_cfg: AppConfig,
    oa_cfg: OpenAIConfig,
    step4_input: Step4Input,
    render_model: str,
) -> Step4Outputs:
    """
    Input CSV: step3_final_review.csv (must contain ai_json)
    Output CSV: step4_hubspot_notes.csv with hubspot_contact_id + html_note
    Failed CSV: step4_failed_render.csv for rows that couldn't be rendered
    """
    os.makedirs(app_cfg.output_dir, exist_ok=True)

    rows = read_csv_rows(step4_input.step3_review_csv_path, delimiter=step4_input.delimiter)

    out_csv_path = os.path.join(app_cfg.output_dir, "step4_hubspot_notes.csv")
    failed_csv_path = os.path.join(app_cfg.output_dir, "step4_failed_render.csv")

    client = OpenAI(api_key=oa_cfg.api_key)

    out_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []

    for r in rows:
        contact_id = (r.get("hubspot_contact_id", "") or "").strip()
        email = (r.get("email", "") or "").strip()
        ai_json_str = (r.get("ai_json", "") or "").strip()

        if not contact_id or not ai_json_str:
            continue

        payload = _safe_json_loads(ai_json_str)
        if not payload:
            failed_rows.append(
                {
                    "hubspot_contact_id": contact_id,
                    "email": email,
                    "error": "invalid_ai_json",
                    "raw_ai_json": ai_json_str,
                }
            )
            continue

        try:
            html_note = _render_html_via_responses(
                client=client,
                model=render_model,
                json_payload=payload,
                max_retries=oa_cfg.max_retries,
                backoff_base_seconds=oa_cfg.backoff_base_seconds,
            )

            out_rows.append({"hubspot_contact_id": contact_id, "email": email, "html_note": html_note})

        except Exception as e:
            failed_rows.append(
                {
                    "hubspot_contact_id": contact_id,
                    "email": email,
                    "error": str(e),
                    "raw_ai_json": ai_json_str,
                }
            )

    if out_rows:
        write_csv_rows(out_csv_path, out_rows, fieldnames=["hubspot_contact_id", "email", "html_note"])

    if failed_rows:
        write_csv_rows(failed_csv_path, failed_rows, fieldnames=["hubspot_contact_id", "email", "error", "raw_ai_json"])

    return Step4Outputs(output_csv_path=out_csv_path, failed_csv_path=failed_csv_path)


# -------------------------------------------------------------------
# NEW: Re-run helper for UI (no refetch; works per-contact directory)
# -------------------------------------------------------------------

def rerun_step4_from_local_ai(
    contact_dir: str,
    render_model: str | None = None,
) -> dict[str, Any]:
    """
    Re-run Step4 for a single contact using already stored local Step3 output.
    """
    contact_dir = os.path.abspath(contact_dir)
    if not os.path.isdir(contact_dir):
        return {"ok": False, "error": "contact_dir_not_found", "step4_html_path": ""}

    # Load configs from .env
    _app_cfg, _trello_cfg, _hs_cfg, oa_cfg = load_config()
    client = OpenAI(api_key=oa_cfg.api_key)

    # Resolve model
    model = (render_model or oa_cfg.step4_render_model or oa_cfg.model or "").strip()
    if not model:
        # last resort default
        model = "gpt-4o-mini" # Updated to current standard mini model

    meta = _safe_read_json(os.path.join(contact_dir, "meta.json"))

    # Load AI payload
    payload = _safe_read_json(os.path.join(contact_dir, "step3_ai.json"))
    if not payload:
        raw = _safe_read_text(os.path.join(contact_dir, "step3_raw.txt")).strip()
        payload = _safe_json_loads(raw) or {}

    if not payload:
        return {"ok": False, "error": "missing_step3_ai", "step4_html_path": ""}

    try:
        html_note = _render_html_via_responses(
            client=client,
            model=model,
            json_payload=payload,
            max_retries=oa_cfg.max_retries,
            backoff_base_seconds=oa_cfg.backoff_base_seconds,
        )

        html_path = os.path.join(contact_dir, "step4_note.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html_note)

        _safe_write_json(
            os.path.join(contact_dir, "step4_rerun_meta.json"),
            {
                "ts": int(time.time()),
                "ok": True,
                "model": model,
                "email": (meta.get("email") or "").strip(),
                "hubspot_contact_id": (meta.get("hubspot_contact_id") or "").strip(),
            },
        )

        return {"ok": True, "error": "", "step4_html_path": html_path}

    except Exception as e:
        _safe_write_json(
            os.path.join(contact_dir, "step4_failed_render.json"),
            {
                "ts": int(time.time()),
                "ok": False,
                "model": model,
                "error": str(e),
                "email": (meta.get("email") or "").strip(),
                "hubspot_contact_id": (meta.get("hubspot_contact_id") or "").strip(),
            },
        )
        return {"ok": False, "error": str(e), "step4_html_path": ""}


# ----------------------------
# Local helper functions (private)
# ----------------------------

def _safe_read_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def _safe_read_json(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _safe_write_json(path: str, obj: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass