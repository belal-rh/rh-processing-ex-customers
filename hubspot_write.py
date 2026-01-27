# hubspot_write.py
from __future__ import annotations

import json
import os
import time
from typing import Any

import requests

from config import HubSpotConfig
from rate_limit import TokenBucketLimiter, RateLimitConfig, compute_backoff


class HubSpotWriteClient:
    def __init__(self, cfg: HubSpotConfig, note_to_contact_type_id: int, note_to_deal_type_id: int):
        self.cfg = cfg
        self.note_to_contact_type_id = note_to_contact_type_id
        self.note_to_deal_type_id = note_to_deal_type_id

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.cfg.private_app_token}",
                "Content-Type": "application/json",
            }
        )

        # conservative default; you can tune per cfg later
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=2.0, burst=2))

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

    def create_note_html(self, html_body: str, timestamp_ms: int | None = None) -> str:
        # hs_note_body supports HTML
        props = {"hs_note_body": html_body}
        if timestamp_ms is not None:
            props["hs_timestamp"] = str(timestamp_ms)

        data = self._request("POST", "/crm/v3/objects/notes", json_body={"properties": props})
        note_id = str(data.get("id", "") or "")
        if not note_id:
            raise RuntimeError("Failed to create note: missing id")
        return note_id

    def associate_note_to_contact(self, note_id: str, contact_id: str) -> None:
        # v4 association create
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/contacts/{contact_id}/{self.note_to_contact_type_id}",
            json_body=None,
        )

    def associate_note_to_deal(self, note_id: str, deal_id: str) -> None:
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/deals/{deal_id}/{self.note_to_deal_type_id}",
            json_body=None,
        )


# -------------------------------------------------------------------
# NEW: UI helper - push verified HTML note to HubSpot for ONE contact
# -------------------------------------------------------------------

def push_verified_note_to_hubspot(
    contact_dir: str,
    *,
    also_associate_deals: bool = True,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """
    Pushes the locally rendered HTML note to HubSpot and stores results to disk.

    Expected files in contact_dir:
      - meta.json (must contain hubspot_contact_id)
      - verified.json (must have {"verified": true})
      - step4_note.html (HTML body)

    Optional:
      - step2_hubspot.json (to discover deal_ids; preferred)
      - hubspot_deals.json (alternative)

    Writes:
      - hubspot_write_result.json on success
      - hubspot_write_error.json on failure

    Returns a dict with result details.
    """
    contact_dir = os.path.abspath(contact_dir)

    def read_json(path: str) -> dict[str, Any]:
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    def read_text(path: str) -> str:
        if not os.path.exists(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            return ""

    def write_json(path: str, obj: Any) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    meta = read_json(os.path.join(contact_dir, "meta.json"))
    verified = read_json(os.path.join(contact_dir, "verified.json"))
    html_note = read_text(os.path.join(contact_dir, "step4_note.html")).strip()

    contact_id = str(meta.get("hubspot_contact_id") or meta.get("contact_id") or "").strip()
    email = str(meta.get("email") or "").strip()

    if not contact_id:
        res = {"ok": False, "error": "missing_hubspot_contact_id", "contact_id": "", "email": email}
        write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
        return res

    if not bool(verified.get("verified", False)):
        res = {"ok": False, "error": "not_verified", "contact_id": contact_id, "email": email}
        write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
        return res

    if not html_note:
        res = {"ok": False, "error": "missing_step4_note_html", "contact_id": contact_id, "email": email}
        write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
        return res

    # Load config + association type ids
    # NOTE: These IDs should exist already in your config / discovery logic.
    from config import load_config  # local import to avoid cycles

    _app_cfg, _trello_cfg, hs_cfg, _oa_cfg = load_config()

    # These should be placed in config.py (constants), but we keep it minimal here:
    note_to_contact_type_id = getattr(hs_cfg, "note_to_contact_type_id", None)
    note_to_deal_type_id = getattr(hs_cfg, "note_to_deal_type_id", None)

    if not isinstance(note_to_contact_type_id, int) or not isinstance(note_to_deal_type_id, int):
        res = {
            "ok": False,
            "error": "missing_association_type_ids_in_hubspot_config",
            "contact_id": contact_id,
            "email": email,
            "hint": "Set hs_cfg.note_to_contact_type_id and hs_cfg.note_to_deal_type_id (int).",
        }
        write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
        return res

    # Deal discovery (best-effort)
    deal_ids: list[str] = []
    if also_associate_deals:
        # preferred: step2_hubspot.json has discovered deal ids
        step2 = read_json(os.path.join(contact_dir, "step2_hubspot.json"))
        # Try common shapes: {"deal_ids":[...]} or {"deals":[{"id":...},...]}
        if isinstance(step2.get("deal_ids"), list):
            deal_ids = [str(x).strip() for x in step2.get("deal_ids") if str(x).strip()]
        elif isinstance(step2.get("deals"), list):
            tmp = []
            for d in step2.get("deals"):
                if isinstance(d, dict) and d.get("id"):
                    tmp.append(str(d["id"]).strip())
            deal_ids = tmp

        # alternative: hubspot_deals.json
        if not deal_ids:
            deals_alt = read_json(os.path.join(contact_dir, "hubspot_deals.json"))
            if isinstance(deals_alt.get("deal_ids"), list):
                deal_ids = [str(x).strip() for x in deals_alt.get("deal_ids") if str(x).strip()]

    client = HubSpotWriteClient(
        hs_cfg,
        note_to_contact_type_id=note_to_contact_type_id,
        note_to_deal_type_id=note_to_deal_type_id,
    )

    try:
        note_id = client.create_note_html(html_note, timestamp_ms=timestamp_ms)
        client.associate_note_to_contact(note_id, contact_id)

        assoc_errors: list[dict[str, Any]] = []
        if also_associate_deals and deal_ids:
            for did in deal_ids:
                try:
                    client.associate_note_to_deal(note_id, did)
                except Exception as e:
                    assoc_errors.append({"deal_id": did, "error": str(e)})

        res = {
            "ok": True,
            "ts": int(time.time()),
            "contact_id": contact_id,
            "email": email,
            "note_id": note_id,
            "deal_ids": deal_ids,
            "deal_assoc_errors": assoc_errors,
        }
        write_json(os.path.join(contact_dir, "hubspot_write_result.json"), res)
        return res

    except Exception as e:
        res = {
            "ok": False,
            "ts": int(time.time()),
            "contact_id": contact_id,
            "email": email,
            "error": str(e),
        }
        write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
        return res
