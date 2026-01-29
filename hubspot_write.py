# hubspot_write.py
from __future__ import annotations

import time
from typing import Any, Iterable

import requests

from config import HubSpotConfig
from rate_limit import TokenBucketLimiter, RateLimitConfig, compute_backoff


HUBSPOT_DEFINED = "HUBSPOT_DEFINED"


class HubSpotWriteClient:
    """
    Write client for HubSpot CRM.
    Preferred strategy:
      - Create Note (v3) AND associate to Contact (+ Deals) in a single POST request.
        This avoids association v4 edge cases / 404s.
    """

    def __init__(self, cfg: HubSpotConfig, note_to_contact_type_id: int, note_to_deal_type_id: int):
        self.cfg = cfg
        self.note_to_contact_type_id = int(note_to_contact_type_id)
        self.note_to_deal_type_id = int(note_to_deal_type_id)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.cfg.private_app_token}",
                "Content-Type": "application/json",
            }
        )

        # Keep conservative; HubSpot can rate limit quickly in bursts
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=2.0, burst=2))

    def _request(self, method: str, path: str, json_body: dict[str, Any] | list[Any] | None = None) -> dict[str, Any]:
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

                # helpful error content
                if resp.status_code >= 400:
                    raise RuntimeError(f"HubSpot Client Error {resp.status_code}: {resp.text}")

                return resp.json() if resp.text else {}

            except Exception as e:
                last_err = e
                time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))

        raise RuntimeError(f"HubSpot write failed: {method} {path}. Last error: {last_err}")

    # -------------------------
    # Legacy methods (keep)
    # -------------------------

    def create_note_html(self, html_body: str, timestamp_ms: int | None = None) -> str:
        """
        Legacy: creates a note only. (No associations)
        NOTE: In many portals hs_timestamp is required.
        """
        props: dict[str, Any] = {"hs_note_body": html_body}

        if timestamp_ms is not None:
            props["hs_timestamp"] = str(timestamp_ms)
        else:
            # safest: set NOW in ms if caller didn't provide
            props["hs_timestamp"] = str(int(time.time() * 1000))

        data = self._request("POST", "/crm/v3/objects/notes", json_body={"properties": props})
        note_id = str(data.get("id", "") or "")
        if not note_id:
            raise RuntimeError("Failed to create note: missing id")
        return note_id

    def associate_note_to_contact(self, note_id: str, contact_id: str) -> None:
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/contacts/{contact_id}/{self.note_to_contact_type_id}",
            json_body=[{"associationCategory": HUBSPOT_DEFINED, "associationTypeId": self.note_to_contact_type_id}],
        )

    def associate_note_to_deal(self, note_id: str, deal_id: str) -> None:
        self._request(
            "PUT",
            f"/crm/v4/objects/notes/{note_id}/associations/deals/{deal_id}/{self.note_to_deal_type_id}",
            json_body=[{"associationCategory": HUBSPOT_DEFINED, "associationTypeId": self.note_to_deal_type_id}],
        )

    # -------------------------
    # Preferred method (NEW)
    # -------------------------

    def create_note_html_with_associations(
        self,
        html_body: str,
        contact_id: str,
        deal_ids: Iterable[str] | None = None,
        timestamp_iso_utc: str | None = None,
        timestamp_ms: int | None = None,
    ) -> str:
        """
        Preferred: Create Note and associate to Contact (+ optional Deals) in a single POST.

        hs_timestamp:
          - If timestamp_iso_utc provided -> used directly (e.g. "2026-01-29T10:45:00Z")
          - Else if timestamp_ms provided -> used as string
          - Else -> now() in ms
        """
        if not contact_id or not str(contact_id).strip():
            raise ValueError("contact_id required")

        # hs_timestamp can be ISO string OR ms string
        if timestamp_iso_utc and timestamp_iso_utc.strip():
            hs_timestamp_val = timestamp_iso_utc.strip()
        elif timestamp_ms is not None:
            hs_timestamp_val = str(int(timestamp_ms))
        else:
            hs_timestamp_val = str(int(time.time() * 1000))

        associations: list[dict[str, Any]] = [
            {
                "to": {"id": str(contact_id)},
                "types": [
                    {
                        "associationCategory": HUBSPOT_DEFINED,
                        "associationTypeId": self.note_to_contact_type_id,
                    }
                ],
            }
        ]

        if deal_ids:
            for did in deal_ids:
                did = (str(did) or "").strip()
                if not did:
                    continue
                associations.append(
                    {
                        "to": {"id": did},
                        "types": [
                            {
                                "associationCategory": HUBSPOT_DEFINED,
                                "associationTypeId": self.note_to_deal_type_id,
                            }
                        ],
                    }
                )

        payload = {
            "properties": {
                "hs_timestamp": hs_timestamp_val,
                "hs_note_body": html_body,
            },
            "associations": associations,
        }

        data = self._request("POST", "/crm/v3/objects/notes", json_body=payload)
        note_id = str(data.get("id", "") or "")
        if not note_id:
            raise RuntimeError(f"Failed to create note with associations: missing id. resp={data}")
        return note_id

    # Convenience helper for pipeline usage
    def push_verified_note_to_hubspot(
        self,
        contact_id: str,
        html_note: str,
        deal_ids: list[str] | None = None,
        timestamp_ms: int | None = None,
        timestamp_iso_utc: str | None = None,
    ) -> dict[str, Any]:
        """
        Returns a structured result for logging/CSV:
          { ok, contact_id, note_id, deal_ids, error }
        """
        try:
            note_id = self.create_note_html_with_associations(
                html_body=html_note,
                contact_id=contact_id,
                deal_ids=deal_ids or [],
                timestamp_ms=timestamp_ms,
                timestamp_iso_utc=timestamp_iso_utc,
            )
            return {
                "ok": True,
                "contact_id": str(contact_id),
                "note_id": str(note_id),
                "deal_ids": [str(d) for d in (deal_ids or [])],
                "error": "",
            }
        except Exception as e:
            return {
                "ok": False,
                "contact_id": str(contact_id),
                "note_id": "",
                "deal_ids": [str(d) for d in (deal_ids or [])],
                "error": str(e),
            }

# ... (dein bestehender Code der Klasse HubSpotWriteClient) ...

# Füge dies AM ENDE von hubspot_write.py hinzu:

def push_verified_note_to_hubspot(
    contact_dir: str,
    *,
    also_associate_deals: bool = True,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """
    Standalone UI Helper: Liest Daten aus contact_dir und nutzt HubSpotWriteClient.
    """
    contact_dir = os.path.abspath(contact_dir)

    # 1. Daten lesen
    def _read_json(path):
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f: return json.load(f)
        return {}

    def _read_text(path):
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f: return f.read()
        return ""

    def _write_json(path, obj):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    meta = _read_json(os.path.join(contact_dir, "meta.json"))
    verified = _read_json(os.path.join(contact_dir, "verified.json"))
    html_note = _read_text(os.path.join(contact_dir, "step4_note.html")).strip()

    contact_id = str(meta.get("hubspot_contact_id") or meta.get("contact_id") or "").strip()
    email = str(meta.get("email") or "").strip()

    # 2. Validierung
    error_res = None
    if not contact_id:
        error_res = {"error": "missing_hubspot_contact_id"}
    elif not verified.get("verified"):
        error_res = {"error": "not_verified"}
    elif not html_note:
        error_res = {"error": "missing_step4_note_html"}

    if error_res:
        error_res.update({"ok": False, "contact_id": contact_id, "email": email})
        _write_json(os.path.join(contact_dir, "hubspot_write_error.json"), error_res)
        return error_res

    # 3. Config laden & Client init
    from config import load_config
    _app, _trello, hs_cfg, _oa = load_config()

    # IDs aus Config oder Env Fallback
    note_to_contact = getattr(hs_cfg, "note_to_contact_type_id", 0)
    note_to_deal = getattr(hs_cfg, "note_to_deal_type_id", 0)

    client = HubSpotWriteClient(
        hs_cfg, 
        note_to_contact_type_id=note_to_contact, 
        note_to_deal_type_id=note_to_deal
    )

    # 4. Deal IDs sammeln
    deal_ids = []
    if also_associate_deals:
        step2 = _read_json(os.path.join(contact_dir, "step2_hubspot.json"))
        # Versuche verschiedene Formate zu parsen
        if isinstance(step2.get("deal_ids"), list):
            deal_ids = step2["deal_ids"]
        elif isinstance(step2.get("deals"), list):
             # Falls Format: deals: [{id: 123}, ...]
            deal_ids = [str(d.get("id")) for d in step2["deals"] if d.get("id")]
    
    # Filtern leerer IDs
    deal_ids = [str(d).strip() for d in deal_ids if str(d).strip()]

    # 5. Ausführen über neue Methode
    try:
        # Hier nutzen wir deine NEUE Methode
        res = client.push_verified_note_to_hubspot(
            contact_id=contact_id,
            html_note=html_note,
            deal_ids=deal_ids,
            timestamp_ms=timestamp_ms
        )
        
        # Ergebnis speichern
        if res["ok"]:
            _write_json(os.path.join(contact_dir, "hubspot_write_result.json"), res)
        else:
            _write_json(os.path.join(contact_dir, "hubspot_write_error.json"), res)
            
        return res

    except Exception as e:
        final_err = {"ok": False, "error": str(e), "contact_id": contact_id}
        _write_json(os.path.join(contact_dir, "hubspot_write_error.json"), final_err)
        return final_err