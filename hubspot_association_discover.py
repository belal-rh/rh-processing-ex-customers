# hubspot_association_discover.py
from __future__ import annotations

import os
import sys
import json
import requests
from typing import Any


def _get_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def fetch_association_labels(token: str, from_type: str, to_type: str) -> dict[str, Any]:
    """
    HubSpot v4 association labels:
      GET /crm/v4/associations/{fromObjectType}/{toObjectType}/labels
    """
    url = f"https://api.hubapi.com/crm/v4/associations/{from_type}/{to_type}/labels"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def main() -> None:
    """
    Usage:
      export HUBSPOT_PRIVATE_APP_TOKEN=...
      python hubspot_association_discover.py

    Prints the association labels (typeId) for:
      notes -> contacts
      notes -> deals
    """
    token = _get_env("HUBSPOT_PRIVATE_APP_TOKEN")

    pairs = [
        ("notes", "contacts"),
        ("notes", "deals"),
    ]

    out: dict[str, Any] = {}
    for frm, to in pairs:
        data = fetch_association_labels(token, frm, to)
        out[f"{frm}_to_{to}"] = data

    print(json.dumps(out, ensure_ascii=False, indent=2))

    # Helpful: extract all typeIds
    def extract_type_ids(data: dict[str, Any]) -> list[int]:
        results = data.get("results") or data.get("labels") or []
        type_ids: list[int] = []
        if isinstance(results, list):
            for r in results:
                tid = r.get("typeId")
                if isinstance(tid, int):
                    type_ids.append(tid)
        return type_ids

    print("\n--- Suggested .env values (pick the correct label if multiple exist) ---")
    notes_contacts_ids = extract_type_ids(out.get("notes_to_contacts", {}))
    notes_deals_ids = extract_type_ids(out.get("notes_to_deals", {}))

    if notes_contacts_ids:
        print(f"HS_ASSOC_NOTE_TO_CONTACT_TYPE_ID={notes_contacts_ids[0]}")
    else:
        print("HS_ASSOC_NOTE_TO_CONTACT_TYPE_ID=<NOT_FOUND>")

    if notes_deals_ids:
        print(f"HS_ASSOC_NOTE_TO_DEAL_TYPE_ID={notes_deals_ids[0]}")
    else:
        print("HS_ASSOC_NOTE_TO_DEAL_TYPE_ID=<NOT_FOUND>")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
