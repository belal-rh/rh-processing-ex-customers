# step2_hubspot_fetch.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from config import AppConfig, HubSpotConfig
from hubspot_client import HubSpotClient
from utils_csv import read_csv_rows, write_csv_rows


def _ms_to_iso(ms: str | int | None) -> str:
    if ms is None or ms == "":
        return ""
    try:
        val = int(ms)
        dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
        # Keep ISO UTC, e.g. 2025-01-02T13:45:00+00:00
        return dt.isoformat()
    except Exception:
        return str(ms)


def _build_hubspot_text(notes: list[dict[str, Any]], calls: list[dict[str, Any]]) -> str:
    """
    notes/calls entries should already include timestamped, normalized fields.
    """
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
            outcome = c.get("outcome", "")
            body = c.get("body", "")
            line = f"- [{ts}] OUTCOME={outcome} | {body}".strip()
            parts.append(line)
        parts.append("")

    return "\n".join(parts).strip()


@dataclass(frozen=True)
class Step2Input:
    step1_ready_csv_path: str
    delimiter: str = ","


def run_step2_hubspot_fetch(
    app_cfg: AppConfig,
    hs_cfg: HubSpotConfig,
    step2_input: Step2Input,
) -> dict[str, str]:
    """
    Input: output/step1_ready_for_step2.csv
      columns: email, hubspot_contact_id, trello_id, trello_url, trello_text

    Output:
      - output/step2_hubspot_enriched.jsonl (per contact: notes + calls + hubspot_text)
      - output/step2_merged_ready_for_ai.csv (trello_text + hubspot_text combined)
    """
    os.makedirs(app_cfg.output_dir, exist_ok=True)

    rows = read_csv_rows(step2_input.step1_ready_csv_path, delimiter=step2_input.delimiter)
    client = HubSpotClient(hs_cfg)

    jsonl_path = os.path.join(app_cfg.output_dir, app_cfg.hubspot_enriched_jsonl_name)
    merged_csv_path = os.path.join(app_cfg.output_dir, app_cfg.merged_ready_csv_name)

    merged_rows: list[dict[str, Any]] = []

    with open(jsonl_path, "w", encoding="utf-8") as jf:
        for r in rows:
            contact_id = (r.get("hubspot_contact_id", "") or "").strip()
            if not contact_id:
                continue

            # 1) Find associated object IDs
            note_ids = client.list_associated_object_ids(contact_id, "notes")
            call_ids = client.list_associated_object_ids(contact_id, "calls")

            # 2) Batch read objects
            note_props = ["hs_note_body", "hs_timestamp", "hs_createdate"]
            call_props = ["hs_call_body", "hs_call_outcome", "hs_timestamp", "hs_createdate"]

            notes_raw = client.batch_read_objects("notes", note_ids, note_props) if note_ids else []
            calls_raw = client.batch_read_objects("calls", call_ids, call_props) if call_ids else []

            # 3) Normalize & sort chronologically
            notes_norm: list[dict[str, str]] = []
            for n in notes_raw:
                props = n.get("properties", {}) or {}
                ts = props.get("hs_timestamp") or props.get("hs_createdate")
                notes_norm.append(
                    {
                        "timestamp": _ms_to_iso(ts),
                        "body": (props.get("hs_note_body") or "").strip(),
                        "id": str(n.get("id", "")),
                    }
                )
            notes_norm.sort(key=lambda x: x.get("timestamp", ""))

            calls_norm: list[dict[str, str]] = []
            for c in calls_raw:
                props = c.get("properties", {}) or {}
                ts = props.get("hs_timestamp") or props.get("hs_createdate")
                calls_norm.append(
                    {
                        "timestamp": _ms_to_iso(ts),
                        "outcome": (props.get("hs_call_outcome") or "").strip(),
                        "body": (props.get("hs_call_body") or "").strip(),
                        "id": str(c.get("id", "")),
                    }
                )
            calls_norm.sort(key=lambda x: x.get("timestamp", ""))

            hubspot_text = _build_hubspot_text(notes_norm, calls_norm)

            enriched = {
                "email": (r.get("email", "") or "").strip(),
                "hubspot_contact_id": contact_id,
                "hubspot_notes": notes_norm,
                "hubspot_calls": calls_norm,
                "hubspot_text": hubspot_text,
            }
            jf.write(json.dumps(enriched, ensure_ascii=False) + "\n")

            merged_text = "\n\n".join(
                [
                    (r.get("trello_text", "") or "").strip(),
                    hubspot_text,
                ]
            ).strip()

            merged_rows.append(
                {
                    "email": (r.get("email", "") or "").strip(),
                    "hubspot_contact_id": contact_id,
                    "trello_id": (r.get("trello_id", "") or "").strip(),
                    "trello_url": (r.get("trello_url", "") or "").strip(),
                    "merged_context_text": merged_text,
                }
            )

    if merged_rows:
        fields = ["email", "hubspot_contact_id", "trello_id", "trello_url", "merged_context_text"]
        write_csv_rows(merged_csv_path, merged_rows, fields)

    return {
        "hubspot_jsonl": jsonl_path,
        "merged_ready_csv": merged_csv_path if merged_rows else "",
    }
