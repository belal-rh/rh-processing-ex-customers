# step1_trello_fetch.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

from config import AppConfig, TrelloConfig
from trello_client import TrelloClient
from utils_csv import normalize_email, read_csv_rows, write_csv_rows


@dataclass(frozen=True)
class Step1ColumnMapping:
    # CSV#1
    csv1_email_col: str
    csv1_hubspot_id_col: str
    # CSV#2
    csv2_email_col: str
    csv2_trello_id_col: str


def _build_trello_text(card: dict[str, Any]) -> str:
    """
    Baut einen klaren Textblock inkl. Timestamps.
    Erwartet ein Trello "full card" dict (desc, checklists, actions).
    """
    name = card.get("name", "")
    desc = card.get("desc", "") or ""
    url = card.get("url", "") or ""

    # Checklists
    checklists = card.get("checklists", []) or []
    checklist_lines: list[str] = []
    for cl in checklists:
        if not isinstance(cl, dict):
            continue
        cl_name = cl.get("name", "")
        items = cl.get("checkItems", []) or []
        if cl_name:
            checklist_lines.append(f"- {cl_name}")
        for it in items:
            if not isinstance(it, dict):
                continue
            state = it.get("state", "")
            it_name = it.get("name", "")
            if it_name:
                checklist_lines.append(f"  - [{state}] {it_name}" if state else f"  - {it_name}")

    # Comments (actions)
    actions = card.get("actions", []) or []
    actions_sorted = sorted(actions, key=lambda a: a.get("date", ""))

    comment_lines: list[str] = []
    for a in actions_sorted:
        if not isinstance(a, dict):
            continue
        if a.get("type") != "commentCard":
            continue
        dt = a.get("date", "")
        mc = (a.get("memberCreator") or {}) if isinstance(a.get("memberCreator"), dict) else {}
        member = mc.get("fullName", "") or mc.get("username", "") or ""
        text = ((a.get("data") or {}) if isinstance(a.get("data"), dict) else {}).get("text") or ""
        if text.strip():
            head = f"{dt}"
            if member:
                head = f"{head} · {member}"
            comment_lines.append(f"- [{head}] {text.strip()}")

    parts: list[str] = []
    parts.append(f"TRELLO_CARD: {name}".strip())
    parts.append(f"URL: {url}".strip())

    if desc.strip():
        parts.append("\nDESCRIPTION:\n" + desc.strip())

    if checklist_lines:
        parts.append("\nCHECKLISTS:\n" + "\n".join(checklist_lines))

    if comment_lines:
        parts.append("\nCOMMENTS (timestamped):\n" + "\n".join(comment_lines))

    return "\n".join([p for p in parts if p]).strip()


def run_step1_trello_fetch(
    app_cfg: AppConfig,
    trello_cfg: TrelloConfig,
    csv1_path: str,
    csv2_path: str,
    mapping: Step1ColumnMapping,
    delimiter1: str = ",",
    delimiter2: str = ",",
) -> dict[str, str]:
    """
    NEU:
    - Matcht emails aus CSV#1 gegen CSV#2
    - Wenn mehrere Trello-IDs: wird NICHT mehr geskippt, sondern ALLE werden verarbeitet.
    - Zusätzlich: duplicates CSV bleibt bestehen als Review-Liste (IDs + Links in separaten Spalten).
    - Output:
        - output/step1_duplicates.csv  (Review)
        - output/step1_trello_enriched.jsonl (Audit/Debug)
        - output/step1_ready_for_step2.csv (Input für Step2)
    """
    os.makedirs(app_cfg.output_dir, exist_ok=True)

    csv1 = read_csv_rows(csv1_path, delimiter=delimiter1)
    csv2 = read_csv_rows(csv2_path, delimiter=delimiter2)

    # index CSV#2 by email
    email_to_trello_ids: dict[str, list[str]] = {}
    for r in csv2:
        em = normalize_email(r.get(mapping.csv2_email_col, ""))
        tid = (r.get(mapping.csv2_trello_id_col, "") or "").strip()
        if not em or not tid:
            continue
        email_to_trello_ids.setdefault(em, []).append(tid)

    client = TrelloClient(trello_cfg)

    duplicates_rows: list[dict[str, Any]] = []
    ready_rows: list[dict[str, Any]] = []

    jsonl_path = os.path.join(app_cfg.output_dir, app_cfg.trello_enriched_jsonl_name)
    duplicates_path = os.path.join(app_cfg.output_dir, app_cfg.duplicates_csv_name)
    ready_path = os.path.join(app_cfg.output_dir, app_cfg.trello_ready_csv_name)

    # for duplicates CSV: dynamic columns link_1..link_n
    max_dupe = 0
    tmp_dupes: list[tuple[str, list[str]]] = []

    # Candidates: now also store ALL trello_ids
    candidates: list[dict[str, Any]] = []

    for r in csv1:
        em = normalize_email(r.get(mapping.csv1_email_col, ""))
        hs_id = (r.get(mapping.csv1_hubspot_id_col, "") or "").strip()
        if not em or not hs_id:
            continue

        trello_ids = email_to_trello_ids.get(em, [])
        unique_ids: list[str] = []
        seen = set()
        for tid in trello_ids:
            tid = (tid or "").strip()
            if not tid or tid in seen:
                continue
            seen.add(tid)
            unique_ids.append(tid)

        if len(unique_ids) == 0:
            # aktuell: skip (kannst Du später als "missing_trello.csv" ergänzen)
            continue

        if len(unique_ids) > 1:
            max_dupe = max(max_dupe, len(unique_ids))
            tmp_dupes.append((em, unique_ids))

        # IMPORTANT: we still process ALL ids
        candidates.append({"email": em, "hubspot_contact_id": hs_id, "trello_ids": unique_ids})

    # Build duplicates CSV with separate id/link columns (still useful for manual review)
    if tmp_dupes:
        link_cols = [f"trello_link_{i}" for i in range(1, max_dupe + 1)]
        id_cols = [f"trello_id_{i}" for i in range(1, max_dupe + 1)]
        dupe_fieldnames = ["email"] + id_cols + link_cols

        for em, ids in tmp_dupes:
            row: dict[str, Any] = {"email": em}
            for idx, tid in enumerate(ids, start=1):
                row[f"trello_id_{idx}"] = tid
                # keep your existing short_link_base usage
                row[f"trello_link_{idx}"] = f"{trello_cfg.short_link_base}{tid}"
            duplicates_rows.append(row)

        write_csv_rows(duplicates_path, duplicates_rows, dupe_fieldnames)

    # Fetch + enrich candidates
    with open(jsonl_path, "w", encoding="utf-8") as jf:
        for c in candidates:
            trello_ids: list[str] = c["trello_ids"]

            card_payloads: list[dict[str, Any]] = []
            per_card_errors: list[dict[str, Any]] = []
            trello_blocks: list[str] = []
            trello_urls: list[str] = []

            for tid in trello_ids:
                try:
                    card = client.fetch_card_full(tid)
                    card_payloads.append(card)
                    url = card.get("url", "") or f"{trello_cfg.short_link_base}{tid}"
                    trello_urls.append(url)

                    txt = _build_trello_text(card)
                    trello_blocks.append(
                        f"===== TRELLO CARD START: {tid} =====\n{txt}\n===== TRELLO CARD END: {tid} ====="
                    )
                except Exception as e:
                    per_card_errors.append({"trello_id": tid, "error": str(e)})

            if not trello_blocks:
                # Nothing usable for this contact -> still write an audit line
                enriched_fail = {
                    "email": c["email"],
                    "hubspot_contact_id": c["hubspot_contact_id"],
                    "trello_ids": trello_ids,
                    "errors": per_card_errors,
                    "trello_text": "",
                    "status": "error",
                }
                jf.write(json.dumps(enriched_fail, ensure_ascii=False) + "\n")
                continue

            trello_text = "\n\n".join(trello_blocks).strip()

            enriched = {
                "email": c["email"],
                "hubspot_contact_id": c["hubspot_contact_id"],
                "trello_ids": trello_ids,
                "trello_urls": trello_urls,
                "errors": per_card_errors,
                "trello_text": trello_text,
                "status": "ok",
            }
            jf.write(json.dumps(enriched, ensure_ascii=False) + "\n")

            # ready for step2: keep compatibility + include multi info
            ready_rows.append(
                {
                    "email": c["email"],
                    "hubspot_contact_id": c["hubspot_contact_id"],
                    # compatibility fields:
                    "trello_id": trello_ids[0],
                    "trello_url": trello_urls[0] if trello_urls else f"{trello_cfg.short_link_base}{trello_ids[0]}",
                    # new fields:
                    "trello_ids": ";".join(trello_ids),
                    "trello_urls": ";".join(trello_urls),
                    "trello_text": trello_text,
                    "trello_errors_json": json.dumps(per_card_errors, ensure_ascii=False),
                }
            )

    if ready_rows:
        ready_fields = [
            "email",
            "hubspot_contact_id",
            "trello_id",
            "trello_url",
            "trello_ids",
            "trello_urls",
            "trello_text",
            "trello_errors_json",
        ]
        write_csv_rows(ready_path, ready_rows, ready_fields)

    return {
        "duplicates_csv": duplicates_path if duplicates_rows else "",
        "trello_jsonl": jsonl_path,
        "ready_csv": ready_path if ready_rows else "",
    }
