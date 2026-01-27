# step3_openai_assistant.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any

from config import AppConfig, OpenAIConfig, load_config
from openai_assistant_client import OpenAIAssistantClient
from utils_csv import read_csv_rows, write_csv_rows


@dataclass(frozen=True)
class Step3Input:
    step2_merged_csv_path: str
    delimiter: str = ","


def _safe_json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _get(obj: dict[str, Any], path: list[str], default=None):
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur


def _join_list(items: Any, sep: str = " | ") -> str:
    if not items:
        return ""
    if isinstance(items, list):
        return sep.join([str(x) for x in items if x is not None and str(x).strip() != ""])
    return str(items)


def _flatten_successes(items: Any) -> str:
    if not isinstance(items, list) or not items:
        return ""
    parts = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "").strip()
        details = (it.get("details") or "").strip()
        approx = (it.get("approx_date") or "").strip()
        line = " - ".join([x for x in [title, details] if x])
        if approx:
            line = f"[{approx}] {line}" if line else f"[{approx}]"
        if line:
            parts.append(line)
    return "\n".join(parts)


def _flatten_challenges(items: Any) -> str:
    if not isinstance(items, list) or not items:
        return ""
    parts = []
    for it in items:
        if not isinstance(it, dict):
            continue
        title = (it.get("title") or "").strip()
        details = (it.get("details") or "").strip()
        approx = (it.get("approx_date") or "").strip()
        line = " - ".join([x for x in [title, details] if x])
        if approx:
            line = f"[{approx}] {line}" if line else f"[{approx}]"
        if line:
            parts.append(line)
    return "\n".join(parts)


def _flatten_churn_reasons(items: Any) -> str:
    if not isinstance(items, list) or not items:
        return ""
    parts = []
    for it in items:
        if not isinstance(it, dict):
            continue
        reason = (it.get("reason") or "").strip()
        conf = (it.get("confidence") or "").strip()
        approx = (it.get("approx_date") or "").strip()
        line = reason
        if conf:
            line = f"{line} (confidence={conf})" if line else f"(confidence={conf})"
        if approx:
            line = f"[{approx}] {line}" if line else f"[{approx}]"
        if line:
            parts.append(line)
    return "\n".join(parts)


def _validate_schema_min(parsed: dict[str, Any]) -> tuple[bool, str]:
    """
    Minimal validation: must be dict and contain the top-level keys we need.
    Keep it light to avoid false negatives.
    """
    required = [
        "summary",
        "successes",
        "challenges",
        "churn_reasons",
        "relationship_value",
        "next_best_actions",
        "open_questions_for_review",
        "red_flags",
    ]
    for k in required:
        if k not in parsed:
            return False, f"missing_key:{k}"
    if not isinstance(parsed.get("summary"), dict):
        return False, "summary_not_dict"
    if not isinstance(parsed.get("relationship_value"), dict):
        return False, "relationship_value_not_dict"
    return True, ""


def run_step3_openai_assistant(
    app_cfg: AppConfig,
    oa_cfg: OpenAIConfig,
    step3_input: Step3Input,
    extra_user_prompt: str = "",
) -> dict[str, str]:
    os.makedirs(app_cfg.output_dir, exist_ok=True)

    rows = read_csv_rows(step3_input.step2_merged_csv_path, delimiter=step3_input.delimiter)
    client = OpenAIAssistantClient(oa_cfg)

    jsonl_path = os.path.join(app_cfg.output_dir, app_cfg.step3_ai_jsonl_name)
    out_csv_path = os.path.join(app_cfg.output_dir, app_cfg.step3_output_csv_name)
    failed_csv_path = os.path.join(app_cfg.output_dir, app_cfg.step3_failed_parse_csv_name)

    out_rows: list[dict[str, Any]] = []
    failed_rows: list[dict[str, Any]] = []

    with open(jsonl_path, "w", encoding="utf-8") as jf:
        for r in rows:
            email = (r.get("email", "") or "").strip()
            contact_id = (r.get("hubspot_contact_id", "") or "").strip()
            merged_text = (r.get("merged_context_text", "") or "").strip()

            if not contact_id or not merged_text:
                continue

            raw = client.summarize_with_assistant(
                merged_context_text=merged_text,
                extra_user_prompt=extra_user_prompt if extra_user_prompt.strip() else None,
            )

            record_base = {
                "email": email,
                "hubspot_contact_id": contact_id,
            }

            parsed: dict[str, Any] | None = None
            parse_error = ""
            schema_ok = False
            schema_error = ""

            try:
                parsed_candidate = json.loads(raw)
                if isinstance(parsed_candidate, dict):
                    parsed = parsed_candidate
                    schema_ok, schema_error = _validate_schema_min(parsed)
                else:
                    parse_error = "json_not_object"
            except Exception as e:
                parse_error = f"json_parse_error:{type(e).__name__}"

            # write jsonl for audit regardless
            jf.write(
                _safe_json_dumps(
                    {
                        **record_base,
                        "raw": raw,
                        "parsed": parsed,
                        "parse_error": parse_error,
                        "schema_ok": schema_ok,
                        "schema_error": schema_error,
                    }
                )
                + "\n"
            )

            if not parsed or parse_error or not schema_ok:
                failed_rows.append(
                    {
                        "hubspot_contact_id": contact_id,
                        "email": email,
                        "error": parse_error or schema_error or "unknown_error",
                        "raw": raw,
                    }
                )
                continue

            # flatten important fields for review
            summary_one_liner = str(_get(parsed, ["summary", "one_liner"], "") or "")
            summary_short = str(_get(parsed, ["summary", "short"], "") or "")
            time_from = str(_get(parsed, ["summary", "time_range", "from"], "") or "")
            time_to = str(_get(parsed, ["summary", "time_range", "to"], "") or "")
            recency_note = str(_get(parsed, ["summary", "data_recency_note"], "") or "")

            rel_score = _get(parsed, ["relationship_value", "score_1_to_5"], "")
            rel_expl = str(_get(parsed, ["relationship_value", "explanation"], "") or "")
            rel_pos = _join_list(_get(parsed, ["relationship_value", "signals_positive"], []), sep=" | ")
            rel_neg = _join_list(_get(parsed, ["relationship_value", "signals_negative"], []), sep=" | ")

            successes_txt = _flatten_successes(parsed.get("successes"))
            challenges_txt = _flatten_challenges(parsed.get("challenges"))
            churn_txt = _flatten_churn_reasons(parsed.get("churn_reasons"))

            next_actions = parsed.get("next_best_actions", [])
            next_actions_txt = ""
            if isinstance(next_actions, list) and next_actions:
                lines = []
                for a in next_actions:
                    if not isinstance(a, dict):
                        continue
                    action = (a.get("action") or "").strip()
                    why = (a.get("why") or "").strip()
                    prio = (a.get("priority") or "").strip()
                    line = action
                    if prio:
                        line = f"[{prio}] {line}" if line else f"[{prio}]"
                    if why:
                        line = f"{line} — {why}" if line else why
                    if line:
                        lines.append(line)
                next_actions_txt = "\n".join(lines)

            open_q_txt = _join_list(parsed.get("open_questions_for_review"), sep=" | ")
            red_flags_txt = _join_list(parsed.get("red_flags"), sep=" | ")

            out_rows.append(
                {
                    "hubspot_contact_id": contact_id,
                    "email": email,
                    "summary_one_liner": summary_one_liner,
                    "summary_short": summary_short,
                    "time_range_from": time_from,
                    "time_range_to": time_to,
                    "data_recency_note": recency_note,
                    "successes": successes_txt,
                    "challenges": challenges_txt,
                    "churn_reasons": churn_txt,
                    "relationship_score_1_to_5": rel_score,
                    "relationship_explanation": rel_expl,
                    "relationship_signals_positive": rel_pos,
                    "relationship_signals_negative": rel_neg,
                    "next_best_actions": next_actions_txt,
                    "open_questions_for_review": open_q_txt,
                    "red_flags": red_flags_txt,
                    # full JSON for your renderer step later
                    "ai_json": _safe_json_dumps(parsed),
                }
            )

    if out_rows:
        fields = [
            "hubspot_contact_id",
            "email",
            "summary_one_liner",
            "summary_short",
            "time_range_from",
            "time_range_to",
            "data_recency_note",
            "successes",
            "challenges",
            "churn_reasons",
            "relationship_score_1_to_5",
            "relationship_explanation",
            "relationship_signals_positive",
            "relationship_signals_negative",
            "next_best_actions",
            "open_questions_for_review",
            "red_flags",
            "ai_json",
        ]
        write_csv_rows(out_csv_path, out_rows, fields)

    if failed_rows:
        failed_fields = ["hubspot_contact_id", "email", "error", "raw"]
        write_csv_rows(failed_csv_path, failed_rows, failed_fields)

    return {
        "step3_ai_jsonl": jsonl_path,
        "step3_output_csv": out_csv_path if out_rows else "",
        "step3_failed_parse_csv": failed_csv_path if failed_rows else "",
    }


# -------------------------------------------------------------------
# NEW: Re-run helper for UI (no refetch; works per-contact directory)
# -------------------------------------------------------------------

def rerun_step3_from_local_context(
    contact_dir: str,
    extra_user_prompt: str = "",
) -> dict[str, Any]:
    """
    Re-run Step3 for a single contact using already stored local context.

    Expects:
      - step2_merged_context.txt (preferred)
        OR step2_hubspot_text.txt + step1_trello_text.txt (fallback)
      - meta.json (optional, for email/contact_id)

    Writes:
      - step3_raw.txt
      - step3_ai.json
      - step3_rerun_meta.json (audit)

    Returns:
      {"ok": bool, "error": str, "step3_ai_path": str}
    """
    contact_dir = os.path.abspath(contact_dir)
    if not os.path.isdir(contact_dir):
        return {"ok": False, "error": "contact_dir_not_found", "step3_ai_path": ""}

    # Load configs from .env
    app_cfg, _trello_cfg, _hs_cfg, oa_cfg = load_config()
    client = OpenAIAssistantClient(oa_cfg)

    meta_path = os.path.join(contact_dir, "meta.json")
    meta = {}
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f) or {}
        except Exception:
            meta = {}

    merged_path = os.path.join(contact_dir, "step2_merged_context.txt")
    merged_text = _read_text_file(merged_path)

    # fallback: if merged missing, combine step1 + step2 texts
    if not merged_text.strip():
        t1 = _read_text_file(os.path.join(contact_dir, "step1_trello_text.txt"))
        h2 = _read_text_file(os.path.join(contact_dir, "step2_hubspot_text.txt"))
        merged_text = _build_fallback_merged_context(t1, h2)

    if not merged_text.strip():
        return {"ok": False, "error": "missing_local_context_text", "step3_ai_path": ""}

    # Call assistant
    raw = client.summarize_with_assistant(
        merged_context_text=merged_text,
        extra_user_prompt=extra_user_prompt if extra_user_prompt.strip() else None,
    )

    # Persist raw
    raw_path = os.path.join(contact_dir, "step3_raw.txt")
    try:
        with open(raw_path, "w", encoding="utf-8") as f:
            f.write(raw or "")
    except Exception:
        pass

    parsed: dict[str, Any] | None = None
    parse_error = ""
    schema_ok = False
    schema_error = ""

    try:
        parsed_candidate = json.loads(raw)
        if isinstance(parsed_candidate, dict):
            parsed = parsed_candidate
            schema_ok, schema_error = _validate_schema_min(parsed)
        else:
            parse_error = "json_not_object"
    except Exception as e:
        parse_error = f"json_parse_error:{type(e).__name__}"

    if not parsed or parse_error or not schema_ok:
        # Persist debug meta
        debug_path = os.path.join(contact_dir, "step3_rerun_meta.json")
        _write_json_safely(
            debug_path,
            {
                "ts": int(time.time()),
                "ok": False,
                "email": (meta.get("email") or "").strip(),
                "hubspot_contact_id": str(meta.get("hubspot_contact_id") or meta.get("hubspot_contact_id", "")).strip(),
                "parse_error": parse_error,
                "schema_ok": schema_ok,
                "schema_error": schema_error,
            },
        )
        return {
            "ok": False,
            "error": parse_error or schema_error or "unknown_error",
            "step3_ai_path": "",
        }

    # Persist parsed AI JSON
    ai_path = os.path.join(contact_dir, "step3_ai.json")
    _write_json_safely(ai_path, parsed)

    # Persist audit meta
    debug_path = os.path.join(contact_dir, "step3_rerun_meta.json")
    _write_json_safely(
        debug_path,
        {
            "ts": int(time.time()),
            "ok": True,
            "email": (meta.get("email") or "").strip(),
            "hubspot_contact_id": (meta.get("hubspot_contact_id") or "").strip(),
            "schema_ok": schema_ok,
        },
    )

    return {"ok": True, "error": "", "step3_ai_path": ai_path}


# ----------------------------
# Local helper functions (private)
# ----------------------------

def _read_text_file(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def _write_json_safely(path: str, obj: Any) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _build_fallback_merged_context(trello_text: str, hubspot_text: str) -> str:
    trello_text = (trello_text or "").strip()
    hubspot_text = (hubspot_text or "").strip()
    if not trello_text and not hubspot_text:
        return ""
    parts = []
    if trello_text:
        parts.append("### Trello\n" + trello_text)
    if hubspot_text:
        parts.append("### HubSpot\n" + hubspot_text)
    return "\n\n".join(parts).strip()
