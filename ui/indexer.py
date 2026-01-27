# ui/indexer.py
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Public data model
# ----------------------------

@dataclass
class ContactIndexEntry:
    """
    One entry per (job_id, contact_id).

    This is derived from filesystem artifacts under:
      output/jobs/<job_id>/contacts/<contact_id>/
    """
    job_id: str
    contact_id: str

    email: str = ""
    trello_id: str = ""

    # Derived flags
    has_step1: bool = False
    has_step2: bool = False
    has_step3: bool = False
    has_step4: bool = False

    verified: bool = False
    pushed_to_hubspot: bool = False
    hubspot_note_id: str = ""

    # Useful for UI
    status: str = ""   # best-effort: done/error/duplicate/unknown
    step: str = ""     # best-effort: step1..step4/write/unknown

    # Timestamps (best-effort)
    updated_ts: float = 0.0

    # Paths (internal convenience)
    contact_dir: str = ""


# ----------------------------
# Indexer (filesystem -> in-memory)
# ----------------------------

class ContactIndexer:
    """
    Lightweight filesystem indexer for existing job outputs.
    No DB. No dependencies.

    - Scans output/jobs/<job_id>/contacts/<contact_id> folders
    - Builds an in-memory list of ContactIndexEntry
    - Supports searching by email/contact_id/trello_id
    - Optional caching to json file to speed up restarts
    """

    def __init__(
        self,
        jobs_base_dir: str = "output/jobs",
        cache_path: str = "output/contact_index_cache.json",
        cache_ttl_seconds: int = 30,
    ) -> None:
        self.jobs_base_dir = jobs_base_dir
        self.cache_path = cache_path
        self.cache_ttl_seconds = cache_ttl_seconds

        self._entries: List[ContactIndexEntry] = []
        self._last_built_ts: float = 0.0

        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)

    # -------- public --------

    def get_entries(self, force_rebuild: bool = False) -> List[ContactIndexEntry]:
        if force_rebuild:
            self.rebuild()
            return self._entries

        # if already built recently, return
        if self._entries and (time.time() - self._last_built_ts) < self.cache_ttl_seconds:
            return self._entries

        # try load from cache if valid
        if self._load_cache_if_fresh():
            return self._entries

        # else rebuild
        self.rebuild()
        return self._entries

    def rebuild(self) -> None:
        entries: List[ContactIndexEntry] = []
        if not os.path.isdir(self.jobs_base_dir):
            self._entries = []
            self._last_built_ts = time.time()
            self._save_cache()
            return

        for job_id in sorted(os.listdir(self.jobs_base_dir)):
            job_dir = os.path.join(self.jobs_base_dir, job_id)
            contacts_dir = os.path.join(job_dir, "contacts")
            if not os.path.isdir(contacts_dir):
                continue

            for contact_id in sorted(os.listdir(contacts_dir)):
                cdir = os.path.join(contacts_dir, contact_id)
                if not os.path.isdir(cdir):
                    continue
                entry = self._build_entry(job_id=job_id, contact_id=contact_id, contact_dir=cdir)
                entries.append(entry)

        # Sort: most recently updated first
        entries.sort(key=lambda e: e.updated_ts, reverse=True)

        self._entries = entries
        self._last_built_ts = time.time()
        self._save_cache()

    def search(
        self,
        query: str,
        limit: int = 50,
        force_rebuild: bool = False,
    ) -> List[ContactIndexEntry]:
        q = (query or "").strip().lower()
        if not q:
            return self.get_entries(force_rebuild=force_rebuild)[:limit]

        entries = self.get_entries(force_rebuild=force_rebuild)

        def match(e: ContactIndexEntry) -> bool:
            if q in (e.contact_id or "").lower():
                return True
            if q in (e.email or "").lower():
                return True
            if q in (e.trello_id or "").lower():
                return True
            if q in (e.hubspot_note_id or "").lower():
                return True
            return False

        out = [e for e in entries if match(e)]
        return out[:limit]

    def find(self, job_id: str, contact_id: str) -> Optional[ContactIndexEntry]:
        job_id = (job_id or "").strip()
        contact_id = (contact_id or "").strip()
        if not job_id or not contact_id:
            return None
        for e in self.get_entries():
            if e.job_id == job_id and e.contact_id == contact_id:
                return e
        # fallback: build directly if exists
        cdir = os.path.join(self.jobs_base_dir, job_id, "contacts", contact_id)
        if os.path.isdir(cdir):
            return self._build_entry(job_id, contact_id, cdir)
        return None

    # -------- internals --------

    def _build_entry(self, job_id: str, contact_id: str, contact_dir: str) -> ContactIndexEntry:
        entry = ContactIndexEntry(job_id=job_id, contact_id=contact_id, contact_dir=contact_dir)

        # meta.json contains email + hubspot_contact_id
        meta = self._read_json(os.path.join(contact_dir, "meta.json")) or {}
        entry.email = (meta.get("email") or "").strip()

        # Step1 match: status + trello_ids or trello_id
        step1_match = self._read_json(os.path.join(contact_dir, "step1_match.json")) or {}
        if step1_match:
            entry.has_step1 = True
            status = (step1_match.get("status") or "").strip()
            if status:
                entry.status = status if status in ("duplicate", "no_match") else entry.status

            trello_ids = step1_match.get("trello_ids")
            if isinstance(trello_ids, list) and trello_ids:
                # if single -> trello_id
                if len(trello_ids) == 1:
                    entry.trello_id = str(trello_ids[0]).strip()

        # If step1 match didn't include it, try from JOB_STORE style field written later:
        # The pipeline writes current state into JOB_STORE, but not persisted.
        # We can infer trello_id from existence of step1_trello.json by reading card.url:
        if not entry.trello_id:
            trello_bundle = self._read_json(os.path.join(contact_dir, "step1_trello.json"))
            if isinstance(trello_bundle, dict):
                entry.has_step1 = True
                card = trello_bundle.get("card") if isinstance(trello_bundle.get("card"), dict) else {}
                url = (card.get("url") or "").strip()
                # url is like https://trello.com/c/<shortlink>/...
                # extract after /c/
                if "/c/" in url:
                    try:
                        entry.trello_id = url.split("/c/")[1].split("/")[0].strip()
                    except Exception:
                        pass

        # Step2 hubspot
        if os.path.exists(os.path.join(contact_dir, "step2_hubspot.json")) or os.path.exists(os.path.join(contact_dir, "step2_merged_context.txt")):
            entry.has_step2 = True

        # Step3 ai json
        if os.path.exists(os.path.join(contact_dir, "step3_ai.json")):
            entry.has_step3 = True

        # Step4 html note
        if os.path.exists(os.path.join(contact_dir, "step4_note.html")):
            entry.has_step4 = True

        # Verified
        ver = self._read_json(os.path.join(contact_dir, "verified.json")) or {}
        if isinstance(ver, dict):
            entry.verified = bool(ver.get("verified", False))

        # HubSpot write result
        wr = self._read_json(os.path.join(contact_dir, "hubspot_write_result.json")) or {}
        if isinstance(wr, dict) and wr.get("note_id"):
            entry.pushed_to_hubspot = True
            entry.hubspot_note_id = str(wr.get("note_id")).strip()
            entry.step = "write"
            entry.status = "done"
        else:
            # If not pushed, infer step/status
            entry.step, entry.status = self._infer_step_status(entry, step1_match)

        # updated_ts: latest mtime of known artifacts
        entry.updated_ts = self._compute_updated_ts(contact_dir)

        return entry

    def _infer_step_status(self, entry: ContactIndexEntry, step1_match: dict[str, Any]) -> Tuple[str, str]:
        # Explicit duplicate / no_match
        s = (step1_match.get("status") or "").strip()
        if s == "duplicate":
            return "step1", "duplicate"
        if s == "no_match":
            return "step1", "error"

        # If step4 exists, it's "done"
        if entry.has_step4:
            return "step4", "done"
        if entry.has_step3:
            return "step3", "running"
        if entry.has_step2:
            return "step2", "running"
        if entry.has_step1:
            return "step1", "running"
        return "unknown", "unknown"

    def _compute_updated_ts(self, contact_dir: str) -> float:
        latest = 0.0
        try:
            for name in os.listdir(contact_dir):
                path = os.path.join(contact_dir, name)
                try:
                    mt = os.path.getmtime(path)
                    if mt > latest:
                        latest = mt
                except Exception:
                    continue
        except Exception:
            return 0.0
        return latest

    def _read_json(self, path: str) -> Optional[Dict[str, Any]]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else None
        except Exception:
            return None

    # -------- cache --------

    def _save_cache(self) -> None:
        try:
            payload = {
                "built_ts": self._last_built_ts,
                "jobs_base_dir": self.jobs_base_dir,
                "entries": [asdict(e) for e in self._entries],
            }
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception:
            # cache is optional; ignore failures
            pass

    def _load_cache_if_fresh(self) -> bool:
        if not os.path.exists(self.cache_path):
            return False
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            built_ts = float(payload.get("built_ts", 0.0))
            if not built_ts:
                return False

            # if cache too old, ignore
            if (time.time() - built_ts) > self.cache_ttl_seconds:
                return False

            raw_entries = payload.get("entries", [])
            if not isinstance(raw_entries, list):
                return False

            entries: List[ContactIndexEntry] = []
            for r in raw_entries:
                if not isinstance(r, dict):
                    continue
                entries.append(ContactIndexEntry(**r))

            self._entries = entries
            self._last_built_ts = built_ts
            return True
        except Exception:
            return False


# Convenience singleton (optional)
INDEXER = ContactIndexer()
