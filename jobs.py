# jobs.py
from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass
class ContactState:
    email: str
    hubspot_contact_id: str
    trello_id: str = ""
    status: str = "queued"   # queued|running|duplicate|done|error
    step: str = "—"          # step1|step2|step3|step4|write|—
    last_message: str = ""
    error: str = ""
    verified: bool = False   # for Step4 review

class JobStore:
    def __init__(self, base_dir: str = "output/jobs"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()

    def create_job(self, meta: dict[str, Any]) -> str:
        job_id = uuid.uuid4().hex[:10]
        job_dir = os.path.join(self.base_dir, job_id)
        os.makedirs(job_dir, exist_ok=True)
        os.makedirs(os.path.join(job_dir, "contacts"), exist_ok=True)

        with self.lock:
            self.jobs[job_id] = {
                "id": job_id,
                "created_at": time.time(),
                "status": "created",  # created|running|done|error
                "meta": meta,
                "contacts": {},       # contact_id -> ContactState (as dict)
                "events": queue.Queue(),
                "job_dir": job_dir,
                "progress": {"total": 0, "done": 0, "errors": 0, "duplicates": 0},
            }
        return job_id

    def job_dir(self, job_id: str) -> str:
        return self.jobs[job_id]["job_dir"]

    def emit(self, job_id: str, event: dict[str, Any]) -> None:
        # event for SSE
        self.jobs[job_id]["events"].put(event)

    def set_status(self, job_id: str, status: str) -> None:
        with self.lock:
            self.jobs[job_id]["status"] = status
        self.emit(job_id, {"type": "job_status", "status": status})

    def upsert_contact(self, job_id: str, contact_id: str, state: ContactState) -> None:
        with self.lock:
            self.jobs[job_id]["contacts"][contact_id] = state.__dict__
        self.emit(job_id, {"type": "contact_update", "contact": state.__dict__})

    def update_contact(self, job_id: str, contact_id: str, **updates) -> None:
        with self.lock:
            c = self.jobs[job_id]["contacts"].get(contact_id, {})
            c.update(updates)
            self.jobs[job_id]["contacts"][contact_id] = c
        self.emit(job_id, {"type": "contact_update", "contact": self.jobs[job_id]["contacts"][contact_id]})

    def set_progress(self, job_id: str, **updates) -> None:
        with self.lock:
            self.jobs[job_id]["progress"].update(updates)
        self.emit(job_id, {"type": "progress", "progress": self.jobs[job_id]["progress"]})

    def get_snapshot(self, job_id: str) -> dict[str, Any]:
        with self.lock:
            job = self.jobs[job_id]
            return {
                "id": job["id"],
                "status": job["status"],
                "meta": job["meta"],
                "progress": dict(job["progress"]),
                "contacts": dict(job["contacts"]),
                "job_dir": job["job_dir"],
            }

    def stream_events(self, job_id: str):
        q: queue.Queue = self.jobs[job_id]["events"]
        while True:
            try:
                ev = q.get(timeout=25)
                yield ev
            except queue.Empty:
                # keep-alive ping for SSE
                yield {"type": "ping", "ts": time.time()}

JOB_STORE = JobStore()
