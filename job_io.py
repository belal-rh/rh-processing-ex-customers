# job_io.py
from __future__ import annotations
import json
import os
from typing import Any

def contact_dir(job_dir: str, contact_id: str) -> str:
    d = os.path.join(job_dir, "contacts", str(contact_id))
    os.makedirs(d, exist_ok=True)
    return d

def write_json(path: str, data: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def write_text(path: str, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text or "")
