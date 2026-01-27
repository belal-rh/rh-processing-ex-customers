# utils_csv.py
from __future__ import annotations
import csv
from typing import Any

def normalize_email(email: str) -> str:
    if email is None:
        return ""
    return email.strip().lower()

def read_csv_rows(path: str, delimiter: str = ",") -> list[dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        return [dict(r) for r in reader]

def write_csv_rows(path: str, rows: list[dict[str, Any]], fieldnames: list[str], delimiter: str = ",") -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

def detect_delimiter(sample_path: str) -> str:
    """
    Einfacher Delimiter-Guess: ',' vs ';'
    """
    with open(sample_path, "r", encoding="utf-8-sig", newline="") as f:
        head = f.read(4096)
    return ";" if head.count(";") > head.count(",") else ","
