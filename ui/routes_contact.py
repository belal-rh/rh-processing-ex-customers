# ui/routes_contact.py
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict

from flask import Blueprint, render_template_string, redirect, url_for, request

from ui.templates import BASE_LAYOUT
from ui.indexer import INDEXER, ContactIndexEntry
from step3_openai_assistant import rerun_step3_from_local_context
from step4_render_hubspot_html import rerun_step4_from_local_ai
from hubspot_write import push_verified_note_to_hubspot


bp_contact = Blueprint("contact", __name__)


# ----------------------------
# Helpers
# ----------------------------

def _read_text(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return ""


def _read_json(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _layout(content: str, title: str = "") -> str:
    html = BASE_LAYOUT
    html = html.replace("{{ title or \"Ex-Kunden Analyse\" }}", title)
    html = html.replace("{{ content | safe }}", content)
    html = html.replace("{{ 'active' if nav=='upload' else '' }}", "")
    html = html.replace("{{ 'active' if nav=='search' else '' }}", "active")
    html = html.replace("{{ nav }}", "search")
    return html


# ----------------------------
# Page Template
# ----------------------------

CONTACT_PAGE = """
<div class="card">
  <div class="row">
    <div>
      <h2 style="margin:0;">Kontakt-Analyse</h2>
      <div class="muted">
        Job: <code>{{ entry.job_id }}</code> ·
        Contact ID: <code>{{ entry.contact_id }}</code>
        {% if entry.email %} · Email: <code>{{ entry.email }}</code>{% endif %}
      </div>
    </div>
    <div class="spacer"></div>

    <form method="post" action="/contact/{{ entry.job_id }}/{{ entry.contact_id }}/verify">
      {% if entry.verified %}
        <button class="btn ok" disabled>✔ Verified</button>
      {% else %}
        <button class="btn primary">Verify</button>
      {% endif %}
    </form>

    <form method="post" action="/contact/{{ entry.job_id }}/{{ entry.contact_id }}/push">
      <button class="btn warn">→ HubSpot schreiben</button>
    </form>
  </div>
</div>

<div class="grid-2">

  <!-- INPUT -->
  <div class="card">
    <h3>Input (Trello + HubSpot)</h3>

    <h4>Trello</h4>
    <pre>{{ trello_text }}</pre>

    <h4>HubSpot</h4>
    <pre>{{ hubspot_text }}</pre>

    <h4>Merged Context (Step2)</h4>
    <pre>{{ merged_context }}</pre>
  </div>

  <!-- OUTPUT -->
  <div class="card">
    <h3>Output (AI)</h3>

    <div class="row">
      <form method="post" action="/contact/{{ entry.job_id }}/{{ entry.contact_id }}/rerun-step3">
        <button class="btn">Re-run Step 3 (Extract)</button>
      </form>

      <form method="post" action="/contact/{{ entry.job_id }}/{{ entry.contact_id }}/rerun-step4">
        <button class="btn">Re-run Step 4 (HTML)</button>
      </form>
    </div>

    <h4>AI JSON (Step3)</h4>
    <pre>{{ step3_json }}</pre>

    <h4>HTML Vorschau (Step4)</h4>
    {% if step4_html %}
      <iframe srcdoc="{{ step4_html|e }}"></iframe>
    {% else %}
      <div class="muted">Kein HTML vorhanden.</div>
    {% endif %}
  </div>

</div>
"""


# ----------------------------
# Routes
# ----------------------------

@bp_contact.get("/contact/<job_id>/<contact_id>")
def contact_detail(job_id: str, contact_id: str):
    entry: ContactIndexEntry | None = INDEXER.find(job_id, contact_id)
    if not entry:
        return "Kontakt nicht gefunden", 404

    cdir = entry.contact_dir

    trello_text = _read_text(os.path.join(cdir, "step1_trello_text.txt"))
    hubspot_text = _read_text(os.path.join(cdir, "step2_hubspot_text.txt"))
    merged_context = _read_text(os.path.join(cdir, "step2_merged_context.txt"))
    step3_json = json.dumps(_read_json(os.path.join(cdir, "step3_ai.json")), ensure_ascii=False, indent=2)
    step4_html = _read_text(os.path.join(cdir, "step4_note.html"))

    page = _layout(CONTACT_PAGE, title="Kontakt · Analyse")

    return render_template_string(
        page,
        entry=entry,
        trello_text=trello_text,
        hubspot_text=hubspot_text,
        merged_context=merged_context,
        step3_json=step3_json,
        step4_html=step4_html,
    )


@bp_contact.post("/contact/<job_id>/<contact_id>/rerun-step3")
def rerun_step3(job_id: str, contact_id: str):
    entry = INDEXER.find(job_id, contact_id)
    if not entry:
        return "Kontakt nicht gefunden", 404

    rerun_step3_from_local_context(entry.contact_dir)
    INDEXER.rebuild()
    return redirect(url_for("contact.contact_detail", job_id=job_id, contact_id=contact_id))


@bp_contact.post("/contact/<job_id>/<contact_id>/rerun-step4")
def rerun_step4(job_id: str, contact_id: str):
    entry = INDEXER.find(job_id, contact_id)
    if not entry:
        return "Kontakt nicht gefunden", 404

    rerun_step4_from_local_ai(entry.contact_dir)
    INDEXER.rebuild()
    return redirect(url_for("contact.contact_detail", job_id=job_id, contact_id=contact_id))


@bp_contact.post("/contact/<job_id>/<contact_id>/verify")
def verify_contact(job_id: str, contact_id: str):
    entry = INDEXER.find(job_id, contact_id)
    if not entry:
        return "Kontakt nicht gefunden", 404

    path = os.path.join(entry.contact_dir, "verified.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"verified": True, "ts": int(time.time())}, f)

    INDEXER.rebuild()
    return redirect(url_for("contact.contact_detail", job_id=job_id, contact_id=contact_id))


@bp_contact.post("/contact/<job_id>/<contact_id>/push")
def push_to_hubspot(job_id: str, contact_id: str):
    entry = INDEXER.find(job_id, contact_id)
    if not entry:
        return "Kontakt nicht gefunden", 404

    push_verified_note_to_hubspot(entry.contact_dir)
    INDEXER.rebuild()
    return redirect(url_for("contact.contact_detail", job_id=job_id, contact_id=contact_id))
