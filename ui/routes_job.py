# ui/routes_job.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from flask import Blueprint, Response, abort, redirect, render_template_string, request, send_from_directory, url_for

from ui.templates import BASE_LAYOUT
from jobs import JOB_STORE
from pipeline_job_runner import set_verified, push_verified_to_hubspot
from config import load_config


bp_job = Blueprint("job", __name__)


# ----------------------------
# Templates
# ----------------------------

DASHBOARD_HTML = """
<div class="card">
  <div class="row">
    <div>
      <h2 style="margin:0;">Job Dashboard</h2>
      <div class="muted">Job: <code id="jobId">{{ job_id }}</code> · Status: <code id="jobStatus">{{ status }}</code></div>
    </div>
    <div class="spacer"></div>
    <a class="btn" href="/review/{{ job_id }}">Review & Verify</a>
  </div>

  <div class="row" style="margin-top:12px;">
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Total</div><div style="font-size:20px;font-weight:700;" id="k_total">{{ progress.total }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Done</div><div style="font-size:20px;font-weight:700;" id="k_done">{{ progress.done }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Errors</div><div style="font-size:20px;font-weight:700;" id="k_errors">{{ progress.errors }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Duplicates</div><div style="font-size:20px;font-weight:700;" id="k_dups">{{ progress.duplicates }}</div>
    </div>
  </div>
</div>

<div class="card">
  <div class="row">
    <form method="post" action="/push-to-hubspot/{{ job_id }}">
      <button class="btn warn" type="submit">Verified → HubSpot schreiben (Kontakt + Deals)</button>
    </form>
    <div class="muted">Nur Kontakte mit “Verified” werden geschrieben.</div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Email</th>
        <th>Contact ID</th>
        <th>Trello</th>
        <th>Status</th>
        <th>Step</th>
        <th>Last message</th>
        <th>Verified</th>
        <th>Details</th>
      </tr>
    </thead>
    <tbody id="tbody">
      {% for cid, c in contacts.items() %}
        <tr id="row-{{ cid }}">
          <td>{% if c.email %}<code>{{ c.email }}</code>{% else %}<span class="muted">—</span>{% endif %}</td>
          <td><code>{{ cid }}</code></td>
          <td>{% if c.trello_id %}<code>{{ c.trello_id }}</code>{% else %}<span class="muted">—</span>{% endif %}</td>

          <td>
            {% if c.status=='done' %}<span class="status-ok">done</span>
            {% elif c.status=='duplicate' %}<span class="status-warn">duplicate</span>
            {% elif c.status=='error' %}<span class="status-error">error</span>
            {% else %}<span class="muted">{{ c.status }}</span>{% endif %}
          </td>

          <td><code>{{ c.step }}</code></td>
          <td>{{ c.last_message }}</td>
          <td>{{ "✅" if c.verified else "—" }}</td>
          <td><a class="btn" href="/contact/{{ job_id }}/{{ cid }}">Öffnen</a></td>
        </tr>
      {% endfor %}
    </tbody>
  </table>
</div>

<script>
  const jobId = "{{ job_id }}";
  const es = new EventSource(`/events/${jobId}`);

  function upsertRow(c) {
    const cid = c.hubspot_contact_id;
    const rowId = `row-${cid}`;
    let tr = document.getElementById(rowId);

    const statusClass = (c.status === "done") ? "status-ok" : (c.status === "error") ? "status-error" : (c.status === "duplicate") ? "status-warn" : "muted";
    const verifiedTxt = c.verified ? "✅" : "—";

    const emailCell = c.email ? `<code>${c.email}</code>` : `<span class="muted">—</span>`;
    const trelloCell = c.trello_id ? `<code>${c.trello_id}</code>` : `<span class="muted">—</span>`;

    const html = `
      <td>${emailCell}</td>
      <td><code>${cid}</code></td>
      <td>${trelloCell}</td>
      <td><span class="${statusClass}">${c.status || ""}</span></td>
      <td><code>${c.step || ""}</code></td>
      <td>${c.last_message || ""}</td>
      <td>${verifiedTxt}</td>
      <td><a class="btn" href="/contact/${jobId}/${cid}">Öffnen</a></td>
    `;

    if (!tr) {
      tr = document.createElement("tr");
      tr.id = rowId;
      tr.innerHTML = html;
      document.getElementById("tbody").appendChild(tr);
    } else {
      tr.innerHTML = html;
    }
  }

  es.onmessage = (e) => {
    try {
      const ev = JSON.parse(e.data);

      if (ev.type === "job_status") {
        document.getElementById("jobStatus").textContent = ev.status;
      }
      if (ev.type === "progress") {
        const p = ev.progress || {};
        document.getElementById("k_total").textContent = p.total ?? "";
        document.getElementById("k_done").textContent = p.done ?? "";
        document.getElementById("k_errors").textContent = p.errors ?? "";
        document.getElementById("k_dups").textContent = p.duplicates ?? "";
      }
      if (ev.type === "contact_update") {
        upsertRow(ev.contact);
      }
    } catch (err) {}
  };
</script>
"""

REVIEW_HTML = """
<div class="card">
  <div class="row">
    <div>
      <h2 style="margin:0;">Review & Verify</h2>
      <div class="muted">Job: <code>{{ job_id }}</code> · zeigt nur <code>status=done</code> mit vorhandenem <code>step4_note.html</code></div>
    </div>
    <div class="spacer"></div>
    <a class="btn" href="/dashboard/{{ job_id }}">← Dashboard</a>
    <form method="post" action="/push-to-hubspot/{{ job_id }}">
      <button class="btn warn" type="submit">Verified → HubSpot schreiben</button>
    </form>
  </div>
</div>

<div class="card">
  <table>
    <thead>
      <tr>
        <th>Email</th>
        <th>Contact ID</th>
        <th>Verified</th>
        <th>Details</th>
      </tr>
    </thead>
    <tbody>
      {% for r in rows %}
        <tr>
          <td>{% if r.email %}<code>{{ r.email }}</code>{% else %}<span class="muted">—</span>{% endif %}</td>
          <td><code>{{ r.contact_id }}</code></td>
          <td>
            <form method="post" action="/verify/{{ job_id }}/{{ r.contact_id }}">
              <input type="hidden" name="verified" value="{{ '0' if r.verified else '1' }}">
              {% if r.verified %}
                <button class="btn ok" type="submit">✅</button>
              {% else %}
                <button class="btn primary" type="submit">Verify</button>
              {% endif %}
            </form>
          </td>
          <td><a class="btn" href="/contact/{{ job_id }}/{{ r.contact_id }}">Öffnen</a></td>
        </tr>
      {% endfor %}
      {% if rows|length == 0 %}
        <tr><td colspan="4" class="muted">Keine Einträge.</td></tr>
      {% endif %}
    </tbody>
  </table>
</div>
"""

PUSH_RESULT_HTML = """
<div class="card">
  <div class="row">
    <div>
      <h2 style="margin:0;">HubSpot Push Ergebnis</h2>
      <div class="muted">Job: <code>{{ job_id }}</code></div>
    </div>
    <div class="spacer"></div>
    <a class="btn" href="/dashboard/{{ job_id }}">← Dashboard</a>
  </div>
</div>

<div class="card">
  <div class="row">
    <div><b>Created:</b> <code>{{ created }}</code></div>
    <div><b>Errors:</b> <code>{{ errors }}</code></div>
  </div>
  <pre>{{ details }}</pre>
</div>
"""


def _layout(content: str, title: str) -> str:
    html = BASE_LAYOUT
    html = html.replace("{{ title or \"Ex-Kunden Analyse\" }}", title)
    html = html.replace("{{ content | safe }}", content)
    html = html.replace("{{ 'active' if nav=='upload' else '' }}", "")
    html = html.replace("{{ 'active' if nav=='search' else '' }}", "active")
    html = html.replace("{{ nav }}", "search")
    return html


# ----------------------------
# Routes
# ----------------------------

@bp_job.get("/dashboard/<job_id>")
def dashboard(job_id: str):
    snap = JOB_STORE.get_snapshot(job_id)
    page = _layout(DASHBOARD_HTML, title="Dashboard · Ex-Kunden Analyse")
    return render_template_string(
        page,
        job_id=job_id,
        status=snap.get("status", "unknown"),
        progress=snap.get("progress", {}),
        contacts=snap.get("contacts", {}),
    )


@bp_job.get("/events/<job_id>")
def events(job_id: str):
    def gen():
        for ev in JOB_STORE.stream_events(job_id):
            yield f"data: {json.dumps(ev, ensure_ascii=False)}\n\n"
    return Response(gen(), mimetype="text/event-stream")


@bp_job.get("/review/<job_id>")
def review(job_id: str):
    snap = JOB_STORE.get_snapshot(job_id)
    job_dir = snap.get("job_dir", "")
    rows: List[Dict[str, Any]] = []

    contacts: Dict[str, Any] = snap.get("contacts", {})
    for cid, c in contacts.items():
        if c.get("status") != "done":
            continue
        cdir = os.path.join(job_dir, "contacts", str(cid))
        html_path = os.path.join(cdir, "step4_note.html")
        if not os.path.exists(html_path):
            continue
        rows.append({"contact_id": cid, "email": c.get("email", ""), "verified": bool(c.get("verified"))})

    page = _layout(REVIEW_HTML, title="Review · Ex-Kunden Analyse")
    return render_template_string(page, job_id=job_id, rows=rows)


@bp_job.post("/verify/<job_id>/<contact_id>")
def verify(job_id: str, contact_id: str):
    verified = (request.form.get("verified") or "0").strip() == "1"
    set_verified(job_id, contact_id, verified)
    return redirect(request.referrer or url_for("job.dashboard", job_id=job_id))


@bp_job.post("/push-to-hubspot/<job_id>")
def push_to_hubspot(job_id: str):
    _app_cfg, _trello_cfg, hs_cfg, _oa_cfg = load_config()
    res = push_verified_to_hubspot(job_id, hs_cfg, also_associate_deals=True)

    page = _layout(PUSH_RESULT_HTML, title="HubSpot Push · Ex-Kunden Analyse")
    return render_template_string(
        page,
        job_id=job_id,
        created=res.get("created", 0),
        errors=res.get("errors", 0),
        details=json.dumps(res.get("details", []), ensure_ascii=False, indent=2),
    )


@bp_job.get("/contact-file/<job_id>/<contact_id>/<path:filename>")
def contact_file(job_id: str, contact_id: str, filename: str):
    """
    Optional helper endpoint to serve raw artifacts.
    Useful if you want to open step4_note.html directly or download json/txt.
    """
    snap = JOB_STORE.get_snapshot(job_id)
    job_dir = snap.get("job_dir", "")
    cdir = os.path.join(job_dir, "contacts", str(contact_id))
    full = os.path.join(cdir, filename)
    if not os.path.exists(full):
        abort(404)
    return send_from_directory(cdir, filename)
