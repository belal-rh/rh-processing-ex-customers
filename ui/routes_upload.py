# ui/routes_upload.py
from __future__ import annotations

import json
import os
import tempfile
import threading
from typing import Any

from flask import Blueprint, Response, request, redirect, url_for, render_template_string

from config import load_config
from jobs import JOB_STORE
from pipeline_job_runner import run_pipeline_job

from utils_csv import detect_delimiter, read_csv_rows, normalize_email
from ui.templates import BASE_LAYOUT


bp_upload = Blueprint("upload", __name__)

# one temp dir per process (simple + works)
_TMP_DIR = tempfile.mkdtemp(prefix="csv_upload_")


# ----------------------------
# Templates (use BASE_LAYOUT)
# ----------------------------

UPLOAD_CONTENT = """
<div class="card">
  <h2 style="margin:0;">CSV Upload</h2>
  <div class="muted">CSV #1: HubSpot Contact-ID + E-Mail · CSV #2: E-Mail + Trello-ID</div>
</div>

{% if error %}
  <div class="card" style="border-color:#c00;">
    <b style="color:#c00;">{{ error }}</b>
  </div>
{% endif %}

<div class="card">
  <form action="/upload" method="post" enctype="multipart/form-data">
    <div class="grid-2">
      <div class="card" style="margin:0;">
        <h3>CSV #1 (HubSpot)</h3>
        <div class="muted">HubSpot Kontakt-ID + E-Mail</div>
        <div style="margin-top:10px;">
          <input type="file" name="csv1" required>
        </div>
      </div>

      <div class="card" style="margin:0;">
        <h3>CSV #2 (Trello)</h3>
        <div class="muted">E-Mail + Trello-ID</div>
        <div style="margin-top:10px;">
          <input type="file" name="csv2" required>
        </div>
      </div>
    </div>

    <div style="margin-top:12px;">
      <button class="btn primary" type="submit">Weiter: Spalten auswählen</button>
    </div>
  </form>
</div>
"""

MAPPING_CONTENT = """
<div class="card">
  <h2 style="margin:0;">Spalten auswählen</h2>
  <div class="muted">Wähle die passenden Spalten in beiden CSV-Dateien.</div>
</div>

<div class="card">
  <form action="/preview" method="post">
    <input type="hidden" name="csv1_path" value="{{ csv1_path }}">
    <input type="hidden" name="csv2_path" value="{{ csv2_path }}">
    <input type="hidden" name="delim1" value="{{ delim1 }}">
    <input type="hidden" name="delim2" value="{{ delim2 }}">

    <div class="grid-2">
      <div class="card" style="margin:0;">
        <h3>CSV #1</h3>
        <div class="muted">E-Mail + HubSpot Contact-ID</div>

        <div style="margin-top:10px;">
          <label><b>E-Mail Spalte</b></label><br>
          <select name="csv1_email_col" required style="width:100%; padding:10px; border-radius:10px; border:1px solid #eee;">
            {% for c in csv1_cols %}<option value="{{c}}">{{c}}</option>{% endfor %}
          </select>
        </div>

        <div style="margin-top:10px;">
          <label><b>HubSpot Contact-ID Spalte</b></label><br>
          <select name="csv1_hubspot_id_col" required style="width:100%; padding:10px; border-radius:10px; border:1px solid #eee;">
            {% for c in csv1_cols %}<option value="{{c}}">{{c}}</option>{% endfor %}
          </select>
        </div>
      </div>

      <div class="card" style="margin:0;">
        <h3>CSV #2</h3>
        <div class="muted">E-Mail + Trello-ID</div>

        <div style="margin-top:10px;">
          <label><b>E-Mail Spalte</b></label><br>
          <select name="csv2_email_col" required style="width:100%; padding:10px; border-radius:10px; border:1px solid #eee;">
            {% for c in csv2_cols %}<option value="{{c}}">{{c}}</option>{% endfor %}
          </select>
        </div>

        <div style="margin-top:10px;">
          <label><b>Trello-ID Spalte</b></label><br>
          <select name="csv2_trello_id_col" required style="width:100%; padding:10px; border-radius:10px; border:1px solid #eee;">
            {% for c in csv2_cols %}<option value="{{c}}">{{c}}</option>{% endfor %}
          </select>
        </div>
      </div>
    </div>

    <div style="margin-top:12px;">
      <button class="btn primary" type="submit">Weiter: Matching-Übersicht</button>
    </div>
  </form>
</div>
"""

PREVIEW_CONTENT = """
<div class="card">
  <h2 style="margin:0;">Matching-Übersicht</h2>
  <div class="muted">Kontakte mit mehreren Trello-IDs werden übersprungen, bis Du sie bereinigt hast.</div>
</div>

<div class="card">
  <div class="row">
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Kontakte (CSV#1)</div><div style="font-size:20px;font-weight:700;">{{ kpi_total }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Kein Trello-Match</div><div style="font-size:20px;font-weight:700;">{{ kpi_none }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px;">
      <div class="muted">Genau 1 Trello-ID</div><div style="font-size:20px;font-weight:700;">{{ kpi_single }}</div>
    </div>
    <div class="card" style="margin:0; padding:10px 12px; min-width:180px; border-color:#c60;">
      <div class="muted">Mehrere Trello-IDs</div><div style="font-size:20px;font-weight:700;">{{ kpi_multi }}</div>
    </div>
  </div>
</div>

<div class="grid-2">
  <div class="card" style="border-color:#c60;">
    <h3>Doppelte Trello-IDs (Top {{ preview_limit }})</h3>
    <div class="muted">Diese Kontakte werden nicht automatisch verarbeitet.</div>
    <div style="max-height:420px; overflow:auto; border:1px solid #eee; border-radius:12px; margin-top:10px;">
      <table>
        <thead><tr><th>E-Mail</th><th>Trello IDs</th><th>Links</th></tr></thead>
        <tbody>
          {% if duplicates|length == 0 %}
            <tr><td colspan="3" class="muted">Keine doppelten Trello-IDs ✅</td></tr>
          {% else %}
            {% for row in duplicates %}
              <tr>
                <td><code>{{ row.email }}</code></td>
                <td>{% for tid in row.trello_ids %}<div><code>{{ tid }}</code></div>{% endfor %}</td>
                <td>{% for link in row.links %}<div><a href="{{ link }}" target="_blank">{{ link }}</a></div>{% endfor %}</td>
              </tr>
            {% endfor %}
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <h3>Verarbeitbare Matches (Top {{ preview_limit }})</h3>
    <div class="muted">Diese Kontakte können direkt verarbeitet werden.</div>
    <div style="max-height:420px; overflow:auto; border:1px solid #eee; border-radius:12px; margin-top:10px;">
      <table>
        <thead><tr><th>E-Mail</th><th>HubSpot Contact ID</th><th>Trello-ID</th><th>Link</th></tr></thead>
        <tbody>
          {% if singles|length == 0 %}
            <tr><td colspan="4" class="muted">Keine verarbeitbaren Matches.</td></tr>
          {% else %}
            {% for row in singles %}
              <tr>
                <td><code>{{ row.email }}</code></td>
                <td><code>{{ row.hubspot_contact_id }}</code></td>
                <td><code>{{ row.trello_id }}</code></td>
                <td><a href="{{ row.link }}" target="_blank">{{ row.link }}</a></td>
              </tr>
            {% endfor %}
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
</div>

<div class="card">
  <h3>Start</h3>
  <form action="/start-job" method="post">
    <input type="hidden" name="csv1_path" value="{{ csv1_path }}">
    <input type="hidden" name="csv2_path" value="{{ csv2_path }}">
    <input type="hidden" name="delim1" value="{{ delim1 }}">
    <input type="hidden" name="delim2" value="{{ delim2 }}">
    <input type="hidden" name="csv1_email_col" value="{{ csv1_email_col }}">
    <input type="hidden" name="csv1_hubspot_id_col" value="{{ csv1_hubspot_id_col }}">
    <input type="hidden" name="csv2_email_col" value="{{ csv2_email_col }}">
    <input type="hidden" name="csv2_trello_id_col" value="{{ csv2_trello_id_col }}">
    <button class="btn primary" type="submit">Start Job</button>
  </form>
</div>
"""

def _page(content: str, title: str, nav: str) -> str:
    html = BASE_LAYOUT
    html = html.replace("{{ title or \"Ex-Kunden Analyse\" }}", title)
    html = html.replace("{{ content | safe }}", content)
    html = html.replace("{{ 'active' if nav=='upload' else '' }}", "active" if nav == "upload" else "")
    html = html.replace("{{ 'active' if nav=='search' else '' }}", "active" if nav == "search" else "")
    html = html.replace("{{ nav }}", nav)
    return html


# ----------------------------
# Matching overview
# ----------------------------

def _compute_match_overview(
    csv1_path: str,
    csv2_path: str,
    delim1: str,
    delim2: str,
    csv1_email_col: str,
    csv1_hubspot_id_col: str,
    csv2_email_col: str,
    csv2_trello_id_col: str,
    preview_limit: int = 100,
) -> dict[str, Any]:
    csv1 = read_csv_rows(csv1_path, delimiter=delim1)
    csv2 = read_csv_rows(csv2_path, delimiter=delim2)

    email_to_trello_ids: dict[str, list[str]] = {}
    for r in csv2:
        em = normalize_email(r.get(csv2_email_col, ""))
        tid = (r.get(csv2_trello_id_col, "") or "").strip()
        if not em or not tid:
            continue
        email_to_trello_ids.setdefault(em, []).append(tid)

    total = 0
    none = 0
    single = 0
    multi = 0

    duplicates: list[dict[str, Any]] = []
    singles: list[dict[str, Any]] = []

    for r in csv1:
        em = normalize_email(r.get(csv1_email_col, ""))
        hs_id = (r.get(csv1_hubspot_id_col, "") or "").strip()
        if not em or not hs_id:
            continue

        total += 1
        trello_ids = email_to_trello_ids.get(em, [])

        seen = set()
        uniq = []
        for tid in trello_ids:
            if tid not in seen:
                seen.add(tid)
                uniq.append(tid)

        if len(uniq) == 0:
            none += 1
            continue

        if len(uniq) == 1:
            single += 1
            if len(singles) < preview_limit:
                tid = uniq[0]
                singles.append(
                    {
                        "email": em,
                        "hubspot_contact_id": hs_id,
                        "trello_id": tid,
                        "link": f"https://trello.com/c/{tid}",
                    }
                )
            continue

        multi += 1
        if len(duplicates) < preview_limit:
            duplicates.append(
                {
                    "email": em,
                    "trello_ids": uniq,
                    "links": [f"https://trello.com/c/{tid}" for tid in uniq],
                }
            )

    return {
        "kpi_total": total,
        "kpi_none": none,
        "kpi_single": single,
        "kpi_multi": multi,
        "duplicates": duplicates,
        "singles": singles,
        "preview_limit": preview_limit,
    }


# ----------------------------
# Routes
# ----------------------------

@bp_upload.get("/")
def index():
    page = _page(UPLOAD_CONTENT, title="Upload · Ex-Kunden Analyse", nav="upload")
    return render_template_string(page, error="")


@bp_upload.post("/upload")
def upload():
    if "csv1" not in request.files or "csv2" not in request.files:
        page = _page(UPLOAD_CONTENT, title="Upload · Ex-Kunden Analyse", nav="upload")
        return render_template_string(page, error="Bitte beide Dateien hochladen.")

    f1 = request.files["csv1"]
    f2 = request.files["csv2"]

    csv1_path = os.path.join(_TMP_DIR, "csv1.csv")
    csv2_path = os.path.join(_TMP_DIR, "csv2.csv")
    f1.save(csv1_path)
    f2.save(csv2_path)

    delim1 = detect_delimiter(csv1_path)
    delim2 = detect_delimiter(csv2_path)

    rows1 = read_csv_rows(csv1_path, delimiter=delim1)
    rows2 = read_csv_rows(csv2_path, delimiter=delim2)
    if not rows1 or not rows2:
        page = _page(UPLOAD_CONTENT, title="Upload · Ex-Kunden Analyse", nav="upload")
        return render_template_string(page, error="Eine der CSVs ist leer oder nicht lesbar.")

    csv1_cols = list(rows1[0].keys())
    csv2_cols = list(rows2[0].keys())

    page = _page(MAPPING_CONTENT, title="Mapping · Ex-Kunden Analyse", nav="upload")
    return render_template_string(
        page,
        csv1_path=csv1_path,
        csv2_path=csv2_path,
        delim1=delim1,
        delim2=delim2,
        csv1_cols=csv1_cols,
        csv2_cols=csv2_cols,
    )


@bp_upload.post("/preview")
def preview():
    csv1_path = request.form["csv1_path"]
    csv2_path = request.form["csv2_path"]
    delim1 = request.form["delim1"]
    delim2 = request.form["delim2"]

    csv1_email_col = request.form["csv1_email_col"]
    csv1_hubspot_id_col = request.form["csv1_hubspot_id_col"]
    csv2_email_col = request.form["csv2_email_col"]
    csv2_trello_id_col = request.form["csv2_trello_id_col"]

    overview = _compute_match_overview(
        csv1_path,
        csv2_path,
        delim1,
        delim2,
        csv1_email_col,
        csv1_hubspot_id_col,
        csv2_email_col,
        csv2_trello_id_col,
        preview_limit=100,
    )

    page = _page(PREVIEW_CONTENT, title="Preview · Ex-Kunden Analyse", nav="upload")
    return render_template_string(
        page,
        csv1_path=csv1_path,
        csv2_path=csv2_path,
        delim1=delim1,
        delim2=delim2,
        csv1_email_col=csv1_email_col,
        csv1_hubspot_id_col=csv1_hubspot_id_col,
        csv2_email_col=csv2_email_col,
        csv2_trello_id_col=csv2_trello_id_col,
        **overview,
    )


@bp_upload.post("/start-job")
def start_job():
    app_cfg, trello_cfg, hs_cfg, oa_cfg = load_config()

    csv1_path = request.form["csv1_path"]
    csv2_path = request.form["csv2_path"]
    delim1 = request.form["delim1"]
    delim2 = request.form["delim2"]
    mapping = {
        "csv1_email_col": request.form["csv1_email_col"],
        "csv1_hubspot_id_col": request.form["csv1_hubspot_id_col"],
        "csv2_email_col": request.form["csv2_email_col"],
        "csv2_trello_id_col": request.form["csv2_trello_id_col"],
    }

    # keep same behavior as old web_ui.py
    render_model = os.getenv("OPENAI_RENDER_MODEL", "gpt-4o-mini").strip()
    extra_user_prompt_step3 = (
        "Analysiere die folgenden Trello- und HubSpot-Notizen und liefere ausschließlich JSON im geforderten Schema."
    )

    job_id = JOB_STORE.create_job(
        meta={
            "csv1_path": csv1_path,
            "csv2_path": csv2_path,
            "delim1": delim1,
            "delim2": delim2,
            "mapping": mapping,
            "render_model": render_model,
        }
    )

    t = threading.Thread(
        target=run_pipeline_job,
        daemon=True,
        args=(
            job_id,
            app_cfg,
            trello_cfg,
            hs_cfg,
            oa_cfg,
            csv1_path,
            csv2_path,
            delim1,
            delim2,
            mapping,
            extra_user_prompt_step3,
            render_model,
        ),
    )
    t.start()

    return redirect(url_for("job.dashboard", job_id=job_id))
