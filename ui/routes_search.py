# ui/routes_search.py
from __future__ import annotations

from typing import Any, List

from flask import Blueprint, request, render_template_string, redirect, url_for

from ui.templates import BASE_LAYOUT
from ui.indexer import INDEXER, ContactIndexEntry


bp_search = Blueprint("search", __name__)


SEARCH_PAGE = """
<div class="card">
  <div class="row">
    <div style="flex:1;">
      <h2 style="margin:0;">Suche (über alle Jobs)</h2>
      <div class="muted">Suche nach E-Mail, HubSpot Contact ID, Trello-ID oder HubSpot Note-ID.</div>
    </div>
    <form method="get" action="/search" class="search-box" style="width:420px;">
      <input type="text" name="q" placeholder="z.B. max@firma.de oder 123456 oder trelloShortId" value="{{ q|e }}">
      <div class="row" style="margin-top:10px;">
        <button class="btn primary" type="submit">Suchen</button>
        <a class="btn" href="/search">Reset</a>
        <button class="btn" type="submit" name="rebuild" value="1" title="Index neu bauen">Index neu bauen</button>
      </div>
    </form>
  </div>
</div>

<div class="card">
  <div class="row">
    <div class="muted">
      Treffer: <b>{{ results|length }}</b>
      {% if q %}· Query: <code>{{ q }}</code>{% endif %}
    </div>
    <div class="spacer"></div>
    <div class="muted">Sortierung: zuletzt aktualisiert zuerst</div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Job</th>
        <th>Contact ID</th>
        <th>Email</th>
        <th>Trello</th>
        <th>Status</th>
        <th>Step</th>
        <th>Verified</th>
        <th>HubSpot Note</th>
        <th>Aktion</th>
      </tr>
    </thead>
    <tbody>
      {% if results|length == 0 %}
        <tr><td colspan="9" class="muted">Keine Treffer.</td></tr>
      {% else %}
        {% for r in results %}
          <tr>
            <td><code>{{ r.job_id }}</code></td>
            <td><code>{{ r.contact_id }}</code></td>
            <td>{% if r.email %}<code>{{ r.email }}</code>{% else %}<span class="muted">—</span>{% endif %}</td>
            <td>{% if r.trello_id %}<code>{{ r.trello_id }}</code>{% else %}<span class="muted">—</span>{% endif %}</td>

            <td>
              {% if r.status == "done" %}
                <span class="status-ok">done</span>
              {% elif r.status == "duplicate" %}
                <span class="status-warn">duplicate</span>
              {% elif r.status == "error" %}
                <span class="status-error">error</span>
              {% else %}
                <span class="muted">{{ r.status or "unknown" }}</span>
              {% endif %}
            </td>

            <td><code>{{ r.step or "unknown" }}</code></td>

            <td>
              {% if r.verified %}
                <span class="status-ok">✅</span>
              {% else %}
                <span class="muted">—</span>
              {% endif %}
            </td>

            <td>
              {% if r.pushed_to_hubspot %}
                <span class="status-ok">✅</span>
                {% if r.hubspot_note_id %}
                  <div class="muted">note_id: <code>{{ r.hubspot_note_id }}</code></div>
                {% endif %}
              {% else %}
                <span class="muted">—</span>
              {% endif %}
            </td>

            <td>
              <a class="btn" href="/contact/{{ r.job_id }}/{{ r.contact_id }}">Öffnen</a>
            </td>
          </tr>
        {% endfor %}
      {% endif %}
    </tbody>
  </table>
</div>
"""


def _layout(content: str, title: str, nav: str) -> str:
    """
    Uses BASE_LAYOUT (keeps dependencies minimal, no extra template engine needed).
    """
    html = BASE_LAYOUT
    html = html.replace("{{ title or \"Ex-Kunden Analyse\" }}", title)
    html = html.replace("{{ content | safe }}", content)
    html = html.replace("{{ 'active' if nav=='upload' else '' }}", "active" if nav == "upload" else "")
    html = html.replace("{{ 'active' if nav=='search' else '' }}", "active" if nav == "search" else "")
    html = html.replace("{{ nav }}", nav)
    return html


@bp_search.get("/search")
def search_page():
    q = (request.args.get("q") or "").strip()
    rebuild = (request.args.get("rebuild") or "").strip() == "1"

    # Keep it fast: only rebuild if user explicitly asks
    results: List[ContactIndexEntry] = INDEXER.search(q, limit=200, force_rebuild=rebuild)

    page = _layout(SEARCH_PAGE, title="Suche · Ex-Kunden Analyse", nav="search")
    return render_template_string(page, q=q, results=results)
