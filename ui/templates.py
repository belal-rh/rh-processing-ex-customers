# ui/templates.py
"""
Zentrale Template-Sammlung für die UI.

Ziel:
- Einheitliches Layout (Header / Navigation / Content)
- Keine Abhängigkeit von externen CSS/JS Frameworks
- Klar, professionell, leicht erweiterbar
- Alle Templates als Python-Strings (kein extra templates/-Ordner nötig)

Hinweis:
- Wird von den Route-Modulen via render_template_string genutzt
"""

# ----------------------------
# Base Layout
# ----------------------------

BASE_LAYOUT = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8"/>
  <title>{{ title or "Ex-Kunden Analyse" }}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>

  <style>
    :root {
      --bg: #f7f7f8;
      --card: #ffffff;
      --border: #e5e5e5;
      --text: #1f2937;
      --muted: #6b7280;
      --primary: #2563eb;
      --danger: #dc2626;
      --warn: #d97706;
      --ok: #16a34a;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }

    a { color: var(--primary); text-decoration: none; }
    a:hover { text-decoration: underline; }

    header {
      background: var(--card);
      border-bottom: 1px solid var(--border);
      padding: 12px 20px;
      display: flex;
      align-items: center;
      gap: 20px;
    }

    header .logo {
      font-weight: 700;
      font-size: 16px;
    }

    header nav {
      display: flex;
      gap: 14px;
    }

    header nav a {
      color: var(--muted);
      font-weight: 500;
    }

    header nav a.active {
      color: var(--text);
      font-weight: 600;
    }

    main {
      padding: 24px;
      max-width: 1400px;
      margin: 0 auto;
    }

    h1, h2, h3 {
      margin-top: 0;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      margin-bottom: 16px;
    }

    .row {
      display: flex;
      gap: 12px;
      align-items: center;
      flex-wrap: wrap;
    }

    .spacer {
      flex: 1;
    }

    .btn {
      padding: 8px 14px;
      border-radius: 8px;
      border: 1px solid var(--border);
      background: #fff;
      cursor: pointer;
      font-weight: 500;
    }

    .btn.primary {
      background: var(--primary);
      border-color: var(--primary);
      color: #fff;
    }

    .btn.warn {
      background: #fff7ed;
      border-color: var(--warn);
      color: var(--warn);
    }

    .btn.danger {
      background: #fef2f2;
      border-color: var(--danger);
      color: var(--danger);
    }

    .btn.ok {
      background: #ecfdf5;
      border-color: var(--ok);
      color: var(--ok);
    }

    .btn:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }

    th, td {
      border-bottom: 1px solid var(--border);
      padding: 8px 10px;
      text-align: left;
      vertical-align: top;
      font-size: 14px;
    }

    th {
      background: #fafafa;
      font-weight: 600;
      color: var(--muted);
    }

    code, pre {
      font-family: var(--mono);
      font-size: 13px;
    }

    code {
      background: #f1f1f1;
      padding: 2px 6px;
      border-radius: 6px;
    }

    pre {
      background: #f9fafb;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 12px;
      overflow: auto;
      max-height: 420px;
    }

    .status-ok { color: var(--ok); font-weight: 600; }
    .status-warn { color: var(--warn); font-weight: 600; }
    .status-error { color: var(--danger); font-weight: 600; }

    .muted { color: var(--muted); font-size: 13px; }

    .grid-2 {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }

    iframe {
      width: 100%;
      height: 420px;
      border: 1px solid var(--border);
      border-radius: 10px;
      background: #fff;
    }

    input[type="text"], textarea {
      width: 100%;
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid var(--border);
      font-size: 14px;
    }

    .search-box {
      max-width: 420px;
    }

    footer {
      margin-top: 40px;
      padding: 20px;
      text-align: center;
      color: var(--muted);
      font-size: 12px;
    }
  </style>
</head>

<body>
  <header>
    <div class="logo">Ex-Kunden Analyse</div>
    <nav>
      <a href="/" class="{{ 'active' if nav=='upload' else '' }}">Neuer Job</a>
      <a href="/search" class="{{ 'active' if nav=='search' else '' }}">Suche</a>
    </nav>
  </header>

  <main>
    {{ content | safe }}
  </main>

  <footer>
    Lokales Analyse-Tool · keine externen Datenquellen · Dateien bleiben lokal
  </footer>
</body>
</html>
"""

# ----------------------------
# Helper to render pages
# ----------------------------

def render_page(content: str, title: str = "", nav: str = "") -> str:
    """
    Wraps page content into BASE_LAYOUT.
    Usage:
      render_template_string(
          render_page(PAGE_HTML, title="Suche", nav="search"),
          **context
      )
    """
    return BASE_LAYOUT.replace("{{ content | safe }}", content).replace("{{ title or \"Ex-Kunden Analyse\" }}", title).replace("{{ nav }}", nav)
