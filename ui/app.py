# ui/app.py
from __future__ import annotations

import threading
import webbrowser

from flask import Flask

from ui.routes_search import bp_search
from ui.routes_contact import bp_contact
from ui.routes_job import bp_job
from ui.routes_upload import bp_upload


def _open_browser(url: str) -> None:
    try:
        webbrowser.open(url, new=1)
    except Exception:
        pass


def create_app() -> Flask:
    """
    Unified Flask app (modular).
    Essential routes included:
    - Upload / Mapping / Preview / Start Job
    - Search (across all jobs)
    - Contact validation (input vs output, rerun step3/4, verify, push)
    - Job dashboard (SSE), review
    """
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50MB Upload-Schutz

    app.register_blueprint(bp_upload)
    app.register_blueprint(bp_search)
    app.register_blueprint(bp_contact)
    app.register_blueprint(bp_job)

    return app


def run_ui(host: str = "127.0.0.1", port: int = 5055, open: bool = True) -> None:
    app = create_app()
    url = f"http://{host}:{port}/"
    if open:
        threading.Timer(0.6, lambda: _open_browser(url)).start()
    app.run(host=host, port=port, debug=False)


if __name__ == "__main__":
    run_ui()
