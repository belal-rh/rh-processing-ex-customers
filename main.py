# main.py
from __future__ import annotations

import argparse

from ui.app import run_ui


def main() -> None:
    """
    Startet die UI (Upload/Preview/Start Job + Suche + Kontakt-Validierung + Push to HubSpot).
    """
    parser = argparse.ArgumentParser(description="Ex-Kunden Aufbereitung - UI Runner")
    parser.add_argument("--host", default="127.0.0.1", help="Host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5055, help="Port (default: 5055)")
    parser.add_argument("--no-open", action="store_true", help="Browser nicht automatisch öffnen")
    args = parser.parse_args()

    run_ui(host=args.host, port=args.port, open=(not args.no_open))


if __name__ == "__main__":
    main()
