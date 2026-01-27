# trello_client.py (ÄNDERUNGEN)
from __future__ import annotations
import time
import requests
from typing import Any, Optional

from config import TrelloConfig
from rate_limit import TokenBucketLimiter, RateLimitConfig, compute_backoff

class TrelloClient:
    def __init__(self, cfg: TrelloConfig):
        self.cfg = cfg
        self.session = requests.Session()

        # konservativ starten (anpassbar)
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=5.0, burst=5))

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.cfg.api_base}{path}"
        base_params = {"key": self.cfg.api_key, "token": self.cfg.api_token}
        if params:
            base_params.update(params)

        last_err: Exception | None = None

        for attempt in range(8):
            self.limiter.acquire(1.0)

            try:
                resp = self.session.get(url, params=base_params, timeout=self.cfg.timeout_seconds)

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        time.sleep(float(retry_after))
                    else:
                        time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))
                    continue

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_err = e
                time.sleep(compute_backoff(attempt, base=0.8, max_s=20.0))

        raise RuntimeError(f"Trello request failed after retries: GET {path}. Last error: {last_err}")
