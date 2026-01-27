# hubspot_client.py
from __future__ import annotations

import time
from typing import Any
import requests

from config import HubSpotConfig
from rate_limit import TokenBucketLimiter, RateLimitConfig, compute_backoff


class HubSpotClient:
    """
    Read-only usage in this project:
      - associations list (contacts -> notes/calls)
      - batch read objects (notes/calls)
    """

    def __init__(self, cfg: HubSpotConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.cfg.private_app_token}",
                "Content-Type": "application/json",
            }
        )

        # Conservative default. Tune if needed.
        # If you still hit 429, lower rate/burst (e.g. rate=2, burst=2).
        self.limiter = TokenBucketLimiter(RateLimitConfig(rate=4.0, burst=4))

    def _request(self, method: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.cfg.api_base}{path}"
        last_err: Exception | None = None

        for attempt in range(self.cfg.max_retries):
            self.limiter.acquire(1.0)

            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    timeout=self.cfg.timeout_seconds,
                )

                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            time.sleep(float(retry_after))
                        except Exception:
                            time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                    else:
                        time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                    continue

                resp.raise_for_status()
                return resp.json()

            except Exception as e:
                last_err = e
                time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))

        raise RuntimeError(f"HubSpot request failed after retries: {method} {path}. Last error: {last_err}")

    def list_associated_object_ids(
        self,
        contact_id: str,
        to_object_type: str,
        limit: int | None = None,
    ) -> list[str]:
        """
        HubSpot CRM v4 Associations:
          GET /crm/v4/objects/contacts/{contactId}/associations/{toObjectType}
        Returns list of IDs from results[].toObjectId
        """
        collected: list[str] = []
        after: str | None = None
        use_limit = limit or self.cfg.page_limit

        while True:
            params: dict[str, Any] = {"limit": use_limit}
            if after:
                params["after"] = after

            data = self._request(
                "GET",
                f"/crm/v4/objects/contacts/{contact_id}/associations/{to_object_type}",
                params=params,
            )

            results = data.get("results", []) or []
            for r in results:
                to_id = r.get("toObjectId")
                if to_id is not None:
                    collected.append(str(to_id))

            paging = data.get("paging", {}) or {}
            next_info = paging.get("next", {}) if paging else {}
            after = next_info.get("after")
            if not after:
                break

        # de-dupe preserve order
        seen = set()
        uniq: list[str] = []
        for x in collected:
            if x not in seen:
                seen.add(x)
                uniq.append(x)
        return uniq

    def batch_read_objects(
        self,
        object_type: str,
        ids: list[str],
        properties: list[str],
        batch_size: int = 100,
    ) -> list[dict[str, Any]]:
        """
        Batch Read:
          POST /crm/v3/objects/{objectType}/batch/read
        Body: {"properties": [...], "inputs":[{"id":"..."}, ...]}
        """
        out: list[dict[str, Any]] = []

        if not ids:
            return out

        # HubSpot batch read uses POST with JSON body.
        url = f"{self.cfg.api_base}/crm/v3/objects/{object_type}/batch/read"

        for i in range(0, len(ids), batch_size):
            chunk = ids[i : i + batch_size]
            body = {
                "properties": properties,
                "inputs": [{"id": str(x)} for x in chunk],
            }

            last_err: Exception | None = None

            for attempt in range(self.cfg.max_retries):
                self.limiter.acquire(1.0)

                try:
                    resp = self.session.post(url, json=body, timeout=self.cfg.timeout_seconds)

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("Retry-After")
                        if retry_after:
                            try:
                                time.sleep(float(retry_after))
                            except Exception:
                                time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                        else:
                            time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))
                        continue

                    resp.raise_for_status()
                    data = resp.json()
                    results = data.get("results", []) or []
                    out.extend(results)
                    last_err = None
                    break

                except Exception as e:
                    last_err = e
                    time.sleep(compute_backoff(attempt, base=self.cfg.backoff_base_seconds, max_s=30.0))

            if last_err:
                raise RuntimeError(f"HubSpot batch read failed for {object_type}. Last error: {last_err}")

        return out
