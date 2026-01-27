# rate_limit.py
from __future__ import annotations
import random
import time
from dataclasses import dataclass
from typing import Optional

@dataclass
class RateLimitConfig:
    # tokens per second
    rate: float
    # max burst tokens
    burst: int
    # base backoff seconds
    backoff_base: float = 0.8
    # max backoff seconds
    backoff_max: float = 30.0

class TokenBucketLimiter:
    def __init__(self, cfg: RateLimitConfig):
        self.cfg = cfg
        self.tokens = float(cfg.burst)
        self.last = time.monotonic()

    def acquire(self, tokens: float = 1.0) -> None:
        while True:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.cfg.burst, self.tokens + elapsed * self.cfg.rate)

            if self.tokens >= tokens:
                self.tokens -= tokens
                return

            need = (tokens - self.tokens) / self.cfg.rate if self.cfg.rate > 0 else 1.0
            time.sleep(max(0.01, need))

def compute_backoff(attempt: int, base: float, max_s: float) -> float:
    # exponential backoff + jitter
    raw = min(max_s, base * (2 ** attempt))
    jitter = random.uniform(0, raw * 0.25)
    return raw + jitter
