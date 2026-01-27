# openai_assistant_client.py
from __future__ import annotations

import time
from typing import Any, Optional

from openai import OpenAI
from config import OpenAIConfig


class OpenAIAssistantClient:
    def __init__(self, cfg: OpenAIConfig):
        self.cfg = cfg
        self.client = OpenAI(api_key=cfg.api_key)

    def _sleep_backoff(self, attempt: int) -> None:
        time.sleep(self.cfg.backoff_base_seconds * (2 ** attempt))

    def summarize_with_assistant(
        self,
        merged_context_text: str,
        extra_user_prompt: Optional[str] = None,
    ) -> str:
        """
        Creates a new thread per contact:
          - message: merged_context_text (+ optional extra_user_prompt)
          - run: assistant_id
          - poll until completed
          - return assistant text output
        """
        content = merged_context_text.strip()
        if extra_user_prompt:
            content = f"{extra_user_prompt.strip()}\n\n{content}"

        last_err: Exception | None = None

        for attempt in range(self.cfg.max_retries):
            try:
                thread = self.client.beta.threads.create()
                self.client.beta.threads.messages.create(
                    thread_id=thread.id,
                    role="user",
                    content=content,
                )

                run = self.client.beta.threads.runs.create(
                    thread_id=thread.id,
                    assistant_id=self.cfg.assistant_id,
                )

                # Poll run
                start = time.time()
                while True:
                    run = self.client.beta.threads.runs.retrieve(
                        thread_id=thread.id,
                        run_id=run.id,
                    )

                    status = getattr(run, "status", None)
                    if status in ("completed", "failed", "cancelled", "expired"):
                        break

                    if (time.time() - start) > self.cfg.max_poll_seconds:
                        raise RuntimeError(f"OpenAI run polling timed out after {self.cfg.max_poll_seconds}s")

                    time.sleep(self.cfg.poll_interval_seconds)

                if run.status != "completed":
                    raise RuntimeError(f"OpenAI run ended with status={run.status}")

                # Fetch messages (latest first)
                msgs = self.client.beta.threads.messages.list(
                    thread_id=thread.id,
                    order="desc",
                    limit=20,
                )

                # Find first assistant message with text content
                for m in msgs.data:
                    if getattr(m, "role", "") != "assistant":
                        continue
                    content_items = getattr(m, "content", []) or []
                    for item in content_items:
                        # text block
                        if getattr(item, "type", "") == "text":
                            txt = item.text.value
                            if txt and txt.strip():
                                return txt.strip()

                raise RuntimeError("No assistant text message found in thread.")

            except Exception as e:
                last_err = e
                self._sleep_backoff(attempt)

        raise RuntimeError(f"OpenAI assistant call failed after retries. Last error: {last_err}")
