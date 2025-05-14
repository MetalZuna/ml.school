"""
prompt_response_card.py  –  prompt → LLM → styled HTML card

Quick start
-----------
uv pip install openai python-dotenv

# Option A: key in .env
OPENAI_API_KEY=sk-•••

# Option B: pass on CLI
python prompt_response_card.py run --api_key sk-•••

Default prompt:
python prompt_response_card.py run

Custom prompt:
python prompt_response_card.py run --prompt "Explain Basel III in two sentences"
"""

from __future__ import annotations

import os
import json
from textwrap import shorten

from dotenv import load_dotenv
from openai import OpenAI, RateLimitError, APIConnectionError
from metaflow import FlowSpec, Parameter, step, card

load_dotenv()  # so OPENAI_API_KEY in .env is picked up


class PromptCardFlow(FlowSpec):
    """One-shot LLM call + HTML card visualisation."""

    prompt = Parameter(
        "prompt",
        default="Tell me a fun fact about space.",
        help="Text to send to the chat model",
    )

    api_key = Parameter(
        "api_key",
        default=None,
        help="OpenAI key (overrides env var OPENAI_API_KEY)",
    )

    # ------------------------------------------------------------------
    @step
    def start(self):
        """Call the LLM (or stub) and store the answer."""
        key = self.api_key or os.getenv("OPENAI_API_KEY")
        client = OpenAI(api_key=key) if key else None

        try:
            if not client:
                raise ValueError("No key supplied")

            resp = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": self.prompt}],
                temperature=0.7,
                max_tokens=64,
            )
            self.answer = resp.choices[0].message.content.strip()

        except (RateLimitError, APIConnectionError, Exception) as exc:
            # graceful fallback so the flow still completes and card renders
            self.answer = (
                "Sorry, the LLM is unavailable right now "
                f"(reason: {shorten(str(exc), 60, placeholder='…')})."
            )


        self.next(self.report)

    # ------------------------------------------------------------------
    @card(type="html")
    @step
    def report(self):
        """Generate a pretty HTML card of prompt → response."""
        # Store both artifacts for downstream / lineage
        self.prompt_text = self.prompt
        self.response_text = self.answer

        self.html = f"""
        <html>
        <head>
          <style>
            body {{ font-family: Arial, sans-serif; padding: 1.5rem; }}
            .prompt {{ color:#555; font-style: italic; margin-bottom:1rem; }}
            .answer {{ background:#f4f6ff; border-left:4px solid #4c6ef5;
                       padding:1rem; border-radius:4px; }}
          </style>
        </head>
        <body>
          <h2>LLM Prompt &amp; Response</h2>
          <div class="prompt">“{self.prompt_text}”</div>
          <div class="answer">{self.response_text}</div>
        </body>
        </html>
        """
        self.next(self.end)

    # ------------------------------------------------------------------
    @step
    def end(self):
        pass


if __name__ == "__main__":
    PromptCardFlow()
