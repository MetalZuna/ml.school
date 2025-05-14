"""
students_llm.py  –  LLM-generated (or mocked) list → foreach transform → aggregate

# Install deps in your uv / venv
uv pip install openai python-dotenv

# Put your key in an `.env` or pass on CLI
OPENAI_API_KEY=sk-•••
python students_llm.py run
python students_llm.py run --increase 5
python students_llm.py run --api_key sk-•••
"""

from __future__ import annotations

import json
import os
import random
import string
from typing import List, Dict

from dotenv import load_dotenv
from openai import OpenAI, APIConnectionError, RateLimitError
from metaflow import FlowSpec, Parameter, step, retry

load_dotenv()  # pick up OPENAI_API_KEY from local .env if present


class StudentLLMFlow(FlowSpec):
    """Generate or mock a list of students, fan-out, transform, aggregate."""

    api_key = Parameter(
        "api_key",
        default=None,
        help="OpenAI key; defaults to env var OPENAI_API_KEY",
    )

    increase = Parameter(
        "increase", default=10, type=int, help="Points to add to each score"
    )

    # ────────────────────────────────────────────────────────────────
    @step
    def start(self):
        """
        1. Try calling OpenAI to get 5 random students.
        2. If the call fails (no key, quota, rate-limit, etc.), fall back to
           locally mocked data so the rest of the DAG still runs.
        """
        key = self.api_key or os.getenv("OPENAI_API_KEY")
        client = OpenAI(api_key=key) if key else None

        prompt = (
            "Return ONLY valid JSON (no markdown) containing a list of 5 "
            "objects. Each object has keys 'name' (string) and 'score' (0-100)."
        )

        try:
            if not client:
                raise ValueError("No API key supplied")

            resp = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=150,
            )
            data: List[Dict] = json.loads(resp.choices[0].message.content)
            assert isinstance(data, list)
            self.students = data
            print("[start] ✅ LLM produced:", self.students)

        except (json.JSONDecodeError, AssertionError) as exc:
            raise ValueError(f"LLM returned invalid JSON: {exc}") from exc

        except (RateLimitError, APIConnectionError, Exception) as exc:
            # Any error: quota exceeded, network, missing key, etc.
            print(f"[start] ❗ LLM unavailable ({exc}); using mock data.")
            self.students = [
                {
                    "name": "".join(random.choices(string.ascii_letters, k=5)),
                    "score": random.randint(0, 100),
                }
                for _ in range(5)
            ]
            print("[start] 🧪 Mock students:", self.students)

        self.next(self.process_student, foreach="students")

    # ────────────────────────────────────────────────────────────────
    @retry(times=3, minutes_between_retries=0.05)
    @step
    def process_student(self):
        """Upper-case the name, bump the score."""
        stu = dict(self.input)  # defensive copy
        stu["name"] = stu["name"].upper()
        stu["score"] = int(stu["score"]) + self.increase
        self.updated = stu
        print(f"[process] {stu}")
        self.next(self.join)

    # ────────────────────────────────────────────────────────────────
    @step
    def join(self, inputs):
        """Aggregate results."""
        self.updated_students = [i.updated for i in inputs]
        self.total_score = sum(s["score"] for s in self.updated_students)

        print("\n--- Updated roster ---")
        for s in self.updated_students:
            print(s)
        print("Total score:", self.total_score)
        print("----------------------\n")

        self.next(self.end)

    # ────────────────────────────────────────────────────────────────
    @step
    def end(self):
        """Flow completed."""
        pass


if __name__ == "__main__":
    StudentLLMFlow()
