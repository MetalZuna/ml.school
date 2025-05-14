"""
environment.py  –  compare @environment vs. python-dotenv

How to run:
    # 1️⃣ create a .env file (local only, not pushed to remote executors)
    echo "DOTENV_VAR=loaded-from-dotenv" > .env

    # 2️⃣ execute the flow
    python environment.py run
"""

import os
from metaflow import FlowSpec, step, environment
from dotenv import load_dotenv           # pip install python-dotenv


class EnvCompare(FlowSpec):
    """Show two ways to pass config into a Metaflow step."""

    # ------------------------------------------------------------------
    @step
    def start(self):
        """
        Load variables from .env at *run time* using python-dotenv.
        Works only where the .env file is physically present
        (laptop, or baked into container image).
        """
        load_dotenv()                           # parse the .env file
        self.dotenv_val = os.getenv("DOTENV_VAR")
        print(f"[start] DOTENV_VAR = {self.dotenv_val!r}")
        self.next(self.decorator_step)

    # ------------------------------------------------------------------
    @environment(vars={"DECORATOR_VAR": "hello-from-decorator"})
    @step
    def decorator_step(self):
        """
        Here DECORATOR_VAR exists because the @environment decorator
        injects it *before the task process starts* – even on a
        remote batch / Kubernetes worker.
        """
        self.decorator_val = os.getenv("DECORATOR_VAR")
        print(f"[decorator_step] DECORATOR_VAR = {self.decorator_val!r}")
        self.next(self.end)

    # ------------------------------------------------------------------
    @step
    def end(self):
        print("\n--- Summary -----------------------------")
        print("Value loaded via python-dotenv :", self.dotenv_val)
        print("Value injected by @environment:", self.decorator_val)
        print("-----------------------------------------")

# entry-point -----------------------------------------------------------
if __name__ == "__main__":
    EnvCompare()
