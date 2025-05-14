from metaflow import FlowSpec, Parameter, step, retry
import json, random


class SquareNumbers(FlowSpec):
    """
    Run:
        python parameters.py run --numbers "[10, 20]"
    """

    numbers = Parameter(
        "numbers",
        help='JSON list of ints, e.g. "[1, 2, 3]"',
        default="[1, 2, 3]",
        type=str,
    )

    # ──────────────────────────────────────────────

    @step
    def start(self):
        self.num_list = json.loads(self.numbers)
        print(f"[start] Parsed list: {self.num_list}")
        self.next(self.flaky_service)

    # -- flaky step protected by automatic retries -----------------
    @retry(times=3, minutes_between_retries=0.05)   # 3 tries, ~3 s apart
    @step
    def flaky_service(self):
        print("[flaky_service] Calling external API …")
        if random.random() < 0.5:                    # 50 % failure chance
            print("   ✖ Simulated 503 error – raising")
            raise RuntimeError("Mock service unavailable")
        else:
            print("   ✔ Service call succeeded")
        self.next(self.square, foreach="num_list")
    # --------------------------------------------------------------

    @step
    def square(self):
        num = self.input
        self.squared = num * num
        print(f"[square] {num}² = {self.squared}")
        self.next(self.join)

    @step
    def join(self, inputs):
        self.num_list = inputs[0].num_list
        self.squares  = [i.squared for i in inputs]
        self.total    = sum(self.squares)

        print("\n--- Results ---")
        print("Original list :", self.num_list)
        print("Squared list  :", self.squares)
        print("Sum of squares:", self.total)
        print("---------------\n")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    SquareNumbers()
