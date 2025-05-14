from metaflow import FlowSpec, step, Parameter

class Artifacts(FlowSpec):
    """A flow that demonstrates how Metaflow artifacts track values across
    multiple steps while we perform different arithmetic operations.
    Run with:
        python artifacts.py run --start_value 3
    """

    # Allow overriding the initial number at runtime
    start_value = Parameter(
        "start_value",
        default=1,
        type=int,
        help="Initial number for the arithmetic flow",
    )

    @step
    def start(self):
        """Initialize the running value and history list."""
        self.value = self.start_value
        self.history = [self.value]
        print(f"[start] value = {self.value}")
        self.next(self.add)

    @step
    def add(self):
        """Add 10 to the current value."""
        self.value += 10
        self.history.append(self.value)
        print(f"[add] value = {self.value}")
        self.next(self.subtract)

    @step
    def subtract(self):
        """Subtract 2 from the current value."""
        self.value -= 20
        self.history.append(self.value)
        print(f"[subtract] value = {self.value}")
        self.next(self.multiply)

    @step
    def multiply(self):
        """Multiply the current value by 3."""
        self.value *= 5
        self.history.append(self.value)
        print(f"[multiply] value = {self.value}")
        self.next(self.end)

    @step
    def end(self):
        """Report the full history, its sum, and the average."""
        self.total = sum(self.history)
        self.average = self.total / len(self.history)

        print("\n--- Results ---")
        print("History :", self.history)
        print("Sum     :", self.total)
        print(f"Average : {self.average:.2f}")
        print("---------------\n")

if __name__ == "__main__":
    Artifacts()
