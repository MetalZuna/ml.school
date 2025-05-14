from metaflow import FlowSpec, step, Parameter

ADD_CONST  = 3   # branch-1 will add this
MULT_CONST = 2   # branch-2 will multiply by this


class Branches(FlowSpec):
    """
    Initialise a number → split into two branches
    (add, multiply) → join → show both results and their sum.
    """

    # Let users override the starting value on the CLI.
    start_value = Parameter("start_value", default=1, type=int)

    @step
    def start(self):
        """Seed the number and branch out."""
        self.x = self.start_value
        self.next(self.add_branch, self.mult_branch)

    @step
    def add_branch(self):
        """Add a constant to x."""
        self.add_result = self.x + ADD_CONST
        self.next(self.join)

    @step
    def mult_branch(self):
        """Multiply x by a constant."""
        self.mult_result = self.x * MULT_CONST
        self.next(self.join)

    @step
    def join(self, inputs):
        """Merge the two branches and compute the total."""
        # Pull in the non-conflicting artifacts automatically
        self.merge_artifacts(inputs)

        add_val  = inputs.add_branch.add_result
        mult_val = inputs.mult_branch.mult_result
        total    = add_val + mult_val

        print("\n--- Branch outcomes ---")
        print(f"Add  branch result : {add_val}")
        print(f"Mult branch result : {mult_val}")
        print(f"Sum of both        : {total}")
        print("-----------------------\n")

        # Persist for downstream use / inspection
        self.sum_of_results = total
        self.next(self.end)

    @step
    def end(self):
        """Show a concise recap."""
        print(f"Start value : {self.x}")
        print(f"Final sum   : {self.sum_of_results}")


if __name__ == "__main__":
    Branches()
