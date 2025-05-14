from metaflow import FlowSpec, IncludeFile, step
import csv, io


class Files(FlowSpec):
    """
    Usage example
    -------------
    Place `sample.csv` next to this script, then run:

        python files.py run --file @sample.csv
    """

    file = IncludeFile(
        "file",
        is_text=True,
        help="A CSV file to inspect (pass via --file @<path>)",
    )

    # ──────────────────────────────

    @step
    def start(self):
        """Parse the included CSV and report its shape with validation."""
        text = (self.file or "").strip()

        # 1️⃣  quick empty-file check
        if not text:
            print("❌ CSV is empty.")
            self.valid = False
            self.next(self.end)
            return

        try:
            reader = csv.reader(io.StringIO(text))
            rows = list(reader)

            # 2️⃣  check for inconsistent row lengths
            col_counts = {len(r) for r in rows}
            if len(col_counts) > 1:
                raise ValueError("inconsistent column counts")

            self.n_rows = len(rows)
            self.n_cols = col_counts.pop() if rows else 0
            self.valid = True

            print(f"✅ Parsed OK – rows: {self.n_rows}, cols: {self.n_cols}")

        except Exception as e:
            print(f"❌ Couldn't parse CSV – {e}")
            self.valid = False

        self.next(self.end)

    # ──────────────────────────────

    @step
    def end(self):
        """Finish the flow – nothing else to do."""
        if not self.valid:
            print("Flow completed with CSV errors; see logs above.")


if __name__ == "__main__":
    Files()
