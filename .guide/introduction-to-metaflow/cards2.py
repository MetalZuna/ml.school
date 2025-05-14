from metaflow import FlowSpec, step, card
import random, statistics, base64, io
import matplotlib.pyplot as plt


class VizCardDemo(FlowSpec):
    """
    Run:
        python cards.py run
        python cards.py card view report
    """

    @step
    def start(self):
        """Generate a random dataset."""
        self.data = [random.random() for _ in range(200)]
        self.next(self.report)

    # ---- custom HTML card with a live plot -----------------------
    @card(type="html")
    @step
    def report(self):
        """Create a histogram + stats in a custom card."""
        # 1️⃣  make a matplotlib histogram
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.hist(self.data, bins=20, edgecolor="black")
        ax.set_title("Distribution of Random Numbers")
        ax.set_ylabel("Frequency")
        fig.tight_layout()

        # 2️⃣  convert the plot to base-64 so we can inline it
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        img_b64 = base64.b64encode(buf.getvalue()).decode()

        # 3️⃣  simple descriptive stats
        mean = statistics.mean(self.data)
        stdev = statistics.stdev(self.data)
        dmin, dmax = min(self.data), max(self.data)

        # 4️⃣  build the HTML for the card
        self.html = f"""
        <h1 style="margin-bottom:0">Random-Data Report</h1>
        <p style="color:#555;margin-top:4px">
            Generated inside Metaflow – refresh to reproduce
        </p>

        <h2>Histogram</h2>
        <img src="data:image/png;base64,{img_b64}" style="max-width:100%;" />

        <h2>Summary statistics</h2>
        <table border="1" cellpadding="4">
            <tr><th>Count</th><td>{len(self.data)}</td></tr>
            <tr><th>Mean</th><td>{mean:.4f}</td></tr>
            <tr><th>Std&nbsp;Dev</th><td>{stdev:.4f}</td></tr>
            <tr><th>Min</th><td>{dmin:.4f}</td></tr>
            <tr><th>Max</th><td>{dmax:.4f}</td></tr>
        </table>
        """

        self.next(self.end)
    # --------------------------------------------------------------

    @step
    def end(self):
        pass


if __name__ == "__main__":
    VizCardDemo()
