#!/usr/bin/env python3
"""Plot schema-2 benchmark CSV; requires the optional matplotlib package."""
import argparse
import csv
from pathlib import Path

def report_plot(filename):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    path = Path(filename)
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    if not rows or any(row.get("schema_version") != "2" for row in rows):
        raise ValueError("Expected nonempty schema-2 hibench.csv")
    labels = [row["workload"] + " / " + row["scale"] + " / " + row["run_id"][:8] for row in rows]
    fig, ax = plt.subplots(figsize=(10, max(3, len(rows) * .4)))
    ax.barh(labels, [float(row["duration_s"]) for row in rows])
    ax.set_xlabel("Duration (seconds)")
    fig.tight_layout()
    output = path.with_name("duration.png")
    fig.savefig(output)
    plt.close(fig)
    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("report")
    print(report_plot(parser.parse_args().report))
