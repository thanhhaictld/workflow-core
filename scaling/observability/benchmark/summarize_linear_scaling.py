#!/usr/bin/env python3
"""Summarize fixed-load 1/2/4/8/16 worker scaling from persisted k6 JSON.

CI runners are CPU-constrained; this report is a regression signal, NOT a
dedicated-host linear scalability claim. Throughput is server-side workflow
completion throughput, not request submission RPS.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", default="batch")
    args = parser.parse_args()

    rows = []
    for path in sorted(args.input.glob("k6-*-*.json")):
        parts = path.stem.split("-")
        if len(parts) != 3:
            continue
        workers, mode = int(parts[1]), parts[2]
        data = json.loads(path.read_text())
        count = int(data["count"])
        completed = int(data["completed"])
        failed = int(data["failed"])
        if count != completed or failed != 0:
            raise SystemExit(f"Incomplete workflow run: {path}: {completed}/{count}, failed={failed}")
        rows.append({
            "variant": args.label,
            "policy": mode,
            "processors": workers,
            "completed": completed,
            "throughput_workflows_s": round(float(data["serverWorkflowsPerSecond"]), 4),
            "workflow_p95_ms": round(float(data["workflowP95Ms"]), 2),
            "workflow_p99_ms": round(float(data["workflowP99Ms"]), 2),
        })
    if not rows:
        raise SystemExit(f"No k6 summary files under {args.input}")

    for row in rows:
        baseline = next((r for r in rows if r["policy"] == row["policy"]
                         and r["processors"] == 1), None)
        if baseline is None:
            raise SystemExit(f"Missing 1-processor baseline for {row['policy']}")
        speedup = row["throughput_workflows_s"] / baseline["throughput_workflows_s"]
        row["speedup_vs_one"] = round(speedup, 3)
        row["scaling_efficiency"] = round(speedup / row["processors"], 3)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    print(f"SCALING SUMMARY [{args.label}]")
    print("policy workers throughput/s speedup efficiency p95_ms p99_ms")
    for row in sorted(rows, key=lambda r: (r["policy"], r["processors"])):
        print(f"{row['policy']:>10} {row['processors']:>7} "
              f"{row['throughput_workflows_s']:>12.3f} "
              f"{row['speedup_vs_one']:>7.3f} "
              f"{row['scaling_efficiency']:>9.3f} "
              f"{row['workflow_p95_ms']:>8.2f} {row['workflow_p99_ms']:>8.2f}")


if __name__ == "__main__":
    main()
