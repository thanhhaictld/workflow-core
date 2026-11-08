#!/usr/bin/env python3
"""Summarize a same-run OFF -> ON -> OFF, 1,000-workflow per-policy A/B.

This report intentionally does not turn noisy throughput into a CI failure.
The k6 and persisted lease/FIFO checks remain hard assertions in the workflow.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

MODES = ("unordered", "partition", "keyed")
STAGES = ("off_a", "on", "off_b")

def percent(value: float) -> str:
    return f"{value:+.1f}%"

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    root = args.root

    rows = []
    results = {}
    for mode in MODES:
        arms = {}
        for stage in STAGES:
            path = root / f"verify-{stage}-{mode}.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            assert data["completed"] == args.count and data["failed"] == 0, (stage, mode, data)
            assert data["extraAttempts"] == 0 and data["unfinishedAttempts"] == 0, (stage, mode, data)
            assert data["overlappingLeasesWithinPolicyScope"] == 0 or mode == "unordered", (stage, mode, data)
            arms[stage] = data
        results[mode] = arms

        first = arms["off_a"]["throughputWorkflowsPerSecond"]
        tuned = arms["on"]["throughputWorkflowsPerSecond"]
        last = arms["off_b"]["throughputWorkflowsPerSecond"]
        reference = (first + last) / 2.0
        lift = (tuned / reference - 1) * 100
        drift = (last / first - 1) * 100
        p95_reference = (arms["off_a"]["workflowP95Ms"] + arms["off_b"]["workflowP95Ms"]) / 2.0
        p99_reference = (arms["off_a"]["workflowP99Ms"] + arms["off_b"]["workflowP99Ms"]) / 2.0
        p95_delta = (arms["on"]["workflowP95Ms"] / p95_reference - 1) * 100
        p99_delta = (arms["on"]["workflowP99Ms"] / p99_reference - 1) * 100
        stable = abs(drift) <= 15
        rows.append({
            "mode": mode, "offA": first, "on": tuned, "offB": last,
            "throughputLiftPct": round(lift, 2),
            "controlDriftPct": round(drift, 2),
            "p95ChangePct": round(p95_delta, 2),
            "p99ChangePct": round(p99_delta, 2),
            "baselineStable": stable,
            "assessment": "promising, repeat required"
                if stable and lift > 5 and p99_delta <= 10
                else "inconclusive: control drift" if not stable
                else "no demonstrated benefit",
        })
    output = {"cohortWorkflows": args.count, "stages": list(STAGES), "modes": rows}
    (root / "comparison.json").write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")

    markdown = [
        "## Same-run indexed FIFO A/B: 1,000 workflows per policy per stage",
        "",
        "Eight processor replicas; two Hub replicas; four matching shards; shared PostgreSQL.",
        "OFF → ON → OFF on one GitHub runner. Every stage requires PostgreSQL-verified",
        "completions, FIFO invariants, and no extra activity attempts.",
        "",
        "| Mode | OFF A WF/s | ON WF/s | OFF B WF/s | ON vs OFF avg | OFF drift | p95 change | p99 change | Assessment |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in rows:
        markdown.append(
            f"| {row['mode']} | {row['offA']:.2f} | {row['on']:.2f} | {row['offB']:.2f} | "
            f"{percent(row['throughputLiftPct'])} | {percent(row['controlDriftPct'])} | "
            f"{percent(row['p95ChangePct'])} | {percent(row['p99ChangePct'])} | "
            f"{row['assessment']} |"
        )
    markdown.extend([
        "",
        "A single 9,000-workflow run is not a statistically independent repeat.",
        "Baseline drift above 15% invalidates a performance conclusion.",
        "The ON path must also be evaluated for CPU, connection, WAL and tail-latency cost.",
        ""
    ])
    report = "\n".join(markdown)
    print(report)
    (root / "comparison.md").write_text(report, encoding="utf-8")
    if args.markdown:
        with args.markdown.open("a", encoding="utf-8") as file:
            file.write(report + "\n")

if __name__ == "__main__":
    main()
