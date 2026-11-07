#!/usr/bin/env python3
"""Compare bounded worker concurrency and demand-aware claim admission.

All performance figures are experimental signals from short shared CI runners,
not SLO evidence or proof of production speedup. Verification JSON is the
durable source of completed workflows and ordering/attempt safety.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import median
from summarize_claim_diagnostics import histogram_quantile_ms, metric


def median_number(rows, name):
    items = [float(r[name]) for r in rows if r.get(name) is not None]
    return round(median(items), 2) if items else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expected", type=int, default=16)
    args = p.parse_args()

    rows = []
    for path in sorted(args.input.glob("diag-*.json")):
        bits = path.stem.split("-")
        if len(bits) != 5:
            continue
        _, policy, cap, batch, repeat = bits
        verify_path = path.with_name("verify-" + "-".join(bits[1:]) + ".json")
        if policy not in ("queue", "demand") or batch not in ("true", "false"):
            raise SystemExit("Unexpected diagnostic label: " + str(path))
        if not verify_path.exists():
            raise SystemExit("Missing durable verification: " + str(verify_path))
        v = json.loads(verify_path.read_text())
        if v["completed"] != v["workflows"] or v["failed"] or v["extraAttempts"] or v["unfinishedAttempts"]:
            raise SystemExit("Durable verification failed: " + str(verify_path))
        d = json.loads(path.read_text())
        telemetry = d["prometheusDelta"]

        claim_p99 = histogram_quantile_ms(telemetry, "workflow_hub_claim_duration_seconds", .99)
        admission_p99 = histogram_quantile_ms(telemetry, "workflow_hub_claim_admission_wait_seconds", .99)
        completion_p99 = histogram_quantile_ms(telemetry, "workflow_hub_worker_completion_http_duration_seconds", .99)
        worker_poll_p99 = histogram_quantile_ms(
            telemetry, "workflow_hub_worker_claim_http_duration_seconds", .99,
            {"outcome": "claimed"})
        matching_poll_p99 = histogram_quantile_ms(
            telemetry, "workflow_hub_matching_poll_duration_seconds", .99,
            {"outcome": "leased"})
        worker_empty_p99 = histogram_quantile_ms(
            telemetry, "workflow_hub_worker_claim_http_duration_seconds", .99,
            {"outcome": "empty"})
        if any(x is None for x in (claim_p99, admission_p99, completion_p99, worker_poll_p99, matching_poll_p99)):
            raise SystemExit("Prometheus histogram buckets missing for claim/admission/completion/poll: " + str(path))

        rows.append({
            "admission_mode": policy, "worker_cap_per_lane": int(cap),
            "batch": batch, "repeat": int(repeat),
            "completed": int(v["completed"]),
            "throughput_workflows_s": float(v["throughputWorkflowsPerSecond"]),
            "workflow_p95_ms": float(v["workflowP95Ms"]),
            "workflow_p99_ms": float(v["workflowP99Ms"]),
            "claim_p95_ms": histogram_quantile_ms(telemetry, "workflow_hub_claim_duration_seconds", .95),
            "claim_p99_ms": claim_p99,
            "admission_p99_ms": admission_p99,
            "connection_p99_ms": histogram_quantile_ms(telemetry, "workflow_hub_claim_db_connection_acquire_seconds", .99),
            "batch_p99_ms": histogram_quantile_ms(telemetry, "workflow_hub_claim_unconstrained_batch_duration_seconds", .99),
            "matching_p99_ms": histogram_quantile_ms(telemetry, "workflow_hub_matching_dispatch_duration_seconds", .99),
            "completion_p99_ms": completion_p99,
            "worker_poll_p99_ms": worker_poll_p99,
            "matching_poll_p99_ms": matching_poll_p99,
            "worker_empty_poll_p99_ms": worker_empty_p99,
            "claim_deferrals": metric(telemetry, "workflow_hub_claim_admission_deferred_total"),
            "postgres_cpu_pct": d["pgWaitSamples"]["cpuPctByRole"].get("postgres",{}).get("mean"),
            "processor_cpu_pct": d["pgWaitSamples"]["cpuPctByRole"].get("processor",{}).get("mean"),
        })

    if len(rows) != args.expected:
        raise SystemExit(f"Expected {args.expected} cases, found {len(rows)}")

    grouped = {}
    for row in rows:
        key = (row["batch"], row["admission_mode"], row["worker_cap_per_lane"])
        grouped.setdefault(key, []).append(row)
    if len(grouped) * 2 != args.expected or any(len(vals) != 2 for vals in grouped.values()):
        raise SystemExit("Expected exactly two independent trials per combination")

    fields = list(rows[0])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.with_suffix(".csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda x: (x["batch"],x["admission_mode"],x["worker_cap_per_lane"],x["repeat"])))

    report = [
        "# Demand-aware claim admission vs worker concurrency A/B",
        "",
        "16 processor containers, 2 Hub replicas, 4 matching shards; each row is the median of two verified runs.",
        "All quantiles are classic histogram estimates from **per-run Prometheus bucket deltas**.",
        "",
        "| Batch | Admission | Worker cap/lane | Throughput (wf/s) | Workflow p95 (s) | Claim p99 (ms) | Admission p99 (ms) | Claimed worker HTTP p99 (ms) | Leased matching poll p99 (ms) | Completion HTTP p99 (ms) | Deferrals | Processor CPU % |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for (batch, policy, cap), vals in sorted(grouped.items()):
        def f(key, suffix="", divisor=1):
            x = median_number(vals, key)
            return "n/a" if x is None else f"{x/divisor:.2f}{suffix}"
        report.append(
            f"| {batch} | {policy} | {cap or 'unlimited'} | "
            f"{f('throughput_workflows_s')} | {f('workflow_p95_ms', divisor=1000)} | "
            f"{f('claim_p99_ms')} | {f('admission_p99_ms')} | {f('worker_poll_p99_ms')} | "
            f"{f('matching_poll_p99_ms')} | {f('completion_p99_ms')} | "
            f"{f('claim_deferrals')} | {f('processor_cpu_pct')} |"
        )

    report.extend(["",
        "## Controls and interpretation", "",
        "- Queue mode preserves the prior semaphore waiting behavior. Demand mode bounds its wait queue to one and defers excess event-matched claims without touching PostgreSQL. A deferred hint is retained and retried; it is **not** an empty-task result.",
        "- Worker cap applies independently to activity and workflow lanes on each processor replica, not globally across all replicas.",
        "- Every scenario checks actual PostgreSQL attempt records and workflow completion, with zero unexpected replays.",
        "- Non-claim connections are reserved at the application admission level; worker completion HTTP p99 is recorded as an end-to-end guardrail, not the PostgreSQL commit latency alone.",
        "- A 4-core GitHub runner is shared by worker replicas, Hub gateways, PostgreSQL and telemetry. Results are noisy and not capacity forecasts.",
        "- p99 for admission, claim and completion are independent distributions and must **not** be summed.",
        "- **Claimed-outcome only** worker HTTP p99 captures delays deferred *outside* the SQL claim. Empty long-poll timeouts can be ~4 seconds by design and would distort useful-work claim p99.",
        "- **Leased-outcome only** matching long-poll p99 is reported; empty and errored poll durations remain in the per-run CSV.",
        "- The default configuration is unchanged. Compare queue/unlimited vs demand/cap=2, then isolate each factor by comparing the other rows.",
        "",
    ])

    args.output.write_text("\n".join(report))
    print("\n".join(report))


if __name__ == "__main__":
    main()
