#!/usr/bin/env python3
"""Join independently verified workflow results with claim/DB/CPU observations.

Report medians and range across repeated jobs; never label shared GitHub
host results as proof of near-linear production scaling.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import statistics


def sql_kind(sql: str) -> str:
    upper = sql.upper()
    if "WITH CANDIDATES AS MATERIALIZED" in upper:
        return "batch_lease"
    if "SELECT ID FROM WFC_SCALE.HUB_TASK" in upper:
        return "legacy_candidate"
    if "FOR UPDATE SKIP LOCKED" in upper and "HUB_TASK WHERE ID" in upper:
        return "legacy_lock"
    if "WFC_SCALE.HUB_RESOURCE" in upper or "WFC_SCALE.HUB_TASK_CONSTRAINT" in upper:
        return "resource_permit"
    if "UPDATE WFC_SCALE.HUB_TASK" in upper:
        return "legacy_update"
    return "other"


def metric(metrics, target):
    return round(sum(float(value) for key,value in metrics.items()
                     if key.startswith(target) and (len(key) == len(target)
                         or key[len(target)] == "{")), 4)


def histogram_quantile_ms(metrics, instrument, quantile, required_labels=None):
    """Estimate the per-case histogram quantile from BEFORE/AFTER bucket deltas.

    Aggregates the bounded outcome/strategy labels by bucket upper bound,
    equivalent to sum by (le) before PromQL histogram_quantile.
    Assumes a classic cumulative histogram exported by the OTEL collector.
    """
    import math

    prefix = instrument + "_bucket{"
    buckets = {}
    for key, value in metrics.items():
        if not key.startswith(prefix) or not key.endswith("}"):
            continue
        labels = dict(part.split("=", 1)
                      for part in key[len(prefix):-1].split(",") if "=" in part)
        if "le" not in labels:
            continue
        if required_labels and any(labels.get(key) != val for key, val in required_labels.items()):
            continue
        upper = float(labels["le"])
        buckets[upper] = buckets.get(upper, 0) + float(value)

    if not buckets or math.inf not in buckets or buckets[math.inf] <= 0:
        return None

    rank = quantile * buckets[math.inf]
    previous_count = 0.0
    previous_upper = 0.0
    for upper, cumulative in sorted(buckets.items()):
        # Metric reset or misaligned scrape windows: reject rather than lie.
        if cumulative + 0.01 < previous_count:
            return None
        if cumulative >= rank:
            if math.isinf(upper):
                # Tail exceeds the largest finite bucket; cannot estimate it.
                return None
            if cumulative == previous_count:
                return round(upper * 1000, 2)
            ratio = (rank - previous_count) / (cumulative - previous_count)
            return round(1000 * (previous_upper + ratio * (upper - previous_upper)), 2)
        previous_count, previous_upper = cumulative, upper
    return None


def median(values):
    return statistics.median(values) if values else 0.0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expected", type=int, default=24)
    a = p.parse_args()
    rows = []
    for path in sorted(a.input.glob("diag-*.json")):
        bits = path.stem.split("-")
        if len(bits) != 5:
            continue
        _, variant, shards, repeat, workers = bits
        verified_path = path.with_name("verify-" + "-".join(bits[1:]) + ".json")
        if not verified_path.exists():
            raise SystemExit("Missing durable verification: " + str(verified_path))
        verified = json.loads(verified_path.read_text())
        if verified["completed"] != verified["workflows"] or verified["failed"] != 0:
            raise SystemExit("Incomplete workflow execution: " + str(verified_path))
        if verified["extraAttempts"] != 0 or verified["unfinishedAttempts"] != 0:
            raise SystemExit("Unexpected task replay/incomplete attempt: " + str(verified_path))
        d = json.loads(path.read_text())
        if not d["pgStatStatements"]:
            raise SystemExit("Missing pg_stat_statements: " + str(path))
        total_sql = 0.0
        kind_costs = {}
        for statement in d["pgStatStatements"]:
            cost = float(statement["total_ms"])
            total_sql += cost
            kind = sql_kind(statement["sql"])
            kind_costs[kind] = kind_costs.get(kind,0.0) + cost
        ms = d["prometheusDelta"]
        claim_p99 = histogram_quantile_ms(ms, "workflow_hub_claim_duration_seconds", .99)
        if claim_p99 is None:
            raise SystemExit("Missing/invalid Prometheus claim p99 histogram buckets: " + str(path))
        cpu = d["pgWaitSamples"].get("cpuPctByRole", {})
        rows.append({
            "batch": variant, "shards": int(shards), "repeat": int(repeat),
            "processors": int(workers), "workflows": verified["completed"],
            "throughput": round(float(verified["throughputWorkflowsPerSecond"]),3),
            "p95_ms": round(float(verified["workflowP95Ms"]),2),
            "p99_ms": round(float(verified["workflowP99Ms"]),2),
            "claim_p95_ms": histogram_quantile_ms(ms, "workflow_hub_claim_duration_seconds", .95),
            "claim_p99_ms": claim_p99,
            "admission_p99_ms": histogram_quantile_ms(ms, "workflow_hub_claim_admission_wait_seconds", .99),
            "connection_p99_ms": histogram_quantile_ms(ms, "workflow_hub_claim_db_connection_acquire_seconds", .99),
            "batch_claim_p99_ms": histogram_quantile_ms(ms, "workflow_hub_claim_unconstrained_batch_duration_seconds", .99),
            "fallback_p99_ms": histogram_quantile_ms(ms, "workflow_hub_claim_legacy_fallback_duration_seconds", .99),
            "matching_p99_ms": histogram_quantile_ms(ms, "workflow_hub_matching_dispatch_duration_seconds", .99),
            "claim_duration_ms": round(1000*metric(ms,"workflow_hub_claim_duration_seconds_sum"),2),
            "batch_claim_ms": round(1000*metric(ms,"workflow_hub_claim_unconstrained_batch_duration_seconds_sum"),2),
            "fallback_ms": round(1000*metric(ms,"workflow_hub_claim_legacy_fallback_duration_seconds_sum"),2),
            "candidates_scanned": metric(ms,"workflow_hub_claim_candidates_scanned_sum"),
            "returned_tasks": metric(ms,"workflow_hub_claim_tasks_returned_sum"),
            "batch_returned": metric(ms,"workflow_hub_claim_unconstrained_batch_count_sum"),
            "matching_claims": metric(ms,"workflow_hub_matching_dispatch_attempts_total"),
            "postgres_sql_ms": round(total_sql,2),
            "batch_sql_ms": round(kind_costs.get("batch_lease",0),2),
            "legacy_candidate_sql_ms": round(kind_costs.get("legacy_candidate",0),2),
            "legacy_update_sql_ms": round(kind_costs.get("legacy_update",0),2),
            "postgres_mean_cpu_pct": cpu.get("postgres",{}).get("mean"),
            "hubs_mean_cpu_pct": cpu.get("hub",{}).get("mean"),
            "processors_mean_cpu_pct": cpu.get("processor",{}).get("mean"),
            "postgres_peak_active": d["pgWaitSamples"]["maxActivePostgresBackends"],
            "db_wait_lock_samples": sum(n for k,n in
                d["pgWaitSamples"]["waitSampledBackendCounts"].items()
                if k.startswith("Lock:")),
            "samples": d["pgWaitSamples"]["samples"],
            "cpu_quota": d["cpuQuota"],
        })
    if len(rows) != a.expected:
        raise SystemExit(f"Expected {a.expected} verified diagnostic cases, got {len(rows)}")
    groups = {}
    for row in rows:
        groups.setdefault((row["batch"],row["shards"],row["processors"]),[]).append(row)
    expected_groups = a.expected // 2
    if len(groups) != expected_groups:
        raise SystemExit(f"Expected {expected_groups} groups, got {len(groups)}")
    lines = ["# Workflow Hub claim-attribution diagnostics", "",
             "Shared CI runner: results are diagnostic, not dedicated-node capacity measurements.",
             "",
             "| Batch | Matching shards | Processors | Throughput median (range), wf/s | Workflow p95, s | Claim p99, ms | Admission p99, ms | Pool p99, ms | Batch p99, ms | DB SQL total, ms | PostgreSQL CPU, % | Processor CPU, % |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for key, values in sorted(groups.items(),key=lambda item:(item[0][1],item[0][0],item[0][2])):
        speeds = [v["throughput"] for v in values]
        def numeric(name):
            return [float(v[name]) for v in values if v.get(name) is not None]
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | "
            f"{median(speeds):.2f} ({min(speeds):.2f}–{max(speeds):.2f}) | "
            f"{median(numeric('p95_ms'))/1000:.2f} | "
            f"{median(numeric('claim_p99_ms')):.1f} | "
            f"{median(numeric('admission_p99_ms')):.1f} | "
            f"{median(numeric('connection_p99_ms')):.1f} | "
            f"{median(numeric('batch_claim_p99_ms')):.1f} | "
            f"{median(numeric('postgres_sql_ms')):.1f} | "
            f"{median(numeric('postgres_mean_cpu_pct')):.1f} | "
            f"{median(numeric('processors_mean_cpu_pct')):.1f} |")
    lines += ["", "## Observability interpretation", "",
        "- Query total execution time is summed across PostgreSQL backends; it can exceed wall-clock duration.",
        "- CPU% is a Docker instantaneous estimate normalized to one CPU core; 100% means approximately one core.",
        "- Sampled wait events can miss short locks, and client-side Npgsql pool wait is absent from pg_stat_activity.",
        "- A single matching shard serializes claim dispatch by design; compare 1 versus 4 shards at equal worker count.",
        "- The application may execute batch SQL and a legacy fallback in the same claim call; compare the batch/legacy SQL cost.",
        "- Repeated CI samples are still vulnerable to runner contention and changing database size.",
        "- Prometheus p95/p99 are histogram estimates, not exact order statistics; choose fine buckets and retain raw deltas.",
        "- Claim p99 measures end-to-end claim latency; PostgreSQL SQL p99 needs an independent query-duration histogram.",
        "",
        "## Raw cases", "",
        "The artifact includes all case JSON, sampled PostgreSQL waits, Docker stats, and SQL statements.",
        ""]
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text("\n".join(lines))
    with a.output.with_suffix(".csv").open("w",newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(sorted(rows,key=lambda r:(r["shards"],r["batch"],r["repeat"],r["processors"])))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
