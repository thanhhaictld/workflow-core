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
        cpu = d["pgWaitSamples"].get("cpuPctByRole", {})
        rows.append({
            "batch": variant, "shards": int(shards), "repeat": int(repeat),
            "processors": int(workers), "workflows": verified["completed"],
            "throughput": round(float(verified["throughputWorkflowsPerSecond"]),3),
            "p95_ms": round(float(verified["workflowP95Ms"]),2),
            "p99_ms": round(float(verified["workflowP99Ms"]),2),
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
             "| Batch | Matching shards | Processors | Throughput median (range), wf/s | p95 median, s | DB SQL total median, ms | Batch SQL median, ms | Legacy candidate SQL median, ms | PostgreSQL CPU mean, % | Hub CPU mean, % | Processor CPU mean, % |",
             "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for key, values in sorted(groups.items(),key=lambda item:(item[0][1],item[0][0],item[0][2])):
        speeds = [v["throughput"] for v in values]
        def numeric(name):
            return [float(v[name]) for v in values if v.get(name) is not None]
        lines.append(f"| {key[0]} | {key[1]} | {key[2]} | "
            f"{median(speeds):.2f} ({min(speeds):.2f}–{max(speeds):.2f}) | "
            f"{median(numeric('p95_ms'))/1000:.2f} | "
            f"{median(numeric('postgres_sql_ms')):.1f} | "
            f"{median(numeric('batch_sql_ms')):.1f} | "
            f"{median(numeric('legacy_candidate_sql_ms')):.1f} | "
            f"{median(numeric('postgres_mean_cpu_pct')):.1f} | "
            f"{median(numeric('hubs_mean_cpu_pct')):.1f} | "
            f"{median(numeric('processors_mean_cpu_pct')):.1f} |")
    lines += ["", "## Observability interpretation", "",
        "- Query total execution time is summed across PostgreSQL backends; it can exceed wall-clock duration.",
        "- CPU% is a Docker instantaneous estimate normalized to one CPU core; 100% means approximately one core.",
        "- Sampled wait events can miss short locks, and client-side Npgsql pool wait is absent from pg_stat_activity.",
        "- A single matching shard serializes claim dispatch by design; compare 1 versus 4 shards at equal worker count.",
        "- The application may execute batch SQL and a legacy fallback in the same claim call; compare the batch/legacy SQL cost.",
        "- Repeated CI samples are still vulnerable to runner contention and changing database size.",
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
