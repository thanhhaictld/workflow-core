#!/usr/bin/env python3
"""Attribute workflow benchmark pressure to PostgreSQL SQL families and Docker services.

A diagnostic observer, not a workload driver. Never output SQL text, task
payloads, database credentials, or tenant IDs. All SQL-stat counters are
database-wide cumulative deltas; exec time is NOT CPU time.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import pathlib
import signal
import statistics
import subprocess
import time
import urllib.parse
import urllib.request

COMPOSE = [
    "docker", "compose",
    "-f", "scaling/observability/compose.yaml",
    "-f", "scaling/observability/compose.matching-ha.yaml",
    "-f", "scaling/observability/compose.matching-shards.yaml",
    "-f", "scaling/observability/compose.pg-statement-profile.yaml",
    "--env-file", "scaling/observability/.env.example",
]

STATEMENTS_SQL = """
SELECT coalesce(json_agg(row_to_json(s)), '[]'::json)::text
FROM (
 SELECT queryid::text AS fingerprint, toplevel,
   CASE
    WHEN query ILIKE '%hub_matching_partition%' OR query ILIKE '%hub_matching_lease%' THEN 'matching_owner'
    WHEN query ILIKE '%hub_resource%' OR query ILIKE '%hub_task_constraint%' THEN 'policy_admission'
    WHEN query ILIKE '%hub_task_attempt%' THEN 'task_attempt'
    WHEN query ILIKE '%hub_task%' THEN 'task_queue'
    WHEN query ILIKE '%hub_native%' THEN 'native_execution'
    WHEN query ILIKE '%hub_workflow%' THEN 'workflow_state'
    ELSE 'other'
   END AS family,
   calls, total_exec_time, rows, shared_blks_hit, shared_blks_read,
   shared_blk_read_time, wal_bytes::text AS wal_bytes
 FROM pg_stat_statements
 WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND queryid IS NOT NULL AND query NOT ILIKE '%pg_stat_statements%'
 ORDER BY total_exec_time DESC LIMIT 1000
) s;
"""

SESSIONS_SQL = """
SELECT coalesce(json_agg(row_to_json(a)), '[]'::json)::text
FROM (
 SELECT coalesce(nullif(application_name,''), 'unattributed') AS application,
        state, coalesce(wait_event_type,'None') AS wait_type,
        coalesce(wait_event,'None') AS wait_event, count(*) AS sessions
 FROM pg_stat_activity
 WHERE datname=current_database() AND pid<>pg_backend_pid()
 GROUP BY 1,2,3,4
) a;
"""

PROM_QUERY = '{__name__=~"workflow_hub_(claim_db_connection_acquire|claim_admission_wait|claim_candidate_query|matching_dispatch_attempts|matching_dispatch_duration|worker_claim_requests).*"}'


def exec_json_sql(sql: str) -> object:
    result = subprocess.run(
        COMPOSE + ["exec", "-T", "postgres", "psql",
                   "-X", "-U", "workflow", "-d", "workflow", "-Atc", sql],
        capture_output=True, text=True, timeout=25, check=True,
    )
    return json.loads(result.stdout.strip())


def prometheus_snapshot() -> list[dict]:
    url = "http://127.0.0.1:9090/api/v1/query?" + urllib.parse.urlencode(
        {"query": PROM_QUERY}
    )
    with urllib.request.urlopen(url, timeout=8) as response:
        result = json.load(response)
    if result.get("status") != "success":
        raise ValueError("Prometheus query failed")
    # Bounded metric labels only; no workflow IDs, paths, tokens or payloads.
    return [
        {
            "name": item["metric"].get("__name__", ""),
            "service": item["metric"].get("service_name", "unknown"),
            "instance": item["metric"].get("service_instance_id", ""),
            "outcome": item["metric"].get("outcome", ""),
            "value": float(item["value"][1]),
        }
        for item in result["data"]["result"]
    ]


def docker_cpu_sample() -> dict:
    names = subprocess.run(
        ["docker", "ps",
         "--filter", "label=com.docker.compose.project=workflow-hub-observability",
         "--format", '{{.Names}}|{{.Label "com.docker.compose.service"}}'],
        check=True, capture_output=True, text=True, timeout=10,
    ).stdout
    name_to_service = {}
    for row in names.splitlines():
        if "|" in row:
            name, service = row.split("|", 1)
            if service in {"postgres", "hub", "hub-2", "processor"}:
                name_to_service[name] = service
    if not name_to_service:
        raise RuntimeError("No matching benchmark containers found for resource sample")

    stats = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}",
         *name_to_service.keys()],
        check=True, capture_output=True, text=True, timeout=30,
    ).stdout
    cpu_cores = defaultdict(float)
    container_counts = defaultdict(int)
    for line in stats.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        name = row.get("Name", "")
        service = name_to_service.get(name)
        if service is None:
            continue
        cpu_cores[service] += float(str(row["CPUPerc"]).rstrip("%")) / 100
        container_counts[service] += 1
    return {
        "cpuCores": {k: round(v, 3) for k, v in cpu_cores.items()},
        "containers": dict(container_counts),
    }


def nonnegative(value: float) -> float:
    return max(0.0, value)


def diff_statements(before: list[dict], after: list[dict]) -> list[dict]:
    previous = {(r["fingerprint"], r["toplevel"]): r for r in before}
    result = []
    for row in after:
        old = previous.get((row["fingerprint"], row["toplevel"]), {})
        calls = int(row["calls"]) - int(old.get("calls", 0))
        if calls <= 0:
            continue
        elapsed = nonnegative(float(row["total_exec_time"]) - float(old.get("total_exec_time", 0)))
        result.append({
            "fingerprint": row["fingerprint"],
            "topLevel": row["toplevel"],
            "family": row["family"],
            "calls": calls,
            "executionWallMs": round(elapsed, 2),
            "meanExecMs": round(elapsed / calls, 3),
            "rows": int(row["rows"]) - int(old.get("rows", 0)),
            "sharedBlockReads": int(row["shared_blks_read"]) - int(old.get("shared_blks_read", 0)),
            "sharedBlockHits": int(row["shared_blks_hit"]) - int(old.get("shared_blks_hit", 0)),
            "sharedBlockReadMs": round(nonnegative(
                float(row["shared_blk_read_time"]) - float(old.get("shared_blk_read_time", 0))), 2),
            "walBytes": int(row["wal_bytes"]) - int(old.get("wal_bytes", 0)),
        })
    return sorted(result, key=lambda x: x["executionWallMs"], reverse=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--output", required=True, type=pathlib.Path)
    p.add_argument("--interval", type=float, default=3.0)
    args = p.parse_args()
    if args.interval < 1:
        p.error("sampling interval must be >= 1 second")

    running = True
    def stop(_signum, _frame) -> None:
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    errors = []
    before = exec_json_sql(STATEMENTS_SQL)
    if not isinstance(before, list):
        raise RuntimeError("pg_stat_statements was not initialized")
    start_prom = []
    try:
        start_prom = prometheus_snapshot()
    except (ValueError, OSError) as exc:
        errors.append("initial metrics: " + str(exc)[:180])

    cpu_samples = []
    db_activity_samples = []
    began = time.monotonic()
    while running:
        try:
            cpu_samples.append({"atSeconds": round(time.monotonic()-began,2),
                                **docker_cpu_sample()})
            db_activity_samples.append(exec_json_sql(SESSIONS_SQL))
        except (subprocess.SubprocessError, OSError, ValueError, RuntimeError) as exc:
            errors.append("sample: "+str(exc)[:180])
        # Responsive shutdown without adding a busy loop or another SQL poll.
        until = time.monotonic() + args.interval
        while running and time.monotonic() < until:
            time.sleep(min(0.2, until - time.monotonic()))

    # Final query captures counter deltas even when the last Docker sample was
    # a few seconds earlier. SQL text itself is NEVER returned.
    after = exec_json_sql(STATEMENTS_SQL)
    end_prom = []
    try:
        end_prom = prometheus_snapshot()
    except (ValueError, OSError) as exc:
        errors.append("final metrics: " + str(exc)[:180])
    stats = diff_statements(before, after)
    per_service = defaultdict(list)
    for sample in cpu_samples:
        for service, cpu in sample["cpuCores"].items():
            per_service[service].append(cpu)
    avg_cpu = {k: round(statistics.mean(v), 3) for k,v in per_service.items()}
    peak_cpu = {k: round(max(v), 3) for k,v in per_service.items()}

    active_by_app = defaultdict(int)
    for snapshot in db_activity_samples:
        for row in snapshot:
            if row["state"] == "active":
                active_by_app[row["application"]] += int(row["sessions"])
    by_family = defaultdict(lambda: {"calls": 0, "executionWallMs": 0.0,
                                     "walBytes": 0, "sharedBlockReads": 0})
    for row in stats:
        agg = by_family[row["family"]]
        agg["calls"] += row["calls"]
        agg["executionWallMs"] += row["executionWallMs"]
        agg["walBytes"] += row["walBytes"]
        agg["sharedBlockReads"] += row["sharedBlockReads"]

    report = {
        "seconds": round(time.monotonic()-began,2),
        "cpuSampleCount": len(cpu_samples),
        "averageCpuCoresByService": avg_cpu,
        "peakCpuCoresByService": peak_cpu,
        "pgActiveBackendSampleCountsByApplication": dict(active_by_app),
        "pgStatementSamples": stats[:50],
        "pgSqlFamilyTotals": dict(by_family),
        "prometheusBefore": start_prom,
        "prometheusAfter": end_prom,
        "errors": errors[:20],
        "limitations": (
            "Docker stats on a shared runner may add non-trivial overhead. "
            "ExecutionWallMs is aggregated wall execution time, NOT PostgreSQL CPU time. "
            "pg_stat_statements is cumulative, and statements not retained in its "
            "bounded table may not be captured. DB connections from telemetry "
            "may be unattributed. Prometheus scrapes every 5s; short cohorts "
            "may miss samples. Controlled dedicated-host runs remain necessary."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps({
        "cpuSamples":len(cpu_samples),
        "averageCpuCores":avg_cpu,
        "highestCostSql": stats[:5],
        "errors":errors[:3],
    }))


if __name__ == "__main__":
    main()
