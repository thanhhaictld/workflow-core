#!/usr/bin/env python3
"""Capture attribution evidence for Hub claim SQL under a short-lived CI database.

Only local Docker, PostgreSQL and Prometheus are queried. Diagnostics never
contact production or store bearer tokens. pg_stat_statements needs the opt-in
Docker Compose diagnostics overlay. Snapshot intervals are observational, not
a CPU profiler or a substitute for EXPLAIN on representative SQL.
"""
from __future__ import annotations

import argparse
import collections
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.parse
import urllib.request

COMPOSE = ["docker", "compose", "-f", "scaling/observability/compose.yaml",
           "-f", "scaling/observability/compose.matching-ha.yaml",
           "-f", "scaling/observability/compose.matching-shards.yaml",
           "-f", "scaling/observability/compose.claim-diagnostics.yaml",
           "--env-file", "scaling/observability/.env.example"]


def psql(sql: str) -> str:
    cp = subprocess.run(COMPOSE + ["exec", "-T", "postgres", "psql", "-X",
        "-U", "workflow", "-d", "workflow", "-At", "-c", sql],
        capture_output=True, text=True, check=True, timeout=30)
    return cp.stdout.strip()


def psql_json(sql: str):
    return json.loads(psql(sql))


def prom_snapshot() -> dict[str, float]:
    # Histogram sums/counts and counters; avoid high-cardinality bucket exports.
    query = '{__name__=~"workflow_hub_(claim|matching|worker_claim|worker_empty).*(_sum|_count|_total)"}'
    url = "http://127.0.0.1:9090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            result = json.load(response)
        if result.get("status") != "success":
            return {"_prom_error": -1.0}
        metrics: dict[str, float] = collections.defaultdict(float)
        for series in result["data"]["result"]:
            labels = series["metric"]
            label_suffix = ",".join(key + "=" + str(labels[key])
                                    for key in ("outcome", "claim_strategy", "batch_result")
                                    if key in labels)
            metric_key = labels["__name__"] + (
                "{" + label_suffix + "}" if label_suffix else "")
            metrics[metric_key] += float(series["value"][1])
        return dict(metrics)
    except (OSError, ValueError, KeyError):
        return {"_prom_error": -1.0}


ACTIVE_SQL = """
SELECT coalesce(json_agg(row_to_json(r)), '[]'::json) FROM (
 SELECT coalesce(state,'unknown') AS state,
        coalesce(wait_event_type,'running') AS wait_type,
        coalesce(wait_event,'none') AS wait_event, count(*) AS connections
 FROM pg_stat_activity
 WHERE datname=current_database() AND pid<>pg_backend_pid()
 GROUP BY 1,2,3
) r
"""

STATEMENTS_SQL = """
SELECT coalesce(json_agg(row_to_json(r)), '[]'::json) FROM (
 SELECT queryid::text AS query_id, calls, rows,
        round(total_exec_time::numeric,2) AS total_ms,
        round(mean_exec_time::numeric,3) AS mean_ms,
        round(max_exec_time::numeric,3) AS max_ms,
        shared_blks_hit AS blocks_hit, shared_blks_read AS blocks_read,
        round(blk_read_time::numeric,2) AS block_read_ms,
        regexp_replace(left(query,420), '[[:space:]]+', ' ', 'g') AS sql
 FROM pg_stat_statements
 WHERE query ILIKE '%wfc_scale.hub_task%'
    OR query ILIKE '%wfc_scale.hub_workflow%'
    OR query ILIKE '%wfc_scale.hub_resource%'
 ORDER BY total_exec_time DESC LIMIT 35
) r
"""

DB_SQL = """
SELECT row_to_json(r) FROM (
 SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit,
        tup_returned, tup_fetched, tup_inserted, tup_updated, deadlocks,
        temp_files
 FROM pg_stat_database WHERE datname=current_database()
) r
"""


def stats() -> list[dict]:
    cp = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}"],
        capture_output=True, text=True, check=True, timeout=25)
    collected = []
    for row in cp.stdout.splitlines():
        try:
            item = json.loads(row)
            name = item.get("Name", "")
            if "workflow-hub-observability" not in name:
                continue
            cpu_pct = float(str(item.get("CPUPerc", "0")).strip().rstrip("%"))
            role = "postgres" if "-postgres-" in name else (
                "hub" if re.search(r"-hub(-2)?-\d+$", name) else (
                "processor" if "-processor-" in name else "other"))
            collected.append({"role": role, "name": name, "cpu_pct": cpu_pct})
        except (ValueError, KeyError):
            continue
    return collected


def sample(output: Path, interval: float):
    observations = []
    def stop(_signal, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    try:
        while True:
            observation = {"timestamp": time.time()}
            try:
                observation["connections"] = psql_json(ACTIVE_SQL)
            except (OSError, subprocess.CalledProcessError, ValueError) as ex:
                observation["postgres_error"] = str(ex)[:200]
            try:
                observation["containers"] = stats()
            except (OSError, subprocess.CalledProcessError) as ex:
                observation["docker_error"] = str(ex)[:200]
            observations.append(observation)
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        wait_totals = collections.Counter()
        cpu_totals = collections.defaultdict(list)
        peak_active = 0
        for observation in observations:
            active = 0
            for connection in observation.get("connections", []):
                n = int(connection["connections"])
                if connection["state"] == "active":
                    active += n
                    wait_totals[connection["wait_type"] + ":" +
                                connection["wait_event"]] += n
            peak_active = max(peak_active, active)
            by_role = collections.Counter()
            for container in observation.get("containers", []):
                by_role[container["role"]] += container["cpu_pct"]
            for role, cpu in by_role.items():
                cpu_totals[role].append(cpu)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps({
            "samples": len(observations),
            "waitSampledBackendCounts": dict(wait_totals),
            "maxActivePostgresBackends": peak_active,
            "cpuPctByRole": {role: {"mean": round(sum(v)/len(v), 2),
                                    "max": round(max(v), 2)}
                             for role,v in cpu_totals.items()},
            "observations": observations,
            "limitations": "docker stats CPU% is per core; samples are neither perf profiles nor pod isolation"
        }, indent=2) + "\n")
        print("Sampled diagnostics:", output, len(observations))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("phase", choices=["prepare", "sample", "finish"])
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--before", type=Path)
    p.add_argument("--sampler", type=Path)
    p.add_argument("--interval", type=float, default=1.0)
    a = p.parse_args()
    if a.phase == "sample":
        sample(a.output, a.interval)
        return
    if a.phase == "prepare":
        psql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements; SELECT pg_stat_statements_reset();")
        a.output.parent.mkdir(parents=True, exist_ok=True)
        a.output.write_text(json.dumps({
            "startedAt": time.time(), "prometheus": prom_snapshot(),
            "cpuCount": os.cpu_count(), "cpuQuota": (
                Path("/sys/fs/cgroup/cpu.max").read_text().strip()
                if Path("/sys/fs/cgroup/cpu.max").exists() else "unknown"),
            "db": psql_json(DB_SQL)
        }, indent=2) + "\n")
        print("Prepared SQL statement reset:", a.output)
        return
    if not a.before or not a.sampler:
        p.error("finish requires --before and --sampler")
    before = json.loads(a.before.read_text())
    sampled = json.loads(a.sampler.read_text())
    current = prom_snapshot()
    delta = {key: round(value - before["prometheus"].get(key, 0), 4)
             for key, value in current.items() if not key.startswith("_")}
    after_db = psql_json(DB_SQL)
    db_delta = {key: after_db[key] - before["db"][key]
                for key in after_db if key != "numbackends"
                and isinstance(after_db[key], (int,float))
                and isinstance(before["db"].get(key), (int,float))}
    report = {"startedAt": before["startedAt"], "finishedAt": time.time(),
              "cpuCount": before["cpuCount"], "cpuQuota": before["cpuQuota"],
              "prometheusDelta": delta, "pgDatabaseDelta": db_delta,
              "pgStatStatements": psql_json(STATEMENTS_SQL),
              "pgWaitSamples": sampled}
    a.output.write_text(json.dumps(report, indent=2) + "\n")
    print("Claim diagnosis:", json.dumps({
        "cpuCount": report["cpuCount"], "cpuQuota": report["cpuQuota"],
        "statements": len(report["pgStatStatements"]),
        "samples": sampled["samples"], "waits": sampled["waitSampledBackendCounts"],
        "cpuPctByRole": sampled["cpuPctByRole"]
    }))


if __name__ == "__main__":
    main()
