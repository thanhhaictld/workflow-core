#!/usr/bin/env python3
"""Sample host CPU utilization and PostgreSQL work/waits during a workflow cohort.

Diagnostic only: GitHub runners have limited CPU; pg_stat_activity is sampled and
cannot prove DB CPU saturation, while /proc/stat measures the shared host, not
individual Hub/processor containers. Never collect task payloads or credentials.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import signal
import subprocess
import time
from collections import Counter

SQL = """
SELECT json_build_object(
  'database', (SELECT json_build_object(
    'xact_commit', xact_commit, 'xact_rollback', xact_rollback,
    'tup_returned', tup_returned, 'tup_fetched', tup_fetched,
    'tup_inserted', tup_inserted, 'tup_updated', tup_updated,
    'tup_deleted', tup_deleted, 'blks_read', blks_read,
    'blks_hit', blks_hit, 'deadlocks', deadlocks,
    'temp_bytes', temp_bytes, 'numbackends', numbackends)
    FROM pg_stat_database WHERE datname=current_database()),
  'wal', (SELECT json_build_object('wal_bytes', wal_bytes::text,
    'wal_records', wal_records, 'wal_fpi', wal_fpi) FROM pg_stat_wal),
  'waits', (SELECT coalesce(json_agg(row_to_json(q)), '[]'::json) FROM (
    SELECT coalesce(wait_event_type,'None') AS wait_type,
           coalesce(wait_event,'None') AS wait_event,
           count(*) AS backends
    FROM pg_stat_activity
    WHERE datname=current_database() AND state='active'
      AND pid<>pg_backend_pid()
    GROUP BY 1,2
  ) q)
)::text
"""

def snapshot() -> dict:
    command = [
        "docker", "compose", "-f", "scaling/observability/compose.yaml",
        "--env-file", "scaling/observability/.env.example",
        "exec", "-T", "postgres", "psql", "-X", "-U", "workflow",
        "-d", "workflow", "-Atc", SQL,
    ]
    out = subprocess.run(command, capture_output=True, text=True,
                         check=True, timeout=20).stdout
    return json.loads(out.strip())

def cpu_times() -> tuple[int, int]:
    line = pathlib.Path("/proc/stat").read_text().splitlines()[0].split()
    ticks = list(map(int, line[1:]))
    return sum(ticks), ticks[3] + (ticks[4] if len(ticks) > 4 else 0)

def cgroup_cpu() -> dict:
    # cgroup v2 can report quota and throttling, but may describe the
    # runner rather than all sibling Docker containers; record as context.
    root = pathlib.Path("/sys/fs/cgroup")
    result: dict = {}
    try:
        result["cpuMax"] = (root / "cpu.max").read_text().strip()
        result["cpuStat"] = {
            k: int(v) for k, v in (
                line.split() for line in (root / "cpu.stat").read_text().splitlines()
            )
        }
    except (OSError, ValueError):
        pass
    return result

def nonnegative_delta(before: dict, after: dict) -> dict:
    return {
        k: int(after[k]) - int(before[k])
        for k in before.keys() & after.keys()
        if k != "numbackends" and before[k] is not None and after[k] is not None
    }

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    if args.interval < 0.5:
        parser.error("--interval must be >= 0.5 seconds")

    observations = []
    waits = Counter()
    failures = []
    running = True
    def terminate(_signum, _frame) -> None:
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    before = None
    first_cpu = cpu_times()
    last_cpu = first_cpu
    started = time.monotonic()
    while running:
        try:
            sample = snapshot()
            if before is None:
                before = sample
            previous = last_cpu
            last_cpu = cpu_times()
            total = last_cpu[0] - previous[0]
            idle = last_cpu[1] - previous[1]
            busy = max(0.0, min(100.0, 100 * (1 - idle / total))) if total > 0 else None
            for row in sample.get("waits") or []:
                waits[f"{row['wait_type']}:{row['wait_event']}"] += int(row["backends"])
            observations.append({
                "seconds": round(time.monotonic() - started, 3),
                "hostCpuBusyPercent": None if busy is None else round(busy, 2),
                "hostLoad1m": round(os.getloadavg()[0], 2),
                "postgresActive": sum(int(r["backends"]) for r in sample.get("waits") or []),
                "pgWaits": sample.get("waits") or [],
                "dbBackends": sample.get("database", {}).get("numbackends"),
            })
            final = sample
        except (subprocess.SubprocessError, OSError, ValueError, json.JSONDecodeError) as ex:
            failures.append(str(ex)[:160])
        # Sleep in short intervals so SIGTERM produces a final snapshot quickly.
        for _ in range(max(1, int(args.interval * 5))):
            if not running: break
            time.sleep(args.interval / max(1, int(args.interval * 5)))

    # Read a final snapshot after SIGTERM to include commits from the last
    # sampling window. Missing snapshots are explicit, never silently zero.
    try:
        final = snapshot()
    except (subprocess.SubprocessError, OSError, ValueError, json.JSONDecodeError) as ex:
        failures.append("final: " + str(ex)[:160])
        final = locals().get("final")

    total_cpu = last_cpu[0] - first_cpu[0]
    idle_cpu = last_cpu[1] - first_cpu[1]
    average_busy = round(100 * (1 - idle_cpu / total_cpu), 2) if total_cpu > 0 else None
    delta = {
        section: nonnegative_delta(before[section], final[section])
        if before and final and before.get(section) and final.get(section) else None
        for section in ("database", "wal")
    }
    report = {
        "durationSeconds": round(time.monotonic() - started, 3),
        "samples": len(observations),
        "hostCpuBusyPercent": average_busy,
        "cpuCount": os.cpu_count(),
        "cgroupCpu": cgroup_cpu(),
        "dbDelta": delta["database"],
        "walDelta": delta["wal"],
        "sampledPgWaitBackendCounts": dict(waits),
        "observations": observations,
        "errors": failures[:20],
        "limitations": (
            "Host CPU includes Hub, PostgreSQL, worker and k6 workloads; "
            "pg_stat_activity samples miss brief locks; counter deltas include "
            "background DB work, not per-workflow attribution; pg_stat_wal "
            "and pg_stat_database are cumulative and may reset externally."
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report[k] for k in (
        "samples", "hostCpuBusyPercent", "dbDelta", "walDelta",
        "sampledPgWaitBackendCounts", "errors"
    )}))

if __name__ == "__main__":
    main()
