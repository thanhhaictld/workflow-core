#!/usr/bin/env python3
"""Reproduce the audit-event export scan question in disposable PostgreSQL 17."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import pathlib
import subprocess
import sys
import time


CONTAINER = "orca-hub-audit-export-pg17-luna-20261115"
IMAGE = "postgres:17"
LABELS = {
    "org.orca.owner": "hub-audit-export-experiment-luna",
    "org.orca.scope": "synthetic-only",
    "org.orca.worktree": "hub-matching-scale-luna",
}
RUN_ID = "audit-cohort"

BASELINE_SQL = f"""SELECT coalesce(json_agg(t),'[]')
FROM (
  SELECT * FROM wfc_scale.hub_workflow_event
  WHERE workflow_id IN (
    SELECT id FROM wfc_scale.hub_workflow WHERE run_id='{RUN_ID}'
  )
) t;"""

CANDIDATE_SQL = f"""WITH selected_workflows AS MATERIALIZED (
  SELECT id FROM wfc_scale.hub_workflow WHERE run_id='{RUN_ID}'
), selected_events AS (
  SELECT e.*
  FROM selected_workflows AS w
  CROSS JOIN LATERAL (
    SELECT we.*
    FROM wfc_scale.hub_workflow_event AS we
    WHERE we.workflow_id=w.id
    OFFSET 0
  ) AS e
)
SELECT coalesce(json_agg(t),'[]')
FROM (SELECT * FROM selected_events) t;"""

SCENARIOS = {
    "small": {"workflows": 7000, "background_14_events": 3000, "expected_events": 98000},
    "large": {"workflows": 66472, "background_14_events": 30365, "expected_events": 898501},
}
COHORT_WORKFLOWS = 4000
COHORT_EVENTS_PER_WORKFLOW = 14


def run(args: list[str], *, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(args, input=input_text, text=True, capture_output=True, check=False)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {args[0:5]}: {result.stderr.strip()}")
    return result


def docker(*args: str, input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    return run(["docker", *args], input_text=input_text, check=check)


def sql(db: str, text: str, *, output: pathlib.Path | None = None) -> str:
    result = docker(
        "exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db,
        "-X", "-v", "ON_ERROR_STOP=1", "-A", "-t", input_text=text,
    )
    if output is not None:
        output.write_text(result.stdout, encoding="utf-8")
    return result.stdout


def create_db(db: str) -> None:
    docker("exec", CONTAINER, "createdb", "-U", "postgres", db)


def setup_sql_exact(scenario_name: str, scenario: dict[str, int]) -> str:
    """Generate per-background event counts that sum to the target fixture size."""
    # Deterministic order by generated workflow UUID is avoided; use a stable id rank.
    # The two fixtures are small enough to recreate through a temporary ordinal column.
    workflows = scenario["workflows"]
    background_14 = scenario["background_14_events"]
    total = scenario["expected_events"]
    return f"""
CREATE SCHEMA wfc_scale;
CREATE TABLE wfc_scale.hub_workflow (
  id uuid PRIMARY KEY,
  ordinal integer NOT NULL,
  run_id text NOT NULL
);
CREATE INDEX ix_hub_workflow_run ON wfc_scale.hub_workflow(run_id);
CREATE TABLE wfc_scale.hub_workflow_event (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workflow_id uuid NOT NULL REFERENCES wfc_scale.hub_workflow(id),
  step_index integer NOT NULL,
  task_id uuid,
  activity_type text NOT NULL,
  event_type text NOT NULL,
  occurred_at timestamptz NOT NULL,
  payload jsonb,
  correlation_id text,
  execution_pointer_id text,
  parent_pointer_id text,
  scope jsonb,
  attempt integer
);
CREATE INDEX ix_hub_workflow_event_trace
  ON wfc_scale.hub_workflow_event(workflow_id, occurred_at, id);
INSERT INTO wfc_scale.hub_workflow(id,ordinal,run_id)
SELECT md5('workflow-'||g)::uuid,g,
       CASE WHEN g <= {COHORT_WORKFLOWS} THEN '{RUN_ID}' ELSE 'background-history' END
FROM generate_series(1,{workflows}) AS g;
INSERT INTO wfc_scale.hub_workflow_event
  (workflow_id,step_index,task_id,activity_type,event_type,occurred_at,payload,
   correlation_id,execution_pointer_id,parent_pointer_id,scope,attempt)
SELECT w.id,
       (n.seq % 4)::integer,
       CASE WHEN n.seq % 14 = 0 THEN NULL ELSE md5('task-'||w.id::text||'-'||n.seq)::uuid END,
       CASE WHEN n.seq % 3 = 0 THEN '__native_workflow__' ELSE 'synthetic-activity' END,
       CASE n.seq % 5 WHEN 0 THEN 'WorkflowTaskClaimed' WHEN 1 THEN 'StepStarted'
             WHEN 2 THEN 'ActivityScheduled' WHEN 3 THEN 'ActivityResultReceived'
             ELSE 'WorkflowCompleted' END,
       timestamptz '2026-11-15 04:00:00+00' + (n.seq * interval '1 millisecond'),
       CASE WHEN n.seq % 7 = 0 THEN NULL
            ELSE jsonb_build_object('synthetic',true,'text',repeat('x',124)) END,
       NULL,
       CASE WHEN n.seq % 7 < 3 THEN NULL ELSE md5('pointer-'||w.id::text||'-'||n.seq) END,
       NULL,
       CASE WHEN n.seq % 7 < 3 THEN NULL ELSE jsonb_build_object('synthetic',true) END,
       CASE WHEN n.seq % 7 < 3 THEN NULL ELSE 0 END
FROM wfc_scale.hub_workflow AS w
CROSS JOIN LATERAL generate_series(
  1,
  CASE WHEN w.ordinal <= {COHORT_WORKFLOWS} THEN {COHORT_EVENTS_PER_WORKFLOW}
       WHEN w.ordinal <= {COHORT_WORKFLOWS + background_14} THEN 14
       ELSE 13 END
) AS n(seq);
ANALYZE wfc_scale.hub_workflow;
ANALYZE wfc_scale.hub_workflow_event;
"""


def explain_sql(query: str) -> str:
    return f"EXPLAIN (ANALYZE, BUFFERS, SERIALIZE TEXT, TIMING TRUE, FORMAT JSON)\n{query}\n"


def output_summary(path: pathlib.Path) -> dict[str, object]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return {
        "rows": len(data),
        "bytes": path.stat().st_size,
        "id_count": len({row["id"] for row in data}),
        "sorted_by_id_sha256": hashlib.sha256(
            json.dumps(sorted(data, key=lambda row: row["id"]), separators=(",", ":"), sort_keys=True).encode()
        ).hexdigest(),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--artifact-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, object] = {
        "generated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "container_name": CONTAINER,
        "postgres_image": IMAGE,
        "network_mode": "none",
        "limits": {"cpus": 1, "memory": "1g", "shm_size": "128m"},
        "labels": LABELS,
        "sql_sha256": {
            "baseline": hashlib.sha256(BASELINE_SQL.encode()).hexdigest(),
            "candidate": hashlib.sha256(CANDIDATE_SQL.encode()).hexdigest(),
        },
        "fixtures": {},
        "cleanup": "container is --rm and stopped in finally; no external/named mounts; image's anonymous data volume removed on exit",
    }
    old = docker("ps", "-a", "--filter", f"name=^{CONTAINER}$", "--format", "{{.Names}}", check=False)
    if old.stdout.strip():
        raise SystemExit(f"Refusing to reuse existing container named {CONTAINER}")
    try:
        docker(
            "run", "--detach", "--rm", "--name", CONTAINER,
            "--label", f"org.orca.owner={LABELS['org.orca.owner']}",
            "--label", f"org.orca.scope={LABELS['org.orca.scope']}",
            "--label", f"org.orca.worktree={LABELS['org.orca.worktree']}",
            "--network", "none", "--cpus=1", "--memory=1g", "--shm-size=128m",
            "-e", "POSTGRES_HOST_AUTH_METHOD=trust", IMAGE,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            probe = docker("exec", CONTAINER, "pg_isready", "-U", "postgres", check=False)
            if probe.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("isolated PostgreSQL did not become ready within 60 seconds")
        identity = docker(
            "inspect", "--format",
            '{{.Id}}|{{.Image}}|{{.HostConfig.NetworkMode}}|{{.HostConfig.NanoCpus}}|{{.HostConfig.Memory}}|{{json .Mounts}}|{{json .Config.Labels}}',
            CONTAINER,
        ).stdout.strip()
        manifest["container_identity"] = identity
        manifest["postgres_version"] = docker(
            "exec", CONTAINER, "psql", "-U", "postgres", "-d", "postgres", "-At", "-c", "select version()"
        ).stdout.strip()

        for name, scenario in SCENARIOS.items():
            db = f"audit_{name}"
            create_db(db)
            sql(db, setup_sql_exact(name, scenario))
            counts = json.loads(sql(db, """
SELECT json_build_object(
 'workflows',(SELECT count(*) FROM wfc_scale.hub_workflow),
 'cohort_workflows',(SELECT count(*) FROM wfc_scale.hub_workflow WHERE run_id='audit-cohort'),
 'events',(SELECT count(*) FROM wfc_scale.hub_workflow_event),
 'cohort_events',(SELECT count(*) FROM wfc_scale.hub_workflow_event e JOIN wfc_scale.hub_workflow w ON w.id=e.workflow_id WHERE w.run_id='audit-cohort'),
 'event_pages',(SELECT relpages FROM pg_class WHERE oid='wfc_scale.hub_workflow_event'::regclass),
 'event_reltuples',(SELECT reltuples FROM pg_class WHERE oid='wfc_scale.hub_workflow_event'::regclass),
 'event_avg_width',(SELECT avg_width FROM pg_stats WHERE schemaname='wfc_scale' AND tablename='hub_workflow_event' AND attname='payload'),
 'event_null_payload_fraction',(SELECT null_frac FROM pg_stats WHERE schemaname='wfc_scale' AND tablename='hub_workflow_event' AND attname='payload'),
 'event_heap_bytes',pg_relation_size('wfc_scale.hub_workflow_event'),
 'event_total_bytes',pg_total_relation_size('wfc_scale.hub_workflow_event')
);
""").strip())
            if int(counts["events"]) != scenario["expected_events"]:
                raise RuntimeError(f"{name}: expected {scenario['expected_events']} events, got {counts['events']}")
            if int(counts["cohort_workflows"]) != COHORT_WORKFLOWS or int(counts["cohort_events"]) != 56000:
                raise RuntimeError(f"{name}: cohort fixture count mismatch: {counts}")
            record: dict[str, object] = {"fixture": counts, "plans": [], "outputs": {}}

            # Keep all actual plans. Alternating order helps expose cache/order effects.
            order = [("baseline", BASELINE_SQL), ("candidate", CANDIDATE_SQL),
                     ("baseline", BASELINE_SQL), ("candidate", CANDIDATE_SQL),
                     ("baseline", BASELINE_SQL), ("candidate", CANDIDATE_SQL)]
            for i, (shape, query) in enumerate(order, 1):
                plan_path = args.artifact_dir / f"{name}-{i}-{shape}-explain-analyze.json"
                sql(db, explain_sql(query), output=plan_path)
                record["plans"].append({"order": i, "shape": shape, "path": plan_path.name})

            for shape, query in [("baseline", BASELINE_SQL), ("candidate", CANDIDATE_SQL)]:
                outpath = args.artifact_dir / f"{name}-{shape}-export.json"
                result = docker("exec", "-i", CONTAINER, "psql", "-U", "postgres", "-d", db,
                                "-X", "-v", "ON_ERROR_STOP=1", "-A", "-t", input_text=query)
                outpath.write_text(result.stdout, encoding="utf-8")
                record["outputs"][shape] = output_summary(outpath)
            if record["outputs"]["baseline"]["sorted_by_id_sha256"] != record["outputs"]["candidate"]["sorted_by_id_sha256"]:
                raise RuntimeError(f"{name}: baseline/candidate JSON row multisets differ")
            manifest["fixtures"][name] = record
            print(name, json.dumps(record["fixture"], sort_keys=True),
                  "output bytes", record["outputs"]["baseline"]["bytes"],
                  "identical multiset", record["outputs"]["baseline"]["sorted_by_id_sha256"] == record["outputs"]["candidate"]["sorted_by_id_sha256"])
        (args.artifact_dir / "experiment-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        return 0
    finally:
        stop = docker("stop", CONTAINER, check=False)
        if stop.returncode not in (0, 1):
            print("warning: could not stop owned container", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
