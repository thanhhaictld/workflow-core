#!/usr/bin/env python3
"""Per-workflow critical-path reconstruction for the 100-workflow p99 diagnostic.

All timestamps are PostgreSQL-persisted. Leased duration includes remote
transport/completion and is NOT the 75 ms application body duration.
No runtime behavior is modified.
"""
import argparse
import csv
import io
import json
import math
import os
import pathlib
import re
import statistics
import subprocess

p = argparse.ArgumentParser()
p.add_argument("--run-id", required=True)
p.add_argument("--mode", choices=["unordered", "partition", "keyed"], required=True)
p.add_argument("--stage", choices=["8", "16"], required=True)
p.add_argument("--output", required=True)
args = p.parse_args()
assert re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id)
tasktypes = {"unordered": "policy-unordered-step",
             "partition": "policy-partition-step",
             "keyed": "policy-keyed-step"}
tasktype = tasktypes[args.mode]

# Native worker-task attempts have no workflow_step, so hub_task_attempt
# only covers the activity. Native claims are reconstructed from event journal.
sql = f"""
WITH wf AS (
  SELECT id, created_at, completed_at, partition_id
  FROM wfc_scale.hub_workflow WHERE run_id='{args.run_id}'
),
activity AS (
  SELECT t.workflow_id, min(t.created_at) AS created_at
  FROM wfc_scale.hub_task t JOIN wf ON wf.id=t.workflow_id
  WHERE t.task_type='{tasktype}'
  GROUP BY t.workflow_id
),
attempt AS (
  SELECT t.workflow_id, min(a.claimed_at) AS claimed_at,
         max(a.finished_at) AS finished_at, count(*) AS attempts
  FROM wfc_scale.hub_task t JOIN wf ON wf.id=t.workflow_id
  JOIN wfc_scale.hub_task_attempt a ON a.task_id=t.id
  WHERE t.task_type='{tasktype}'
  GROUP BY t.workflow_id
),
native AS (
  SELECT e.workflow_id, min(e.occurred_at) AS first_claim,
         count(*) AS native_claims
  FROM wfc_scale.hub_workflow_event e JOIN wf ON wf.id=e.workflow_id
  WHERE e.event_type='WorkflowTaskClaimed'
    AND e.activity_type='__native_workflow__'
  GROUP BY e.workflow_id
)
SELECT wf.id::text AS workflow_id, wf.partition_id,
  round(extract(epoch FROM wf.completed_at-wf.created_at)::numeric,6) AS total_s,
  round(extract(epoch FROM native.first_claim-wf.created_at)::numeric,6) AS initial_queue_s,
  round(extract(epoch FROM activity.created_at-native.first_claim)::numeric,6) AS initial_processing_s,
  round(extract(epoch FROM attempt.claimed_at-activity.created_at)::numeric,6) AS activity_queue_s,
  round(extract(epoch FROM attempt.finished_at-attempt.claimed_at)::numeric,6) AS activity_lease_s,
  round(extract(epoch FROM resume.claimed_at-attempt.finished_at)::numeric,6) AS resume_queue_s,
  round(extract(epoch FROM wf.completed_at-resume.claimed_at)::numeric,6) AS final_processing_s,
  coalesce(attempt.attempts,0) AS activity_attempts,
  coalesce(native.native_claims,0) AS native_claims
FROM wf
LEFT JOIN activity ON activity.workflow_id=wf.id
LEFT JOIN attempt ON attempt.workflow_id=wf.id
LEFT JOIN native ON native.workflow_id=wf.id
LEFT JOIN LATERAL (
    SELECT min(e.occurred_at) AS claimed_at
    FROM wfc_scale.hub_workflow_event e
    WHERE e.workflow_id=wf.id
      AND e.event_type='WorkflowTaskClaimed'
      AND e.activity_type='__native_workflow__'
      AND e.occurred_at >= attempt.finished_at
) resume ON true
ORDER BY wf.completed_at-wf.created_at DESC NULLS LAST
"""
command = [
    "docker", "compose", "-f", "scaling/observability/compose.yaml",
    "--env-file", "scaling/observability/.env.example", "exec", "-T",
    "postgres", "psql", "-X", "-U", "workflow", "-d", "workflow",
    "-c", "COPY (" + sql + ") TO STDOUT WITH (FORMAT csv, HEADER true)"
]
result = subprocess.run(command, text=True, capture_output=True, check=True, timeout=90)
rows = list(csv.DictReader(io.StringIO(result.stdout)))
numeric = ["total_s", "initial_queue_s", "initial_processing_s",
           "activity_queue_s", "activity_lease_s", "resume_queue_s",
           "final_processing_s"]
for row in rows:
    for key in numeric:
        row[key] = float(row[key]) if row[key] else None
    for key in ("partition_id", "activity_attempts", "native_claims"):
        row[key] = int(row[key])

def percentile(values, pct):
    numbers = sorted(v for v in values if v is not None)
    if not numbers:
        return None
    idx = (len(numbers)-1)*pct
    low = math.floor(idx)
    high = math.ceil(idx)
    return round(numbers[low] + (numbers[high]-numbers[low])*(idx-low), 3)

aggregates = {}
for key in numeric:
    vals = [r[key] for r in rows]
    valid = [v for v in vals if v is not None]
    aggregates[key] = {
        "observed": len(valid), "missing": len(vals)-len(valid),
        "p50": percentile(vals, .50), "p95": percentile(vals, .95),
        "p99": percentile(vals, .99),
        "mean": round(statistics.mean(valid),3) if valid else None,
        "max": round(max(valid),3) if valid else None,
    }
slowest = rows[:10]
segments = numeric[1:]
for row in slowest:
    candidates = {k: max(0,row[k]) for k in segments if row[k] is not None}
    row["largest_segment"] = max(candidates,key=candidates.get) if candidates else "missing"
    row["largest_segment_seconds"] = round(candidates[row["largest_segment"]],3) if candidates else None
    row["accounted_seconds"] = round(sum(row[k] for k in segments if row[k] is not None),3)
    row["unaccounted_seconds"] = round(row["total_s"]-row["accounted_seconds"],3) if row["total_s"] is not None else None
report = {
    "runId": args.run_id, "mode": args.mode, "processors": int(args.stage),
    "workflows": len(rows), "timingUnits": "seconds",
    "definition": "workflow p99 is completed_at-created_at; task activity lease includes Hub transport and completion",
    "caveat": "Native claim events use transaction timestamps; sub-second negative adjacent intervals should be treated as timestamp artifacts, not negative runtime.",
    "latencyByStage": aggregates, "slowestWorkflows": slowest,
    "activityRetries": sum(max(0,r["activity_attempts"]-1) for r in rows),
    "nativeClaims": sum(r["native_claims"] for r in rows),
    "coverage": {
        "firstNativeClaim": sum(r["initial_queue_s"] is not None for r in rows),
        "activityCreated": sum(r["activity_queue_s"] is not None for r in rows),
        "activityClaimAndFinish": sum(r["activity_lease_s"] is not None for r in rows),
        "nativeResumed": sum(r["resume_queue_s"] is not None for r in rows),
    },
}
pathlib.Path(args.output).parent.mkdir(parents=True,exist_ok=True)
pathlib.Path(args.output).write_text(json.dumps(report,indent=2)+"\n")
print(json.dumps({
    "mode":args.mode,"processors":int(args.stage),"count":len(rows),
    "p99ByStage":{k:v["p99"] for k,v in aggregates.items()},
    "coverage":report["coverage"],
    "slowest":slowest[:5],
},indent=2))
assert len(rows)==100, f"Expected 100 completed workflows; got {len(rows)}"
assert report["coverage"]["activityClaimAndFinish"]==100, "Missing activity claim timeline"
assert report["coverage"]["firstNativeClaim"]==100, "Missing native workflow claims"
