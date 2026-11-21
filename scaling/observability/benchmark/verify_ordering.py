#!/usr/bin/env python3
"""Verify the durable activity attempts and task routing of a completed k6 cohort."""
import argparse
import datetime
import json
import os
import subprocess
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, choices=["unordered", "partition"])
parser.add_argument("--run-id", required=True)
parser.add_argument("--count", required=True, type=int)
parser.add_argument("--output", required=True)
# Allow at-least-once replay only in deliberate owner-crash tests. All
# regular ordering/performance runs preserve strict no-retry assertions.
parser.add_argument("--max-extra-attempts", type=int, default=0)
args = parser.parse_args()

hub = "policy-benchmark"
definition = {
    "unordered": "policy-unordered",
    "partition": "policy-partition-routed",
}[args.mode]
tasktype = {
    "unordered": "policy-unordered-step",
    "partition": "policy-partition-step",
}[args.mode]

# All interpolated variables are strictly validated; SQL executes in a
# short-lived GitHub CI database, not in the live application.
import re
assert re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id)
assert 0 < args.count <= 100000
assert 0 <= args.max_extra_attempts <= args.count

# The former verification helper called compose.ps1 with only the base file.
# In Actions that wrapper chooses a distinct project name and therefore
# queried a PostgreSQL container which CI had never started. Use the exact
# active 2-Hub/four-partition project for both CI and local tests.
def compose_command():
    files = [
        "-f", "scaling/observability/compose.yaml",
        "-f", "scaling/observability/compose.matching-ha.yaml",
        "-f", "scaling/observability/compose.matching-shards.yaml",
        "--env-file", "scaling/observability/.env.example",
    ]
    if os.environ.get("GITHUB_ACTIONS") == "true":
        # In CI, compose.yaml derives project name from GITHUB_RUN_ID.
        return ["docker", "compose", *files]
    # Local development sets its unique project name in this wrapper.
    return ["pwsh", "-NoProfile", "-File",
            "scaling/observability/compose.ps1", *files]


def query(sql):
    result = subprocess.run(compose_command() + [
        "exec", "-T", "postgres", "psql", "-X", "-U", "workflow",
        "-d", "workflow", "-Atc", sql
    ], check=True, capture_output=True, text=True, timeout=60)
    return int(result.stdout.strip())

base = f"""
WITH attempts AS (
  SELECT t.id, t.partition_id, w.partition_id AS workflow_partition, a.finished_at
  FROM wfc_scale.hub_task t
  JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
  JOIN wfc_scale.hub_task_attempt a ON a.task_id=t.id
  WHERE w.hub_id='{hub}' AND w.definition_id='{definition}'
    AND w.run_id='{args.run_id}' AND t.task_type='{tasktype}'
)
"""
total = query(base + "SELECT count(*) FROM attempts")
distinct_tasks = query(base + "SELECT count(DISTINCT id) FROM attempts")
unclosed = query(base + "SELECT count(*) FROM attempts WHERE finished_at IS NULL")
distinct_partition = query(base + "SELECT count(DISTINCT partition_id) FROM attempts")
misrouted = query(base + "SELECT count(*) FROM attempts WHERE partition_id<>workflow_partition")

token = next(v.split("=", 1)[1].strip() for v in
    open("scaling/observability/.env.example") if v.startswith("WFC_HUB_AUTH_TOKEN="))
url = (os.environ.get("WFC_HUB_VERIFY_URL", "http://127.0.0.1:5100").rstrip("/") +
       "/api/hub/runs/" +
       urllib.parse.quote(args.run_id, safe="") + "/summary")
with urllib.request.urlopen(urllib.request.Request(url,
    headers={"Authorization": "Bearer " + token}), timeout=40) as resp:
    summary = json.load(resp)
assert summary["total"] == summary["completed"] == args.count and summary["failed"] == 0, summary

def parse(x):
    return datetime.datetime.fromisoformat(x.replace("Z", "+00:00"))

elapsed = (parse(summary["lastCompletedAt"]) -
           parse(summary["firstCreatedAt"])).total_seconds()
report = {
    "mode": args.mode, "runId": args.run_id,
    "workflows": summary["total"], "completed": summary["completed"],
    "failed": summary["failed"], "activityAttempts": total,
    "distinctActivityTasks": distinct_tasks,
    "extraAttempts": total - distinct_tasks,
    "maxExtraAttemptsAllowed": args.max_extra_attempts,
    "unfinishedAttempts": unclosed, "misroutedActivityAttempts": misrouted,
    "distinctWorkflowPartitions": distinct_partition,
    "elapsedSeconds": round(elapsed, 3),
    "throughputWorkflowsPerSecond": round(args.count / elapsed, 3),
    "workflowP95Ms": round(summary["p95CompletionMs"], 3),
    "workflowP99Ms": round(summary["p99CompletionMs"], 3),
}

with open(args.output, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))

assert distinct_tasks == args.count and unclosed == 0, "Missing or incomplete routed activity attempts"
assert total <= args.count + args.max_extra_attempts, "Too many replayed activity attempts"
assert distinct_partition >= 2, "Workload did not span partitions"
assert misrouted == 0, "Activity task was routed outside its workflow partition"
