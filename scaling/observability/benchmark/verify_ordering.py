#!/usr/bin/env python3
"""Verify true PostgreSQL lease-time admission policies after a k6 workflow run.

Checks real activity claims/acks; no in-process synthetic counters.
Note: lease intervals are a safety proxy for admission, NOT actual CPU execution.
"""
import argparse
import datetime
import json
import os
import subprocess
import urllib.parse
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("--mode", required=True, choices=["unordered", "partition", "keyed"])
parser.add_argument("--run-id", required=True)
parser.add_argument("--count", required=True, type=int)
parser.add_argument("--output", required=True)
args = parser.parse_args()

hub = "policy-benchmark"
definition = {
    "unordered": "policy-unordered",
    "partition": "policy-partition-fifo",
    "keyed": "policy-keyed-fifo",
}[args.mode]
tasktype = {
    "unordered": "policy-unordered-step",
    "partition": "policy-partition-step",
    "keyed": "policy-keyed-step",
}[args.mode]

# All interpolated variables are strictly validated; SQL executes in a
# short-lived GitHub CI database, not in the live application.
import re
assert re.fullmatch(r"[A-Za-z0-9_-]{1,100}", args.run_id)
assert 0 < args.count <= 100000

def query(sql):
    result = subprocess.run([
        "docker", "compose", "-f", "scaling/observability/compose.yaml",
        "--env-file", "scaling/observability/.env.example",
        "exec", "-T", "postgres", "psql", "-X", "-U", "workflow", "-d",
        "workflow", "-Atc", sql
    ], check=True, capture_output=True, text=True, timeout=60)
    return int(result.stdout.strip())

scope = {
    "unordered": "t.partition_id::text",
    "partition": "t.partition_id::text",
    "keyed": "t.payload->>'resourceId'",
}[args.mode]
base = f"""
WITH attempts AS (
  SELECT t.id, t.partition_id, t.sequence, t.payload,
         a.claimed_at, a.finished_at, {scope} AS policy_scope
  FROM wfc_scale.hub_task t
  JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
  JOIN wfc_scale.hub_task_attempt a ON a.task_id=t.id
  WHERE w.hub_id='{hub}' AND w.definition_id='{definition}'
    AND w.run_id='{args.run_id}' AND t.task_type='{tasktype}'
)
"""
total = query(base + "SELECT count(*) FROM attempts")
unclosed = query(base + "SELECT count(*) FROM attempts WHERE finished_at IS NULL")
overlap = query(base + """
SELECT COUNT(*) FROM attempts a WHERE EXISTS (
  SELECT 1 FROM attempts b
  WHERE a.id <> b.id AND a.policy_scope=b.policy_scope
    AND a.claimed_at < b.finished_at AND b.claimed_at < a.finished_at
)
""")
constraints = query(f"""
SELECT count(*) FROM wfc_scale.hub_task_constraint c
JOIN wfc_scale.hub_task t ON t.id=c.task_id
JOIN wfc_scale.hub_workflow w ON w.id=t.workflow_id
WHERE w.run_id='{args.run_id}' AND w.definition_id='{definition}'
  AND t.task_type='{tasktype}'
""")
distinct_partition = query(base + "SELECT count(DISTINCT partition_id) FROM attempts")
cross_partition_key = query(base + """
SELECT COUNT(*) FROM (
  SELECT policy_scope FROM attempts
  GROUP BY policy_scope HAVING count(DISTINCT partition_id)>1
) groups
""") if args.mode == "keyed" else None

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
    "unfinishedAttempts": unclosed, "overlappingLeasesWithinPolicyScope": overlap,
    "activityConstraints": constraints, "distinctWorkflowPartitions": distinct_partition,
    "resourceKeysAcrossPartitions": cross_partition_key,
    "elapsedSeconds": round(elapsed, 3),
    "throughputWorkflowsPerSecond": round(args.count / elapsed, 3),
    "workflowP95Ms": round(summary["p95CompletionMs"], 3),
    "workflowP99Ms": round(summary["p99CompletionMs"], 3),
}

with open(args.output, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=2)
print(json.dumps(report, indent=2))

assert total == args.count and unclosed == 0, "Missing or incomplete routed activity attempts"
assert distinct_partition >= 2, "Workload did not span partitions"
if args.mode == "unordered":
    assert constraints == 0, "Unordered activities have an unexpected implicit resource constraint"
    assert overlap > 0, "Expected observable same-partition overlap for unordered tasks"
elif args.mode == "partition":
    assert constraints == args.count, "Expected exactly one partition permit per activity"
    assert overlap == 0, "PartitionSerial allowed overlapping activity leases in same partition"
else:
    assert constraints == args.count, "Expected exactly one keyed permit per activity"
    assert overlap == 0, "KeyedFifo allowed overlapping activity leases for a resource key"
    assert cross_partition_key > 0, "Keyed test did not prove cross-partition coordination"

print("PASS: mode=" + args.mode + " durable workflow and ordering guarantees")
