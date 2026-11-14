"""Validate and summarize manually collected isolated Hub matching trials.

Input is a JSON array; see hub-matching-scale-benchmark.md for the schema.
Missing, duplicate, failed, or incomplete trials fail closed rather than passing gates.
"""
import argparse
import json
import statistics
import sys

parser = argparse.ArgumentParser()
parser.add_argument("input", help="JSON trial array")
parser.add_argument("--output", required=True)
args = parser.parse_args()
with open(args.input, encoding="utf-8") as source:
    trials = json.load(source)
keys = [(mode, lanes, workload) for mode in ("baseline", "tuned")
        for lanes in (8, 400, 4000) for workload in ("idle", "burst", "backlog", "constrained")]
errors, summary = [], {}
for mode, lanes, workload in keys:
    group = [x for x in trials if x.get("mode") == mode and x.get("lanes") == lanes and x.get("workload") == workload]
    warm = [x for x in group if x.get("warmup") is True]
    measured = [x for x in group if x.get("warmup") is False]
    label = f"{mode}/{lanes}/{workload}"
    if not warm or len(measured) != 5 or len({x.get("repetition") for x in measured}) != 5:
        errors.append(f"{label}: needs warmup and five uniquely numbered measured runs")
        continue
    required = ["seconds", "acquisition_statements", "matching_statements", "transactions", "wal_bytes", "cpu_seconds", "completed_tasks"]
    if workload != "idle": required += ["throughput", "schedule_start_p95_ms"]
    if any(x.get("ok") is not True or any(not isinstance(x.get(k), (int, float)) or x[k] < 0 for k in required)
           or x["seconds"] <= 0 or (workload != "idle" and (x["completed_tasks"] <= 0 or x["throughput"] <= 0 or x["schedule_start_p95_ms"] <= 0)) for x in measured):
        errors.append(f"{label}: failed run or missing/invalid measurements")
        continue
    summary[label] = {k: statistics.median(x[k] for x in measured) for k in required}
    summary[label]["statements_per_second"] = statistics.median(x["matching_statements"] / x["seconds"] for x in measured)
    if workload != "idle":
        for k in ("acquisition_statements", "transactions", "wal_bytes", "cpu_seconds"):
            summary[label][k + "_per_task"] = statistics.median(x[k] / x["completed_tasks"] for x in measured)

checks = {}
if not errors:
    if summary["baseline/4000/backlog"]["acquisition_statements_per_task"] <= 0:
        errors.append("baseline/4000/backlog: acquisition statements must be positive")
if not errors:
    checks["backlog_4000_statements_reduced_50pct"] = (
        summary["tuned/4000/backlog"]["acquisition_statements_per_task"] <=
        0.5 * summary["baseline/4000/backlog"]["acquisition_statements_per_task"])
    small, large = summary["tuned/400/idle"]["statements_per_second"], summary["tuned/4000/idle"]["statements_per_second"]
    checks["idle_400_to_4000_within_20pct"] = large <= 1.2 * small if small > 0 else large == 0
    for workload in ("burst", "backlog", "constrained"):
        base, tuned = summary[f"baseline/8/{workload}"], summary[f"tuned/8/{workload}"]
        checks[f"eight_lanes_{workload}_throughput"] = tuned["throughput"] >= 0.9 * base["throughput"]
        checks[f"eight_lanes_{workload}_p95"] = tuned["schedule_start_p95_ms"] <= 1.1 * base["schedule_start_p95_ms"]
result = {"complete": not errors, "errors": errors, "checks": checks,
          "all_gates_pass": bool(checks) and all(checks.values()), "summary": summary}
with open(args.output, "w", encoding="utf-8") as output:
    json.dump(result, output, indent=2)
print(json.dumps({"complete": result["complete"], "all_gates_pass": result["all_gates_pass"], "errors": errors}))
sys.exit(0 if result["all_gates_pass"] else 1)
