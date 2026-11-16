"""Summarize all raw paired runtime repetitions without hiding failures."""
import argparse
import json
import pathlib
import statistics

parser = argparse.ArgumentParser()
parser.add_argument("records", type=pathlib.Path)
args = parser.parse_args()
records = json.loads(args.records.read_text(encoding="utf-8"))
groups = {}
for row in records:
    if row.get("repetition") == 0:
        continue
    groups.setdefault((row["arm"], row["scenario"]), []).append(row)

summary = []
for (arm, scenario), reps in sorted(groups.items()):
    successful = [r for r in reps if r.get("workloadExit") == 0 and
                  r.get("metricsExit") == 0 and r.get("workload", {}).get("completedWorkflows") ==
                  r.get("workload", {}).get("workflowCount")]
    tasks = sum(r.get("workload", {}).get("taskCount", 0) for r in successful)
    durations = [r["durationSeconds"] for r in successful if r.get("durationSeconds") is not None]
    total_db = [r.get("database", [{}])[0] for r in successful]
    classes = {}
    for record in successful:
        db = record.get("database", [{}, []])
        class_rows = db[1] if len(db) > 3 and isinstance(db[1], list) else []
        for entry in class_rows:
            target = classes.setdefault(entry["class"], {"calls": 0, "rows": 0,
                "executionMs": 0.0, "sharedHit": 0, "sharedRead": 0, "walBytes": 0})
            for key in target:
                target[key] += entry.get(key, 0)
    all_latencies = sorted(latency for r in successful
                           for item in r.get("load", {}).get("items", [])
                           if (latency := item.get("elapsed_ms")) is not None)
    throughputs = [r["workload"].get("taskCount", 0) / r["durationSeconds"]
                   for r in successful if r.get("durationSeconds")]
    def percentile(values, p):
        if not values:
            return None
        index = min(len(values) - 1, max(0, int((len(values) - 1) * p)))
        return values[index]
    summary.append({
        "arm": arm, "scenario": scenario, "plannedRepetitions": len(reps),
        "successfulRepetitions": len(successful),
        "failedRuns": [{"repetition": r.get("repetition"),
                        "workloadExit": r.get("workloadExit"),
                        "metricsExit": r.get("metricsExit"),
                        "completedWorkflows": r.get("workload", {}).get("completedWorkflows"),
                        "requestedWorkflows": r.get("workload", {}).get("workflowCount")}
                       for r in reps if r not in successful],
        "completedTasks": tasks,
        "throughputTasksPerSecondMedian": statistics.median(throughputs) if throughputs else None,
        "durationSecondsMedian": statistics.median(durations) if durations else None,
        "durationSecondsRange": [min(durations), max(durations)] if durations else None,
        "workflowElapsedMsP50P95P99": {
            "p50": statistics.median(all_latencies) if all_latencies else None,
            "p95": percentile(all_latencies, .95),
            "p99": percentile(all_latencies, .99), "samples": len(all_latencies)},
        "databaseTotals": {key: sum(row.get(key, 0) for row in total_db)
                           for key in ("statements", "executionMs", "sharedHit", "sharedRead",
                                       "sharedDirtied", "sharedWritten", "sharedReadMs",
                                       "sharedWriteMs", "walRecords", "walBytes")},
        "databasePerDurablyCompletedTask": {
            key: (sum(row.get(key, 0) for row in total_db) / tasks if tasks else None)
            for key in ("statements", "executionMs", "sharedHit", "sharedRead",
                        "sharedDirtied", "sharedWritten", "sharedReadMs", "sharedWriteMs",
                        "walRecords", "walBytes")},
        "queryClassTotals": classes,
    })

output = args.records.with_name("summary.json")
output.write_text(json.dumps({"sourceRecords": args.records.name, "groups": summary}, indent=2),
                  encoding="utf-8")
print(output)
