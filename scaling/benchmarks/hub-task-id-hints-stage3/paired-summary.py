"""Create paired per-repetition runtime summaries without pooling latency tails."""
import json
import pathlib
import statistics

root = pathlib.Path(__file__).resolve().parent
records = json.loads((root / "raw-runtime-qualified/records.json").read_text())
samples = []
for row in records:
    if row["repetition"] == 0:
        continue
    workload = row["workload"]
    db, classes = row["database"][:2]
    by_class = {item["class"]: item for item in classes}
    task_count = workload["completedTasks"]
    def cls(name, metric):
        return by_class.get(name, {}).get(metric, 0)
    samples.append({
        "arm": row["arm"], "scenario": row["scenario"],
        "repetition": row["repetition"], "runId": row["load"].get("runId"),
        "workloadExit": row["workloadExit"], "metricsExit": row["metricsExit"],
        "completedWorkflows": workload["completedWorkflows"],
        "completedTasks": task_count, "durationSeconds": row["durationSeconds"],
        "endToEndP95Ms": workload["endToEndP95Ms"],
        "endToEndP99Ms": workload["endToEndP99Ms"],
        "scheduleStartP95Ms": workload["scheduleStartP95Ms"],
        "statementsPerTask": db["statements"] / task_count,
        "executionMsPerTask": db["executionMs"] / task_count,
        "sharedHitsPerTask": db["sharedHit"] / task_count,
        "sharedReadsPerTask": db["sharedRead"] / task_count,
        "walBytesPerTask": db["walBytes"] / task_count,
        "discoveryCalls": cls("candidate-discovery-page", "calls"),
        "discoveryRows": cls("candidate-discovery-page", "rows"),
        "hydrationCalls": cls("id-hydration", "calls"),
        "hydrationRows": cls("id-hydration", "rows"),
        "outboxProducerRows": cls("notification-outbox-producer", "rows"),
        "notifyProducerRows": cls("notification-notify-producer", "rows"),
        "retries": workload["tasksRetried"], "leaseExpiries": workload["leaseExpiries"],
        "deadTasks": workload["deadTasks"],
    })

summary = []
metrics = ("durationSeconds", "endToEndP95Ms", "endToEndP99Ms",
           "scheduleStartP95Ms", "statementsPerTask", "executionMsPerTask",
           "sharedHitsPerTask", "sharedReadsPerTask", "walBytesPerTask",
           "discoveryCalls", "discoveryRows", "hydrationCalls", "hydrationRows",
           "outboxProducerRows", "notifyProducerRows")
for scenario in sorted({row["scenario"] for row in samples}):
    arms = {arm: [row for row in samples if row["scenario"] == scenario and row["arm"] == arm]
            for arm in ("baseline", "candidate")}
    if any(len(values) != 5 for values in arms.values()):
        raise SystemExit(f"Expected five complete repetitions for {scenario}: "
                         f"{[len(v) for v in arms.values()]}")
    group = {"scenario": scenario, "repetitionsPerArm": 5, "arms": {}}
    for arm, rows in arms.items():
        group["arms"][arm] = {"samples": rows, "summary": {}}
        for metric in metrics:
            values = [row[metric] for row in rows]
            group["arms"][arm]["summary"][metric] = {
                "median": statistics.median(values), "min": min(values), "max": max(values)}
    paired = []
    for n in range(1, 6):
        b = next(row for row in arms["baseline"] if row["repetition"] == n)
        c = next(row for row in arms["candidate"] if row["repetition"] == n)
        paired.append({"repetition": n, "order": [r["arm"] for r in samples
                       if r["scenario"] == scenario and r["repetition"] == n],
                       **{m: {"candidateMinusBaseline": c[m] - b[m],
                              "candidateOverBaseline": c[m] / b[m] if b[m] else None}
                          for m in metrics if isinstance(c[m], (int, float))}})
    group["paired"] = paired
    summary.append(group)

out = {"sourceRecords": "raw-runtime-qualified/records.json",
       "method": "five paired interleaved repetitions per arm after one warmup per scenario and arm; latency summaries are per-repetition medians/ranges, not pooled tails",
       "groups": summary}
(root / "paired-summary.json").write_text(json.dumps(out, indent=2))
print(root / "paired-summary.json")
