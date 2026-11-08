#!/usr/bin/env python3
"""Compute 8->16->8 processor scaling from real completed workflow E2E reports.

Use bracketing 8-worker controls to expose cache warming and runner drift.
Do NOT fail CI for sublinear throughput; that is a legitimate benchmark result.
"""
import json
import os
import pathlib
import statistics

root = pathlib.Path(os.environ.get(
    "POLICY_SCALING_RESULT_DIR", "scaling/observability/results/policy-scaling"))
modes = ("unordered", "partition", "keyed")
report = {
    "method": "same-runner 8 -> 16 -> 8 processor bracketing",
    "metric": "server-created to last-completed window; workflow latency includes queue waiting",
    "expectedIdealSpeedup": 2.0,
    "runnerVcpus": os.cpu_count(),
    "notes": [
        "same 1000-workflow payload, 60 k6 VUs, same Hub, PostgreSQL, workload and per-processor settings",
        "sequential stages can be affected by shared-runner CPU, warmed caches, database growth, and changing backlog",
        "partition FIFO has 8 partition admission permits; keyed FIFO has 8 resource admission permits",
        "16 worker processes alone cannot imply linear scaling when Hub/DB/permit bottlenecks dominate",
        "do not interpret one bracketing comparison as a production scalability SLA",
    ],
    "modes": {},
}
for mode in modes:
    data = {stage: json.loads((root / f"{stage}-{mode}.json").read_text())
            for stage in ("8a", "16", "8b")}
    for stage, row in data.items():
        assert row["mode"] == mode, (stage, row)
        assert row["workflows"] == row["completed"] and row["failed"] == 0, row
        assert row["activityAttempts"] == row["workflows"], row
        if mode == "unordered":
            assert row["activityConstraints"] == 0 and row["overlappingLeasesWithinPolicyScope"] > 0
        else:
            assert row["activityConstraints"] == row["workflows"]
            assert row["overlappingLeasesWithinPolicyScope"] == 0
    controls = [data["8a"]["throughputWorkflowsPerSecond"],
                data["8b"]["throughputWorkflowsPerSecond"]]
    baseline = statistics.mean(controls)
    treatment = data["16"]["throughputWorkflowsPerSecond"]
    speedup = treatment / baseline
    drift_pct = (controls[1] / controls[0] - 1) * 100
    efficiency = 100 * speedup / 2
    report["modes"][mode] = {
        "eightProcessorsFirst": data["8a"],
        "sixteenProcessors": data["16"],
        "eightProcessorsRepeat": data["8b"],
        "eightProcessorThroughputMean": round(baseline, 3),
        "sixteenProcessorThroughput": treatment,
        "throughputSpeedup": round(speedup, 3),
        "scalingEfficiencyPercent": round(efficiency, 1),
        "eightControlDriftPercent": round(drift_pct, 1),
        "p95DeltaPercent": round(
            (data["16"]["workflowP95Ms"] / statistics.mean(
                [data["8a"]["workflowP95Ms"], data["8b"]["workflowP95Ms"]]) - 1) * 100, 1),
        "p99DeltaPercent": round(
            (data["16"]["workflowP99Ms"] / statistics.mean(
                [data["8a"]["workflowP99Ms"], data["8b"]["workflowP99Ms"]]) - 1) * 100, 1),
        "scalingAssessment": (
            "control-drift-too-high" if abs(drift_pct) > 20 else
            "near-linear" if speedup >= 1.8 else
            "sublinear" if speedup > 1 else "no-speedup"
        ),
    }
(root / "comparison.json").write_text(json.dumps(report, indent=2) + "\n")
print(json.dumps(report, indent=2))
print("PASS: all 9 mode/stage runs completed with verified admission policy correctness")
