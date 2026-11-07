#!/usr/bin/env python3
"""Compare 8a -> 16 -> 8b cohorts on the same runner.

Analysis is observational: hosted CI CPU, shared resources, warmup and load
variance can dominate. Never claim linear processor scaling from this report.
"""
from __future__ import annotations
import argparse
import json
import statistics
from pathlib import Path

STAGES = ("8a", "16", "8b")
POLICIES = ("unordered", "partition", "keyed")

def load(input_dir: Path, stage: str, policy: str):
    verify = json.loads((input_dir / f"{stage}-{policy}.json").read_text())
    pressure = json.loads((input_dir / f"pressure-{stage}-{policy}.json").read_text())
    assert verify["completed"] == verify["workflows"] and verify["failed"] == 0
    assert verify.get("extraAttempts", 0) == 0
    return verify, pressure

def safe_ratio(numer: float | None, denom: float | None) -> float | None:
    return round(numer / denom, 3) if numer is not None and denom and denom > 0 else None

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", required=True)
    args = parser.parse_args()
    report = {
        "variant": args.mode,
        "runnerVcpus": None,
        "notes": [
            "same-runner 8 -> 16 -> 8 bracket for drift, not a controlled hardware scale-out proof",
            "host CPU includes k6, PostgreSQL, both Hubs and all processor containers",
            "database and WAL counters include background work",
            "2-second wait sampling cannot rule out transient locks or pool waits",
            "FIFO ordering limits throughput by design, independent of processor count"
        ],
        "policies": {},
    }
    lines = [
        f"### Demand/capacity diagnosis: {args.mode}",
        "| Policy | 8a WF/s | 16 WF/s | 8b WF/s | 16/8-mean | 8-stage drift | 16 CPU busy | 16 DB commits/WF | 16 WAL KB/WF |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for policy in POLICIES:
        raw = {stage: load(args.input, stage, policy) for stage in STAGES}
        if report["runnerVcpus"] is None:
            report["runnerVcpus"] = raw["16"][1].get("cpuCount")
        throughput = {stage: float(raw[stage][0]["throughputWorkflowsPerSecond"])
                      for stage in STAGES}
        controls = [throughput["8a"], throughput["8b"]]
        mean8 = statistics.mean(controls)
        speedup = safe_ratio(throughput["16"], mean8)
        drift = round(100 * (controls[1] / controls[0] - 1), 1)
        def workload_db(stage: str):
            verified, pressure = raw[stage]
            completed = verified["completed"]
            db = pressure.get("dbDelta") or {}
            wal = pressure.get("walDelta") or {}
            return {
                "hostCpuBusyPercent": pressure.get("hostCpuBusyPercent"),
                "pgWaitBackendSamples": pressure.get("sampledPgWaitBackendCounts", {}),
                "postgresDbCommitsPerWorkflow": safe_ratio(db.get("xact_commit"), completed),
                "postgresWalKilobytesPerWorkflow": safe_ratio(wal.get("wal_bytes"), completed * 1024),
                "postgresRowsUpdatedPerWorkflow": safe_ratio(db.get("tup_updated"), completed),
                "diagnosticsSamples": pressure.get("samples", 0),
                "samplerErrors": pressure.get("errors", []),
            }
        measurements = {stage: workload_db(stage) for stage in STAGES}
        report["policies"][policy] = {
            "completedByStage": {stage: raw[stage][0]["completed"] for stage in STAGES},
            "throughputWorkflowsSecond": throughput,
            "bracketed8Mean": round(mean8, 3),
            "speedup16Over8": speedup,
            "eightProcessorDriftPercent": drift,
            "p95Milliseconds": {stage: raw[stage][0]["workflowP95Ms"] for stage in STAGES},
            "p99Milliseconds": {stage: raw[stage][0]["workflowP99Ms"] for stage in STAGES},
            "resourceMeasurements": measurements,
            "interpretation": (
                "high-control-drift" if abs(drift) >= 20 else
                "throughput-decreases-at-16" if speedup is not None and speedup < 0.9 else
                "no-near-linear-gain" if speedup is not None and speedup < 1.8 else
                "near-linear-observed"
            ),
        }
        cpu = measurements["16"]["hostCpuBusyPercent"]
        commits = measurements["16"]["postgresDbCommitsPerWorkflow"]
        wal = measurements["16"]["postgresWalKilobytesPerWorkflow"]
        fmt = lambda v: "n/a" if v is None else f"{v:.2f}"
        lines.append(
            f"| {policy} | {throughput['8a']:.2f} | {throughput['16']:.2f} | "
            f"{throughput['8b']:.2f} | {fmt(speedup)} | {drift:+.1f}% | "
            f"{fmt(cpu)}% | {fmt(commits)} | {fmt(wal)} |"
        )
    lines.append("")
    lines.append(
        "**Caution:** A green benchmark proves only completed workloads and "
        "ordering checks. Inspect drift, resource contention, and repeated "
        "dedicated-host runs before changing production defaults."
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    markdown = "\n".join(lines) + "\n"
    args.output.with_suffix(".md").write_text(markdown)
    print(markdown)

if __name__ == "__main__":
    main()
