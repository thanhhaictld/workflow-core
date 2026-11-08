#!/usr/bin/env python3
"""Summarize SQL family + per-container CPU cost for a 8a-16-8b benchmark.

Must not conclude causality when CI runner drift is large. All activity
ordering and durable workflow completion checks are upstream of this script.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from statistics import mean

STAGES = ("8a", "16", "8b")
POLICIES = ("unordered", "partition", "keyed")


def read(input_dir: Path, stage: str, policy: str) -> dict:
    verified = json.loads((input_dir / f"{stage}-{policy}.json").read_text())
    profile = json.loads((input_dir / f"profile-{stage}-{policy}.json").read_text())
    assert verified["workflows"] == verified["completed"] and verified["failed"] == 0, verified
    assert int(verified.get("extraAttempts", 0)) == 0, verified
    assert profile.get("cpuSampleCount", 0) > 0, profile.get("errors", [])
    assert len(profile.get("pgStatementSamples", [])) > 0, "No pg_stat_statements data"
    return {"verify": verified, "profile": profile}


def roundn(value, places=2):
    return None if value is None else round(value, places)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    report = {
        "scope": "One 4-vCPU GitHub-hosted runner; same runner 8a/16/8b controls; "
                 "two Hubs, one PostgreSQL, policy-only workers; statement profiling enabled",
        "methodLimits": [
            "pg_stat_statements execution wall milliseconds are not database CPU time",
            "SQL family grouping is heuristic and may mix multiple statement shapes",
            "Docker stats sampling has observer overhead",
            "8a/16/8b cohort sequence can cause caching and database growth drift",
            "PostgreSQL/Hub/processor still share the runner and are not dedicated machines",
            "p95/p99 and ordering correctness are mandatory even if SQL cost decreases"
        ],
        "policies": {},
    }
    markdown = [
        "# Workflow bottleneck attribution",
        "",
        "| Policy | Stage | Throughput WF/s | p95 s | PG CPU cores | Hub CPU cores | Processor CPU cores | Matching owner SQL calls/WF | All tracked SQL calls/WF | Tracked SQL wall ms/WF |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for policy in POLICIES:
        data = {stage: read(args.input, stage, policy) for stage in STAGES}
        throughputs = {}
        for stage in STAGES:
            verified, profile = data[stage]["verify"], data[stage]["profile"]
            completed = verified["completed"]
            t = float(verified["throughputWorkflowsPerSecond"])
            throughputs[stage] = t
            cpu = profile.get("averageCpuCoresByService") or {}
            sql = profile.get("pgStatementSamples") or []
            owner = profile.get("pgSqlFamilyTotals", {}).get("matching_owner", {})
            total_calls = sum(s["calls"] for s in sql)
            # Profile stores only top 50 rows for artifacts; totals span
            # up to 1000 statement fingerprints, so prefer family totals.
            families = profile.get("pgSqlFamilyTotals") or {}
            total_calls = sum(v["calls"] for v in families.values())
            total_wall = sum(v["executionWallMs"] for v in families.values())
            hub_cpu = cpu.get("hub", 0) + cpu.get("hub-2", 0)
            postgres_cpu = cpu.get("postgres")
            processor_cpu = cpu.get("processor")
            cells = [
                policy, stage, f"{t:.2f}", f"{verified['workflowP95Ms']/1000:.2f}",
                "n/a" if postgres_cpu is None else f"{postgres_cpu:.2f}",
                f"{hub_cpu:.2f}",
                "n/a" if processor_cpu is None else f"{processor_cpu:.2f}",
                f"{owner.get('calls', 0)/completed:.2f}",
                f"{total_calls/completed:.2f}", f"{total_wall/completed:.1f}",
            ]
            markdown.append("| " + " | ".join(cells) + " |")
        control = mean([throughputs["8a"], throughputs["8b"]])
        speedup = throughputs["16"] / control
        drift = (throughputs["8b"] / throughputs["8a"] - 1) * 100
        report["policies"][policy] = {
            "throughput": {k: roundn(v, 3) for k,v in throughputs.items()},
            "speedup16OverBracketed8": roundn(speedup, 3),
            "eightProcessorDriftPercent": roundn(drift, 1),
            "interpretation": "high-control-drift" if abs(drift) >= 20 else (
                "regresses-at-16" if speedup < 0.9 else
                "sublinear" if speedup < 1.8 else "near-linear-observation"
            ),
            "sqlFamilyAt16": data["16"]["profile"].get("pgSqlFamilyTotals"),
            "cpuCoresAt16": data["16"]["profile"].get("averageCpuCoresByService"),
            "topStatementsAt16": data["16"]["profile"].get("pgStatementSamples", [])[:15],
            "observationErrorsAt16": data["16"]["profile"].get("errors", []),
        }
    markdown.extend([
        "",
        "## Bracketed 8 → 16 → 8 speedup",
        "",
        "| Workload | 16 / mean(8a,8b) | 8-control drift | Assessment |",
        "|---|---:|---:|---|",
    ])
    for name, result in report["policies"].items():
        markdown.append(
            f"| {name} | {result['speedup16OverBracketed8']:.2f}× | "
            f"{result['eightProcessorDriftPercent']:+.1f}% | "
            f"{result['interpretation']} |"
        )
    markdown.extend([
        "",
        "**Do not enable optimizations from this run alone.** The experiment "
        "attributes where CPU and SQL cost accumulate, not how the engine scales "
        "on isolated infrastructure.",
    ])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+"\n")
    md = "\n".join(markdown)+"\n"
    args.output.with_suffix(".md").write_text(md)
    print(md)


if __name__ == "__main__":
    main()
