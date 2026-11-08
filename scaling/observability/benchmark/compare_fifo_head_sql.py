#!/usr/bin/env python3
"""FIFO-head SQL A/B: off1 -> on -> off2, same runner and persistent DB.

This report compares a bracketing control, not randomized repeated production
experiments. Never infer safety from performance: verify_ordering runs first.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
from statistics import mean

POLICIES = ("partition", "keyed")
STAGES = ("off1", "on", "off2")
FAMILIES = ("claim_candidate_window", "claim_row_lock", "policy_admission",
            "task_queue", "matching_owner")

def fmt(v, digits=2):
    return "n/a" if v is None else f"{v:.{digits}f}"

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    report = {"policies": {}, "method": "off1 -> on -> off2, same runner, 8 processors",
              "limitations": [
                  "database state and queue history increase across stages",
                  "profiling SQL wall time is not CPU time",
                  "8-processor controls quantify drift, not elimination of drift",
                  "shared runner has no isolated CPU budgets",
                  "a single run cannot justify turning feature on by default",
              ]}
    table = [
        "# FIFO head prefilter cost / benefit",
        "",
        "| Policy | Stage | WF/s | Workflow p95 s | Candidate SQL calls/WF | Policy SQL calls/WF | Policy SQL wall ms/WF | PG CPU cores |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for mode in POLICIES:
        rows = {}
        for stage in STAGES:
            base = args.input
            verify = json.loads((base / f"{stage}-{mode}.json").read_text())
            profile = json.loads((base / f"profile-{stage}-{mode}.json").read_text())
            assert verify["completed"] == verify["workflows"] and verify["failed"] == 0, verify
            assert verify.get("extraAttempts", 0) == 0, verify
            assert profile.get("cpuSampleCount", 0) > 0
            assert profile.get("pgStatementSamples"), f"No pg_stat_statements output for {stage}/{mode}"
            completed = verify["completed"]
            families = profile.get("pgSqlFamilyTotals") or {}
            policy = families.get("policy_admission") or {}
            candidate = families.get("claim_candidate_window") or {}
            throughput = float(verify["throughputWorkflowsPerSecond"])
            rows[stage] = {
                "workflows": completed,
                "throughput": throughput,
                "p95Seconds": verify["workflowP95Ms"] / 1000,
                "p99Seconds": verify["workflowP99Ms"] / 1000,
                "sqlPerWorkflow": {
                    family: {"calls": round((families.get(family) or {}).get("calls", 0)/completed, 3),
                             "execWallMs": round((families.get(family) or {}).get("executionWallMs", 0)/completed, 3)}
                    for family in FAMILIES
                },
                "pgCpuCores": (profile.get("averageCpuCoresByService") or {}).get("postgres"),
                "hubCpuCores": sum((profile.get("averageCpuCoresByService") or {}).get(k, 0)
                                   for k in ("hub", "hub-2")),
                "processorCpuCores": (profile.get("averageCpuCoresByService") or {}).get("processor"),
                "sqlTopStatements": profile.get("pgStatementSamples", [])[:10],
                "errors": profile.get("errors", []),
            }
            line = [
                mode,stage,fmt(throughput),fmt(verify["workflowP95Ms"]/1000),
                fmt(candidate.get("calls", 0)/completed),
                fmt(policy.get("calls", 0)/completed),
                fmt(policy.get("executionWallMs", 0)/completed),
                fmt(rows[stage]["pgCpuCores"])
            ]
            table.append("| "+" | ".join(line)+" |")
        control_wf = mean([rows["off1"]["throughput"], rows["off2"]["throughput"]])
        control_policy_calls = mean([
            rows["off1"]["sqlPerWorkflow"]["policy_admission"]["calls"],
            rows["off2"]["sqlPerWorkflow"]["policy_admission"]["calls"]
        ])
        gain = rows["on"]["throughput"] / control_wf if control_wf else None
        drift = (rows["off2"]["throughput"] / rows["off1"]["throughput"] - 1) * 100
        report["policies"][mode] = {
            "stages": rows,
            "onVsMeanOffThroughputRatio": None if gain is None else round(gain, 3),
            "offControlDriftPercent": round(drift, 1),
            "onVsMeanOffPolicyCallsRatio": (round(rows["on"]["sqlPerWorkflow"]["policy_admission"]["calls"] /
               control_policy_calls, 3) if control_policy_calls else None),
            "stableControls": abs(drift) < 20
        }
    table += [
        "",
        "## Bracketed outcome",
        "",
        "| Policy | ON throughput / mean OFF | OFF control drift | ON policy calls / mean OFF |",
        "|---|---:|---:|---:|",
    ]
    for mode in POLICIES:
        item = report["policies"][mode]
        table.append(f"| {mode} | {fmt(item['onVsMeanOffThroughputRatio'])}x | "
                     f"{item['offControlDriftPercent']:+.1f}% | "
                     f"{fmt(item['onVsMeanOffPolicyCallsRatio'])}x |")
    table += [
        "",
        "**Keep FIFO prefilter OFF by default.** Investigate query plans and repeat"
        " on dedicated resources if the results are promising. Regressions and"
        " correctness failures block promotion.",
    ]
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+"\n")
    markdown="\n".join(table)+"\n"
    args.output.with_suffix(".md").write_text(markdown)
    print(markdown)

if __name__=="__main__":
    main()
