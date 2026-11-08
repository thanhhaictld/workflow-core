#!/usr/bin/env python3
"""Same-runner OFF / ON / OFF comparison for worker shard-credit fanout.

No causal claims when controls drift; one hosted 4-vCPU runner is not a proof
of dedicated multi-host scaling. All stages must pass PostgreSQL ordering.
"""
import argparse
import json
from pathlib import Path
from statistics import mean

POLICIES = ("unordered", "partition", "keyed")
STAGES = ("off1", "on", "off2")

def load(folder, stage, policy):
    v = json.loads((folder / f"{stage}-{policy}.json").read_text())
    p = json.loads((folder / f"pressure-{stage}-{policy}.json").read_text())
    assert v["workflows"] == v["completed"] and v["failed"] == 0, v
    assert v.get("extraAttempts", 0) == 0, v
    assert p.get("samples", 0) > 0, p.get("errors", [])
    return v, p

def main():
    cli=argparse.ArgumentParser()
    cli.add_argument("--input",type=Path,required=True)
    cli.add_argument("--output",type=Path,required=True)
    args=cli.parse_args()
    report={
        "comparison":"worker shard-credit fanout OFF -> ON -> OFF, 8 processors",
        "limitations":[
            "Shared GitHub-hosted runner; not independent Hub/Postgres CPU budgets",
            "Sequential workload stages grow database history and alter caches",
            "OFF control drift >= 20% invalidates throughput improvement claims",
            "Worker-side credit reservations do not implement server push or a durable capacity registry",
            "A passing E2E suite proves ordering and persistence, not production latency SLOs"
        ],
        "policies":{},
    }
    lines=[
        "# Worker capacity-aware shard polling A/B",
        "",
        "| Policy | Stage | WF/s | Workflow p95 s | Workflow p99 s | Host CPU busy | PG commits/WF |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for policy in POLICIES:
        stages={}
        for stage in STAGES:
            verified,pressure=load(args.input,stage,policy)
            completed=verified["completed"]
            commits=(pressure.get("dbDelta") or {}).get("xact_commit")
            stages[stage]={
                "throughput":verified["throughputWorkflowsPerSecond"],
                "workflowP95Ms":verified["workflowP95Ms"],
                "workflowP99Ms":verified["workflowP99Ms"],
                "cpuBusyPercent":pressure.get("hostCpuBusyPercent"),
                "pgCommitsPerWorkflow":None if commits is None else round(commits/completed,3),
                "samples":pressure.get("samples"),
                "errors":pressure.get("errors",[]),
            }
            d=stages[stage]
            cpu="n/a" if d["cpuBusyPercent"] is None else f'{d["cpuBusyPercent"]:.1f}%'
            c="n/a" if d["pgCommitsPerWorkflow"] is None else f'{d["pgCommitsPerWorkflow"]:.1f}'
            lines.append(f'| {policy} | {stage} | {d["throughput"]:.2f} | '
                f'{d["workflowP95Ms"]/1000:.2f} | {d["workflowP99Ms"]/1000:.2f} | '
                f'{cpu} | {c} |')
        off=mean([stages["off1"]["throughput"],stages["off2"]["throughput"]])
        ratio=stages["on"]["throughput"]/off
        drift=(stages["off2"]["throughput"]/stages["off1"]["throughput"]-1)*100
        report["policies"][policy]={
            "stages":stages,
            "onOverMeanOff":round(ratio,3),
            "offDriftPercent":round(drift,1),
            "interpretation":"unstable-controls" if abs(drift)>=20 else (
                "regression" if ratio<0.9 else
                "no-convincing-gain" if ratio<1.1 else
                "possible-improvement-needs-repeat")
        }
    lines += [
        "",
        "| Policy | ON/mean OFF | OFF drift | Interpretation |",
        "|---|---:|---:|---|",
    ]
    for policy,r in report["policies"].items():
        lines.append(f'| {policy} | {r["onOverMeanOff"]:.2f}x | '
            f'{r["offDriftPercent"]:+.1f}% | {r["interpretation"]} |')
    lines += ["","**No flag promotion without stable controls, p95/p99, "
              "HA/failover checks and repeated dedicated-resource measurements.**"]
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+"\n")
    md="\n".join(lines)+"\n"
    args.output.with_suffix(".md").write_text(md)
    print(md)

if __name__=="__main__":
    main()
