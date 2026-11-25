#!/usr/bin/env python3
"""Collect native A-D end-to-end benchmark PostgreSQL/cgroup snapshots and compare ABBA."""
import argparse
import json
import pathlib
import statistics
import subprocess

def shell(*args):
    return subprocess.check_output(args, text=True, timeout=25).strip()

def cgroup(container):
    def read(path):
        return shell("docker", "exec", container, "cat", path)
    cpu = {key:int(value) for key,value in
           (line.split() for line in read("/sys/fs/cgroup/cpu.stat").splitlines())}
    read_bytes=write_bytes=0
    for line in read("/sys/fs/cgroup/io.stat").splitlines():
        for token in line.split():
            if token.startswith("rbytes="):
                read_bytes+=int(token.partition("=")[2])
            elif token.startswith("wbytes="):
                write_bytes+=int(token.partition("=")[2])
    return {"cpuUsec":cpu["usage_usec"],"diskReadBytes":read_bytes,
            "diskWriteBytes":write_bytes}

SQL = """
SELECT json_build_object(
  'walBytes',(SELECT wal_bytes::text FROM pg_stat_wal),
  'walRecords',(SELECT wal_records FROM pg_stat_wal),
  'walWrites',(SELECT wal_write FROM pg_stat_wal),
  'walSyncs',(SELECT wal_sync FROM pg_stat_wal),
  'commits',(SELECT xact_commit FROM pg_stat_database WHERE datname=current_database()),
  'rollbacks',(SELECT xact_rollback FROM pg_stat_database WHERE datname=current_database()),
  'blockHits',(SELECT blks_hit FROM pg_stat_database WHERE datname=current_database()),
  'blockReads',(SELECT blks_read FROM pg_stat_database WHERE datname=current_database()),
  'deadlocks',(SELECT deadlocks FROM pg_stat_database WHERE datname=current_database()),
  'ioReads',(SELECT COALESCE(sum(reads),0) FROM pg_stat_io),
  'ioWrites',(SELECT COALESCE(sum(writes),0) FROM pg_stat_io),
  'ioFsyncs',(SELECT COALESCE(sum(fsyncs),0) FROM pg_stat_io)
)::text
"""
def snapshot(pg, hub, hub2):
    # Distinct one-shot PostgreSQL sessions; stats are monotonic within each
    # database volume. Both snapshots occur after fixture/warmup and before teardown.
    stats=json.loads(shell("docker","exec",pg,"psql","-X","-U","workflow","-d",
                           "workflow","-Atc",SQL))
    stats={key:int(value) for key,value in stats.items()}
    return {"postgres":cgroup(pg),"hub1":cgroup(hub),"hub2":cgroup(hub2),
            "pgStats":stats}

def delta(before,after):
    return {component:{k:after[component][k]-before[component][k]
                        for k in before[component]}
            for component in before}

def summarize(folder):
    stages=("off1","on1","on2","off2")
    reports=[]
    for stage in stages:
        result=json.loads((folder/f"{stage}.json").read_text())
        before=json.loads((folder/f"{stage}-before.json").read_text())
        after=json.loads((folder/f"{stage}-after.json").read_text())
        result["resourceDelta"]=delta(before,after)
        result["pgCpuMsPerWorkflow"]=round(result["resourceDelta"]["postgres"]["cpuUsec"]/1000/result["completed"],3)
        result["hubCpuMsPerWorkflow"]=round(
            (result["resourceDelta"]["hub1"]["cpuUsec"]+result["resourceDelta"]["hub2"]["cpuUsec"])/1000/result["completed"],3)
        result["walBytesPerWorkflow"]=round(result["resourceDelta"]["pgStats"]["walBytes"]/result["completed"],2)
        reports.append(result)
    off=[r for r in reports if not r["allNativeOptimizations"]]
    on=[r for r in reports if r["allNativeOptimizations"]]
    if len(off)!=2 or len(on)!=2:raise ValueError("Expected OFF/ON/ON/OFF")
    def avg(group,metric):return statistics.mean(float(r[metric]) for r in group)
    metrics=("throughputWorkflowsPerSec","workflowP95Ms","workflowP99Ms",
             "pgCpuMsPerWorkflow","hubCpuMsPerWorkflow","walBytesPerWorkflow",
             "durableHistoryEvents","extraTaskAttempts")
    comparison={}
    for metric in metrics:
        baseline=avg(off,metric);optimized=avg(on,metric)
        comparison[metric]={"off":round(baseline,3),"on":round(optimized,3),
            "changePercent":round(100*(optimized/baseline-1),2) if baseline else None}
    ratio=off[1]["throughputWorkflowsPerSec"]/off[0]["throughputWorkflowsPerSec"]
    out={"method":"4 cohorts OFF/ON/ON/OFF with fresh PostgreSQL volumes",
         "source":"1000 durable tenant-offboarding-fast workflows per cohort; two Hubs; 16 processors",
         "workloadCaveat":"Fast offboarding is local processor steps, so C's activity dispatch and B's subscriptions are NOT exercised by this E2E workload; use Phase B/C persistence benchmark as complementary evidence.",
         "controlDriftPercent":round(100*(ratio-1),2),
         "comparison":comparison,"stages":reports,
         "verdict":"review-required" if abs(100*(ratio-1))>20 else "comparable-runner-controls"}
    (folder/"comparison.json").write_text(json.dumps(out,indent=2)+"\n")
    rows=["# Native commit A-D: 1000-workflow end-to-end ABBA","","| Metric | OFF | ON | Change |",
          "|---|---:|---:|---:|"]
    for metric in metrics:
        m=comparison[metric]
        rows.append(f"| {metric} | {m['off']:,.3f} | {m['on']:,.3f} | " +
                    ("n/a" if m["changePercent"] is None else f"{m['changePercent']:+.1f}%")+" |")
    rows+=["",f"Baseline control drift: {out['controlDriftPercent']:+.1f}%.",
           "Includes submitted-to-completed workflow latency. PostgreSQL cgroup and WAL are per cohort.",
           out["workloadCaveat"],"Shared GitHub Actions runner: treat results as directional, not an SLO guarantee."]
    md="\n".join(rows)+"\n"
    (folder/"summary.md").write_text(md)
    print(md)
    return out

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("mode", choices=["snapshot","summarize"])
    parser.add_argument("--output")
    parser.add_argument("--postgres")
    parser.add_argument("--hub")
    parser.add_argument("--hub2")
    parser.add_argument("--folder")
    args=parser.parse_args()
    if args.mode=="snapshot":
        if not all((args.output,args.postgres,args.hub,args.hub2)):
            parser.error("snapshot requires output, postgres, hub, hub2")
        pathlib.Path(args.output).write_text(json.dumps(
            snapshot(args.postgres,args.hub,args.hub2),indent=2)+"\n")
    else:
        if not args.folder:parser.error("summarize requires --folder")
        summarize(pathlib.Path(args.folder))
if __name__=="__main__":main()
