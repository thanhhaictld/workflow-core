#!/usr/bin/env python3
"""Diagnose activity lease occupancy from exact Hub -> worker -> HTTP -> DB OTLP traces.

Correlates per-workflow IDs from the durable database timelines. Native
workflow acknowledgements are deliberately excluded from activity ACK counts.
Keeps span attributes on traces (not as Prometheus metric labels).
"""
import argparse
import collections
import json
import math
import statistics
from pathlib import Path

def percentile(values, pct):
    vals=sorted(values)
    if not vals:return None
    k=(len(vals)-1)*pct
    lo,hi=math.floor(k),math.ceil(k)
    return round(vals[lo]+(vals[hi]-vals[lo])*(k-lo),6)

def stats(vals):
    return {"count":len(vals),"meanSeconds":round(statistics.mean(vals),6) if vals else None,
            "p50Seconds":percentile(vals,.5),"p95Seconds":percentile(vals,.95),
            "p99Seconds":percentile(vals,.99),"maxSeconds":round(max(vals),6) if vals else None}

def attrs(items):
    return {x["key"]:next(iter(x.get("value",{}).values()),None) for x in items or []}

p=argparse.ArgumentParser()
p.add_argument("--traces",required=True)
p.add_argument("--results",required=True)
p.add_argument("--output",required=True)
args=p.parse_args()
root=Path(args.results)
runs={}
ids_to_run={}
for processors in (8,16):
    for mode in ("unordered","partition","keyed"):
        key=f"{processors}-{mode}"
        report=json.loads((root/f"timeline-{key}.json").read_text())
        runs[key]=report
        for workflow in report["workflowTimelines"]:
            ids_to_run[workflow["workflow_id"]]=key

# Only a small number of span types are needed in memory. For the HTTP
# server operation, url.path (not normalized route) identifies the task ID.
by_trace=collections.defaultdict(list)
tagged=collections.defaultdict(list)
line_count=0
span_count=0
with open(args.traces) as fp:
    for line in fp:
        if not line.strip():continue
        line_count+=1
        item=json.loads(line)
        for resource in item.get("resourceSpans",[]):
            service=attrs(resource.get("resource",{}).get("attributes")).get("service.name","unknown")
            for scope in resource.get("scopeSpans",[]):
                for sp in scope.get("spans",[]):
                    span_count+=1
                    att=attrs(sp.get("attributes"))
                    workflow=att.get("workflow.id")
                    key=ids_to_run.get(workflow)
                    name=sp.get("name","")
                    if not key and name not in ("hub.task.claim","hub.activity.db_commit") and not (
                        name.startswith("POST /api/hub/tasks/") and name.endswith("/complete")):
                        continue
                    first=int(sp.get("startTimeUnixNano",0))/1e9
                    last=int(sp.get("endTimeUnixNano",0))/1e9
                    entry={"name":name,"service":service,"start":first,"end":last,
                           "seconds":max(0,last-first),"traceId":sp.get("traceId"),
                           "spanId":sp.get("spanId"),"parentSpanId":sp.get("parentSpanId"),
                           "workflowId":workflow,"taskId":att.get("task.id"),
                           "path":att.get("url.path"),
                           "candidatesScanned":att.get("tasks.candidates_scanned"),
                           "events":{e["name"]:attrs(e.get("attributes")) for e in sp.get("events",[])}}
                    by_trace[entry["traceId"]].append(entry)
                    if key:tagged[key].append(entry)

result={"source":args.traces,"otlpLines":line_count,"totalSpans":span_count,"runs":{}}
problems=[]
for key,run in runs.items():
    metrics=collections.defaultdict(list)
    slow_ids={r["workflow_id"] for r in run["slowestWorkflows"][:10]}
    slowest=[]
    processed=0
    joined=0
    for w in run["workflowTimelines"]:
        wid=w["workflow_id"]
        activity=next((x for x in tagged[key] if x["workflowId"]==wid
                       and x["name"]=="hub.worker.activity.execute"),None)
        if activity is None:
            problems.append(f"{key}/{wid}: missing activity span")
            continue
        group=by_trace[activity["traceId"]]
        task=activity["taskId"]
        def find(name, pred=None):
            return next((x for x in group if x["name"]==name
                         and (pred is None or pred(x))),None)
        body=find("hub.worker.activity.body",lambda s:s["taskId"]==task)
        ack=find("hub.worker.activity.completion_http",lambda s:s["taskId"]==task)
        store=find("hub.activity.complete",lambda s:s["taskId"]==task)
        server=find("POST /api/hub/tasks/{id:guid}/complete",
                    lambda s:s["path"]==f"/api/hub/tasks/{task}/complete")
        commit=find("hub.activity.db_commit",
                    lambda s:store is not None and s["parentSpanId"]==store["spanId"])
        claim=find("hub.task.claim",lambda s:s["spanId"]==activity["parentSpanId"])
        if any(x is None for x in (body,ack,store,server,commit)):
            absent=[n for n,v in (("body",body),("ack",ack),("store",store),
                                 ("server",server),("commit",commit)) if v is None]
            problems.append(f"{key}/{wid}: missing {absent}")
            continue
        processed+=1
        joined+=int(ack["traceId"]==store["traceId"])
        event=store["events"]
        pool=float(event.get("db.pool.acquired",{}).get("duration.seconds",0))
        wf_lock=float(event.get("db.workflow_row.locked",{}).get("duration.seconds",0))
        task_lock=float(event.get("db.task_row.locked",{}).get("duration.seconds",0))
        stages={
            "activityBody":body["seconds"],
            "completionHttp":ack["seconds"],
            "httpBeforeStore":max(0,store["start"]-server["start"]),
            "hubCompletionStore":store["seconds"],
            "dbConnectionPoolWait":pool,
            "dbWorkflowRowLockQuery":wf_lock,
            "dbTaskRowLockQuery":task_lock,
            "dbCommit":commit["seconds"],
            "httpNetworkRemainder":max(0,ack["seconds"]-server["seconds"]),
        }
        if claim:
            stages["claimRequest"]=claim["seconds"]
            metrics["claimCandidateScanCount"].append(float(claim["candidatesScanned"] or 0))
        for name,v in stages.items():metrics[name].append(v)
        if wid in slow_ids:
            slowest.append({"workflowId":wid,"workflowSeconds":w["total_s"],
                            "activityQueueSeconds":w["activity_queue_s"],
                            "traceId":activity["traceId"],
                            "claimCandidatesScanned":claim["candidatesScanned"] if claim else None,
                            "stagesSeconds":{k:round(v,6) for k,v in stages.items()}})
    summary={"workflowCount":run["workflows"],"joinedActivityTraces":joined,
             "tracesWithCompleteBreakdown":processed,
             "operationDurations":{n:stats(v) for n,v in metrics.items()},
             "slowestWorkflows":sorted(slowest,key=lambda x:x["workflowSeconds"],reverse=True)}
    result["runs"][key]=summary
    if processed!=100 or joined!=100:
        problems.append(f"{key}: expected 100 correlated traces, got {processed} / {joined}")
    print(key, "correlated",processed,"HTTP joined",joined,
          "body",summary["operationDurations"].get("activityBody"),
          "ACK",summary["operationDurations"].get("completionHttp"),
          "pool",summary["operationDurations"].get("dbConnectionPoolWait"),
          "pre-store",summary["operationDurations"].get("httpBeforeStore"))
result["errors"]=problems[:100]
Path(args.output).write_text(json.dumps(result,indent=2)+"\n")
if problems:
    raise SystemExit("OTEL FORENSIC COVERAGE FAILED: "+"; ".join(problems[:10]))
print("PASS: 600 traced activities, complete worker->Hub HTTP and DB time breakdown")
