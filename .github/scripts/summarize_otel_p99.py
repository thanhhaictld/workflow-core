#!/usr/bin/env python3
"""Summarize captured OTLP/JSONL spans by the exact workflow IDs from the SQL report.

The distributed trace links Hub claim -> remote worker -> completion HTTP ->
Hub completion. A workflow's durable queue gaps are measured by SQL, not by
pretending that a long-lived workflow can be one continuously open span.
"""
import argparse
import collections
import json
import math
from pathlib import Path
import statistics

def p(values, pct):
    nums=sorted(values)
    if not nums:return None
    pos=(len(nums)-1)*pct
    lo=math.floor(pos)
    hi=math.ceil(pos)
    return round(nums[lo]+(nums[hi]-nums[lo])*(pos-lo),6)

def attrs(items):
    result={}
    for item in items or []:
        v=item.get("value",{})
        result[item.get("key")]=next(iter(v.values()),None)
    return result

parser=argparse.ArgumentParser()
parser.add_argument("--traces",required=True)
parser.add_argument("--results",required=True)
parser.add_argument("--output",required=True)
args=parser.parse_args()

root=Path(args.results)
runs={}
ids_to_run={}
for processors in (8,16):
    for mode in ("unordered","partition","keyed"):
        key=f"{processors}-{mode}"
        file=root/f"timeline-{key}.json"
        run=json.loads(file.read_text())
        runs[key]=run
        for w in run["workflowTimelines"]:
            ids_to_run[w["workflow_id"]]=key

spans=collections.defaultdict(list)
per_workflow=collections.defaultdict(list)
line_count=0
span_count=0
with Path(args.traces).open() as src:
    for line in src:
        if not line.strip():continue
        line_count+=1
        record=json.loads(line)
        for resource in record.get("resourceSpans",[]):
            service=attrs(resource.get("resource",{}).get("attributes")) .get("service.name","unknown")
            for scope in resource.get("scopeSpans",[]):
                for item in scope.get("spans",[]):
                    span_count+=1
                    att=attrs(item.get("attributes"))
                    workflow_id=att.get("workflow.id")
                    key=ids_to_run.get(workflow_id)
                    if key is None:continue
                    begin=int(item.get("startTimeUnixNano",0))
                    end=int(item.get("endTimeUnixNano",0))
                    duration=max(0,(end-begin)/1e9)
                    entry={"name":item.get("name","unknown"),"service":service,
                           "durationSeconds":round(duration,6),
                           "traceId":item.get("traceId"),"parentSpanId":item.get("parentSpanId"),
                           "spanId":item.get("spanId"),
                           "workflowId":workflow_id}
                    spans[key].append(entry)
                    per_workflow[workflow_id].append(entry)

summaries={}
for key,run in runs.items():
    matching=spans[key]
    grouped=collections.defaultdict(list)
    for s in matching:grouped[s["name"]].append(s["durationSeconds"])
    perf={name:{"count":len(v),"meanSeconds":round(statistics.mean(v),6),
                "p50Seconds":p(v,.5),"p95Seconds":p(v,.95),"p99Seconds":p(v,.99),
                "maxSeconds":round(max(v),6)}
          for name,v in sorted(grouped.items())}
    # A 100-workflow benchmark must show one body, HTTP completion and Hub
    # completion per activity. Missing spans are a failed diagnostic, not
    # evidence of zero latency.
    required=["hub.worker.activity.body","hub.worker.completion_http",
              "hub.activity.complete"]
    missing={name:100-len(grouped[name]) for name in required}
    top=[]
    for w in run["slowestWorkflows"][:10]:
        timeline=sorted(per_workflow[w["workflow_id"]],
                        key=lambda v:v["durationSeconds"],reverse=True)
        top.append({"workflowId":w["workflow_id"],"workflowSeconds":w["total_s"],
                    "activityQueueSeconds":w["activity_queue_s"],
                    "largestSpans":timeline[:15],
                    "workerToHubJoined":any(
                        a["traceId"]==b["traceId"] for a in timeline for b in timeline
                        if a["name"]=="hub.worker.activity.execute"
                        and b["name"]=="hub.activity.complete")})
    summaries[key]={"tracedWorkflows":len({
        s["workflowId"] for s in matching}),"spanCount":len(matching),
        "missingRequiredSpans":missing,"operationDurations":perf,
        "slowestWorkflows":top}
    print(key, "spans",len(matching),"trace coverage",summaries[key]["tracedWorkflows"])
    print("HOT SPANS", {name:perf[name] for name in required if name in perf})
    print("MISSING",missing)
result={"source":str(args.traces),"otlpLines":line_count,"totalSpans":span_count,
        "correlatedSpans":sum(len(v) for v in spans.values()),
        "runs":summaries}
Path(args.output).write_text(json.dumps(result,indent=2)+"\n")
# Require real cross-process tracing on this short, fully sampled diagnostic.
assert all(not any(v!=0 for v in item["missingRequiredSpans"].values())
           for item in summaries.values()),"Missing activity or Hub completion spans"
assert all(all(w["workerToHubJoined"] for w in item["slowestWorkflows"])
           for item in summaries.values()),"Worker-to-Hub HTTP trace propagation broken"
print("PASS: required OTel spans and worker -> Hub propagation present in all 6 runs")
