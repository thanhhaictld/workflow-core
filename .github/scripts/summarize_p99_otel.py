#!/usr/bin/env python3
"""Summarize the real OTLP JSON file archive and correlate slow workflows.

OTel collector file exporter emits newline-delimited ExportTraceServiceRequest.
Only controlled benchmark trace IDs and workflow IDs are printed; no payloads.
"""
import collections
import json
import math
import pathlib
import statistics

ROOT = pathlib.Path("scaling/observability/results/p99-100")
TRACE = pathlib.Path("scaling/observability/results/otel-p99/traces.json")
if not TRACE.exists():
    raise RuntimeError("Missing OTLP archive: diagnostic MUST export traces")
slow = {}
for file in ROOT.glob("timeline-*.json"):
    report = json.loads(file.read_text())
    key = str(report["processors"]) + "/" + report["mode"]
    for item in report["slowestWorkflows"]:
        slow[item["workflow_id"]] = {"run": key, "totalSeconds": item["total_s"]}

scopes=collections.Counter()
matched=collections.defaultdict(list)
all_spans=collections.defaultdict(list)
service_counts=collections.Counter()
errors=0
lines=0
claim_outcomes=collections.Counter()

def attr(sp,key):
    for item in sp.get("attributes",[]):
        if item.get("key")==key:
            v=item.get("value",{})
            # OTLP intValue may be numeric zero, which is a valid claimed=0 value.
            for field in ("stringValue","intValue","doubleValue","boolValue"):
                if field in v:
                    return v[field]
    return None

def parse_time(ns):
    try: return int(ns)/1e9
    except (TypeError, ValueError): return None

with TRACE.open() as reader:
    for line in reader:
        if not line.strip(): continue
        lines+=1
        try: payload=json.loads(line)
        except ValueError:
            errors+=1
            continue
        for res in payload.get("resourceSpans",[]):
            svc=next((v.get("value",{}).get("stringValue") for v in res.get("resource",{}).get("attributes",[])
                      if v.get("key")=="service.name"),"unknown")
            for group in res.get("scopeSpans",[]):
                for sp in group.get("spans",[]):
                    name=sp.get("name","unknown")
                    if name=="hub.task.claim":
                        claimed=attr(sp,"tasks.claimed")
                        if claimed is not None:
                            claim_outcomes["total"]+=1
                            claim_outcomes["leased"]+=int(claimed)
                            if int(claimed)==0:
                                claim_outcomes["empty"]+=1
                    service_counts[svc]+=1
                    scopes[(svc,name)]+=1
                    t0=parse_time(sp.get("startTimeUnixNano"))
                    t1=parse_time(sp.get("endTimeUnixNano"))
                    duration=max(0,t1-t0) if t0 is not None and t1 is not None else None
                    wf=attr(sp,"workflow.id")
                    rec={"service":svc,"name":name,"durationSeconds":round(duration,6) if duration is not None else None,
                         "taskId":attr(sp,"task.id"),"traceId":sp.get("traceId"),
                         "events":[{"name":e.get("name"),
                                    "elapsedSeconds":attr(e,"elapsed.seconds")}
                                   for e in sp.get("events",[])]}
                    all_spans[name].append(duration)
                    if wf in slow: matched[wf].append(rec)

def percentile(values,pct):
    vals=sorted(v for v in values if v is not None)
    if not vals:return None
    ix=(len(vals)-1)*pct
    l=math.floor(ix);h=math.ceil(ix)
    return round(vals[l]+(vals[h]-vals[l])*(ix-l),6)

important=["hub.worker.activity.execute","hub.worker.workflow.execute",
           "hub.worker.execute.body","hub.worker.completion.http",
           "hub.task.claim","hub.task.claim.admission_wait",
           "hub.matching.claim_drain","hub.activity.complete.transaction"]
stats={k:{"count":len([x for x in v if x is not None]),
          "p50":percentile(v,.5),"p95":percentile(v,.95),"p99":percentile(v,.99),
          "max":max((x for x in v if x is not None),default=None)}
       for k,v in all_spans.items() if k in important}
out={"source":"real OTLP JSON trace archive","otlpBatches":lines,"invalidJsonLines":errors,
     "services":dict(service_counts),"claimSummary":dict(claim_outcomes),"spanCounts":{f"{svc}:{name}":n for (svc,name),n in scopes.items()},
     "importantSpanDurationsSeconds":stats,
     "slowWorkflows":{wf:{**meta,"spans":matched[wf]}
                      for wf,meta in slow.items() if wf in matched},
     "slowWorkflowsWithSpans":len([wf for wf in slow if wf in matched]),
     "slowWorkflowsExpected":len(slow)}
ROOT.mkdir(parents=True,exist_ok=True)
(ROOT/"otel-investigation.json").write_text(json.dumps(out,indent=2)+"\n")
print(json.dumps({"otlpBatches":lines,"services":dict(service_counts),
                  "claimSummary":dict(claim_outcomes),
                  "importantSpanDurationsSeconds":stats,
                  "slowWorkflowsWithSpans":out["slowWorkflowsWithSpans"],
                  "slowWorkflowsExpected":out["slowWorkflowsExpected"]},indent=2))
assert lines>0, "No exported OTLP batches"
assert any(n.startswith("workflow-offboarding-processor") for n in service_counts), "Missing worker spans"
assert "workflow-hub" in service_counts, "Missing Hub spans"
assert stats.get("hub.worker.execute.body",{}).get("count",0)>=600, "Insufficient worker body spans"
assert stats.get("hub.worker.completion.http",{}).get("count",0)>=600, "Insufficient completion spans"
assert stats.get("hub.activity.complete.transaction",{}).get("count",0)>=600, "Insufficient native activity completion spans"
assert out["slowWorkflowsWithSpans"]>=40, "Missing trace correlation for slow workflows"
