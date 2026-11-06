#!/usr/bin/env python3
"""Low-frequency PostgreSQL active backend wait sampler during k6.

Samples are observational, not proof of CPU utilization; no-wait active
backends can be executing on CPU or runnable, and client pool wait is invisible.
"""
import argparse
import collections
import json
import pathlib
import subprocess
import time

p=argparse.ArgumentParser()
p.add_argument("--output",required=True)
p.add_argument("--interval",type=float,default=2)
args=p.parse_args()
sql="""SELECT coalesce(wait_event_type,'None'),coalesce(wait_event,'None'),count(*)
FROM pg_stat_activity
WHERE datname=current_database() AND state='active'
  AND pid<>pg_backend_pid()
GROUP BY 1,2"""
records=[]
try:
    while True:
        cmd=["docker","compose","-f","scaling/observability/compose.yaml",
             "--env-file","scaling/observability/.env.example",
             "exec","-T","postgres","psql","-X","-U","workflow","-d","workflow",
             "-At","-F","|","-c",sql]
        try:
            r=subprocess.run(cmd,check=True,capture_output=True,text=True,timeout=15)
            for line in r.stdout.splitlines():
                event_type,event,n=line.split("|",2)
                records.append({"at":time.time(),"waitType":event_type,
                                "waitEvent":event,"backends":int(n)})
        except (subprocess.CalledProcessError,subprocess.TimeoutExpired,ValueError) as ex:
            records.append({"at":time.time(),"error":str(ex)[:160]})
        time.sleep(args.interval)
except KeyboardInterrupt:
    pass
finally:
    summary=collections.Counter()
    for r in records:
        if "waitType" in r:
            summary[r["waitType"]+":"+r["waitEvent"]]+=r["backends"]
    result={"samples":len(records),"waitSampledBackendCounts":dict(summary),
            "limitations":"two-second pg_stat_activity snapshots only; backend no-wait is not proof of CPU saturation; client Npgsql pool waits excluded",
            "observations":records}
    pathlib.Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    pathlib.Path(args.output).write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps({"samples":len(records),
                      "waitSampledBackendCounts":dict(summary)}))
