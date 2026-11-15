#!/usr/bin/env python3
"""Read-only live diagnosis; optional synthetic write experiments in a NEW PG17 container.

Never runs ANALYZE, DDL, DML or EXPLAIN ANALYZE on the source database.
The isolated arms are attribution experiments, not deployment migrations.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess
import time
import uuid


def run(args, text=None):
    p = subprocess.run(args, input=text, text=True, capture_output=True, encoding="utf-8", timeout=180)
    if p.returncode:
        raise RuntimeError(f"{args[:5]}: {p.stderr}\n{p.stdout[-2000:]}")
    return p.stdout


def sql(container, user, database, statement):
    return run(["docker", "exec", "-i", container, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                "-U", user, "-d", database], statement)


LIVE_SQL = r"""
BEGIN READ ONLY;
SET LOCAL statement_timeout='30s';
SELECT jsonb_build_object(
 'captured_at',clock_timestamp(),'version',version(),'database',current_database(),
 'reset',(SELECT to_jsonb(s) FROM pg_stat_statements_info s),
 'statements',(SELECT jsonb_agg(to_jsonb(s)) FROM (
   SELECT queryid,stats_since,calls,rows,total_exec_time,mean_exec_time,max_exec_time,stddev_exec_time,
     shared_blks_hit,shared_blks_read,shared_blks_dirtied,wal_bytes,temp_blks_written,query
   FROM pg_stat_statements WHERE queryid IN (702976769898827252,325984884542718125)
   ORDER BY queryid) s),
 'top_statements',(SELECT jsonb_agg(to_jsonb(s)) FROM (
   SELECT queryid,calls,total_exec_time,mean_exec_time,left(query,200) query
   FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
   ORDER BY total_exec_time DESC LIMIT 12) s),
 'tables',(SELECT jsonb_agg(to_jsonb(s)) FROM (
   SELECT relname,n_live_tup,n_dead_tup,n_tup_ins,n_tup_upd,n_tup_hot_upd,last_autovacuum,last_autoanalyze
   FROM pg_stat_user_tables WHERE schemaname='wfc_scale' AND relname IN ('hub_task','hub_workflow')) s),
 'indexes',(SELECT jsonb_agg(to_jsonb(s)) FROM (
   SELECT c.relname,pg_relation_size(c.oid) bytes,pg_get_indexdef(c.oid) definition,
     i.indisvalid,i.indisunique,s.idx_scan,s.idx_tup_read,s.idx_tup_fetch
   FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
   LEFT JOIN pg_stat_user_indexes s ON s.indexrelid=i.indexrelid
   WHERE i.indrelid='wfc_scale.hub_task'::regclass ORDER BY c.relname) s),
 'triggers',(SELECT jsonb_agg(pg_get_triggerdef(oid)) FROM pg_trigger
   WHERE tgrelid='wfc_scale.hub_task'::regclass),
 'notify_function',pg_get_functiondef('wfc_scale.notify_hub_work_ready()'::regprocedure),
 'settings',(SELECT jsonb_object_agg(name,setting) FROM pg_settings WHERE name IN
   ('shared_buffers','track_io_timing','pg_stat_statements.track_planning','synchronous_commit','default_toast_compression')),
 'result_sizes',(SELECT jsonb_agg(to_jsonb(s)) FROM (
   SELECT task_type,count(*),avg(pg_column_size(result_json)) stored_bytes,
     avg(octet_length(result_json::text)) text_bytes,max(octet_length(result_json::text)) max_text_bytes
   FROM (SELECT task_type,result_json FROM wfc_scale.hub_task ORDER BY sequence DESC LIMIT 16000) t
   GROUP BY task_type) s),
 'waits',(SELECT coalesce(jsonb_agg(to_jsonb(s)),'[]') FROM (
   SELECT state,wait_event_type,wait_event,count(*) FROM pg_stat_activity
   WHERE datname=current_database() AND pid<>pg_backend_pid() GROUP BY 1,2,3) s));
COMMIT;
"""


def objects(text):
    decoder = json.JSONDecoder()
    while text.strip():
        value, end = decoder.raw_decode(text.lstrip())
        yield value
        text = text.lstrip()[end:]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source-container", required=True)
    p.add_argument("--user", default="workflow")
    p.add_argument("--database", default="workflow")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--experiment", action="store_true")
    p.add_argument("--plans-only", action="store_true", help="Run only the stale/fresh completion comparison")
    p.add_argument("--repetitions", type=int, default=5)
    p.add_argument("--tasks", type=int, default=160)
    a = p.parse_args()
    if a.plans_only and not a.experiment:
        p.error("--plans-only requires --experiment")
    if a.tasks < 8 or a.repetitions < 1:
        p.error("tasks must be >=8 and repetitions >=1")
    a.output.mkdir(parents=True, exist_ok=False)
    live = json.loads(sql(a.source_container,a.user,a.database,LIVE_SQL))
    (a.output / "live.json").write_text(json.dumps(live,indent=2),encoding="utf-8")
    # Cost-only plans with actual IDs; no statement is executed by EXPLAIN.
    selected = json.loads(sql(a.source_container,a.user,a.database,"""
      BEGIN READ ONLY; SET LOCAL statement_timeout='10s';
      SELECT to_jsonb(t) FROM (SELECT id,workflow_id,hub_id,partition_id,last_owner,epoch
      FROM wfc_scale.hub_task ORDER BY sequence DESC LIMIT 1) t; COMMIT;
      """))
    q = lambda value: "'" + str(value).replace("'", "''") + "'"
    update = """UPDATE wfc_scale.hub_task SET state=2,owner=NULL,lease_until=NULL,result_json=$1::jsonb,updated_at=now()
      WHERE id=$2 AND owner=$3 AND epoch=$4 AND state=1 AND lease_until>clock_timestamp()"""
    wake = """INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload)
      SELECT $1,$2,$3,'__native_workflow__',1,$4,'{}'::jsonb
      FROM wfc_scale.hub_workflow WHERE id=$4 AND native_status='Runnable' ON CONFLICT DO NOTHING"""
    plans = []
    for mode in ("force_custom_plan", "force_generic_plan"):
        output = sql(a.source_container,a.user,a.database,f"""
          BEGIN READ ONLY; SET LOCAL statement_timeout='10s'; SET LOCAL plan_cache_mode={mode};
          PREPARE ack(text,uuid,text,bigint) AS {update};
          PREPARE wake(uuid,text,integer,uuid) AS {wake};
          EXPLAIN (FORMAT JSON) EXECUTE ack('{{}}',{q(selected['id'])},{q(selected['last_owner'])},{selected['epoch']});
          EXPLAIN (FORMAT JSON) EXECUTE wake('{uuid.uuid4()}',{q(selected['hub_id'])},{selected['partition_id']},{q(selected['workflow_id'])});
          COMMIT;""")
        plans.append({"mode":mode,"plans":list(objects(output))})
    (a.output / "live-cost-only-plans.json").write_text(json.dumps(plans,indent=2),encoding="utf-8")
    if not a.experiment:
        print(json.dumps({"output":str(a.output),"live_read_only":True}))
        return
    schema = run(["docker","exec",a.source_container,"pg_dump","-U",a.user,"-d",a.database,
                  "--schema-only","--schema=wfc_scale","--no-owner","--no-privileges"])
    (a.output / "source-schema.sql").write_text(schema,encoding="utf-8")
    container = "orca-completion-write-cost-" + uuid.uuid4().hex[:10]
    created = False
    try:
        run(["docker","run","-d","--name",container,"--label","io.orca.experiment=completion-write-cost",
             "--network","none","-e","POSTGRES_HOST_AUTH_METHOD=trust","postgres:17"])
        created = True
        for _ in range(60):
            ready = subprocess.run(["docker","exec",container,"pg_isready","-U","postgres"],capture_output=True)
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise RuntimeError("isolated PostgreSQL did not become ready")
        sql(container,"postgres","postgres","CREATE DATABASE fixture;")
        sql(container,"postgres","fixture",schema)
        # All data is synthetic. Live schema/indexes/trigger definitions are preserved.
        seed = f"""
          INSERT INTO wfc_scale.hub_workflow(id,hub_id,definition_id,definition_version,partition_id,input_json,step_count,execution_mode)
          SELECT md5('workflow-'||g)::uuid,'synthetic','synthetic',1,g%64,'{{}}',1,'native'
          FROM generate_series(1,{12000+a.tasks}) g;
          INSERT INTO wfc_scale.hub_task(id,hub_id,partition_id,task_type,task_version,workflow_id,payload,state,owner,last_owner,epoch,lease_until,attempts)
          SELECT md5('task-'||g)::uuid,'synthetic',g%64,
            CASE WHEN g%4=0 THEN 'synthetic-activity' ELSE '__native_workflow__' END,1,
            md5('workflow-'||g)::uuid,'{{}}',CASE WHEN g<=12000 THEN 2 ELSE 1 END,
            CASE WHEN g>12000 THEN 'synthetic-worker' END,'synthetic-worker',1,
            CASE WHEN g>12000 THEN now()+interval '1 day' END,1
          FROM generate_series(1,{12000+a.tasks}) g;
          VACUUM ANALYZE wfc_scale.hub_task;
          VACUUM ANALYZE wfc_scale.hub_workflow;
        """
        sql(container,"postgres","fixture",seed)
        # Reproduce the live filter-only expired-index scan with actual schema
        # and unchanged index definitions; only isolated rows/stats are changed.
        sql(container,"postgres","postgres","CREATE DATABASE plan_fixture TEMPLATE fixture;")
        sql(container,"postgres","plan_fixture","""
          ALTER TABLE wfc_scale.hub_task SET (autovacuum_enabled=false);
          UPDATE wfc_scale.hub_task SET state=2,owner=NULL,lease_until=NULL;
          VACUUM ANALYZE wfc_scale.hub_task;
          UPDATE wfc_scale.hub_task SET state=1,owner='synthetic-worker',lease_until=now()+interval '1 day';
        """)
        source=(Path(__file__).resolve().parents[2]/"src/WorkflowCore.Hub/HubNativeWorkflowStore.cs").read_text(encoding="utf-8")
        match=re.search(r'CompleteTaskSql\s*=\s*"""\s*\n(.*?)\n\s*""";',source,re.S)
        if not match:
            raise RuntimeError("Cannot extract production CompleteTaskSql")
        current=match[1]
        for name,position in (("result",1),("task",2),("owner",3),("epoch",4)):
            current=current.replace("@"+name,f"${position}")
        target=uuid.UUID(bytes=hashlib.md5(f'task-{12000+a.tasks}'.encode()).digest())
        comparisons=[]
        for state in ("stale-empty-leased-stats","analyzed-backlog"):
            if state=="analyzed-backlog":
                sql(container,"postgres","plan_fixture","ANALYZE wfc_scale.hub_task;")
            for mode in ("force_custom_plan","force_generic_plan"):
                for rep in range(a.repetitions):
                    variants=[("baseline",update),("current",current)]
                    if rep%2:
                        variants.reverse()
                    for variant,query in variants:
                        output=sql(container,"postgres","plan_fixture",f"""
                          SET plan_cache_mode={mode}; PREPARE ack(text,uuid,text,bigint) AS {query};
                          BEGIN; EXECUTE ack('{{}}','{target}','synthetic-worker',1); ROLLBACK;
                          BEGIN;
                          EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON)
                          EXECUTE ack('{{}}','{target}','synthetic-worker',1);
                          ROLLBACK;
                        """)
                        plan=json.loads(output)[0]
                        scan=plan["Plan"]["Plans"][0]
                        assert scan["Actual Rows"]==1
                        work=scan["Actual Rows"]+scan.get("Rows Removed by Filter",0)
                        if variant=="current":
                            assert scan["Index Name"]=="hub_task_pkey" and work==1,plan
                        comparisons.append({"state":state,"mode":mode,"rep":rep,"variant":variant,
                          "ms":plan["Execution Time"],"rows_visited":work,
                          "blocks":plan["Plan"].get("Shared Hit Blocks",0)+plan["Plan"].get("Shared Read Blocks",0),
                          "plan":plan})
        stale_before=[r for r in comparisons if r["state"]=="stale-empty-leased-stats" and r["variant"]=="baseline"]
        assert max(r["rows_visited"] for r in stale_before)>=12000, "Baseline defect did not reproduce"
        (a.output/"completion-plan-comparison.json").write_text(json.dumps(comparisons,indent=2),encoding="utf-8")
        print("Completion primary-key plan and baseline reproduction checks passed.",flush=True)
        arms = {
          "baseline":"",
          "without_operations_index":"DROP INDEX wfc_scale.ix_hub_task_operations;",
          "without_notify_trigger":"ALTER TABLE wfc_scale.hub_task DISABLE TRIGGER trg_hub_work_ready;",
          "lz4_result":"ALTER TABLE wfc_scale.hub_task ALTER COLUMN result_json SET COMPRESSION lz4;",
        }
        summaries = []
        for rep in range(0 if a.plans_only else a.repetitions):
            order = list(arms)
            if rep%2:
                order.reverse()
            for arm in order:
                database=f"trial_{rep}_{arm}"
                sql(container,"postgres","postgres",f'CREATE DATABASE "{database}" TEMPLATE fixture;')
                sql(container,"postgres",database,arms[arm])
                statements=[]
                for g in range(12001,12001+a.tasks):
                    # About 3KB JSON with varied fields; no copied user payloads.
                    result={"outputs":{},"result":None,"error":None} if g%4==0 else {
                      "commit":{"ProtocolVersion":1,"ExpectedRevision":2,"Snapshot":{
                        "Pointers":[{"Id":f"pointer-{i}","StepId":i,"Status":"Complete",
                          "Data":{f"field-{k}":f"synthetic-value-{g}-{i}-{k}" for k in range(8)}} for i in range(6)]},
                        "History":[],"Status":"Runnable"},"revision":3,"status":"Runnable"}
                    tid=uuid.UUID(bytes=hashlib.md5(f'task-{g}'.encode()).digest())
                    wid=uuid.UUID(bytes=hashlib.md5(f'workflow-{g}'.encode()).digest())
                    statements += ["BEGIN;",
                      "EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON) " + update.replace("$1",q(json.dumps(result))).replace("$2",q(tid)).replace("$3","'synthetic-worker'").replace("$4","1")+";",
                      "EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON) " + wake.replace("$1",q(uuid.uuid4())).replace("$2","'synthetic'").replace("$3",str(g%64)).replace("$4",q(wid))+";",
                      "COMMIT;"]
                raw=sql(container,"postgres",database,"\n".join(statements))
                results=[obj[0] for obj in objects(raw)]
                (a.output/f"{database}.json").write_text(json.dumps(results),encoding="utf-8")
                for kind, rows in (("completion",results[::2]),("wake",results[1::2])):
                    assert len(rows)==a.tasks
                    for row in rows:
                        plan=row["Plan"]
                        if kind=="completion":
                            assert plan["Plans"][0]["Actual Rows"]==1, row
                        else:
                            assert plan.get("Tuples Inserted")==1, row
                    summaries.append({"rep":rep,"arm":arm,"query":kind,
                      "mean_ms":statistics.mean(r["Execution Time"] for r in rows),
                      "median_ms":statistics.median(r["Execution Time"] for r in rows),
                      "mean_shared_blocks":statistics.mean(r["Plan"].get("Shared Hit Blocks",0)+r["Plan"].get("Shared Read Blocks",0) for r in rows),
                      "mean_wal_bytes":statistics.mean(r["Plan"].get("WAL Bytes",0) for r in rows),
                      "mean_trigger_ms":statistics.mean(sum(t["Time"] for t in r.get("Triggers",[]) if "Time" in t) for r in rows)})
                print(json.dumps(summaries[-2:]),flush=True)
        (a.output/"summary.json").write_text(json.dumps(summaries,indent=2),encoding="utf-8")
        (a.output/"metadata.json").write_text(json.dumps({"container":container,"source":a.source_container,
          "tasks":a.tasks,"repetitions":a.repetitions,"timestamp":dt.datetime.now(dt.timezone.utc).isoformat(),
          "baseline_sql_sha256":hashlib.sha256(update.encode()).hexdigest(),
          "current_sql_sha256":hashlib.sha256(current.encode()).hexdigest(),
          "source_schema_sha256":hashlib.sha256(schema.encode()).hexdigest(),
          "arms":arms,"data":"synthetic; source schema only; no user rows copied"},indent=2),encoding="utf-8")
    finally:
        if created:
            run(["docker","stop",container])
            print(f"Stopped isolated container {container}; synthetic data retained.",flush=True)


if __name__=="__main__":
    main()
