"""Paired real-Hub comparison for workflow/resource metadata enrichment.

Requires an isolated PG17 container with migration 032, identical baseline and
candidate databases, WFC_BENCH_PG_BASE/WFC_BENCH_PG_CAND connection strings,
the baseline source archive build path, and the current Release build. Raw
per-run load and SQL artifacts are retained even when a run fails.
"""
import argparse
import hashlib
import json
import os
import pathlib
import subprocess
import sys
import time
import urllib.request
import re

ROOT = pathlib.Path(__file__).resolve().parents[3]
LOAD = ROOT / "scaling/benchmarks/hub-push-load.py"
SQL_METRICS = ROOT / "scaling/benchmarks/hub-push-metrics.sql"
PG_SUMMARY = """SELECT json_build_object(
  'statements',coalesce(sum(calls),0),
  'executionMs',coalesce(sum(total_exec_time),0),
  'sharedHit',coalesce(sum(shared_blks_hit),0),
  'sharedRead',coalesce(sum(shared_blks_read),0),
  'sharedDirtied',coalesce(sum(shared_blks_dirtied),0),
  'sharedWritten',coalesce(sum(shared_blks_written),0),
  'sharedReadMs',coalesce(sum(shared_blk_read_time),0),
  'sharedWriteMs',coalesce(sum(shared_blk_write_time),0),
  'tempRead',coalesce(sum(temp_blks_read),0),
  'tempWritten',coalesce(sum(temp_blks_written),0),
  'walRecords',coalesce(sum(wal_records),0),
  'walBytes',coalesce(sum(wal_bytes),0))
FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database());
SELECT coalesce(json_agg(json_build_object('class',class,'calls',calls,'rows',rows,
 'executionMs',execution_ms,'sharedHit',shared_hit,'sharedRead',shared_read,
 'walBytes',wal_bytes) ORDER BY class),'[]'::json)
FROM (
 SELECT CASE
   WHEN query ILIKE '%requested AS MATERIALIZED%' THEN 'id-hydration'
   WHEN query ILIKE '%page AS MATERIALIZED%' THEN 'candidate-discovery-page'
   WHEN query ILIKE '%INSERT INTO wfc_scale.hub_notification_outbox%' THEN 'notification-outbox-producer'
   WHEN query ILIKE '%pg_notify(%' THEN 'notification-notify-producer'
   WHEN query ILIKE '%hub_notification_outbox%' THEN 'notification-outbox'
   WHEN query ILIKE '%hub_task%' THEN 'task-other'
   ELSE 'other'
 END AS class, sum(calls)::bigint AS calls, sum(rows)::bigint AS rows,
 sum(total_exec_time)::double precision AS execution_ms,
 sum(shared_blks_hit)::bigint AS shared_hit,
 sum(shared_blks_read)::bigint AS shared_read,
 sum(wal_bytes)::bigint AS wal_bytes
 FROM pg_stat_statements
 WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
 GROUP BY class
) grouped;
SELECT coalesce(json_agg(json_build_object('queryId',queryid,'query',left(query,600),
 'calls',calls,'rows',rows,'executionMs',total_exec_time,
 'sharedHit',shared_blks_hit,'sharedRead',shared_blks_read,'walBytes',wal_bytes)
 ORDER BY calls DESC),'[]'::json)
FROM (
 SELECT queryid,query,calls,rows,total_exec_time,shared_blks_hit,shared_blks_read,wal_bytes
 FROM pg_stat_statements
 WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database())
   AND (query ILIKE '%hub_task%' OR query ILIKE '%hub_notification_outbox%')
 ORDER BY calls DESC LIMIT 100
) top_task_queries;
SELECT json_build_object('tasksInserted',n_tup_ins,'outboxInserted',outbox_ins,
 'outboxDeleted',outbox_del) FROM pg_stat_user_tables t
 CROSS JOIN LATERAL (SELECT coalesce(n_tup_ins,0) outbox_ins,coalesce(n_tup_del,0) outbox_del
 FROM pg_stat_user_tables WHERE schemaname='wfc_scale' AND relname='hub_notification_outbox') o
 WHERE t.schemaname='wfc_scale' AND t.relname='hub_task';"""

parser = argparse.ArgumentParser()
parser.add_argument("--baseline-root", required=True)
parser.add_argument("--artifact-dir", required=True)
parser.add_argument("--pg-container", required=True)
parser.add_argument("--baseline-database", default="stageBbase")
parser.add_argument("--candidate-database", default="stageBcand")
parser.add_argument("--smoke", action="store_true")
parser.add_argument("--profile", action="store_true", help="five burst repetitions for per-query classification")
parser.add_argument("--scenarios", nargs="+", choices=["burst", "steady", "sparse", "fifo"],
                    default=["burst", "steady", "sparse", "fifo"])
args = parser.parse_args()
out = pathlib.Path(args.artifact_dir).resolve()
out.mkdir(parents=True, exist_ok=True)
baseline_root = pathlib.Path(args.baseline_root).resolve()
final_root = ROOT
baseline_hub = baseline_root / "scaling/src/WorkflowCore.Hub.Server/bin/Release/net10.0/WorkflowCore.Hub.Server.dll"
baseline_worker = baseline_root / "scaling/sample/onboarding/WorkflowCore.Provisioner.Host/bin/Release/net10.0/WorkflowCore.Provisioner.Host.dll"
final_hub = final_root / "scaling/src/WorkflowCore.Hub.Server/bin/Release/net10.0/WorkflowCore.Hub.Server.dll"
final_worker = final_root / "scaling/sample/onboarding/WorkflowCore.Provisioner.Host/bin/Release/net10.0/WorkflowCore.Provisioner.Host.dll"
if not all(p.exists() for p in (baseline_hub, baseline_worker, final_hub, final_worker)):
    raise SystemExit("missing baseline/final Release binaries")


def sha256(path):
    digest = hashlib.sha256()
    with pathlib.Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


source_pairs = {
    "HubTaskStore.cs": (baseline_root / "scaling/src/WorkflowCore.Hub/HubTaskStore.cs",
                        final_root / "scaling/src/WorkflowCore.Hub/HubTaskStore.cs"),
    "HubCandidateReader.cs": (baseline_root / "scaling/src/WorkflowCore.Hub/HubCandidateReader.cs",
                              final_root / "scaling/src/WorkflowCore.Hub/HubCandidateReader.cs"),
    "HubPartitionCandidateRegistry.cs": (baseline_root / "scaling/src/WorkflowCore.Hub/HubPartitionCandidateRegistry.cs",
                                         final_root / "scaling/src/WorkflowCore.Hub/HubPartitionCandidateRegistry.cs"),
    "HubCandidateAcquisition.cs": (baseline_root / "scaling/src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs",
                                   final_root / "scaling/src/WorkflowCore.Hub.Gateway/HubCandidateAcquisition.cs"),
    "HubMatchingOwnership.cs": (baseline_root / "scaling/src/WorkflowCore.Hub.Gateway/HubMatchingOwnership.cs",
                                final_root / "scaling/src/WorkflowCore.Hub.Gateway/HubMatchingOwnership.cs"),
    "PostgresHubNotificationListener.cs": (baseline_root / "scaling/src/WorkflowCore.Hub.Gateway/PostgresHubNotificationListener.cs",
                                            final_root / "scaling/src/WorkflowCore.Hub.Gateway/PostgresHubNotificationListener.cs"),
    "HubReadinessBroadcaster.cs": (baseline_root / "scaling/src/WorkflowCore.Hub.Gateway/HubReadinessBroadcaster.cs",
                                    final_root / "scaling/src/WorkflowCore.Hub.Gateway/HubReadinessBroadcaster.cs"),
    "HubGatewayEndpoints.cs": (baseline_root / "scaling/src/WorkflowCore.Hub.Gateway/HubGatewayEndpoints.cs",
                               final_root / "scaling/src/WorkflowCore.Hub.Gateway/HubGatewayEndpoints.cs"),
}


candidate_head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
manifest = {
    "baselineSourceCommit": "19d1cdedb05b4eeba0cff7594e8d8a257a096048",
    "candidateSourceHeadAtRun": candidate_head,
    "baselineHubBinarySha256": sha256(baseline_hub),
    "baselineWorkerBinarySha256": sha256(baseline_worker),
    "candidateHubBinarySha256": sha256(final_hub),
    "candidateWorkerBinarySha256": sha256(final_worker),
    "runtimeSourceSha256": {
        name: {"baseline": sha256(pair[0]), "candidate": sha256(pair[1])}
        for name, pair in source_pairs.items()
    },
    "schemaMigrationSha256": sha256(final_root / "scaling/sql/032_hub_candidate_task_id_hints.sql"),
    "runtimeFlags": {
        "hubOwners": 2,
        "matchingPartitions": 2,
        "matchingEnabled": True,
        "candidateTopologyBaseline": True,
        "candidateTopologyFinal": True,
        "metadataEnrichmentBaseline": False,
        "metadataEnrichmentFinal": True,
        "taskIdHintsBaseline": False,
        "taskIdHintsFinal": False,
        "workerLaneInstances": 4,
        "workflowAndActivityWorkersPerLane": 1,
        "pushEnabled": True,
        "claimAdmission": "queue, limit 3",
        "candidateBackpressure": False,
        "candidateBatchSize": 32,
        "outboxEnabled": True,
        "writerOutboxSetting": "database-level wfc.hub_notification_outbox=on for isolated paired DBs",
        "outboxDispatcher": True,
        "historyTemplateRows": 20000,
    },
    "databaseMeasurement": {
        "postgresMajor": 17,
        "pgStatStatementsTrack": "all (including nested trigger statements)",
        "trackIoTiming": True,
        "workloadSize": "20 workflows/200 tasks burst and steady; 8 workflows/80 tasks sparse and shared-resource strict FIFO",
        "rawRepetitionsPerArmAndScenario": 5,
        "isolation": "dedicated disposable PostgreSQL 17 container; paired databases start with equal 20,000 terminal-task history rows and zero workflows",
        "baselineDatabase": args.baseline_database,
        "candidateDatabase": args.candidate_database,
        "bufferTiming": "track_io_timing=on",
        "serverCpu": "not directly available; pg_stat_statements execution time includes wait time and is not CPU time",
        "candidateMetrics": "Hub counters and bounded-memory gauges exported to an isolated local OTLP collector; PostgreSQL CPU sampled from Docker stats",
        "scenarios": args.scenarios,
    },
}
(out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

base_connection = os.environ["WFC_BENCH_PG_BASE"]
candidate_connection = os.environ["WFC_BENCH_PG_CAND"]
token = "stageB-taskid-benchmark-local"
ports = (55100, 55101)
processes = []
logs = []


def pg(database, sql, run_id=None):
    env = os.environ.copy()
    args_ = ["docker", "exec", "-i", args.pg_container, "psql", "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"]
    if run_id:
        args_ += ["-v", "run_id=" + run_id]
    args_ += ["-U", "workflow", "-d", database, "-c", sql]
    result = subprocess.run(args_, capture_output=True, text=True, env=env)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def scrape_candidate_metrics(run_id):
    endpoint = os.getenv("WFC_BENCH_OTEL_METRICS", "")
    if not endpoint:
        return {}
    try:
        with urllib.request.urlopen(endpoint, timeout=3) as response:
            body = response.read().decode("utf-8", errors="replace")
    except Exception:
        return {}
    values = {}
    for line in body.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name = line.split("{", 1)[0].split(" ", 1)[0]
        if not name.startswith("workflow_hub_candidate_"):
            continue
        instance = re.search(r'\binstance="([^"]+)"', line)
        if instance is None or run_id not in instance.group(1):
            continue
        try:
            value = float(line.rsplit(" ", 1)[1])
        except (ValueError, IndexError):
            continue
        values[name] = values.get(name, 0.0) + value
    return values


def metric_delta(before, after):
    return {name: after.get(name, 0.0) - before.get(name, 0.0)
            for name in sorted(set(before) | set(after))}


def environment(arm, node, database, task_id_hints_enabled, run_id):
    env = os.environ.copy()
    db = base_connection if arm == "baseline" else candidate_connection
    # The supplied connection string contains no Database key; database name
    # is appended without writing the string to logs/artifacts.
    env.update({
        "WFC_PG": db + ";Database=" + database,
        "WFC_HUB_AUTH_TOKEN": token,
        "WFC_HUB_MATCHING_ENABLED": "true",
        "WFC_HUB_MATCHING_PARTITIONS": "2",
        "WFC_HUB_MATCHING_PEERS": f"http://127.0.0.1:{ports[0]},http://127.0.0.1:{ports[1]}",
        # Compare metadata enrichment within the established in-memory matcher;
        # candidate topology is on for both arms.
        "WFC_HUB_MATCHING_CANDIDATES_ENABLED": "true",
        "WFC_HUB_CANDIDATE_TASK_ID_HINTS_ENABLED": "true" if task_id_hints_enabled else "false",
        "WFC_HUB_PUSH_ENABLED": "true",
        "WFC_HUB_CLAIM_ADMISSION_ENABLED": "true",
        "WFC_HUB_CLAIM_ADMISSION_MODE": "queue",
        "WFC_HUB_CLAIM_ADMISSION_LIMIT": "3",
        "WFC_HUB_OUTBOX_ENABLED": "true",
        "WFC_HUB_OUTBOX_MIN_DELAY_MS": "10",
        "WFC_HUB_OUTBOX_MAX_DELAY_MS": "100",
        "WFC_HUB_CANDIDATE_BACKPRESSURE": "false",
        "WFC_HUB_CANDIDATE_BATCH_SIZE": "32",
        "WFC_HUB_WORKER_HUB": "onboarding",
        "OTEL_EXPORTER_OTLP_ENDPOINT": os.getenv("WFC_BENCH_OTEL_EXPORTER_ENDPOINT", ""),
        "OTEL_METRIC_EXPORT_INTERVAL": "1000",
        "OTEL_SERVICE_INSTANCE_ID": f"enrichment-{arm}-{node}-{run_id}",
        "Logging__LogLevel__Default": "Critical",
    })
    env["WFC_HUB_MATCHING_NODE_ID"] = "stageB-" + arm + "-" + node
    env["WFC_HUB_MATCHING_ADVERTISE_URL"] = f"http://127.0.0.1:{ports[0 if node == 'a' else 1]}"
    return env


def start(arm, database, task_id_hints_enabled, run_id):
    hub = baseline_hub if arm == "baseline" else final_hub
    worker = baseline_worker if arm == "baseline" else final_worker
    for index, node in enumerate(("a", "b")):
        log = (out / f"{arm}-{node}.log").open("a", encoding="utf-8")
        logs.append(log)
        env = environment(arm, node, database, task_id_hints_enabled, run_id)
        env["ASPNETCORE_URLS"] = f"http://127.0.0.1:{ports[index]}"
        processes.append(subprocess.Popen(["dotnet", str(hub)], cwd=hub.parent, env=env,
                                          stdout=log, stderr=subprocess.STDOUT))
    ready = False
    for _ in range(80):
        if any(p.poll() is not None for p in processes[-2:]):
            raise RuntimeError(f"{arm} Hub exited during startup; see {out}")
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{ports[0]}/health/ready", timeout=1).close()
            urllib.request.urlopen(f"http://127.0.0.1:{ports[1]}/health/ready", timeout=1).close()
            ready = True
            break
        except Exception:
            time.sleep(.25)
    if not ready:
        raise RuntimeError(f"{arm} Hub readiness timed out")
    worker_log = (out / f"{arm}-worker.log").open("a", encoding="utf-8")
    logs.append(worker_log)
    env = environment(arm, "worker", database, task_id_hints_enabled, run_id)
    env.update({
        "WFC_HUB_URL": f"http://127.0.0.1:{ports[0]}",
        "WFC_HUB_WORKER_LANE_INSTANCES": "4",
        "WORKFLOW_WORKERS": "1",
        "ACTIVITY_WORKERS": "1",
    })
    processes.append(subprocess.Popen(["dotnet", str(worker)], cwd=worker.parent, env=env,
                                      stdout=worker_log, stderr=subprocess.STDOUT))
    time.sleep(3)
    if processes[-1].poll() is not None:
        raise RuntimeError(f"{arm} worker exited during startup; see {out}")


def stop():
    global processes, logs
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
    for process in reversed(processes):
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    processes = []
    for log in logs:
        log.close()
    logs = []


def execute(arm, database, task_id_hints_enabled, scenario, repetition, warmup=False):
    run_id = f"stageB-{arm}-{scenario}-{'warm' if warmup else repetition}"
    metrics_before = scrape_candidate_metrics(run_id)
    start(arm, database, task_id_hints_enabled, run_id)
    try:
        # Reset only this isolated PG17 container's cumulative query/table stats.
        pg(database, "SELECT pg_stat_statements_reset(); SELECT pg_stat_reset();")
        params = {
            "burst": (20, 4, 0, False),
            "steady": (20, 4, 400, False),
            "sparse": (8, 1, 1500, False),
            "fifo": (8, 4, 400, True),
        }[scenario]
        count, vus, stagger, constrained = params
        if warmup:
            count, vus, stagger, constrained = (max(4, count // 2), max(1, vus // 2), 0, constrained)
        raw_path = out / f"{run_id}.json"
        env = os.environ.copy()
        env.update({
            "WFC_HUB_URL": f"http://127.0.0.1:{ports[0]}",
            "WFC_HUB_AUTH_TOKEN": token,
            "WFC_BENCH_RUN_ID": run_id,
            "WFC_BENCH_OUTPUT": str(raw_path),
            "WFC_BENCH_POSTGRES_CONTAINER": args.pg_container,
            "WFC_BENCH_VUS": str(vus),
            "WFC_BENCH_WORKFLOWS": str(count),
            "WFC_BENCH_TIMEOUT": "180",
            "WFC_BENCH_STAGGER_MS": str(stagger),
            "WFC_BENCH_SHARED_TENANT": str(constrained).lower(),
        })
        workload = subprocess.run([sys.executable, str(LOAD)], cwd=ROOT, env=env,
                                  capture_output=True, text=True)
        (out / f"{run_id}.stdout.txt").write_text(workload.stdout, encoding="utf-8")
        (out / f"{run_id}.stderr.txt").write_text(workload.stderr, encoding="utf-8")
        stop()
        # PG17 may publish table counters shortly after backend transaction end.
        time.sleep(5)
        metrics_after = scrape_candidate_metrics(run_id)
        try:
            load = json.loads(raw_path.read_text(encoding="utf-8"))
        except Exception:
            load = {"run_id": run_id, "load_exit": workload.returncode, "artifact_missing": True}
        metric_result = subprocess.run(
            ["docker", "exec", "-i", args.pg_container, "psql", "-X", "-A", "-t",
             "-v", "ON_ERROR_STOP=1", "-v", "run_id=" + run_id,
             "-U", "workflow", "-d", database, "-f", "-"],
            input=SQL_METRICS.read_text(encoding="utf-8"), capture_output=True, text=True)
        (out / f"{run_id}-workflow-metrics.json").write_text(metric_result.stdout, encoding="utf-8")
        summary = pg(database, PG_SUMMARY)
        try:
            workload_summary = json.loads(metric_result.stdout.strip().splitlines()[-1])
            db_summary = [json.loads(line) for line in summary.splitlines() if line.strip()]
        except Exception:
            workload_summary, db_summary = {}, []
        record = {
            "arm": arm, "candidateTopologyEnabled": True,
            "taskIdHintsEnabled": task_id_hints_enabled,
            "scenario": scenario, "repetition": repetition,
            "workloadExit": workload.returncode,
            "metricsExit": metric_result.returncode,
            "workload": workload_summary,
            "database": db_summary,
            "candidateTelemetryDelta": metric_delta(metrics_before, metrics_after),
            "durationSeconds": load.get("duration_seconds"),
            "sharedTenant": constrained,
            "staggerMs": stagger,
            "load": load,
            "benchmarkManifest": manifest,
        }
        (out / f"{run_id}-record.json").write_text(json.dumps(record, indent=2), encoding="utf-8")
        (out / f"{run_id}-manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return record
    finally:
        stop()


records = []
try:
    arms = [("baseline", args.baseline_database, False),
            ("candidate", args.candidate_database, False)]
    for arm, database, enabled in arms:
        for scenario in args.scenarios:
            print(f"warmup {arm} {scenario}", flush=True)
            records.append(execute(arm, database, enabled, scenario, 0, warmup=True))
    scenarios = args.scenarios
    if args.smoke or args.profile:
        scenarios = ["burst"]
    for repetition in range(1, 6 if not args.smoke else 2):
        for scenario in scenarios:
            ordered = arms if (repetition + scenarios.index(scenario)) % 2 else list(reversed(arms))
            for arm, database, enabled in ordered:
                print(f"{arm} {scenario} repetition {repetition}", flush=True)
                records.append(execute(arm, database, enabled, scenario, repetition))
finally:
    stop()
    (out / "records.json").write_text(json.dumps(records, indent=2), encoding="utf-8")
