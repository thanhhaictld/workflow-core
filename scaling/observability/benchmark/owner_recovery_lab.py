"""Isolated ownership lab. Never operates on containers outside its named prefix.

Run from repository root; Python stdlib and Docker only. Resources and volumes
are retained. Each trace has DB UTC plus monotonic elapsed seconds. Credentials
are fixed, disposable lab credentials, not inherited from the host environment.
"""
import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import threading
import time
import urllib.request
import urllib.error
import zipfile

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = ROOT / 'scaling/docs/cadence-inspired-evidence'
PREFIX = 'cadence-owner-20261109'
TOKEN = os.getenv('OWNER_RECOVERY_TOKEN') or 'owner-lab-development-only'
COMPOSE_PROJECT = None
COMPOSE_CONTAINERS = {}
PORTS = {'hub': 25200, 'hub-2': 25201, 'lb': 25210, 'pg': 25440, 'collector': 25289}


def run(*args, input=None, check=True):
    p = subprocess.run(args, input=input, text=True, capture_output=True, cwd=ROOT, timeout=180)
    if check and p.returncode:
        raise RuntimeError(f'{args}: {p.stdout}\n{p.stderr}')
    return p.stdout.strip()


def docker(*args, **kw):
    return run('docker', *args, **kw)


def name(service):
    if COMPOSE_PROJECT:
        if service not in COMPOSE_CONTAINERS:
            compose_service = 'processor' if service.startswith('worker-') else service
            filters = ['--filter', 'label=com.docker.compose.project=' + COMPOSE_PROJECT,
                       '--filter', 'label=com.docker.compose.service=' + compose_service]
            if service.startswith('worker-'):
                filters += ['--filter', 'label=com.docker.compose.container-number=' + service[7:]]
            ids = docker('ps', '-aq', *filters).splitlines()
            if len(ids) != 1:
                raise RuntimeError('Expected exactly one suite container for ' + service)
            label = docker('inspect', '--format', '{{index .Config.Labels "cadence.owner-suite"}}', ids[0])
            if label != COMPOSE_PROJECT:
                raise RuntimeError('Container is outside the ownership suite: ' + service)
            COMPOSE_CONTAINERS[service] = ids[0]
        return COMPOSE_CONTAINERS[service]
    return PREFIX + '-' + service


def sql(query):
    return docker('exec', '-i', name('pg'), 'psql', '-X', '-U', 'workflow',
                  '-d', 'workflow', '-At', '-v', 'ON_ERROR_STOP=1', input=query)


def http(port, path, body=None, timeout=3):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f'http://127.0.0.1:{port}{path}', data=data,
        headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def wait_ready(service):
    for _ in range(100):
        try:
            if http(PORTS[service], '/health/ready')[0] == 200:
                return
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('Not ready: ' + service)


def create(service, image, env=None, extra=None, command=None):
    args = ['create', '--name', name(service), '--label', 'cadence.owner-lab=' + PREFIX,
            '--network', PREFIX, '--network-alias', service]
    for k, v in (env or {}).items():
        args += ['-e', k + '=' + str(v)]
    args += extra or []
    args += [image] + (command or [])
    docker(*args)
    docker('start', name(service))


def setup(tag):
    # Deliberately fails if an old project exists: never resets a retained DB.
    if docker('volume', 'ls', '--filter', 'name=^' + PREFIX + '-pg$', '--format', '{{.Name}}'):
        raise RuntimeError('Retained volume exists; choose a new --project')
    docker('image', 'inspect', 'cadence-owner-hub:' + tag, 'cadence-owner-worker:' + tag)
    docker('network', 'create', '--label', 'cadence.owner-lab=' + PREFIX, PREFIX)
    create('pg', 'postgres:17', {'POSTGRES_USER': 'workflow',
        'POSTGRES_PASSWORD': 'owner_lab_only', 'POSTGRES_DB': 'workflow'},
        ['-p', f'127.0.0.1:{PORTS["pg"]}:5432', '-v', PREFIX + '-pg:/var/lib/postgresql/data'],
        ['postgres', '-c', 'shared_preload_libraries=pg_stat_statements', '-c', 'compute_query_id=on'])
    for _ in range(60):
        if docker('exec', name('pg'), 'pg_isready', '-U', 'workflow', check=False):
            try:
                sql('SELECT 1;')
                break
            except RuntimeError:
                pass
        time.sleep(1)
    for migration in sorted((ROOT / 'scaling/sql').glob('*.sql')):
        sql(migration.read_text(encoding='utf-8-sig'))
    sql('CREATE EXTENSION IF NOT EXISTS pg_stat_statements;')
    telemetry()
    start_hubs(tag)
    create('lb', 'nginx:1.29-alpine', extra=['-p', f'127.0.0.1:{PORTS["lb"]}:5100', '-v',
        str(ROOT / 'scaling/observability/nginx.matching-ha.conf') + ':/etc/nginx/conf.d/default.conf:ro'])
    wait_ready('lb')
    start_workers(tag)
    identity(tag)


def start_workers(tag):
    for n in range(1, 5):
        create('worker-' + str(n), 'cadence-owner-worker:' + tag, {
            'WFC_HUB_URL': 'http://lb:5100', 'WFC_HUB_AUTH_TOKEN': TOKEN,
            'WFC_HUB_ALLOW_INSECURE_HTTP': 'true', 'WORKFLOW_WORKERS': 8,
            'ACTIVITY_WORKERS': 8, 'WFC_HUB_MATCHING_PARTITIONS': 4,
            'WFC_HUB_TASK_SSE_PUSH': 'true', 'WFC_HUB_LONG_POLL_ENABLED': 'true',
            'POLICY_BENCHMARK_ENABLED': 'true', 'POLICY_BENCHMARK_ONLY': 'true',
            'OTEL_EXPORTER_OTLP_ENDPOINT': '' if os.getenv('OWNER_LAB_METRICS') == 'off' else 'http://collector:4318',
            'OTEL_METRIC_EXPORT_INTERVAL': '1000',
            'OTEL_SDK_DISABLED': 'true' if os.getenv('OWNER_LAB_METRICS') == 'off' else 'false',
            'OTEL_SERVICE_INSTANCE_ID': 'worker-' + str(n)}, command=['WorkflowCore.Offboarding.Processor.dll'])


def telemetry():
    create('collector', 'otel/opentelemetry-collector-contrib:0.140.0', extra=[
        '-p', f'127.0.0.1:{PORTS["collector"]}:8889', '-v', str(ROOT / 'scaling/observability/benchmark/owner-lab-collector.yaml') +
        ':/etc/owner-lab.yaml:ro'], command=['--config=/etc/owner-lab.yaml'])


def start_hubs(tag):
    for service, node in [('hub', 'hub-a'), ('hub-2', 'hub-b')]:
        create(service, 'cadence-owner-hub:' + tag, {
            'ASPNETCORE_URLS': 'http://0.0.0.0:5100',
            'WFC_PG': 'Host=pg;Database=workflow;Username=workflow;Password=owner_lab_only;Maximum Pool Size=12',
            'WFC_HUB_AUTH_TOKEN': TOKEN, 'WFC_HUB_MATCHING_ENABLED': 'true',
            'WFC_HUB_MATCHING_PARTITIONS': 4, 'WFC_HUB_MATCHING_NODE_ID': node,
            'WFC_HUB_MATCHING_ADVERTISE_URL': f'http://{service}:5100',
            'WFC_HUB_MATCHING_PEERS': 'http://hub:5100,http://hub-2:5100',
            'WFC_HUB_MATCHING_EVENT_GATED': 'true', 'WFC_HUB_CLAIM_ADMISSION_ENABLED': 'true',
            'WFC_HUB_CLAIM_ADMISSION_MODE': 'demand', 'WFC_HUB_BATCH_UNCONSTRAINED_CLAIMS': 'true',
            'WorkflowHub__Scheduler__Enabled': 'true',
            'OTEL_EXPORTER_OTLP_ENDPOINT': '' if os.getenv('OWNER_LAB_METRICS') == 'off' else 'http://collector:4318',
            'OTEL_METRIC_EXPORT_INTERVAL': '1000', 'OTEL_SERVICE_INSTANCE_ID': node,
            'WFC_HUB_OWNER_DIAGNOSTICS': os.getenv('OWNER_LAB_DIAGNOSTICS', 'true'),
            'WFC_HUB_PG_NOTIFY_ENABLED': 'false' if os.getenv('OWNER_LAB_NOTIFY') == 'off' else 'true',
            'OTEL_SDK_DISABLED': 'true' if os.getenv('OWNER_LAB_METRICS') == 'off' else 'false'},
            ['-p', f'127.0.0.1:{PORTS[service]}:5100'], ['WorkflowCore.Hub.Server.dll'])
    for service in ['hub', 'hub-2']:
        wait_ready(service)


def identity(label):
    data = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'commit': run('git', 'rev-parse', 'HEAD'), 'status': run('git', 'status', '--short'),
        'diff': run('git', 'diff'),
        'images': json.loads(docker('image', 'inspect', 'cadence-owner-hub:' + label,
                                   'cadence-owner-worker:' + label)),
        'containers': json.loads(docker('inspect', *[name(x) for x in
            ['pg', 'hub', 'hub-2', 'lb'] + ['worker-' + str(n) for n in range(1, 5)]]))}
    (EVIDENCE / (label + '-identity-' + str(time.time_ns()) + '.json')).write_text(json.dumps(data, indent=2))


def cpu_snapshot():
    return {s: docker('exec', name(s), 'cat', '/sys/fs/cgroup/cpu.stat') for s in
            ['pg', 'hub', 'hub-2', 'lb'] + ['worker-' + str(n) for n in range(1, 5)]}


def export_run(out, label):
    for table in ['hub_workflow', 'hub_task', 'hub_task_attempt', 'hub_workflow_event']:
        query = (f"SELECT coalesce(json_agg(t),'[]') FROM (SELECT * FROM wfc_scale.{table} " +
            (f"WHERE run_id='{label}'" if table == 'hub_workflow' else
             f"WHERE workflow_id IN (SELECT id FROM wfc_scale.hub_workflow WHERE run_id='{label}')") + ') t;')
        (out / (table + '.json')).write_text(sql(query), encoding='utf-8')


def benchmark(label, count, mode):
    out = EVIDENCE / label
    out.mkdir(exist_ok=False)
    before = {'cpu': cpu_snapshot(), 'statements': json.loads(sql(
        'SELECT json_agg(t) FROM (SELECT queryid,calls,total_exec_time,rows,query FROM pg_stat_statements)t;'))}
    (out / 'before.json').write_text(json.dumps(before, indent=2))
    start = time.monotonic()
    stop = threading.Event()
    def sample():
        with (out / 'samples.jsonl').open('w') as f:
            while not stop.is_set():
                now = time.monotonic()
                try:
                    f.write(json.dumps({'elapsed': now - start, 'db': json.loads(sql(OWNER_SQL))}) + '\n')
                    with urllib.request.urlopen(f'http://127.0.0.1:{PORTS["collector"]}/metrics', timeout=2) as response:
                        (out / f'metrics-{int(now-start):04}.prom').write_bytes(response.read())
                    f.flush()
                except Exception as e:
                    f.write(json.dumps({'elapsed': now - start, 'error': str(e)}) + '\n')
                stop.wait(max(0, 1 - (time.monotonic() - now)))
    thread = threading.Thread(target=sample)
    thread.start()
    try:
        workload(label, count, mode)
        while time.monotonic() - start < 180:
            code, summary = http(PORTS['lb'], '/api/hub/runs/' + label + '/summary')
            if code == 200 and summary['completed'] == count and summary['failed'] == 0:
                break
            time.sleep(.25)
        else:
            raise RuntimeError('Benchmark incomplete: ' + json.dumps(summary))
        (out / 'summary.json').write_text(json.dumps({'mode': mode, 'count': count,
            'wallSeconds': time.monotonic() - start, 'summary': summary}, indent=2))
        # Include one export interval after completion; explicit window caveat.
        time.sleep(2)
    finally:
        stop.set()
        thread.join()
    after = {'cpu': cpu_snapshot(), 'statements': json.loads(sql(
        'SELECT json_agg(t) FROM (SELECT queryid,calls,total_exec_time,rows,query FROM pg_stat_statements)t;'))}
    (out / 'after.json').write_text(json.dumps(after, indent=2))
    export_run(out, label)


def workload(run_id, count, mode='unordered'):
    def submit(i):
        for attempt in range(8):
            try:
                code, result = http(PORTS['lb'], '/api/hub/workflows', {
                    'hubId': 'policy-benchmark', 'definitionId': {
                        'unordered': 'policy-unordered', 'keyed': 'policy-keyed-fifo',
                        'partition': 'policy-partition-fifo'}[mode],
                    'version': 1, 'runId': run_id, 'idempotencyKey': f'{run_id}:{i}',
                    'input': {'resourceId': 'account-' + str(i % 8)}}, timeout=15)
                if code == 202:
                    return result
            except Exception:
                pass
            time.sleep(.25 * (attempt + 1))
        raise RuntimeError('Submission failed: ' + str(i))
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        return list(pool.map(submit, range(count)))


OWNER_SQL = """SELECT json_build_object('dbUtc',clock_timestamp(),'owners',
 (SELECT json_agg(row_to_json(o)) FROM (SELECT *,lease_until>clock_timestamp() AS live
 FROM wfc_scale.hub_matching_partition_owner WHERE hub_id='policy-benchmark'
 ORDER BY hub_id,matching_partition)o),
 'tasks',(SELECT json_agg(row_to_json(t)) FROM (SELECT task_type,state,count(*)
 FROM wfc_scale.hub_task WHERE hub_id='policy-benchmark' GROUP BY task_type,state)t),
 'waits',(SELECT json_agg(row_to_json(w)) FROM (SELECT wait_event_type,wait_event,count(*)
 FROM pg_stat_activity WHERE datname=current_database() GROUP BY wait_event_type,wait_event)w));"""


def experiment(label, victim, busy, seconds, fault='kill'):
    out = EVIDENCE / label
    out.mkdir(exist_ok=False)
    start = time.monotonic()
    stop = threading.Event()
    def emit(kind, **data):
        with lock:
            trace.write(json.dumps({'elapsed': time.monotonic() - start,
                'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'kind': kind, **data}) + '\n')
            trace.flush()
    lock = threading.Lock()
    trace = (out / 'timeline.jsonl').open('w', encoding='utf-8')
    def sample():
        while not stop.is_set():
            before = time.monotonic()
            try:
                emit('database', data=json.loads(sql(OWNER_SQL)))
                for service in ['hub', 'hub-2', 'lb']:
                    try:
                        code, data = http(PORTS[service], '/health/ready', timeout=.4)
                        emit('route', service=service, status=code)
                    except Exception as e:
                        emit('route', service=service, error=str(e))
            except Exception as e:
                emit('sample-error', error=str(e))
            stop.wait(max(0, 1 - (time.monotonic() - before)))
    thread = threading.Thread(target=sample)
    thread.start()
    # Independent zero-credit subscriptions expose per-shard stream liveness,
    # session generation and reconnects without consuming or executing tasks.
    def observe(shard):
        body = {'claim': {'workerId': f'lab-observer-{shard}',
            'hubs': ['policy-benchmark'], 'capabilities': [{'hubId': 'policy-benchmark',
                'taskType': '__native_workflow__', 'version': 1}], 'maxTasks': 1,
            'matchingPartition': shard, 'matchingPartitionCount': 4},
            'capacity': 1, 'initialCredits': 0}
        while not stop.is_set():
            req = urllib.request.Request(f'http://127.0.0.1:{PORTS["lb"]}/api/hub/workers/tasks:stream',
                data=json.dumps(body).encode(), headers={'Authorization': 'Bearer ' + TOKEN,
                'Content-Type': 'application/json'})
            try:
                with urllib.request.urlopen(req, timeout=4) as response:
                    emit('stream-open', shard=shard, status=response.status)
                    while not stop.is_set():
                        line = response.readline()
                        if not line:
                            break
                        if line.strip():
                            emit('stream-frame', shard=shard, line=line.decode().strip())
            except Exception as e:
                emit('stream-error', shard=shard, error=str(e))
            stop.wait(.5)
    observers = [threading.Thread(target=observe, args=(s,)) for s in range(4)]
    for observer in observers:
        observer.start()
    logs = []
    for service in ['hub', 'hub-2', 'lb'] + ['worker-' + str(n) for n in range(1, 5)]:
        f = (out / (service + '.txt')).open('w', encoding='utf-8')
        p = subprocess.Popen(['docker', 'logs', '--timestamps', '--since', '2s', '-f', name(service)],
                             stdout=f, stderr=subprocess.STDOUT)
        logs.append((p, f))
    try:
        emit('begin', victim=victim, busy=busy, fault=fault)
        time.sleep(3)
        if fault != 'database':
            owner = 'hub-a' if victim == 'hub' else 'hub-b'
            count = int(sql("SELECT count(*) FROM wfc_scale.hub_matching_partition_owner "
                f"WHERE hub_id='policy-benchmark' AND owner_id='{owner}' AND lease_until>clock_timestamp();"))
            emit('victim-owned-shards', count=count)
            if count == 0:
                raise RuntimeError('Invalid fault precondition: victim owns no live shard')
        if busy:
            future_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = future_pool.submit(workload, label, 120)
            time.sleep(1)
        target = 'pg' if fault == 'database' else victim
        emit('fault-request', victim=target, fault=fault)
        docker('kill' if fault == 'kill' else 'pause', name(target))
        emit('fault-complete', victim=target, fault=fault)
        if fault != 'kill':
            time.sleep(16)
            docker('unpause', name(target))
            emit('resumed', victim=target)
        if not busy:
            time.sleep(12)
            future_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = future_pool.submit(workload, label, 40)
        emit('submitted', ids=future.result())
        while time.monotonic() - start < seconds:
            try:
                code, summary = http(PORTS['lb'], '/api/hub/runs/' + label + '/summary')
                emit('business-summary', status=code, summary=summary)
            except Exception as e:
                emit('summary-error', error=str(e))
            time.sleep(1)
        # Capture persisted task attempts/history/snapshots for later exact assertions.
        export_run(out, label)
        (out / 'db-statements.json').write_text(sql("SELECT json_agg(t) FROM (SELECT queryid,calls,total_exec_time,rows,query FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 40)t;"))
        (out / 'stats.txt').write_text(docker('stats', '--no-stream', '--format', '{{json .}}',
            *[name(x) for x in ['pg', 'hub', 'hub-2'] + ['worker-' + str(n) for n in range(1, 5)]]))
    finally:
        emit('end-before-restart')
        stop.set()
        thread.join()
        for observer in observers:
            observer.join()
        trace.close()
        for p, f in logs:
            p.terminate()
            p.wait()
            f.close()
        if fault == 'kill':
            docker('start', name(victim))
            wait_ready(victim)


def snapshot():
    source = ROOT.parent / 'feat-sse-session-fencing-dynamic-credits-from-pr29'
    files = [source / 'scaling/docs' / n for n in [
        'sse-task-distributor-walkthrough.md', 'sse-scaling-measurement-report.md']]
    files += sorted((source / 'scaling/docs/sse-scaling-evidence').rglob('*'))
    manifest = []
    with zipfile.ZipFile(EVIDENCE / 'historical-evidence.zip', 'x', zipfile.ZIP_DEFLATED) as archive:
        for p in files:
            if p.is_file():
                relative = p.relative_to(source).as_posix()
                data = p.read_bytes()
                manifest.append({'path': relative, 'bytes': len(data),
                                 'sha256': hashlib.sha256(data).hexdigest()})
                archive.writestr(relative, data)
    (EVIDENCE / 'historical-manifest.json').write_text(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['setup', 'experiment', 'snapshot', 'identity', 'upgrade', 'warmup', 'telemetry', 'benchmark'])
    parser.add_argument('--tag', default='baseline')
    parser.add_argument('--project', default=PREFIX)
    parser.add_argument('--compose-project', help='Use labeled Compose containers for measurement/fault actions only')
    parser.add_argument('--evidence-dir', type=Path, default=EVIDENCE)
    parser.add_argument('--port-base', type=int, default=25200)
    parser.add_argument('--label', default='baseline-idle-a-1')
    parser.add_argument('--victim', choices=['hub', 'hub-2'], default='hub')
    parser.add_argument('--busy', action='store_true')
    parser.add_argument('--seconds', type=int, default=85)
    parser.add_argument('--fault', choices=['kill', 'pause', 'database'], default='kill')
    parser.add_argument('--count', type=int, default=200)
    parser.add_argument('--mode', choices=['unordered', 'keyed', 'partition'], default='unordered')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{1,60}', args.project):
        raise ValueError('Project must be a lowercase Docker-safe name')
    if not re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,98}', args.label):
        raise ValueError('Label must be a safe, unique run ID of at most 99 characters')
    PREFIX = args.project
    EVIDENCE = args.evidence_dir.resolve()
    if args.compose_project:
        if args.compose_project != PREFIX or args.action not in ['experiment', 'benchmark', 'warmup']:
            raise ValueError('Compose project must match --project and only supports experiment/benchmark/warmup')
        COMPOSE_PROJECT = args.compose_project
    if not 1024 <= args.port_base <= 65200:
        raise ValueError('Port base must leave room for all loopback ports')
    PORTS = {key: args.port_base + offset for key, offset in
        {'hub': 0, 'hub-2': 1, 'lb': 10, 'pg': 240, 'collector': 89}.items()}
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if COMPOSE_PROJECT:
        containers = [name(s) for s in ['pg', 'hub', 'hub-2', 'lb', 'collector'] +
                      ['worker-' + str(n) for n in range(1, 5)]]
        (EVIDENCE / (args.label + '-containers-' + str(time.time_ns()) + '.json')).write_text(
            docker('inspect', *containers), encoding='utf-8')
    if args.action == 'setup': setup(args.tag)
    elif args.action == 'snapshot': snapshot()
    elif args.action == 'telemetry': telemetry()
    elif args.action == 'identity': identity(args.tag)
    elif args.action == 'warmup': print(workload('warmup-' + str(int(time.time())), 40))
    elif args.action == 'benchmark': benchmark(args.label, args.count, args.mode)
    elif args.action == 'upgrade':
        # Verify build completion before stopping/replacing any owned process.
        docker('image', 'inspect', 'cadence-owner-hub:' + args.tag, 'cadence-owner-worker:' + args.tag)
        for n in range(1, 5):
            docker('stop', name('worker-' + str(n)))
        for service in ['hub', 'hub-2']:
            docker('stop', name(service))
            docker('rm', name(service))
        start_hubs(args.tag)
        for n in range(1, 5):
            docker('rm', name('worker-' + str(n)))
        start_workers(args.tag)
        identity(args.tag)
    else: experiment(args.label, args.victim, args.busy, args.seconds, args.fault)
