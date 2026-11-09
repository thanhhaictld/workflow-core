"""Validate retained lab evidence. Results distinguish invalid and stalled runs."""
from collections import Counter, defaultdict
import argparse
import datetime as dt
import json
import math
import re
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-dir', type=Path,
    default=Path(__file__).resolve().parents[3] / 'scaling/docs/cadence-inspired-evidence')
root = parser.parse_args().evidence_dir.resolve()


def stamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp()


def percentile(values, fraction):
    values = sorted(values)
    if not values:
        return None
    index = (len(values) - 1) * fraction
    lo, hi = int(index), math.ceil(index)
    return values[lo] + (values[hi] - values[lo]) * (index - lo)


def prometheus(path):
    result = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        match = re.match(r'^(\w+)(\{.*\})? ([^ ]+)', line)
        if match:
            result[(match[1], match[2] or '')] = float(match[3])
    return result


def metric_window(directory):
    files = sorted(directory.glob('metrics-*.prom'))
    if not files:
        return {}
    before, after = prometheus(files[0]), prometheus(files[-1])
    def delta(name, selector=lambda labels: True):
        return sum(max(0, value-before.get((metric, labels), 0))
            for (metric, labels), value in after.items() if metric == name and selector(labels))
    stages = {}
    for name in ['claim_duration', 'claim_candidate_query', 'claim_db_connection_acquire',
                 'claim_admission_wait', 'matching_dispatch_duration', 'worker_completion_http_duration']:
        prefix = 'workflow_hub_' + name + '_seconds'
        count = delta(prefix + '_count')
        buckets = {}
        for metric, labels in after:
            if metric != prefix + '_bucket':
                continue
            bound = float(re.search(r'le="([^"]+)"', labels)[1])
            buckets[bound] = buckets.get(bound, 0) + max(0, after[(metric, labels)]-before.get((metric, labels), 0))
        stages[name] = {'observations': count, 'meanMs': delta(prefix + '_sum') / count * 1000 if count else None,
            'p95BucketCeilingMs': next((b*1000 for b, n in sorted(buckets.items()) if n >= .95*count), None) if count else None}
    gauges = []
    connections = []
    for file in files:
        metrics = prometheus(file)
        gauges.append(sum(v for (n, _), v in metrics.items() if n == 'workflow_hub_sse_task_sessions'))
        connections.append(sum(v for (n, labels), v in metrics.items() if n == 'http_client_open_connections'
            and 'job="workflow-hub"' in labels and 'http_connection_state="active"' in labels
            and ('server_address="hub"' in labels or 'server_address="hub-2"' in labels)))
    return {'firstFile': files[0].name, 'lastFile': files[-1].name, 'stages': stages,
        'sseSessionsSampleRange': [min(gauges), max(gauges)],
        'activeInterHubHttpConnectionSampleRangeIncludingControl': [min(connections), max(connections)],
        'emptyDispatchObservations': delta('workflow_hub_matching_dispatch_duration_seconds_count', lambda labels: 'outcome="empty"' in labels)}


def audit(directory):
    workflows = json.loads((directory / 'hub_workflow.json').read_text())
    tasks = json.loads((directory / 'hub_task.json').read_text())
    attempts = json.loads((directory / 'hub_task_attempt.json').read_text())
    events = json.loads((directory / 'hub_workflow_event.json').read_text())
    completions = Counter(e['workflow_id'] for e in events if e['event_type'] == 'WorkflowCompleted')
    results = Counter(e['workflow_id'] for e in events if e['event_type'] == 'ActivityResultReceived')
    assert all(v == 1 for v in completions.values()), directory.name + ': duplicate completion'
    assert all(v == 1 for v in results.values()), directory.name + ': duplicate durable activity result'
    complete = [w for w in workflows if w['state'] == 2]
    for w in complete:
        assert w['native_snapshot']['Data']['value']['Completed'] is True
        assert completions[w['id']] == 1
        assert results[w['id']] == 1
    by_task = Counter(a['task_id'] for a in attempts)
    keyed = {t['id']: t for t in tasks if t['task_type'] == 'policy-keyed-step'}
    fifo = {}
    if keyed:
        groups = defaultdict(list)
        for attempt in attempts:
            if attempt['task_id'] in keyed:
                task = keyed[attempt['task_id']]
                groups[task['payload']['resourceId']].append((task, attempt))
        inversions = overlaps = spanning = 0
        for entries in groups.values():
            entries.sort(key=lambda pair: stamp(pair[1]['claimed_at']))
            spanning += len({t['partition_id'] for t, _ in entries}) > 1
            for (previous, a), (current, b) in zip(entries, entries[1:]):
                inversions += current['sequence'] < previous['sequence']
                overlaps += a['finished_at'] is None or stamp(a['finished_at']) > stamp(b['claimed_at'])
        assert inversions == overlaps == 0, directory.name + ': keyed FIFO/capacity violation'
        fifo = {'keys': len(groups), 'keysSpanningPartitions': spanning,
                'sequenceInversions': inversions, 'overlappingAttemptIntervals': overlaps}
    return {'submitted': len(workflows), 'completed': len(complete), 'keyedFifo': fifo,
        'failed': sum(w['state'] == 3 for w in workflows),
        'activityAttempts': len(attempts), 'extraActivityAttempts': sum(max(0, n-1) for n in by_task.values()),
        'durableCompletions': sum(completions.values()), 'durableResults': sum(results.values()),
        'taskStates': dict(Counter(t['state'] for t in tasks))}


def recovery(directory):
    rows = [json.loads(line) for line in (directory / 'timeline.jsonl').read_text().splitlines()]
    begin = next(r for r in rows if r['kind'] == 'begin')
    fault = next((r for r in rows if r['kind'] in ['fault-complete', 'kill-complete']), None)
    owned = [r['count'] for r in rows if r['kind'] == 'victim-owned-shards']
    excluded = directory.name in ['modified-idle-a-1', 'modified-idle-a-2']
    result = {'label': directory.name, 'valid': not excluded and fault is not None,
        'victim': begin['victim'], 'busy': begin['busy'], 'fault': begin.get('fault', 'kill'),
        'preconditionOwnedShards': owned, 'contaminatedByConcurrentTests': directory.name == 'baseline-idle-a-1'}
    if (directory / 'hub_workflow.json').exists():
        result.update(audit(directory))
    if not result['valid']:
        result['exclusion'] = 'missing workers' if directory.name.endswith('a-1') else 'victim had no owned shards'
        return result
    fault_at = fault['elapsed']
    result['faultElapsed'] = fault_at
    before = [r for r in rows if r['kind'] == 'database' and r['elapsed'] < fault_at][-1]
    previous = {o['matching_partition']: o for o in before['data']['owners'] if o['hub_id'] == 'policy-benchmark'}
    victim = 'hub-a' if begin['victim'] == 'hub' else 'hub-b'
    affected = list(previous) if result['fault'] == 'database' else [k for k, o in previous.items() if o['owner_id'] == victim and o['live']]
    result['shards'] = []
    events = json.loads((directory / 'hub_workflow_event.json').read_text())
    workflows = {w['id']: w for w in json.loads((directory / 'hub_workflow.json').read_text())}
    for shard in affected:
        expired = None
        acquired = None
        new_owner = None
        for r in rows:
            if r['kind'] != 'database' or r['elapsed'] <= fault_at:
                continue
            o = next((o for o in r['data']['owners'] or [] if o['hub_id'] == 'policy-benchmark' and o['matching_partition'] == shard), None)
            if o is None:
                continue
            if not o['live'] and expired is None:
                expired = r['elapsed'] - fault_at
            if o['live'] and o['epoch'] > previous[shard]['epoch']:
                acquired = r['elapsed'] - fault_at
                new_owner = o
                break
        stream = None
        if new_owner:
            stream = next((r['elapsed'] - fault_at for r in rows if r['kind'] == 'stream-frame'
                and r['shard'] == shard and r['elapsed'] > fault_at and r['line'].startswith('data: ')
                and json.loads(r['line'][6:]).get('ownerEpoch') == new_owner['epoch']), None)
        def first_event(kind):
            if not new_owner:
                return None
            found = [e['occurred_at'] for e in events if e['event_type'] == kind
                and workflows[e['workflow_id']]['partition_id'] % 4 == shard
                and stamp(e['occurred_at']) >= stamp(new_owner['updated_at'])]
            return min(found, key=stamp) if found else None
        disconnected = next((r['elapsed'] - fault_at for r in rows if r['kind'] == 'stream-error'
            and r['shard'] == shard and r['elapsed'] > fault_at), None)
        result['shards'].append({'shard': shard, 'oldEpoch': previous[shard]['epoch'],
            'oldOwner': previous[shard]['owner_id'], 'lastObservedExpiry': previous[shard]['lease_until'],
            'firstExpiredSampleAfterFaultSeconds': expired, 'newOwnerSampleAfterFaultSeconds': acquired,
            'newOwner': new_owner['owner_id'] if new_owner else None,
            'newEpoch': new_owner['epoch'] if new_owner else None,
            'newSessionAfterFaultSeconds': stream,
            'firstObserverDisconnectAfterFaultSeconds': disconnected,
            'firstDurableWorkflowClaimAfterAcquisitionDbUtc': first_event('WorkflowTaskClaimed'),
            'firstActivityResultAfterAcquisitionDbUtc': first_event('ActivityResultReceived'),
            'firstWorkflowCompletionAfterAcquisitionDbUtc': first_event('WorkflowCompleted')})
    summaries = [r for r in rows if r['kind'] == 'business-summary']
    result['lastSummary'] = summaries[-1]['summary'] if summaries else None
    result['windowSeconds'] = rows[-1]['elapsed']
    if directory.name.startswith('modified') and result['valid']:
        assert result['shards'], directory.name + ': no affected owner generation observed'
        assert all(s['newEpoch'] is not None for s in result['shards']), directory.name + ': failed acquisition'
        assert result['completed'] == result['submitted'], directory.name + ': incomplete business outcomes'
        assert result['failed'] == 0
    return result


recovered = []
benchmarks = []
for directory in sorted(root.iterdir()):
    if not directory.is_dir():
        continue
    if (directory / 'timeline.jsonl').exists():
        if not (directory / 'hub_workflow.json').exists():
            continue  # An experiment may still be collecting its window.
        recovered.append(recovery(directory))
    elif (directory / 'summary.json').exists():
        result = json.loads((directory / 'summary.json').read_text())
        result['label'] = directory.name
        result['audit'] = audit(directory)
        summary = result['summary']
        result['serverSeconds'] = stamp(summary['lastCompletedAt']) - stamp(summary['firstCreatedAt'])
        result['workflowsPerSecond'] = result['count'] / result['serverSeconds']
        before = json.loads((directory / 'before.json').read_text())
        after = json.loads((directory / 'after.json').read_text())
        result['cpuSecondsIncludingSamplingTail'] = {}
        for service, raw in after['cpu'].items():
            def usage(text):
                return int(dict(line.split() for line in text.splitlines())['usage_usec'])
            result['cpuSecondsIncludingSamplingTail'][service] = (usage(raw) - usage(before['cpu'][service])) / 1e6
        # The same queryid can occur in several test databases/users. Never
        # overwrite duplicate rows and subtract the last row multiple times.
        # These snapshots omit dbid/userid, so aggregate all rows per queryid
        # and report an all-database window rather than claim per-DB attribution.
        old = Counter()
        current = Counter()
        for statement in before['statements']:
            old[statement['queryid']] += statement['calls']
        for statement in after['statements']:
            current[statement['queryid']] += statement['calls']
        decreases = [key for key in old if current[key] < old[key]]
        delta_calls = sum(current.values()) - sum(old.values())
        result['dbStatementCounterDecreases'] = len(decreases)
        result['dbCallsPerWorkflowIncludingBackground'] = delta_calls / result['count'] if not decreases else None
        result['metricWindow'] = metric_window(directory)
        tasks = {t['id']: t for t in json.loads((directory / 'hub_task.json').read_text())}
        attempts = json.loads((directory / 'hub_task_attempt.json').read_text())
        waits = [stamp(a['claimed_at'])-stamp(tasks[a['task_id']]['created_at']) for a in attempts]
        result['activityCreatedToFirstLeaseSeconds'] = {'p50': percentile(waits, .5), 'p95': percentile(waits, .95)}
        samples = [json.loads(line) for line in (directory / 'samples.jsonl').read_text().splitlines()]
        wait_samples = Counter()
        for sample in samples:
            for wait in sample.get('db', {}).get('waits') or []:
                wait_samples[str(wait['wait_event_type']) + '/' + str(wait['wait_event'])] += wait['count']
        result['dbWaitPointSampleCountsNotDurations'] = dict(wait_samples)
        result['samplingErrors'] = sum('error' in sample for sample in samples)
        assert result['audit']['completed'] == result['count']
        benchmarks.append(result)
(root / 'analysis.json').write_text(json.dumps({'faults': recovered, 'benchmarks': benchmarks}, indent=2))
for r in recovered:
    print(r['label'], 'valid=' + str(r['valid']), r.get('completed'), '/', r.get('submitted'),
          'takeover=', [s['newOwnerSampleAfterFaultSeconds'] for s in r.get('shards', [])])
for b in benchmarks:
    print(b['label'], round(b['workflowsPerSecond'], 2), 'wf/s', b['audit'])
