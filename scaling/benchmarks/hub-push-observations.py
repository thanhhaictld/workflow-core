"""Summarize independently recorded worker starts and sample side effects."""
import collections
import json
import sys
from pathlib import Path

load, executions, effects, output = map(Path, sys.argv[1:5])
workflows = {i['workflow_id'].lower() for i in json.loads(load.read_text(encoding='utf-8-sig'))['items']
             if 'workflow_id' in i}

def read(path):
    if not path.exists():
        raise SystemExit(f'Missing observation file: {path}')
    return [json.loads(line) for line in path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]

starts = [e for e in read(executions) if str(e.get('workflowId', '')).lower() in workflows]
calls = [e for e in read(effects) if str(e.get('workflowId', '')).lower() in workflows]
same_epoch = collections.Counter((e['taskId'], e['epoch']) for e in starts)
stable_task = collections.Counter(e['taskId'] for e in starts)
known = [e for e in calls if e.get('dispatchKey')]
side_key = collections.Counter((e['workflowId'], e['dispatchKey']) for e in known)
result = {'workflowCount': len(workflows), 'executionStarts': len(starts),
          'duplicateSameEpochStarts': sum(n - 1 for n in same_epoch.values() if n > 1),
          'repeatTaskStartsAcrossEpochs': sum(n - 1 for n in stable_task.values() if n > 1),
          'sideEffectInvocations': len(calls), 'sideEffectMissingDispatchKeys': len(calls) - len(known),
          'duplicateSideEffectInvocations': sum(n - 1 for n in side_key.values() if n > 1)}
output.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result))
if result['sideEffectMissingDispatchKeys']:
    raise SystemExit(1)
