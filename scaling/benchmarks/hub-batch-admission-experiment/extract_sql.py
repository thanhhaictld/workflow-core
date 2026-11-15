from pathlib import Path
import hashlib
import json
import re
import subprocess

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parents[2]
source_path = REPO / 'scaling/src/WorkflowCore.Hub/HubTaskStore.cs'
src = source_path.read_text(encoding='utf-8')
start = src.index('internal async Task<HubCandidateBatchResult> ClaimCandidateBatchAsync')
end = src.index('// Shared transactional admission', start)
method = src[start:end]
match = re.search(r'new NpgsqlCommand\("""\s*(.*?)\s*""",db,tx\)', method, re.S)
if not match:
    raise SystemExit('Could not extract production raw string')
sql = match.group(1).strip() + '\n'
out = ROOT / 'baseline.sql'
normalize = lambda value: re.sub(r'\s+', ' ', value.strip())
if out.exists() and normalize(out.read_text(encoding='utf-8')) != normalize(sql):
    raise SystemExit('Existing baseline.sql differs from ClaimCandidateBatchAsync raw SQL')
out.write_text(sql, encoding='utf-8', newline='\n')
source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
sql_hash = hashlib.sha256(sql.encode()).hexdigest()
commit = subprocess.check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip()
metadata = {'commit': commit, 'source': str(source_path.relative_to(REPO)).replace('\\', '/'),
            'source_sha256': source_hash, 'method': 'ClaimCandidateBatchAsync',
            'sql_file': 'baseline.sql', 'sql_sha256': sql_hash,
            'source_raw_sql_normalized_sha256': hashlib.sha256(normalize(sql).encode()).hexdigest(),
            'baseline_normalized_sha256': hashlib.sha256(normalize(out.read_text(encoding='utf-8')).encode()).hexdigest(),
            'normalized_extraction_match': normalize(out.read_text(encoding='utf-8')) == normalize(sql)}
(ROOT / 'raw/source-metadata.json').write_text(json.dumps(metadata, indent=2) + '\n', encoding='utf-8')
print('source_sha256=' + source_hash)
print('sql_sha256=' + sql_hash)
print('commit=' + commit)
print(f'lines={len(sql.splitlines())} bytes={len(sql.encode())}')
