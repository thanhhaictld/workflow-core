"""Build exact committed sources without changing a branch or making a worktree."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--commit', required=True)
parser.add_argument('--tag', required=True)
parser.add_argument('--evidence-dir', type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parents[3]
out = (args.evidence_dir or root / 'scaling/docs/cadence-inspired-evidence').resolve()
out.mkdir(parents=True, exist_ok=True)
commit = subprocess.run(['git', 'rev-parse', args.commit], cwd=root,
                        capture_output=True, text=True, check=True).stdout.strip()
archive = subprocess.run(['git', 'archive', '--format=tar', commit], cwd=root,
                         capture_output=True, check=True).stdout
identity = {'sourceCommit': commit, 'archiveSha256': hashlib.sha256(archive).hexdigest(),
            'utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
for role, project in [('hub', 'scaling/src/WorkflowCore.Hub.Server/WorkflowCore.Hub.Server.csproj'),
                      ('worker', 'scaling/sample/offboarding/WorkflowCore.Offboarding.Processor/WorkflowCore.Offboarding.Processor.csproj')]:
    image = 'cadence-owner-' + role + ':' + args.tag
    with (out / (args.tag + '-exact-' + role + '-build.txt')).open('xb') as f:
        subprocess.run(['docker', 'build', '-f', 'scaling/observability/Dockerfile.app',
            '--build-arg', 'PROJECT=' + project, '-t', image, '-'], input=archive,
            stdout=f, stderr=subprocess.STDOUT, cwd=root, check=True)
    identity[role] = json.loads(subprocess.run(['docker', 'image', 'inspect', image],
        capture_output=True, text=True, check=True).stdout)
(out / (args.tag + '-exact-source-build.json')).write_text(json.dumps(identity, indent=2))
