"""Package or verify retained ownership evidence without changing raw results.

Run at repository root with Python's standard library. Creation refuses to
replace an existing archive or manifest. Verification reads both historical
and runtime archives and checks every member's path, size and SHA-256.
"""
import argparse
import hashlib
import json
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[3]
EVIDENCE = ROOT / 'scaling/docs/cadence-inspired-evidence'
ARCHIVE = EVIDENCE / 'runtime-evidence.zip'
MANIFEST = EVIDENCE / 'runtime-manifest.json'
OUTSIDE_ARCHIVE = {
    '.gitignore', 'README.md', 'runtime-evidence.zip', 'runtime-manifest.json',
    'archive-integrity.json', 'historical-evidence.zip', 'historical-manifest.json',
}


def verify(archive_path, manifest_path):
    manifest = json.loads(manifest_path.read_text(encoding='utf-8-sig'))
    expected = {entry['path']: entry for entry in manifest}
    assert len(expected) == len(manifest), 'Duplicate manifest path'
    with zipfile.ZipFile(archive_path) as archive:
        assert len(archive.namelist()) == len(expected), 'Duplicate or extra member'
        assert set(archive.namelist()) == set(expected), 'Member set mismatch'
        assert archive.testzip() is None, 'Archive CRC failure'
        for entry in manifest:
            path = Path(entry['path'])
            assert not path.is_absolute() and '..' not in path.parts, 'Unsafe path'
            data = archive.read(entry['path'])
            assert len(data) == entry['bytes'], entry['path'] + ': size mismatch'
            assert hashlib.sha256(data).hexdigest() == entry['sha256'], entry['path'] + ': hash mismatch'
    return {'archive': archive_path.name, 'bytes': archive_path.stat().st_size,
            'sha256': hashlib.sha256(archive_path.read_bytes()).hexdigest(),
            'files': len(manifest), 'uncompressedBytes': sum(e['bytes'] for e in manifest),
            'manifestSha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest()}


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--verify', action='store_true', help='Verify without recreating archives')
args = parser.parse_args()
if not args.verify:
    if ARCHIVE.exists() or MANIFEST.exists():
        raise FileExistsError('Retained package exists; use --verify, never replace raw evidence')
    entries = []
    with zipfile.ZipFile(ARCHIVE, 'x', zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(EVIDENCE.rglob('*')):
            if not path.is_file():
                continue
            relative = path.relative_to(EVIDENCE).as_posix()
            if relative in OUTSIDE_ARCHIVE:
                continue
            data = path.read_bytes()
            entries.append({'path': relative, 'bytes': len(data),
                            'sha256': hashlib.sha256(data).hexdigest()})
            archive.writestr(relative, data)
    with MANIFEST.open('x', encoding='utf-8') as output:
        output.write(json.dumps(entries, indent=2) + '\n')

results = [verify(EVIDENCE / 'historical-evidence.zip', EVIDENCE / 'historical-manifest.json'),
           verify(ARCHIVE, MANIFEST)]
print(json.dumps(results, indent=2))
