"""Refresh the durable migration evidence inventory and verify referenced files."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    ledger_path = ROOT / 'docs/reproducibility/migrations.json'
    ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
    names = {ledger['policy'], 'bin/reproducibility_snapshot.py',
             'bin/update_evidence_manifest.py'}
    names.update(ledger['engineering_records'])
    for pattern in ('configs/smoke*.yaml', 'tests/fixtures/*original*.json'):
        names.update(path.relative_to(ROOT).as_posix() for path in ROOT.glob(pattern))
    for record in ledger['records']:
        for field in ('documentation', 'validation', 'historical_context'):
            if field in record:
                names.add(record[field])
        for field in ('scale_or_profile_reference', 'verifiers', 'implementation'):
            names.update(record.get(field, []))
    for path in (ROOT / 'docs/reproducibility').rglob('*'):
        if path.is_file() and path.name != 'evidence-manifest.json':
            names.add(path.relative_to(ROOT).as_posix())
    files = {}
    for name in sorted(names):
        path = (ROOT / name).resolve()
        if ROOT not in path.parents or not path.is_file():
            raise ValueError(f'Missing or unsafe evidence path: {name}')
        data = path.read_bytes()
        files[name] = {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
    result = {
        'schema_version': 1,
        'baseline_revision': subprocess.check_output(
            ['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip(),
        'scope': 'Current working-tree evidence, including changes after baseline revision; not a release certification.',
        'companion_cluster_revision': ledger['companion_cluster_revision'],
        'files': files,
    }
    destination = ROOT / 'docs/reproducibility/evidence-manifest.json'
    destination.write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
    print(f'Inventoried {len(files)} evidence files: {destination}')


if __name__ == '__main__':
    main()
