# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Refresh the durable migration evidence inventory and verify referenced files."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    ledger_path = ROOT / 'docs/reproducibility/migrations.json'
    ledger = json.loads(ledger_path.read_text(encoding='utf-8'))
    names = {ledger['policy'], 'tests/tools/reproducibility_snapshot.py',
             'tests/tools/update_evidence_manifest.py', 'tests/tools/build_acceptance_matrix.py',
             'tests/test_acceptance_matrix.py', 'tests/test_control.py'}
    names.update(ledger['engineering_records'])
    for pattern in ('tests/fixtures/experiments/smoke*.yaml', 'tests/fixtures/*original*.json'):
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
