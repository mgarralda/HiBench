# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Explicit immutable evidence backup, including Git-ignored documentation."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import sys
import zipfile


def archive(destination, source_archive):
    root = Path(__file__).resolve().parents[2]
    destination = Path(destination).resolve()
    source_archive = Path(source_archive).resolve()
    campaign = json.loads((destination / 'campaign.json').read_text())
    tag = campaign['profile']
    durable = root / 'docs/reproducibility/evidence/2026-10-09' / tag
    durable.mkdir(parents=True, exist_ok=True)
    for path in destination.iterdir():
        if path.is_file() and path.suffix in ('.json', '.log', '.txt'):
            shutil.copy2(path, durable / path.name)
    paths = [p for p in durable.iterdir() if p.is_file() and p.suffix != '.zip'
             and not p.name.startswith('evidence-manifest') and not p.name.endswith('.sha256')]
    paths += [p for p in (root / 'docs').rglob('*') if p.is_file()
              and durable not in p.parents and 'candidate-source-evidence' not in p.parts
              and p.suffix != '.zip']
    paths += list((root / 'docs_internal').glob('*.md'))
    paths += [root / p for p in (
        'tests/tools/run_intermediate_campaign.py', 'tests/fixtures/experiments/intermediate-representative.yaml',
        'tests/tools/measurement.py', 'tests/tools/campaign_measurement.py',
        'tests/tools/campaign_validation.py', 'tests/tools/campaign_archive.py',
        'tests/test_measurement.py', 'tests/integration/verify_intermediate_data.py',
        'tests/integration/verify_candidate_data.py', 'tests/integration/verify_intermediate_svd.py')]
    paths += [source_archive, root / 'report/reproducibility/20261009T071213146576Z/hibench-next-source.zip']
    if tag.startswith('default-preset-'):
        paths += [root / p for p in ('tests/tools/run_preset_campaign.py', 'tests/tools/preset_verification.py',
                                    'tests/test_preset_campaign.py', 'tests/integration/verify_svd_controller.py',
                                    'tests/integration/verify_nweight_controller.py')]
        paths += list((root / 'tests/fixtures/experiments').glob('preset-*.yaml'))
    paths += list((destination / 'state/runs').glob('*/output.log'))
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output = durable / ('evidence-' + stamp + '.zip')
    manifest = {'created_utc': stamp, 'scope': tag + ' local campaign; explicit documentation/source backup; no HDFS data or Docker volumes', 'files': {}}
    with zipfile.ZipFile(output, 'x', zipfile.ZIP_DEFLATED) as bundle:
        for path in sorted(set(paths)):
            name = path.relative_to(root).as_posix()
            data = path.read_bytes()
            manifest['files'][name] = {'sha256': hashlib.sha256(data).hexdigest(), 'bytes': len(data)}
            bundle.writestr(name, data)
        bundle.writestr('evidence-manifest.json', json.dumps(manifest, indent=2))
    with zipfile.ZipFile(output) as bundle:
        for name, record in manifest['files'].items():
            assert hashlib.sha256(bundle.read(name)).hexdigest() == record['sha256'], name
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix('.sha256').write_text(digest + '  ' + output.name + '\n')
    (durable / 'evidence-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'archive': str(output), 'sha256': digest, 'verified_files': len(manifest['files'])}, indent=2))


if __name__ == '__main__':
    archive(sys.argv[1], sys.argv[2])
