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

"""Sequential bounded acceptance on Docker/YARN; keep each failed receipt."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hibench import config, store, worker
from hibench.catalog import IDS

SOURCES = ('smoke-micro.yaml', 'smoke-sql.yaml', 'smoke-als.yaml',
           'smoke-clustering.yaml', 'smoke-linear-overrides.yaml',
           'smoke-lr-overrides.yaml', 'smoke-trees-overrides.yaml',
           'smoke-statistics-overrides.yaml', 'smoke-final-ml-overrides.yaml',
           'smoke-svd-overrides.yaml', 'smoke-nweight-overrides.yaml')


def main():
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=False)
    os.environ['HIBENCH_STATE_DIR'] = str(destination / 'state')
    specifications = {}
    for source in SOURCES:
        base = config.load(ROOT / 'tests/fixtures/experiments' / source)
        for item in base['workloads']:
            spec = {**base, 'workloads': [item], 'name': 'candidate-acceptance-' + item['id']}
            specifications[item['id']] = config.validate(spec)
    assert set(specifications) == set(IDS)
    summary = {'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'purpose': 'Functional bounded acceptance, not performance study',
               'runs': [], 'artifact_sha256': {}}
    for path in ROOT.glob('**/target/*jar'):
        if path.name in ('sparkbench-assembly-2.0.0-dist.jar',
                         ):
            summary['artifact_sha256'][path.relative_to(ROOT).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    for name in IDS:
        spec = specifications[name]
        spec_path = destination / (name + '.json')
        spec_path.write_text(json.dumps(spec, indent=2) + '\n', encoding='utf-8')
        run_id, _ = store.create(spec)
        worker.launch(run_id)
        print(name + ' started ' + run_id, flush=True)
        while store.get(run_id)['state'] not in ('succeeded', 'failed', 'cancelled'):
            time.sleep(2)
        row = store.get(run_id)
        record = {'workload': name, 'run_id': run_id, 'state': row['state'],
                  'detail': row['detail'], 'spec': row['spec'], 'events': worker.events(run_id)}
        summary['runs'].append(record)
        (destination / 'campaign.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
        print(name + ' ' + row['state'], flush=True)
    summary['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    (destination / 'campaign.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')


if __name__ == '__main__':
    main()
