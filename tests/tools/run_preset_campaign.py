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

"""Validate named HiBench presets without workload-parameter substitutions."""
import argparse
import copy
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
from hibench.catalog import workloads


def default_spec(path):
    spec = config.load(path)
    if any(item['parameters'] for item in spec['workloads']):
        raise ValueError('Preset validation forbids workload parameter overrides')
    return spec


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('experiment')
    parser.add_argument('destination')
    args = parser.parse_args()
    spec = default_spec(args.experiment)
    destination = Path(args.destination).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    os.environ['HIBENCH_STATE_DIR'] = str(destination / 'state')
    configurations = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted((ROOT / 'conf').rglob('*.conf'))}
    receipt = destination / 'campaign.json'
    catalog = {w['id']: w for w in workloads()}
    if receipt.exists():
        summary = json.loads(receipt.read_text())
        if summary['experiment'] != spec or summary['configuration_sha256'] != configurations:
            raise ValueError('Experiment/default files changed; use a new campaign destination')
    else:
        summary = {'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   'profile': 'default-preset-' + spec['scale'], 'experiment': spec,
                   'method': 'Unmodified named workload presets; one prepare/run per workload for functional acceptance, not a repeated performance study.',
                   'configuration_sha256': configurations, 'runs': [],
                   'artifact_sha256': {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                                       for p in ROOT.glob('**/target/*.jar') if p.name in (
                                           'sparkbench-assembly-2.5.0-dist.jar',
                                           )}}

    def save():
        receipt.write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')

    save()
    for item in spec['workloads']:
        name = item['id']
        run = next((r for r in summary['runs'] if r['workload'] == name), None)
        if run and run['state'] in ('succeeded', 'failed', 'cancelled'):
            continue
        if not run:
            selected = copy.deepcopy(spec)
            selected.update(name=summary['profile'] + '-' + name, workloads=[item])
            run_id, _ = store.create(selected)
            run = {'workload': name, 'run_id': run_id, 'spec': selected, 'state': 'queued',
                   'default_parameters': {k: v['presets'][spec['scale']]
                                         for k, v in catalog[name]['parameters'].items()}}
            summary['runs'].append(run)
            (destination / (name + '.json')).write_text(json.dumps(selected, indent=2) + '\n')
            save()
        run_id = run['run_id']
        if store.get(run_id)['state'] == 'queued':
            worker.launch(run_id)
        print(name + ' started ' + run_id, flush=True)
        while store.get(run_id)['state'] not in ('succeeded', 'failed', 'cancelled'):
            time.sleep(2)
        row = store.get(run_id)
        run.update(state=row['state'], detail=row['detail'], events=worker.events(run_id))
        save()
        print(name + ' ' + row['state'], flush=True)
    summary['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()


if __name__ == '__main__':
    main()
