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

"""Serial, resumable intermediate campaign. First execution is excluded a priori."""
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

PROFILES = {
    'micro.terasort': {'datasize': 1000000},
    'sql.join': {'pages': 5000, 'uservisits': 50000},
    'ml.kmeans': {'num_of_samples': 100000, 'dimensions': 8,
                  'num_of_clusters': 5, 'max_iteration': 5, 'seed': 1234},
    'ml.svd': {'examples': 5000, 'features': 64, 'singularvalues': 16,
               'seed': 17, 'computeU': False},
    'websearch.pagerank': {'pages': 2000, 'num_iterations': 3, 'seed': 1},
    'graph.nweight': {'edges': 200000, 'degree': 3, 'max_out_edges': 30, 'seed': 42},
}


def main():
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    os.environ['HIBENCH_STATE_DIR'] = str(destination / 'state')
    receipt = destination / 'campaign.json'
    summary = json.loads(receipt.read_text()) if receipt.exists() else {
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'profile': 'intermediate-v1', 'runs': [],
        'method': 'One preparation, one excluded initial execution, three measured executions per workload. Serial fresh Spark applications; OS/HDFS caches uncontrolled.',
        'artifact_sha256': {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in ROOT.glob('**/target/*.jar') if p.name in (
                                'sparkbench-assembly-2.5.0-dist.jar',
                                )}}

    def save():
        receipt.write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')

    for name, parameters in PROFILES.items():
        existing = next((r for r in summary['runs'] if r['workload'] == name), None)
        if existing and existing['state'] in ('succeeded', 'failed', 'cancelled'):
            continue
        if existing:
            run_id = existing['run_id']
            if store.get(run_id)['state'] == 'queued':
                worker.launch(run_id)
        else:
            spec = config.load(ROOT / 'tests/fixtures/experiments/smoke-micro.yaml')
            spec.update(name='intermediate-v1-' + name, repetitions=4, timeout_s=1800,
                        workloads=[{'id': name, 'parameters': parameters}])
            spec['resources'].update(executor_instances=2, executor_cores=2,
                                     executor_memory='2g', driver_memory='2g',
                                     generation_partitions=4, generation_shuffle_partitions=4,
                                     shuffle_partitions=4)
            spec['spark_conf'].update({'spark.speculation': False,
                                      'spark.eventLog.enabled': True})
            spec = config.validate(spec)
            (destination / (name + '.json')).write_text(json.dumps(spec, indent=2) + '\n')
            run_id, _ = store.create(spec)
            existing = {'workload': name, 'run_id': run_id, 'state': 'submitted', 'spec': spec}
            summary['runs'].append(existing)
            save()
            worker.launch(run_id)
        print(name + ' started ' + run_id, flush=True)
        while store.get(run_id)['state'] not in ('succeeded', 'failed', 'cancelled'):
            time.sleep(2)
        row = store.get(run_id)
        existing.update(state=row['state'], detail=row['detail'], events=worker.events(run_id))
        save()
        print(name + ' ' + row['state'], flush=True)
    summary['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    save()


if __name__ == '__main__':
    main()
