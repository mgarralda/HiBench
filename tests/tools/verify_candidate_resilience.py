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

"""Controlled cancellation/crash of this script's own Sleep jobs only."""
import json
import os
from pathlib import Path
import re
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from hibench import config, store, worker, recovery
from hibench.adapters import create_adapter


def wait_for(run_id, condition, timeout=180):
    started = time.monotonic()
    while not condition():
        if time.monotonic() - started > timeout:
            raise TimeoutError('Resilience condition timed out for ' + run_id)
        time.sleep(2)


def main():
    destination = Path(sys.argv[1]).resolve()
    destination.mkdir(parents=True, exist_ok=False)
    os.environ['HIBENCH_STATE_DIR'] = str(destination / 'state')
    spec = config.load(ROOT / 'tests/fixtures/experiments/smoke-micro.yaml')
    spec.update(mode='run', name='candidate-controlled-resilience',
                workloads=[{'id': 'micro.sleep', 'parameters': {'mapper.seconds': 120}}])
    spec = config.validate(spec)
    adapter = create_adapter(spec['target'])
    adapter.doctor()
    checks = []
    for scenario in ('cancel', 'worker_crash'):
        run_id, _ = store.create(spec)
        worker.launch(run_id)
        try:
            # Wait for an actually running tagged YARN job, not just its submit process.
            def active():
                return adapter.command([spec['target']['hadoop_home'] + '/bin/yarn',
                                        'application', '-list', '-appStates', 'RUNNING',
                                        '-appTags', 'hibench-' + run_id])
            listing = ['']
            def running():
                listing[0] = active()
                return bool(re.search(r'application_\d+_\d+', listing[0]))
            wait_for(run_id, running)
            apps = sorted(set(re.findall(r'application_\d+_\d+', listing[0])))
            if scenario == 'cancel':
                store.cancel(run_id)
                wait_for(run_id, lambda: store.get(run_id)['state'] in ('cancelled', 'failed'))
                assert store.get(run_id)['state'] == 'cancelled'
            else:
                pid = store.get(run_id)['worker_pid']
                assert pid and pid != os.getpid()
                os.kill(pid, signal.SIGTERM)
                wait_for(run_id, lambda: not recovery.process_alive(pid), timeout=30)
                assert store.get(run_id)['state'] == 'running'
                recovery.reconcile(run_id)
                assert store.get(run_id)['state'] == 'failed'
            statuses = {app: adapter.command([spec['target']['hadoop_home'] + '/bin/yarn',
                                              'application', '-status', app]) for app in apps}
            assert all('Final-State : KILLED' in value for value in statuses.values())
            # A fresh run can acquire the destination after cleanup.
            next_id, _ = store.create(spec)
            assert store.acquire(next_id, spec['target'])
            store.release(next_id)
            store.update(next_id, 'cancelled', 'Lease probe only; no job submitted')
            checks.append({'scenario': scenario, 'run_id': run_id, 'state': store.get(run_id)['state'],
                           'applications': statuses, 'lease_reusable': True,
                           'events': worker.events(run_id)})
            (destination / 'resilience.json').write_text(json.dumps(checks, indent=2) + '\n', encoding='utf-8')
            print(scenario + ' verified', flush=True)
        except Exception:
            # Avoid deliberately leaving the controlled Sleep application running.
            store.cancel(run_id)
            if not recovery.process_alive(store.get(run_id)['worker_pid']):
                recovery.reconcile(run_id)
            raise


if __name__ == '__main__':
    main()
