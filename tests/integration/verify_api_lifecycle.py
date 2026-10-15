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

"""Real Docker/Spark API lifecycle evidence; custom Sleep is not preset acceptance."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
from hibench import config

destination = Path(os.environ.get('HIBENCH_API_EVIDENCE', 'report/api-lifecycle-2026-10-09')).resolve()
destination.mkdir(parents=True, exist_ok=True)
base = 'http://127.0.0.1:8003'
env = {**os.environ, 'HIBENCH_STATE_DIR': str(destination / 'state'), 'HIBENCH_API_TOKEN': '', 'HIBENCH_START_UI': '0'}
receipt = {'scope': 'real API and detached worker lifecycle; custom Sleep on Docker local[2], not scale/performance acceptance', 'checks': []}
process = None
log = (destination / 'server.log').open('a', encoding='utf-8')


def request(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read()


def get(path):
    return json.loads(request(path))


def start():
    p = subprocess.Popen([sys.executable, '-m', 'hibench.launcher', '--port', '8003'], env=env,
                         stdout=log, stderr=log,
                         **({'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}))
    for _ in range(30):
        if p.poll() is not None:
            raise RuntimeError('API failed to start')
        try:
            get('/health')
            return p
        except Exception:
            time.sleep(1)
    raise TimeoutError('API health timeout')


def wait(run_id, predicate):
    for _ in range(300):
        row = get('/api/v1/runs/' + run_id)
        if predicate(row):
            return row
        if row['state'] in ('failed', 'cancelled'):
            raise RuntimeError(row['detail'])
        time.sleep(2)
    raise TimeoutError('Run lifecycle timeout')


def wait_application(run_id):
    for _ in range(120):
        result = get('/api/v1/runs/' + run_id + '/results')
        references = [e['reference'] for e in result['events'] if e.get('kind') == 'job_reference']
        if references and references[-1]['metadata'].get('application_ids'):
            return references[-1]['metadata']['application_ids']
        if result['run']['state'] in ('failed', 'cancelled', 'succeeded'):
            raise RuntimeError('Run ended before observing an active Spark application')
        time.sleep(2)
    raise TimeoutError('Spark application identity timeout')


try:
    process = start()
    spec = config.default_experiment()
    spec.update(name='api-lifecycle-sleep', mode='run', timeout_s=300,
                workloads=[{'id': 'micro.sleep', 'parameters': {'mapper.seconds': 10}}])
    spec['target']['master'] = 'local[2]'
    spec['resources']['driver_memory'] = '512m'
    created = json.loads(request('/api/v1/runs', {'experiment': spec, 'request_key': 'restart-proof'}))
    run_id = created['run_id']
    receipt['completed_run_id'] = run_id
    wait(run_id, lambda row: row['state'] == 'running' and '/run/' in row['detail'])
    receipt['application_before_restart'] = wait_application(run_id)
    process.terminate()  # Simulate loss of the owned API process; worker must survive.
    process.wait(timeout=20)
    process = start()
    row = wait(run_id, lambda row: row['state'] == 'succeeded')
    results = get('/api/v1/runs/' + run_id + '/results')
    assert results['metrics'] and results['provenance']['effective_workload_parameters']['micro.sleep']['mapper.seconds'] == 10
    (destination / 'completed-results.json').write_text(json.dumps(results, indent=2))
    (destination / 'completed-export.zip').write_bytes(request('/api/v1/runs/' + run_id + '/export'))
    assert get('/api/v1/runs/' + run_id + '/logs')['next_offset'] > 0
    receipt['checks'].append('real Spark run, API process restart, surviving worker, metrics/logs/provenance/export')
    spec['workloads'][0]['parameters']['mapper.seconds'] = 300
    cancelled = json.loads(request('/api/v1/runs', {'experiment': spec, 'request_key': 'cancel-proof'}))['run_id']
    receipt['cancelled_run_id'] = cancelled
    wait(cancelled, lambda row: row['state'] == 'running' and '/run/' in row['detail'])
    receipt['application_before_cancel'] = wait_application(cancelled)
    request('/api/v1/runs/' + cancelled + '/cancel', {})
    row = wait(cancelled, lambda row: row['state'] == 'cancelled')
    receipt['checks'].append('real cancellation acknowledged by worker')
    receipt['status'] = 'passed'
except Exception as exc:
    receipt.update(status='failed', error=repr(exc))
    raise
finally:
    (destination / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    if process is not None and process.poll() is None:
        process.terminate()
        process.wait(timeout=20)
    log.close()
