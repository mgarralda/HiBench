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

"""Exercise the built Compose image, real Spark submission and host persistence.
Uses alternate ports and a separate Compose project. Run only with an idle cluster.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
ENV = {**os.environ, 'HIBENCH_API_TOKEN': secrets.token_urlsafe(32),
       'HIBENCH_API_PORT': '18800', 'HIBENCH_UI_PORT': '18851'}
CMD = ['docker', 'compose', '-p', 'hibench-next-validation', '-f', str(ROOT / 'deployment/compose.yml')]


def command(*args):
    return subprocess.run([*CMD, *args], env=ENV, text=True, capture_output=True, check=True)


def request(path, data=None):
    payload = json.dumps(data).encode() if data is not None else None
    req = urllib.request.Request('http://127.0.0.1:18800/api/v1/' + path, data=payload,
        headers={'Authorization': 'Bearer ' + ENV['HIBENCH_API_TOKEN'], 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=180) as response:
        return json.load(response)


def main():
    from hibench import store
    assert not any(r['state'] in ('queued', 'waiting', 'running') for r in store.recent()), 'Cluster is busy'
    proof = ROOT / 'conf/.deployment-persistence-proof'
    assert not proof.exists(), 'Unexpected existing proof file'
    receipt = {}
    try:
        command('config', '--quiet')
        command('up', '-d', '--no-build', '--wait', '--wait-timeout', '120')
        with urllib.request.urlopen('http://127.0.0.1:18851/_stcore/health') as response:
            assert response.read() == b'ok'
        schema_request = urllib.request.Request('http://127.0.0.1:18800/openapi.json')
        with urllib.request.urlopen(schema_request, timeout=30) as response:
            schema = json.load(response)
        assert 'ExperimentPreset' in schema['components']['schemas']
        assert 'SparkHistoryLinks' in schema['components']['schemas']
        assert 'ApiError' in schema['components']['schemas']
        receipt['openapi_paths'] = len(schema['paths'])
        receipt['pydantic_schemas'] = len(schema['components']['schemas'])
        receipt['image_id'] = subprocess.run(['docker', 'image', 'inspect', 'hibench-next:local', '--format', '{{.Id}}'], text=True, capture_output=True, check=True).stdout.strip()
        spec = request('experiments/defaults?scale=tiny')
        spec['name'] = 'Deployment validation: canonical tiny Sleep'
        spec['workloads'] = [{'id': 'micro.sleep', 'parameters': {}}]
        receipt['install'] = request('diagnostics/install', spec)['output']
        receipt['doctor_ok'] = 'HIBENCH_DOCTOR_OK' in request('diagnostics/doctor', spec)['output']
        assert receipt['doctor_ok']
        run_id = request('runs', {'experiment': spec})['run_id']
        receipt['run_id'] = run_id
        deadline = time.monotonic() + 240
        while time.monotonic() < deadline:
            run = request('runs/' + run_id)
            if run['state'] in ('succeeded', 'failed', 'cancelled'):
                break
            time.sleep(2)
        assert run['state'] == 'succeeded', run
        receipt['run_state'] = run['state']
        assert (ROOT / 'report/control/runs' / run_id / 'experiment.json').exists()
        run_dir = ROOT / 'report/control/runs' / run_id
        html_reports = list(run_dir.rglob('report.html'))
        assert html_reports, 'Resource HTML report missing'
        receipt['resource_reports_on_host'] = len(html_reports)
        marker = secrets.token_hex(12)
        proof.write_text(marker)
        command('exec', '-T', 'hibench', 'python', '-c', "from pathlib import Path; assert Path('/opt/hibench/conf/.deployment-persistence-proof').exists()")
        command('up', '-d', '--force-recreate', '--no-build', '--wait', '--wait-timeout', '120')
        assert request('runs/' + run_id)['state'] == 'succeeded'
        assert all(path.exists() for path in html_reports)
        result = command('exec', '-T', 'hibench', 'python', '-c', "from pathlib import Path; print(Path('/opt/hibench/conf/.deployment-persistence-proof').read_text())")
        assert result.stdout.strip() == marker
        receipt['persistent_configuration_and_runs_after_recreate'] = True
        receipt['scope'] = 'Deployment integration, standard tiny Sleep; not a performance campaign'
        print(json.dumps(receipt, indent=2))
        (ROOT / 'report/deployment-validation.json').write_text(json.dumps(receipt, indent=2) + '\n', encoding='utf-8')
    finally:
        if proof.exists():
            proof.unlink()
        command('down')


if __name__ == '__main__':
    main()
