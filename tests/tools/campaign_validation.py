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

"""Post-measurement actual-input checks for intermediate-v1 on Docker/YARN."""
import json
from pathlib import Path
import re
import subprocess
import sys


def validate(destination):
    destination = Path(destination).resolve()
    root = Path(__file__).resolve().parents[2]
    campaign = json.loads((destination / 'campaign.json').read_text())
    if 'finished_utc' not in campaign or len(campaign['runs']) != 6 or any(r['state'] != 'succeeded' for r in campaign['runs']):
        raise ValueError('Complete successful campaign required; validation must not overlap measured jobs')
    plans = []
    svd = None
    for run in campaign['runs']:
        name = run['workload']
        spec = run['spec']
        source = [e['path'] for e in run['events'] if e['kind'] == 'input'][-1]
        index = [e['index'] for e in run['events'] if e['kind'] == 'phase_finished' and e['phase'] == 'run'][-1]
        output = spec['target']['hdfs_uri'] + '/HiBench-Control/runs/' + run['run_id'] + '/' + name + '/' + str(index)
        if name in ('sql.join', 'websearch.pagerank'):
            plans.append({'workload': name, 'input': source, 'output': output,
                          'parameters': spec['workloads'][0]['parameters'], 'partitions': 4})
        if name == 'ml.svd':
            params = spec['workloads'][0]['parameters']
            log = (destination / 'state/runs' / run['run_id'] / 'output.log').read_text()
            svd = {'input': source, 'rows': params['examples'], 'dimensions': params['features'],
                   'components': params['singularvalues'],
                   'reported_values': [json.loads(v) for v in re.findall(r'SVD singularValues=(\[[^\r\n]+\])', log)]}
    (destination / 'independent-plans.json').write_text(json.dumps(plans, indent=2))
    (destination / 'svd-plan.json').write_text(json.dumps(svd, indent=2))
    target = campaign['runs'][0]['spec']['target']
    container = target['container']
    remote = '/tmp/hibench-intermediate-' + destination.name
    subprocess.run(['docker', 'exec', container, 'mkdir', '-p', remote], check=True)
    scripts = ['verify_intermediate_data.py', 'verify_candidate_data.py', 'verify_intermediate_svd.py']
    for name in scripts:
        subprocess.run(['docker', 'cp', str(root / 'tests/integration' / name), container + ':' + remote + '/' + name], check=True)
    for name in ('campaign.json', 'independent-plans.json', 'svd-plan.json'):
        subprocess.run(['docker', 'cp', str(destination / name), container + ':' + remote + '/' + name], check=True)
    base = ['docker', 'exec', '-e', 'HADOOP_CONF_DIR=' + target['hadoop_home'] + '/etc/hadoop',
            '-e', 'PYSPARK_PYTHON=python3', container, target['spark_home'] + '/bin/spark-submit',
            '--master', 'yarn', '--deploy-mode', 'client', '--num-executors', '2',
            '--executor-cores', '2', '--executor-memory', '2g', '--driver-memory', '2g',
            '--conf', 'spark.sql.shuffle.partitions=4', '--conf', 'spark.sql.adaptive.enabled=false',
            '--conf', 'spark.dynamicAllocation.enabled=false']
    cases = [('structural', scripts[0], 'campaign.json'),
             ('independent', scripts[1], 'independent-plans.json'),
             ('svd-reference', scripts[2], 'svd-plan.json')]
    receipts = []
    for label, script, plan in cases:
        filename = label + '-results.json'
        command = base + [remote + '/' + script, remote + '/' + plan, remote + '/' + filename]
        print('Post-measurement validation: ' + label, flush=True)
        with (destination / (label + '-validation.log')).open('w', encoding='utf-8') as stream:
            code = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT).returncode
        copied = subprocess.run(['docker', 'cp', container + ':' + remote + '/' + filename,
                                 str(destination / filename)], capture_output=True).returncode == 0
        receipts.append({'case': label, 'returncode': code, 'result_copied': copied})
        print(label + ': returncode=' + str(code), flush=True)
    (destination / 'validation-receipt.json').write_text(json.dumps(receipts, indent=2))
    if any(r['returncode'] or not r['result_copied'] for r in receipts):
        raise RuntimeError('Some validation cases failed; original logs/receipts retained')


if __name__ == '__main__':
    validate(sys.argv[1])
