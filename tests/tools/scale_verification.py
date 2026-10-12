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

"""Run distributed preset integrity checks after workload execution finishes."""
import json
from pathlib import Path
import subprocess
import sys


def verify(destination):
    destination = Path(destination).resolve()
    campaign = json.loads((destination / 'campaign.json').read_text())
    if 'finished_utc' not in campaign:
        raise ValueError('Wait for the campaign to finish before launching verification')
    target = campaign['experiment']['target']
    container = target['container']
    remote = '/tmp/hibench-scale-' + destination.name
    subprocess.run(['docker', 'exec', container, 'mkdir', '-p', remote], check=True)
    script = Path(__file__).resolve().parents[2] / 'tests/integration/verify_scale_data.py'
    for path in (script, destination / 'campaign.json'):
        subprocess.run(['docker', 'cp', str(path), container + ':' + remote + '/' + path.name], check=True)
    command = ['docker', 'exec', '-e', 'HADOOP_CONF_DIR=' + target['hadoop_home'] + '/etc/hadoop',
               '-e', 'PYSPARK_PYTHON=python3', container, target['spark_home'] + '/bin/spark-submit',
               '--master', 'yarn', '--deploy-mode', 'client', '--num-executors', '2',
               '--executor-cores', '2', '--executor-memory', '2g', '--driver-memory', '2g',
               '--conf', 'spark.sql.shuffle.partitions=4', '--conf', 'spark.dynamicAllocation.enabled=false',
               remote + '/' + script.name, remote + '/campaign.json', remote + '/results.json']
    with (destination / 'scale-verification.log').open('w', encoding='utf-8') as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT)
    copied = subprocess.run(['docker', 'cp', container + ':' + remote + '/results.json', str(destination / 'scale-results.json')], capture_output=True)
    checks = json.loads((destination / 'scale-results.json').read_text()) if copied.returncode == 0 else []
    receipt = {'returncode': result.returncode, 'expected_workloads': len(campaign['runs']),
               'received_workloads': len(checks), 'checks_passed': bool(checks) and result.returncode == 0
               and len(checks) == len(campaign['runs']) and all(r['status'] == 'passed' for r in checks),
               'scope': 'distributed integrity only; per-workload limitations recorded in scale-results.json'}
    (destination / 'scale-verification-receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt), flush=True)


if __name__ == '__main__':
    verify(sys.argv[1])
