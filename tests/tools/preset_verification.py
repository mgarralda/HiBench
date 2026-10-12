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

"""Independent checks after a default-preset campaign; preserve failures."""
import json
from pathlib import Path
import subprocess
import sys
from hibench.catalog import workloads
from hibench.workload_parameters import typed_value


def verify(destination):
    destination = Path(destination).resolve()
    root = Path(__file__).resolve().parents[2]
    campaign = json.loads((destination / 'campaign.json').read_text())
    if 'finished_utc' not in campaign:
        raise ValueError('Wait for the preset campaign to finish before reference jobs')
    catalog = {w['id']: w for w in workloads()}
    plans, svd, nweight = [], [], []
    for run in campaign['runs']:
        if run['state'] != 'succeeded':
            continue
        spec = run['spec']
        name = run['workload']
        parameters = {k: typed_value(v, run['default_parameters'][k])
                      for k, v in catalog[name]['parameters'].items()
                      if '${' not in str(run['default_parameters'][k])}
        inputs = [e['path'] for e in run['events'] if e['kind'] == 'input']
        indices = [e['index'] for e in run['events'] if e['kind'] == 'phase_finished' and e['phase'] == 'run']
        output = spec['target']['hdfs_uri'] + f"/HiBench-Control/runs/{run['run_id']}/{name}/{indices[-1]}"
        plan = {'workload': name, 'input': inputs[-1] if inputs else None, 'output': output,
                'parameters': parameters, 'partitions': spec['resources']['shuffle_partitions']}
        if name == 'micro.repartition' and parameters['disableoutput']:
            app = [e['metrics']['application_id'] for e in run['events'] if e['kind'] == 'result'][-1]
            uri = spec['spark_conf']['spark.eventLog.dir'].rstrip('/') + '/' + app
            raw = subprocess.run(['docker', 'exec', spec['target']['container'],
                                  spec['target']['hadoop_home'] + '/bin/hdfs', 'dfs', '-cat', uri],
                                 capture_output=True, check=True).stdout
            stages = {}
            for line in raw.splitlines():
                event = json.loads(line)
                if event.get('Event') == 'SparkListenerTaskEnd' and event.get('Task End Reason', {}).get('Reason') == 'Success':
                    stage = str(event['Stage ID'])
                    stages[stage] = stages.get(stage, 0) + event.get('Task Metrics', {}).get('Shuffle Write Metrics', {}).get('Shuffle Records Written', 0)
            plan['successful_shuffle_records_by_stage'] = stages
        plans.append(plan)
        if name == 'ml.svd':
            svd.append({'path': plan['input'], 'examples': parameters['examples'],
                        'dimensions': parameters['features'], 'components': parameters['singularvalues'],
                        'seed': parameters['seed'], 'partitions': plan['partitions']})
        if name == 'graph.nweight':
            nweight.append({'path': plan['input'], 'output': output, 'pairs': parameters['edges'],
                            'degree': parameters['degree'], 'topk': parameters['max_out_edges'],
                            'seed': parameters['seed'], 'partitions': plan['partitions']})
    for name, data in [('plans', plans), ('svd', svd), ('nweight', nweight)]:
        (destination / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')
    target = campaign['experiment']['target']
    container = target['container']
    remote = '/tmp/hibench-' + destination.name
    subprocess.run(['docker', 'exec', container, 'mkdir', '-p', remote], check=True)
    scripts = ['verify_candidate_data.py', 'verify_svd_controller.py', 'verify_nweight_controller.py']
    for name in scripts:
        subprocess.run(['docker', 'cp', str(root / 'tests/integration' / name), container + ':' + remote + '/' + name], check=True)
    for name in ('plans', 'svd', 'nweight'):
        subprocess.run(['docker', 'cp', str(destination / (name + '.json')), container + ':' + remote + '/' + name + '.json'], check=True)
    base = ['docker', 'exec', '-e', 'HADOOP_CONF_DIR=' + target['hadoop_home'] + '/etc/hadoop',
            '-e', 'PYSPARK_PYTHON=python3', container, target['spark_home'] + '/bin/spark-submit',
            '--master', 'yarn', '--deploy-mode', 'client', '--num-executors', '2',
            '--executor-cores', '2', '--executor-memory', '2g', '--driver-memory', '2g',
            '--conf', 'spark.sql.shuffle.partitions=4', '--conf', 'spark.dynamicAllocation.enabled=false',
            '--jars', target['workspace'] + '/sparkbench/assembly/target/sparkbench-assembly-2.0.0-dist.jar']
    cases = [('actual', scripts[0], 'plans', [remote + '/actual-results.json']),
             ('svd', scripts[1], 'svd', []),
             ('nweight', scripts[2], 'nweight', [target['workspace'] + '/sparkbench/graph/src/main/resources/nweight-user-features'])]
    results = []
    for name, script, plan, extras in cases:
        print('Preset reference: ' + name, flush=True)
        logfile = destination / (name + '-verification.log')
        with logfile.open('w', encoding='utf-8') as stream:
            code = subprocess.run(base + [remote + '/' + script, remote + '/' + plan + '.json'] + extras,
                                  stdout=stream, stderr=subprocess.STDOUT).returncode
        if name == 'actual':
            subprocess.run(['docker', 'cp', container + ':' + remote + '/actual-results.json',
                            str(destination / 'actual-results.json')], check=True)
        markers = [line for line in logfile.read_text().splitlines() if line.startswith('HIBENCH_')]
        if name == 'actual':
            checks = json.loads((destination / 'actual-results.json').read_text())
            passed = len(checks) == len(plans) and all(
                c['status'] == 'passed' or (c['workload'] == 'graph.nweight' and c['status'] == 'dedicated_verifier_required')
                for c in checks)
        else:
            passed = bool(markers)
        results.append({'case': name, 'returncode': code, 'checks_passed': code == 0 and passed, 'markers': markers})
        (destination / 'verification-receipt.json').write_text(json.dumps(results, indent=2) + '\n')
        print(name + ' returncode=' + str(code), flush=True)


if __name__ == '__main__':
    verify(sys.argv[1])
