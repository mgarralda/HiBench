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

"""Collect event-log evidence for a completed intermediate campaign (Docker)."""
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import sys
from tests.tools.measurement import summarize_events


def collect(destination):
    destination = Path(destination)
    campaign = json.loads((destination / 'campaign.json').read_text())
    preset_campaign = campaign['profile'].startswith('default-preset-')
    rows = []
    for run in campaign['runs']:
        phase = None
        phases = []
        for event in run.get('events', []):
            if event['kind'] == 'phase_started':
                phase = {**event, 'applications': [], 'metrics': None}
                phases.append(phase)
            elif phase is not None and event['kind'] == 'job_reference':
                phase['remote_directory'] = str(Path(event['reference']['metadata']['pidfile']).parent).replace('\\', '/')
                for app in event['reference']['metadata'].get('application_ids', []):
                    if app not in phase['applications']:
                        phase['applications'].append(app)
            elif phase is not None and event['kind'] == 'result':
                phase['metrics'] = event['metrics']
            elif phase is not None and event['kind'] == 'phase_finished':
                phase['finished_at'] = event['at']
        run_number = 0
        for phase in phases:
            if phase['phase'] == 'run':
                run_number += 1
            for app in phase['applications']:
                target = run['spec']['target']
                uri = run['spec']['spark_conf']['spark.eventLog.dir'].rstrip('/') + '/' + app
                raw = subprocess.run(['docker', 'exec', target['container'],
                                      target['hadoop_home'] + '/bin/hdfs', 'dfs', '-cat', uri],
                                     capture_output=True, check=True).stdout
                parsed = summarize_events(json.loads(line) for line in raw.splitlines() if line.strip())
                row = {'workload': run['workload'], 'run_id': run['run_id'],
                       'phase': phase['phase'], 'index': phase['index'],
                       'execution_number': run_number if phase['phase'] == 'run' else None,
                       'measured': phase['phase'] == 'run' and (preset_campaign or run_number > 1),
                       'application_id': app, 'event_log_uri': uri,
                       'event_log_sha256': hashlib.sha256(raw).hexdigest(), **parsed}
                resources = run['spec']['resources']
                expected = {'spark.executor.instances': str(resources['executor_instances']),
                            'spark.executor.cores': str(resources['executor_cores']),
                            'spark.executor.memory': resources['executor_memory'],
                            'spark.driver.memory': resources['driver_memory'],
                            'spark.sql.shuffle.partitions': str(resources['shuffle_partitions']),
                            'spark.master': target['master'],
                            'spark.submit.deployMode': target['deploy_mode']}
                expected.update({k: str(v).lower() if isinstance(v, bool) else str(v)
                                 for k, v in run['spec']['spark_conf'].items()
                                 if k in ('spark.sql.adaptive.enabled', 'spark.dynamicAllocation.enabled',
                                          'spark.speculation')})
                row['property_mismatches'] = {k: {'requested': v, 'observed': parsed['effective_properties'].get(k)}
                                              for k, v in expected.items() if parsed['effective_properties'].get(k) != v}
                metrics = phase['metrics']
                if not metrics:
                    code = "import json,pathlib,sys; print(json.dumps([json.loads(p.read_text()) for p in pathlib.Path(sys.argv[1]).rglob('bench.log.execution.json')]))"
                    metadata = json.loads(subprocess.run(
                        ['docker', 'exec', target['container'], 'python3', '-c', code, phase['remote_directory']],
                        capture_output=True, check=True).stdout)
                    native = next((m for m in metadata if m.get('application_id') == app and 'finished_at' in m), None)
                    if native:
                        metrics = {'application_id': app,
                                   'duration_s': native['finished_at'] - native['started_at'],
                                   'started_at_ms': native['started_at'] * 1000}
                        row['submit_timing_source'] = 'execute_with_log metadata'
                if metrics and metrics['application_id'] == app:
                    row.setdefault('submit_timing_source', 'legacy shell report')
                    row['submit_duration_s'] = metrics['duration_s']
                    row['input_bytes_physical'] = metrics.get('input_bytes')
                    delta = (parsed['application_start_ms'] - metrics['started_at_ms']) / 1000 if parsed['application_start_ms'] is not None else None
                    row['submit_to_application_start_s'] = delta
                    row['clock_order_valid'] = delta is not None and delta >= 0
                row['phase_wall_duration_s'] = phase.get('finished_at', phase['at']) - phase['at']
                rows.append(row)
    summaries = []
    for run in campaign['runs']:
        selected = [r for r in rows if r['workload'] == run['workload'] and r['measured']]
        summary = {'workload': run['workload'], 'state': run['state'],
                   'measured_applications': len(selected),
                   'timing_scope': 'Single functional observation; no performance repetitions' if preset_campaign else 'Excluded initial execution followed by measured repetitions',
                   'complete': run['state'] == 'succeeded' and len(selected) == (run['spec']['repetitions'] if preset_campaign else run['spec']['repetitions'] - 1)
                   and all(r['complete'] and r.get('clock_order_valid') and not r['property_mismatches'] for r in selected)}
        for key in ('submit_duration_s', 'application_duration_s', 'job_interval_union_s'):
            values = [r[key] for r in selected if r.get(key) is not None]
            if values:
                summary[key] = {'values': values, 'median': statistics.median(values),
                                'mean': statistics.mean(values), 'min': min(values), 'max': max(values)}
        summaries.append(summary)
    result = {'profile': campaign['profile'], 'method': campaign['method'],
              'timing_definitions': {
                  'submit_duration_s': 'Existing shell measurement around spark-submit; includes startup and shutdown.',
                  'application_duration_s': 'Spark ApplicationStart to ApplicationEnd; includes data access, computation and shutdown.',
                  'job_interval_union_s': 'Union of JobStart/JobEnd envelopes, avoiding overlap; includes reads/writes and scheduling; not exclusive algorithm time.',
                  'executor_metrics': 'Sums over all completed task attempts, including failed/retried attempts; not wall-clock time.'},
              'applications': rows, 'workloads': summaries}
    (destination / 'measurements.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    collect(sys.argv[1])
