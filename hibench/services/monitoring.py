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

"""Durable, per-phase resource observations shared by worker, UI and API."""
import importlib.util
import json
import os
from pathlib import Path
import re
import time
from hibench import store
from hibench.catalog import repository_root

SCOPE = ('Linux /proc system counters; CPU, memory and disk may describe the shared Docker host. '
         'These are not container limits or Spark-only metrics; summed host counters may overlap.')
UNITS = {'cpu': 'percent', 'memory': 'KiB', 'network': 'bytes/packets delta per probe interval',
         'disk': 'operations/bytes/seconds delta per probe interval', 'proc': 'load averages/counts'}


def legacy_reader(interval=5):
    spec = importlib.util.spec_from_file_location('hibench_legacy_monitor', repository_root() / 'bin/functions/monitor.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.PROBE_INTERVAL = float(interval)
    return module


def identifier(workload, phase, index):
    return f'{workload}-{phase}-{index}'


def phase_directory(run_id, report_id):
    store.get(run_id)
    if not re.fullmatch(r'[a-z]+\.[a-z]+-(prepare|run)-\d+', report_id):
        raise ValueError('Invalid monitoring report identifier')
    return store.directory(run_id) / 'monitoring' / report_id


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2), encoding='utf-8')
    os.replace(temporary, path)


def collect(backend, reference):
    """Copy raw evidence and render locally; collection errors never fail a workload."""
    settings = reference.metadata.get('monitoring', {})
    if not settings.get('enabled'):
        return None
    report_id = identifier(reference.metadata['workload'], reference.phase, reference.metadata['index'])
    directory = phase_directory(reference.run_id, report_id)
    directory.mkdir(parents=True, exist_ok=True)
    expected = reference.metadata['expected_nodes']
    metadata = dict(id=report_id, workload=reference.metadata['workload'], phase=reference.phase,
                    index=reference.metadata['index'], status='unavailable', interval_s=settings.get('interval_s', 5),
                    schema_version=1, collector_version='legacy-proc-v2', scope=SCOPE,
                    expected_nodes=expected, observed_nodes=[], sample_count=0, errors=[],
                    collected_at=time.time(), report_available=False)
    try:
        # Report subfolders differ between prepare and run. Only known basenames are copied.
        probe = '''import json,sys
from pathlib import Path
root=Path(sys.argv[1]); files={}
for name in ('monitor.log','bench.log','monitor.html.errors.log'):
 paths=list(root.rglob(name))
 if len(paths)>1: raise ValueError('Ambiguous monitor evidence: '+name)
 if paths:
  if paths[0].stat().st_size>32*1024*1024: raise ValueError('Monitor evidence exceeds 32 MiB: '+name)
  files[name]=paths[0].read_text(encoding='utf-8',errors='replace')
print(json.dumps(files))'''
        files = json.loads(backend.command(['python3', '-c', probe, reference.metadata['remote_dir']], timeout=30))
        for name, text in files.items():
            (directory / name).write_text(text, encoding='utf-8')
        reader = legacy_reader(metadata['interval_s'])
        samples = []
        for line in files.get('monitor.log', '').splitlines():
            try:
                record = reader.parse_monitor_sample(line)
                metrics = {key: value._asdict() for key, value in record.items() if hasattr(value, '_asdict')}
                samples.append(dict(schema_version=1, timestamp=record['timestamp'], node=record['hostname'],
                                    scope='system', metrics=metrics))
            except (ValueError, SyntaxError, TypeError, KeyError) as exc:
                metadata['errors'].append('Invalid sample: ' + str(exc))
        (directory / 'samples.jsonl').write_text(''.join(json.dumps(row) + '\n' for row in samples), encoding='utf-8')
        metadata.update(sample_count=len(samples), observed_nodes=sorted({row['node'] for row in samples}), units=UNITS)
        diagnostics = files.get('monitor.html.errors.log', '').strip()
        if diagnostics:
            metadata['errors'].append(diagnostics[-4000:])
        if samples:
            if 'bench.log' not in files:
                (directory / 'bench.log').write_text('', encoding='utf-8')
            try:
                reader.generate_report(report_id, str(directory / 'monitor.log'), str(directory / 'bench.log'),
                                       str(directory / 'report.html'))
                metadata['report_available'] = True
            except Exception as exc:
                metadata['errors'].append('Report rendering: ' + str(exc))
            # load_config canonicalizes hostnames; accept the explicit host or its qualified form.
            covered = all(any(node == host or node.startswith(host + '.') for node in metadata['observed_nodes'])
                          for host in expected)
            metadata['status'] = 'available' if covered and not metadata['errors'] and metadata['report_available'] else 'partial'
        else:
            metadata['errors'].append('No samples collected; check SSH, Python and probe connectivity on workers.')
    except Exception as exc:
        metadata['errors'].append(str(exc))
    atomic_json(directory / 'metadata.json', metadata)
    return metadata


def reports(run_id):
    run = store.get(run_id)
    directory = store.directory(run_id) / 'monitoring'
    items = [json.loads(path.read_text(encoding='utf-8')) for path in sorted(directory.glob('*/metadata.json'))]
    enabled = run['spec'].get('monitoring', {}).get('enabled', False)
    status = ('partial' if any(item['status'] != 'available' for item in items) else 'available') if items else (
        'pending' if enabled and run['state'] not in ('succeeded', 'failed', 'cancelled') else 'unavailable' if enabled else 'disabled')
    return dict(status=status, scope=SCOPE, reports=items)


def artifact(run_id, report_id, filename):
    if filename not in ('report.html', 'samples.jsonl', 'metadata.json'):
        raise ValueError('Invalid monitoring artifact')
    path = phase_directory(run_id, report_id) / filename
    if not path.is_file():
        raise KeyError('Monitoring artifact unavailable')
    return path


def samples(run_id, report_id, offset=0, limit=1000):
    path = artifact(run_id, report_id, 'samples.jsonl')
    import itertools
    with path.open(encoding='utf-8') as stream:
        rows = [json.loads(line) for line in itertools.islice(stream, offset, offset + limit)]
    return dict(offset=offset, next_offset=offset + len(rows), samples=rows)
