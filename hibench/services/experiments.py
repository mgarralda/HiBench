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

"""Submission, provenance, comparison and exports independent of UI/HTTP."""
import hashlib
import copy
import io
import json
import platform
import zipfile
from importlib.metadata import version, PackageNotFoundError
from hibench import config, store, worker
from hibench.catalog import repository_root, workloads
from hibench.workload_parameters import typed_value


def snapshot(spec):
    from hibench.spark_configuration import resolve
    spark_properties, spark_sources = resolve(spec['spark_conf'], scale=spec['scale'])
    catalog = {w['id']: w for w in workloads()}
    effective = {}
    for w in spec['workloads']:
        effective[w['id']] = {key: w['parameters'].get(key, typed_value(schema, schema['presets'][spec['scale']]))
                             for key, schema in catalog[w['id']]['parameters'].items()
                             if '${' not in str(schema['presets'][spec['scale']])}
    root = repository_root()
    files = {}
    for path in sorted((root / 'conf').rglob('*')):
        if path.is_file():
            files[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    if spec.get('monitoring', {}).get('enabled'):
        for relative in ('bin/functions/monitor.py', 'bin/functions/chart-template.html',
                         'bin/functions/workload_functions.sh', 'hibench/services/monitoring.py'):
            files[relative] = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    for relative in ('sparkbench/assembly/target/sparkbench-assembly-2.0.0-dist.jar',):
        path = root / relative
        if path.exists():
            digest = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            files[relative] = digest.hexdigest()
    packages = {}
    for package in ('fastapi', 'pydantic', 'streamlit'):
        try:
            packages[package] = version(package)
        except PackageNotFoundError:
            pass
    return {'schema_version': 1, 'configuration': spec, 'effective_workload_parameters': effective,
            'resolved_spark_properties': spark_properties, 'spark_property_sources': spark_sources,
            'file_sha256': files, 'controller_python': platform.python_version(), 'controller_packages': packages,
            'runtime_scope': 'runtime versions and actual Spark properties are reported by doctor/events; controller versions are not cluster versions'}


def submit(spec, request_key=None):
    run_id, created = store.create(config.validate(spec), request_key)
    if created:
        try:
            worker.launch(run_id)
        except Exception as exc:
            store.update(run_id, 'failed', 'Worker could not start: ' + str(exc))
            worker.append(run_id, {'kind': 'error', 'message': 'Worker could not start: ' + str(exc)})
            raise
    return {'run_id': run_id, 'created': created}


def results(run_id):
    run = store.get(run_id)
    directory = store.directory(run_id)
    path = directory / 'provenance.json'
    provenance = json.loads(path.read_text()) if path.exists() else {'scope': 'legacy run; submission provenance unavailable'}
    events = []
    for line in (directory / 'events.jsonl').read_text(encoding='utf-8').splitlines() if (directory / 'events.jsonl').exists() else []:
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue  # A concurrently appended incomplete line is not a complete event.
    return {'run': run, 'provenance': provenance, 'events': events,
            'metrics': [e['metrics'] for e in events if e.get('kind') == 'result']}


def logs(run_id, offset=0, limit=40000):
    store.get(run_id)
    path = store.directory(run_id) / 'output.log'
    with path.open('rb') if path.exists() else io.BytesIO() as stream:
        stream.seek(offset)
        data = stream.read(limit)
    return {'offset': offset, 'next_offset': offset + len(data), 'text': data.decode('utf-8', errors='replace')}


def export(run_id):
    result = results(run_id)
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr('results.json', json.dumps(result, indent=2))
        bundle.writestr('resolved-experiment.json', json.dumps(duplicate(run_id), indent=2))
        for name in ('experiment.json', 'provenance.json', 'output.log', 'events.jsonl', 'worker.log'):
            path = store.directory(run_id) / name
            if path.exists():
                bundle.write(path, name)
        monitoring = store.directory(run_id) / 'monitoring'
        if monitoring.exists():
            for path in sorted(monitoring.rglob('*')):
                if path.is_file() and not path.is_symlink():
                    bundle.write(path, path.relative_to(store.directory(run_id)).as_posix())
    return output.getvalue()


def duplicate(run_id):
    result = results(run_id)
    spec = copy.deepcopy(result['run']['spec'])
    if 'resolved_spark_properties' in result['provenance']:
        spec['spark_conf'] = copy.deepcopy(result['provenance']['resolved_spark_properties'])
    frozen = result['provenance'].get('effective_workload_parameters', {})
    for workload in spec['workloads']:
        workload['parameters'] = {**frozen.get(workload['id'], {}), **workload['parameters']}
    return config.validate(spec)


def compare(left_id, right_id):
    left, right = results(left_id), results(right_id)
    differences = []

    def walk(a, b, path):
        if isinstance(a, dict) and isinstance(b, dict):
            for key in sorted(set(a) | set(b)):
                walk(a.get(key), b.get(key), path + '.' + key)
        elif a != b:
            differences.append({'field': path, 'left': a, 'right': b})

    for field in ('target', 'resources', 'scale', 'mode', 'monitoring'):
        walk(left['run']['spec'].get(field), right['run']['spec'].get(field), field)
    walk(left['provenance'].get('resolved_spark_properties', left['run']['spec'].get('spark_conf')),
         right['provenance'].get('resolved_spark_properties', right['run']['spec'].get('spark_conf')), 'spark_conf')
    walk([w['id'] for w in left['run']['spec']['workloads']], [w['id'] for w in right['run']['spec']['workloads']], 'workload_selection')
    for field in ('effective_workload_parameters', 'file_sha256'):
        walk(left['provenance'].get(field), right['provenance'].get(field), 'provenance.' + field)
    complete = all('effective_workload_parameters' in r['provenance'] for r in (left, right))
    return {'left': left_id, 'right': right_id, 'configuration_equivalent': complete and not differences,
            'differences': differences, 'left_metrics': left['metrics'], 'right_metrics': right['metrics'],
            'scope': 'configuration equivalence only; check runtime, input profiles, retries and repetitions before performance conclusions'}


def preset(run_id):
    """Export editable conditions from a run; includes all selected workloads."""
    return config.overrides(duplicate(run_id))
