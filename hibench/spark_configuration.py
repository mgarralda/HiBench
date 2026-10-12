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

"""Shared Spark defaults, explicit experiment overrides and auditable resolution."""
from .catalog import repository_root
import hashlib
import os
import re
import shutil
import tempfile
from pathlib import Path
from .spark_scale import parse_spark, resolve_scale

# These values belong to dedicated target/resources fields or launcher internals.
MANAGED = {'spark.extraListeners', 'spark.master', 'spark.app.name', 'spark.submit.deployMode',
           'spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory',
           'spark.driver.memory', 'spark.sql.shuffle.partitions',
           'spark.yarn.tags', 'spark.app.benchmark.group.id', 'spark.yarn.submit.waitAppCompletion',
           'spark.app.benchmark.workload', 'spark.app.benchmark.data.size'}


def scale_values(properties, scale):
    return resolve_scale(properties, scale)


def configuration(path=None, scale='small'):
    from .catalog import read_properties
    path = Path(path or repository_root() / 'conf/spark.conf')
    return scale_values({**parse_spark(path.read_text(encoding='utf-8')), **read_properties(path.parent / 'hibench.conf')}, scale)


def defaults(path=None, scale='small'):
    path = path or repository_root() / 'conf/spark.conf'
    values, _ = configuration(path, scale)
    return {k: v for k, v in values.items() if k.startswith('spark.') and k not in MANAGED and v != ''}


def resolve(overrides, path=None, scale='small'):
    values = defaults(path, scale)
    _, origins = configuration(path, scale)
    sources = {key: 'conf/spark.conf' + ' (' + origins[key] + ')' for key in values}
    values.update(overrides)
    sources.update({key: 'experiment override' for key in overrides})
    return values, sources


def shared(scale=None):
    from . import config
    path = repository_root() / 'conf/spark.conf'
    raw = path.read_bytes()
    return {'configuration_file': str(path), 'text': raw.decode('utf-8'),
            'sha256': hashlib.sha256(raw).hexdigest(),
            'benchmark_text': (path.parent / 'hibench.conf').read_text(encoding='utf-8'),
            'benchmark_sha256': hashlib.sha256((path.parent / 'hibench.conf').read_bytes()).hexdigest(), 'defaults': config.default_experiment(path.parent, scale=scale)}


def save_shared(text, expected_sha256, benchmark_text=None, benchmark_sha256=None):
    """Validate a candidate base before atomically replacing the single Spark file."""
    from . import config, store
    state = shared()
    path = repository_root() / 'conf/spark.conf'
    if state['sha256'] != expected_sha256:
        raise ValueError('Spark configuration changed. Reload before saving.')
    if store.has_active_runs():
        raise ValueError('Wait for queued or active runs to finish before changing the shared base.')
    benchmark_path = path.parent / 'hibench.conf'
    if benchmark_text is not None and state['benchmark_sha256'] != benchmark_sha256:
        raise ValueError('Benchmark configuration changed. Reload before saving.')
    if '\x00' in text:
        raise ValueError('Spark configuration contains an invalid null character.')
    parse_spark(text)
    if benchmark_text is not None:
        seen = set()
        for line in benchmark_text.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key = line.split(None, 1)[0]
            if not re.fullmatch(r'(hibench|sparkbench)\.[\w.]+', key) or key in seen or '\x00' in line:
                raise ValueError('Invalid or duplicate benchmark property: ' + key)
            seen.add(key)
    from .catalog import SCALES
    with tempfile.TemporaryDirectory() as folder:
        candidate = Path(folder)
        for source in path.parent.glob('*.conf'):
            shutil.copyfile(source, candidate / source.name)
        shutil.copyfile(path.parent / 'benchmarks.lst', candidate / 'benchmarks.lst')
        (candidate / 'spark.conf').write_text(text, encoding='utf-8')
        if benchmark_text is not None:
            (candidate / 'hibench.conf').write_text(benchmark_text, encoding='utf-8')
        for scale in SCALES:
            # Validate Spark resources independently of workload solver limits at huge scales.
            config.default_experiment(candidate, scale=scale, selected_workloads=['micro.sleep'])
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, suffix='.tmp', delete=False) as stream:
            temporary = stream.name
            stream.write(text.encode('utf-8'))
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_sha256:
            raise ValueError('Spark configuration changed while saving. Reload before saving.')
        if benchmark_text is not None:
            if hashlib.sha256(benchmark_path.read_bytes()).hexdigest() != benchmark_sha256:
                raise ValueError('Benchmark configuration changed while saving.')
            original = benchmark_path.read_bytes()
            with tempfile.NamedTemporaryFile(dir=path.parent, suffix='.tmp', delete=False) as other:
                other.write(benchmark_text.encode('utf-8'))
                other_name = other.name
            try:
                os.replace(other_name, benchmark_path)
                os.replace(temporary, path)
            except Exception:
                benchmark_path.write_bytes(original)
                raise
            finally:
                if os.path.exists(other_name):
                    os.unlink(other_name)
        else:
            os.replace(temporary, path)
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)
    return shared()


def sections(text):
    """Editable blocks in the authoritative Spark file, without section headers."""
    result = {'common': ''}
    current = 'common'
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith('[') and stripped.endswith(']'):
            current = stripped[1:-1]
            result.setdefault(current, '')
        else:
            result[current] += line
    return result


def replace_sections(text, changes):
    blocks = sections(text)
    blocks.update(changes)
    combined = blocks.pop('common', '').rstrip() + '\n'
    for name, body in blocks.items():
        combined += '\n[' + name + ']\n' + body.rstrip() + '\n'
    parse_spark(combined)
    return combined
