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

import tempfile
import unittest
from pathlib import Path
from hibench.parameters.spark import resolve
from hibench import config
from hibench.adapters.runtime import properties
from hibench.catalog import repository_root
from hibench.services.experiments import snapshot


class SparkConfigurationTest(unittest.TestCase):
    def test_execution_group_survives_api_and_preset_and_submission(self):
        from hibench.api.models import Experiment
        spec = config.default_experiment(selected_workloads=['micro.sleep'])
        spec['execution_group'] = 'paper-baseline'
        spec = config.validate(spec)
        self.assertEqual(Experiment(**spec).execution_group, 'paper-baseline')
        self.assertEqual(config.resolve(config.overrides(spec)), spec)
        item = spec['workloads'][0]
        values = dict(line.split(' ', 1) for line in properties(spec, item, 'run-uuid', '/data', '/report', '/out').splitlines())
        self.assertEqual(values['spark.app.benchmark.group.id'], 'paper-baseline')
        self.assertEqual(values['spark.app.benchmark.workload'], 'micro.sleep')
        self.assertEqual(values['spark.app.benchmark.data.size'], spec['scale'])
        self.assertEqual(values['spark.yarn.tags'], 'hibench-run-uuid')
        spec['execution_group'] = ''
        self.assertIn('spark.app.benchmark.group.id run-uuid\n', properties(spec, item, 'run-uuid', '/data', '/report', '/out'))

    def test_launcher_metadata_cannot_be_overridden(self):
        for key in ('spark.app.benchmark.group.id', 'spark.app.benchmark.workload', 'spark.app.benchmark.data.size'):
            spec = config.default_experiment(selected_workloads=['micro.sleep'])
            spec['spark_conf'][key] = 'wrong'
            with self.assertRaises(ValueError):
                config.validate(spec)
        spec = config.default_experiment(selected_workloads=['micro.sleep'])
        spec['execution_group'] = 'bad\nproperty'
        with self.assertRaises(ValueError):
            config.validate(spec)
        text = (repository_root() / 'conf/spark.conf').read_text(encoding='utf-8')
        self.assertNotIn('spark.app.benchmark.', text)
        self.assertIn('spark.eventLog.enabled', text)
        self.assertIn('spark.eventLog.dir', text)

    def test_initial_workloads_use_the_shared_list(self):
        names = [line.strip() for line in (repository_root() / 'conf/benchmarks.lst').read_text().splitlines()
                 if line.strip() and not line.strip().startswith('#')]
        self.assertEqual([w['id'] for w in config.default_experiment()['workloads']], names)
    def test_exported_overrides_reload_without_repository_recipe(self):
        spec = config.default_experiment()
        spec['spark_conf']['spark.sql.adaptive.enabled'] = False
        spec['resources']['executor_cores'] += 1
        exported = config.overrides(spec)
        self.assertNotIn('target', exported)
        self.assertNotIn('scale', exported)
        self.assertEqual(exported['resources'], {'executor_cores': spec['resources']['executor_cores']})
        self.assertEqual(config.resolve(exported), spec)

    def test_base_changes_drive_initial_target_resources_and_scale(self):
        import shutil
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for path in (repository_root() / 'conf').glob('*.conf'):
                shutil.copyfile(path, root / path.name)
            (root / 'benchmarks.lst').write_text('micro.wordcount\nmicro.sort\n')
            with (root / 'hibench.conf').open('a') as stream:
                stream.write('\nhibench.scale.profile tiny\nhibench.generator.partitions 8\n')
            path = root / 'spark.conf'
            path.write_text(path.read_text(encoding='utf-8').replace('spark.executor.memory         1g', 'spark.executor.memory         3g'), encoding='utf-8')
            spec = config.default_experiment(root)
            self.assertEqual(spec['scale'], 'tiny')
            self.assertEqual(spec['resources']['generation_partitions'], 8)
            self.assertEqual(spec['resources']['executor_memory'], '3g')
    def test_base_overrides_and_sources(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'spark.conf'
            path.write_text('# comment\nspark.sql.adaptive.enabled true\nspark.executor.memory 99g\nspark.task.cpus 1\n')
            (path.parent / 'hibench.conf').write_text('', encoding='utf-8')
            values, sources = resolve({'spark.sql.adaptive.enabled': False}, path)
            self.assertEqual(values, {'spark.sql.adaptive.enabled': False, 'spark.task.cpus': '1'})
            self.assertEqual(sources['spark.task.cpus'], 'conf/spark.conf (common)')
            self.assertEqual(sources['spark.sql.adaptive.enabled'], 'experiment override')

    def test_default_experiment_inherits_and_freezes(self):
        spec = config.default_experiment()
        self.assertEqual(spec['spark_conf'], {})
        frozen = snapshot(spec)
        values = dict(line.split(' ', 1) for line in properties(spec, spec['workloads'][0], 'test', '/data', '/report', '/output').splitlines())
        for key, value in frozen['resolved_spark_properties'].items():
            self.assertEqual(values[key], config.scalar(value))
        spec['spark_conf']['spark.sql.adaptive.enabled'] = False
        self.assertIn('spark.sql.adaptive.enabled false\n', properties(spec, spec['workloads'][0], 'test', '/data', '/report', '/output'))
