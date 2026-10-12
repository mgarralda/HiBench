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

import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock
from hibench import config, spark_configuration
from hibench.backend import properties
from hibench.catalog import repository_root


class SparkScalesTests(unittest.TestCase):
    def test_shared_configuration_uses_regular_imports(self):
        with patch('importlib.util.spec_from_file_location', side_effect=AssertionError('Dynamic loading is forbidden')):
            parsed = spark_configuration.parse_spark('spark.eventLog.enabled true\n[tiny]\nspark.executor.memory 1g')
            resolved, _ = spark_configuration.scale_values(parsed, 'tiny')
            self.assertEqual(resolved['spark.executor.memory'], '1g')
            self.assertEqual(config.default_experiment()['schema_version'], 1)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(repository_root() / 'conf', self.root / 'conf')
        self.spark = self.root / 'conf/spark.conf'
        text = self.spark.read_text(encoding='utf-8')
        body = spark_configuration.sections(text)['tiny'].replace('spark.executor.memory         1g', 'spark.executor.memory         2g').replace('spark.sql.shuffle.partitions  200', 'spark.sql.shuffle.partitions  17').replace('spark.sql.adaptive.enabled true', 'spark.sql.adaptive.enabled false')
        self.spark.write_text(spark_configuration.replace_sections(text, {'tiny': body}), encoding='utf-8')

    def test_scale_changes_spark_but_not_generator_settings(self):
        tiny = config.default_experiment(self.root / 'conf', scale='tiny')
        small = config.default_experiment(self.root / 'conf', scale='small')
        self.assertEqual(tiny['resources']['executor_memory'], '2g')
        self.assertEqual(tiny['resources']['shuffle_partitions'], 17)
        self.assertEqual(tiny['resources']['generation_shuffle_partitions'], small['resources']['generation_shuffle_partitions'])
        self.assertEqual(tiny['resources']['generation_partitions'], small['resources']['generation_partitions'])
        values, sources = spark_configuration.resolve({'spark.sql.adaptive.enabled': True}, self.spark, scale='tiny')
        self.assertIs(values['spark.sql.adaptive.enabled'], True)
        self.assertEqual(sources['spark.sql.adaptive.enabled'], 'experiment override')
        values, sources = spark_configuration.resolve({}, self.spark, scale='tiny')
        self.assertEqual(values['spark.sql.adaptive.enabled'], 'false')
        self.assertIn('scale:tiny', sources['spark.sql.adaptive.enabled'])

    def test_import_export_and_submission_use_selected_scale(self):
        original = config.default_experiment
        with patch('hibench.config.default_experiment', side_effect=lambda conf_dir=None, **kwargs: original(conf_dir or self.root / 'conf', **kwargs)), patch('hibench.spark_configuration.repository_root', return_value=self.root):
            spec = config.resolve({'scale': 'tiny', 'resources': {'executor_memory': '3g'}, 'spark_conf': {'spark.sql.adaptive.enabled': True}})
            self.assertEqual(spec['resources']['shuffle_partitions'], 17)
            self.assertEqual(config.resolve(config.overrides(spec)), spec)
            wire = properties(spec, spec['workloads'][0], 'test', '/data', '/report', '/output')
            self.assertIn('spark.executor.memory 3g\n', wire)
            self.assertIn('spark.sql.shuffle.partitions 17\n', wire)
            self.assertIn('hibench.generator.shuffle.partitions 200\n', wire)
            self.assertIn('spark.sql.adaptive.enabled true\n', wire)
            self.assertNotIn('__scale__.', wire)

    def test_invalid_scale_keys_are_rejected(self):
        for key in ('__scale__.invalid.spark.executor.memory', '__scale__.tiny.hibench.generator.partitions', '__scale__.tiny.spark.master'):
            with self.assertRaises(ValueError):
                spark_configuration.scale_values({key: '2'}, 'tiny')

    def test_shared_save_validates_every_scale(self):
        with patch('hibench.spark_configuration.repository_root', return_value=self.root), patch('hibench.store.has_active_runs', return_value=False):
            state = spark_configuration.shared()
            with self.assertRaises(ValueError):
                spark_configuration.save_shared(state['text'].replace('spark.executor.memory         1g', 'spark.executor.memory         0g'), state['sha256'])
            self.assertEqual(spark_configuration.shared()['sha256'], state['sha256'])

    def test_ui_previews_scale_without_changing_experiment(self):
        from streamlit.testing.v1 import AppTest
        original = config.default_experiment
        with patch('hibench.config.default_experiment', side_effect=lambda conf_dir=None, **kwargs: original(conf_dir or self.root / 'conf', **kwargs)), patch('hibench.spark_configuration.repository_root', return_value=self.root):
            draft = config.default_experiment()
            app = AppTest.from_string('from hibench.ui.spark_settings import render\nrender()')
            app.session_state['experiment'] = draft
            app.run()
            app.selectbox[0].select('tiny').run()
            self.assertFalse(app.exception)
            self.assertEqual(app.session_state.experiment['scale'], 'small')
            self.assertTrue(any('2g executor memory' in x.value for x in app.markdown))

    def test_installed_loader_preserves_explicit_overrides(self):
        folder = repository_root() / 'bin/functions'
        with patch.object(sys, 'path', [str(folder), *sys.path]), patch.dict(sys.modules, {'fcntl': Mock()}):
            spec = importlib.util.spec_from_file_location('scale_loader_test', folder / 'load_config.py')
            loader = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(loader)
            props = {'hibench.scale.profile': 'tiny', 'spark.executor.memory': '3g',
                     '__scale__.tiny.spark.executor.memory': '2g'}
            loader.HibenchConf.update(props)
            loader.HibenchConfRef['spark.executor.memory'] = '/override.conf'
            # Exercise the real load path; isolate platform probing/export from precedence.
            with patch.object(loader, 'parse_conf'), patch.object(loader, 'override_conf_from_environment'), patch.object(loader, 'override_conf_by_paching_conf'), patch.object(loader, 'waterfall_config'), patch.object(loader, 'generate_optional_value'), patch.object(loader, 'check_config'), patch.object(loader, 'export_config', return_value='test'), patch.dict(os.environ, {'HIBENCH_OVERRIDE_FILE': '/override.conf'}):
                loader.load_config('/conf', '/workload.conf', '/micro/sleep/spark')
                self.assertEqual(loader.HibenchConf['spark.executor.memory'], '3g')
                self.assertFalse(any(key.startswith('__scale__.') for key in loader.HibenchConf))

    def test_file_separation_and_preset_roundtrip(self):
        from hibench.services import experiments
        for line in self.spark.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith(('#', '[')):
                self.assertTrue(line.split()[0].startswith('spark.'))
        original = config.default_experiment
        with patch('hibench.config.default_experiment', side_effect=lambda conf_dir=None, **kwargs: original(conf_dir or self.root / 'conf', **kwargs)), patch('hibench.spark_configuration.repository_root', return_value=self.root):
            spec = config.resolve({'scale': 'tiny', 'resources': {'executor_memory': '3g'}, 'workloads': [{'id': 'micro.wordcount', 'parameters': {}}, {'id': 'micro.sort', 'parameters': {}}]})
            self.assertNotIn('default_parallelism', spec['resources'])
            with patch('hibench.services.experiments.duplicate', return_value=spec):
                self.assertEqual(config.resolve(experiments.preset('test')), spec)

    def test_pair_update_preserves_revision_guards(self):
        with patch('hibench.spark_configuration.repository_root', return_value=self.root), patch('hibench.store.has_active_runs', return_value=False):
            state = spark_configuration.shared()
            changed = state['benchmark_text'].replace('hibench.generator.partitions       14', 'hibench.generator.partitions       15')
            with self.assertRaises(ValueError):
                spark_configuration.save_shared(state['text'], state['sha256'], changed, '0'*64)
            self.assertEqual(spark_configuration.shared()['benchmark_sha256'], state['benchmark_sha256'])
            spark_configuration.save_shared(state['text'], state['sha256'], changed, state['benchmark_sha256'])
            self.assertIn('hibench.generator.partitions       15', spark_configuration.shared()['benchmark_text'])
            with self.assertRaises(ValueError):
                current = spark_configuration.shared()
                spark_configuration.save_shared(current['text'] + '\nhibench.spark.home /bad\n', current['sha256'])

    def test_sections_reject_bad_keys_and_ui_encoding(self):
        for text in ('[wrong]\nspark.task.cpus 1', '[small]\nhibench.spark.executor.memory 2g', '[small]\nspark.task.cpus 1\nspark.task.cpus 2'):
            with self.assertRaises(ValueError):
                spark_configuration.parse_spark(text)
        for path in (repository_root() / 'hibench/ui').glob('*.py'):
            source = path.read_text(encoding='utf-8')
            self.assertFalse(any(marker in source for marker in ('Ã', 'Â', 'â')), str(path))

    def test_navigation_has_separate_configuration_pages(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(repository_root() / 'hibench/web.py'))
        app.session_state['ui_page'] = 'HiBenchConfiguration'
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual(app.title[0].value, 'HiBench configuration')
        self.assertEqual(len(app.text_area), 1)
        next(b for b in app.button if b.key == 'nav-SparkConfiguration').click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.title[0].value, 'Spark configuration')
        self.assertEqual(len(app.text_area), 2)

    def test_selected_scale_edit_preserves_other_sizes_and_extends_properties(self):
        original = self.spark.read_text(encoding='utf-8')
        blocks = spark_configuration.sections(original)
        changed = blocks['tiny'].replace('spark.executor.memory         2g', 'spark.executor.memory         4g') + '\nspark.sql.adaptive.coalescePartitions.enabled false\n'
        updated = spark_configuration.replace_sections(original, {'tiny': changed})
        after = spark_configuration.sections(updated)
        for scale in ('small', 'large', 'huge', 'gigantic', 'bigdata'):
            self.assertEqual(after[scale].strip(), blocks[scale].strip())
        with patch('hibench.spark_configuration.repository_root', return_value=self.root), patch('hibench.store.has_active_runs', return_value=False):
            state = spark_configuration.shared()
            spark_configuration.save_shared(updated, state['sha256'])
            tiny = spark_configuration.configuration(self.spark, 'tiny')[0]
            small = spark_configuration.configuration(self.spark, 'small')[0]
            self.assertEqual(tiny['spark.executor.memory'], '4g')
            self.assertEqual(tiny['spark.sql.adaptive.coalescePartitions.enabled'], 'false')
            self.assertEqual(small['spark.executor.memory'], '1g')
            self.assertNotIn('spark.sql.adaptive.coalescePartitions.enabled', small)
