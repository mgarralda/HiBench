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

import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench import spark_configuration, config
from hibench.catalog import repository_root


class SparkBaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'conf').mkdir()
        for file in (repository_root() / 'conf').glob('*'):
            if file.is_file() and (file.suffix == '.conf' or file.name == 'benchmarks.lst'):
                shutil.copyfile(file, self.root / 'conf' / file.name)
        mock = patch('hibench.spark_configuration.repository_root', return_value=self.root)
        mock.start()
        self.addCleanup(mock.stop)
        idle = patch('hibench.store.has_active_runs', return_value=False)
        idle.start()
        self.addCleanup(idle.stop)
        self.state = spark_configuration.shared()

    def test_resources_and_event_logs_change_the_shared_base(self):
        text = self.state['text'].replace('spark.executor.memory         1g', 'spark.executor.memory         3g')
        text = text.replace('hdfs://spark-cluster-master:9000/shared/spark-logs', 'wasbs://hdp/events')
        result = spark_configuration.save_shared(text, self.state['sha256'])
        self.assertEqual(result['defaults']['resources']['executor_memory'], '3g')
        self.assertIn('wasbs://hdp/events', result['text'])
        self.assertEqual((self.root / 'conf/spark.conf').read_bytes().decode('utf-8'), text)

    def test_invalid_duplicate_or_stale_edits_leave_base_unchanged(self):
        for text, digest in ((self.state['text']+'\nspark.task.cpus 2', self.state['sha256']),
                             (self.state['text'].replace('spark.driver.memory          1g', 'spark.driver.memory 0g'), self.state['sha256']),
                             ('spark.sql.adaptive.enabled: true', self.state['sha256']),
                             (self.state['text'], '0'*64)):
            with self.assertRaises(ValueError):
                spark_configuration.save_shared(text, digest)
            self.assertEqual(spark_configuration.shared()['sha256'], self.state['sha256'])

    def test_ui_preserves_explicit_overrides_when_base_changes(self):
        from streamlit.testing.v1 import AppTest
        draft = config.default_experiment()
        draft['spark_conf']['spark.sql.adaptive.enabled'] = False
        app = AppTest.from_string('from hibench.ui.spark_settings import render\nrender()')
        app.session_state['experiment'] = draft
        app.run()
        text = self.state['text'].replace('spark.executor.memory         1g', 'spark.executor.memory         3g')
        app.text_area[0].set_value(spark_configuration.sections(text)['small'])
        # Resolve against the test conf after writing, while catalog validation stays real.
        original_default = config.default_experiment
        with patch('hibench.services.runtime.create_adapter') as adapter, patch('hibench.config.default_experiment', side_effect=lambda conf_dir=None, **kwargs: original_default(conf_dir or self.root / 'conf', **kwargs)):
            next(b for b in app.button if b.label == 'Save and update target').click().run()
        adapter.return_value.install.assert_called_once()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment['spark_conf']['spark.sql.adaptive.enabled'], False)
        self.assertEqual(app.session_state.experiment['resources']['executor_memory'], '3g')

    def test_failed_update_keeps_local_save_and_retry_installs_saved_config(self):
        from streamlit.testing.v1 import AppTest
        original_default = config.default_experiment
        with patch('hibench.services.runtime.create_adapter') as adapter, patch('hibench.config.default_experiment', side_effect=lambda conf_dir=None, **kwargs: original_default(conf_dir or self.root / 'conf', **kwargs)):
            app = AppTest.from_string('from hibench.ui.spark_settings import render\nrender()')
            app.session_state['experiment'] = config.default_experiment()
            app.run()
            app.text_area[0].set_value(app.text_area[0].value.replace('1g', '3g'))
            adapter.return_value.install.side_effect = RuntimeError('Target offline')
            next(b for b in app.button if b.label == 'Save and update target').click().run()
            self.assertFalse(app.exception)
            self.assertIn('3g', (self.root / 'conf/spark.conf').read_text(encoding='utf-8'))
            self.assertTrue(any('saved locally' in w.value for w in app.warning))
            saved = (self.root / 'conf/spark.conf').read_bytes()
            adapter.return_value.install.side_effect = None
            next(b for b in app.button if b.label == 'Retry target update').click().run()
            self.assertFalse(app.exception)
            self.assertEqual((self.root / 'conf/spark.conf').read_bytes(), saved)
            self.assertTrue(any('updated on target' in w.value for w in app.success))
            self.assertEqual(adapter.return_value.install.call_count, 2)

    def test_active_run_blocks_save_and_remote_install(self):
        from streamlit.testing.v1 import AppTest
        with patch('hibench.services.runtime.create_adapter') as adapter, patch('hibench.store.has_active_runs', return_value=True):
            app = AppTest.from_string('from hibench.ui.spark_settings import render\nrender()')
            app.session_state['experiment'] = config.default_experiment()
            app.run()
            original = (self.root / 'conf/spark.conf').read_bytes()
            next(b for b in app.button if b.label == 'Save and update target').click().run()
            self.assertFalse(app.exception)
            self.assertEqual((self.root / 'conf/spark.conf').read_bytes(), original)
            adapter.assert_not_called()
            self.assertTrue(any('active runs' in e.value for e in app.error))
