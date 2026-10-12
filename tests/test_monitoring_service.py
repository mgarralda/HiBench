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

import copy
import io
import json
import os
import tempfile
import unittest
import zipfile
from unittest.mock import Mock, patch
from fastapi.testclient import TestClient
from hibench import config, store
from hibench.adapters import JobReference
from hibench.api.app import create_app
from hibench.services import experiments, monitoring


class MonitoringServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'HIBENCH_STATE_DIR': self.temp.name, 'HIBENCH_START_UI': '0', 'HIBENCH_API_TOKEN': ''})
        self.env.start()
        self.spec = config.default_experiment()
        self.spec['workloads'] = [{'id': 'micro.sleep', 'parameters': {}}]
        self.spec['monitoring'] = {'enabled': True, 'interval_s': 5}
        self.run_id, _ = store.create(self.spec)
        self.ref = JobReference('docker', self.run_id, 'run', 'test',
                                dict(monitoring=self.spec['monitoring'], remote_dir='/tmp/test',
                                     workload='micro.sleep', index=0, expected_nodes=['worker']))

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def collect(self):
        reader = monitoring.legacy_reader()
        lines = [repr({'timestamp': float(t), 'hostname': 'worker.network',
                       'cpu/total': reader.CPU('cpu', 10, 0, 10, 80, 0, 0, 0),
                       'memory/total': reader.Memory('total', 100, 30, 20, 50, 0)}) for t in (100, 105)]
        backend = Mock()
        backend.command.return_value = json.dumps({'monitor.log': '\n'.join(lines), 'bench.log': ''})
        return monitoring.collect(backend, self.ref)

    def test_collect_api_export_and_reopen(self):
        report = self.collect()
        self.assertEqual(report['status'], 'available')
        self.assertEqual(report['sample_count'], 2)
        with TestClient(create_app()) as client:
            prefix = f'/api/v1/runs/{self.run_id}/monitoring'
            response = client.get(prefix)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()['reports'][0]['sample_count'], 2)
            page = client.get(prefix+'/'+report['id']+'/samples?offset=1&limit=1')
            self.assertEqual(page.status_code, 200, page.text)
            self.assertEqual(page.json()['samples'][0]['timestamp'], 105)
            html = client.get(prefix+'/'+report['id']+'/artifacts/report.html')
            self.assertEqual(html.status_code, 200)
            self.assertIn('CPU average', html.text)
            self.assertEqual(client.get(prefix+'/'+report['id']+'/artifacts/worker.log').status_code, 422)
        # Closing the application does not remove stored reports.
        self.assertEqual(monitoring.reports(self.run_id)['reports'][0]['id'], report['id'])
        with zipfile.ZipFile(io.BytesIO(experiments.export(self.run_id))) as archive:
            self.assertIn('monitoring/'+report['id']+'/samples.jsonl', archive.namelist())

    def test_failed_collection_and_missing_node_are_explicit(self):
        backend = Mock()
        backend.command.side_effect = RuntimeError('SSH unavailable')
        report = monitoring.collect(backend, self.ref)
        self.assertEqual(report['status'], 'unavailable')
        self.assertIn('SSH unavailable', report['errors'])
        self.ref.metadata['expected_nodes'].append('missing')
        self.assertEqual(self.collect()['status'], 'partial')
        self.assertEqual(store.get(self.run_id)['state'], 'queued')

    def test_validation_disabled_and_traversal(self):
        for invalid in ({'enabled': 'true'}, {'interval_s': 0}, {'interval_s': True}, {'unexpected': 1}):
            spec = copy.deepcopy(self.spec)
            spec['monitoring'] = invalid
            with self.assertRaises(ValueError):
                config.validate(spec)
        with self.assertRaises(ValueError):
            monitoring.artifact(self.run_id, '../../worker', 'report.html')
        self.ref.metadata['monitoring']['enabled'] = False
        self.assertIsNone(monitoring.collect(Mock(), self.ref))

    def test_comparison_includes_monitoring_overhead_setting(self):
        other = copy.deepcopy(self.spec)
        other['monitoring']['enabled'] = False
        right, _ = store.create(other)
        result = experiments.compare(self.run_id, right)
        self.assertTrue(any(d['field'] == 'monitoring.enabled' for d in result['differences']))

    def test_ui_displays_persisted_report(self):
        self.collect()
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_string('from hibench.ui.monitoring import render\nrender('+repr(self.run_id)+')').run()
        self.assertFalse(app.exception)
        self.assertEqual(app.selectbox[0].value, 'micro.sleep-run-0')
        self.assertTrue(any('2 samples' in item.value for item in app.markdown))

    def test_history_selection_opens_monitoring_without_duplicate_selector(self):
        self.collect()
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_string('from hibench.ui.executions import render\nrender()').run()
        self.assertFalse(any(item.label == 'Open run' for item in app.selectbox))
        self.assertTrue(next(b for b in app.button if b.label.startswith('Compare configurations')).disabled)
        key = 'run-history-' + app.dataframe[-1].proto.id.split('run-history-')[-1]
        app.session_state[key] = {'selection': {'rows': [0], 'columns': [], 'cells': []}}
        app.session_state['runs-tab'] = 'History'
        app.run()
        next(b for b in app.button if b.label == 'Open run').click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state['runs-tab'], 'Monitoring')
        self.assertEqual(app.session_state['active_run'], self.run_id)
        self.assertTrue(any('2 samples' in item.value for item in app.markdown))
