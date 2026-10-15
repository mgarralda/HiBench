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
import os
import tempfile
import unittest
import uuid
import zipfile
from unittest.mock import patch
from hibench.services import comparisons


def source():
    events = [{'kind': 'phase_started', 'phase': 'prepare', 'index': 0, 'workload': 'micro.wordcount'},
              {'kind': 'result', 'metrics': {'duration_s': 999}},
              {'kind': 'dataset_prepared', 'manifest': {'key': 'dataset', 'workload': {'id': 'micro.wordcount'}}}]
    for index, (language, duration) in enumerate([('scala', 10), ('pyspark', 12), ('scala', 20), ('pyspark', 18)], 1):
        events += [{'kind': 'phase_started', 'phase': 'run', 'workload': 'micro.wordcount', 'index': index},
                   {'kind': 'result', 'metrics': {'duration_s': duration, 'implementation': language,
                                                'deploy_mode': 'client', 'application_id': 'app-' + str(index)}},
                   {'kind': 'phase_finished', 'phase': 'run', 'workload': 'micro.wordcount', 'index': index}]
    return {'run': {'id': str(uuid.uuid4()), 'state': 'succeeded', 'spec': {'scale': 'small', 'implementation': 'both',
                   'resources': {}, 'target': {}, 'workloads': [{'id': 'micro.wordcount'}]}},
            'provenance': {'resolved_spark_properties': {}, 'effective_workload_parameters': {}},
            'events': events, 'metrics': []}


class ComparisonTests(unittest.TestCase):
    def test_phase_protocols_missing_measurements_and_exports(self):
        metrics = [e['metrics'] for e in self.source['events'] if e['kind'] == 'result'][1:]
        for row, seconds in zip(metrics, (2, 12, 4, 18)):
            row['workload_measurement'] = {'schema_version': 1, 'protocol': 'svd-phases-v1',
                'phase_seconds': {'gram_action_s': seconds, 'u_plan_s': 0, 'invalid_s': float('nan')},
                'numerical_runtime': {'effective_native_threads': None}}
        analysis = self.analyze()
        gram = next(r for r in analysis['phase_summary'] if r['phase'] == 'gram_action_s')
        self.assertEqual(gram['left']['median_s'], 3)
        self.assertEqual(gram['right']['median_s'], 15)
        self.assertEqual(gram['ratio'], 5)
        self.assertFalse(any(r['phase'] == 'invalid_s' for r in analysis['phase_summary']))
        self.assertIn('phase-summary.csv', comparisons.artifacts(analysis))
        self.assertIn('Instrumented phase durations', comparisons.artifacts(analysis)['report.html'])
        metrics[-1]['workload_measurement']['protocol'] = 'different-protocol'
        groups = self.analyze()['phase_summary']
        alternate = next(r for r in groups if r['protocol'] == 'different-protocol' and r['phase'] == 'gram_action_s')
        self.assertEqual(alternate['left']['count'], 0)
        self.assertIsNone(alternate['ratio'])
        del metrics[1]['workload_measurement']
        original = next(r for r in self.analyze()['phase_summary'] if r['protocol'] == 'svd-phases-v1' and r['phase'] == 'gram_action_s')
        self.assertEqual(original['right']['count'], 0)
        self.assertIsNone(original['ratio'])

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {'HIBENCH_STATE_DIR': self.temp.name, 'HIBENCH_START_UI': '0', 'HIBENCH_API_TOKEN': ''})
        env.start()
        self.addCleanup(env.stop)
        self.source = source()
        mock = patch('hibench.services.experiments.results', return_value=self.source)
        mock.start()
        self.addCleanup(mock.stop)

    def analyze(self):
        identifier = self.source['run']['id']
        return comparisons.analyze(identifier, identifier, 'scala', 'pyspark')

    def test_same_run_languages_and_repetitions(self):
        result = self.analyze()
        row = result['summary'][0]
        self.assertEqual(row['left']['count'], 2)
        self.assertEqual(row['left']['median_s'], 15)
        self.assertEqual(row['ratio'], 1)
        self.assertEqual(row['difference_pct'], 0)
        self.assertEqual(len(result['observations']['left']), 2)
        self.assertEqual(next(r for r in result['conditions'] if r['field'] == 'runtime_versions')['status'], 'Unknown')

    def test_no_pooling_active_runs_and_invalid_durations(self):
        with self.assertRaisesRegex(ValueError, 'must not be pooled'):
            comparisons.analyze('a', 'b')
        self.source['run']['state'] = 'running'
        with self.assertRaisesRegex(ValueError, 'active results'):
            self.analyze()
        self.source['run']['state'] = 'failed'
        self.source['events'][-2]['metrics']['duration_s'] = 0
        result = self.analyze()
        self.assertEqual(result['summary'][0]['right']['count'], 1)
        self.assertEqual(result['summary'][0]['ratio'], .8)
        self.source['events'].pop()
        self.assertEqual(self.analyze()['observations']['right'][-1]['exclusion'], 'Unfinished phase')
        self.source['events'][-1]['metrics']['duration_s'] = float('inf')
        self.assertIsNone(self.analyze()['observations']['right'][-1]['duration_s'])
        self.assertTrue(comparisons.export(self.analyze()))

    def test_save_is_snapshot_and_exports_are_offline_and_escaped(self):
        result = self.analyze()
        result['selection']['workload'] = '<script>alert(1)</script>'
        saved = comparisons.save(result)
        self.assertEqual(comparisons.source_status(saved), ['Unchanged', 'Unchanged'])
        self.source['run']['state'] = 'failed'
        self.assertEqual(comparisons.source_status(saved), ['Changed', 'Changed'])
        self.assertEqual(comparisons.load(saved['comparison_id']), saved)
        result['summary'].clear()
        self.assertTrue(comparisons.load(saved['comparison_id'])['summary'])
        self.assertEqual(comparisons.recent()[0]['comparison_id'], saved['comparison_id'])
        files = comparisons.artifacts(saved)
        self.assertIn('&lt;script&gt;', files['report.html'])
        self.assertIn('<svg', files['report.html'])
        self.assertNotIn('<script>', files['report.html'])
        self.assertIn('left_median_s', files['summary.csv'])
        with zipfile.ZipFile(io.BytesIO(comparisons.export(saved))) as bundle:
            self.assertEqual(set(bundle.namelist()), set(files))

    def test_missing_workload_has_no_fabricated_ratio(self):
        identifier = self.source['run']['id']
        self.assertEqual(comparisons.analyze(identifier, identifier, 'scala', 'pyspark', 'ml.svd')['summary'], [])
        other = copy.deepcopy(self.source)
        other['events'] = []
        with patch('hibench.services.experiments.results', side_effect=[self.source, other]):
            result = comparisons.analyze(identifier, identifier, 'scala', 'pyspark')
        self.assertIsNone(result['summary'][0]['ratio'])

    def test_api_analyze_save_export_and_invalid_selection(self):
        from fastapi.testclient import TestClient
        from hibench.api.app import create_app
        identifier = self.source['run']['id']
        selection = {'left': identifier, 'right': identifier, 'left_implementation': 'scala', 'right_implementation': 'pyspark'}
        with TestClient(create_app()) as client:
            self.assertEqual(client.post('/api/v1/comparisons/analyze', json=selection).status_code, 200)
            saved = client.post('/api/v1/comparisons', json=selection).json()
            self.assertEqual(client.get('/api/v1/comparisons').json()[0]['comparison_id'], saved['comparison_id'])
            self.assertEqual(client.get('/api/v1/comparisons/' + saved['comparison_id']).json(), saved)
            self.assertEqual(client.get('/api/v1/comparisons/' + saved['comparison_id'] + '/export').status_code, 200)
            self.assertEqual(client.get('/api/v1/comparisons/' + str(uuid.uuid4())).status_code, 404)
            self.assertEqual(client.post('/api/v1/comparisons', json={**selection, 'left': 'invalid'}).status_code, 422)

    def test_web_charts_and_downloads(self):
        from streamlit.testing.v1 import AppTest
        with patch('hibench.store.recent', return_value=[{'id': self.source['run']['id'], 'state': 'succeeded'}]), \
             patch('hibench.store.get', return_value=self.source['run']):
            app = AppTest.from_string('from hibench.ui.comparisons import render\nrender()').run(timeout=30)
        self.assertFalse(app.exception)
        self.assertEqual(len(app.dataframe), 3)
        self.assertEqual(len(app.get('vega_lite_chart')), 2)

    def test_completed_multilanguage_run_opens_comparison(self):
        import json
        from hibench import config, store
        from streamlit.testing.v1 import AppTest
        spec = config.default_experiment(selected_workloads=['micro.sleep'])
        spec['implementation'] = 'both'
        identifier, _ = store.create(spec)
        store.update(identifier, 'succeeded')
        self.source['run'] = store.get(identifier)
        (store.directory(identifier) / 'events.jsonl').write_text('\n'.join(json.dumps(e) for e in self.source['events']), encoding='utf-8')
        app = AppTest.from_string('from hibench.ui.executions import render\nrender()')
        app.session_state['active_run'] = identifier
        app.run(timeout=30)
        self.assertFalse(app.exception)
        next(b for b in app.button if b.label == 'Compare Scala and PySpark').click().run(timeout=30)
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state['runs-tab'], 'Comparisons')
        self.assertEqual(app.session_state['comparison-run-A'], identifier)
        self.assertEqual(app.session_state['comparison-run-B'], identifier)


if __name__ == '__main__':
    unittest.main()
