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
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock
import zipfile
from fastapi.testclient import TestClient
from hibench import config, store
from hibench.api.app import create_app
from hibench.services import configuration, experiments


class ApiTests(TestCase):
    def test_ssh_target_is_typed_and_invalid_fields_are_rejected(self):
        spec = copy.deepcopy(self.spec)
        spec['target'].pop('container', None)
        spec['target'].update(kind='ssh', host='localhost', username='sparker', port=2222)
        self.assertEqual(self.client.post('/api/v1/experiments/validate', json=spec).status_code, 200)
        spec['target']['port'] = '2222'
        self.assertEqual(self.client.post('/api/v1/experiments/validate', json=spec).status_code, 422)

    def setUp(self):
        self.temp = TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {'HIBENCH_STATE_DIR': self.temp.name, 'HIBENCH_START_UI': '0', 'HIBENCH_API_TOKEN': ''})
        self.env.start()
        self.spec = config.default_experiment()
        self.spec['workloads'] = [{'id': 'micro.sleep', 'parameters': {}}]
        self.client = TestClient(create_app())
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None, None, None)
        self.env.stop()
        self.temp.cleanup()

    def test_catalog_and_strict_validation(self):
        self.assertEqual(self.client.get('/health').status_code, 200)
        self.assertEqual(len(self.client.get('/api/v1/workloads').json()), 24)
        invalid = copy.deepcopy(self.spec)
        invalid['spark_conf']['spark.executor.cores'] = 9
        self.assertEqual(self.client.post('/api/v1/experiments/validate', json=invalid).status_code, 422)
        invalid = copy.deepcopy(self.spec)
        invalid['repetitions'] = '2'
        self.assertEqual(self.client.post('/api/v1/experiments/validate', json=invalid).status_code, 422)

    def test_profile_precedence_and_snapshot_isolation(self):
        settings = {key: self.spec[key] for key in configuration.SECTIONS}
        settings = copy.deepcopy(settings)
        settings['spark_conf']['spark.sql.adaptive.enabled'] = True
        self.assertEqual(self.client.put('/api/v1/profiles/lab', json=settings).status_code, 200)
        experiment = {key: value for key, value in self.spec.items() if key not in configuration.SECTIONS}
        experiment['spark_conf'] = {'spark.sql.adaptive.enabled': False}
        response = self.client.post('/api/v1/experiments/resolve', json={'experiment': experiment, 'profile': 'lab'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIs(response.json()['spark_conf']['spark.sql.adaptive.enabled'], False)
        run_id, _ = store.create(response.json())
        settings['target']['hdfs_uri'] = 'hdfs://changed:9000'
        self.client.put('/api/v1/profiles/lab', json=settings)
        self.assertNotEqual(store.get(run_id)['spec']['target']['hdfs_uri'], 'hdfs://changed:9000')

    def test_submission_idempotency_export_logs_duplicate_and_cancel(self):
        with mock.patch('hibench.services.experiments.worker.launch') as launch:
            request = {'experiment': self.spec, 'request_key': 'unique'}
            first = self.client.post('/api/v1/runs', json=request)
            self.assertEqual(first.status_code, 202, first.text)
            run_id = first.json()['run_id']
            self.assertFalse(self.client.post('/api/v1/runs', json=request).json()['created'])
            launch.assert_called_once()
            changed = copy.deepcopy(request)
            changed['experiment']['scale'] = 'tiny' if self.spec['scale'] != 'tiny' else 'small'
            self.assertEqual(self.client.post('/api/v1/runs', json=changed).status_code, 422)
        (store.directory(run_id) / 'output.log').write_text('abcdef')
        logs = self.client.get(f'/api/v1/runs/{run_id}/logs?offset=2&limit=2').json()
        self.assertEqual(logs['text'], 'cd')
        self.assertEqual(logs['next_offset'], 4)
        self.assertEqual(self.client.get(f'/api/v1/runs/{run_id}/results').status_code, 200)
        bundle = self.client.get(f'/api/v1/runs/{run_id}/export')
        with zipfile.ZipFile(io.BytesIO(bundle.content)) as archive:
            self.assertIn('provenance.json', archive.namelist())
        duplicate = self.client.post(f'/api/v1/runs/{run_id}/duplicate').json()
        self.assertEqual(duplicate['target'], self.spec['target'])
        self.client.post(f'/api/v1/runs/{run_id}/cancel')
        self.assertEqual(store.get(run_id)['cancel'], 1)

    def test_comparison_detects_parameters_and_missing_provenance(self):
        left, _ = store.create(self.spec)
        right, _ = store.create(self.spec)
        self.assertTrue(experiments.compare(left, right)['configuration_equivalent'])
        (store.directory(right) / 'provenance.json').unlink()
        self.assertFalse(experiments.compare(left, right)['configuration_equivalent'])

    def test_all_catalog_defaults_can_be_frozen(self):
        from hibench.catalog import workloads
        spec = copy.deepcopy(self.spec)
        spec['workloads'] = [{'id': w['id'], 'parameters': {}} for w in workloads()]
        frozen = experiments.snapshot(config.validate(spec))
        self.assertEqual(len(frozen['effective_workload_parameters']), 24)
        schema = next(w for w in workloads() if w['id'] == 'micro.terasort')['parameters']
        self.assertEqual(frozen['effective_workload_parameters']['micro.terasort']['datasize'], int(schema['datasize']['presets'][spec['scale']]))
        self.assertIsInstance(frozen['effective_workload_parameters']['ml.rf']['model_seed'], int)
        for workload in spec['workloads']:
            workload['parameters'] = frozen['effective_workload_parameters'][workload['id']]
        config.validate(spec)

    def test_duplication_pins_defaults_and_comparison_uses_effective_values(self):
        left, _ = store.create(self.spec)
        duplicate = experiments.duplicate(left)
        self.assertIn('mapper.seconds', duplicate['workloads'][0]['parameters'])
        right, _ = store.create(duplicate)
        self.assertTrue(experiments.compare(left, right)['configuration_equivalent'])

    def test_unknown_runs_and_authentication(self):
        self.assertEqual(self.client.get('/api/v1/runs/00000000-0000-0000-0000-000000000000').status_code, 404)
        self.assertEqual(self.client.get('/api/v1/runs/invalid').status_code, 422)
        with mock.patch.dict(os.environ, {'HIBENCH_API_TOKEN': 'test-token'}):
            self.assertEqual(self.client.get('/api/v1/runs').status_code, 401)
            self.assertEqual(self.client.get('/api/v1/runs', headers={'Authorization': 'Bearer test-token'}).status_code, 200)

    def test_lifespan_does_not_launch_or_cancel_benchmarks(self):
        with mock.patch('hibench.worker.launch') as launch, mock.patch('hibench.store.cancel') as cancel:
            with TestClient(create_app()) as client:
                client.get('/health')
            launch.assert_not_called()
            cancel.assert_not_called()

    def test_lifespan_owns_only_its_ui_process(self):
        process = mock.Mock()
        process.poll.return_value = None
        with mock.patch.dict(os.environ, {'HIBENCH_START_UI': '1'}), \
                mock.patch('hibench.api.app.subprocess.Popen', return_value=process), \
                mock.patch('hibench.api.app.asyncio.sleep', new_callable=mock.AsyncMock), \
                mock.patch('hibench.store.cancel') as cancel:
            with TestClient(create_app()) as client:
                self.assertEqual(client.get('/health').status_code, 200)
            process.terminate.assert_called_once()
            process.wait.assert_called_once()
            cancel.assert_not_called()

    def test_launch_failure_is_recorded_instead_of_left_queued(self):
        with mock.patch('hibench.services.experiments.worker.launch', side_effect=RuntimeError('cannot spawn')):
            with self.assertRaises(RuntimeError):
                experiments.submit(self.spec, 'spawn-failure')
        self.assertEqual(store.recent()[0]['state'], 'failed')

    def test_openapi_contracts_and_scale_enum(self):
        schema = self.client.get('/openapi.json').json()
        for path, model in (('/api/v1/configuration/hibench', 'BenchmarkBase'),
                            ('/api/v1/runs/{run_id}/preset', 'ExperimentPreset'),
                            ('/api/v1/runs/{run_id}/spark-history', 'SparkHistoryLinks')):
            response = schema['paths'][path]['get']['responses']['200']['content']['application/json']['schema']
            self.assertEqual(response['$ref'], '#/components/schemas/' + model)
        self.assertEqual(self.client.get('/api/v1/experiments/defaults?scale=typo').status_code, 422)
        self.assertEqual(self.client.get('/api/v1/configuration/hibench').status_code, 200)
        self.assertEqual(self.client.get('/docs').status_code, 200)

    def test_preset_roundtrip_has_no_injected_null_fields(self):
        run_id, _ = store.create(self.spec)
        response = self.client.get(f'/api/v1/runs/{run_id}/preset')
        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(None, response.json().values())
        resolved = self.client.post('/api/v1/experiments/resolve', json={'experiment': response.json()})
        self.assertEqual(resolved.status_code, 200, resolved.text)
        self.assertEqual(resolved.json()['scale'], self.spec['scale'])
        self.assertEqual(resolved.json()['workloads'][0]['id'], 'micro.sleep')

    def test_install_is_blocked_when_registry_contains_active_run(self):
        store.create(self.spec)
        with mock.patch('hibench.services.runtime.create_adapter') as adapter:
            response = self.client.post('/api/v1/diagnostics/install', json=self.spec)
            self.assertEqual(response.status_code, 422, response.text)
            adapter.assert_not_called()

    def test_history_api_returns_links_and_rejects_invalid_base(self):
        run_id, _ = store.create(self.spec)
        (store.directory(run_id) / 'events.jsonl').write_text(json.dumps({'kind': 'result', 'metrics': {'application_id': 'application_1_2'}})+'\n')
        response = self.client.get(f'/api/v1/runs/{run_id}/spark-history', params={'base_url': 'http://localhost:18080'})
        self.assertEqual(response.status_code, 200, response.text)
        app = response.json()['applications'][0]
        self.assertEqual(app['history_url'], 'http://localhost:18080/history/application_1_2/')
        self.assertTrue(app['event_logs_url'].endswith('/logs'))
        self.assertEqual(self.client.get(f'/api/v1/runs/{run_id}/spark-history', params={'base_url': 'file:///tmp'}).status_code, 422)

    def test_pagination_does_not_hide_active_runs_from_install_guard(self):
        # An old active row can fall outside the latest 100 displayed runs.
        run_id, _ = store.create(self.spec)
        with store.connect() as db:
            db.execute('UPDATE runs SET created=0 WHERE id=?', (run_id,))
            for index in range(101):
                import uuid
                item = str(uuid.uuid4())
                db.execute('INSERT INTO runs(id,request_key,state,spec,created,updated) VALUES(?,?,?,?,?,?)',
                           (item, item, 'succeeded', json.dumps(self.spec), index+1, index+1))
        self.assertEqual(len(store.recent()), 100)
        self.assertTrue(store.has_active_runs())
        first = self.client.get('/api/v1/runs?limit=2&offset=0').json()
        second = self.client.get('/api/v1/runs?limit=2&offset=2').json()
        self.assertEqual(len(first), 2)
        self.assertTrue({r['id'] for r in first}.isdisjoint(r['id'] for r in second))
        self.assertEqual(self.client.get('/api/v1/runs?limit=0').status_code, 422)

    def test_runtime_failures_have_actionable_typed_errors(self):
        import subprocess
        for error, status in ((subprocess.CalledProcessError(1, ['docker'], output='container stopped'), 503),
                              (subprocess.TimeoutExpired(['docker'], 5), 504),
                              (FileNotFoundError('docker executable missing'), 503)):
            with mock.patch('hibench.api.routes.create_adapter') as adapter:
                adapter.return_value.doctor.side_effect = error
                response = self.client.post('/api/v1/diagnostics/doctor', json=self.spec)
                self.assertEqual(response.status_code, status, response.text)
                self.assertIsInstance(response.json()['detail'], str)

    def test_cluster_status_failure_is_an_actionable_service_error(self):
        import subprocess
        with mock.patch('hibench.services.clusters.plan', return_value=[['docker', 'compose', 'ps']]), mock.patch('hibench.services.clusters.subprocess.run') as command:
            command.return_value = subprocess.CompletedProcess(['docker', 'compose'], 1, '', 'Docker unavailable')
            response = self.client.get('/api/v1/cluster/status')
        self.assertEqual(response.status_code, 503, response.text)
        self.assertIn('Docker unavailable', response.json()['detail'])

    def test_full_submission_requires_resources_and_import_inherits_conf(self):
        minimal = copy.deepcopy(self.spec)
        minimal.pop('resources')
        self.assertEqual(self.client.post('/api/v1/experiments/validate', json=minimal).status_code, 422)
        response = self.client.post('/api/v1/experiments/resolve', json={'experiment': {'scale': 'tiny', 'workloads': [{'id': 'micro.sleep'}]}})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()['resources'], config.default_experiment(scale='tiny')['resources'])
        self.assertEqual(response.json()['monitoring'], config.default_experiment(scale='tiny')['monitoring'])
