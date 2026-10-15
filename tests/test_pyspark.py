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
import os
import tempfile
import unittest
import ast
import re
from zipfile import ZipFile
from pathlib import Path
from unittest.mock import patch, create_autospec

from hibench import config
from hibench.adapters import PhaseJob, create_adapter
from hibench.adapters.runtime import dataset_key
from hibench.api.models import Experiment, ExperimentPreset
from hibench.adapters.base import ExecutionAdapter, JobReference


class PySparkTests(unittest.TestCase):
    def spec(self, implementation='both'):
        spec = config.default_experiment(selected_workloads=['micro.wordcount'])
        spec['implementation'] = implementation
        return config.validate(spec)

    def test_presets_and_api_round_trip(self):
        for implementation in ('scala', 'pyspark', 'both'):
            spec = self.spec(implementation)
            exported = ExperimentPreset(**config.overrides(spec)).model_dump(exclude_none=True)
            self.assertEqual(config.resolve(exported), spec)
            self.assertEqual(Experiment(**spec).implementation, implementation)

    def test_other_groups_supported_and_pyspark_cluster_rejected(self):
        spec = self.spec()
        spec['workloads'] = [{'id': 'sql.scan', 'parameters': {}}]
        self.assertEqual(config.validate(spec)['implementation'], 'both')
        spec = self.spec('pyspark')
        spec['target'].update(master='yarn', deploy_mode='cluster')
        with self.assertRaisesRegex(ValueError, 'client'):
            config.validate(spec)

    def test_all_workload_entry_points_have_individual_python_modules(self):
        from hibench.catalog import IDS
        from pysparkbench.registry import module_for
        for workload in IDS:
            category, name = workload.split('.')
            script = Path('bin/workloads') / category / name / 'spark/run.sh'
            scala_class = re.search(r'run_spark_job\s+(org\.hibench\.sparkbench\.[\w.$]+)', script.read_text())[1]
            scala_class = scala_class.replace('$CLASSNAME', 'ScalaRepartition')
            module = module_for(scala_class, name)
            path = Path(*module.split('.')).with_suffix('.py')
            self.assertTrue(path.is_file(), workload)
            nodes = ast.parse(path.read_text()).body
            functions = {node.name for node in nodes if isinstance(node, ast.FunctionDef)}
            functions.update(alias.name for node in nodes if isinstance(node, ast.ImportFrom) for alias in node.names)
            self.assertTrue({'parse', 'run'} <= functions, workload)
        self.assertFalse(Path('pysparkbench/micro/workload.py').exists())
        with self.assertRaises(ValueError):
            module_for('org.hibench.sparkbench.micro.TextDataGenerator')

    def test_python_bundle_preserves_namespace_and_excludes_bytecode(self):
        import importlib.util
        helper_spec = importlib.util.spec_from_file_location('package_pyspark', 'bin/functions/package_pyspark.py')
        helper = importlib.util.module_from_spec(helper_spec)
        helper_spec.loader.exec_module(helper)
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / 'pysparkbench.zip'
            helper.package('pysparkbench', destination)
            with ZipFile(destination) as archive:
                self.assertIn('pysparkbench/micro/wordcount.py', archive.namelist())
                self.assertIn('pysparkbench/ml/svd.py', archive.namelist())
                self.assertFalse(any('__pycache__' in name or name.endswith('.pyc') for name in archive.namelist()))

    def test_all_catalog_presets_accept_both_and_freeze_all_sources(self):
        from hibench.catalog import IDS
        from hibench.services.experiments import snapshot
        spec = config.default_experiment(selected_workloads=list(IDS))
        spec['implementation'] = 'both'
        frozen = snapshot(config.validate(spec))
        paths = frozen['file_sha256']
        for path in Path('pysparkbench').rglob('*.py'):
            self.assertIn(path.as_posix(), paths)
        self.assertEqual(Experiment(**spec).implementation, 'both')

    def test_staging_preserves_scala_generation_and_client_python(self):
        spec = self.spec()
        spec['target'].update(master='yarn', deploy_mode='cluster')
        adapter = create_adapter(spec['target'])
        with tempfile.TemporaryDirectory() as directory, patch.object(adapter, 'write_file'):
            for phase, implementation, expected in [('prepare', 'scala', 'cluster'),
                                                    ('run', 'scala', 'cluster'),
                                                    ('run', 'pyspark', 'client')]:
                job = PhaseJob('test', spec, spec['workloads'][0], phase, 0, '/data', Path(directory), implementation)
                adapter.stage(job)
                text = next(Path(directory).glob('*.conf')).read_text()
                self.assertIn('hibench.yarn.deploy.mode ' + expected, text)
                self.assertIn('hibench.workload.implementation ' + implementation, text)
        self.assertEqual(spec['target']['deploy_mode'], 'cluster')

    def test_implementations_reuse_the_same_dataset(self):
        spec = self.spec('scala')
        python_spec = copy.deepcopy(spec)
        python_spec['implementation'] = 'pyspark'
        self.assertEqual(dataset_key(spec, spec['workloads'][0]),
                         dataset_key(python_spec, python_spec['workloads'][0]))

    def test_worker_prepares_once_and_runs_both_per_repetition(self):
        from hibench import store, worker
        spec = self.spec()
        spec['repetitions'] = 2
        adapter = create_autospec(ExecutionAdapter, instance=True)
        adapter.dataset_key.return_value = 'both-test'
        adapter.dataset_root.return_value = 'hdfs://test/data'
        adapter.dataset_available.return_value = True
        adapter.doctor.return_value = 'ready'
        adapter.submit.return_value = JobReference('example', 'test', 'run', 'job')
        jobs = []
        adapter.stage.side_effect = lambda job: jobs.append(job) or job
        def wait(ref, timeout, cancelled, emit, log):
            if jobs[-1].phase == 'prepare':
                emit({'kind': 'input', 'path': 'hdfs://test/data/input'})
            else:
                emit({'kind': 'result', 'metrics': {'duration_s': 1}})
        adapter.wait.side_effect = wait
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            run_id, _ = store.create(spec)
            with patch('hibench.worker.create_adapter', return_value=adapter):
                worker.work(run_id)
            self.assertEqual(store.get(run_id)['state'], 'succeeded')
            self.assertEqual([(job.phase, job.implementation) for job in jobs],
                             [('prepare', 'scala'), ('run', 'scala'), ('run', 'pyspark'),
                              ('run', 'scala'), ('run', 'pyspark')])
            self.assertEqual(len({job.dataset_root for job in jobs}), 1)
            metrics = [event['metrics'] for event in worker.events(run_id) if event['kind'] == 'result']
            self.assertEqual([row['implementation'] for row in metrics], ['scala', 'pyspark'] * 2)

    def test_api_resolves_and_exports_implementation(self):
        from fastapi.testclient import TestClient
        from hibench.api.app import create_app
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                'HIBENCH_STATE_DIR': directory, 'HIBENCH_START_UI': '0', 'HIBENCH_API_TOKEN': ''}):
            with TestClient(create_app()) as client:
                response = client.post('/api/v1/experiments/resolve', json={'experiment': {
                    'implementation': 'both', 'workloads': [{'id': 'micro.sleep', 'parameters': {}}]}})
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(response.json()['implementation'], 'both')
                catalog = client.get('/api/v1/workloads').json()
                self.assertEqual(next(item for item in catalog if item['id'] == 'micro.sleep')['implementations'],
                                 ['scala', 'pyspark'])

    def test_ui_pyspark_only_offers_client(self):
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            next(widget for widget in app.multiselect if widget.label == 'Batch workloads').set_value(['micro.sleep'])
            next(widget for widget in app.selectbox if widget.label == 'Implementation').set_value('pyspark').run()
            self.assertFalse(app.exception)
            deploy = next(widget for widget in app.selectbox if widget.label == 'Deploy mode')
            self.assertEqual(deploy.options, ['client'])
            self.assertEqual(deploy.value, 'client')
