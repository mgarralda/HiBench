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

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from streamlit.testing.v1 import AppTest


class UIReviewTests(unittest.TestCase):
    def test_monitoring_reuses_configuration_without_submitting(self):
        from hibench import config, store
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}), \
             patch('hibench.services.experiments.submit') as submit:
            spec = config.default_experiment(selected_workloads=['micro.wordcount'])
            run_id, _ = store.create(spec)
            store.update(run_id, 'succeeded', 'Completed test fixture')
            import json
            from hibench import worker
            worker.append(run_id, {'kind': 'phase_started', 'phase': 'run', 'workload': 'micro.wordcount', 'index': 0})
            worker.append(run_id, {'kind': 'result', 'metrics': {'implementation': 'scala',
                'application_id': 'local-123', 'duration_s': 3, 'workload_measurement': {
                    'schema_version': 1, 'protocol': 'test-phases',
                    'phase_seconds': {'workload_s': 2}, 'numerical_runtime': {'effective_native_threads': None}}}})
            worker.append(run_id, {'kind': 'phase_finished', 'phase': 'run', 'workload': 'micro.wordcount', 'index': 0})
            from hibench.services.experiments import duplicate
            expected = duplicate(run_id)
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            app.session_state.active_run = run_id
            app.session_state['runs-navigation'] = 'Monitoring'
            app.button(key='nav-Runs').click().run()
            self.assertFalse(app.exception)
            self.assertTrue(any(e.label == 'Instrumented workload phases' for e in app.expander))
            self.assertFalse(any(w.label in ('Run', 'New run', 'Cancel run') for w in app.button))
            self.assertTrue(any(w.label == 'Download run configuration' for w in app.get('download_button')))
            next(w for w in app.button if w.label == 'Reuse configuration').click().run()
            self.assertEqual(app.session_state.ui_page, 'Experiments')
            self.assertEqual(app.session_state.experiment['workloads'], expected['workloads'])
            self.assertFalse(app.exception)
            submit.assert_not_called()

    def test_target_actions_do_not_submit_and_stale_checks_are_invalidated(self):
        from hibench.adapters import create_adapter
        from hibench import config
        adapter = create_adapter(config.default_experiment()['target'])
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}), \
             patch('hibench.ui.experiments.create_adapter', return_value=adapter), \
             patch.object(adapter, 'doctor', return_value='HIBENCH_DOCTOR_OK') as doctor, \
             patch('hibench.services.runtime.install') as install, \
             patch('hibench.services.experiments.submit') as submit:
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            self.assertFalse(any(w.label == 'Master container' for w in app.text_input))
            self.assertFalse(any(w.label == 'Execution adapter' for w in app.selectbox))
            next(w for w in app.button if w.label == 'Check connection').click().run()
            doctor.assert_called_once()
            next(w for w in app.button if w.label == 'Update target').click().run()
            install.assert_called_once_with(app.session_state.experiment['target'])
            self.assertEqual(doctor.call_count, 2)
            submit.assert_not_called()
            import copy
            draft = copy.deepcopy(app.session_state.experiment)
            draft['target']['container'] = 'another-target'
            app.session_state.experiment = draft
            app.run()
            self.assertTrue(any('has not been checked' in w.value for w in app.info))
            self.assertFalse(app.exception)

    def test_target_connection_edits_are_applied_and_survive_navigation(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            next(w for w in app.button if w.label == 'Edit target & storage').click().run()
            next(w for w in app.text_input if w.label == 'Master container').set_value('new-master')
            next(w for w in app.button if w.label == 'Apply target settings').click().run()
            self.assertEqual(app.session_state.experiment['target']['container'], 'new-master')
            app.button(key='nav-Experiments').click().run()
            self.assertEqual(app.session_state.experiment['target']['container'], 'new-master')
            self.assertFalse(app.exception)

    def test_spark_mode_selection_preserves_valid_master_and_deployment(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            def select(label):
                return next(widget for widget in app.selectbox if widget.label == label)
            select('Spark execution mode').set_value('yarn').run()
            self.assertEqual(select('Deploy mode').options, ['client', 'cluster'])
            select('Deploy mode').set_value('cluster').run()
            self.assertEqual(app.session_state.experiment['target']['deploy_mode'], 'cluster')
            select('Spark execution mode').set_value('standalone').run()
            self.assertEqual(select('Deploy mode').options, ['client'])
            self.assertTrue(select('Deploy mode').disabled)
            url = next(widget for widget in app.text_input if widget.label == 'Standalone master URL')
            url.set_value('spark://spark-cluster-master:7077').run()
            self.assertFalse(app.exception)
            self.assertEqual(app.session_state.experiment['target']['master'], 'spark://spark-cluster-master:7077')
            self.assertEqual(app.session_state.experiment['target']['deploy_mode'], 'client')

            # Imported presets and infrastructure profiles replace the draft;
            # prior widget selections must not override the loaded target.
            import copy
            imported = copy.deepcopy(app.session_state.experiment)
            imported['target']['master'] = 'spark://other-master:7077'
            app.session_state.experiment = imported
            app.run()
            self.assertEqual(select('Spark execution mode').value, 'standalone')
            self.assertEqual(app.session_state.experiment['target']['master'], 'spark://other-master:7077')
            select('Spark execution mode').set_value('local').run()
            self.assertEqual(app.session_state.experiment['target']['master'], 'local[2]')
            self.assertEqual(select('Deploy mode').options, ['client'])
            select('Spark execution mode').set_value('yarn').run()
            select('Implementation').set_value('both').run()
            select('Deploy mode').set_value('cluster').run()
            self.assertEqual(app.session_state.experiment['target']['master'], 'yarn')
            self.assertEqual(app.session_state.experiment['target']['deploy_mode'], 'cluster')
            select('Implementation').set_value('pyspark').run()
            self.assertEqual(select('Deploy mode').options, ['client'])
            self.assertEqual(app.session_state.experiment['target']['deploy_mode'], 'client')

    def test_all_navigation_pages_render_and_planned_adapter_cannot_submit(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
            self.assertFalse(app.exception)
            for key in ('SparkConfiguration', 'HiBenchConfiguration', 'Workloads', 'General', 'Clusters', 'Help', 'About', 'Runs', 'Experiments'):
                app.button(key='nav-' + key).click().run()
                self.assertFalse(app.exception, key)
            app.button(key='nav-General').click().run()
            adapter = next(item for item in app.selectbox if item.label == 'Execution adapter')
            adapter.set_value('dataproc').run()
            self.assertFalse(app.exception)
            self.assertTrue(next(button for button in app.button if button.label == 'Apply target settings').disabled)
            import copy
            planned = copy.deepcopy(app.session_state.experiment)
            planned['target']['kind'] = 'dataproc'
            app.session_state.experiment = planned
            app.button(key='nav-Experiments').click().run()
            self.assertTrue(next(button for button in app.button if button.label == 'Run').disabled)
