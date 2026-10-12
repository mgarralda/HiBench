# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

import json, tempfile, os, unittest, copy, subprocess, sys
from pathlib import Path
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from hibench import config, store
from hibench.backend import ClassicBackend

class ControlTests(unittest.TestCase):
    def setUp(self):
        self.spec=config.default_experiment()
        self.tmp=tempfile.TemporaryDirectory()
        self.env=patch.dict(os.environ, {'HIBENCH_STATE_DIR':self.tmp.name})
        self.env.start()
    def tearDown(self):
        self.env.stop(); self.tmp.cleanup()
    def test_injection_and_unknown_parameters_rejected(self):
        for mutate in (lambda x:x['target'].update(workspace='/tmp/../etc'), lambda x:x['spark_conf'].update({'spark.executor.memory':'9g'}), lambda x:x['workloads'][0]['parameters'].update({'invented':1})):
            spec=copy.deepcopy(self.spec); mutate(spec)
            with self.assertRaises(ValueError): config.validate(spec)
    def test_idempotence_claim_and_destination_lease(self):
        first,created=store.create(self.spec,'click'); self.assertTrue(created)
        self.assertEqual(store.create(self.spec,'click'),(first,False))
        changed=copy.deepcopy(self.spec); changed['scale']='tiny' if self.spec['scale'] != 'tiny' else 'small'
        with self.assertRaises(ValueError):store.create(changed,'click')
        self.assertTrue(store.claim(first)); self.assertFalse(store.claim(first))
        second,_=store.create(self.spec)
        self.assertTrue(store.acquire(first,self.spec['target']))
        self.assertFalse(store.acquire(second,self.spec['target']))
        store.release(first); self.assertTrue(store.acquire(second,self.spec['target']))
        store.cancel(second); self.assertEqual(store.get(second)['cancel'],1)
    def test_simultaneous_requests_create_one_run(self):
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _:store.create(self.spec,'simultaneous-click'),range(4)))
        self.assertEqual(len({run_id for run_id,_ in results}),1)
        self.assertEqual(sum(created for _,created in results),1)

    def test_docker_and_ssh_share_the_canonical_cluster_lease(self):
        first, _ = store.create(self.spec)
        second, _ = store.create(self.spec)
        ssh_target = {**self.spec['target'], 'kind': 'ssh', 'host': '127.0.0.1', 'username': 'sparker', 'port': 2222}
        self.assertTrue(store.acquire(first, self.spec['target']))
        self.assertFalse(store.acquire(second, ssh_target))
        store.release(first)
        self.assertTrue(store.acquire(second, ssh_target))
    def test_fractional_record_counts_are_rejected(self):
        self.spec['workloads']=[{'id':'ml.kmeans','parameters':{'num_of_samples':1.5}}]
        with self.assertRaises(ValueError): config.validate(self.spec)
    def test_run_snapshot_and_lease_survive_a_new_python_process(self):
        run_id, _ = store.create(self.spec, 'restart-check')
        self.assertTrue(store.claim(run_id))
        self.assertTrue(store.acquire(run_id, self.spec['target']))
        store.update(run_id, state='running', detail='before restart')
        code = ('import json; from hibench import store; '
                'row=store.get(' + repr(run_id) + '); '
                'print(json.dumps(dict(state=row["state"], detail=row["detail"], '
                'spec=row["spec"], snapshot=json.loads((store.directory(row["id"])/'
                '"experiment.json").read_text()), '
                'lease_acquired=store.acquire(row["id"],row["spec"]["target"]))))')
        result = subprocess.run([sys.executable, '-c', code], capture_output=True,
                                text=True, check=True, timeout=20)
        restored = json.loads(result.stdout)
        self.assertEqual(restored['state'], 'running')
        self.assertEqual(restored['detail'], 'before restart')
        self.assertEqual(restored['snapshot'], restored['spec'])
        self.assertFalse(restored['lease_acquired'])
        # Persistence is demonstrated; automatic crash reconciliation is not.
        store.release(run_id)
    def test_sleep_does_not_require_a_physical_dataset(self):
        backend=ClassicBackend(self.spec['target'])
        with patch.object(backend,'command') as command:
            self.assertTrue(backend.dataset_available({'requires_input':False,'input_path':None}))
            command.assert_not_called()
    def test_subprocess_preserves_arguments_and_failure(self):
        log=Path(self.tmp.name)/'with spaces.log'
        result=subprocess.run([sys.executable,'bin/functions/execute_with_log.py',str(log),sys.executable,'-c','import sys; print(sys.argv[1]); print("application_123_0042"); sys.exit(7)','one argument with spaces'],capture_output=True,text=True)
        self.assertEqual(result.returncode,7)
        self.assertIn('one argument with spaces',log.read_text())
        meta=json.loads(Path(str(log)+'.execution.json').read_text())
        self.assertEqual(meta['returncode'],7); self.assertEqual(meta['application_id'],'application_123_0042')
    def test_streamlit_loads_without_exception(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path('hibench/web.py').resolve())).run(timeout=20)
        self.assertFalse(app.exception)
        app.multiselect[0].set_value(['ml.kmeans']).run()
        self.assertFalse(app.exception)
    def test_streamlit_repeated_run_does_not_duplicate_submission(self):
        from streamlit.testing.v1 import AppTest
        with patch('hibench.worker.launch') as submit:
            app=AppTest.from_file(str(Path('hibench/web.py').resolve())).run(timeout=20)
            next(button for button in app.button if button.label == 'Run').click().run()
            next(button for button in app.button if button.label == 'Run').click().run()
            self.assertFalse(app.exception)
            self.assertEqual(submit.call_count,1)
            self.assertEqual(len(store.recent()),1)
    def test_destination_selection_and_independent_connection_check(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path('hibench/web.py').resolve())).run(timeout=20)
        self.assertEqual(app.selectbox[0].label, 'Execution adapter')
        app.selectbox[0].set_value('local').run()
        self.assertFalse(any(field.label == 'Master container' for field in app.text_input))
        app.multiselect[0].set_value([]).run()
        with patch('hibench.adapters.classic.LocalAdapter.doctor', return_value='Runtime comprobado') as doctor:
            next(button for button in app.button if button.label == 'Check connection').click().run()
            doctor.assert_called_once()
        self.assertFalse(app.exception)

if __name__=='__main__': unittest.main()
