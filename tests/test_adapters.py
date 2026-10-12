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

import copy
import json
import subprocess
import sys
import tempfile
import os
import unittest
from pathlib import Path
from unittest.mock import patch, create_autospec

from hibench import config
from hibench.adapters import REGISTRY, JobReference, PhaseJob, create_adapter
from hibench.adapters.classic import DockerAdapter, LocalAdapter
from hibench.adapters.base import ExecutionAdapter

class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.spec = config.default_experiment()

    def test_ssh_quotes_remote_arguments_and_enforces_host_verification(self):
        import shlex
        spec = config.resolve({'target': {'kind': 'ssh', 'host': 'localhost', 'username': 'sparker', 'port': 2222}})
        adapter = create_adapter(spec['target'])
        command = ['python3', '-c', "print('a; b $HOME')"]
        args = adapter.argv(command)
        self.assertIn('StrictHostKeyChecking=yes', args)
        self.assertIn('BatchMode=yes', args)
        self.assertEqual(shlex.split(args[-1]), ['exec', *command])
        for values in ({'host': '-oProxyCommand=bad'}, {'username': 'user;command'}, {'port': True}, {'host': None}):
            with self.assertRaises(ValueError):
                config.resolve({'target': {**spec['target'], **values}})

    def test_registry_and_planned_destinations(self):
        self.assertIsInstance(create_adapter(self.spec["target"]), DockerAdapter)
        target = {**self.spec["target"], "kind": "local"}
        self.assertIsInstance(create_adapter(target), LocalAdapter)
        for info in REGISTRY:
            if not info.implemented:
                with self.subTest(adapter=info.kind):
                    spec = copy.deepcopy(self.spec)
                    spec["target"] = {"kind": info.kind}
                    with self.assertRaisesRegex(ValueError, "planned but not implemented"):
                        config.validate(spec)

    def test_password_auth_does_not_put_secret_in_target_or_command(self):
        import shlex
        secret = 'test-secret-not-for-argv-or-export'
        with patch.dict(os.environ, {'HIBENCH_SSH_PASSWORD': secret}):
            spec = config.resolve({'target': {'kind': 'ssh', 'host': 'localhost', 'username': 'sparker', 'port': 2222}})
            adapter = create_adapter(spec['target'])
            args = adapter.argv(['printf', 'one argument; $HOME'])
            payload = json.loads(args[-1])
            self.assertEqual(payload['command'], ['printf', 'one argument; $HOME'])
            self.assertNotIn(secret, json.dumps(spec))
            self.assertNotIn(secret, json.dumps(args))
            self.assertNotIn('password', payload['target'])
            spec['target']['identity_file'] = '/controller/authorized-key'
            self.assertEqual(create_adapter(spec['target']).argv(['true'])[0], 'ssh')

    def test_password_transport_rejects_unknown_hosts_before_authentication(self):
        from hibench.adapters import ssh_password
        from unittest.mock import MagicMock
        import paramiko
        client = MagicMock()
        client.connect.side_effect = paramiko.SSHException('Server is not present in known_hosts')
        with patch.dict(os.environ, {'HIBENCH_SSH_PASSWORD': 'test-password'}), patch.object(paramiko, 'SSHClient', return_value=client):
            with self.assertRaises(paramiko.SSHException):
                ssh_password.execute({'host': 'localhost', 'username': 'sparker', 'known_hosts_file': '/controller/known_hosts'}, ['true'])
        self.assertIsInstance(client.set_missing_host_key_policy.call_args.args[0], paramiko.RejectPolicy)
        client.load_host_keys.assert_called_once_with(str(Path('/controller/known_hosts')))
        client.close.assert_called_once()

    def test_ssh_environment_defaults_are_resolved_and_explicit_values_win(self):
        with patch.dict(os.environ, {'HIBENCH_SSH_HOST': 'localhost', 'HIBENCH_SSH_USERNAME': 'sparker', 'HIBENCH_SSH_PORT': '2222'}):
            spec = config.resolve({'target': {'kind': 'ssh'}})
            self.assertEqual(spec['target']['host'], 'localhost')
            self.assertEqual(spec['target']['port'], 2222)
            explicit = config.resolve({'target': {'kind': 'ssh', 'host': 'other-host', 'port': 22}})
            self.assertEqual(explicit['target']['host'], 'other-host')
            self.assertEqual(explicit['target']['port'], 22)

    def test_preparation_and_run_capabilities_are_separate(self):
        self.spec["workloads"] = [{"id": "ml.bayes", "parameters": {}}]
        adapter = create_adapter(self.spec["target"])
        with patch.object(adapter, "capabilities", return_value={"spark_batch"}):
            adapter.validate(self.spec)
            self.spec["mode"] = "run"
            adapter.validate(self.spec)
        with patch.object(adapter, "capabilities", return_value=set()):
            with self.assertRaisesRegex(ValueError, "spark_batch"):
                adapter.validate(self.spec)

    def test_worker_uses_only_adapter_contract(self):
        from hibench import store, worker
        self.spec.update(mode="run", workloads=[{"id": "micro.sleep", "parameters": {}}])
        adapter = create_autospec(ExecutionAdapter, instance=True)
        adapter.dataset_key.return_value = "provider-dataset"
        adapter.dataset_root.return_value = "s3://example/dataset"
        adapter.doctor.return_value = "Provider ready"
        adapter.submit.return_value = JobReference("example", "example-run", "run", "provider-job-id")
        adapter.wait.side_effect = lambda ref, timeout, cancelled, emit, log: emit(
            {"kind": "result", "metrics": {"duration_s": 1}})
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"HIBENCH_STATE_DIR": tmp}):
            run_id, _ = store.create(self.spec)
            with patch("hibench.worker.create_adapter", return_value=adapter):
                worker.work(run_id)
            self.assertEqual(store.get(run_id)["state"], "succeeded")
            staged_job = adapter.stage.call_args.args[0]
            self.assertEqual(staged_job.dataset_root, "s3://example/dataset")
            self.assertEqual(staged_job.phase, "run")
            adapter.submit.assert_called_once()

    def test_text_workloads_need_spark_only_and_validate_byte_size(self):
        self.spec['workloads'] = [{'id': 'micro.wordcount', 'parameters': {}}, {'id': 'micro.sort', 'parameters': {}}]
        adapter = create_adapter(self.spec["target"])
        with patch.object(adapter, "capabilities", return_value={"spark_batch"}):
            adapter.validate(self.spec)
        for workload in self.spec["workloads"]:
            workload["parameters"] = {"datasize": 1}
            with self.assertRaisesRegex(ValueError, "at least 2 bytes"):
                config.validate(self.spec)
            workload["parameters"] = {"datasize": 129, "seed": 43}
        config.validate(self.spec)

    def test_classic_collection_and_authoritative_failure(self):
        adapter = create_adapter({**self.spec["target"], "kind": "local"})
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            job = PhaseJob("test-run", self.spec, self.spec["workloads"][0], "run", 0, "hdfs://test", directory)
            code = 'print("application_123_0001"); print(' + repr('HIBENCH_RESULT={"duration_s":1}') + ')'
            staged = dict(job=job, argv=[sys.executable, "-c", code], pidfile="/tmp/test.pid", remote_dir="/tmp")
            events = []
            reference = adapter.submit(staged)
            with patch.object(adapter, "command", return_value="Final-State : SUCCEEDED"):
                adapter.wait(reference, 10, lambda: False, events.append, directory / "output.log")
            self.assertTrue(any(e["kind"] == "result" for e in events))
            self.assertTrue(any(e["kind"] == "application" for e in events))
            first = adapter.logs(reference)
            self.assertIn("HIBENCH_RESULT", first["text"])
            self.assertEqual(adapter.logs(reference, first["cursor"])["text"], "")
            json.dumps(reference.snapshot())
            reference = adapter.submit(staged)
            with patch.object(adapter, "command", return_value="Final-State : FAILED"):
                with self.assertRaisesRegex(RuntimeError, "did not finish SUCCEEDED"):
                    adapter.wait(reference, 10, lambda: False, events.append, directory / "output.log")

    def test_cancel_and_timeout_use_adapter_cancellation(self):
        adapter = create_adapter({**self.spec["target"], "kind": "local", "master": "local[2]"})
        with tempfile.TemporaryDirectory() as tmp:
            for cancelled, timeout, expected in ((lambda: True, 10, InterruptedError), (lambda: False, 0, TimeoutError)):
                proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True)
                reference = JobReference("local", "test", "run", str(proc.pid),
                                         {"pidfile": "/tmp/test.pid", "application_ids": []}, proc)
                with patch.object(adapter, "cancel", side_effect=lambda ref: ref.runtime.terminate()) as cancel:
                    with self.assertRaises(expected):
                        adapter.wait(reference, timeout, cancelled, lambda event: None, Path(tmp) / "output.log")
                    cancel.assert_called_once_with(reference)
                self.assertIsNotNone(proc.poll())
