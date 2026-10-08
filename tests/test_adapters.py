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
        self.spec = config.load("configs/docker-yarn.yaml")

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

    def test_preparation_and_run_capabilities_are_separate(self):
        self.spec["workloads"] = [{"id": "ml.bayes", "parameters": {}}]
        adapter = create_adapter(self.spec["target"])
        with patch.object(adapter, "capabilities", return_value={"spark_batch", "classic_spark"}):
            with self.assertRaisesRegex(ValueError, "mapreduce"):
                adapter.validate(self.spec)
            self.spec["mode"] = "run"
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
            staged = dict(job=job, argv=[sys.executable, "-c", code], pidfile="/tmp/test.pid")
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
