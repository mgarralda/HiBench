import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from hibench import config
from hibench.catalog import requires_input, workloads
from hibench.adapters import create_adapter
from hibench.backend import properties


class MicroContractTests(unittest.TestCase):
    def test_original_scale_presets_are_preserved(self):
        reference = json.loads(Path("docs/micro-scale-reference.json").read_text())["workloads"]
        catalog = {item["id"]: item for item in workloads()}
        for workload, expected in reference.items():
            actual = catalog[workload]["parameters"][expected["parameter"]]["presets"]
            self.assertEqual({scale: int(value) for scale, value in actual.items()}, expected["presets"])

    def test_micro_only_needs_spark_batch(self):
        spec = config.load("configs/smoke-micro.yaml")
        adapter = create_adapter(spec["target"])
        with patch.object(adapter, "capabilities", return_value={"spark_batch"}):
            adapter.validate(spec)
        for item in workloads():
            if item["category"] == "micro":
                self.assertFalse(item["requires_classic"])
                self.assertNotIn("mapreduce", item["prepare_capabilities"])
        emitted = properties(spec, spec["workloads"][0], "test", "/data", "/report", "/output")
        self.assertNotIn("hibench.hadoop.examples", emitted)

    def test_in_memory_repartition_and_sleep_have_no_input(self):
        self.assertFalse(requires_input({"id": "micro.sleep"}))
        self.assertFalse(requires_input({"id": "micro.repartition", "parameters": {"fromhdfs": False}}))
        self.assertTrue(requires_input({"id": "micro.repartition"}))

    def test_tera_size_and_memory_overflow_are_rejected(self):
        spec = config.load("configs/smoke-micro.yaml")
        for params in ({"datasize": 0}, {"datasize": 2**63},
                       {"datasize": (2**63 - 1) // 100, "fromhdfs": False}):
            bad = copy.deepcopy(spec)
            bad["workloads"] = [{"id": "micro.repartition", "parameters": params}]
            with self.assertRaises(ValueError):
                config.validate(bad)

    def test_no_micro_rdd_or_legacy_example_dependency(self):
        root = Path("sparkbench/micro/src/main")
        for path in root.rglob("*.scala"):
            source = path.read_text(encoding="utf-8")
            for token in ("sparkContext", "new SparkContext", ".rdd", "org.apache.hadoop.examples"):
                self.assertNotIn(token, source, str(path))
        self.assertNotIn("hadoop-mapreduce-examples", Path("sparkbench/micro/pom.xml").read_text())
