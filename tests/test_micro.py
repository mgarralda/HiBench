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
import unittest
from pathlib import Path
from unittest.mock import patch

from hibench import config
from hibench.catalog import requires_input, workloads
from hibench.adapters import create_adapter
from hibench.adapters.runtime import properties


class MicroContractTests(unittest.TestCase):
    def test_recalibrated_payload_families(self):
        catalog = {item['id']: item for item in workloads()}
        for scale in ('tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'):
            text = int(catalog['micro.wordcount']['parameters']['datasize']['presets'][scale])
            self.assertEqual(int(catalog['micro.sort']['parameters']['datasize']['presets'][scale]), text)
            for name in ('micro.terasort', 'micro.repartition'):
                self.assertEqual(int(catalog[name]['parameters']['datasize']['presets'][scale]) * 100, text)

    def test_micro_only_needs_spark_batch(self):
        spec = config.load("tests/fixtures/experiments/smoke-micro.yaml")
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
        spec = config.load("tests/fixtures/experiments/smoke-micro.yaml")
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

    def test_dynamic_memory_repartition_and_sleep_receive_explicit_tasks(self):
        folder = Path('sparkbench/micro/src/main/scala/org/hibench/sparkbench/micro')
        for name in ('ScalaSleep.scala', 'ScalaInMemRepartition.scala'):
            self.assertNotIn('spark.default.parallelism', (folder / name).read_text(encoding='utf-8'))
        self.assertIn('val parts = args(4).toInt', (folder / 'ScalaInMemRepartition.scala').read_text(encoding='utf-8'))
        self.assertIn('val tasks = args(1).toInt', (folder / 'ScalaSleep.scala').read_text(encoding='utf-8'))
        self.assertIn('$DISABLE_OUTPUT $NUM_MAPS', Path('bin/workloads/micro/repartition/spark/run.sh').read_text(encoding='utf-8'))
        self.assertIn('$MAP_SLEEP_TIME $NUM_MAPS', Path('bin/workloads/micro/sleep/spark/run.sh').read_text(encoding='utf-8'))
