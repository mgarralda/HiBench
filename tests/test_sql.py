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

import json
import hashlib
import unittest
from pathlib import Path
from hibench.catalog import workloads
from hibench import config

class SqlContractTests(unittest.TestCase):
    def test_original_scales_preserved(self):
        reference = json.loads(Path("tests/fixtures/contracts/sql-scale-reference.json").read_text())
        catalog = {w["id"]:w for w in workloads()}
        for name, scales in reference.items():
            item = catalog["sql." + name]
            for scale, fields in scales.items():
                for field, expected in fields.items():
                    self.assertEqual(item["parameters"][field]["presets"][scale], expected)
            self.assertFalse(item["requires_classic"])
            self.assertEqual(item["prepare_capabilities"], ["spark_batch"])

    def test_invalid_generation_sizes(self):
        spec = config.load("tests/fixtures/experiments/smoke-sql.yaml")
        for field, value in (("pages", 1), ("pages", 2**63), ("uservisits", 0), ("uservisits", 2**63)):
            spec["workloads"][0]["parameters"] = {field:value}
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_weighted_vocabularies_preserved(self):
        reference = json.loads(Path("tests/fixtures/contracts/sql-original-reference.json").read_text())
        for name, expected in reference["vocabularies"].items():
            data = (Path("sparkbench/sql/src/main/resources/org/hibench/sql") / name).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected["sha256"])
            rows = data.decode("utf-8").splitlines()
            self.assertEqual(len(rows), expected["entries"])
            self.assertTrue(all(row.count(",") == (1 if name == "country_codes" else 0) for row in rows))
        fixture = Path("tests/fixtures/sql-original-tiny.json").read_bytes()
        self.assertEqual(hashlib.sha256(fixture).hexdigest(), reference["fixture_sha256"])
