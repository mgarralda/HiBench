import json
import hashlib
import unittest
from pathlib import Path
from hibench.catalog import workloads
from hibench import config

class SqlContractTests(unittest.TestCase):
    def test_original_scales_preserved(self):
        reference = json.loads(Path("docs/sql-scale-reference.json").read_text())
        catalog = {w["id"]:w for w in workloads()}
        for name, scales in reference.items():
            item = catalog["sql." + name]
            for scale, fields in scales.items():
                for field, expected in fields.items():
                    self.assertEqual(item["parameters"][field]["presets"][scale], expected)
            self.assertFalse(item["requires_classic"])
            self.assertEqual(item["prepare_capabilities"], ["spark_batch"])

    def test_invalid_generation_sizes(self):
        spec = config.load("configs/smoke-sql.yaml")
        for field, value in (("pages", 1), ("pages", 2**63), ("uservisits", 0), ("uservisits", 2**63)):
            spec["workloads"][0]["parameters"] = {field:value}
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_weighted_vocabularies_preserved(self):
        reference = json.loads(Path("docs/sql-original-reference.json").read_text())
        for name, expected in reference["vocabularies"].items():
            data = (Path("sparkbench/sql/src/main/resources/org/hibench/sql") / name).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected["sha256"])
            rows = data.decode("utf-8").splitlines()
            self.assertEqual(len(rows), expected["entries"])
            self.assertTrue(all(row.count(",") == (1 if name == "country_codes" else 0) for row in rows))
        fixture = Path("tests/fixtures/sql-original-tiny.json").read_bytes()
        self.assertEqual(hashlib.sha256(fixture).hexdigest(), reference["fixture_sha256"])
