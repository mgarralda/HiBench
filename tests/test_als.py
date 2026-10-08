import json
import unittest
from pathlib import Path
from hibench import config
from hibench.catalog import workloads


class AlsContractTests(unittest.TestCase):
    def test_presets_preserved(self):
        reference = json.loads(Path('docs/ml-als-scale-reference.json').read_text())
        als = next(w for w in workloads() if w['id'] == 'ml.als')
        self.assertFalse(als['requires_classic'])
        self.assertEqual(als['prepare_capabilities'], ['spark_batch'])
        for scale, values in reference.items():
            for key, value in values.items():
                self.assertEqual(als['parameters'][key]['presets'][scale], value)

    def test_invalid_counts(self):
        for params in ({'users': 0}, {'products': 2**31}, {'ratings': 2**63},
                       {'users': 2, 'products': 2, 'ratings': 5}):
            spec = config.load('configs/smoke-als.yaml')
            spec['workloads'][0]['parameters'] = params
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_ratings_allow_long_counts(self):
        spec = config.load('configs/smoke-als.yaml')
        spec['workloads'][0]['parameters'] = {'users': 100000, 'products': 100000, 'ratings': 3_000_000_000}
        config.validate(spec)
