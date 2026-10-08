import json
from pathlib import Path
import unittest
from hibench import config
from hibench.catalog import workloads


class LinearContractTests(unittest.TestCase):
    def test_original_presets_and_payload(self):
        reference = json.loads(Path('docs/ml-linear-scale-reference.json').read_text())
        item = next(w for w in workloads() if w['id'] == 'ml.linear')
        self.assertFalse(item['requires_classic'])
        self.assertEqual(item['prepare_capabilities'], ['spark_batch'])
        for scale, fields in reference.items():
            for key in ('examples', 'features'):
                self.assertEqual(int(item['parameters'][key]['presets'][scale]), fields[key])
            self.assertEqual(fields['feature_payload_bytes'], fields['examples'] * fields['features'] * 8)

    def test_invalid_contracts(self):
        for params in ({'examples': 0}, {'examples': 2**63}, {'features': 0},
                       {'features': 2**31}, {'seed': 2**63}, {'noise_std': -1},
                       {'elasticnet_param': 1.01}, {'num_iterations': 0}, {'tolerance': 0},
                       {'test_fraction': 1}):
            spec = config.load('configs/smoke-linear.yaml')
            spec['workloads'][0]['parameters'] = params
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_valid_zero_noise_and_long_count(self):
        spec = config.load('configs/smoke-linear.yaml')
        spec['workloads'][0]['parameters'] = {'noise_std': 0, 'seed': 0, 'examples': 3_000_000_000,
                                             'tolerance': 1e-7, 'test_fraction': 0}
        config.validate(spec)


if __name__ == '__main__':
    unittest.main()
