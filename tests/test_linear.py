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
from pathlib import Path
import unittest
from hibench import config
from hibench.catalog import workloads


class LinearContractTests(unittest.TestCase):
    def test_capabilities_and_historical_payload_accounting(self):
        reference = json.loads(Path('tests/fixtures/contracts/ml-linear-scale-reference.json').read_text())
        item = next(w for w in workloads() if w['id'] == 'ml.linear')
        self.assertFalse(item['requires_classic'])
        self.assertEqual(item['prepare_capabilities'], ['spark_batch'])
        for scale, fields in reference.items():
            self.assertEqual(fields['feature_payload_bytes'], fields['examples'] * fields['features'] * 8)

    def test_invalid_contracts(self):
        for params in ({'examples': 0}, {'examples': 2**63}, {'features': 0},
                       {'features': 2**31}, {'seed': 2**63}, {'noise_std': -1},
                       {'elasticnet_param': 1.01}, {'num_iterations': 0}, {'tolerance': 0},
                       {'test_fraction': 1}):
            spec = config.load('tests/fixtures/experiments/smoke-linear.yaml')
            spec['workloads'][0]['parameters'] = params
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_valid_zero_noise_and_long_count(self):
        spec = config.load('tests/fixtures/experiments/smoke-linear.yaml')
        spec['workloads'][0]['parameters'] = {'noise_std': 0, 'seed': 0, 'examples': 3_000_000_000,
                                             'tolerance': 1e-7, 'test_fraction': 0}
        config.validate(spec)


if __name__ == '__main__':
    unittest.main()
