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


class LogisticContractTests(unittest.TestCase):
    def test_capabilities_and_historical_payload_accounting(self):
        reference = json.loads(Path('tests/fixtures/contracts/ml-lr-scale-reference.json').read_text())
        item = next(w for w in workloads() if w['id'] == 'ml.lr')
        self.assertFalse(item['requires_classic'])
        self.assertEqual(item['prepare_capabilities'], ['spark_batch'])
        for scale, fields in reference.items():
            self.assertEqual(fields['feature_payload_bytes'], fields['examples'] * fields['features'] * 8)

    def test_invalid_settings(self):
        for params in ({'examples': 1}, {'features': 0}, {'features': 2**31},
                       {'seed': 2**63-1}, {'num_iterations': 0}, {'class_shift': -1},
                       {'train_fraction': 0}, {'train_fraction': 1}, {'threshold': 1.01},
                       {'tolerance': 0}, {'split_seed': 2**63}):
            spec = config.load('tests/fixtures/experiments/smoke-lr.yaml')
            spec['workloads'][0]['parameters'] = params
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_modern_optimizer_and_long_generation_parameters(self):
        spec = config.load('tests/fixtures/experiments/smoke-lr.yaml')
        spec['workloads'][0]['parameters'] = {
            'examples': 3_000_000_000, 'class_shift': 0.0, 'seed': 0,
            'split_seed': 0, 'fit_intercept': True, 'tolerance': 1e-8}
        config.validate(spec)
