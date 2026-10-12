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


class TreeContractTests(unittest.TestCase):
    def test_original_sizes_and_modern_capabilities(self):
        reference = json.loads(Path('tests/fixtures/contracts/ml-trees-scale-reference.json').read_text())
        for family in ('rf', 'gbt'):
            item = next(w for w in workloads() if w['id'] == 'ml.' + family)
            self.assertFalse(item['requires_classic'])
            self.assertEqual(item['prepare_capabilities'], ['spark_batch'])
            for scale, fields in reference[family].items():
                for key in ('examples', 'features'):
                    self.assertEqual(int(item['parameters'][key]['presets'][scale]), fields[key])
                self.assertEqual(fields['feature_payload_bytes'], fields['examples'] * fields['features'] * 8)
            self.assertEqual(float(item['parameters']['class_shift']['default']), .3)

    def test_invalid_settings(self):
        cases = ({'examples': 1}, {'features': 0}, {'maxDepth': 31}, {'maxBins': 1},
                 {'seed': 2**63-1}, {'train_fraction': 0}, {'train_fraction': 1},
                 {'class_shift': -1}, {'numClasses': 1}, {'model_seed': 2**63})
        for family in ('rf', 'gbt'):
            extra = ({'numTrees': 0}, {'impurity': 'variance'}, {'featureSubsetStrategy': 'invalid'}) if family == 'rf' else (
                {'numClasses': 3}, {'numIterations': 0}, {'learningRate': 0}, {'learningRate': 1.1})
            for params in cases + extra:
                spec = config.load('tests/fixtures/experiments/smoke-trees.yaml')
                spec['workloads'] = [{'id': 'ml.'+family, 'parameters': params}]
                with self.assertRaises(ValueError):
                    config.validate(spec)

    def test_valid_zero_depth_and_custom_parameters(self):
        spec = config.load('tests/fixtures/experiments/smoke-trees-overrides.yaml')
        config.validate(spec)
        spec['workloads'][0]['parameters'].update({'maxDepth': 0, 'class_shift': 0,
                                                  'examples': 3_000_000_000, 'seed': 0})
        config.validate(spec)
