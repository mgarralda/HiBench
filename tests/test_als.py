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
import unittest
from pathlib import Path
from hibench import config
from hibench.catalog import workloads


class AlsContractTests(unittest.TestCase):
    def test_recalibrated_ratings_and_cardinalities(self):
        als = next(w for w in workloads() if w['id'] == 'ml.als')
        self.assertFalse(als['requires_classic'])
        self.assertEqual(als['prepare_capabilities'], ['spark_batch'])
        for scale in ('tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'):
            users = int(als['parameters']['users']['presets'][scale])
            self.assertEqual(int(als['parameters']['products']['presets'][scale]), users)
            self.assertEqual(int(als['parameters']['ratings']['presets'][scale]), 20 * users)

    def test_invalid_counts(self):
        for params in ({'users': 0}, {'products': 2**31}, {'ratings': 2**63},
                       {'users': 2, 'products': 2, 'ratings': 5}):
            spec = config.load('tests/fixtures/experiments/smoke-als.yaml')
            spec['workloads'][0]['parameters'] = params
            with self.assertRaises(ValueError):
                config.validate(spec)

    def test_ratings_allow_long_counts(self):
        spec = config.load('tests/fixtures/experiments/smoke-als.yaml')
        spec['workloads'][0]['parameters'] = {'users': 100000, 'products': 100000, 'ratings': 3_000_000_000}
        config.validate(spec)

    def test_generator_partitions_are_explicit_and_independent_of_sql_shuffle(self):
        source = Path('sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml/RatingDataGenerator.scala').read_text(encoding='utf-8')
        launcher = Path('bin/workloads/ml/als/prepare/prepare.sh').read_text(encoding='utf-8')
        self.assertIn('val partitions = args(5).toInt', source)
        self.assertNotIn('spark.sql.shuffle.partitions', source)
        self.assertNotIn('IOCommon', source)
        self.assertIn('$IMPLICITPREFS_ALS $NUM_REDS', launcher)
        self.assertIn('NUM_REDS="hibench.generator.shuffle.partitions"', Path('bin/functions/hibench_prop_env_mapping.py').read_text(encoding='utf-8'))
