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

class SVDContractTests(unittest.TestCase):
    def test_confirmation_campaign_balances_order_and_excludes_warmup(self):
        from tests.tools.svd_phase_campaign import plan
        base = config.default_experiment(scale='small', selected_workloads=['ml.svd'])
        jobs = plan(base, ['small', 'large'], 10)
        self.assertEqual(len(jobs), 46)
        for scale in ('small', 'large'):
            selected = [(role, pair, spec) for role, pair, spec in jobs if spec['scale'] == scale]
            self.assertEqual(sum(role == 'prepare' for role, _, _ in selected), 1)
            self.assertEqual(sum(role == 'warmup' for role, _, _ in selected), 2)
            measured = [spec['implementation'] for role, _, spec in selected if role == 'measurement']
            self.assertEqual(measured.count('scala'), 10)
            self.assertEqual(measured.count('pyspark'), 10)
            self.assertEqual(measured[:4], ['scala', 'pyspark', 'pyspark', 'scala'])
            for _, _, spec in selected:
                config.validate(spec)
                self.assertEqual(spec['target']['deploy_mode'], 'client')
                self.assertEqual(spec['repetitions'], 1)

    def test_requested_components_fit_recalibrated_shapes(self):
        item = next(w for w in workloads() if w['id'] == 'ml.svd')
        for scale in ('tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'):
            p = item['parameters']
            self.assertLessEqual(int(p['singularvalues']['presets'][scale]),
                                 min(int(p['features']['presets'][scale]), int(p['examples']['presets'][scale])))
    def test_spark_only_boundaries(self):
        item=next(w for w in workloads() if w['id']=='ml.svd')
        self.assertFalse(item['requires_classic'])
        self.assertEqual(item['prepare_capabilities'],['spark_batch'])
        for name in ('SVDExample','SVDDataGenerator','UncenteredSVD'):
            text=Path('sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml/'+name+'.scala').read_text(encoding='utf-8')
            for legacy in ('org.apache.spark.mllib','org.apache.spark.rdd','sparkContext','objectFile'):
                self.assertNotIn(legacy,text)
    def test_invalid_budgets_and_cutoffs(self):
        for parameters in ({'features':0},{'singularvalues':1001},{'rcond':0},{'rcond':float('inf')},{'matrix_budget_bytes':1},{'seed':-1}):
            spec=config.load('tests/fixtures/experiments/smoke-svd.yaml');spec['workloads'][0]['parameters']=parameters
            with self.assertRaises(ValueError):config.validate(spec)
    def test_smoke_specs(self):
        for name in ('svd','svd-overrides'):config.validate(config.load('tests/fixtures/experiments/smoke-'+name+'.yaml'))
