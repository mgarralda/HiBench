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
    def test_preserved_sizes(self):
        reference=json.loads(Path('tests/fixtures/contracts/ml-final-scale-reference.json').read_text(encoding='utf-8'))['svd']
        item=next(w for w in workloads() if w['id']=='ml.svd')
        for scale,fields in reference.items():
            for key,value in fields.items():
                expected=value.strip('"') if key=='maxresultsize' else value
                self.assertEqual(item['parameters'][key]['presets'][scale],expected)
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
