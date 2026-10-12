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

class StatisticsContractTests(unittest.TestCase):
    def test_presets_and_capabilities(self):
        references=json.loads(Path('tests/fixtures/contracts/ml-statistics-scale-reference.json').read_text())
        for family,scales in references.items():
            item=next(w for w in workloads() if w['id']=='ml.'+family)
            self.assertFalse(item['requires_classic'])
            for scale,fields in scales.items():
                for key in ('examples','features'):
                    self.assertEqual(int(item['parameters'][key]['presets'][scale]),fields[key])
                self.assertEqual(fields['feature_payload_bytes'],fields['examples']*fields['features']*8)
    def test_invalid_settings(self):
        for family in ('pca','correlation','summarizer'):
            for params in ({'examples':1},{'features':0},{'seed':2**63}):
                spec=config.load('tests/fixtures/experiments/smoke-statistics.yaml')
                spec['workloads']=[{'id':'ml.'+family,'parameters':params}]
                with self.assertRaises(ValueError):config.validate(spec)
        for family,params in (('pca',{'features':2,'k':3}),('correlation',{'corrType':'invalid'})):
            spec=config.load('tests/fixtures/experiments/smoke-statistics.yaml');spec['workloads']=[{'id':'ml.'+family,'parameters':params}]
            with self.assertRaises(ValueError):config.validate(spec)
    def test_no_legacy_production_boundary(self):
        root=Path('sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml')
        for name in ('PCA','Correlation','Summarizer'):
            for suffix in ('Example','DataGenerator'):
                text=(root/(name+suffix+'.scala')).read_text()
                for forbidden in ('org.apache.spark.rdd','org.apache.spark.mllib','sparkContext','objectFile','saveAsObjectFile'):
                    self.assertNotIn(forbidden,text)
