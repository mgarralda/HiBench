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
from hibench.adapters import create_adapter

class MLContractTests(unittest.TestCase):
    def test_all_original_preset_fields(self):
        reference=json.loads(Path('tests/fixtures/contracts/ml-final-scale-reference.json').read_text(encoding='utf-8'))
        for family in ('bayes','svm','lda'):
            item=next(w for w in workloads() if w['id']=='ml.'+family)
            self.assertEqual(item['validation_status'],'verified')
            self.assertFalse(item['requires_classic'])
            self.assertEqual(item['prepare_capabilities'],['spark_batch'])
            for scale,fields in reference[family].items():
                for key,value in fields.items():
                    self.assertEqual(item['parameters'][key]['presets'][scale],value)
    def test_valid_smoke_variants(self):
        for name in ('bayes','bayes-dense','bayes-overrides','svm','svm-overrides','lda','lda-em'):
            config.validate(config.load('tests/fixtures/experiments/smoke-'+name+'.yaml'))
    def test_invalid_effective_parameters(self):
        cases={'bayes':({'classes':1},{'document_seed':0},{'pages':24443},{'pages':2**63-1},{'train_fraction':1},{'smoothing':float('inf')}),
               'svm':({'features':0},{'seed':2**63-1},{'tolerance':0},{'train_fraction':0},{'weight_seed':2**63}),
               'lda':({'doc_len_min':20,'doc_len_max':10},{'num_of_topics':1},{'optimizer':'unsupported'},{'num_of_documents':0})}
        for family,params_list in cases.items():
            for params in params_list:
                spec=config.load('tests/fixtures/experiments/smoke-'+family+'.yaml');spec['workloads'][0]['parameters']=params
                with self.assertRaises(ValueError):config.validate(spec)
    def test_retired_generators_and_no_legacy_api(self):
        for path in ('autogen/src/main/scala/HiBench/BayesDataGen.scala','autogen/src/main/java/HiBench/BayesData.java','sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml/SVMWithSGDExample.scala'):
            self.assertFalse(Path(path).exists())
        root=Path('sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml')
        for name in ('BayesDataGenerator','NaiveBayesExample','SVMDataGenerator','LinearSVMExample','LDADataGenerator','LDAExample'):
            source=(root/(name+'.scala')).read_text(encoding='utf-8')
            for forbidden in ('org.apache.spark.rdd','org.apache.spark.mllib','sparkContext','objectFile','saveAsObjectFile'):
                self.assertNotIn(forbidden,source)
    def test_dictionary_byte_identity_and_eof_profile(self):
        new=Path('sparkbench/common/src/main/resources/hibench-words').read_bytes()
        import hashlib
        self.assertEqual(hashlib.sha256(new).hexdigest(), '1184684f1fb4eb5d4772376dfb559e318956a45cdd2b7acd201941463777e1a5')
        self.assertEqual(len(new.decode('utf-8').splitlines()),99171)
        self.assertIn('DictionaryPositions = 479623',Path('sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml/BayesDataGenerator.scala').read_text(encoding='utf-8'))
    def test_svd_has_no_legacy_fallback(self):
        item=next(w for w in workloads() if w['id']=='ml.svd')
        self.assertTrue(item['execution_available'])
        spec=config.load('tests/fixtures/experiments/smoke-svm.yaml');spec['workloads']=[{'id':'ml.svd','parameters':{}}]
        adapter=create_adapter(spec['target'])
        adapter.validate(spec)
        for phase,file in (('prepare','prepare.sh'),('spark','run.sh')):
            source=Path('bin/workloads/ml/svd/'+phase+'/'+file).read_text(encoding='utf-8')
            self.assertIn('run_spark_job',source)
            self.assertNotIn('objectFile',source)

    def test_pagerank_modern_boundary_verified(self):
        spec=config.load('tests/fixtures/experiments/smoke-pagerank.yaml');config.validate(spec)
        item=next(w for w in workloads() if w['id']=='websearch.pagerank')
        self.assertEqual(item['validation_status'],'verified')
        self.assertFalse(item['requires_classic'])
        self.assertEqual(item['prepare_capabilities'],['spark_batch'])
        root=Path('sparkbench/websearch/src/main/scala/org/hibench/sparkbench/websearch')
        for name in ('PageRankDataGenerator.scala','DataFramePageRank.scala'):
            source=(root/name).read_text(encoding='utf-8')
            for forbidden in ('org.apache.spark.rdd','sparkContext','SparkContext','org.apache.spark.mllib'):
                self.assertNotIn(forbidden,source)
    def test_pagerank_budget_and_seeds(self):
        for params in ({'pages':1},{'seed':0},{'pages':2**63-1}):
            spec=config.load('tests/fixtures/experiments/smoke-pagerank.yaml');spec['workloads'][0]['parameters']=params
            with self.assertRaises(ValueError):config.validate(spec)
