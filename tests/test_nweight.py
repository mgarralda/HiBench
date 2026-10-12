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

import json,struct
from pathlib import Path
import unittest
from unittest.mock import patch
from hibench import config
from hibench.catalog import workloads

class NWeightContractTests(unittest.TestCase):
    def test_original_pair_sizes(self):
        item=next(w for w in workloads() if w['id']=='graph.nweight')
        for scale,count in zip(('tiny','small','large','huge','gigantic','bigdata'),(100000,1000000,10000000,100000000,425000000,4250000000)):
            self.assertEqual(int(item['parameters']['edges']['presets'][scale]),count)
    def test_no_production_legacy_variants(self):
        root=Path('sparkbench/graph/src/main/scala/org/hibench/sparkbench/graph/nweight')
        for name in ('Driver.scala','GraphxNWeight.scala','PregelNWeight.scala','Utils.scala','NWeightDataGenerator.scala'):
            self.assertFalse((root/name).exists())
        for name in ('DataFrameNWeight.scala','UniformWeightedGraphGenerator.scala'):
            source=(root/name).read_text(encoding='utf-8')
            for old in ('org.apache.spark.rdd','org.apache.spark.mllib','sparkContext','SparkContext','GraphImpl'):
                self.assertNotIn(old,source)
    def test_frozen_original_results(self):
        cases=json.loads(Path('tests/fixtures/nweight-original-profile.json').read_text())
        self.assertEqual(len(cases),48)
        self.assertEqual({c['model'] for c in cases},{'graphx','pregel'})
        chain={c['degree']:c['rows'] for c in cases if c['fixture']=='chain' and c['model']=='graphx' and c['topk']==3}
        self.assertEqual(chain[2],chain[3])
    def test_model_size(self):
        data=Path('sparkbench/graph/src/main/resources/nweight-user-features').read_bytes()
        self.assertEqual(len(data),2401080*4)
        self.assertEqual(len(list(struct.iter_unpack('>f',data))),2401080)
    def test_model_is_installed_and_hash_checked(self):
        from hibench.backend import ClassicBackend,RUNTIME_RESOURCES
        model='sparkbench/graph/src/main/resources/nweight-user-features'
        self.assertIn(model,RUNTIME_RESOURCES)
        backend=ClassicBackend(config.load('tests/fixtures/experiments/smoke-nweight.yaml')['target'])
        with patch('hibench.backend.ARTIFACTS',()),patch('hibench.backend.subprocess.run') as run,patch.object(backend,'command',return_value='1000'):
            backend.install()
            copies=[call.args[0] for call in run.call_args_list if call.args[0][:2]==['docker','cp']]
            self.assertTrue(any(model in ' '.join(args).replace('\\','/') for args in copies))
        runtime='openjdk version "11.0.23"\nPython 3.11.9\nversion 3.5.9\nUsing Scala version 2.12.18\n'
        with patch('hibench.backend.ARTIFACTS',()),patch.object(backend,'command',return_value=runtime) as command:
            backend.doctor()
            self.assertIn(model,command.call_args.args[0][-1])
    def test_invalid_parameters(self):
        for parameters in ({'edges':0},{'edges':4250000001},{'degree':0},{'max_out_edges':0},{'seed':-1}):
            spec=config.load('tests/fixtures/experiments/smoke-nweight.yaml');spec['workloads'][0]['parameters']=parameters
            with self.assertRaises(ValueError):config.validate(spec)
    def test_big_profile_partition_budget(self):
        spec=config.load('tests/fixtures/experiments/smoke-nweight.yaml');spec['scale']='bigdata'
        with self.assertRaises(ValueError):config.validate(spec)
        spec['resources']['generation_shuffle_partitions']=4
        config.validate(spec)
