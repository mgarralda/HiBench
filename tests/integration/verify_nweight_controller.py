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

"""Small actual generated graphs: multiplicity, symmetry, weights and node sampling."""
import collections,hashlib,json,sys,math
from decimal import Decimal,ROUND_HALF_UP
import numpy as np
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('NWeight generator profile acceptance').getOrCreate()
try:
    model=open(sys.argv[2],'rb').read()
    features=np.frombuffer(model,dtype='>f4').astype(float)
    assert len(features)==2401080
    checks=[]
    for record in json.load(open(sys.argv[1])):
        assert record['pairs']<=100000,'Reference collection budget exceeded'
        frame=spark.read.parquet(record['path'])
        profile=spark.read.parquet(record['path']+'/_generator_profile').head().asDict()
        assert profile['pairs']==record['pairs'] and profile['directedEdges']==2*record['pairs']
        assert profile['seed']==record['seed'] and profile['partitions']==record['partitions']
        assert profile['modelSha256']==hashlib.sha256(model).hexdigest()
        edges=[(row.src,row.dst,row.weight) for row in frame.collect()]
        assert len(edges)==record['pairs']*2
        counts=collections.Counter(edges)
        assert counts==collections.Counter((dst,src,w) for src,dst,w in edges)
        for src,dst,w in edges:
            assert 0<=src<len(features) and 0<=dst<len(features) and src!=dst
            expected=float(Decimal(str(float(features[src]*features[dst]))).quantize(Decimal('0.0001'),rounding=ROUND_HALF_UP))
            assert abs(w-expected)<1e-12,(src,dst,w,expected)
        histogram=np.histogram([src for src,_,_ in edges],bins=16,range=(0,len(features)))[0]
        nodes={src for src,_,_ in edges}
        if record['pairs']>=100000:
            expected=len(edges)/16
            assert max(abs(histogram-expected))<6*np.sqrt(expected),histogram.tolist()
            expected_nodes=len(features)*(-np.expm1(-2*record['pairs']/len(features)))
            observed_nodes=len(nodes)
            assert abs(observed_nodes-expected_nodes)<6*np.sqrt(2*record['pairs']),(observed_nodes,expected_nodes)
        if 'output' in record:
            state={}
            for src,dst,w in edges:state.setdefault(src,{})[dst]=state.get(src,{}).get(dst,0)+w
            def prune(mapping):
                return {src:dict(sorted(targets.items(),key=lambda pair:(pair[1],pair[0]),reverse=True)[:record['topk']]) for src,targets in mapping.items()}
            state=prune(state)
            for _ in range(1,record['degree']):
                next_state={}
                for src,middle,w in edges:
                    for dst,current in state.get(middle,{}).items():
                        if src!=dst:
                            targets=next_state.setdefault(src,{})
                            targets[dst]=targets.get(dst,0)+w*current
                state=prune(next_state)
            observed=spark.read.parquet(record['output']).collect()
            assert len(observed)==len(nodes) and {row.id for row in observed}==nodes
            for row in observed:
                targets={entry.dst:entry.weight for entry in row.targets}
                expected=state.get(row.id,{})
                assert targets.keys()==expected.keys(),row.id
                assert all(math.isclose(targets[dst],w,rel_tol=1e-9,abs_tol=1e-12) for dst,w in expected.items())
        checks.append(dict(record,rows=len(edges),vertices=len(nodes),distinct_directed_edges=len(counts),zero_edges=sum(w==0 for _,_,w in edges),
                           symmetry=True,original_feature_products=True,recurrence_output=('output' in record),node_histogram=histogram.tolist()))
    print('HIBENCH_NWEIGHT_CONTROLLER_VERIFIED='+json.dumps(checks))
finally:spark.stop()
