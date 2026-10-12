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

"""Independent weighted-path recurrence for the corrected versioned workload."""
import collections,json,math
from pyspark.sql import SparkSession,DataFrame

fixtures={
 'chain':[(0,1,2.),(1,2,3.),(2,3,4.)],
 'cycle-self':[(0,0,.5),(0,1,2.),(1,2,3.),(2,0,4.)],
 'duplicates-ties':[(0,1,2.),(0,1,5.),(0,2,2.),(1,3,3.),(1,4,3.),(2,3,3.),(2,4,3.)],
 'signed-weights':[(0,1,-2.),(0,2,1.),(1,3,3.),(2,3,-4.),(3,0,.5)]}
def expected(edges,degree,k):
    nodes=sorted({v for src,dst,_ in edges for v in (src,dst)})
    def top(values):
        result={}
        for src in nodes:
            targets=sorted([(dst,w) for (s,dst),w in values.items() if s==src],key=lambda item:(item[1],item[0]),reverse=True)[:k]
            result.update({(src,dst):w for dst,w in targets})
        return result
    initial=collections.defaultdict(float)
    for src,dst,w in edges:initial[src,dst]+=w
    state=top(initial)
    for _ in range(1,degree):
        values=collections.defaultdict(float)
        for src,middle,w in edges:
            for (s,dst),current in state.items():
                if s==middle and src!=dst:values[src,dst]+=w*current
        state=top(values)
    return {src:sorted([(dst,w) for (s,dst),w in state.items() if s==src]) for src in nodes}
spark=SparkSession.builder.appName('Corrected NWeight reference acceptance').getOrCreate()
try:
    checks=[]
    for name,edges in fixtures.items():
        frame=spark.createDataFrame(edges,['src','dst','weight']).repartition(2)
        for degree in (1,2,3):
            for k in (1,3):
                result=DataFrame(spark._jvm.org.hibench.sparkbench.graph.nweight.DataFrameNWeight.compute(frame._jdf,degree,k),spark)
                try:
                    observed={row.id:sorted([(t.dst,t.weight) for t in row.targets]) for row in result.collect()}
                    reference=expected(edges,degree,k)
                    assert observed.keys()==reference.keys()
                    for src,targets in reference.items():
                        assert len(observed[src])==len(targets),(name,degree,k,observed,reference)
                        for (dst,weight),(actual_dst,actual_weight) in zip(targets,observed[src]):
                            assert dst==actual_dst and math.isclose(weight,actual_weight,abs_tol=1e-12,rel_tol=1e-12)
                    checks.append(dict(fixture=name,degree=degree,topk=k,weighted_recurrence=True))
                finally:result.unpersist()
    print('HIBENCH_NWEIGHT_VERIFIED='+json.dumps(checks))
finally:spark.stop()
