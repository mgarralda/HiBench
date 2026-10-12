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

"""Original raw-edge multiset and fixed-iteration PageRank semantics; bounded fixtures only."""
import json,math,sys
from collections import Counter,defaultdict
from pyspark.sql import SparkSession,DataFrame
spark=SparkSession.builder.appName('PageRank migration reference acceptance').getOrCreate()
try:
    original=json.load(open(sys.argv[1],encoding='utf-8'))
    base=spark._jvm.org.hibench.sparkbench.websearch
    generated=DataFrame(base.PageRankDataGenerator.edges(spark._jsparkSession,50,2,1),spark)
    expected=Counter(tuple(x) for x in original['edges'])
    actual=Counter((r.src,r.dst) for r in generated.collect())
    assert actual==expected
    def reference(edges,iterations):
        adjacency=defaultdict(set)
        for src,dst in edges:adjacency[src].add(dst)
        ranks={src:1. for src in adjacency}
        for _ in range(iterations):
            sums=defaultdict(float)
            for src,neighbors in adjacency.items():
                if src in ranks:
                    for dst in neighbors:sums[dst]+=ranks[src]/len(neighbors)
            ranks={dst:.15+.85*value for dst,value in sums.items()}
        return ranks
    fixtures=[('original-tiny',list(expected)),('dangling-duplicate-self',[(0,1),(0,1),(0,2),(1,0),(1,1),(3,4)]),('cycle',[(0,1),(1,2),(2,0)])]
    checks=[]
    for name,edges in fixtures:
        frame=spark.createDataFrame(edges,['src','dst'])
        for iterations in (0,1,3):
            result=DataFrame(base.DataFramePageRank.ranks(frame._jdf,iterations),spark)
            values={r.id:r.rank for r in result.collect()};wanted=reference(edges,iterations)
            assert set(values)==set(wanted)
            assert all(math.isclose(values[k],v,abs_tol=1e-10) for k,v in wanted.items())
            checks.append(dict(fixture=name,iterations=iterations,reference_ranks=True,vertices=len(values)))
    checks.append(dict(exact_original_raw_edges=True,raw_edges=sum(actual.values()),distinct_edges=len(actual),self_edges=sum(v for (s,d),v in actual.items() if s==d)))
    print('HIBENCH_PAGERANK_VERIFIED='+json.dumps(checks))
finally:spark.stop()
