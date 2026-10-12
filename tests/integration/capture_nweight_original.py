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

"""Isolated historical GraphX/Pregel oracle; never used by a maintained workload."""
import json
import pathlib
import sys
from pyspark.sql import SparkSession

root=pathlib.Path(sys.argv[1]);root.mkdir(parents=True,exist_ok=True)
fixtures={
 'chain':[(0,1,2.),(1,2,3.),(2,3,4.)],
 'cycle-self':[(0,0,.5),(0,1,2.),(1,2,3.),(2,0,4.)],
 'duplicates-ties':[(0,1,2.),(0,1,5.),(0,2,2.),(1,3,3.),(1,4,3.),(2,3,3.),(2,4,3.)],
 'signed-weights':[(0,1,-2.),(0,2,1.),(1,3,3.),(2,3,-4.),(3,0,.5)]}
spark=SparkSession.builder.appName('Historical NWeight reference capture').getOrCreate()
try:
    checks=[]
    for name,edges in fixtures.items():
        path=root/(name+'.txt')
        sources=sorted({src for src,_,_ in edges})
        path.write_text('\n'.join(str(src)+' '+','.join(str(dst)+':'+str(w) for s,dst,w in edges if s==src) for src in sources)+'\n')
        for model in ('graphx','pregel'):
            api=getattr(spark._jvm.org.hibench.sparkbench.graph.nweight,'GraphxNWeight' if model=='graphx' else 'PregelNWeight')
            for degree in (1,2,3):
                for topk in (1,3):
                    output=(root/(name+'-'+model+'-'+str(degree)+'-'+str(topk))).as_uri()
                    api.nweight(spark.sparkContext._jsc.sc(),path.as_uri(),output,degree,topk,2,spark._jvm.org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK())
                    rows=[]
                    for line in spark.read.text(output).collect():
                        fields=line.value.split();values=[]
                        for pair in fields[1:]:
                            dst,weight=pair.split(':');values.append([int(dst),float(weight)])
                        rows.append(dict(id=int(fields[0]),targets=sorted(values)))
                    checks.append(dict(fixture=name,model=model,degree=degree,topk=topk,edges=edges,rows=sorted(rows,key=lambda row:row['id'])))
    print('HIBENCH_NWEIGHT_ORIGINAL='+json.dumps(checks))
finally:spark.stop()
