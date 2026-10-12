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

"""Independent direct-SVD numerical acceptance; no input RDD APIs."""
import json
import numpy as np
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

spark=SparkSession.builder.appName('Uncentered SVD reference acceptance').getOrCreate()
try:
    api=spark._jvm.org.hibench.sparkbench.ml.UncenteredSVD
    rng=np.random.default_rng(19)
    cases={'tall':rng.normal(size=(31,7)), 'wide':rng.normal(size=(7,31)),
           'offset':rng.normal(size=(23,6))+7,
           'rank-deficient':np.array([[1,2,3],[2,4,6],[-1,-2,-3],[0,0,0]],dtype=float),
           'repeated':np.eye(5)*3, 'zero':np.zeros((4,3)),
           'small-tail':np.diag([3,1,1e-10])}
    checks=[]
    for name,a in cases.items():
        frame=spark.createDataFrame([(i,Vectors.dense(row)) for i,row in enumerate(a)],['id','features']).repartition(3)
        result=api.decompose(frame._jdf,a.shape[1],a.shape[1],1e-9,256*1024*1024)
        singular=np.array(list(result.singularValues()))
        columns=list(result.rightVectors())
        v=np.array([list(col) for col in columns]).T if len(columns) else np.zeros((a.shape[1],0))
        exact=np.linalg.svd(a,compute_uv=False)
        floor=max(1e-9,np.sqrt(a.shape[1]*np.finfo(float).eps))
        expected=exact[exact>exact[0]*floor] if exact[0]>0 else exact[:0]
        assert len(singular)==len(expected),(name,singular,expected)
        assert np.allclose(singular,expected,rtol=1e-7,atol=1e-8),(name,singular,expected)
        assert np.allclose(v.T@v,np.eye(len(singular)),atol=1e-8)
        from pyspark.sql import DataFrame
        uframe=DataFrame(api.leftVectors(frame._jdf,result),spark)
        ordered=sorted(uframe.collect(),key=lambda row:row.id)
        u=np.array([row.features.toArray() for row in ordered])
        assert np.allclose(u.T@u,np.eye(len(singular)),atol=1e-7)
        residual=float(np.linalg.norm(a-(u*singular)@v.T)/max(1,np.linalg.norm(a)))
        assert residual<1e-7,(name,residual)
        checks.append(dict(fixture=name,shape=list(a.shape),rank=len(singular),residual=residual))
    # Truncated output is assessed against the best rank-k residual, not full reconstruction.
    a=cases['tall'];frame=spark.createDataFrame([(i,Vectors.dense(row)) for i,row in enumerate(a)],['id','features'])
    result=api.decompose(frame._jdf,a.shape[1],3,1e-9,256*1024*1024)
    values=np.array(list(result.singularValues()));v=np.array([list(col) for col in result.rightVectors()]).T
    exact=np.linalg.svd(a,compute_uv=False)
    assert np.allclose(values,exact[:3],rtol=1e-7)
    assert np.isclose(np.linalg.norm(a-a@v@v.T),np.linalg.norm(exact[3:]),rtol=1e-7)
    checks.append(dict(fixture='truncated',components=3,best_rank_residual=True))
    for budget in (1,0):
        try:api.decompose(frame._jdf,a.shape[1],3,1e-9,budget)
        except Exception:pass
        else:raise AssertionError('Missing matrix budget guard')
    print('HIBENCH_SVD_VERIFIED='+json.dumps(checks))
finally:spark.stop()
