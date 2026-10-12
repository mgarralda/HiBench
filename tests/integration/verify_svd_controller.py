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

"""Actual small controller datasets: shape, Gaussian profile and independent SVD.
Reference-only collection is capped at 200,000 scalar values; workload code never collects rows.
"""
import json,sys
import numpy as np
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('SVD controller numerical acceptance').getOrCreate()
try:
    checks=[]
    for record in json.load(open(sys.argv[1])):
        frame=spark.read.parquet(record['path'])
        profile=spark.read.parquet(record['path']+'/_generator_profile').head().asDict()
        for key in ('examples','dimensions','seed','partitions'):assert profile[key]==record[key]
        assert record['examples']*record['dimensions']<=200000,'Reference collection budget exceeded'
        rows=sorted(frame.collect(),key=lambda row:row.id)
        assert [row.id for row in rows]==list(range(record['examples']))
        a=np.array([row.features.toArray() for row in rows])
        assert a.shape==(record['examples'],record['dimensions'])
        assert np.isfinite(a).all()
        result=spark._jvm.org.hibench.sparkbench.ml.UncenteredSVD.decompose(frame._jdf,record['dimensions'],record['components'],1e-9,2147483648)
        values=np.array(list(result.singularValues()))
        exact=np.linalg.svd(a,compute_uv=False)[:record['components']]
        assert np.allclose(values,exact,rtol=1e-7,atol=1e-8)
        checks.append(dict(record,shape_verified=True,numerical_singular_values=True,mean=float(a.mean()),std=float(a.std()),retained=len(values),logical_feature_bytes=a.size*8))
    print('HIBENCH_SVD_CONTROLLER_VERIFIED='+json.dumps(checks))
finally:spark.stop()
