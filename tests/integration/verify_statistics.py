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

"""Bounded generation profiles and independent NumPy numerical references."""
import json, sys
import numpy as np
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.ml.feature import PCA
from pyspark.ml.stat import Correlation, Summarizer
from pyspark.mllib.random import RandomRDDs  # Isolated historical test reference only.

spark=SparkSession.builder.appName('Statistics migration reference verification').getOrCreate()
try:
    base=spark._jvm.org.hibench.sparkbench.ml
    checks=[]
    for name in ('pca','correlation','summarizer'):
        gen=getattr(base, {'pca':'PCADataGenerator','correlation':'BinaryGaussianDataGenerator','summarizer':'SummarizerDataGenerator'}[name])
        def data(n,d,p,seed=42):
            args=[spark._jsparkSession,n,d,p,seed]
            if name=='correlation': args.append(3.0)
            return DataFrame(gen.dataset(*args),spark)
        for n,d,p in ((10,10,2),(103,9,7),(3,5,8)):
            frame=data(n,d,p);rows=frame.orderBy('id').collect()
            assert len(rows)==n and [r.id for r in rows]==list(range(n))
            assert all(len(r.features)==d for r in rows)
            if name!='summarizer':
                for row in rows:
                    rng=spark._jvm.java.util.Random(42+row.id)
                    y=rng.nextGaussian() if name=='pca' else row.id%2
                    expected=[rng.nextGaussian()+(-.5 if name=='pca' else y*3) for _ in range(d)]
                    assert row.label==y and list(row.features)==expected
                assert {r.id:tuple(r.features) for r in rows}=={r.id:tuple(r.features) for r in data(n,d,1).collect()}
            else:
                assert [tuple(r.features) for r in rows]==[tuple(r.features) for r in data(n,d,p).orderBy('id').collect()]
                for partition in range(p):
                    rng=spark._jvm.java.util.Random(42 ^ partition)
                    for row in rows[n*partition//p:n*(partition+1)//p]:
                        assert list(row.features)==[rng.nextGaussian() for _ in range(d)]
            path=sys.argv[1]+'/'+name
            frame.write.mode('overwrite').parquet(path)
            assert [tuple(r.features) for r in rows]==[tuple(r.features) for r in spark.read.parquet(path).orderBy('id').collect()]
            checks.append(dict(workload=name,rows=n,dimensions=d,partitions=p,profile_and_roundtrip=True))
        frame=data(401,6,3); x=np.array([list(r.features) for r in frame.orderBy('id').collect()])
        if name=='pca':
            model=PCA(k=3,inputCol='features',outputCol='projection').fit(frame)
            eigenvalues,eigenvectors=np.linalg.eigh(np.cov(x,rowvar=False))
            order=np.argsort(eigenvalues)[::-1]; expected=eigenvectors[:,order[:3]]
            actual=model.pc.toArray()
            assert np.allclose(actual@actual.T,expected@expected.T,atol=1e-9)
            assert np.allclose(model.explainedVariance.toArray(),eigenvalues[order[:3]]/sum(eigenvalues),atol=1e-9)
        elif name=='correlation':
            assert np.allclose(Correlation.corr(frame,'features','pearson').head()[0].toArray(),np.corrcoef(x,rowvar=False),atol=1e-10)
            ranks=np.empty_like(x)
            for j in range(x.shape[1]):ranks[np.argsort(x[:,j]),j]=np.arange(len(x))
            assert np.allclose(Correlation.corr(frame,'features','spearman').head()[0].toArray(),np.corrcoef(ranks,rowvar=False),atol=1e-10)
        else:
            metrics=('count','mean','variance','max','min','numNonZeros','normL1','normL2')
            result=frame.select(Summarizer.metrics(*metrics).summary(F.col('features')).alias('s')).select('s.*').head()
            assert result['count']==len(x)
            references={'mean':x.mean(0),'variance':x.var(0,ddof=1),'max':x.max(0),'min':x.min(0),'numNonZeros':(x!=0).sum(0),'normL1':abs(x).sum(0),'normL2':np.sqrt((x*x).sum(0))}
            for key,value in references.items():assert np.allclose(result[key].toArray(),value,atol=1e-9)
            modern=np.array([list(r.features) for r in data(20001,8,3).collect()])
            historical=np.array(RandomRDDs.normalVectorRDD(spark.sparkContext,20001,8,numPartitions=3,seed=42).collect())
            profiles=[]
            for label,values in (('historical_fixed_test_seed',historical),('modern',modern)):
                means=values.mean(0);variances=values.var(0);cov=np.cov(values,rowvar=False);np.fill_diagonal(cov,0)
                tail=float((abs(values)>2).mean())
                assert max(abs(means))<.04 and max(abs(variances-1))<.05 and abs(tail-.0455)<.003 and max(abs(cov).flatten())<.04
                profiles.append(dict(source=label,means=means.tolist(),variances=variances.tolist(),tail_gt_2=tail,max_off_diagonal_covariance=float(max(abs(cov).flatten()))))
            checks.append(dict(workload=name,normal_profiles=profiles))
        checks.append(dict(workload=name,numerical_reference='numpy',rows=401,dimensions=6,passed=True))
    print('HIBENCH_STATISTICS_VERIFIED='+json.dumps(checks))
finally:spark.stop()
