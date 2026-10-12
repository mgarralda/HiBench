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

"""Prepared acceptance checks. Requires rebuilt current JAR; not yet run on the target cluster."""
import json, math, sys
import numpy as np
from collections import Counter
from pyspark.sql import SparkSession,DataFrame,functions as F
from pyspark.ml.classification import NaiveBayes,LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.clustering import LDA,LocalLDAModel,DistributedLDAModel
spark=SparkSession.builder.appName('Final ML migration acceptance').getOrCreate()
try:
    base=spark._jvm.org.hibench.sparkbench.ml
    checks=[]
    for n,d,p in ((100,8,3),(103,9,7),(3,5,8)):
        dense=DataFrame(base.BayesDataGenerator.dense(spark._jsparkSession,n,d,10,p,1234,3.0),spark)
        svm=DataFrame(base.SVMDataGenerator.dataset(spark._jsparkSession,n,d,p,42,94720,.1),spark)
        rng=spark._jvm.java.util.Random(94720);weights=[rng.nextGaussian() for _ in range(d)]
        for row in dense.collect():
            rng=spark._jvm.java.util.Random(1234+row.id);label=float(rng.nextInt(10))
            assert row.origin_label==label and list(row.features)==[abs(rng.nextGaussian()+label*3) for _ in range(d)]
        for row in svm.collect():
            rng=spark._jvm.java.util.Random(42+row.id);x=[rng.nextDouble()*2-1 for _ in range(d)]
            score=sum(a*b for a,b in zip(weights,x))+rng.nextGaussian()*.1
            assert list(row.features)==x and row.label==(0. if score<0 else 1.)
        lda=DataFrame(base.LDADataGenerator.dataset(spark._jsparkSession,n,11,5,20,p,42),spark)
        rows={r.id:r for r in lda.collect()}
        for partition in range(p):
            rng=spark._jvm.java.util.Random(42 ^ partition)
            for i in range(n*partition//p,n*(partition+1)//p):
                length=rng.nextInt(16)+5;counts=Counter(rng.nextInt(11) for _ in range(length))
                assert list(rows[i].features)==[float(counts.get(j,0)) for j in range(11)]
        assert len(rows)==n
        checks.append(dict(rows=n,features=d,partitions=p,bayes_svm_lda_original_formulas=True))
    # Document mode: historical HtmlCore seeds, Zipf word positions and EOF dictionary behavior.
    dictionary=open(sys.argv[2],encoding='utf-8').read().splitlines()[:479623]
    dictionary += ['null']*(479623-len(dictionary))
    n,classes,p=25000,3,3
    sample_per_partition=80
    slot_size=(n-1)//p+1
    docs=DataFrame(base.BayesDataGenerator.documents(spark._jsparkSession,n,classes,p,1),spark).where(f"id % {slot_size} < {sample_per_partition}").cache()
    actual={r.id:r for r in docs.collect()}
    slot_size=(n-1)//p+1
    for partition in range(p):
        slot=partition+1;rng=spark._jvm.java.util.Random(slot)
        rng.nextLong();rng.nextLong();kernel=spark._jvm.HiBench.BayesOriginalKernel.wordKernel(n);kernel.setRandSeed(rng.nextLong())
        length_rng=spark._jvm.java.util.Random(rng.nextLong());labels=spark._jvm.java.util.Random(slot*1000+101)
        for i in range(slot_size*partition,min(n,slot_size*partition+sample_per_partition)):
            label=labels.nextInt(classes);g=length_rng.nextGaussian()
            while g < -10 or g > (32767-800)/80:g=length_rng.nextGaussian()
            length=math.ceil(math.floor(800+80*g+.5)*500/800)
            tokens=[dictionary[kernel.next()] for _ in range(length)] if length else ['']
            assert actual[i].origin_label==label and list(actual[i].tokens)==tokens
    weighted=DataFrame(base.BayesDataGenerator.weighted(spark._jsparkSession,docs._jdf),spark).collect()
    counts=Counter(t for row in actual.values() for t in row.tokens);total=sum(counts.values())
    vocabulary=sorted(counts);indices={t:i for i,t in enumerate(vocabulary)}
    assert len(weighted)==len(actual)
    for row in weighted:
        frequencies=Counter(actual[row.id].tokens)
        expected={indices[t]:v*counts[t]/total for t,v in frequencies.items()}
        assert row.features.size==len(vocabulary)
        assert set(row.features.indices)==set(expected)
        assert all(math.isclose(row.features[i],v,abs_tol=1e-10) for i,v in expected.items())
    checks.append(dict(bayes_document_original_tokens_and_weights=True,rows=len(actual),historical_generator_pages=n,vocabulary=len(vocabulary),tokens=total))
    # Small positive multinomial matrix: independent add-one parameter reference.
    fixture=DataFrame(base.BayesDataGenerator.dense(spark._jsparkSession,101,8,3,3,17,3.0),spark)
    indexed=DataFrame(base.BayesDataGenerator.indexed(fixture._jdf),spark)
    model=NaiveBayes(smoothing=1.0,modelType='multinomial').fit(indexed)
    rows=indexed.collect();n=len(rows);k=3
    pi=[];theta=[]
    for label in range(k):
        values=np.array([list(r.features) for r in rows if r.label==label])
        sums=values.sum(0);pi.append(math.log((len(values)+1)/(n+k)))
        theta.append(np.log((sums+1)/(sums.sum()+8)))
    assert np.allclose(model.pi.toArray(),pi,atol=1e-10)
    assert np.allclose(model.theta.toArray(),theta,atol=1e-10)
    checks.append(dict(bayes_independent_multinomial_parameters=True))
    frame=DataFrame(base.SVMDataGenerator.dataset(spark._jsparkSession,1000,8,3,17,23,0.),spark)
    train,test=frame.where('id < 700'),frame.where('id >= 700')
    model=LinearSVC(maxIter=100,regParam=.01,fitIntercept=False,standardization=False,tol=1e-6).fit(train)
    predictions=model.transform(test).select('label','rawPrediction').collect()
    scores=[(r.rawPrediction[1],r.label) for r in predictions]
    positives=[s for s,y in scores if y==1];negatives=[s for s,y in scores if y==0]
    auc=sum((1 if a>b else .5 if a==b else 0) for a in positives for b in negatives)/(len(positives)*len(negatives))
    actual=BinaryClassificationEvaluator(numBins=0).evaluate(model.transform(test))
    assert math.isclose(auc,actual,abs_tol=1e-10) and auc>.9
    checks.append(dict(svm_auc_reference=True,auc=auc))
    corpus=DataFrame(base.LDADataGenerator.dataset(spark._jsparkSession,101,20,5,20,3,17),spark)
    for optimizer in ('online','em'):
        model=LDA(k=3,maxIter=3,seed=7,optimizer=optimizer).fit(corpus)
        path=sys.argv[1]+'/'+optimizer;model.write().overwrite().save(path)
        restored=(DistributedLDAModel if optimizer=='em' else LocalLDAModel).load(path)
        assert np.allclose(model.topicsMatrix().toArray(),restored.topicsMatrix().toArray(),atol=1e-12)
        assert restored.transform(corpus).count()==101
        checks.append(dict(lda_optimizer=optimizer,save_load_topics=True))
    print('HIBENCH_FINAL_ML_VERIFIED='+json.dumps(checks))
finally:spark.stop()
