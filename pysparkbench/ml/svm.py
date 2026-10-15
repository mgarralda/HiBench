# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""linear-svc-v1 with exact ROC evaluation and both classes in each split."""
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pysparkbench.common import arguments, require, finite, boolean, storage_level


def parse(args):
    return arguments(args, [('input', str)], [('numIterations', int, 100), ('regParam', float, .01),
        ('tolerance', float, 1e-6), ('storageLevel', str, 'MEMORY_ONLY'), ('trainFraction', float, .6),
        ('splitSeed', int, 11), ('fitIntercept', boolean, False), ('standardization', boolean, False)])


def run(spark, p):
    require(p.numIterations > 0 and p.regParam >= 0 and p.tolerance > 0, 'Invalid SVM optimizer settings')
    finite(p.regParam, 'Invalid regularization'); finite(p.tolerance, 'Invalid tolerance')
    require(0 < p.trainFraction < 1, 'Invalid training fraction')
    training, test = spark.read.parquet(p.input).select('label', 'features').randomSplit([p.trainFraction, 1-p.trainFraction], p.splitSeed)
    training.persist(storage_level(p.storageLevel))
    try:
        ntrain, ntest = training.count(), test.count()
        require(ntrain > 0 and ntest > 0 and training.select('label').distinct().count() == 2
                and test.select('label').distinct().count() == 2, 'SVM training and test sets must contain both binary classes')
        model = LinearSVC(maxIter=p.numIterations, regParam=p.regParam, tol=p.tolerance,
                          fitIntercept=p.fitIntercept, standardization=p.standardization).fit(training)
        auc = finite(BinaryClassificationEvaluator(metricName='areaUnderROC', numBins=0).evaluate(model.transform(test)), 'AUC must be finite')
        print(f'SVM algorithm=linear-svc-v1 numTraining={ntrain} numTest={ntest} splitSeed={p.splitSeed}\nArea under ROC = {auc}')
        return {'training': ntrain, 'test': ntest, 'auc': auc}
    finally:
        training.unpersist()
