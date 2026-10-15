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

"""Binary logistic-v2, preserving train/test split and optimizer settings."""
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import functions as F
from pysparkbench.common import arguments, boolean, require, finite


def parse(args):
    return arguments(args, [('input', str), ('max_iter', int), ('tol', float), ('reg_param', float),
        ('fit_intercept', boolean), ('standardization', boolean), ('train_fraction', float), ('split_seed', int), ('threshold', float)])


def run(spark, p):
    require(p.max_iter > 0 and p.tol > 0 and p.reg_param >= 0, 'Invalid optimizer settings')
    require(0 < p.train_fraction < 1, 'Train fraction must be between zero and one')
    training, test = spark.read.parquet(p.input).select('label', 'features').randomSplit([p.train_fraction, 1-p.train_fraction], p.split_seed)
    training.cache(); test.cache()
    try:
        counts = training.groupBy('label').count().collect()
        require(len(counts) == 2 and all(row.label in (0., 1.) for row in counts), 'Training must contain both binary labels 0 and 1')
        ntrain = sum(row['count'] for row in counts)
        ntest = test.count()
        require(ntest > 0, 'Logistic evaluation split is empty')
        model = LogisticRegression(family='binomial', maxIter=p.max_iter, tol=p.tol, regParam=p.reg_param,
                                   elasticNetParam=0., fitIntercept=p.fit_intercept, standardization=p.standardization,
                                   threshold=p.threshold).fit(training)
        result = model.transform(test).agg(F.count(F.lit(1)).alias('n'),
            F.sum(F.when(F.col('prediction') == F.col('label'), 1).otherwise(0)).alias('correct')).first()
        accuracy = finite(result.correct / result.n, 'Non-finite held-out accuracy')
        print(f'Loaded data: numTraining = {ntrain}, numTest = {ntest}, numFeatures = {model.numFeatures}')
        print(f'numClasses = {model.numClasses}; numIterations = {model.summary.totalIterations}\nAccuracy = {accuracy}')
        return {'training': ntrain, 'test': ntest, 'accuracy': accuracy}
    finally:
        training.unpersist(); test.unpersist()
