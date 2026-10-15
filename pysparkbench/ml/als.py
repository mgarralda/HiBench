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

"""ALS with the same fixed model/split seeds and cold-start evaluation."""
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import types as T
from pysparkbench.common import arguments, boolean, require, finite


def parse(args):
    return arguments(args, [('input', str)], [('rank', int, 10), ('numIterations', int, 10),
        ('lambda', float, .1), ('numUserBlocks', int, 10), ('numProductBlocks', int, 10), ('implicitPrefs', boolean, False)])


def run(spark, p):
    regularization = getattr(p, 'lambda')
    require(p.rank > 0 and p.numIterations > 0 and regularization >= 0 and p.numUserBlocks > 0 and p.numProductBlocks > 0,
            'Invalid ALS parameters')
    ratings = spark.read.parquet(p.input)
    require(all(ratings.schema[name].dataType == kind for name, kind in
                [('user', T.IntegerType()), ('item', T.IntegerType()), ('rating', T.FloatType())]),
            'Expected ALS Parquet v1: user INT, item INT, rating FLOAT')
    training, test = ratings.randomSplit([.8, .2], 1)
    model = ALS(seed=0, rank=p.rank, maxIter=p.numIterations, regParam=regularization,
                implicitPrefs=p.implicitPrefs, numUserBlocks=p.numUserBlocks, numItemBlocks=p.numProductBlocks,
                userCol='user', itemCol='item', ratingCol='rating').fit(training)
    model.setColdStartStrategy('drop')
    evaluated = model.transform(test).cache()
    try:
        count = evaluated.count()
        require(count > 0, 'No evaluable test ratings after cold-start filtering')
        rmse = finite(RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction').evaluate(evaluated),
                      'ALS produced a non-finite evaluation metric')
        print(f'Evaluated ratings = {count}\nRoot-mean-square error = {rmse}')
        return {'evaluated_ratings': count, 'rmse': rmse}
    finally:
        evaluated.unpersist()
