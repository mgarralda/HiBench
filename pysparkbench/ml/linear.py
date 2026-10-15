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

"""Elastic-net linear regression over the same cached training split."""
from pyspark.ml.regression import LinearRegression
from pysparkbench.common import arguments, require, finite


def parse(args):
    return arguments(args, [('input', str)], [('regParam', float, .3), ('elasticNetParam', float, .8),
        ('maxIter', int, 10), ('tol', float, 1e-6), ('fracTest', float, .25)])


def run(spark, p):
    require(0 <= p.fracTest < 1, 'Test fraction must be in [0,1)')
    training = spark.read.parquet(p.input).select('label', 'features').randomSplit([1-p.fracTest, p.fracTest], 12345)[0].cache()
    try:
        count = training.count()
        require(count > 0, 'Linear regression training split is empty')
        dimensions = len(training.select('features').first()[0])
        model = LinearRegression(featuresCol='features', labelCol='label', maxIter=p.maxIter, tol=p.tol,
                                 regParam=p.regParam, elasticNetParam=p.elasticNetParam).fit(training)
        summary = model.summary
        print(f'Loaded data: numTraining = {count}, numFeatures = {dimensions}')
        print(f'Coefficients: {model.coefficients} Intercept: {model.intercept}')
        print(f'numIterations: {summary.totalIterations}\nobjectiveHistory: {summary.objectiveHistory}')
        summary.residuals.show()
        rmse = finite(summary.rootMeanSquaredError, 'Non-finite training RMSE')
        print(f'RMSE: {rmse}\nr2: {summary.r2}')
        return {'training': count, 'dimensions': dimensions, 'coefficients': model.coefficients.toArray().tolist(),
                'intercept': model.intercept, 'rmse': rmse}
    finally:
        training.unpersist()
