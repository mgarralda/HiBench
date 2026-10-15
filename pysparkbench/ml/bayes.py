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

"""Multinomial Naive Bayes with the shared smoothing and split parameters."""
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pysparkbench.common import arguments, require, finite


def parse(args):
    return arguments(args, [('input', str)], [('lambda', float, 1.), ('trainFraction', float, .8), ('splitSeed', int, 1234)])


def run(spark, p):
    smoothing = getattr(p, 'lambda')
    require(smoothing >= 0, 'Invalid smoothing'); finite(smoothing, 'Invalid smoothing')
    require(0 < p.trainFraction < 1, 'Invalid training fraction')
    data = spark.read.parquet(p.input).select('label', 'features')
    training, test = data.randomSplit([p.trainFraction, 1-p.trainFraction], p.splitSeed)
    ntrain, ntest = training.count(), test.count()
    require(ntrain > 0 and ntest > 0, 'Bayes split must be nonempty')
    model = NaiveBayes(modelType='multinomial', smoothing=smoothing).fit(training)
    predictions = model.transform(test)
    predictions.show()
    accuracy = finite(MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
                      .evaluate(predictions), 'Accuracy must be finite')
    print(f'Bayes numTraining={ntrain} numTest={ntest} smoothing={smoothing} splitSeed={p.splitSeed}')
    print(f'Test set accuracy = {accuracy}')
    return {'training': ntrain, 'test': ntest, 'accuracy': accuracy}
