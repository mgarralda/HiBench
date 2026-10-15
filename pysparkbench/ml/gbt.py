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

"""Binary boosted trees with fixed logistic loss, all features and explicit seeds."""
from pyspark.ml.classification import GBTClassifier
from pysparkbench.common import arguments, require
from pysparkbench.ml.tree_data import split, evaluate


def parse(args):
    return arguments(args, [('input', str)], [('numClasses', int, 2), ('maxDepth', int, 30), ('maxBins', int, 32),
        ('numIterations', int, 20), ('learningRate', float, .1), ('modelSeed', int, 0), ('splitSeed', int, 11), ('trainFraction', float, .7)])


def run(spark, p):
    require(p.numClasses == 2 and p.numIterations > 0, 'GBT classification requires exactly two classes and positive iterations')
    require(0 < p.learningRate <= 1, 'GBT learning rate must be in (0,1]')
    require(0 <= p.maxDepth <= 30 and p.maxBins >= 2, 'Invalid tree depth or bin count')
    training, test, count = split(spark, p.input, p.numClasses, p.trainFraction, p.splitSeed)
    model = GBTClassifier(maxIter=p.numIterations, stepSize=p.learningRate, maxDepth=p.maxDepth, maxBins=p.maxBins,
                          lossType='logistic', featureSubsetStrategy='all', seed=p.modelSeed, subsamplingRate=1.).fit(training)
    ntest, error = evaluate(model.transform(test))
    print(f'Loaded data: numTraining = {count}, numTest = {ntest}, numFeatures = {model.numFeatures}')
    print(f'numClasses = {model.numClasses}; numTrees = {len(model.trees)}; totalNumNodes = {model.totalNumNodes}')
    print(f'Model seed = {p.modelSeed}; split seed = {p.splitSeed}\nTest Error = {error}')
    return {'training': count, 'test': ntest, 'error': error, 'trees': len(model.trees), 'nodes': model.totalNumNodes}
