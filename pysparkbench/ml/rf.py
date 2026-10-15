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

"""Random forest with explicit class metadata and historical bootstrap behavior."""
from pyspark.ml.classification import RandomForestClassifier
from pysparkbench.common import arguments, require
from pysparkbench.ml.tree_data import split, evaluate


def parse(args):
    return arguments(args, [('input', str)], [('numTrees', int, 3), ('numClasses', int, 2),
        ('featureSubsetStrategy', str, 'auto'), ('impurity', str, 'gini'), ('maxDepth', int, 4),
        ('maxBins', int, 32), ('modelSeed', int, 1), ('splitSeed', int, 11), ('trainFraction', float, .7)])


def run(spark, p):
    require(p.numClasses >= 2 and p.numTrees > 0, 'Invalid RF class/tree count')
    require(0 <= p.maxDepth <= 30 and p.maxBins >= 2, 'Invalid tree depth or bin count')
    training, test, count = split(spark, p.input, p.numClasses, p.trainFraction, p.splitSeed)
    model = RandomForestClassifier(numTrees=p.numTrees, featureSubsetStrategy=p.featureSubsetStrategy, impurity=p.impurity,
        maxDepth=p.maxDepth, maxBins=p.maxBins, seed=p.modelSeed, subsamplingRate=1., bootstrap=p.numTrees > 1).fit(training)
    ntest, error = evaluate(model.transform(test))
    print(f'Loaded data: numTraining = {count}, numTest = {ntest}, numFeatures = {model.numFeatures}')
    print(f'numClasses = {model.numClasses}; numTrees = {len(model.trees)}; totalNumNodes = {model.totalNumNodes}')
    print(f'Model seed = {p.modelSeed}; split seed = {p.splitSeed}\nTest Error = {error}')
    return {'training': count, 'test': ntest, 'error': error, 'trees': len(model.trees), 'nodes': model.totalNumNodes}
