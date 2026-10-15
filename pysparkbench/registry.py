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

"""Scala entry points and their Python modules; generators are never dispatched."""
import sys

MODULES = {
    'micro.ScalaSleep': 'micro.sleep',
    'micro.ScalaSort': 'micro.sort',
    'micro.ScalaWordCount': 'micro.wordcount',
    'micro.ScalaTeraSort': 'micro.terasort',
    'micro.ScalaRepartition': 'micro.repartition',
    'micro.ScalaInMemRepartition': 'micro.inmem_repartition',
    'ml.ALSExample': 'ml.als',
    'ml.DenseKMeans': 'ml.kmeans',
    'ml.GaussianMixtureModel': 'ml.gmm',
    'ml.LinearRegressionWithElasticNet': 'ml.linear',
    'ml.LogisticRegression': 'ml.lr',
    'ml.RandomForestClassification': 'ml.rf',
    'ml.GradientBoostedTree': 'ml.gbt',
    'ml.PCAExample': 'ml.pca',
    'ml.CorrelationExample': 'ml.correlation',
    'ml.SummarizerExample': 'ml.summarizer',
    'ml.NaiveBayesExample': 'ml.bayes',
    'ml.LinearSVMExample': 'ml.svm',
    'ml.LDAExample': 'ml.lda',
    'ml.SVDExample': 'ml.svd',
    'websearch.DataFramePageRank': 'websearch.pagerank',
    'graph.nweight.DataFrameNWeight': 'graph.nweight',
}


def module_for(scala_class, first_argument=''):
    prefix = 'org.hibench.sparkbench.'
    name = scala_class.removeprefix(prefix)
    if name == 'sql.ScalaSparkSQLBench':
        if first_argument not in ('scan', 'aggregation', 'join'):
            raise ValueError('Unknown SQL workload: ' + first_argument)
        return 'pysparkbench.sql.' + first_argument
    if name not in MODULES or not scala_class.startswith(prefix):
        raise ValueError('No PySpark execution implementation for ' + scala_class)
    return 'pysparkbench.' + MODULES[name]


if __name__ == '__main__':
    print(module_for(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else ''))
