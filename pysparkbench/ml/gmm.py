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

"""Gaussian mixture with the same model seed and estimator defaults."""
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.linalg import VectorUDT
from pysparkbench.common import arguments, require, finite, storage_level


def parse(args):
    return arguments(args, [('input', str)], [('k', int, -1), ('numIterations', int, 10), ('storageLevel', str, 'MEMORY_ONLY')])


def run(spark, p):
    require(p.k > 0 and p.numIterations > 0, 'Positive k and iteration count required')
    data = spark.read.parquet(p.input).select('features')
    require(isinstance(data.schema['features'].dataType, VectorUDT), 'Expected Gaussian Parquet v1 with Spark ML vector features')
    examples = data.persist(storage_level(p.storageLevel))
    try:
        count = examples.count()
        require(count >= p.k, 'Dataset must contain at least k samples')
        model = GaussianMixture(k=p.k, maxIter=p.numIterations, seed=1).fit(examples)
        likelihood = finite(model.summary.logLikelihood, 'Non-finite GMM log likelihood')
        print(f'numExamples = {count}.\nLog likelihood = {likelihood}')
        # The Scala workload reads all k means/covariances for its console summary.
        gaussians = model.gaussiansDF.collect()
        for index, row in enumerate(gaussians):
            print(f'Gaussian {index}: weight={model.weights[index]} mu={row.mean} sigma={row.cov}')
        return {'examples': count, 'log_likelihood': likelihood, 'weights': model.weights,
                'means': [row.mean.toArray().tolist() for row in gaussians]}
    finally:
        examples.unpersist()
