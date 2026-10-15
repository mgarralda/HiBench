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

"""Dense KMeans; explicit seed, zero tolerance and the configured storage level."""
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import VectorUDT
from pysparkbench.common import arguments, require, finite, storage_level


def parse(args):
    return arguments(args, [('input', str)], [('k', int, -1), ('numIterations', int, 10),
        ('storageLevel', str, 'MEMORY_ONLY'), ('initMode', str, 'Random')])


def run(spark, p):
    require(p.k > 0 and p.numIterations > 0, 'Positive k and iteration count required')
    require(p.initMode in ('Random', 'Parallel'), 'Unsupported KMeans initialization mode')
    data = spark.read.parquet(p.input).select('features')
    require(isinstance(data.schema['features'].dataType, VectorUDT), 'Expected Gaussian Parquet v1 with Spark ML vector features')
    examples = data.persist(storage_level(p.storageLevel))
    try:
        count = examples.count()
        require(count >= p.k, 'Dataset must contain at least k samples')
        model = KMeans(k=p.k, maxIter=p.numIterations, seed=1, tol=0.0,
                       initMode={'Random': 'random', 'Parallel': 'k-means||'}[p.initMode]).fit(examples)
        cost = finite(model.summary.trainingCost, 'Non-finite KMeans training cost')
        print(f'numExamples = {count}.\nTotal cost = {cost}.')
        return {'examples': count, 'cost': cost, 'centers': [center.tolist() for center in model.clusterCenters()]}
    finally:
        examples.unpersist()
