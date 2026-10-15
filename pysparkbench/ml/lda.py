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

"""Spark ML LDA with saved-model reload and vocabulary/topic validation."""
from pyspark.ml.clustering import LDA, LocalLDAModel, DistributedLDAModel
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('input', str), ('output', str)], [('numTopics', int, 10), ('maxIterations', int, 10),
        ('optimizer', str, 'online'), ('maxResultSize', str, '1g'), ('modelSeed', int, 0)])


def run(spark, p):
    require(p.numTopics > 1 and p.maxIterations > 0, 'Invalid LDA topic/iteration count')
    require(p.optimizer in ('online', 'em'), 'Unsupported LDA optimizer')
    corpus = spark.read.parquet(p.input).select('id', 'features')
    count = corpus.count()
    require(count > 0, 'LDA corpus is empty')
    model = LDA(k=p.numTopics, maxIter=p.maxIterations, optimizer=p.optimizer, seed=p.modelSeed).fit(corpus)
    model.write().save(p.output)
    saved = (DistributedLDAModel if p.optimizer == 'em' else LocalLDAModel).load(p.output)
    require(saved.getK() == p.numTopics and saved.vocabSize() == model.vocabSize(), 'LDA reload schema differs')
    print(f'LDA documents={count} vocabulary={model.vocabSize()} topics={saved.getK()} optimizer={p.optimizer} seed={p.modelSeed}')
    return {'documents': count, 'vocabulary': model.vocabSize(), 'topics': saved.getK(), 'optimizer': p.optimizer}
