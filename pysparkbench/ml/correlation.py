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

"""Pearson or Spearman correlation using Spark ML statistics."""
from pyspark.ml.stat import Correlation
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('input', str)], [('corrType', str, 'pearson')])


def run(spark, p):
    require(p.corrType in ('pearson', 'spearman'), 'Unsupported correlation method')
    training = spark.read.parquet(p.input).select('features').cache()
    try:
        count = training.count()
        require(count > 1, 'Correlation requires at least two rows')
        dimensions = len(training.first()[0])
        matrix = Correlation.corr(training, 'features', p.corrType).first()[0]
        print(f'numTraining = {count}\nnumFeatures = {dimensions}\n{p.corrType} correlation matrix:\n{matrix}')
        return {'rows': count, 'dimensions': dimensions, 'matrix': matrix.toArray().tolist()}
    finally:
        training.unpersist()
