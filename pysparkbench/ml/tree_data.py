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

"""Shared class metadata, label validation and held-out tree evaluation."""
from pyspark.sql import functions as F
from pysparkbench.common import require, finite


def split(spark, path, classes, fraction, seed):
    require(classes >= 2 and 0 < fraction < 1, 'Invalid classes or train fraction')
    metadata = {'ml_attr': {'name': 'label', 'type': 'nominal', 'num_vals': classes}}
    data = spark.read.parquet(path).select(F.col('label').alias('label', metadata=metadata), 'features')
    training, test = data.randomSplit([fraction, 1-fraction], seed)
    invalid = F.col('label').isNull() | F.isnan('label') | (F.col('label') < 0) | (F.col('label') >= classes) | (F.col('label') != F.floor('label'))
    summary = training.agg(F.count(F.lit(1)).alias('n'), F.sum(F.when(invalid, 1).otherwise(0)).alias('invalid')).first()
    require(summary.n > 0, 'Tree training split is empty')
    require(summary.invalid == 0, 'Tree labels must be integer class IDs in the configured range')
    return training, test, summary.n


def evaluate(predictions):
    result = predictions.agg(F.count(F.lit(1)).alias('n'),
        F.sum(F.when(F.col('label') != F.col('prediction'), 1).otherwise(0)).alias('errors')).first()
    require(result.n > 0, 'Tree evaluation split is empty')
    return result.n, finite(result.errors / result.n, 'Non-finite tree test error')
