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

"""The current source-only PageRank recurrence, including dangling-node behavior."""
from pyspark.sql import functions as F
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('input', str), ('output', str), ('iterations', int)])


def iterate(links, iterations):
    require(iterations >= 0, 'PageRank iterations must be nonnegative')
    result = links.select(F.col('src').alias('id')).distinct().withColumn('rank', F.lit(1.0))
    for _ in range(iterations):
        current = result.select(F.col('id').alias('src'), 'rank')
        result = links.join(current, ['src']).groupBy('dst').agg(
            F.sum(F.col('rank') / F.col('degree')).alias('contribution')).select(
            F.col('dst').alias('id'), (F.lit(.15) + F.lit(.85) * F.col('contribution')).alias('rank'))
    return result


def run(spark, p):
    edges = spark.read.parquet(p.input).select('src', 'dst').distinct()
    degree = edges.groupBy('src').agg(F.count(F.lit(1)).alias('degree'))
    links = edges.join(degree, ['src']).cache()
    try:
        iterate(links, p.iterations).write.mode('errorifexists').parquet(p.output)
    finally:
        links.unpersist()
