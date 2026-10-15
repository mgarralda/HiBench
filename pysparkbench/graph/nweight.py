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

"""weighted-path-topk-v1: sum duplicate paths; weight/destination descending top K."""
from pyspark import StorageLevel
from pyspark.sql import functions as F, Window
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('input', str), ('output', str), ('degree', int), ('top_k', int)])


def top(frame, limit):
    window = Window.partitionBy('src').orderBy(F.col('weight').desc(), F.col('dst').desc())
    return frame.withColumn('position', F.row_number().over(window)).filter(F.col('position') <= limit).drop('position')


def compute(raw, degree, limit):
    require(degree >= 1 and limit >= 1, 'NWeight degree and top-K must be positive')
    level = StorageLevel.MEMORY_AND_DISK_DESER
    edges = raw.select('src', 'dst', 'weight').persist(level)
    state = None
    output = None
    try:
        require(edges.filter(F.isnan('weight') | (F.abs('weight') == F.lit(float('inf')))).count() == 0,
                'Nonfinite edge weight')
        state = top(edges.groupBy('src', 'dst').agg(F.sum('weight').alias('weight')), limit).persist(level)
        state.count()
        for _ in range(2, degree + 1):
            previous = state
            candidates = edges.alias('e').join(previous.alias('s'), F.col('e.dst') == F.col('s.src')).select(
                F.col('e.src').alias('src'), F.col('s.dst').alias('dst'),
                (F.col('e.weight') * F.col('s.weight')).alias('weight')).filter(
                F.col('src') != F.col('dst')).groupBy('src', 'dst').agg(F.sum('weight').alias('weight'))
            state = top(candidates, limit).persist(level)
            state.count()
            previous.unpersist()
        vertices = edges.select(F.col('src').alias('id')).union(edges.select(F.col('dst').alias('id'))).distinct()
        targets = state.groupBy('src').agg(F.sort_array(F.collect_list(F.struct('dst', 'weight'))).alias('targets'))
        output = vertices.join(targets, vertices.id == targets.src, 'left').select('id', F.coalesce(
            'targets', F.array().cast('array<struct<dst:bigint,weight:double>>')).alias('targets')).persist(level)
        output.count()
        return output
    except Exception:
        if output is not None:
            output.unpersist()
        raise
    finally:
        if state is not None:
            state.unpersist()
        edges.unpersist()


def run(spark, p):
    output = compute(spark.read.parquet(p.input), p.degree, p.top_k)
    try:
        output.write.mode('errorifexists').parquet(p.output)
        count = output.count()
        print(f'NWeight algorithm=weighted-path-topk-v1 degree={p.degree} topK={p.top_k} vertices={count}')
        return {'vertices': count, 'degree': p.degree, 'top_k': p.top_k}
    finally:
        output.unpersist()
