# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Check actual campaign inputs; consume workload/path/expected profile records."""
import json
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder.appName('Final ML controller input acceptance').getOrCreate()
try:
    results = []
    for record in json.load(open(sys.argv[1], encoding='utf-8')):
        path = record['path']
        profile = spark.read.parquet(path + '/_generator_profile').head().asDict()
        for key, expected in record['profile'].items():
            assert profile[key] == expected, (record['workload'], key, profile[key], expected)
        if record['workload'] == 'websearch.pagerank':
            vertices = spark.read.parquet(path + '/vertices')
            edges = spark.read.parquet(path + '/edges')
            assert vertices.count() == record['rows']
            assert vertices.select('id').distinct().count() == record['rows']
            assert edges.filter((F.col('src') < 0) | (F.col('dst') < 0) |
                                (F.col('src') >= record['rows']) | (F.col('dst') >= record['rows'])).count() == 0
            observed = dict(vertices=record['rows'], raw_edges=edges.count(),
                            distinct_edges=edges.distinct().count(),
                            self_edges=edges.filter('src = dst').count())
        else:
            frame = spark.read.parquet(path)
            assert frame.count() == record['rows']
            assert frame.select('id').distinct().count() == record['rows']
            vector_size = F.udf(lambda vector: vector.size, 'int')
            dimensions = frame.select(vector_size('features').alias('d')).agg(
                F.min('d').alias('lo'), F.max('d').alias('hi')).head()
            expected_d = record.get('dimensions', profile.get('dimensions', profile.get('vocabulary')))
            assert dimensions.lo == dimensions.hi == expected_d
            observed = dict(rows=record['rows'], dimensions=expected_d)
            if record['workload'] == 'ml.lda':
                totals = frame.select(F.aggregate(vector_to_array('features'), F.lit(0.0),
                                                  lambda a, b: a + b).alias('tokens'))
                sizes = totals.agg(F.min('tokens').alias('lo'), F.max('tokens').alias('hi'),
                                  F.sum('tokens').alias('total')).head()
                assert sizes.lo >= profile['lengthMin'] and sizes.hi <= profile['lengthMax']
                observed.update(tokens=int(sizes.total), min_tokens=int(sizes.lo), max_tokens=int(sizes.hi))
            else:
                assert frame.filter(~F.col('label').isin(*range(record['classes']))).count() == 0
                observed['labels'] = {str(row.label): row['count'] for row in frame.groupBy('label').count().collect()}
        results.append(dict(record, observed=observed, verified=True))
    print('HIBENCH_FINAL_ML_CONTROLLER_VERIFIED=' + json.dumps(results))
finally:
    spark.stop()
