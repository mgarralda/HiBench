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

"""Distributed structural checks; separate from measured workload executions."""
import json
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder.appName('Intermediate dataset integrity').getOrCreate()
results = []
try:
    campaign = json.load(open(sys.argv[1], encoding='utf-8'))
    for run in campaign['runs']:
        name = run['workload']
        row = {'workload': name}
        try:
            assert run['state'] == 'succeeded', run['state']
            params = run['spec']['workloads'][0]['parameters']
            path = [e['path'] for e in run['events'] if e['kind'] == 'input'][-1]
            indices = [e['index'] for e in run['events'] if e['kind'] == 'phase_finished' and e['phase'] == 'run']
            output = run['spec']['target']['hdfs_uri'] + '/HiBench-Control/runs/' + run['run_id'] + '/' + name + '/' + str(indices[-1])
            if name == 'micro.terasort':
                data = spark.read.parquet(path)
                actual = spark.read.parquet(output)
                assert data.count() == actual.count() == params['datasize']
                assert data.filter((F.length('key') != 10) | (F.length('value') != 90)).count() == 0
                assert data.exceptAll(actual).limit(1).count() == actual.exceptAll(data).limit(1).count() == 0
                row.update(rows=params['datasize'], logical_payload_bytes=100 * params['datasize'],
                           checks=['record count, 100-byte payload, full output multiset'])
            elif name == 'sql.join':
                assert spark.read.parquet(path + '/rankings').count() == params['pages']
                assert spark.read.parquet(path + '/uservisits').count() == params['uservisits']
                row.update(rankings=params['pages'], uservisits=params['uservisits'],
                           output_rows=spark.read.parquet(output).count(), checks=['input cardinalities and readable output'])
            elif name in ('ml.kmeans', 'ml.svd'):
                data = spark.read.parquet(path + '/samples' if name == 'ml.kmeans' else path)
                n = params.get('num_of_samples', params.get('examples'))
                d = params.get('dimensions', params.get('features'))
                assert data.count() == data.select('id').distinct().count() == n
                values = vector_to_array('features')
                assert data.filter(F.size(values) != d).count() == 0
                assert data.filter(F.exists(values, lambda x: x.isNull() | F.isnan(x) | (F.abs(x) == float('inf')))).count() == 0
                row.update(rows=n, dimensions=d, checks=['row count, unique IDs, finite features and dimension'])
                if name == 'ml.svd':
                    profile = spark.read.parquet(path + '/_generator_profile').head().asDict()
                    assert profile['seed'] == params['seed'] and profile['partitions'] == 4
                    row['profile'] = profile
                    stats = data.select(F.posexplode(values).alias('dimension', 'value')).groupBy('dimension').agg(
                        F.avg('value').alias('mean'), F.stddev_pop('value').alias('stddev')).collect()
                    assert all(abs(r['mean']) < .1 and .9 < r.stddev < 1.1 for r in stats)
                    row['distribution_check'] = '64 marginal normal(0,1) means within +/-0.1 and standard deviations within [0.9,1.1]; broad profile check, not RNG equivalence'
                else:
                    profiles = spark.read.parquet(path + '/cluster')
                    assert profiles.count() == params['num_of_clusters']
                    assert profiles.agg(F.sum('samples').alias('n')).head().n == n
                    tagged = data.join(F.broadcast(profiles),
                                       (data.id >= profiles.startId) & (data.id < profiles.startId + profiles.samples))
                    marginal = tagged.select('clusterId', 'mean', 'stddev', 'samples',
                                             F.posexplode(vector_to_array('features')).alias('dimension', 'value'))
                    stats = marginal.groupBy('clusterId', 'dimension', 'samples').agg(
                        F.avg('value').alias('observed_mean'), F.stddev_pop('value').alias('observed_stddev'),
                        F.first(F.element_at('mean', F.col('dimension') + 1)).alias('expected_mean'),
                        F.first(F.element_at('stddev', F.col('dimension') + 1)).alias('expected_stddev')).collect()
                    assert len(stats) == params['num_of_clusters'] * d
                    assert all(abs(r.observed_mean - r.expected_mean) <= 5 * r.expected_stddev / r.samples ** .5
                               and .9 <= r.observed_stddev / r.expected_stddev <= 1.1 for r in stats)
                    row['distribution_check'] = '40 cluster/feature means within five standard errors and standard deviations within 10% of saved Gaussian profile'
            elif name == 'websearch.pagerank':
                assert spark.read.parquet(path + '/vertices').count() == params['pages']
                edges = spark.read.parquet(path + '/edges')
                ranks = spark.read.parquet(output)
                assert ranks.filter(F.col('rank').isNull() | F.isnan('rank') | (F.abs('rank') == float('inf'))).count() == 0
                assert ranks.select('id').distinct().count() == ranks.count()
                row.update(vertices=params['pages'], edges=edges.count(), output_rows=ranks.count(),
                           checks=['vertex count, unique result IDs and finite ranks'])
            elif name == 'graph.nweight':
                edges = spark.read.parquet(path)
                assert edges.count() == 2 * params['edges']
                assert edges.select('src', 'dst').distinct().count() == 2 * params['edges']
                assert edges.filter(F.col('src') == F.col('dst')).count() == 0
                assert edges.filter((F.col('src') < 0) | (F.col('src') >= 2401080)
                                    | (F.col('dst') < 0) | (F.col('dst') >= 2401080)).count() == 0
                profile = spark.read.parquet(path + '/_generator_profile').head().asDict()
                assert profile['seed'] == params['seed'] and profile['pairs'] == params['edges']
                assert profile['directedEdges'] == 2 * params['edges'] and profile['universe'] == 2401080
                assert profile['partitions'] == 4
                row['profile'] = profile
                reverse = edges.select(F.col('dst').alias('src'), F.col('src').alias('dst'), 'weight')
                assert edges.exceptAll(reverse).limit(1).count() == 0
                actual = spark.read.parquet(output)
                vertices = edges.select(F.col('src').alias('id')).union(edges.select(F.col('dst').alias('id'))).distinct()
                assert vertices.exceptAll(actual.select('id')).limit(1).count() == 0
                assert actual.select('id').exceptAll(vertices).limit(1).count() == 0
                assert actual.filter(F.size('targets') > params['max_out_edges']).count() == 0
                assert actual.filter(F.exists('targets', lambda x: x['weight'].isNull() | F.isnan(x['weight']) | (F.abs(x['weight']) == float('inf')))).count() == 0
                row.update(directed_edges=2 * params['edges'], output_vertices=actual.count(),
                           checks=['exact unique directed-edge count, symmetry, no self edges, result vertex set, finite top-K weights'])
            row['status'] = 'passed'
        except Exception as exc:
            row.update(status='failed', error=str(exc))
        results.append(row)
        print('HIBENCH_INTERMEDIATE_CHECK=' + json.dumps(row), flush=True)
    with open(sys.argv[2], 'w', encoding='utf-8') as stream:
        json.dump(results, stream, indent=2)
finally:
    spark.stop()
if any(r['status'] != 'passed' for r in results):
    sys.exit(1)
