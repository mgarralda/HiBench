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

"""Distributed integrity checks for named presets; no full driver collection.

These checks do not certify numerical accuracy, physical sort order or scaling.
Output fingerprints have collision risk and are explicitly labelled as such.
"""
import json
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.linalg import VectorUDT
from pyspark.sql.types import StructType, StructField

spark = SparkSession.builder.appName('Named preset distributed integrity').getOrCreate()
results = []


def fingerprint(frame):
    return frame.agg(F.count('*').alias('rows'),
                     F.sum(F.xxhash64(*frame.columns).cast('decimal(38,0)')).alias('hash_sum')).head().asDict()


def same_multiset(left, right):
    assert fingerprint(left) == fingerprint(right)


try:
    for run in json.load(open(sys.argv[1], encoding='utf-8'))['runs']:
        name = run['workload']
        row = {'workload': name, 'scope': 'distributed structural integrity; not independent numerical certification'}
        if run['state'] != 'succeeded':
            row.update(status='not_checked', reason='workload ' + run['state'])
            results.append(row)
            continue
        try:
            p = run['default_parameters']
            paths = [e['path'] for e in run['events'] if e['kind'] == 'input']
            path = paths[-1] if paths else None
            indices = [e['index'] for e in run['events'] if e['kind'] == 'phase_finished' and e['phase'] == 'run']
            output = run['spec']['target']['hdfs_uri'] + '/HiBench-Control/runs/' + run['run_id'] + '/' + name + '/' + str(indices[-1])
            if name == 'micro.sleep':
                row['checks'] = ['completed execution; no external dataset']
            elif name in ('micro.sort', 'micro.wordcount'):
                source = spark.read.text(path)
                stats = source.agg(F.count('*').alias('rows'), F.sum(F.length(F.encode('value', 'UTF-8')) + 1).alias('bytes')).head()
                assert stats.bytes >= int(p['datasize'])
                actual = spark.read.parquet(output)
                if name == 'micro.sort':
                    same_multiset(source, actual.select('value'))
                    row['output_check'] = 'full-scan count/hash-sum fingerprint; collision risk; physical order not checked'
                else:
                    expected = source.select(F.explode(F.split('value', r'\s+')).alias('word')).filter(F.col('word') != '').groupBy('word').count()
                    assert expected.exceptAll(actual.select('word', 'count')).limit(1).count() == 0
                    assert actual.select('word', 'count').exceptAll(expected).limit(1).count() == 0
                    row['output_check'] = 'distributed exact word-count equality using Spark SQL'
                row.update(rows=stats.rows, logical_bytes=stats.bytes)
            elif name in ('micro.terasort', 'micro.repartition'):
                source = spark.read.parquet(path)
                assert source.count() == int(p['datasize'])
                assert source.filter((F.length('key') != 10) | (F.length('value') != 90)).limit(1).count() == 0
                row.update(rows=int(p['datasize']), logical_bytes=100 * int(p['datasize']))
                if name == 'micro.terasort':
                    same_multiset(source.select('key', 'value'), spark.read.parquet(output).select('key', 'value'))
                    row['output_check'] = 'full-scan count/hash-sum fingerprint; collision risk; physical order not checked'
                elif str(p['disableoutput']).lower() == 'true':
                    row['output_check'] = 'no-output mode; shuffle must be verified separately from event logs'
                else:
                    same_multiset(source.select('key', 'value'), spark.read.parquet(output).select('key', 'value'))
            elif name.startswith('sql.'):
                rankings = spark.read.parquet(path + '/rankings')
                visits = spark.read.parquet(path + '/uservisits')
                assert rankings.count() == int(p['pages'])
                count = visits.count()
                assert 0 < count <= int(p['uservisits'])
                actual = spark.read.parquet(output)
                row.update(rankings=int(p['pages']), visits=count, output_rows=actual.count())
                if name == 'sql.scan':
                    same_multiset(visits.select(*actual.columns), actual)
                    row['output_check'] = 'full-scan preservation fingerprint with collision risk'
                else:
                    if name == 'sql.aggregation':
                        expected = visits.groupBy('sourceIP').agg(F.sum('adRevenue').alias('sumAdRevenue'))
                        measures = ['sumAdRevenue']
                    else:
                        qualified = visits.filter(F.col('visitDate').between('1999-01-01', '2000-01-01')).join(rankings, visits.destURL == rankings.pageURL)
                        expected = qualified.groupBy('sourceIP').agg(F.avg('pageRank').alias('avgPageRank'), F.sum('adRevenue').alias('totalRevenue'))
                        measures = ['avgPageRank', 'totalRevenue']
                    assert actual.count() == actual.select('sourceIP').distinct().count() == expected.count()
                    compared = expected.alias('e').join(actual.alias('r'), F.col('e.sourceIP') == F.col('r.sourceIP'), 'full_outer')
                    invalid = F.col('e.sourceIP').isNull() | F.col('r.sourceIP').isNull()
                    for field in measures:
                        a, b = F.col('e.' + field), F.col('r.' + field)
                        invalid = invalid | a.isNull() | b.isNull() | F.isnan(a) | F.isnan(b) | (F.abs(a - b) > F.lit(1e-10) * F.greatest(F.lit(1.0), F.abs(a)))
                    assert compared.filter(invalid).limit(1).count() == 0
                    row['output_check'] = 'distributed reference query, complete key sets and numeric tolerance 1e-10; same Spark engine, not independent numerical engine'
            elif name == 'websearch.pagerank':
                vertices = spark.read.parquet(path + '/vertices')
                edges = spark.read.parquet(path + '/edges')
                actual = spark.read.parquet(output)
                assert vertices.count() == int(p['pages'])
                assert actual.count() == actual.select('id').distinct().count()
                assert actual.filter(F.col('rank').isNull() | F.isnan('rank') | (F.abs('rank') == float('inf'))).limit(1).count() == 0
                row.update(vertices=int(p['pages']), edges=edges.count(), output_rows=actual.count(), output_check='unique result IDs and finite ranks; recurrence not revalidated')
            elif name == 'graph.nweight':
                edges = spark.read.parquet(path)
                assert edges.count() == 2 * int(p['edges'])
                assert edges.filter((F.col('src') == F.col('dst')) | (F.col('src') < 0) | (F.col('src') >= 2401080) | (F.col('dst') < 0) | (F.col('dst') >= 2401080)).limit(1).count() == 0
                reverse = edges.select(F.col('dst').alias('src'), F.col('src').alias('dst'), 'weight')
                same_multiset(edges.select('src', 'dst', 'weight'), reverse)
                actual = spark.read.parquet(output)
                assert actual.filter(F.size('targets') > int(p['max_out_edges'])).limit(1).count() == 0
                assert actual.filter(F.exists('targets', lambda x: x['weight'].isNull() | F.isnan(x['weight']) | (F.abs(x['weight']) == float('inf')))).limit(1).count() == 0
                row.update(directed_edges=2 * int(p['edges']), output_rows=actual.count(), output_check='symmetry fingerprint, finite top-K; numerical recurrence not revalidated')
            elif name == 'ml.als':
                data = spark.read.parquet(path)
                assert data.count() == int(p['ratings'])
                assert data.filter((F.col('user') < 0) | (F.col('user') >= int(p['users'])) | (F.col('item') < 0) | (F.col('item') >= int(p['products']))).limit(1).count() == 0
                row.update(rows=int(p['ratings']), checks=['rating count and user/item ranges'])
            else:
                data_path = path + '/samples' if name in ('ml.kmeans', 'ml.gmm') else path
                original = spark.read.parquet(data_path)
                # Read the physical VectorUDT struct to inspect sparse values without densifying.
                schema = StructType([StructField(f.name, VectorUDT().sqlType() if f.name == 'features' else f.dataType, f.nullable) for f in original.schema])
                data = spark.read.schema(schema).parquet(data_path)
                dimension = F.when(F.col('features.type') == 0, F.col('features.size')).otherwise(F.size('features.values'))
                invalid = F.exists('features.values', lambda x: x.isNull() | F.isnan(x) | (F.abs(x) == float('inf')))
                stats = data.agg(F.count('*').alias('rows'), F.min(dimension).alias('min_dimension'), F.max(dimension).alias('max_dimension'), F.sum(F.when(invalid | F.col('features').isNull(), 1).otherwise(0)).alias('invalid')).head().asDict()
                n = next(p[k] for k in ('num_of_samples', 'num_of_documents', 'examples', 'pages') if k in p)
                assert stats['rows'] == int(n) and stats['invalid'] == 0
                d = next((p[k] for k in ('dimensions', 'num_of_vocabulary', 'features') if k in p), None)
                if d is not None:
                    assert stats['min_dimension'] == stats['max_dimension'] == int(d)
                assert data.select('id').distinct().count() == int(n)
                row.update(stats, checks=['exact count, unique IDs, full sparse/dense dimensions and finite stored values'], output_check='fit completed; model accuracy not revalidated')
            row['status'] = 'passed'
        except Exception as exc:
            row.update(status='failed', error=repr(exc))
        results.append(row)
        with open(sys.argv[2], 'w', encoding='utf-8') as stream:
            json.dump(results, stream, indent=2)
        print('HIBENCH_SCALE_CHECK=' + json.dumps(row), flush=True)
finally:
    spark.stop()
