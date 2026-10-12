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

"""Bounded checks of actual candidate inputs and persisted outputs.

Python collections are reference-only, capped by campaign sizes; workload code
does not collect its datasets. ML fit metrics do not certify predictive quality.
"""
import collections
import datetime
import json
import math
import re
import sys
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName('Candidate data and output acceptance').getOrCreate()
checks = []


def bounded(frame, limit=100000):
    values = frame.limit(limit + 1).collect()
    assert len(values) <= limit, 'Reference collection budget exceeded'
    return values


def count_map(rows):
    return collections.Counter(tuple(bytes(v) if isinstance(v, bytearray) else v for v in row) for row in rows)


try:
    for record in json.load(open(sys.argv[1], encoding='utf-8')):
        name, path, output, params = (record[k] for k in ('workload', 'input', 'output', 'parameters'))
        observed = {'workload': name, 'checks': []}
        try:
            if name == 'micro.sleep':
                observed['checks'].append('no external dataset required; completed scheduling metrics recorded')
            elif name in ('micro.wordcount', 'micro.sort'):
                lines = [r.value for r in bounded(spark.read.text(path))]
                logical = sum(len(line.encode('utf-8')) + 1 for line in lines)
                assert logical >= int(params['datasize'])
                actual = bounded(spark.read.parquet(output).withColumn('_file', F.input_file_name()))
                if name == 'micro.wordcount':
                    expected = collections.Counter(w for line in lines for w in re.split(r'\s+', line) if w)
                    assert {r.word: r['count'] for r in actual} == dict(expected)
                else:
                    assert collections.Counter(r.value for r in actual) == collections.Counter(lines)
                    partitions = collections.defaultdict(list)
                    for row in actual:
                        partitions[row._file].append(row.value)
                    ordered = [v for _, values in sorted(partitions.items()) for v in values]
                    assert ordered == sorted(lines)
                observed.update(rows=len(lines), logical_text_bytes=logical)
                observed['checks'].append('logical byte budget and independent word counts/sort output')
            elif name in ('micro.terasort', 'micro.repartition'):
                source = bounded(spark.read.parquet(path))
                assert all(len(r.key) == 10 and len(r.value) == 90 for r in source)
                # Tera/Repartition datasize counts records, unlike text byte budgets.
                assert len(source) == int(params['datasize'])
                if name == 'micro.repartition' and str(params.get('disableoutput', False)).lower() == 'true':
                    fs = spark._jvm.org.apache.hadoop.fs.Path(output).getFileSystem(spark._jsc.hadoopConfiguration())
                    assert not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(output)), 'No-output mode must not persist a result dataset'
                    assert int(params['datasize']) in record['successful_shuffle_records_by_stage'].values(), 'Actual full-record shuffle not observed'
                    observed.update(rows=len(source), logical_payload_bytes=len(source) * 100,
                                    shuffle_records_by_stage=record['successful_shuffle_records_by_stage'],
                                    output_mode='noop; no persisted output multiset is available')
                    observed['checks'].append('100-byte input records, exact row count, actual full-record shuffle and absent output under disableoutput=true')
                    observed['status'] = 'passed'
                    checks.append(observed)
                    print('HIBENCH_CANDIDATE_CHECK=' + json.dumps(observed), flush=True)
                    continue
                actual = bounded(spark.read.parquet(output).withColumn('_file', F.input_file_name()))
                assert count_map(source) == count_map([r[:-1] for r in actual])
                if name == 'micro.terasort':
                    partitions = collections.defaultdict(list)
                    for row in actual:
                        partitions[row._file].append(bytes(row.key))
                    ordered = [v for _, values in sorted(partitions.items()) for v in values]
                    assert ordered == sorted(bytes(r.key) for r in source)
                observed.update(rows=len(source), logical_payload_bytes=len(source) * 100)
                observed['checks'].append('100-byte records and output multiset; TeraSort key order where applicable')
            elif name.startswith('sql.'):
                ranks = bounded(spark.read.parquet(path + '/rankings'))
                visits = bounded(spark.read.parquet(path + '/uservisits'))
                assert len(ranks) == int(params['pages'])
                assert len(visits) <= int(params['uservisits'])
                result = bounded(spark.read.parquet(output))
                if name == 'sql.scan':
                    columns = ['sourceIP', 'destURL', 'visitDate', 'adRevenue', 'userAgent', 'countryCode', 'languageCode', 'searchWord', 'duration']
                    assert collections.Counter(tuple(r[c] for c in columns) for r in result) == collections.Counter(tuple(r[c] for c in columns) for r in visits)
                elif name == 'sql.aggregation':
                    expected = collections.defaultdict(float)
                    for row in visits:
                        expected[row.sourceIP] += row.adRevenue
                    assert {r.sourceIP for r in result} == set(expected)
                    assert all(math.isclose(r.sumAdRevenue, expected[r.sourceIP], rel_tol=1e-10, abs_tol=1e-10) for r in result)
                else:
                    urls = {r.pageURL: r.pageRank for r in ranks}
                    expected = collections.defaultdict(lambda: [0, 0, 0.])
                    for row in visits:
                        if datetime.date(1999, 1, 1) <= row.visitDate <= datetime.date(2000, 1, 1) and row.destURL in urls:
                            group = expected[row.sourceIP]
                            group[0] += urls[row.destURL]; group[1] += 1; group[2] += row.adRevenue
                    assert {r.sourceIP for r in result} == set(expected)
                    assert all(math.isclose(r.avgPageRank, expected[r.sourceIP][0] / expected[r.sourceIP][1], rel_tol=1e-10) and math.isclose(r.totalRevenue, expected[r.sourceIP][2], rel_tol=1e-10, abs_tol=1e-10) for r in result)
                observed.update(rankings=len(ranks), retained_visits=len(visits), output_rows=len(result))
                observed['checks'].append('cardinalities and independent query output from actual inputs; original RNG equivalence remains historical evidence')
            elif name == 'websearch.pagerank':
                vertices = bounded(spark.read.parquet(path + '/vertices'))
                edges = {(r.src, r.dst) for r in bounded(spark.read.parquet(path + '/edges'))}
                assert len(vertices) == int(params['pages'])
                degree = collections.Counter(src for src, _ in edges)
                state = {src: 1. for src in degree}
                for _ in range(int(params['num_iterations'])):
                    contributions = collections.defaultdict(float)
                    for src, dst in edges:
                        if src in state:
                            contributions[dst] += state[src] / degree[src]
                    state = {dst: .15 + .85 * value for dst, value in contributions.items()}
                actual = {r.id: r['rank'] for r in bounded(spark.read.parquet(output))}
                assert actual.keys() == state.keys()
                assert all(math.isclose(actual[k], value, rel_tol=1e-9, abs_tol=1e-12) for k, value in state.items())
                observed.update(vertices=len(vertices), distinct_edges=len(edges))
                observed['checks'].append('independent historical PageRank recurrence on actual generated edges')
            elif name == 'graph.nweight':
                observed['checks'].append('delegated to dedicated model-weight and recurrence verifier')
                observed['status'] = 'dedicated_verifier_required'
            else:
                data_path = path + '/samples' if name in ('ml.kmeans', 'ml.gmm') else path
                frame = spark.read.parquet(data_path)
                n = frame.count()
                if name == 'ml.als':
                    assert n == int(params['ratings'])
                    assert frame.filter((F.col('user') < 0) | (F.col('user') >= int(params['users'])) | (F.col('item') < 0) | (F.col('item') >= int(params['products']))).count() == 0
                    expected = collections.Counter()
                    implicit = str(params['implicitprefs']).lower() == 'true'
                    for partition in range(record['partitions']):
                        rng = spark._jvm.java.util.Random(partition)
                        for _ in range(n * (partition + 1) // record['partitions'] - n * partition // record['partitions']):
                            expected[rng.nextInt(int(params['users'])), rng.nextInt(int(params['products'])), float(rng.nextInt(5) + 1) - (2.5 if implicit else 0)] += 1
                    assert collections.Counter((r.user, r.item, r.rating) for r in bounded(frame)) == expected
                    observed['checks'].append('exact independent original Java RNG ratings including duplicate pairs')
                else:
                    expected_n = params.get('num_of_samples', params.get('num_of_documents', params.get('examples', params.get('pages'))))
                    assert n == int(expected_n), (name, n, expected_n)
                    sizes = F.udf(lambda v: int(v.size), 'int')
                    dimensions = frame.select(sizes('features').alias('d')).agg(F.min('d').alias('lo'), F.max('d').alias('hi')).head()
                    expected_d = params.get('dimensions', params.get('num_of_vocabulary', params.get('features')))
                    if expected_d is not None:
                        assert dimensions.lo == dimensions.hi == int(expected_d)
                    assert frame.select('id').distinct().count() == n
                    if name not in ('ml.kmeans', 'ml.gmm'):
                        profile = spark.read.parquet(path + '/_generator_profile').head().asDict()
                        if 'seed' in profile and 'seed' in params:
                            assert profile['seed'] == int(params['seed'])
                        if 'partitions' in profile:
                            assert profile['partitions'] == record['partitions']
                        observed['profile'] = {k: v for k, v in profile.items() if isinstance(v, (int, float, str, bool))}
                    if 'label' in frame.columns and name not in ('ml.linear', 'ml.pca'):
                        labels = {r.label: r['count'] for r in frame.groupBy('label').count().collect()}
                        classes = int(params.get('classes', 2))
                        assert all(0 <= label < classes for label in labels)
                        observed['label_counts'] = labels
                    if name == 'ml.pca':
                        for row in bounded(frame):
                            rng = spark._jvm.java.util.Random(int(params['seed']) + row.id)
                            assert math.isclose(row.label, rng.nextGaussian(), rel_tol=1e-12, abs_tol=1e-12)
                            expected = [rng.nextGaussian() - .5 for _ in range(int(params['features']))]
                            assert all(math.isclose(a, b, rel_tol=1e-12, abs_tol=1e-12) for a, b in zip(row.features, expected))
                        observed['checks'].append('exact original row-seeded Gaussian label and feature formula')
                    if name == 'ml.lda':
                        from pyspark.ml.clustering import DistributedLDAModel, LocalLDAModel
                        model = DistributedLDAModel.load(output) if params['optimizer'] == 'em' else LocalLDAModel.load(output)
                        assert model.vocabSize() == int(params['num_of_vocabulary']) and model.getK() == int(params['num_of_topics'])
                        observed['checks'].append('persisted LDA model reload and schema')
                    observed['dimensions'] = dimensions.lo
                    observed['checks'].append('actual input row count, unique IDs, feature dimensions and available seed/partition metadata')
                observed['rows'] = n
                observed['fit_result_scope'] = 'fit completion and logged metrics; model accuracy/numerical equivalence not independently revalidated here'
            observed.setdefault('status', 'passed')
        except Exception as exc:
            observed.update(status='failed', error=repr(exc))
        checks.append(observed)
        print('HIBENCH_CANDIDATE_CHECK=' + json.dumps(observed), flush=True)
    with open(sys.argv[2], 'w', encoding='utf-8') as stream:
        json.dump(checks, stream, indent=2)
finally:
    spark.stop()
