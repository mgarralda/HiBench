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

"""Small-fixture execution, independent checks and Scala parity for non-micro modules.

spark-submit --master local[2] --jars <assembly.jar> pyspark_groups_parity.py <checkout> <new scratch>
Fixtures are validation data, not replacements for the Scala production generators.
"""
import datetime
import importlib
import json
import math
from pathlib import Path
import re
import sys
from zipfile import ZipFile, ZIP_DEFLATED

import numpy as np
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import SparkSession, types as T

root, scratch = map(Path, sys.argv[1:3])
selected = set(sys.argv[3:])
scratch.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(root))
from pysparkbench.registry import module_for
from pysparkbench.ml.uncentered_svd import decompose, left_vectors

bundle = scratch / 'pysparkbench.zip'
with ZipFile(bundle, 'w', ZIP_DEFLATED) as archive:
    for path in sorted((root / 'pysparkbench').rglob('*.py')):
        archive.write(path, 'pysparkbench/' + path.relative_to(root / 'pysparkbench').as_posix())


def session():
    spark = SparkSession.builder.master('local[2]').config('spark.sql.shuffle.partitions', '2').config(
        'spark.hadoop.fs.defaultFS', 'file:///').config('spark.sql.adaptive.enabled', 'false').getOrCreate()
    spark.sparkContext.addPyFile(str(bundle))
    return spark


spark = session()
points = np.array([[math.sin(i * .19), math.cos(i * .37), (i % 11) / 10.] for i in range(160)])
features = str(scratch / 'features')
spark.createDataFrame([(int(i), float(i % 2), Vectors.dense(row)) for i, row in enumerate(points)],
                      T.StructType([
                          T.StructField('id', T.LongType()), T.StructField('label', T.DoubleType()),
                          T.StructField('features', VectorUDT())
                      ])).repartition(2).write.parquet(features)
positive = str(scratch / 'positive')
spark.createDataFrame([(int(i), float(i % 2), Vectors.dense([1. + i % 3, 2. + i % 7, 1. + i % 5]))
                      for i in range(160)], schema=spark.read.parquet(features).schema).repartition(2).write.parquet(positive)
ratings = str(scratch / 'ratings')
spark.createDataFrame([(u, i, float(1 + (u + i) % 5)) for u in range(12) for i in range(10)],
                      'user int, item int, rating float').repartition(2).write.parquet(ratings)
edges = str(scratch / 'edges')
spark.createDataFrame([(0, 1), (0, 2), (1, 2), (2, 0), (2, 3)], 'src long, dst long').write.parquet(edges)
weighted = str(scratch / 'weighted')
weighted_rows = [(0, 1, .5), (0, 1, .25), (0, 2, .2), (1, 2, .7), (2, 0, .4), (2, 3, .3)]
spark.createDataFrame(weighted_rows, 'src long, dst long, weight double').write.parquet(weighted)
sql_input = str(scratch / 'sql')
visit_schema = 'sourceIP string, destURL string, visitDate date, adRevenue double, userAgent string, countryCode string, languageCode string, searchWord string, duration int'
spark.createDataFrame([
    ('a', 'x', datetime.date(1999, 1, 1), 1., 'agent', 'ES', 'en', 'q', 1),
    ('a', 'y', datetime.date(2000, 1, 1), 2., 'agent', 'ES', 'en', 'q', 2),
    ('b', 'x', datetime.date(2001, 1, 1), 3., 'agent', 'ES', 'en', 'q', 3),
], visit_schema).write.parquet(sql_input + '/uservisits')
spark.createDataFrame([('x', 2, 1), ('y', 4, 2)], 'pageURL string, pageRank int, avgDuration int').write.parquet(sql_input + '/rankings')
spark.stop()

cases = [
    ('sql.scan', 'sql.ScalaSparkSQLBench', ['scan', sql_input, '{output}']),
    ('sql.aggregation', 'sql.ScalaSparkSQLBench', ['aggregation', sql_input, '{output}']),
    ('sql.join', 'sql.ScalaSparkSQLBench', ['join', sql_input, '{output}']),
    ('websearch.pagerank', 'websearch.DataFramePageRank', [edges, '{output}', '2']),
    ('graph.nweight', 'graph.nweight.DataFrameNWeight', [weighted, '{output}', '2', '2']),
    ('ml.als', 'ml.ALSExample', ['--rank', '2', '--numIterations', '2', '--numUserBlocks', '2', '--numProductBlocks', '2', ratings]),
    ('ml.kmeans', 'ml.DenseKMeans', ['-k', '2', '--numIterations', '2', '--initMode', 'Parallel', features]),
    ('ml.gmm', 'ml.GaussianMixtureModel', ['-k', '2', '--numIterations', '2', features]),
    ('ml.linear', 'ml.LinearRegressionWithElasticNet', ['--maxIter', '2', features]),
    ('ml.lr', 'ml.LogisticRegression', [features, '2', '1e-6', '.01', 'true', 'true', '.7', '11', '.5']),
    ('ml.rf', 'ml.RandomForestClassification', ['--numTrees', '2', '--maxDepth', '2', '--maxBins', '8', features]),
    ('ml.gbt', 'ml.GradientBoostedTree', ['--numIterations', '2', '--maxDepth', '2', '--maxBins', '8', features]),
    ('ml.pca', 'ml.PCAExample', ['--k', '2', features]),
    ('ml.correlation', 'ml.CorrelationExample', [features]),
    ('ml.summarizer', 'ml.SummarizerExample', [features]),
    ('ml.bayes', 'ml.NaiveBayesExample', [positive]),
    ('ml.svm', 'ml.LinearSVMExample', ['--numIterations', '2', features]),
    ('ml.lda', 'ml.LDAExample', ['--numTopics', '2', '--maxIterations', '2', positive, '{output}']),
    ('ml.svd', 'ml.SVDExample', [features, '3', '3', 'true', '1e-9', '1000000', '1g']),
]
metric_patterns = {
    'ml.als': ('rmse', r'Root-mean-square error = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.kmeans': ('cost', r'Total cost = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.gmm': ('log_likelihood', r'Log likelihood = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.linear': ('rmse', r'RMSE: ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.lr': ('accuracy', r'Accuracy = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.rf': ('error', r'Test Error = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.gbt': ('error', r'Test Error = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.bayes': ('accuracy', r'Test set accuracy = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
    'ml.svm': ('auc', r'Area under ROC = ([-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)'),
}
receipt = []
assert selected <= {workload for workload, _, _ in cases}, 'Unknown selected workload'
for workload, scala_name, template in cases:
    if selected and workload not in selected:
        continue
    print('HIBENCH_PARITY_START=' + workload, flush=True)
    spark = session()
    java_output = spark._jvm.java.io.ByteArrayOutputStream()
    original_stdout = spark._jvm.java.lang.System.out
    stream = spark._jvm.java.io.PrintStream(java_output, True, 'UTF-8')
    scala_args = [value.replace('{output}', str(scratch / ('scala-' + workload))) for value in template]
    java_args = spark.sparkContext._gateway.new_array(spark._jvm.java.lang.String, len(scala_args))
    for index, value in enumerate(scala_args):
        java_args[index] = value
    try:
        spark._jvm.java.lang.System.setOut(stream)
        spark._jvm.scala.Console.setOut(stream)
        java_class = spark._jvm.org.hibench.sparkbench
        for segment in scala_name.split('.'):
            java_class = getattr(java_class, segment)
        java_class.main(java_args)
    finally:
        original_stdout_jvm = spark._jvm.java.lang.System
        original_stdout_jvm.setOut(original_stdout)
        spark._jvm.scala.Console.setOut(original_stdout)
        stream.flush()
        scala_console = java_output.toString('UTF-8')
        spark.stop()
    (scratch / (workload + '-scala.log')).write_text(scala_console)
    spark = session()
    module_name = module_for('org.hibench.sparkbench.' + scala_name, template[0])
    module = importlib.import_module(module_name)
    python_args = [value.replace('{output}', str(scratch / ('python-' + workload))) for value in template]
    if workload.startswith('sql.'):
        python_args = python_args[1:]
    checks = module.run(spark, module.parse(python_args))
    if workload in metric_patterns:
        metric, pattern = metric_patterns[workload]
        match = re.search(pattern, scala_console)
        assert match, (workload, scala_console)
        assert math.isclose(checks[metric], float(match[1]), rel_tol=1e-6, abs_tol=1e-8), (workload, checks, match[1])
    if workload.startswith('sql.') or workload in ('graph.nweight', 'websearch.pagerank'):
        python_data = spark.read.parquet(str(scratch / ('python-' + workload)))
        scala_data = spark.read.parquet(str(scratch / ('scala-' + workload)))
        assert python_data.schema == scala_data.schema, workload
        assert python_data.exceptAll(scala_data).count() == 0 and scala_data.exceptAll(python_data).count() == 0, workload
        if workload == 'sql.aggregation':
            assert {row.sourceIP: row.sumAdRevenue for row in python_data.collect()} == {'a': 3., 'b': 3.}
        elif workload == 'sql.join':
            assert [(row.sourceIP, row.avgPageRank, row.totalRevenue) for row in python_data.collect()] == [('a', 3., 3.)]
        elif workload == 'websearch.pagerank':
            expected = {0: .755625, 1: .394375, 2: .883125, 3: .755625}
            actual = {row.id: row.rank for row in python_data.collect()}
            assert actual.keys() == expected.keys() and all(math.isclose(actual[k], expected[k], abs_tol=1e-12) for k in expected), actual
        elif workload == 'graph.nweight':
            # Independent weighted-path recurrence and tie handling, with duplicate edge contributions.
            state = {}
            for src, dst, weight in weighted_rows:
                state.setdefault(src, {})[dst] = state.setdefault(src, {}).get(dst, 0.) + weight
            state = {src: dict(sorted(targets.items(), key=lambda pair: (pair[1], pair[0]), reverse=True)[:2]) for src, targets in state.items()}
            candidates = {}
            for src, via, edge_weight in weighted_rows:
                for dst, value in state.get(via, {}).items():
                    if src != dst:
                        candidates.setdefault(src, {})[dst] = candidates.setdefault(src, {}).get(dst, 0.) + edge_weight * value
            expected = {src: dict(sorted(targets.items(), key=lambda pair: (pair[1], pair[0]), reverse=True)[:2]) for src, targets in candidates.items()}
            actual = {row.id: {target.dst: target.weight for target in row.targets} for row in python_data.collect()}
            for vertex in range(4):
                assert actual[vertex].keys() == expected.get(vertex, {}).keys(), (vertex, actual, expected)
                assert all(math.isclose(value, expected[vertex][dst], abs_tol=1e-12) for dst, value in actual[vertex].items())
    elif workload == 'ml.pca':
        values = np.linalg.eigvalsh(np.cov(points.T))[::-1]
        assert np.allclose(checks['explained_variance'], (values / values.sum())[:2], rtol=1e-6)
    elif workload == 'ml.correlation':
        assert np.allclose(checks['matrix'], np.corrcoef(points.T), rtol=1e-6, atol=1e-8)
    elif workload == 'ml.summarizer':
        assert checks['count'] == len(points)
        assert np.allclose(checks['mean'], points.mean(axis=0))
        assert np.allclose(checks['variance'], points.var(axis=0, ddof=1))
    elif workload == 'ml.lda':
        from pyspark.ml.clustering import LocalLDAModel
        python_model = LocalLDAModel.load(str(scratch / ('python-' + workload)))
        scala_model = LocalLDAModel.load(str(scratch / ('scala-' + workload)))
        assert np.allclose(python_model.topicsMatrix().toArray(), scala_model.topicsMatrix().toArray(), rtol=1e-6)
    elif workload == 'ml.svd':
        expected = np.linalg.svd(points, compute_uv=False)
        scala_values = json.loads(re.search(r'SVD singularValues=(\[[^\]]*\])', scala_console)[1])
        assert np.allclose(checks['singular_values'], expected, rtol=1e-6)
        assert np.allclose(checks['singular_values'], scala_values, rtol=1e-6)
        scala_measurement = json.loads(re.search(r'HIBENCH_WORKLOAD_MEASUREMENT=(.*)', scala_console)[1])
        for measurement in (scala_measurement, checks['measurement']):
            assert measurement['protocol'] == 'svd-phases-v1'
            phases = measurement['phase_seconds']
            assert all(np.isfinite(value) and value >= 0 for value in phases.values())
            assert phases['workload_s'] >= sum(phases[key] for key in
                ('read_plan_s', 'gram_action_s', 'matrix_construction_s', 'eigendecomposition_s', 'result_assembly_s', 'u_plan_s'))
            assert not measurement['u_materialized']
        result = decompose(spark.read.parquet(features), 3, 3, 1e-9, 1000000)
        u = np.array([row.features.toArray() for row in left_vectors(spark.read.parquet(features), result).orderBy('id').collect()])
        assert np.allclose(u @ np.diag(result.singular_values) @ result.right_vectors, points, rtol=1e-6, atol=1e-8)
        deficient = spark.createDataFrame([(i, Vectors.dense([float(i), 2. * i])) for i in range(1, 5)],
            T.StructType([T.StructField('id', T.LongType()), T.StructField('features', VectorUDT())]))
        deficient_result = decompose(deficient, 2, 2, 1e-9, 1000000)
        assert len(deficient_result.singular_values) == 1, deficient_result
        try:
            decompose(deficient, 2, 2, 1e-9, 1)
            raise AssertionError('SVD budget limit was ignored')
        except ValueError as error:
            assert 'budget' in str(error)
    receipt.append({'workload': workload, 'checks': checks, 'scala_parity': True})
    (scratch / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    print('HIBENCH_PARITY_OK=' + workload, flush=True)
    spark.stop()
print('HIBENCH_GROUPS_PARITY=' + json.dumps({'workloads': len(receipt), 'scope': 'small fixture correctness, not performance'}))
