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

"""Independent historical generation and algorithm checks; bounded fixtures only."""
import collections
import json
import math
import sys
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.ml.functions import vector_to_array
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier

spark = SparkSession.builder.appName('Tree data and algorithm historical verification').getOrCreate()
try:
    base = spark._jvm.org.hibench.sparkbench.ml
    def data(family, n, d, p, seed=42, shift=.3):
        gen = base.RandomForestDataGenerator if family == 'rf' else base.GradientBoostedTreeDataGenerator
        return DataFrame(gen.dataset(spark._jsparkSession, n, d, p, seed, shift), spark)
    def original(family, n, d, p, shift=.3):
        gen = base.RandomForestOriginalReference if family == 'rf' else base.GradientBoostedTreeOriginalReference
        return DataFrame(gen.reference(spark._jsparkSession, n, d, p, shift), spark)
    def values(rows):
        return collections.Counter((r.label, tuple(r.features)) for r in rows)

    checks = []
    for family in ('rf', 'gbt'):
        for n, d, p in ((10, 100, 2), (100, 500, 2), (103, 9, 7), (3, 5, 8)):
            rows = data(family, n, d, p).collect()
            assert values(rows) == values(original(family, n, d, p).collect())
            assert len({r.id for r in rows}) == n
            assert sum(r.label == 0 for r in rows) == (n + 1) // 2
            assert all(r.label == r.id % 2 for r in rows)
            assert {r.id: tuple(r.features) for r in rows} == {
                r.id: tuple(r.features) for r in data(family, n, d, 1).collect()}
            checks.append(dict(family=family, rows=n, dimensions=d, partitions=p,
                               exact_original_match=True, partition_invariant=True))
        path = sys.argv[2] + '/' + family
        frame = data(family, 103, 9, 7)
        frame.write.mode('overwrite').parquet(path)
        assert values(spark.read.parquet(path).collect()) == values(frame.collect())

    with open(sys.argv[1], encoding='utf-8') as f:
        scales = json.load(f)
    for family in ('rf', 'gbt'):
        for scale, fields in scales[family].items():
            n, d = fields['examples'], fields['features']
            assert fields['feature_payload_bytes'] == n * d * 8
            # RF huge and larger are 16–35 GB logical payloads: planning only.
            if family == 'rf' and scale in ('huge', 'gigantic', 'bigdata'):
                checks.append(dict(family=family, scale=scale, rows=n, dimensions=d,
                                   logical_feature_bytes=n*d*8, evidence='preset_definition_checked_only'))
                continue
            summary = data(family, n, d, 2).select('label',
                F.size(vector_to_array('features')).alias('d')).agg(
                    F.count('*').alias('n'), F.min('d').alias('dmin'), F.max('d').alias('dmax'),
                    F.sum(F.when(F.col('label') == 0, 1).otherwise(0)).alias('zeros')).first()
            assert (summary.n, summary.dmin, summary.dmax, summary.zeros) == (n, d, d, (n+1)//2)
            checks.append(dict(family=family, scale=scale, rows=n, dimensions=d,
                               logical_feature_bytes=n*d*8, evidence='actual_generated_count_and_dimensions'))

    # Preserve original empirical joint profile, even when it departs from ideal moments.
    rows = data('rf', 20001, 8, 3).collect()
    assert values(rows) == values(original('rf', 20001, 8, 3).collect())
    assert values(rows) == values(original('gbt', 20001, 8, 3).collect())
    moments = []
    for label in (0., 1.):
        group = [r for r in rows if r.label == label]
        means = [sum(r.features[j] for r in group)/len(group) for j in range(8)]
        variances = [sum((r.features[j]-means[j])**2 for r in group)/len(group) for j in range(8)]
        moments.append(dict(label=label, rows=len(group), means=means, variances=variances))
    checks.append(dict(rows=20001, dimensions=8, exact_original_joint_profile=True, conditional_moments=moments))

    for row in data('rf', 101, 8, 3, 17, 1.5).collect():
        rng = spark._jvm.java.util.Random(17 + row.id)
        assert list(row.features) == [rng.nextGaussian() + row.label*1.5 for _ in range(8)]
    checks.append(dict(seed=17, class_shift=1.5, custom_formula_verified=True))

    # Same controlled training/test rows, continuous features and legacy settings.
    fixture = data('rf', 200, 4, 1).orderBy('id').coalesce(1)
    fixture = fixture.withMetadata('label', {'ml_attr': {'type':'nominal', 'name':'label', 'num_vals':2}})
    train, test = fixture.where('id < 160'), fixture.where('id >= 160')
    for family, trees in (('rf', 5), ('rf', 1), ('gbt', 5)):
        if family == 'rf':
            estimator = RandomForestClassifier(numTrees=trees, maxDepth=3, maxBins=16,
                seed=1, featureSubsetStrategy='all', impurity='gini', bootstrap=trees>1)
        else:
            estimator = GBTClassifier(maxIter=trees, maxDepth=3, maxBins=16,
                seed=0, stepSize=.1, featureSubsetStrategy='all', lossType='logistic')
        model = estimator.fit(train)
        scored = model.transform(test).select('id','prediction','probability').collect()
        prediction = {r.id:r.prediction for r in scored}
        reference = DataFrame(base.TreeAlgorithmOriginalReference.predictions(
            spark._jsparkSession, train._jdf, test._jdf, family=='rf', trees), spark)
        expected = {r.id:r for r in reference.collect()}
        hard_differences = sum(prediction[i] != r.legacyPrediction for i, r in expected.items())
        if family == 'rf':
            for row in scored:
                oracle = expected[row.id]
                assert math.isclose(row.probability[1], oracle.softProbability, rel_tol=0, abs_tol=1e-12)
                if abs(oracle.softProbability - .5) > 1e-12:
                    assert row.prediction == oracle.softPrediction
        else:
            assert hard_differences == 0
        repeated = estimator.fit(train)
        assert prediction == {r.id:r.prediction for r in repeated.transform(test).select('id','prediction').collect()}
        assert len(model.trees) == trees
        checks.append(dict(family=family, trees=trees, training_rows=160, test_rows=40,
                           legacy_hard_vote_differences=hard_differences,
                           legacy_leaf_probability_reference_match=family=='rf',
                           exact_legacy_predictions=hard_differences==0,
                           repeatable_predictions=True))
    print('HIBENCH_TREES_VERIFIED=' + json.dumps(checks))
finally:
    spark.stop()
