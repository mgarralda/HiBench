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

"""Historical exact row/reference profile, partition invariance and Parquet checks."""
import collections
import json
import math
import sys
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Logistic generator reference validation').getOrCreate()
try:
    generator = spark._jvm.org.hibench.sparkbench.ml.LogisticRegressionDataGenerator
    original = spark._jvm.org.hibench.sparkbench.ml.LogisticRegressionOriginalReference
    def data(n, d, p, seed=42, shift=3.):
        return DataFrame(generator.dataset(spark._jsparkSession, n, d, p, seed, shift), spark)
    def values(rows):
        return collections.Counter((r.label, tuple(r.features)) for r in rows)

    checks = []
    for n, d, p, shift in ((100, 1000, 2, 3.), (103, 9, 7, 3.), (3, 5, 8, 0.)):
        actual = data(n, d, p).collect() if shift == 3 else data(n, d, p, shift=shift).collect()
        reference = DataFrame(original.reference(spark._jsparkSession, n, d, p, shift), spark).collect()
        assert values(actual) == values(reference)
        assert len({r.id for r in actual}) == n
        assert sum(r.label == 0 for r in actual) == (n + 1) // 2
        assert all(r.label == r.id % 2 for r in actual)
        assert {r.id: tuple(r.features) for r in actual} == {
            r.id: tuple(r.features) for r in data(n, d, 1, shift=shift).collect()}
        checks.append(dict(rows=n, dimensions=d, partitions=p, class_shift=shift,
                           exact_original_match=True, partition_invariant=True))

    # Check custom seed independently: historical generator had seed 42 hardcoded.
    for row in data(101, 8, 3, seed=17, shift=1.5).collect():
        rng = spark._jvm.java.util.Random(17 + row.id)
        expected = [rng.nextGaussian() + row.label * 1.5 for _ in range(8)]
        assert list(row.features) == expected
    checks.append(dict(rows=101, seed=17, class_shift=1.5, custom_seed_formula_verified=True))

    n, d = 20001, 8
    actual = data(n, d, 3).collect()  # bounded fixture, not a production collection
    reference = DataFrame(original.reference(spark._jsparkSession, n, d, 3, 3.), spark).collect()
    assert values(actual) == values(reference)
    moments = []
    for label in (0., 1.):
        rows = [r for r in actual if r.label == label]
        count = len(rows)
        means = [sum(float(r.features[j]) for r in rows) / count for j in range(d)]
        variances = [sum((r.features[j]-means[j])**2 for r in rows) / count for j in range(d)]
        covariances = [sum((r.features[j]-means[j])*(r.features[k]-means[k]) for r in rows)/count
                       for j in range(d) for k in range(j)]
        tails = [sum(abs(r.features[j]-label*3) > 2 for r in rows)/count for j in range(d)]
        assert all(math.isfinite(x) for x in means + variances + covariances)
        moments.append(dict(label=label, count=count, means=means, variances=variances,
                            two_sigma_tail_rates=tails, max_abs_covariance=max(map(abs, covariances))))
    checks.append(dict(rows=n, dimensions=d, exact_original_profile_match=True, conditional_moments=moments))
    path = sys.argv[1]
    data(103, 9, 7).write.mode('overwrite').parquet(path)
    assert values(spark.read.parquet(path).collect()) == values(data(103, 9, 7).collect())
    print('HIBENCH_LOGISTIC_VERIFIED=' + json.dumps(checks))
finally:
    spark.stop()
