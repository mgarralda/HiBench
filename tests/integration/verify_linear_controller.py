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

"""Verify the historical 50,000 x 1,000, two-partition Linear fixture.

This frozen reference is not the active v2.5.0 Tiny preset."""
import json
import math
import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.ml.functions import vector_to_array

spark = SparkSession.builder.appName('Linear controller input verification').getOrCreate()
try:
    path = sys.argv[1]
    frame = spark.read.parquet(path)
    summary = frame.select('id', F.size(vector_to_array('features')).alias('d')).agg(
        F.count('*').alias('n'), F.countDistinct('id').alias('unique'),
        F.min('id').alias('first'), F.max('id').alias('last'),
        F.min('d').alias('dmin'), F.max('d').alias('dmax')).first()
    assert (summary.n, summary.unique, summary.first, summary.last, summary.dmin, summary.dmax) == (
        50000, 50000, 0, 49999, 1000, 1000)
    profiles = spark.read.parquet(path + '/_generator_profile').collect()
    assert len(profiles) == 1
    profile = profiles[0]
    assert (profile.seed, profile.examples, profile.dimensions, profile.partitions, profile.noiseStd) == (
        42, 50000, 1000, 2, 1.)
    rng = spark._jvm.java.util.Random(42)
    weights = [rng.nextDouble() - .5 for _ in range(1000)]
    assert list(profile.weights) == weights
    ids = [0, 1, 2, 25000, 25001, 25002]
    actual = {r.id: r for r in frame.where(F.col('id').isin(ids)).collect()}
    assert set(actual) == set(ids)
    for part in range(2):
        rng = spark._jvm.java.util.Random(42 ^ part)
        for offset in range(3):
            values = [(rng.nextDouble() - .5) * 2 for _ in range(1000)]
            label = sum(w*x for w, x in zip(weights, values)) + rng.nextGaussian()
            row = actual[part * 25000 + offset]
            assert list(row.features) == values
            assert math.isclose(row.label, label, abs_tol=1e-12, rel_tol=0)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path), spark._jsc.hadoopConfiguration())
    physical_bytes = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path)).getLength()
    print('HIBENCH_LINEAR_CONTROLLER_VERIFIED=' + json.dumps(dict(
        input=path, rows=50000, dimensions=1000, unique_ids=50000,
        logical_feature_bytes=400000000, physical_bytes_including_profile=physical_bytes,
        profile_verified=True, original_selected_rows_verified=6)))
finally:
    spark.stop()
