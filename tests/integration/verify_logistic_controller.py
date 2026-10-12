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

"""Verify the actual original tiny dataset and its metadata against frozen generation."""
import collections
import json
import sys
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Logistic controller input verification').getOrCreate()
try:
    path = sys.argv[1]
    actual = spark.read.parquet(path).collect()  # original tiny: 100 rows / 1000 features only
    assert len(actual) == 100 and {r.id for r in actual} == set(range(100))
    assert all(len(r.features) == 1000 and r.label == r.id % 2 for r in actual)
    original = DataFrame(
        spark._jvm.org.hibench.sparkbench.ml.LogisticRegressionOriginalReference.reference(
            spark._jsparkSession, 100, 1000, 2, 3.), spark).collect()
    def values(rows):
        return collections.Counter((r.label, tuple(r.features)) for r in rows)
    assert values(actual) == values(original)
    profiles = spark.read.parquet(path + '/_generator_profile').collect()
    assert len(profiles) == 1
    p = profiles[0]
    assert (p.seed, p.examples, p.dimensions, p.partitions, p.classShift, p.zeroLabels, p.oneLabels) == (
        42, 100, 1000, 2, 3., 50, 50)
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path), spark._jsc.hadoopConfiguration())
    size = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path)).getLength()
    print('HIBENCH_LOGISTIC_CONTROLLER_VERIFIED=' + json.dumps(dict(input=path,
        rows=100, dimensions=1000, class_counts={'0':50,'1':50},
        logical_feature_bytes=800000, physical_bytes_including_profile=size,
        exact_original_data_match=True, profile_verified=True)))
finally:
    spark.stop()
