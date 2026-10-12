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

"""Check actual original-tiny RF/GBT preparation against historical generators."""
import collections
import json
import sys
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.appName('Tree controller input verification').getOrCreate()
try:
    with open(sys.argv[1], encoding='utf-8') as f:
        paths = json.load(f)
    assert {item['family'] for item in paths} == {'rf', 'gbt'} and len(paths) == 2
    for item in paths:
        path, family = item['input'], item['family']
        actual = spark.read.parquet(path).collect()  # original tiny: exactly ten rows
        assert len(actual) == 10 and {r.id for r in actual} == set(range(10))
        assert all(len(r.features) == 100 and r.label == r.id % 2 for r in actual)
        base = spark._jvm.org.hibench.sparkbench.ml
        gen = base.RandomForestOriginalReference if family == 'rf' else base.GradientBoostedTreeOriginalReference
        expected = DataFrame(gen.reference(spark._jsparkSession, 10, 100, 2, .3), spark).collect()
        def values(rows):
            return collections.Counter((r.label, tuple(r.features)) for r in rows)
        assert values(actual) == values(expected)
        profiles = spark.read.parquet(path + '/_generator_profile').collect()
        assert len(profiles) == 1
        p = profiles[0]
        assert (p.seed, p.examples, p.dimensions, p.partitions, p.classShift, p.zeroLabels, p.oneLabels) == (
            42, 10, 100, 2, .3, 5, 5)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path), spark._jsc.hadoopConfiguration())
        size = fs.getContentSummary(spark._jvm.org.apache.hadoop.fs.Path(path)).getLength()
        print('HIBENCH_TREES_CONTROLLER_VERIFIED=' + json.dumps(dict(family=family, input=path,
            rows=10, dimensions=100, class_counts={'0':5,'1':5}, logical_feature_bytes=8000,
            physical_bytes_including_profile=size, exact_original_data_match=True, profile_verified=True)))
finally:
    spark.stop()
