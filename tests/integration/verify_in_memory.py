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

"""Verify the preserved legacy in-memory profile on small output only."""
import argparse
import json
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("output")
parser.add_argument("records_per_partition", type=int)
parser.add_argument("generation_partitions", type=int)
args = parser.parse_args()
spark = SparkSession.builder.appName("HiBench Next in-memory verification").getOrCreate()
try:
    rows = spark.read.parquet(args.output).collect()
    expected = bytes(range(200))
    assert len(rows) == args.records_per_partition * args.generation_partitions
    assert all(bytes(row.value) == expected for row in rows)
    print("HIBENCH_IN_MEMORY_VERIFIED=" + json.dumps(dict(records=len(rows),
          record_bytes=200, logical_bytes=len(rows) * 200)))
finally:
    spark.stop()
