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

"""Independent tiny-data Tera verification, with no Hadoop examples dependency."""
import argparse
from collections import Counter
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

parser = argparse.ArgumentParser()
parser.add_argument("input")
parser.add_argument("output")
parser.add_argument("records", type=int)
parser.add_argument("--sorted", action="store_true")
args = parser.parse_args()
spark = SparkSession.builder.appName("HiBench Next Tera verification").getOrCreate()


def original_records(count):
    state = 0
    a = 0x2360ED051FC65DA44385DF649FCCF645
    c = 0x4A696D47726179524950202020202001
    mask = (1 << 128) - 1
    for row in range(count):
        state = (state * a + c) & mask
        rand = state.to_bytes(16, "big")
        filler = b"".join(bytes([digit]) * 4 for digit in f"{state:032X}"[20:].encode("ascii"))
        yield rand[:10] + b"\x00\x11" + f"{row:032X}".encode("ascii") + b"\x88\x99\xaa\xbb" + filler + b"\xcc\xdd\xee\xff"


try:
    source = spark.read.parquet(args.input).collect()
    rows = spark.read.parquet(args.output).withColumn("file", input_file_name()).collect()
    actual = Counter(bytes(row.key) + bytes(row.value) for row in source)
    assert len(source) == args.records
    assert all(len(row.key) == 10 and len(row.value) == 90 for row in source)
    assert actual == Counter(original_records(args.records)), "Original TeraGen payload mismatch"
    assert Counter(bytes(row.key) + bytes(row.value) for row in rows) == actual, "Records lost, duplicated or changed"
    if args.sorted:
        partitions = {}
        for row in rows:
            partitions.setdefault(row.file, []).append(bytes(row.key))
        previous = None
        for filename, keys in sorted(partitions.items()):
            assert keys == sorted(keys), "Binary keys out of order within a partition"
            if keys:
                assert previous is None or previous <= keys[0], "Partition ranges out of order"
                previous = keys[-1]
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    physical = sum(status.getLen() for status in fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(args.input))
                   if status.getPath().getName().endswith(".parquet"))
    print("HIBENCH_TERA_VERIFIED=" + json.dumps(dict(records=args.records,
          logical_bytes=args.records * 100, physical_parquet_bytes=physical,
          original_payload=True, preserved=True, sorted=args.sorted)))
finally:
    spark.stop()
