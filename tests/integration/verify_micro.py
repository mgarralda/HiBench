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

"""Independent tiny-data correctness check. Run with spark-submit --master local[2]."""
import argparse
from collections import Counter
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

parser=argparse.ArgumentParser()
parser.add_argument("wordcount_input")
parser.add_argument("wordcount_output")
parser.add_argument("sort_input")
parser.add_argument("sort_output")
args=parser.parse_args()
spark=SparkSession.builder.appName("HiBench correctness verification").getOrCreate()
try:
    lines=[row.value for row in spark.read.text(args.wordcount_input).collect()]
    expected=Counter(word for line in lines for word in re.split(r"\s+",line) if word)
    actual={row.word:row["count"] for row in spark.read.parquet(args.wordcount_output).collect()}
    assert actual == dict(expected), "WordCount output differs from independent token counts"
    lines=[row.value for row in spark.read.text(args.sort_input).collect()]
    rows=spark.read.parquet(args.sort_output).withColumn("file",input_file_name()).collect()
    assert Counter(row.value for row in rows) == Counter(lines), "Sort lost or duplicated input"
    partitions={}
    for row in rows: partitions.setdefault(row.file,[]).append(row.value)
    previous=None
    for filename, values in sorted(partitions.items()):
        assert values == sorted(values), "Sort partition is not ordered"
        if values:
            assert previous is None or previous <= values[0], "Sort partition ranges overlap out of order"
            previous=values[-1]
    print("HIBENCH_CORRECTNESS="+json.dumps(dict(wordcount=True,sort=True,words=sum(expected.values()),sort_rows=len(rows))))
finally:
    spark.stop()
