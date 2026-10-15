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


"""Tiny independent correctness and Scala/PySpark parity checks.

spark-submit --master local[2] pyspark_micro_parity.py <checkout> <scratch-directory>
The scratch directory must be new. No benchmark generator is replaced.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
from collections import Counter

from pyspark.sql import SparkSession, types as T

root, scratch = map(Path, sys.argv[1:])
scratch.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(root))
from pysparkbench.registry import module_for
from pysparkbench.runner import execute
from pysparkbench.micro.io import read_tera

def run_micro(spark, name, args):
    return execute(spark, module_for('org.hibench.sparkbench.micro.' + name), args)

spark = SparkSession.builder.config('spark.sql.shuffle.partitions', '2').config('spark.hadoop.fs.defaultFS', 'file:///').getOrCreate()
text = scratch / 'text'
text.mkdir()
(text / 'part-00000').write_text('beta alpha beta\n\nalpha\tomega\n', encoding='utf-8')
tera = str(scratch / 'tera')
records = [(bytes([index]) * 10, bytes([index + 1]) * 90) for index in (3, 0, 2, 1)]
spark.createDataFrame(records, T.StructType([
    T.StructField('key', T.BinaryType()), T.StructField('value', T.BinaryType())
])).write.parquet(tera)
cases = [
    ('ScalaWordCount', [str(text)], []),
    ('ScalaSort', [str(text)], []),
    ('ScalaTeraSort', [tera], []),
    ('ScalaRepartition', [tera], ['true', 'false']),
    ('ScalaInMemRepartition', ['3'], ['true', 'false', '2']),
]
try:
    for name, before, after in cases:
        run_micro(spark, name, before + [str(scratch / ('python-' + name))] + after)
    run_micro(spark, 'ScalaRepartition', [tera, str(scratch / 'disabled'), 'false', 'true'])
    assert not (scratch / 'disabled').exists()
    run_micro(spark, 'ScalaSleep', ['0', '3'])
    invalid = str(scratch / 'invalid-tera')
    spark.createDataFrame([(b'bad', b'x' * 90)], 'key binary, value binary').write.parquet(invalid)
    try:
        read_tera(spark, invalid).write.format('noop').mode('overwrite').save()
        raise AssertionError('Invalid binary record was accepted')
    except Exception as error:
        assert 'Invalid Tera key' in str(error), str(error)
finally:
    spark.stop()

jar = root / 'sparkbench/assembly/target/sparkbench-assembly-2.5.0-dist.jar'
for name, before, after in cases:
    subprocess.run([os.environ['SPARK_HOME'] + '/bin/spark-submit', '--master', 'local[2]',
                    '--conf', 'spark.sql.shuffle.partitions=2', '--conf', 'spark.hadoop.fs.defaultFS=file:///', '--class',
                    'org.hibench.sparkbench.micro.' + name, str(jar),
                    *before, str(scratch / ('scala-' + name)), *after], check=True)
spark = SparkSession.builder.config('spark.hadoop.fs.defaultFS', 'file:///').getOrCreate()
def rows(path):
    return Counter(tuple(bytes(value) if isinstance(value, bytearray) else value for value in row)
                   for row in spark.read.parquet(str(path)).collect())
try:
    for name, _, _ in cases:
        assert rows(scratch / ('python-' + name)) == rows(scratch / ('scala-' + name)), name
    assert rows(scratch / 'python-ScalaWordCount') == Counter({('alpha', 2): 1, ('beta', 2): 1, ('omega', 1): 1})
    assert rows(scratch / 'python-ScalaTeraSort') == Counter(records)
    assert rows(scratch / 'python-ScalaInMemRepartition') == Counter({(bytes(range(200)),): 6})
    for name, column in [('ScalaSort', 'value'), ('ScalaTeraSort', 'key')]:
        data = spark.read.parquet(str(scratch / ('python-' + name)))
        previous = None
        for filename in sorted(data.inputFiles()):
            values = [row[column] for row in spark.read.parquet(filename).collect()]
            assert values == sorted(values), name
            if values:
                assert previous is None or previous <= values[0], name
                previous = values[-1]
    print('HIBENCH_PYSPARK_PARITY=' + json.dumps({'workloads': 5, 'in_memory_repartition': True,
                                                'disabled_output': True, 'invalid_tera_rejected': True}))
finally:
    spark.stop()
