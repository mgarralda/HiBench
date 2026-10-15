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

"""Shared text and Tera DataFrame input/output contracts."""
import os
from pathlib import Path
from pyspark import SparkFiles
from pyspark.sql import functions as F, types as T

def property_value(key, default):
    value = default
    for name in filter(None, os.environ.get('SPARKBENCH_PROPERTIES_FILES', '').split(',')):
        path = Path(name)
        if not path.is_file():
            path = Path(SparkFiles.get(path.name))
        for line in path.read_text(encoding='utf-8').splitlines():
            parts = line.strip().split(None, 1)
            if parts and parts[0] == key:
                value = parts[1] if len(parts) > 1 else ''
    return value.strip()


def read_tera(spark, path):
    data = spark.read.parquet(path)
    if set(data.columns) != {'key', 'value'} or any(
            not isinstance(data.schema[name].dataType, T.BinaryType) for name in ('key', 'value')):
        raise ValueError('Expected Tera Parquet schema: key binary(10 bytes), value binary(90 bytes)')
    return data.select(*[
        F.when(F.col(name).isNotNull() & (F.length(name) == size), F.col(name))
        .otherwise(F.raise_error(F.lit(f'Invalid Tera {name}: expected {size} bytes'))).alias(name)
        for name, size in (('key', 10), ('value', 90))])


def write(data, path, disabled=False, compression='uncompressed'):
    if disabled:
        data.write.format('noop').mode('overwrite').save()
    else:
        data.write.mode('overwrite').option('compression', compression).parquet(path)


def read_text(spark, path):
    if property_value('sparkbench.inputformat', 'Text').lower() != 'text':
        raise ValueError('WordCount/Sort accept text input only')
    return spark.read.text(path)


def write_text(data, path):
    output_format = property_value('sparkbench.dataframe.outputformat', 'Parquet').lower()
    if output_format not in ('parquet', 'null'):
        raise ValueError(f'Unsupported DataFrame output format: {output_format}')
    write(data, path, output_format == 'null', 'snappy')


def repartition(data, path, cached, disabled):
    from pyspark import StorageLevel
    if cached:
        data.persist(StorageLevel.MEMORY_AND_DISK_DESER)
    try:
        if cached:
            data.count()
        write(data.repartition(int(data.sparkSession.conf.get('spark.sql.shuffle.partitions'))), path, disabled)
    finally:
        if cached:
            data.unpersist()
