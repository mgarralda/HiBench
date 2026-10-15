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

"""Shared schema checks and session-local SQL views."""
from pyspark.sql import types as T
from pysparkbench.common import arguments, require

VISIT_COLUMNS = ['sourceIP', 'destURL', 'visitDate', 'adRevenue', 'userAgent', 'countryCode', 'languageCode', 'searchWord', 'duration']
VISIT_TYPES = [T.StringType(), T.StringType(), T.DateType(), T.DoubleType(), T.StringType(), T.StringType(), T.StringType(), T.StringType(), T.IntegerType()]


def parse(args):
    return arguments(args, [('input', str), ('output', str)])


def require_schema(data, columns, types):
    require(all(name in data.columns and data.schema[name].dataType == kind for name, kind in zip(columns, types)),
            'Invalid SQL schema; regenerate or explicitly convert legacy SQL input')


def visits(spark, path):
    data = spark.read.parquet(path.rstrip('/') + '/uservisits')
    require_schema(data, VISIT_COLUMNS, VISIT_TYPES)
    data.select(*VISIT_COLUMNS).createOrReplaceTempView('hibench_uservisits')


def rankings(spark, path):
    data = spark.read.parquet(path.rstrip('/') + '/rankings')
    columns = ['pageURL', 'pageRank', 'avgDuration']
    require_schema(data, columns, [T.StringType(), T.IntegerType(), T.IntegerType()])
    data.select(*columns).createOrReplaceTempView('hibench_rankings')


def write(data, output):
    data.write.mode('overwrite').option('compression', 'uncompressed').parquet(output)
