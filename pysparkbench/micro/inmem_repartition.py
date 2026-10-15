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

"""Preserve N 200-byte records per generation partition."""
from pyspark.sql import functions as F
from pysparkbench.common import arguments, boolean, require
from pysparkbench.micro.io import repartition


def parse(args):
    return arguments(args, [('records', int), ('output', str), ('cached', boolean), ('disabled', boolean), ('partitions', int)])


def run(spark, p):
    require(p.partitions > 0 and p.records > 0 and p.records <= (2**63 - 1) // p.partitions // 200,
            'Invalid in-memory record count')
    data = spark.range(0, p.records * p.partitions, 1, p.partitions).select(F.lit(bytearray(range(200))).alias('value'))
    repartition(data, p.output, p.cached, p.disabled)
