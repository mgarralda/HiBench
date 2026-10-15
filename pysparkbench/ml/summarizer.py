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

"""Distributed vector count, moments, extrema and norms."""
from pyspark.ml.stat import Summarizer
from pyspark.sql import functions as F
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('input', str)])


def run(spark, p):
    training = spark.read.parquet(p.input).select('features')
    summary = training.select(Summarizer.metrics('count', 'mean', 'variance', 'max', 'min',
        'numNonZeros', 'normL1', 'normL2').summary(F.col('features')).alias('summary')).select('summary.*').first()
    require(summary['count'] > 0, 'Summarizer input must be nonempty')
    print(f'Summarizer count={summary["count"]}')
    for metric in ('mean', 'variance', 'max', 'min'):
        print(f'{metric}={summary[metric]}')
    return {'count': summary['count'], **{metric: summary[metric].toArray().tolist()
            for metric in ('mean', 'variance', 'max', 'min', 'numNonZeros', 'normL1', 'normL2')}}
