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

"""One Python worker sleep per task; includes Python worker overhead."""
import time
from pyspark.sql import functions as F, types as T
from pysparkbench.common import arguments, require


def parse(args):
    return arguments(args, [('seconds', int), ('tasks', int)])


def run(spark, p):
    require(0 <= p.seconds <= (2**63 - 1) // 1000 and p.tasks > 0, 'Invalid sleep duration or task count')
    seconds = p.seconds
    @F.udf(T.LongType())
    def sleep_task(_):
        time.sleep(seconds)
        return 1
    completed = spark.range(0, p.tasks, 1, p.tasks).select(sleep_task('id').alias('completed'))
    count = completed.agg(F.sum('completed')).first()[0]
    require(count == p.tasks, 'Not all sleep tasks completed')
    print(f'HIBENCH_SLEEP_TASKS={count};SECONDS_PER_TASK={seconds}')
    return {'tasks': count, 'seconds_per_task': seconds}
