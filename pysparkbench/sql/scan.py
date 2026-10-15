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

"""Scan query over the existing Scala-generated SQL dataset."""
from pysparkbench.sql.data import parse, visits, rankings, write


QUERY = 'SELECT * FROM hibench_uservisits'


def query(spark, path):
    visits(spark, path)
    return spark.sql(QUERY)


def run(spark, p):
    write(query(spark, p.input), p.output)
