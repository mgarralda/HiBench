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

"""Join query over the existing Scala-generated SQL dataset."""
from pysparkbench.sql.data import parse, visits, rankings, write


QUERY = "SELECT sourceIP, AVG(pageRank) AS avgPageRank, SUM(adRevenue) AS totalRevenue\nFROM hibench_rankings R JOIN (\n  SELECT sourceIP, destURL, adRevenue FROM hibench_uservisits\n  WHERE datediff(visitDate, '1999-01-01') >= 0 AND datediff(visitDate, '2000-01-01') <= 0\n) UV ON R.pageURL = UV.destURL\nGROUP BY sourceIP ORDER BY totalRevenue DESC"


def query(spark, path):
    visits(spark, path)
    rankings(spark, path)
    return spark.sql(QUERY)


def run(spark, p):
    write(query(spark, p.input), p.output)
