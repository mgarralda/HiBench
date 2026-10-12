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

"""Validate actual controller-generated Parquet inputs against effective experiment parameters."""
import json,sys
from pyspark.sql import SparkSession,functions as F
from pyspark.ml.functions import vector_to_array
spark=SparkSession.builder.appName('Statistics controller input acceptance').getOrCreate()
try:
    checks=[]
    for record in json.load(open(sys.argv[1])):
        frame=spark.read.parquet(record['path'])
        shape=frame.select(F.size(vector_to_array('features')).alias('d')).agg(F.count('*').alias('n'),F.min('d').alias('lo'),F.max('d').alias('hi')).head()
        assert (shape.n,shape.lo,shape.hi)==(record['examples'],record['features'],record['features'])
        profile=spark.read.parquet(record['path']+'/_generator_profile').head().asDict()
        for key,expected in (('examples',record['examples']),('dimensions',record['features']),('seed',record['seed']),('partitions',record['partitions'])):assert profile[key]==expected
        if record['workload']=='ml.correlation':assert profile['classShift']==record['class_shift']
        checks.append(dict(record,profile=profile,shape_verified=True))
    print('HIBENCH_STATISTICS_CONTROLLER_VERIFIED='+json.dumps(checks))
finally:spark.stop()
