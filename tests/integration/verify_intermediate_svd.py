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

"""Compare actual campaign outputs with direct NumPy SVD; cap at 1M scalars.

Collection is confined to this independent reference, not the workload.
The planned 5,000 x 64 matrix has 320,000 doubles (2.56 MB logical payload).
"""
import json
import sys
import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Intermediate SVD independent reference').getOrCreate()
try:
    plan = json.load(open(sys.argv[1], encoding='utf-8'))
    assert plan['rows'] * plan['dimensions'] <= 1000000, 'Reference scalar budget exceeded'
    data = spark.read.parquet(plan['input'])
    rows = sorted(data.limit(plan['rows'] + 1).collect(), key=lambda row: row.id)
    assert len(rows) == plan['rows']
    assert [row.id for row in rows] == list(range(plan['rows']))
    matrix = np.array([row.features.toArray() for row in rows])
    assert matrix.shape == (plan['rows'], plan['dimensions']) and np.isfinite(matrix).all()
    reference = np.linalg.svd(matrix, compute_uv=False)[:plan['components']]
    assert len(plan['reported_values']) == 4, 'All four actual workload outputs required'
    for values in plan['reported_values']:
        assert len(values) == plan['components']
        assert np.allclose(values, reference, rtol=1e-7, atol=1e-8)
    result = {'status': 'passed', 'rows': len(rows), 'dimensions': matrix.shape[1],
              'scalar_budget': 1000000, 'logical_feature_bytes': matrix.nbytes,
              'reference_singular_values': reference.tolist(), 'validated_actual_outputs': 4,
              'relative_tolerance': 1e-7, 'absolute_tolerance': 1e-8}
    with open(sys.argv[2], 'w', encoding='utf-8') as stream:
        json.dump(result, stream, indent=2)
    print('HIBENCH_INTERMEDIATE_SVD=' + json.dumps(result), flush=True)
finally:
    spark.stop()
