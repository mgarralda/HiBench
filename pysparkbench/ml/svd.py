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

"""packed-gram-v1 SVD; optional U remains a lazy distributed projection."""
from pysparkbench.common import arguments, boolean, require
from pysparkbench.ml.uncentered_svd import decompose, left_vectors
import contextlib
import io
import os
import time
import numpy as np


def parse(args):
    return arguments(args, [('input', str), ('dimensions', int), ('components', int), ('compute_u', boolean),
        ('rcond', float), ('matrix_budget', int), ('maxResultSize', str)])


def run(spark, p):
    started = time.perf_counter()
    phase_started = started
    data = spark.read.parquet(p.input)
    read_plan_s = time.perf_counter() - phase_started
    result = decompose(data, p.dimensions, p.components, p.rcond, p.matrix_budget)
    phase_started = time.perf_counter()
    if p.compute_u:
        projection = left_vectors(data, result)
        require('features' in projection.columns, 'Invalid U plan')
    phases = {'read_plan_s': read_plan_s, **result.phase_seconds,
              'u_plan_s': time.perf_counter() - phase_started,
              'workload_s': time.perf_counter() - started}
    values = result.singular_values.tolist()
    build = io.StringIO()
    with contextlib.redirect_stdout(build):
        np.show_config()
    numerical_runtime = {'eigensolver': 'numpy.linalg.eigh', 'numpy_version': np.__version__,
        'numpy_build_configuration': build.getvalue(),
        'thread_environment': {key: os.environ.get(key) for key in
            ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS', 'BLIS_NUM_THREADS')},
        'effective_native_threads': None, 'effective_driver_cores': None,
        'driver_cores_configured': spark.sparkContext.getConf().get('spark.driver.cores', None),
        'driver_memory_configured': spark.sparkContext.getConf().get('spark.driver.memory', None),
        'driver_logical_cpus': os.cpu_count()}
    print(f'SVD algorithm=packed-gram-v1 rows={result.rows} requested={p.components} retained={len(values)}')
    print(f'SVD singularValues={values}')
    return {'rows': result.rows, 'requested': p.components, 'retained': len(values), 'singular_values': values,
            'measurement': {'schema_version': 1, 'protocol': 'svd-phases-v1',
                'application_id': spark.sparkContext.applicationId, 'phase_seconds': phases,
                'numerical_runtime': numerical_runtime, 'u_materialized': False}}
