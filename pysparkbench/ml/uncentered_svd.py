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

"""Distributed packed Gram aggregation with bounded partition buffers.

Only the reduced Gram matrix reaches the driver. Normal equations retain the
same conditioning limitation and roundoff floor as the Scala implementation.
Python/Arrow transport and NumPy kernels differ from Scala's typed aggregator.
"""
from dataclasses import dataclass, field
import math
import time
import numpy as np
import pyarrow as pa
from pyspark.ml.functions import vector_to_array
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql import functions as F
from pysparkbench.common import require


@dataclass
class Result:
    rows: int
    singular_values: np.ndarray
    right_vectors: np.ndarray
    phase_seconds: dict = field(default_factory=dict)


def decompose(data, dimensions, requested, rcond, matrix_budget_bytes):
    require(dimensions > 0 and 0 < requested <= dimensions, 'Invalid SVD dimensions/components')
    require(math.isfinite(rcond) and 0 < rcond < 1, 'Invalid SVD reciprocal condition cutoff')
    square = dimensions * dimensions
    require(square <= 2**31 - 1, 'Dense driver matrix exceeds supported array limit')
    packed = dimensions * (dimensions + 1) // 2
    estimate = 8 * (3 * square + dimensions * requested + packed)
    require(matrix_budget_bytes > 0 and estimate <= matrix_budget_bytes,
            f'SVD matrix array estimate {estimate} exceeds configured budget {matrix_budget_bytes}')
    schema = pa.schema([('rows', pa.int64()), ('upper', pa.list_(pa.float64()))])

    def partials(batches):
        upper = np.zeros(packed, dtype=np.float64)
        count = 0
        for batch in batches:
            for scalar in batch.column('features'):
                values = np.asarray(scalar.as_py(), dtype=np.float64)
                require(values.shape == (dimensions,), 'Inconsistent vector dimensions')
                require(np.isfinite(values).all(), 'SVD requires finite features')
                # Packed upper triangle, column-major, matching Scala dspr("U").
                outer = np.outer(values, values)
                offset = 0
                for column in range(dimensions):
                    upper[offset:offset + column + 1] += outer[:column + 1, column]
                    offset += column + 1
                count += 1
                require(count <= 2**63 - 1, 'SVD row count overflow')
        yield pa.RecordBatch.from_arrays([pa.array([count], type=pa.int64()),
            pa.array([upper], type=pa.list_(pa.float64()))], schema=schema)

    def merge(batches):
        upper = np.zeros(packed, dtype=np.float64)
        count = 0
        for batch in batches:
            for rows, values in zip(batch.column('rows'), batch.column('upper')):
                count += rows.as_py()
                require(count <= 2**63 - 1, 'SVD row count overflow')
                upper += np.asarray(values.as_py(), dtype=np.float64)
        yield pa.RecordBatch.from_arrays([pa.array([count], type=pa.int64()),
            pa.array([upper], type=pa.list_(pa.float64()))], schema=schema)

    timings = {}
    started = time.perf_counter()
    partial = data.select('id', vector_to_array('features').alias('features')).mapInArrow(
        partials, 'rows long, upper array<double>')
    gram = partial.repartition(1).mapInArrow(merge, 'rows long, upper array<double>').first()
    timings['gram_action_s'] = time.perf_counter() - started
    started = time.perf_counter()
    require(gram.rows > 0, 'SVD input is empty')
    upper = np.asarray(gram.upper)
    require(np.isfinite(upper).all(), 'Gram aggregation overflow')
    matrix = np.zeros((dimensions, dimensions))
    offset = 0
    for column in range(dimensions):
        values = upper[offset:offset + column + 1]
        matrix[:column + 1, column] = values
        matrix[column, :column + 1] = values
        offset += column + 1
    timings['matrix_construction_s'] = time.perf_counter() - started
    started = time.perf_counter()
    eigenvalues, eigenvectors = np.linalg.eigh(matrix)
    timings['eigendecomposition_s'] = time.perf_counter() - started
    started = time.perf_counter()
    largest = max(0., eigenvalues[-1])
    cutoff = largest * max(rcond * rcond, dimensions * math.ulp(1.))
    retained = [index for index in range(dimensions - 1, -1, -1) if eigenvalues[index] > cutoff]
    retained = retained[:min(requested, gram.rows)]
    values, vectors = np.sqrt(eigenvalues[retained]), eigenvectors[:, retained].T
    timings['result_assembly_s'] = time.perf_counter() - started
    return Result(gram.rows, values, vectors, timings)


def left_vectors(data, result):
    vectors, singular_values = result.right_vectors, result.singular_values
    @F.udf(VectorUDT())
    def project(features):
        return Vectors.dense(vectors.dot(features.toArray()) / singular_values)
    return data.select('id', project('features').alias('features'))
