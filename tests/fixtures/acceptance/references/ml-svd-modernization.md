<!--
Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Uncentered SVD modernization — 2026-10-09

SVD uses a typed Dataset aggregation of the packed upper triangle of AᵀA. Only this reduced matrix reaches the driver; workload code does not collect input rows or call RDD, RowMatrix or ObjectFile APIs. Breeze 2.1.0, provided by Spark 3.5.9, performs a symmetric eigendecomposition of the reduced matrix. The version is `packed-gram-v1`.

All six original row, dimension and requested-component presets remain unchanged. Their requested k exceeds half the dimension; the historical RowMatrix auto strategy therefore also uses a distributed Gram matrix and a local decomposition. The new full symmetric solver, serialization and action plan change timings; this is not a claim of binary or performance equivalence. Spark's [RowMatrix contract](https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/mllib/linalg/distributed/RowMatrix.html) documents the historical mathematical operation and strategies; validation runs use the installed 3.5.9 runtime.

The generator retains dense independent normal(0,1) features and the logical budget rows × dimensions × 8 bytes. Deterministic Java Gaussian partition streams with seed 42 replace clock-seeded RandomRDDs/Mersenne generation. This preserves the statistical profile, not historical row identity. Changing shuffle partitions changes streams. It reuses the already validated normal Dataset kernel; Parquet contains `id` and Spark ML `features`, with hidden seed/shape metadata. ObjectFile has no fallback.

SVD remains uncentered; an offset fixture explicitly distinguishes it from PCA. Eigenpairs are ordered by descending singular value, with at most min(requested k, rows, dimensions) retained. The cutoff is σ₀ × max(rcond, sqrt(dimensions × machine epsilon)). The roundoff floor avoids ghost components from a rank-deficient Gram matrix. Tiny still has 100 rows × 1,000 dimensions and requests 800 components; its numerical rank is 100. Zero matrices return rank zero. Gram methods cannot reliably resolve extremely ill-conditioned tails; the small-tail fixture verifies the documented truncation rather than claiming arbitrary precision.

The original U matrix was a lazy distributed multiplication and was neither written nor materialized. `computeU` keeps that contract; it constructs the Dataset plan A × V × S⁻¹. Numerical acceptance explicitly materializes U to test orthogonality and reconstruction. Workload timings do not include writing U or a fitted model.

Matrix dimensions are checked against the JVM array limit. `matrix_budget_bytes` bounds an estimate of the main packed/full/eigenvector/output arrays; it is not a guarantee of total JVM/native peak memory. Driver heap, Spark result-size limits and executor buffers still need sufficient capacity. Default budget is 2 GiB. Literal shell quotes around the original 1g/4g result-size values are removed without changing the intended values.

Independent NumPy direct-SVD tests cover tall, wide, offset, rank-deficient, repeated-value, zero, small-tail and truncated fixtures; compare singular values, V/U orthogonality and reconstruction or best-rank residual. Tests use invariant quantities, not singular-vector signs. Additional tests verify configuration budgets and every historical preset. Controller-generated tiny/custom matrices are separately checked against direct SVD; reference-only small-input collection is explicitly capped and is never used by workload execution.

See `ml-svd-validation-2026-10-09.json` and the migration ledger for final execution evidence. Larger presets retain their definitions; they are not certified by a tiny smoke run.
