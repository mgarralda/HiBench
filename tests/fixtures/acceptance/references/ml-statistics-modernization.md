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

# PCA, Correlation and Summarizer modernization

Status: Java 11 compilation, bounded numerical references and all original tiny YARN workloads passed; custom controller acceptance is recorded in the validation receipt. Do not treat this document as a performance comparison.

## Preserved data contracts

All six historical example counts and feature dimensions remain unchanged (see `ml-statistics-scale-reference.json`). Logical dense feature payload is `rows * dimensions * 8`, excluding labels, IDs, metadata and storage framing. Parquet replaces Java ObjectFile; physical bytes and read timings are not claimed equivalent.

PCA retains Scala Random's Java Gaussian sequence at seed `42 + rowId`: consume a Gaussian label first, then one Gaussian per feature, subtracting 0.5. The former `eps` and `probOne` arguments did nothing and have been removed. Seed is now explicit and configurable. Correlation retains alternating 0/1 labels and row-seeded Gaussian features shifted by `label * 3.0`, using the same proven binary Gaussian kernel as LR. Neither operation includes the label in its feature calculation.

Summarizer retains dense independent normal(0,1) vectors, exact row and dimension budgets and no sparsity. Historical `RandomRDDs.normalVectorRDD` used a wall-clock seed. The replacement uses lazy Dataset partition streams with `java.util.Random(seed XOR partitionId)` and Gaussian sampling, seed 42 by default. This is distributional equivalence, not historical row identity. Changing partitions changes Summarizer rows; changing partitions does not change PCA/Correlation rows. The hidden `_generator_profile` records each effective seed, shape and partition count; Summarizer also identifies its RNG version.

The old generators are replaced in production. The only historical RandomRDDs call is an isolated verification reference under `tests/integration`, excluded from assembled production artifacts. Input paths include format/profile versions and cannot silently reuse old ObjectFiles. Generation uses configured shuffle parallelism, which was the effective historical setting; formerly unused map-parallelism declarations have been corrected.

## Algorithms and measurement

PCA retains `spark.ml.feature.PCA`, component count and driver max-result-size setting. Its reader is direct Parquet. Literal quotes around historical maxresultsize values were removed: the launcher passed them into Spark and caused executor creation to fail (for example the byte parser received `"1g"` instead of `1g`). The numeric size budgets remain unchanged. Fit remains the measured action; the historical lazy transform was never materialized, so this migration deliberately does not add a transform pass. Explained variance is now reported. Correlation retains `spark.ml.stat.Correlation`, caching, count and matrix materialization with Pearson or Spearman. Misleading label-correlation and hardcoded Pearson messages are corrected; cache is released.

Summarizer replaces `spark.mllib.stat.Statistics.colStats` with one `spark.ml.stat.Summarizer` aggregation requesting count, mean, sample variance, max, min, nonzero counts, L1 and L2 norms. The original colStats summary maintained these statistics even though only mean/variance/max/min were printed. Those four remain the reported vectors, with row count added. All sessions close in finally blocks. Maintained workload and generator sources contain no RDD/SparkContext/ObjectFile boundary. Spark ML algorithms may use RDD internally; this does not establish compatibility with every managed/serverless product.

## Preset execution limits

Preserving a historical preset does not certify it executable. Correlation materializes a dense square matrix: tiny is 8 MB, small 3.2 GB, large 7.2 GB, huge 20 GB, gigantic 51.2 GB and bigdata 80 GB for matrix values alone. Additional working memory is required. Installed Spark 3.5.9 RowMatrix covariance has a 65,535-column guard; a dense JVM array also cannot represent more than a signed Int number of elements. Gigantic/bigdata therefore cannot run unchanged with this implementation; huge also exceeds the single-array element budget. They remain inspectable historical definitions, not certified runnable scales. A distributed or block correlation would be a separate versioned algorithm change.

PCA also has wide-matrix resource and solver constraints. Only tested dimensions are certified here; large presets require explicit resource/solver evaluation. Summarizer has no square result, but its historical bigdata input still represents 800 GB of logical feature values. No large-volume execution is implied by a small reference test.

## Verification

`tests/integration/verify_statistics.py` checks exact PCA/Correlation historical formulas, odd and underfilled partition counts, deterministic replay, partition behavior and Parquet round trips. Bounded PCA eigenspaces/explained variance, Pearson/Spearman correlations and all eight summary metrics are compared with NumPy. Summarizer modern and fixed-test-seed historical generators are checked for means, variances, two-sigma tails and cross-feature covariance. Historical runs never had a fixed seed; the test seed is an explicit reference fixture.

Configuration unit tests verify every preserved preset and reject invalid counts, dimensions, component counts, seeds and correlation methods. `configs/smoke-statistics.yaml` runs original tiny definitions through the shared experiment controller on Spark 3.5.9/YARN. Acceptance results and raw evidence are recorded in `ml-statistics-validation-2026-10-08.json`.

Official API reference: https://spark.apache.org/docs/3.5.7/ml-statistics.html (installed version verified separately as 3.5.9).
