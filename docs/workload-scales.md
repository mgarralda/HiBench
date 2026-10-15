<!-- Copyright 2026 Mariano Garralda-Barrio (HiBench Next). SPDX-License-Identifier: Apache-2.0 -->

# Workload scales in v2.5.0

The active defaults are defined only in conf/workloads/*.conf. This document explains the recalibration; it is not an alternate configuration source. UI, API and CLI resolve those same files. Generators stay in Scala and algorithms are unchanged. Scala and PySpark share all workload scale values.

Scale means increasing input volume within a family, not equal execution time, physical file size or memory consumption across all 24 algorithms. Small through Bigdata grows by 10x in the primary input count. Tiny is an explicit smoke-test shape. Dimensions and model complexity stay fixed from Small upward.

## Active input ladder

| Family / workloads | Tiny | Small | Large | Huge | Gigantic | Bigdata |
|---|---|---|---|---|---|---|
| Sort, WordCount: logical text budget | 32 KB | 320 MB | 3.2 GB | 32 GB | 320 GB | 3.2 TB |
| TeraSort, Repartition from HDFS: 100-byte records | 320 | 3,200,000 | 32,000,000 | 320,000,000 | 3,200,000,000 | 32,000,000,000 |
| Dense native ML rows (Linear, LR, SVM, RF, GBT, PCA, Correlation, Summarizer) | 100 | 400,000 | 4,000,000 | 40,000,000 | 400,000,000 | 4,000,000,000 |
| Dense ML dimensions | 100 | 1,000 | 1,000 | 1,000 | 1,000 | 1,000 |
| Dense native ML feature doubles only | 80 KB | 3.2 GB | 32 GB | 320 GB | 3.2 TB | 32 TB |
| SVD rows (same 100/1,000 feature widths) | 100 | 50,000 | 500,000 | 5,000,000 | 50,000,000 | 500,000,000 |
| SVD feature doubles only | 80 KB | 400 MB | 4 GB | 40 GB | 400 GB | 4 TB |
| KMeans/GMM samples | 1,000 | 50,000 | 500,000 | 5,000,000 | 50,000,000 | 500,000,000 |
| KMeans/GMM dimensions | 3 | 20 | 20 | 20 | 20 | 20 |
| SQL requested visits (all three queries) | 1,000 | 100,000 | 1,000,000 | 10,000,000 | 100,000,000 | 1,000,000,000 |
| SQL requested pages (all three queries) | 120 | 12,000 | 120,000 | 1,200,000 | 12,000,000 | 120,000,000 |
| PageRank pages | 50 | 5,000 | 50,000 | 500,000 | 5,000,000 | 50,000,000 |
| Bayes documents | 25,000 | 30,000 | 300,000 | 3,000,000 | 30,000,000 | 300,000,000 |
| LDA documents | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
| ALS ratings | 2,000 | 20,000 | 200,000 | 2,000,000 | 20,000,000 | 200,000,000 |
| ALS users/items each | 100 | 1,000 | 10,000 | 100,000 | 1,000,000 | 10,000,000 |
| NWeight requested pairs | 10,000 | 100,000 | 1,000,000 | 10,000,000 | 100,000,000 | 1,000,000,000 |
| Sleep seconds per task | 1 | 3 | 10 | 30 | 60 | 90 |

Units are decimal KB/MB/GB. Dense estimates exclude labels, IDs, vector encoding, model, solver workspace and cached copies. Parquet compression and runtime representation can change physical bytes substantially. SQL visits can be removed by link-coverage joins; actual rows must be measured. PageRank generates approximately 40 links per page. NWeight writes two directed edges per pair and uses a fixed feature universe.

## Shape decisions and exceptions

- PCA keeps k=10. SVD requests 50 components in Tiny and 100 thereafter, with 100/1,000 features; the old 4,000-feature Large experiment remains a distinct historical condition. Matrix budgets are still checked independently of data volume. Fixed width limits quadratic state growth, but larger row counts still increase distributed Gram work.
- KMeans/GMM keep 5 generated clusters, model k=10 and 5 maximum iterations. From Small upward they use 20 dimensions and 10,000 samples per generator chunk; this avoids silently changing random-stream chunking with each scale.
- Bayes remains document-based for every scale, with 10 classes. Its legacy Zipf dictionary generator requires at least 24,444 documents, so Tiny retains 25,000 rather than forcing a smaller invalid input. Inactive dense-mode presets follow the dense ladder if explicitly selected. The old ngrams setting is not an effective argument to the current generator; no ngram-complexity claim is made.
- LDA preserves the existing 50..10,000 token length distribution; vocabulary is 1,000 in Tiny and 2,000 thereafter, topics 10/20. Sparse vector volume is governed by nonzeros rather than dense matrix bytes.
- ALS uses 20 sampled ratings per requested user/item cardinality. Generator sampling can repeat pairs; users/products increase with ratings so the number of latent entities still grows. This is not a fixed user-item density or a guarantee of 20 observed ratings per individual.
- Sleep has no external dataset and does not follow a byte or 10x ladder. Task count comes from generation_partitions; total programmed task-seconds equals tasks times seconds.
- Repartition in-memory preserves records **per generation partition** and 200 bytes per record. Logical payload is datasize x generation_partitions x 200, not datasize x 100. At 14 generation partitions, Small is 8.96 GB and Large 89.6 GB. Use an explicit datasize override when targeting a smaller total; the default mode remains fromhdfs=true.

## Reproducibility and dataset reuse

This is an intentional change of benchmark defaults, not historical input equivalence. Pre-recalibration runs must be compared using their saved effective parameters and provenance, not the scale label alone. Existing contract fixtures retain historical values; they are no longer assertions of active presets. The pre-recalibration review is archived locally in `docs_internal/workload-scales-review.md`; its matrices are preserved in `report/workload-scale-audit-before.{json,csv}`. These are historical evidence, not public configuration guides or alternate active defaults.

The existing dataset identity includes effective parameters and generator settings. Changed data-generating presets therefore require a matching new prepared dataset; no old data is relabeled. Explicit saved experiment overrides continue to define their own conditions. To use the active base, create a new experiment without old workload overrides. The target must receive updated conf via Update target or hibench install.

## Validation scope and timing calibration

All 144 workload/scale combinations must pass shared config contracts, monotonic volume and stable shape checks. Unit/contract tests and selected real generation/execution pilots validate the change. Large Linear and PCA pilots use the unchanged general Large cluster resource baseline. Detailed receipts are maintained under report/recalibrated-* and report/workload-scale-recalibration-*.

A successful selected pilot does not certify all 24 workloads or Huge/Gigantic/Bigdata on the development cluster. The large named scales deliberately retain substantial benchmark volume; resources must be selected for the workload and environment.

### Measured timing calibration

The first volume-only ladder produced roughly 25-second Linear/PCA runs at Large.
It was replaced after sequential Scala/PySpark pilots at 10,000, 1,000,000 and
4,000,000 rows with the same Large resource baseline. Small was separately
checked at 400,000 rows with its Small baseline. No delays or iteration changes
were introduced to achieve a timing target.

| Workload / condition | Rows x features | Scala seconds | PySpark seconds |
| --- | --- | ---: | ---: |
| Linear Tiny (smoke) | 100 x 100 | 25.301 | 25.283 |
| Linear Small | 400,000 x 1,000 | 65.348 | 65.378 |
| Linear Large | 4,000,000 x 1,000 | 325.748 | 325.843 |
| PCA Small | 400,000 x 1,000 | 60.297 | 50.345 |
| PCA Large | 4,000,000 x 1,000 | 190.586 | 230.588 |
| SVD screening point (Large resources) | 100,000 x 1,000 | 30.323 | 100.431 |
| SVD Large | 500,000 x 1,000 | 40.283 | 385.885 |

Reference cluster: Spark 3.5.9 / Java 11, YARN Client, master CPU quota 4,
driver heap 2 GB. Small allocates 2 executors x 1 core / 2 GB and one declared
native driver thread; Large allocates 2 executors x 2 cores / 2 GB and four
declared native driver threads. Executor native threads are one. Resource
baselines and generator/algorithm code are unchanged.

Approximate one-minute Small and 3–10-minute Large durations are practical
calibration goals for this reference environment, not enforced runtime limits.
SVD keeps a lower common input volume because its Python/Arrow aggregation has
a different cost; its optimized Scala execution remains much faster. All
implementations keep the same dataset and workload parameters.

Linear job interval unions grew from about 7 seconds at the old 10,000-row
Large to 304–308 seconds at 4 million rows. PCA job unions grew from 8–10 to
173–210 seconds. These are merged Spark job wall intervals, not summed task
times or executor-only phases. SQL alone omits some ML jobs. The increase is
substantial distributed activity, rather than a scale distinction hidden by
common launch overhead.

All 144 configurations and 195 Python tests pass. Numerical checks cover Linear
coefficients/RMSE, PCA explained variance and SVD singular values. Every
calibration application succeeded. Single Scala-first pairs establish
functional/timing evidence, not statistical performance confirmation.

The earlier six-dataset independent input checks and first volume-only pilots
are preserved as historical evidence in report/workload-scale-recalibration-*
files. Current timing evidence: report/duration-calibrated-defaults-validation.md
and report/duration-calibration.png. Changing defaults does not relabel those
earlier runs.

The dense-family row ladder is transferred to the other native ML algorithms as
an input-volume policy; their new durations have not been measured. SVD Small
is a volume extrapolation from its measured Large shape. Remaining families and
Huge/Gigantic/Bigdata have configuration validation, not a full duration/capacity
campaign. Dense Bigdata is 32 TB of feature doubles; select resources and storage
for the workload and environment. No universal wall-time guarantee is implied.

Final input checks independently verified row counts and complete unique ID ranges
for the five final calibration datasets. Scala/PySpark used the same path for
each pair. Feature width was sampled as 1,000 against the fixed generator shape;
this check did not rescan every feature element. Target installation was updated
and doctor passed after saving the defaults.
