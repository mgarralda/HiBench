# Remaining batch workload modernization strategy

Source review: 2026-10-08, current upgrade working tree. This is a migration proposal, not runtime certification. No algorithms or generators were changed during this review. User requirement: maintained workload and generator code must not use RDD APIs; a classic-only RDD exception is not acceptable.

Implementation update after the review: ALS, KMeans and GMM are now migrated; see [ML modernization](ml-modernization.md), [ALS validation](ml-als-validation-2026-10-08.json) and [clustering validation](ml-clustering-validation-2026-10-08.json). The inventory below describes the original review baseline.

## Scope and inventory

The maintained catalog contains 24 workloads. Micro (5) and SQL (3) have completed their migration; the remaining catalog scope is ML (14), graph.nweight and websearch.pagerank. Additional launchers exist for graph.pagerank, optional ml.xgboost and dal.kmeans; these need an explicit disposition rather than being silently omitted or enabled.

| Workloads | Current execution / input | Proposed migration | Relative effort |
| --- | --- | --- | --- |
| ml.als | Spark ML ALS; RDD ObjectFile ratings | Typed user/item/rating Parquet and SparkSession generator | Low |
| ml.kmeans, ml.gmm | Spark ML estimators; Mahout SequenceFile readers; shared MR generator | Shared Gaussian-mixture generator and features Parquet reader | Medium |
| ml.linear, ml.pca, ml.correlation | Spark ML algorithms; ObjectFile/RDD readers and generators | Preserve algorithms; replace generation and readers | Low–medium |
| ml.summarizer | RDD Statistics.colStats; normal-vector generator | Spark ML Summarizer and features Parquet | Low–medium |
| ml.lr, ml.rf, ml.gbt | RDD MLlib estimators and ObjectFile | Spark ML LogisticRegression, RandomForestClassifier, GBTClassifier | Medium |
| ml.bayes | Spark ML / Parquet benchmark; dense Spark RDD generation or MR text plus conversion | Preserve both dense and document-frequency profiles; SparkSession generation | Medium–high |
| ml.lda | RDD LDA and sparse count-vector ObjectFile | Spark ML LDA; documentId/features schema; explicit optimizer settings | Medium |
| ml.svm | SVMWithSGD and ObjectFile | LinearSVC candidate; algorithm revision because optimizer changes | Medium |
| ml.svd | RowMatrix.computeSVD and normal-vector ObjectFile | Separate numerical design decision; no direct estimator replacement | High / decision |
| websearch.pagerank | Custom RDD iteration; MR web-link generator | DataFrame edge generation and iterative joins/aggregations | Medium–high |
| graph.nweight | GraphX/Pregel, internal GraphImpl; RDD weighted-graph generator | Typed weighted edges and explicitly specified DataFrame propagation | High |

Spark's RDD MLlib API is in maintenance mode, not removed. The Maven artifact spark-mllib also supplies Spark ML: retain it as provided. Remove Mahout only after all readers, generators and optional consumers are audited.

## Data contract before migration

Freeze the resolved values of all six size presets for each migrated workload before editing its generator. Store logical rows, dimensions, nonzero counts, class/cardinality parameters and generation settings, not just output bytes. Use Long row counts and streaming partition iterators; avoid driver-sized datasets or one materialized partition array.

Use versioned Parquet contracts: features as Spark ML VectorUDT; label Double for supervised datasets; user and item Integer plus rating Float for ALS; documentId Long for LDA; src/dst Long and optional weight Double for graphs. Generator metadata must record profile version, seed, dimensions, logical counts, partitions and physical encoding. Dataset reuse must distinguish these versions. Superfluous labels may remain as metadata when historically generated, but are not input features.

Changing ObjectFile/SequenceFile/text to Parquet is accepted for HiBench Next. Preserve workload scale and meaningful data characteristics; do not claim historical physical read throughput is comparable across formats. Existing inputs require an explicit import/conversion boundary rather than implicit RDD fallback in the new benchmark.

## Profiles that must survive

- ALS: user/item cardinalities, configured rating count, uniform draws, allowed duplicate pairs. Explicit ratings are 1–5. Historical implicit ratings are -1.5, -0.5, 0.5, 1.5 and 2.5; do not silently make them all positive. The original seed depends on partition index.
- KMeans/GMM: a mixture of Gaussian clusters; means and per-dimension standard deviations are sampled from configurable uniform ranges. Preserve cluster allocation, dimension and total samples including rounding/remainders. The default original generator is nondeterministic; expose a reproducible seed and compare statistical profiles. Sparse Mahout storage does not imply genuinely sparse synthetic data.
- Linear: features uniform in [-1,1], shared random weights and additive Gaussian noise scaled by eps. Preserve the label/features relationship, not just their marginal distributions.
- LR/RF/GBT and correlation: binary label-dependent Gaussian feature shifts; preserve class balance and eps separation. Existing generators share this profile and can share a kernel while keeping workload contracts independent.
- PCA: dense Gaussian features shifted by -0.5, with a separately generated Gaussian label. The eps argument does not control the inspected feature generation; mark ineffective configuration rather than inventing new behavior.
- Summarizer/SVD: dense standard-normal matrices. Validate shape and column moments; their current time-based seeds require statistical rather than byte-for-byte comparison.
- SVM: shared Gaussian separating weights (seed 94720), uniform [-1,1] features and Gaussian decision noise scaled by 0.1, with binary labels. Retain difficulty and noise.
- LDA: sparse document count vectors from uniform vocabulary draws and bounded document lengths; repeated draws accumulate counts. Preserve token totals, lengths, vocabulary and sparsity. This generator is not an existing latent-topic generative model.
- Bayes: maintain two distinct profiles. Dense features are nonnegative shifted Gaussian values conditioned on class. Text mode includes the historical document/class/word-frequency behavior and conversion; preserve its vocabulary and tokenization/hash/indexing semantics before retiring the MR path. Label reindexing by StringIndexer must also be specified.
- PageRank: retain the original web-link generation kernel and Zipf destination skew, node count, degree distribution, duplicate/self-link behavior and partition balancing. SQL's related HTML kernel can help but SQL output is not a substitute graph. Audit raw and distinct edge counts independently.
- NWeight: fixed 2,401,080-ID universe and bundled vertex-feature resource. Edge weights derive from the product of rank-one vertex features and are rounded to four decimals in the original text. Each sampled pair expands into both directions: configured pair count and stored directed edge count differ by a factor of two. Self-pairs are rejected; duplicates may survive across generation rounds. Preserve weighted degree and multiplicity profiles rather than replacing weights with independent uniform values.

## Algorithm and measurement contracts

Keep estimator parameters explicit: seeds, train/test fraction, iteration limits, regularization, initialization, convergence, evaluation and persistence. Spark ML defaults are not assumed to match MLlib defaults. GBT is classification in the inspected code; retain its binary classification constraint and report invalid class settings. Replacing SVMWithSGD with LinearSVC preserves the task but changes the solver: publish a new algorithm version and validate quality, not identical coefficients or runtime.

PageRank currently deduplicates edges, initializes ranks to 1 on source vertices and runs fixed iterations of 0.15 + 0.85 * incoming contributions. Its joins determine which vertices survive; it does not implement generic dangling-mass redistribution. Preserve this contract first. GraphX PageRank has its own semantics and must not be used as an unverified drop-in replacement.

NWeight is weighted path propagation with per-round top-K retention and self-target exclusion. The queue breaks equal-weight ties by vertex ID. GraphxNWeight starts with top-K outgoing neighbors and iterates from round 2; PregelNWeight initializes identity entries and uses Pregel scheduling. Establish small reference graphs for both variants, including duplicate edges, isolated vertices, ties, empty messages and round 1, before deciding whether to preserve both modes. A DataFrame join/group/window implementation may create substantial intermediate data; bound it according to the same top-K contract and measure representative sizes.

SVD has no direct Spark ML estimator corresponding to the current distributed RowMatrix.computeSVD workload. The user rejects a classic-only RDD exception. A fully RDD-free replacement requires a separate distributed numerical design and benchmark version; if that cannot be validated, mark SVD unavailable in the modern catalog until it is implemented. Do not substitute PCA or collect the input matrix to the driver and call that equivalent SVD. There is no approved RDD fallback.

Separate generation, import and benchmark timing. Force lazy transformations through meaningful evaluation or output actions. Record input metadata, algorithm version, effective Spark configuration, iterations, evaluation metrics and output counts/model artifacts. Avoid timing only construction of an unevaluated plan.

## Recommended delivery sequence

1. ALS: establish the first ML schema, metadata and reference validation with a self-contained migration.
2. KMeans and GMM together: replace their shared generator/readers and retire the old MR/Mahout path once optional consumers are addressed.
3. Linear, PCA, correlation and summarizer: complete reader migrations and shared numerical generation kernels.
4. LR, RF and GBT: migrate estimators and preserve classification difficulty and settings.
5. Bayes and LDA: preserve document semantics; then SVM with an explicit solver version.
6. Websearch PageRank: modernize its generator and algorithm as one unit; decide the status of the extra graph.pagerank launcher.
7. NWeight: migrate after reference graphs and variant semantics are established.
8. SVD: resolve the numerical strategy separately; implement and validate a distributed DataFrame-based design or explicitly mark the workload unavailable. Do not retain an RDD execution option.

For each delivery: original small fixtures or statistical baseline, all-preset configuration validation, new generator/profile tests, workload result/quality checks and a real Spark 3.5.9 cluster run. Larger-size runtime claims require actual execution, not extrapolation. Retire the previous production generator and launcher references only after these checks; retain provenance, metadata and small reference fixtures. Shared legacy classes can be removed only after the last remaining consumer migrates.

Optional XGBoost (current profile dependency 1.0.0), DAL/native code and the extra GraphX PageRank launcher require separate dependency/platform reviews. DataFrame APIs alone do not certify Databricks serverless, Fabric or another managed runtime: estimator, custom JVM code, library installation, storage and submission support remain platform-specific checks.

## Primary references

- https://spark.apache.org/docs/3.5.7/api/scala/org/apache/spark/mllib/index.html
- https://spark.apache.org/docs/3.5.7/api/scala/org/apache/spark/ml/index.html
- https://spark.apache.org/docs/3.5.7/api/scala/org/apache/spark/ml/classification/LinearSVC.html
- https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/mllib/linalg/distributed/RowMatrix.html
- https://spark.apache.org/docs/3.5.7/graphx-programming-guide.html

These API references establish the relevant Spark 3.5 API families; the local target for runtime validation remains Spark 3.5.9.

## Clarification: library version and RDD boundary

Use org.apache.spark.ml APIs from the target Spark 3.5.9 runtime, with matching Spark dependencies. Spark ML is bundled with Spark, not an independently upgradeable library whose latest major release can be mixed with a Spark 3.5 cluster. The no-RDD requirement applies to maintained application and generator code, including readers and evaluators. Internal implementation details of Spark ML are a different boundary; DataFrame APIs do not by themselves certify a managed serverless platform.
