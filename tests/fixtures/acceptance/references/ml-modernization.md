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

# Spark ML modernization

Current update (2026-10-09): all 24 batch catalog workloads and preparation paths are modernized without application RDD calls. See [SVD](ml-svd-modernization.md) and [NWeight](graph-nweight-modernization.md) for the final phase contracts and documented numerical/algorithm corrections. Certification is limited to recorded tiny/custom Docker/YARN tests; earlier dated inventories remain historical baselines.



The maintained workloads and generators must use SparkSession/DataFrame/Dataset APIs, without RDD API calls. Spark ML is supplied by the target Spark 3.5.9 runtime. The Maven artifact `spark-mllib_2.12` includes the modern `org.apache.spark.ml` API and remains a provided dependency; its name is not evidence of an obsolete API.

## ALS: ratings Parquet v1

The algorithm already used Spark ML ALS. Its generator and ObjectFile reader have now been replaced. There is no ObjectFile fallback or retained legacy ratings generator.

Input schema: `user INT`, `item INT`, `rating FLOAT`. Input paths now contain `ALS/ratings-parquet-v1/Input/<scale>` so a legacy dataset cannot be silently reused by the shell launcher. Controller reuse also fingerprints the actual JARs and configuration.

The generator uses one logical Dataset task for each configured shuffle partition, with the original Java Random seed equal to the partition index. Each task generates its original floor-sliced share of the record count. It emits rows lazily, permits duplicate user/item pairs, samples IDs uniformly and preserves explicit ratings 1–5 and implicit ratings -1.5 through 2.5. The configured rating count can now use Long; user/item IDs retain the Integer limit required by ALS. The historical `ratings <= users * items` constraint remains.

The six size presets are frozen in [ml-als-scale-reference.json](ml-als-scale-reference.json). Volume means number of ratings and user/item universes; Parquet bytes differ from historical ObjectFile bytes. Generation reads `hibench.default.shuffle.parallelism`, as the previous generator did; the separate historical `hibench.als.partitions` setting was not consumed by that generator.

The benchmark retains the 80/20 split with seed 1, existing rank/iterations/regularization/block settings, implicit preference mode and cold-start dropping. Model seed is now explicitly 0 for reproducibility. It reports the number of evaluable ratings and fails if that count is zero or RMSE is non-finite. RMSE is retained from the existing benchmark; in implicit mode it should not be interpreted as a full recommendation-quality assessment.

`tests/integration/verify_als.py` independently uses Java Random to check exact original row multisets for both modes, uneven partition counts and more partitions than records. It checks all six presets' counts, ID bounds and rating distributions, and verifies explicit/implicit Parquet round trips. Full cluster execution is recorded separately in the validation report; checking large generator counts is not a claim that all large training benchmarks were run.

Validation on 2026-10-08: Java 11 build succeeded, all 42 Python tests passed, and full tiny preparation/training completed in YARN in both modes. Each benchmark evaluated 23 ratings after cold-start filtering; implicit RMSE was 1.2902038838281749 and explicit RMSE was 2.95880545474169. See [the validation record](ml-als-validation-2026-10-08.json) for run IDs and applications. These are synthetic smoke checks, not accuracy targets for a recommendation product or certification of a serverless platform.

## Remaining ML workloads

ALS, KMeans/GMM, linear/logistic regression, RF/GBT, PCA/correlation/summarizer and
Bayes/LDA/SVM are migrated. See [the final phase contracts](ml-final-modernization.md)
for Bayes, SVM, LDA and PageRank. SVD now uses an uncentered Dataset Gram aggregation; NWeight now uses a corrected immutable DataFrame recurrence.
See [SVD](ml-svd-modernization.md) and [NWeight](graph-nweight-modernization.md) for their versioned contracts.
No RDD fallback is approved for the maintained modern suite.

## Linear regression: linear-parquet-v1

The Dataset generator and SparkSession Parquet reader preserve the original
dense linear synthetic model and all six row/dimension presets. The estimator
already used Spark ML and retains its configuration. Generation seed, label noise
and the existing excluded training fraction are now explicit experiment parameters.
See [the full migration contract](ml-linear-modernization.md) and
[validation receipt](ml-linear-validation-2026-10-08.json).

Validation on 2026-10-08: all 11 default modules built with Java 11 and 49 Python
tests passed. Four bounded fixtures matched the frozen original generator exactly.
The conditional noise/feature profile and Parquet round trip passed. Full tiny
preparation and training succeeded in YARN with 50,000 examples and 1,000 features;
37,451 training rows, five optimizer iterations, training RMSE 4.946521740211151
and R2 0.0940384157479559. Physical input including metadata was 400,962,056 bytes.
The existing regularization is retained; these metrics are not predictive quality
targets or held-out scores. A scheduler disconnect trace after metrics near
shutdown is preserved in the receipt despite both applications finishing SUCCEEDED.

## Logistic regression: logistic-parquet-v1 / binary-logistic-v2

Dataset generation preserves the original alternating labels and row-seeded
Gaussian class shift, including observed empirical deviations from ideal moments.
The modern Spark ML binomial estimator replaces the former ten-class MLlib setup
although only two labels existed. That correction is an explicit algorithm
revision; historical training cost is not claimed equivalent. See
[the full migration contract](ml-lr-modernization.md) and
[validation receipt](ml-lr-validation-2026-10-08.json).

Validation on 2026-10-08: Java 11 build of all 11 default modules and 52 Python
tests passed. Original tiny generation (100 rows, 1,000 features) and bounded
reference fixtures matched exactly. Full tiny preparation/training/evaluation
succeeded in YARN: 61 training rows, 39 held-out rows, 28 iterations and accuracy
0.7948717948717948. Actual input including profile metadata was 806,782 bytes.
A customized 1,000-row/eight-feature experiment with intercept, seed 17,
train fraction 0.7, split seed 23 and regularization 0.01 also succeeded:
718 training rows, 282 held-out rows, 15 iterations and accuracy 1.0.
These are synthetic execution checks, not general quality or speedup claims.

## Random Forest and Gradient Boosted Trees

Both now use Spark ML classifiers and a shared Dataset/Parquet generator,
preserving alternating labels, seed 42, the class shift 0.3, dense features and
every original scale. LR delegates to the same kernel with its own shift 3.0;
its historical data regression checks passed after extraction. See
[tree contracts](ml-trees-modernization.md) and
[validation receipt](ml-trees-validation-2026-10-08.json).

RF preserves its configured 100 trees, Gini/auto, depth 4 and bins 32, including
the legacy no-bootstrap rule for one tree. Its modern native probability
aggregation is explicitly versioned: four of 40 controlled predictions differ
from historical hard voting, while independent leaf-probability checks pass.
GBT preserves binary classification, 20 iterations, depth 30, bins 32,
step size 0.1 and its effective historical model seed 0. Controlled GBT
predictions matched exactly. Split seed 11 is newly explicit for both; RF's
formerly random model seed is now 1. Repeat fits passed.

Validation on 2026-10-08: all 11 default modules built with Java 11, 55 Python
tests passed, 25 data/algorithm checks passed and eight YARN applications
completed the original-tiny and customized campaigns. Each original tiny input
has ten rows and 100 features, exactly matching the old generators, with
14,297 physical bytes including metadata. Both trained on eight rows and
evaluated two; RF test error was 1.0 and GBT 0.5. Those two-row scores are not
quality estimates. Customized 1,000-row/eight-feature inputs trained on 718
rows and evaluated 282; each reported error 0.010638297872340425, using
explicitly different data/model parameters recorded in its experiment.
Actual generation checked all six GBT presets and RF tiny/small/large.
RF's 16–35 GB presets were definition checks only. No performance improvement
or cross-platform certification is inferred.

## KMeans and GMM: Gaussian Parquet v1

Both benchmarks use the modern Spark ML estimators with a Parquet `features` VectorUDT reader. Prepared samples also contain a Long `id` for validation; the algorithms select only `features`. The shared generator uses Dataset operations and lazy iterators. No maintained KMeans/GMM generator or reader uses MapReduce, Mahout, SequenceFile or an RDD API.

The generator preserves the historical synthetic profile: equally allocated Gaussian components, per-component/per-feature means uniform in [0,1000), and standard deviations uniform in [0.01,100). Remainders go to the first components. These ranges are now exposed as `mean_min`, `mean_max`, `std_min`, `std_max`, with seed 1234 by default. Random streams now use Java Random rather than the old nondeterministic MersenneTwister defaults: statistical equivalence, rather than identical rows, is the contract for this migration. GMM's training seed is now explicitly 1; KMeans retains its existing seed 1, initialization mode and tolerance 0. Existing iteration, k and storage settings are retained.

The six presets are preserved in [the clustering scale reference](ml-clustering-scale-reference.json), including KMeans bigdata's 1.2 billion samples and GMM bigdata's 200 components, 100 dimensions and 2 million samples. Generated component count and algorithm k are separate settings; tiny continues to generate 5 components and fit k=10.

`samples_per_inputfile` bounds generation chunks and maximum Parquet rows per file. Chunk streams are independent of Spark partition placement; generation partitions now follow `hibench.default.map.parallelism`. Final files may be smaller and their number can differ from MR outputs. IDs are now globally unique rather than restarting in each mapper. The conceptual per-sample payload remains one Long ID plus dimension Double values; Parquet encoding, compression and metadata change physical bytes and read costs.

The `cluster` sibling directory now stores typed distribution metadata: clusterId, startId, samples, mean and stddev. The old initial-centroid output was not used by these Spark estimators. Input paths include `gaussian-parquet-v1` to separate them from legacy data. Metadata is bounded at one million component/feature pairs; sample counts and chunk sizes use Long. No full input matrix is collected by the production generator.

Validation uses an independently run pre-migration JAR to freeze a conditional Gaussian reference. `tests/integration/verify_gaussian.py` checks standardized means, variances and tail frequency against that reference, every component/feature's moments, remainder cases, empty components, repeatability across partition counts, unique IDs, Parquet round trips and file row limits. Every preset is checked for planned allocation and dimensions; actual large-preset generation and training are not implied by these planning checks.

The old GenKMeansDataset production source and its dedicated tests have been removed after the profile checks. Mahout and Uncommons dependencies have been removed from autogen and the maintained ML module. The obsolete DAL variant was subsequently retired completely: its module, native integration, launchers and inactive Maven profiles were removed. It is not a supported option; historical references remain only as provenance. The historical reference runner under tests requires a pre-migration JAR and is not a retained executable workload generator.

Validation on 2026-10-08: all 11 default modules built with Java 11, 46 Python tests passed, and both full tiny preparation/training workloads succeeded in YARN (four Spark applications). Each trained on 30,000 three-dimensional vectors with k=10 and five iterations. KMeans cost was 327075503.64038146; GMM log likelihood was -528069.8452813178. Conditional Gaussian variance was 1.0018814, two-sigma tail frequency 0.0452929 and maximum absolute conditional covariance 0.0139477, within the documented statistical checks. See [the validation record](ml-clustering-validation-2026-10-08.json). Physical input bytes were 853,829 per workload including profile metadata; these are not historical SequenceFile throughput measurements.

The controller's actual prepared samples and distribution metadata were compared exactly with the new generator's reference output for both tiny workloads. This additional verifier uses a bounded 30,000-row collection; production generation remains distributed. An initial local distributed set-operation comparison stalled and was stopped; its cause is not established and is recorded in the validation notes.

## PCA, Correlation and Summarizer

All three now use typed Parquet and SparkSession/Dataset generation. PCA and Correlation retain Spark ML algorithms; Summarizer moves from RDD colStats to Spark ML aggregation. All six presets are preserved. Summarizer receives reproducible partition-seeded normal generation, replacing clock-seeded RandomRDDs; PCA/Correlation retain original row RNG formulas. See [contracts, measurement and wide-preset limits](ml-statistics-modernization.md) and [acceptance record](ml-statistics-validation-2026-10-08.json).
