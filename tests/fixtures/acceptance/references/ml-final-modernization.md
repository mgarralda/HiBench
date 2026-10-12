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

# Bayes, SVM, LDA and PageRank modernization

Follow-up (2026-10-09): SVD and catalog NWeight are now migrated and validated. See [SVD](ml-svd-modernization.md) and [NWeight](graph-nweight-modernization.md). The deferred descriptions below record the earlier state; optional noncatalog GraphX PageRank/XGBoost and legacy utilities remain outside the modern certified suite.

Updated 2026-10-08. This phase preserves the historical workload profiles while removing maintained RDD, ObjectFile and MapReduce boundaries. Execution certification is limited to the configurations recorded in the validation receipt; it is not a cloud or serverless certification.

## Data and volume contracts

All six historical sizes are frozen in `ml-final-scale-reference.json` for ML, with PageRank sizes retained in its workload configuration. Dense data budgets are rows × dimensions × 8 logical bytes, excluding identifiers, labels and storage encoding. Sparse document data must instead be described by documents, token counts, vocabulary and nonzero entries. Parquet size is not the historical SequenceFile/ObjectFile size.

Bayes supports both historical profiles. Dense mode retains Java Random seed 1234 + row ID, uniform class selection and absolute Gaussian features shifted by class × 3. Document mode preserves the original mapper slots, seed ordering, truncated Gaussian document length (approximately 500 tokens), uniform labels and word Zipf exponent 0.9. The historical Zipf sample budget uses pages × 760, including its minimum valid corpus of 24,444 pages. A smaller arbitrary smoke corpus would fail the original generator too.

The historical dictionary declares 479,623 positions but contains only 99,171 lines. Its old reader fills positions past EOF with the literal word `null`. This behavior is preserved explicitly: replacing the missing positions would change the vocabulary distribution and benchmark signal. The original byte-identical dictionary is bundled in the shared module. This is a known historical data characteristic, not a newly introduced missing-value treatment.

Bayes document features retain document term multiplicity × global corpus word frequency. They are not TF-IDF. The vocabulary uses a deterministic lexicographic index instead of the historical unordered RDD index; this permutes feature positions without changing weights. Label indexing retains the default frequency-based StringIndexer semantics. Inputs are versioned Parquet with `id`, `label` and Spark ML `features`. Hidden generator profiles record the actual vocabulary dimension, mode, seed and partitions. The legacy ngrams parameter was unused and remains documented as such.

SVM preserves separating weights from Java Random seed 94720, row streams seeded by 42 + ID, uniform features in [-1, 1), and labels from the separating dot product plus Gaussian noise with standard deviation 0.1. Seeds and noise are now explicit parameters. Row counts and dimensions for every historical size are unchanged.

LDA preserves a discrete uniform document length between the configured bounds, uniform vocabulary draws and repeated-token counts in sparse vectors. These are uniform synthetic count documents, not planted latent topics. An explicit seed 42 replaces the clock seed. Partition streams use seed XOR partition; changing partition count changes generated records, so reproducibility requires recording both. All historical shape, length, topic and iteration presets remain unchanged.

PageRank preserves the historical HtmlCore link stream: Zipf exponent 0.5, approximately 40 links per page before flooring, truncated Gaussian page length, mapper slot partitioning and seed ordering. Duplicate links and self-links remain in generated data. Parquet contains separate `edges(src, dst)` and `vertices(id)` datasets and a generator profile. All six node-count presets remain unchanged.

## Workload changes

Bayes uses Spark ML multinomial NaiveBayes. The existing lambda parameter now reaches `setSmoothing`; previously it was read but ignored. The default remains 1.0. Training fraction 0.8 and split seed 1234 are explicit. Empty splits and invalid metrics are checked. Prediction display and accuracy evaluation remain; additional validation counts mean action plans are not timing-identical.

SVM uses Spark ML LinearSVC instead of SVMWithSGD. This is the versioned `linear-svc-v1` workload: optimization changes, so coefficients, iteration cost and historical timings are not claimed identical. Bytecode inspection of Spark 3.5.9 confirmed historical defaults of no intercept and no feature scaling; the new defaults therefore disable both intercept and standardization. The obsolete SGD step size is removed. The ROC evaluator uses numBins=0 to retain an unbinned metric. Solver tolerance, regularization, split seed and training fraction are configurable.

LDA uses Spark ML LDA and its model persistence API. Both online and EM remain selectable. Reloading now uses LocalLDAModel for online and DistributedLDAModel for EM; the previous always-local reload was wrong for EM. Model seed is explicit. Literal shell quotes were removed from optimizer and driver result-size values while preserving their intended values. Saved and loaded topic matrices are checked by the integration verifier.

PageRank uses DataFrame joins and aggregations for fixed iterations. It retains the former distinct-edge preprocessing and update rank = 0.15 + 0.85 × incoming contributions. It does not introduce dangling-node redistribution or global normalization. Vertices without contributions can disappear exactly as in the original algorithm; the separate generated vertex table records the entire input population but does not change that algorithm. Output is versioned Parquet instead of text.

The shared legacy Zipf kernel was extracted from the modern SQL module into common. Its implementation is unchanged apart from names and package. Native regression checks cover all eight original SQL profiles and three seeds (480,000 draws), Bayes profiles (360,000 draws), and PageRank profiles (18,000 link batches).

## Evidence and limits

The final Java 11 Maven build passed all eleven modules. Independent Spark 3.5.9 integration checks cover exact generator formulas across uneven and empty partition boundaries, 240 original Bayes documents sampled from a valid 25,000-page corpus, corpus weights, multinomial parameters, SVM ROC AUC and LDA online/EM save-load behavior. PageRank tests cover original edges, duplicates, self-links, dangling vertices and iterations 0, 1 and 3.

Original sources are retained as `.txt` provenance under `reproducibility/evidence/2026-10-08`, not as alternative production generators. The obsolete Bayes and PageRank MapReduce generators and SVMWithSGD entry point were removed. Physical I/O, feature indexing, added validation actions and changed solvers preclude an unqualified comparison of historical timing results. Larger presets are definition/profile checks unless explicitly recorded as executed.

## Deferred workloads

SVD is unavailable until an RDD-free distributed uncentered SVD implementation has a numerical contract and reference validation. PCA is centered and cannot silently substitute for SVD. Collecting the input to the driver is not an acceptable distributed benchmark. Retired production SVD sources are preserved only as provenance.

NWeight is unavailable until its GraphX and Pregel variants have reference fixtures covering weighted propagation, duplicate edges, top-K selection and ties. Its legacy sources remain pending review, but launchers and controller reject execution; there is no authorized RDD fallback. Noncatalog GraphX PageRank and legacy Nutch utility sources also remain outside the modern certified workload set.

The optional noncatalog XGBoost source still has an ObjectFile/RDD reader and an old external native dependency. It is not included in the default batch build or the 24-workload controller catalog. Its modernization requires a separate dependency/platform and algorithm-profile decision; this phase does not claim it is usable. Shared IOCommon retains legacy graph/other-module methods and is not used by the migrated ML or websearch workload paths. The no-RDD guarantee applies to the migrated application and preparation paths, not to every historical source file remaining in the repository or Spark's internal implementations.

See `ml-final-validation-2026-10-08.json` for the exact accepted campaigns, raw evidence and outstanding limitations. Future distributed SVD and NWeight work should extend this record rather than overwrite the historical comparison.
