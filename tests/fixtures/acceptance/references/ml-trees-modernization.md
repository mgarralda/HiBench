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

# Random Forest and GBT modernization

Data contract: `binary-gaussian-parquet-v1`. Algorithm implementations:
`spark-ml-rf-soft-vote-v1` and `spark-ml-gbt-v1`. Original Git baseline:
`810a9b74723580b2c7f8ef5d86fa4bd2d503f3ef`.

## Preserved generation kernel

Both original generators were identical except their application/class names.
They alternated labels with `id % 2`, constructed Java Random through Scala
Random with seed `42 + id`, and generated each dense feature as
`nextGaussian() + label * 0.3`. The unused `probOne` argument and comments
describing probabilistic labels/multiplicative scaling did not reflect execution.
The replacement preserves the actual alternation, RNG streams, additive shift
and dimensionality. The positive class shift is **0.3**, not LR's 3.0.

`BinaryGaussianDataGenerator` now implements this shared Dataset kernel for
LR, RF and GBT. Workload entry points retain their own class names and pass
their explicit arguments/application names to it. LR's data values, metadata,
Parquet path and generator signature remain unchanged; its verifier is rerun
after the extraction. No old production RDD generator or ObjectFile importer
is retained. Historical test adapters are excluded from Maven/runtime assembly.

Schema: `id: long`, `label: double`, `features: VectorUDT` (dense).
The hidden `_generator_profile` Parquet directory stores seed, examples,
dimensions, partitions, classShift and exact counts of each label. Normal
Parquet discovery excludes that directory. Odd row counts add the extra example
to label zero; generation partition counts do not affect row values.

The effective historical generation partition default was shuffle parallelism;
the declared per-workload map-partition property was unused. It now controls
generation and defaults to shuffle parallelism. This fixes the unused setting
without changing its effective default. Long row counts extend the former Int
range; checked Long seed addition replaces overflowing Int arithmetic outside
the old range. Exact historical equivalence is asserted for original presets
and tested bounded fixtures, not previously overflowing counts.

Consecutive row seeds produce the same empirical first-feature deviations noted
for LR. Exact comparisons retain these original imperfections, rather than
silently replacing streams to manufacture ideal Gaussian moments. The stored
conditional means/variances document the observed reference profile. A future
change to the RNG profile requires its own version and acceptance evidence.

## Unchanged presets and volume

The [scale reference](ml-trees-scale-reference.json) freezes every original
example count and dimension. Feature payload is `rows * dimensions * 8` bytes,
excluding the Double label, added Long ID, storage overhead and compression.

| Preset | RF rows × dimensions | RF feature bytes | GBT rows × dimensions | GBT feature bytes |
| --- | ---: | ---: | ---: | ---: |
| tiny | 10 × 100 | 8,000 | 10 × 100 | 8,000 |
| small | 100 × 500 | 400,000 | 100 × 500 | 400,000 |
| large | 1,000 × 1,000 | 8,000,000 | 1,000 × 2,000 | 16,000,000 |
| huge | 10,000 × 200,000 | 16,000,000,000 | 1,000 × 4,000 | 32,000,000 |
| gigantic | 10,000 × 300,000 | 24,000,000,000 | 1,000 × 8,000 | 64,000,000 |
| bigdata | 20,000 × 220,000 | 35,200,000,000 | 1,000 × 12,000 | 96,000,000 |

The original tiny contains only ten examples. It tests execution, not scalability
or statistically reliable predictive quality. We deliberately do not enlarge it
under the existing preset name. Separate customized validation experiments are
clearly named and do not replace the reference scale.
Versioned input paths separate Parquet from legacy ObjectFiles, which must be
regenerated. Physical bytes/read costs differ; logical-volume preservation is
not historical serialized-file-size or throughput equivalence.

## Algorithm parameters and measurement

RF now uses `org.apache.spark.ml.classification.RandomForestClassifier`.
The configured defaults remain 100 trees, two classes, featureSubsetStrategy
auto, Gini impurity, depth 4, maxBins 32 and subsampling rate 1.0.
Runtime inspection established that legacy RF disabled bootstrap for one tree
and enabled it for multiple trees. The modern implementation explicitly retains
that condition. RF supports configured class counts >=2 through nominal label
metadata; generated data remains binary. No categorical feature indexing is
introduced: all generated features remain continuous.

RF's prediction aggregation changes with the modern API: the legacy model used
majority voting over each tree's predicted class, whereas Spark ML averages leaf
class probabilities and selects the highest combined probability. The controlled
five-tree fixture had four different predictions out of 40 held-out rows despite
matching leaf-probability references. That is an explicit algorithm revision,
not a data-generation error. The native modern aggregation is retained; no
custom legacy-vote compatibility path is added. Single-tree predictions matched.

GBT now uses `org.apache.spark.ml.classification.GBTClassifier`, preserving
classification rather than substituting a regressor. Defaults remain exactly
two classes, depth 30, maxBins 32, 20 boosting iterations and learningRate 0.1
(mapped to stepSize), logistic loss, all features and subsampling rate 1.0.
GBT's old public constructor had effective training seed **0**, retained as the
explicit default `model_seed`. RF's formerly implicit random training seed is
now explicitly **1**. Seeds can be customized independently from generation.
See the official [RF API](https://spark.apache.org/docs/3.5.9/api/java/org/apache/spark/ml/classification/RandomForestClassifier.html)
and [GBT parameters](https://spark.apache.org/docs/3.5.9/api/python/reference/api/pyspark.ml.classification.GBTClassifier.html).

Both retain the 70/30 training/evaluation fractions. Previously randomSplit had
no seed; `split_seed=11` now makes the split reproducible under the same layout
and runtime. DataFrame sampling/file ordering may select different rows; identical
historical metrics, learned trees or runtime are not promised.
Neither workload adds explicit input caching. A bounded training summary checks
nonempty data and integer labels in the configured class range. Test error still
means incorrect held-out predictions divided by evaluation rows, now computed
in one aggregate rather than two RDD actions. Empty evaluation fails explicitly.
Materializing the training summary is a new measured action. Default tree-engine
internals can still use RDDs inside Spark; maintained workload/generator code does
not expose those APIs.

Small training splits may reduce effective maxBins below the requested value,
as Spark's tree engine does automatically. We preserve the configured value and
record that behavior instead of silently rewriting workload defaults.

## Validation scope and reproduction

`tests/test_trees.py` checks every preset, logical feature bytes, modern capabilities
and valid/invalid parameters. Isolated original Scala generators verify exact
rows including odd counts, uneven/empty partitions and partition invariance.
The 20,001-row/eight-feature fixture checks the original empirical joint profile.
Custom seed/shift and Parquet round-trip checks cover parameter and storage
boundaries. Actual generation of RF tiny/small/large and every GBT preset checks
row count, dimensions and label allocation; RF huge/gigantic/bigdata are planning
checks only. The latter 16–35 GB inputs are not claimed generated or trained.

A controlled 200-row/four-feature fixture uses identical training/test rows and
continuous metadata to compare modern probabilities against a reference computed
from historical RF leaf confidences (five trees and one tree), and modern GBT
predictions against legacy GBT predictions with five iterations. RF's four hard-vote
differences are recorded; GBT's controlled predictions matched exactly. Repeat fits test
prediction reproducibility. This is a bounded algorithm regression check,
not a claim of universal numerical equivalence or equal execution cost.
Full original tiny and separately customized YARN experiments are recorded in
[the validation receipt](ml-trees-validation-2026-10-08.json), including resources,
applications, actual input checks, logs and limits.

PowerShell, with the companion cluster already running:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File bin/build-java11.ps1
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/hibench.exe install configs/smoke-trees.yaml
.venv/Scripts/hibench.exe run configs/smoke-trees.yaml --wait
.venv/Scripts/hibench.exe run configs/smoke-trees-overrides.yaml --wait
```

Compile the three historical Scala adapters under `tests/integration/` into an
isolated JAR using the Spark Scala compiler as described in
[linear reference reproduction](ml-linear-modernization.md#reproduction-commands).
Submit `verify_trees.py` with that reference JAR and the current assembly JAR;
arguments are the scale-reference JSON and a scratch Parquet URI.
Submit `verify_trees_controller.py` with the reference JAR and a JSON list of
the two actual full-tiny input URIs. Submit the existing `verify_logistic.py`
with its own historical reference JAR to verify unchanged LR behavior after
extracting the common kernel. Original adapters never participate in benchmark
submissions. No cloud/serverless certification follows from these checks.
