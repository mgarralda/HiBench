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

# Logistic regression: logistic-parquet-v1 / binary-logistic-v2

## Original data profile and scale

Baseline Git revision: `e0ba3e97f467b5905f0ff8bd803ff8c9bc12241d`.
The original generator used RDDs and MLlib LabeledPoint/ObjectFile. Its actual
labels were `id % 2`, not Bernoulli draws. Its `probOne` argument was unused.
Features used Java Random (through Scala Random), seeded with `42 + id`, and
each feature was `nextGaussian() + label * 3`. Thus the shift was additive,
not the multiplicative scaling described by the old comments.

The Dataset replacement preserves that actual kernel, alternating binary labels,
dense Double features, default seed 42 and class shift 3.0. It adds Long IDs and
an explicit `class_shift` parameter; no ineffective class-probability option is
retained. Values are deterministic across partition counts because each row has
its own random stream. Odd row counts allocate the extra label to class zero.

Subsequent RF/GBT migration extracted this unchanged Dataset kernel into
`BinaryGaussianDataGenerator`. The LR entry point delegates to it with the same
arguments, application name, metadata and path. Its exact historical/profile
verifier passed again; see [the tree migration receipt](ml-trees-validation-2026-10-08.json).

The six historical scales are frozen in [the scale reference](ml-lr-scale-reference.json).
Feature payload means eight bytes per dense Double, excluding label, ID, storage
container and compression overhead:

| Preset | Examples | Features | Logical feature bytes |
| --- | ---: | ---: | ---: |
| tiny | 100 | 1,000 | 800,000 |
| small | 1,000 | 10,000 | 80,000,000 |
| large | 10,000 | 100,000 | 8,000,000,000 |
| huge | 10,000 | 300,000 | 24,000,000,000 |
| gigantic | 10,000 | 500,000 | 40,000,000,000 |
| bigdata | 10,000 | 700,000 | 56,000,000,000 |

Generation partition configuration now uses `lr.partitions`, defaulting to shuffle
parallelism, the value actually used by the former generator. Its old map-default
declaration was unused. Row counts now use Long; dimensions and partitions remain
positive Int. `seed + id` uses checked Long-range arithmetic instead of the old
Int addition. All historical presets are below that old overflow boundary and
retain exact row values; behavior beyond it is an extension, not an equivalence
claim about previously overflowing generation.

Schema: `id: long`, `label: double`, `features: VectorUDT` (dense). A hidden
`_generator_profile` directory stores seed, examples, dimensions, partitions,
classShift, zeroLabels and oneLabels. Algorithms select only label/features;
normal Parquet input discovery excludes the hidden profile. Input paths are
versioned. Legacy ObjectFiles require regeneration, without a retained production
RDD importer or generator. Isolated historical test sources are never packaged.

## Statistical fidelity includes observed original imperfections

The generation routine calls Gaussian draws, but deterministic consecutive row
seeds do not guarantee an empirical fixture with ideal independent normal moments.
The 20,001-row/eight-feature reference had first-feature means -0.2160822 and
2.7887374 for classes zero and one; first-feature variances 0.9374943 and 0.9353435.
Second-feature variances were 0.8818774 and 0.8758403. These observations also occur
in the isolated original implementation, and the new rows match it exactly.
The conditional moments, tails and covariances are preserved in the validation
receipt rather than silently changing random streams to meet a theoretical target.
Changing that profile would require a separate version and documented acceptance.

## Algorithm correction and modern API

The old `LogisticRegressionWithLBFGS` explicitly requested **ten classes** although
the generator supplied only zero and one. The new estimator is
`org.apache.spark.ml.classification.LogisticRegression`, explicitly binomial.
This retires eight absent classes. It is an intentional algorithm revision:
binary-logistic-v2, not an identical computational-cost substitution. The original
multinomial parameterization had nine coefficient vectors; the binary model has
one. Row count, dimensionality, density and classification signal are maintained,
but historical training times and coefficients cannot be interpreted as directly
comparable results. No duplicate legacy algorithm option is supplied.

Inspection of Spark 3.5.9 runtime bytecode confirmed the former effective defaults:
100 LBFGS iterations, tolerance 1E-6, regularization zero, feature scaling enabled
and intercept disabled. Those values are explicit defaults in the new estimator:
maxIter 100, tol 1E-6, regParam 0, elasticNetParam 0, standardization true,
fitIntercept false, probability threshold 0.5. The modern optimizer's internal
implementation is not asserted to be identical to the former multiclass solver.
See [old optimizer defaults](https://spark.apache.org/docs/3.5.9/api/python/reference/api/pyspark.mllib.classification.LogisticRegressionWithLBFGS.html)
and [modern estimator parameters](https://spark.apache.org/docs/3.5.9/api/python/reference/api/pyspark.ml.classification.LogisticRegression.html).

The 60/40 split with seed 11 remains. `train_fraction` and `split_seed` expose it
explicitly; DataFrame ordering/sampling can choose a different subset from RDD
randomSplit. Held-out accuracy retains its definition, correct predictions divided
by evaluation rows. Training requires both binary labels; empty evaluation fails
instead of dividing by zero. Training and evaluation DataFrames are cached and
unpersisted in finally; caching the evaluation split is new. A single aggregate
computes evaluation count/correct predictions rather than the former two RDD
actions. These changes are recorded measurement differences, not speedup evidence.

## Validation and reproduction

The bounded original tiny and two edge cases match the frozen original generator
exactly, including uneven and empty partitions. Partition-invariance, odd-count
class allocation, a custom seed/shift formula and Parquet round trips are checked.
The 20,001-row profile is compared exactly with the historical implementation and
its empirical conditional moments are recorded. All presets are checked against
their original row/dimension definitions; large-preset generation is not implied.
See [the validation receipt](ml-lr-validation-2026-10-08.json) for actual YARN runs,
input verification, application IDs, test logs, metrics and any limitations.

PowerShell, from the repository with the companion cluster already running:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File bin/build-java11.ps1
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/hibench.exe install configs/smoke-lr.yaml
.venv/Scripts/hibench.exe run configs/smoke-lr.yaml --wait
.venv/Scripts/hibench.exe run configs/smoke-lr-overrides.yaml --wait
```

For original-data comparisons, compile `LogisticRegressionOriginalReference.scala`
as an isolated test JAR with Spark's Scala compiler using the same procedure as
[the linear reference](ml-linear-modernization.md#reproduction-commands), replacing
the source/class directory/JAR names with logistic equivalents. Submit
`verify_logistic.py` with both that JAR and the current benchmark assembly JAR,
and a scratch Parquet output path. Submit `verify_logistic_controller.py` with
the isolated reference JAR and the emitted full tiny HIBENCH_INPUT_PATH; its
expected defaults are deliberately fixed to 100 rows, 1,000 features, seed 42,
shift 3 and two generation partitions. No original test JAR is needed for actual
benchmark execution.
