# Linear regression: linear-parquet-v1

## Historical boundary and preserved model

The historical workload already used `org.apache.spark.ml.regression.LinearRegression`.
Its generator and ObjectFile reader still used RDD APIs. The maintained implementation
now generates a Dataset and reads Parquet using SparkSession. The old production
generator has been replaced in place, without a compatibility launcher.

The frozen reference is Git revision `57473c3f8829d86f90d4e42d489f1c66eaf2b5f0`,
path `sparkbench/ml/src/main/scala/org/hibench/sparkbench/ml/LinearRegressionDataGenerator.scala`.
An isolated copy of its generation method lives under `tests/integration/` solely
for independent historical verification. It is not compiled into production JARs.

For dimension d, one coefficient vector is generated with Java Random, seed 42:
`w[j] ~ Uniform[-0.5, 0.5)`. Each example has independent dense features
`x[j] ~ Uniform[-1, 1)`, mean 0, variance 1/3. Labels are
`y = sum(w[j] * x[j]) + eps * Gaussian(0, 1)`, with eps=1 by default.
The original documentation described the noise mean incorrectly: eps is its
standard deviation, not its mean. The new `noise_std` parameter reflects this.
The conditional regression signal, density, noise and floating-point summation
order are retained. Labels are continuous, not classification labels.

One logical generation stream is allocated per partition. Partition i receives
IDs `[floor(n*i/p), floor(n*(i+1)/p))` and RNG seed `seed XOR i`, matching
the original range slicing. Scala Random delegated to Java Random, so the new
Java Random preserves its uniform/Gaussian streams. Changing seed, feature count
or generation partition count changes the dataset. Repeating all these settings
reproduces row values; changing only physical file layout is not a new data profile.

## Sizes, representation and configuration

All six original row/dimension presets remain unchanged. The logical feature
payload uses dense Double values, eight bytes per feature; labels add eight bytes
per row. The new Long `id` adds another eight bytes per row for validation and is
excluded from algorithm input. These are logical sizes, not measured Parquet or
historical serialized file sizes.

| Preset | Examples | Features | Logical feature bytes |
| --- | ---: | ---: | ---: |
| tiny | 50,000 | 1,000 | 400,000,000 |
| small | 100,000 | 20,000 | 16,000,000,000 |
| large | 200,000 | 30,000 | 48,000,000,000 |
| huge | 300,000 | 50,000 | 120,000,000,000 |
| gigantic | 500,000 | 80,000 | 320,000,000,000 |
| bigdata | 1,000,000 | 100,000 | 800,000,000,000 |

The former `linear.partitions` property was declared against map parallelism but
unused: the generator actually read shuffle parallelism. It now controls the
generator and defaults to `hibench.default.shuffle.parallelism`, preserving the
previous effective default. Experiment resource `shuffle_partitions` therefore
controls this default; custom property overrides can explicitly change it.
Examples now use Long; feature and partition counts remain positive Int.
No large preset generation is implied by a preset consistency test.

Input schema: `id: long`, `label: double`, `features: VectorUDT` (dense).
The hidden `_generator_profile` Parquet directory contains one record with
seed, examples, dimensions, partitions, noiseStd and the coefficient vector.
It is excluded from normal input discovery and directly readable for validation.
Versioned input paths separate this dataset from legacy ObjectFiles. Old data
must be regenerated; automatic ObjectFile import is not provided.
Parquet encoding/compression and VectorUDT storage alter physical bytes and
read cost. Historical end-to-end timings cannot be treated as identical-format
measurements, despite preserved synthetic model and logical scale.

## Algorithm behavior

The benchmark selects only `label` and `features`. It retains ElasticNet 0.8,
regularization 0.3, 50 maximum iterations, tolerance 1E-6, Spark's `auto` solver,
default standardization and intercept fitting. Solver auto may select different
methods according to dimension; it is deliberately not forced to a new solver.
See [Spark 3.5.9 LinearRegression documentation](https://spark.apache.org/docs/3.5.9/api/python/reference/api/pyspark.ml.regression.LinearRegression.html).

The original 75/25 random split with seed 12345 is retained. `test_fraction`
now exposes the existing fraction, default 0.25. The excluded rows are not
evaluated: existing RMSE, R2, objectives and residual samples remain **training**
metrics. New file/partition ordering can change the selected training subset;
identical historical coefficients or metrics are not claimed. Training is cached,
counted before fitting and unpersisted afterward. Empty training and nonfinite
training RMSE fail explicitly. Correcting the numeric schema for tolerance allows
scientific-notation numeric overrides to reach the launcher.

## Verification

`tests/test_linear.py` verifies unchanged presets/logical volumes and parameter
validation. `tests/integration/verify_linear.py` compares bounded datasets exactly
against the isolated original Scala generator, including uneven partitions,
empty partitions, different seeds and zero noise. A separate Java Random/formula
reference checks IDs and numerical values. A 20,001-row, eight-feature fixture
checks feature ranges/moments, residual noise mean/variance and feature-noise
covariance, using fixed tolerances declared before executing the checks. Parquet
round-trip checks verify that the new storage preserves generated values.

The integration verifier collects only bounded fixtures; production generation
does not collect the generated dataset. The full tiny Docker/YARN experiment and
its actual data/profile validation are recorded in
[the validation receipt](ml-linear-validation-2026-10-08.json).
No cloud/serverless certification or performance improvement is inferred.

## Reproduction commands

From the repository in PowerShell, with the companion cluster already running:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File bin/build-java11.ps1
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/hibench.exe install configs/smoke-linear.yaml
.venv/Scripts/hibench.exe run configs/smoke-linear.yaml --wait
.venv/Scripts/hibench.exe run configs/smoke-linear-overrides.yaml --wait
```

The second experiment uses 101 rows, nine dimensions, seed 17, zero noise,
zero regularization and no excluded rows. Recovering the known linear relation
with near-zero training RMSE verifies that customized parameters reach generation
and fitting. It does not replace the full original tiny experiment.

To run the historical reference checks, compile the isolated test source using
the Spark distribution's Scala compiler (the paths below match the tested cluster):

```powershell
docker cp tests/integration/LinearRegressionOriginalReference.scala spark-cluster-master:/tmp/LinearRegressionOriginalReference.scala
docker exec spark-cluster-master bash -lc 'mkdir -p /tmp/linear-reference-classes && java -Dscala.usejavacp=true -cp "/usr/local/spark/jars/*:/home/sparker/HiBench/sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar" scala.tools.nsc.Main -d /tmp/linear-reference-classes /tmp/LinearRegressionOriginalReference.scala && jar cf /tmp/linear-original-reference.jar -C /tmp/linear-reference-classes .'
docker cp tests/integration/verify_linear.py spark-cluster-master:/tmp/verify_linear.py
docker exec spark-cluster-master /usr/local/spark/bin/spark-submit --master 'local[2]' --driver-memory 1g --conf spark.ui.enabled=false --jars /home/sparker/HiBench/sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar,/tmp/linear-original-reference.jar /tmp/verify_linear.py hdfs://spark-cluster-master:9000/HiBench-Validation/linear-contract
```

For actual controller input verification, copy `verify_linear_controller.py` to
the container and submit it with the same local Spark runtime and the full tiny
`HIBENCH_INPUT_PATH` emitted by that run. The verifier intentionally asserts the
tested tiny defaults (50,000/1,000, two generation partitions, seed 42, noise 1).
It must not be used to certify arbitrary configurations without adapting its
explicit expected values.
