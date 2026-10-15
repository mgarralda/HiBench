<!-- Copyright 2026 Mariano Garralda-Barrio (HiBench Next). -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Scala workload implementations

HiBench Next implements 24 Spark batch workloads in `org.hibench.sparkbench`,
grouped in `micro`, `sql`, `ml`, `websearch` and `graph`. These Maven modules
produce the assembly JAR. Scala generators are shared with
[PySpark](../pysparkbench/README.md).

## Numerical libraries

Workloads use Spark SQL/DataFrames and the `org.apache.spark.ml` estimators.
Spark dependencies are `provided`: the cluster supplies Spark, Scala, Breeze,
netlib, JSON and logging libraries. HiBench does not bundle replacements for
these libraries. Keep their versions aligned with the target Spark distribution.
The sole additional workload dependency is scopt 4.1.0 for ML argument parsing;
it does not implement numerical computation.

SVD retains its distributed packed Gram implementation: Spark 3.5 has no
equivalent public DataFrame SVD estimator preserving this workload's budget,
rank cutoff and lazy U behavior. Replacing it with a legacy RDD implementation
would change the workload contract. Breeze uses LAPACK `dsyev`; NumPy uses
`dsyevd`, so the local solver paths differ even with native OpenBLAS on both sides.

BLAS implements vector/matrix operations; LAPACK provides numerical solvers,
including eigenvalue decompositions. Breeze accesses them through netlib.
Java can use native OpenBLAS via JNI; without it, netlib falls back to Java
providers including F2j LAPACK. Updating the JDK alone does not enable OpenBLAS.

On Ubuntu/Debian Spark submission hosts and executors:

```sh
sudo apt-get update
sudo apt-get install --no-install-recommends libopenblas0-pthread
```

Install in the Spark target, not just the controller. Include the package in the
companion cluster image for persistence: running-container installations are lost
on recreation. HiBench target installation does not install system packages.

Verify providers in a fresh Spark process:

```scala
println(dev.ludovic.netlib.blas.BLAS.getInstance().getClass.getName)
println(dev.ludovic.netlib.lapack.LAPACK.getInstance().getClass.getName)
```

Native providers are `JNIBLAS` and `JNILAPACK`. SVD records these classes in
`HIBENCH_WORKLOAD_MEASUREMENT` alongside phase timers and declared thread settings.
See [netlib documentation](https://github.com/luhenry/netlib).

For controlled comparisons, apply the same native thread limits before starting
submission processes and propagate them to executors. A single-thread baseline
can use `OPENBLAS_NUM_THREADS=1` and `OMP_NUM_THREADS=1`; executor propagation
uses `spark.executorEnv.<VARIABLE>`. YARN Cluster drivers additionally need
`spark.yarn.appMasterEnv.<VARIABLE>`. Declared limits do not prove active counts.

HiBench also applies declared `spark.yarn.appMasterEnv` numerical thread limits
to Client submission processes. Scala and PySpark share the same driver settings
without shell-profile changes. Experiment overrides take precedence over the
shared scale configuration.

NumPy wheels can bundle a different OpenBLAS version and integer ABI. Matching
provider families removes the Java-fallback versus native-provider difference,
but does not equalize distributed plans, algorithms or transport. Record versions,
resources and numerical correctness. Preserve previous F2j results as a separate
experimental condition.

SVD uses a distributed packed Gram calculation followed by driver-local
eigendecomposition. The Gram timer includes lazy reading, aggregation and return
to the driver. U remains unmaterialized. See the
[confirmation protocol](../tests/README.md#svd-phase-confirmation).

## Shared scale baselines

`conf/spark.conf` is the authoritative resource and thread configuration.

| Scale | Executors | Driver heap | Declared native threads: driver / executor |
| --- | --- | --- | --- |
| Tiny | 3 × 1 core / 1 GB | 1 GB | 1 / 1 |
| Small | 2 × 1 core / 2 GB | 2 GB | 1 / 1 |
| Large | 2 × 2 cores / 2 GB | 2 GB | 4 / 1 |

These scales use Parquet batches of 256 rows, cached columnar batches of 128 rows
and file scan partitions of 64 MiB. They bound reader/cache batching without
changing generators or workload volumes; they do not bound all Spark ML internal
allocations. The validation master container has a four-CPU quota and workers
retain six CPU each. `spark.driver.cores` is not a Client CPU quota.

Earlier Linear Small exhausted executor heap and its old Large trial was
cancelled. Those runs used pre-recalibration data shapes; they are historical
evidence, not capacity results for the new presets. Cluster baselines remain
unchanged and do not guarantee capacity for every workload. New experiments
inherit conf; saved experiments preserve their explicit overrides.

## Recalibrated workload scales

See [the active v2.5.0 scale contract](../docs/workload-scales.md) for all families,
units and exceptions. The authoritative defaults are in conf/workloads; Scala
and PySpark consume the same values. Dense ML uses 100 rows x 100 features in
Tiny, then 400,000 rows x 1,000 features in Small and 4,000,000 in Large,
with 10x row growth thereafter
and fixed feature width. Generators and algorithm source code are unchanged;
the default experiment shapes intentionally differ from previous saved runs.
Historical SVD/PCA and regression pilots retain their original provenance and
must not be interpreted as measurements of these new defaults.

Selected recalibrated pilots passed in both languages: Linear Tiny/Small/Large,
SVD Tiny, KMeans Small and PCA Large. Independent input-volume checks passed.
See the scale contract for the validation scope; larger-scale capacity is not
implied by the scale names.

Duration calibration measured Linear Small at about 65 seconds and Large at
326 seconds; PCA Small at 50–60 seconds and Large at 191–231 seconds on the
reference cluster. SVD has its own 50,000/500,000-row Small/Large ladder, with
1,000 features; its measured Large time is 40 seconds in Scala and 386 seconds
in PySpark. These are single-pair observations, not universal runtime limits.
Other native ML workloads share the input ladder without a duration guarantee;
see the scale contract for the exact measured scope and upper-scale volumes.
