<!-- Copyright 2026 Mariano Garralda-Barrio (HiBench Next). -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# PySpark workload implementations

The v2.5.0 module implements all 24 catalog workloads using PySpark, with one
file per implementation. Repartition has separate disk and in-memory modules.
All generators remain in Scala. Existing workload parameters, scale presets,
input datasets, launch scripts, monitoring and Spark listeners are shared.

In Experiments, select **Scala**, **PySpark**, or **Scala and PySpark**.
The API and downloaded presets use `implementation: scala | pyspark | both`.
For example, import this through the existing experiment resolver or CLI:

```yaml
schema_version: 1
implementation: both
workloads:
  - id: micro.wordcount
    parameters: {}
```

`both` prepares once, then runs Scala and PySpark in that order for each
repetition. Each execution has separate outputs and reports. Scala uses the
selected deploy mode; PySpark uses Client. PySpark-only experiments require
Client. This is a HiBench policy: Spark itself also supports Python Cluster
deployment on YARN.

The default lives in `conf/hibench.conf` as
`hibench.workload.implementation scala`. Direct `bin/workloads/*/*/spark/run.sh`
calls support one implementation per invocation; the controller expands `both`.
Run the existing target installation after updating the checkout. The target
Spark distribution supplies PySpark; workers need compatible Python installations.
The Python namespace is distributed using `--py-files`; no Maven module is
required. The controller does not require PySpark. Spark ML workers need NumPy;
PySpark SVD additionally needs Pandas and PyArrow for streaming Arrow batches.
The tested versions are declared in [pyproject.toml](../pyproject.toml).

### Target Python dependencies

The controller installs its dependencies with `python -m pip install -e .`.
On the Spark submission host and executors, install the numerical dependencies
declared in `pyproject.toml` in the Python environment selected by Spark:

```sh
python -m pip install numpy==1.26.4 pandas==2.2.3 pyarrow==12.0.1
```

This command reflects the versions declared in the project metadata. The target
runtime was tested with Python 3.10/3.11; the controller requires Python 3.11+.
Targets do not need the full controller or a pip-installed PySpark distribution.

SQL/DataFrame operations execute mainly in Spark's JVM in both implementations.
Sleep performs one Python UDF wait per input task and includes Python worker
overhead. In-memory Repartition uses a SQL binary literal for the same 200-byte
payload and record count. Cached Repartition uses deserialized memory-and-disk
storage in both implementations. Shared configuration does not guarantee identical
physical plans or performance. Driver placement, fixed execution order and cache
warm-up must be considered when comparing results.

Modules are organized as follows:

| Namespace | Workload files |
| --- | --- |
| `pysparkbench.micro` | `sleep.py`, `sort.py`, `wordcount.py`, `terasort.py`, `repartition.py`, `inmem_repartition.py` |
| `pysparkbench.sql` | `scan.py`, `aggregation.py`, `join.py` |
| `pysparkbench.ml` | `als.py`, `kmeans.py`, `gmm.py`, `linear.py`, `lr.py`, `rf.py`, `gbt.py`, `pca.py`, `correlation.py`, `summarizer.py`, `bayes.py`, `svm.py`, `lda.py`, `svd.py` |
| `pysparkbench.websearch` | `pagerank.py` |
| `pysparkbench.graph` | `nweight.py` |

Shared input/output and numerical utilities live in `micro/io.py`, `sql/data.py`,
`ml/tree_data.py` and `ml/uncentered_svd.py`. `runner.py` imports the selected
module and manages the Spark session. `registry.py` maps the existing Scala
entry points to Python modules; generators are deliberately excluded.

SVD preserves the distributed packed Gram algorithm, input dimensions, array
budget, reciprocal-condition cutoff, numerical roundoff floor and lazy U contract.
It streams partition summaries through DataFrames/Arrow and merges them on a
single reducer; only the reduced matrix reaches the driver. NumPy replaces the
Scala BLAS/eigensolver, so transport cost, aggregation order, floating-point
roundoff and peak memory can differ. The budget estimates numerical arrays,
not total Python/Arrow/native memory. No input rows are collected to the driver.
SVD emits `HIBENCH_WORKLOAD_MEASUREMENT` with monotonic phase timers and NumPy
build configuration after Spark shutdown. The controller associates it with the
matching application ID and retains it in result JSON. Declared native thread
environment settings are recorded; actual native thread counts and effective CPU
limits remain unknown. No thread settings or materialization actions are added.

### Numerical provider alignment with Scala

Workloads use the target Spark distribution's PySpark SQL/DataFrame and ML APIs.
Most ML estimators delegate to the same JVM implementation used by Scala; the
controller does not install an independent PySpark engine. SVD is a custom
DataFrame/Arrow packed Gram implementation with NumPy local eigendecomposition.
Scala Breeze uses LAPACK `dsyev`, whereas NumPy `eigh` uses `dsyevd`. Matching
Spark versions and native providers does not eliminate this solver difference.

BLAS implements vector/matrix operations; LAPACK provides numerical solvers.
NumPy delegates linear algebra to compiled BLAS/LAPACK. The tested wheel reports
an OpenBLAS build; Scala/Breeze can use native OpenBLAS or a Java fallback.
Language comparisons therefore also depend on numerical providers.

Install OpenBLAS in the Spark target and verify Scala's native provider using
[sparkbench/README.md](../sparkbench/README.md). This does not replace OpenBLAS
bundled in NumPy wheels. Record both versions/builds and control native thread
limits and driver resources. Matching provider families does not guarantee
identical algorithms, integer ABIs or active thread counts. Preserve historical
fallback-provider measurements as a separate condition. Enabling Scala's native
provider requires no changes to workload algorithms or generators.

Tiny output parity against Scala, independent counts, sorted partition ranges,
disabled output, and malformed Tera records are checked by:

```bash
spark-submit --master 'local[2]' tests/integration/pyspark_micro_parity.py \
  "$PWD" /tmp/hibench-pyspark-parity-new
```

Non-micro groups have a separate Scala parity and independent-reference check:

```bash
spark-submit --master 'local[2]' \
  --jars sparkbench/assembly/target/sparkbench-assembly-2.5.0-dist.jar \
  tests/integration/pyspark_groups_parity.py "$PWD" /tmp/hibench-pyspark-groups-new
```

The scratch directory must be new. This test validates correctness on small
fixtures; it does not certify named data scales or performance.

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

Duration calibration measured Linear Small at about 65 seconds and Large at
326 seconds; PCA Small at 50–60 seconds and Large at 191–231 seconds on the
reference cluster. SVD has its own 50,000/500,000-row Small/Large ladder, with
1,000 features; its measured Large time is 40 seconds in Scala and 386 seconds
in PySpark. These are single-pair observations, not universal runtime limits.
Other native ML workloads share the input ladder without a duration guarantee;
see the scale contract for the exact measured scope and upper-scale volumes.
