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

# Maintained validation suite

## Automated Python checks

Run from the repository root using Python 3.11 or later:

```sh
python -m pip install -e ".[test]"
python -m unittest discover -s tests -p "test_*.py" -v
```

The default suite uses temporary state and mocks for submission. It does not
start a cluster or run benchmark campaigns. A Docker installation is not required.

For a focused check, select a test file, for example:

```sh
python -m unittest discover -s tests -p "test_spark_configuration.py" -v
```

The suite includes assertions about source/configuration contracts and historical
fixtures as well as executable Python behavior. Passing these checks does not
prove that every Spark workload produces correct numerical results. Keep dated
test counts in release receipts; the commands here apply to the current checkout.

| Area | Why it is retained |
| --- | --- |
| Workload and generator contracts | Historical volumes, dimensions, seeds, distributions, validation limits and DataFrame-only launch wiring |
| Configuration | Canonical conf/, scale precedence, stale-edit protection and explicit experiment overrides |
| Controller and adapters | Idempotent submission, leases, cancellation, recovery and safe command construction |
| REST API | Typed OpenAPI contracts, validation, authentication, lifecycle, export and paginated history |
| Web UI | Navigation, draft retention, disabled planned adapters and shared configuration save/update failures |
| Monitoring | Raw sample parsing, report integrity, partial collection and history links |
| Evidence attribution | Prevent shared campaign success or custom sizes from certifying unrelated named workloads |
| Deployment and licensing | Persistent mounts, packaging, batch catalog, shell portability and source notices |

`test_ml_contracts.py` replaces the misleading `test_pending_ml.py` name; these
workloads have been migrated. Frozen historical ledgers keep their original
paths. The evidence test reconstructs that historical path only in its temporary
fixture tree; the retired filename is not a runtime compatibility interface.

## JVM build and CI

With JDK 11 and Maven 3.9, compile all workload/generator modules from the root:

```sh
mvn -B clean package
```

On Windows, `powershell -File bin/build-java11.ps1` provides the Docker-based
build alternative. Maven currently has no JVM tests to run; build success checks
compilation and packaging, not numerical equivalence.

The [batch workflow](../.github/workflows/batch.yml) runs the Python suite,
builds Python packages, checks launcher help and compiles the Maven reactor on
Linux with Python 3.11 and Java 11. It does not execute a real Spark campaign.

## Explicit integration checks

`integration/` contains Spark/Docker checks and independent original-generation
references. They are not discovered by the default suite. RDD/old-library code
here is an isolated scientific comparator, never a supported workload runtime.
Keep it: removing it would weaken the evidence for migration fidelity.

Use the companion [Spark 3.5.9 release](https://github.com/mgarralda/hadoop-spark-cluster/releases#release-spark-3.5.9)
(`spark-3.5.9` tag): Spark 3.5.9 / Scala 2.12.18 / Java 11 / Hadoop 3.3.6.
The companion project's current Spark 4.2 stack is not supported by
this release. Individual scripts have different arguments and assumptions;
inspect the selected script before running it.

Run deployment acceptance only with Docker and an idle compatible companion
cluster. Build the current Compose image first; the script uses `--no-build`:

```sh
docker compose -f deployment/compose.yml build
python tests/integration/verify_deployment.py
```

This uses an isolated Compose project and alternate ports, runs canonical tiny
Sleep, verifies reports on the host, recreates the controller and checks persistent
configuration/history. It removes only its owned services and temporary marker.
It shares the checkout's `conf/` and `report/`, installs runtime files on the
target and leaves its run records/reports behind. Its separate Compose project
does not isolate the Spark cluster or host state. Confirm no other controller or
user is running benchmarks, and keep ports 18800 and 18851 available.

## Scientific campaigns and references

### v2.5.0 closure review (October 12, 2026)

The review passed 182 automated tests and compiled all nine Maven reactor
modules with Java 11. The Python wheel and controller Docker image were built
with version 2.5.0. Live Spark 3.5.9 checks passed Scala/PySpark output parity
for the 24 catalog workloads: five Micro workloads and 19 workloads in the
other groups, including independent numerical checks where supplied by the
integration scripts.

Browser checks visited all nine pages and submitted a two-repetition
Scala/PySpark Sleep experiment. A separate WordCount experiment was submitted
through the API. Both completed successfully. Comparison analysis, persistence,
OpenAPI responses and all five export formats were exercised; the browser ZIP
download contained JSON, two CSV files and an offline HTML report.

These are functional and small-data correctness checks. They do not establish
language performance differences, statistical equivalence, standard-scale
capacity, Spark 4.x support or fresh acceptance of every cloud/SSH adapter.
Detailed receipts, screenshots and raw logs are retained in the maintainer's
local `report/` directory, which is excluded from the public distribution.

The October 13 package reorganization passed 183 automated tests, including
all web navigation pages, API lifespan ownership and an isolated import of the
minimal runtime resources installed on targets. The rebuilt wheel was checked
for the new module paths and absence of retired modules. A temporary Docker
controller passed authenticated API health/catalog/OpenAPI checks and started
the supervised Streamlit process from `hibench/ui/app.py`. This follow-up did
not change workload implementations or repeat the scientific campaigns.

PySpark module checks are explicit Spark integration runs:
`integration/pyspark_micro_parity.py` compares Micro outputs with Scala;
`integration/pyspark_groups_parity.py` covers SQL, ML, graph and web search with
small fixtures, Scala parity and independent numerical/result checks. See
[execution commands and target Python dependencies](../pysparkbench/README.md).
These checks validate small-data correctness, not named scales or performance.

See [tools/README.md](tools/README.md) for scientific campaigns. Date-specific
recipes and receipts remain historical evidence, not shared configuration defaults
or release acceptance for a new revision. Unit tests, compilation, functional
integration and performance evidence are separate checks.

| Location | Purpose |
| --- | --- |
| `test_*.py` | Default automated Python and contract checks |
| `integration/` | Explicit live checks and independent Scala/Java/Python references |
| [fixtures/](fixtures/README.md) | Frozen contracts and historical evidence used by tests |
| [fixtures/experiments/](fixtures/experiments/README.md) | Explicit experiment choices for validation |
| [tools/](tools/README.md) | Campaign execution, output verification and evidence collection |

Report preparation/run success separately from input integrity, result correctness
and repeated timing evidence. Preserve seeds, partition counts, named preset
volumes and algorithm contracts; do not certify a standard size using reduced
inputs. Historical receipts identify their recorded runtime and artifacts, not
an untested rebuild.

## SVD phase confirmation

The initial SVD Large pair is an exploratory pilot, not confirmation of a
language effect. Record whole HiBench duration, Spark History application and
SQL/job intervals, and explicit `svd-phases-v1` workload timers separately.

| Timer | Boundary |
| --- | --- |
| `spark_startup_s` | Spark session builder to ready session |
| `read_plan_s` | `read.parquet` returns, including metadata work; not full data scanning |
| `gram_action_s` | Distributed Gram plan construction and action until its result reaches the driver; includes lazy input scanning, execution, transfer and decoding |
| `matrix_construction_s` | Validate/convert packed Gram and construct the dense driver matrix |
| `eigendecomposition_s` | Breeze `eigSym` or NumPy `linalg.eigh` call |
| `result_assembly_s` | Cutoff, rank selection and singular-value/right-vector assembly |
| `u_plan_s` | Construct/check optional lazy U projection; U is not materialized |
| `workload_s` | Read-plan start through numerical result and optional U-plan completion |
| `spark_stop_s` | Spark shutdown |
| `application_lifecycle_s` | Spark session builder through shutdown, excluding language-process launch/imports |

Timers use `System.nanoTime` / `time.perf_counter`. Totals contain smaller phases
and must not be stacked with them. No additional collect, persist, scan or U
materialization is introduced. `gram_action_s` does not separately measure
network transfer or exclusively executor time. Spark SQL durations use Spark's
own event boundaries and are not interchangeable with these explicit timers.
Whole HiBench duration additionally includes submission and process startup.

Result JSON stores timers under `workload_measurement`, bound to application ID.
Monitoring and Comparisons expose them; comparison bundles include `phases.csv`
and `phase-summary.csv` when present. Protocols are kept separate; missing
instrumentation is never treated as zero. Raw console logs remain available,
including singular values and the instrumentation marker. Legacy workload CSV
columns and historical scientific records remain unchanged.

The numerical metadata records the Scala BLAS/LAPACK provider classes and NumPy
build configuration, declared `OMP_NUM_THREADS`, `OPENBLAS_NUM_THREADS`,
`MKL_NUM_THREADS`, `VECLIB_MAXIMUM_THREADS` and `BLIS_NUM_THREADS`, configured
driver memory/cores, and visible logical CPU count. This does not certify active
native thread counts, library versions loaded by every executor, CPU affinity or
cgroup quotas. Effective native threads and effective driver cores are unknown;
verify them on the submission host before attributing a cause. Spark's configured
driver cores do not establish a limit on native numerical threads in Client mode.
Instrumentation does not change BLAS libraries or thread limits.

Use the same prepared input within Small and Large, YARN Client for both
implementations, matched Spark resources and monitoring, and an otherwise idle
target. Freeze target native-library/thread settings before the campaign. Export
a complete experiment JSON and preview the plan before starting:

```sh
python -m tests.tools.svd_phase_campaign experiment.json \
  --output report/svd-confirmation.json --plan-only
python -m tests.tools.svd_phase_campaign experiment.json \
  --output report/svd-confirmation.json
```

The default prepares once per scale, runs one excluded infrastructure/cache
warmup per implementation, then ten measured pairs per scale, alternating
Scala-first and PySpark-first. Each execution launches a fresh application;
warmup does not establish a persistent warm JVM or Python process. The controller
services retain independent run IDs, configurations, provenance and reports.
The manifest records roles, pair/order, metrics and pairwise singular-value
differences. Numerical acceptance is declared before execution: finite values,
equal retained counts, and componentwise `rtol=1e-6`, `atol=1e-8`. Numerical
disagreement and failed/cancelled jobs stop the campaign; output files are not
overwritten. `--scales large --pairs 1` is a validation pilot, not confirmation.

For correctness, compare all retained singular values and counts (default Small:
1,000; Large: 2,000), recording maximum componentwise relative difference. Values
alone do not verify eigenvectors or reconstruction. The tiny integration check
additionally materializes U to verify reconstruction; measured production runs
retain the lazy U contract. Review the declared tolerances before collecting
confirmatory data. Plot phase-specific distributions and total duration separately;
do not attribute residual time to the driver without instrumented evidence.

## Recalibrated scale contracts

`test_workload_scales.py` validates all 144 workload/scale configurations and
checks 10x growth plus fixed dimensions/model settings from Small upward.
Historical scale-reference fixtures are preserved as evidence; active defaults
come from conf, documented in [workload scales](../docs/workload-scales.md).
Selected real-pilot receipts distinguish input checks from full-scale capacity.

## Release validation

A functional release freeze adds no new workload features. Run the automated
suite and clean builds against the final source:

```sh
python -m pip install -e ".[test]" build
python -m unittest discover -s tests -v
python -m build
mvn -B clean package
python -m hibench.launcher --help
docker build -t hibench-next:2.5.0-validation -f deployment/Dockerfile .
docker compose --env-file deployment/.env -f deployment/compose.yml config --quiet
```

Select Java 11 explicitly if the host default differs. Inspect the source archive
for public documentation/configuration and ensure local attachments, scientific
exports, reports and secrets are absent. Start API/web on unused loopback ports
with separate control state. Verify health, OpenAPI version, catalog, a Tiny
Scala/PySpark execution, independent results, comparison saving, downloads and
configuration reuse. Preserve the receipt before stopping temporary services.
This smoke test is not a repetition of full-catalog numerical or performance
acceptance. Historical browser/cluster campaigns retain their original scope.

See [release notes](../CHANGELOG.md) and [configuration model](../docs/configuration-model.md).
