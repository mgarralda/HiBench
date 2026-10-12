<!--
Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

# HiBench Next

![HiBench Next — Reproducible benchmarks for Apache Spark](hibench/ui/assets/hibench-next-banner.png)

## Reproducible benchmarks for Apache Spark. ##

This modernization is **HiBench Next v2.0.0**. Version v1.0.0 identifies the
author's earlier doctoral evolution of HiBench. All migration and validation
work recorded in this development campaign, including its initial tests,
belongs to v2.0.0. Earlier temporary build version strings in receipts retain
their original values for traceability. Detailed lineage and campaign records
remain in the maintainer's local documentation.

HiBench Next is a substantial modernization of Intel's HiBench for current
Apache Spark batch benchmarking. It renews **both dataset generators and
workload implementations**, corrects documented defects, replaces application
RDD boundaries with **SparkSession, DataFrames and Datasets**, and adds a shared
experiment controller accessible through a **web interface, REST API and CLI**.
The result is an open-source suite of **24 batch workloads** with explicit data
contracts, readable configuration and traceable execution records.

This is a community fork, not an official Intel release. The reference
environment is Spark **3.5.9**, Scala **2.12.18**, Java **11** and Hadoop **3.3.6**.
The controller and launch helpers use **Python 3**; the controller requires
**Python 3.11+**.

## What has changed

| Area | HiBench Next modernization |
| --- | --- |
| Workloads and generators | Maintained production code uses SparkSession/DataFrame/Dataset APIs, without application RDD calls. Replaced generators no longer require the old MapReduce autogen pipeline. |
| Machine learning | Modern `spark.ml` estimators, feature vectors and statistical APIs replace legacy application `spark.mllib`/RDD readers and generation paths. Workloads that already used `spark.ml` retain it; SVD and NWeight have documented custom implementations. |
| Data loading | Explicit text, fixed-record or Parquet contracts replace legacy ObjectFile/SequenceFile boundaries where migrated. SQL runs without a Hive metastore. |
| Correctness and reproducibility | Documented generator profiles, seeds, record counts, feature dimensions and partition-dependent random streams; independent reference checks and explicit algorithm corrections. |
| Python tooling | Python 2 launch/configuration/monitoring helpers migrated to Python 3, alongside a packaged Python controller with `pyproject.toml`. |
| Web interface | Streamlit pages for experiments, shared configuration, workloads, infrastructure, live monitoring and durable run history. |
| REST API | FastAPI with Pydantic v2 input/output contracts, interactive OpenAPI documentation and shared configuration, experiment and run services. |
| CLI | Validate, resolve, install, diagnose, submit, inspect, cancel and export experiments programmatically. |
| Configuration | One authoritative `conf/` base, common Spark properties and explicit settings for six data sizes, editable workload parameters and portable YAML/JSON experiment overrides. |
| Execution and reports | Detached workers, persisted run state, cancellation/reconciliation, logs, provenance, resource graphs, HTML reports and downloadable evidence bundles. |
| Deployment | Docker Compose starts API and UI together; host-mounted configuration and reports survive recreation and connect to an existing Spark cluster. |

### Corrected behavior, not only updated dependencies

The modernization addresses concrete limitations of the earlier implementations:

- **Reproducible synthetic data:** clock-seeded or nondeterministic generation
  is replaced with explicit seed contracts where needed. Historical logical
  volume and representative distributions remain the target; exact row identity
  is preserved where verified, and statistical equivalence is stated otherwise.
- **Usable PCA settings:** literal quotes in historical memory settings could
  reach Spark's byte parser and prevent executors from starting. Those values
  are corrected without changing the defined dataset dimensions.
- **Correct NWeight propagation:** captured legacy GraphX/Pregel references
  exposed mutable vertex state and inconsistent propagation. The DataFrame
  implementation defines weighted-path propagation and top-K behavior explicitly,
  with independent checks instead of reproducing the defects.
- **Numerical and configuration boundaries:** SVD rank/conditioning behavior,
  empty evaluation cases, dimensions, seeds and effective generator settings
  are checked or documented rather than silently accepted or ignored.

Changes and tests are recorded in the migration ledger (local record),
statistics corrections (local record),
NWeight contract (local record) and
SVD contract (local record).

### Preserve the benchmark's meaning

The goal is to retain representative Spark batch workloads while making the
suite reproducible, understandable and extensible. Generator migrations account
for **logical record volume, dimensions, distributions, seeds and workload
behavior**. A switch to Parquet or text can change physical bytes and decoding
cost, and algorithm corrections can change outputs: historical elapsed times
are therefore not automatically comparable to the new suite.

Streaming and alternate framework implementations are removed to keep one
coherent Spark batch execution and validation model. New workloads can build on
the same configuration, execution and evidence services rather than introducing
another independent launch system.

## Scope and workload catalog

| Family | Workloads |
| --- | --- |
| Micro | Sleep, WordCount, Sort, TeraSort, Repartition |
| SQL | Scan, aggregation, join |
| ML | ALS, KMeans, Gaussian mixture, linear/logistic regression, Random Forest, Gradient Boosted Trees, PCA, correlation, summarizer, Naive Bayes, Linear SVM, LDA, SVD |
| Graph / web search | NWeight, PageRank |

Application workloads and generators use SparkSession/DataFrames/Datasets rather
than application RDD calls. ML uses modern Spark ML APIs where an estimator or
statistical API exists; custom numerical/graph implementations have explicit
contracts. SQL uses Parquet and does not require a Hive metastore. Spark's own
internal use of RDDs is outside this application-level requirement.

Streaming, alternate execution frameworks, DAL, the old MapReduce autogen and root common modules,
uncatalogued MapReduce PageRank and the optional legacy XGBoost profile are removed.
The active catalog remains 24 workloads. Compatibility with the old development
configuration/schema is not maintained. Generator volume, distributions, seeds,
partition-dependent streams and algorithm corrections are documented, including
format changes that prevent direct comparisons with historical timings.

## Start with Docker Compose

Docker with Compose and Git are the only host prerequisites for this deployment.
The image build compiles the benchmark JARs from source using Java 11 and Maven
inside Docker, and installs the Python dependencies. No local Java, Maven or
Python installation is required. Compiled JARs are generated artifacts and are
not included in the Git repository.

Start the companion [hadoop-spark-cluster Spark 3.5.9 release](https://github.com/mgarralda/hadoop-spark-cluster/releases#release-spark-3.5.9)
first, using its `spark-3.5.9` tag for the validated Spark 3.5.9 / Scala 2.12.18 /
Java 11 / Hadoop 3.3.6 environment. The companion project's current Spark 4.2
stack is a future migration target and is not supported by this release.
Configure the existing container/storage paths in `conf/`, then:

```sh
cd deployment
cp .env.example .env
# Set a generated HIBENCH_API_TOKEN in .env; never commit it.
docker compose --env-file .env -f compose.yml up -d --build --wait
```

Open http://127.0.0.1:8501 (UI) and http://127.0.0.1:8000/docs (API). The API
requires the token. In Parametrization, use **Save and update target** to save
the shared configuration and install HiBench on the target selected in Experiments.
From the CLI, use `hibench install experiment.json` before the first benchmark.
This stack starts API and UI together and connects to the already installed
Spark cluster. Host `conf/` and `report/` bind mounts retain configuration and run
records when the service is recreated. HDFS data/event-log persistence belongs to
the companion cluster. See [deployment details](deployment/README.md), including
backup, trusted-local Docker socket access and port settings.

## Run from a source checkout

Build with JDK 11 and Maven 3.9; on Windows the supplied PowerShell build uses Docker:

```sh
mvn -B clean package
# Windows alternative: powershell -File bin/build-java11.ps1
python -m venv .venv
# Activate your virtual environment.
python -m pip install -e .
python -m hibench.server --with-ui
```

Or use the CLI:

```sh
hibench defaults --scale small > experiment.json
hibench validate experiment.json
hibench install experiment.json
hibench doctor experiment.json
hibench run experiment.json --wait
```

The implemented execution adapters are **Existing Docker**, **SSH** and **Local Linux**.
SSH connects to an existing Linux submission host, including the companion Docker cluster, using an authorized key/agent or `HIBENCH_SSH_PASSWORD` from the controller environment, with verified host keys. An explicit private key path takes precedence over the environment password. Credentials remain outside experiment files; only their controller-side paths are configured.
Livy, EMR, Dataproc, HDInsight, Synapse, Fabric and Databricks are visible as
planned destinations, not functioning integrations. DataFrame migration alone
is not certification for serverless platforms or provider-specific restrictions.

## Configure and reuse experiments

`conf/` is the single authoritative configuration base:

- `spark.conf`: common native Spark properties and explicit sections for tiny, small, large, huge, gigantic and bigdata. All workloads use the selected size's Spark settings. Initial resource values are conservative existing defaults, not measured sizing recommendations.
- `hibench.conf`: benchmark/launch defaults, generator partitions and monitoring.
- `hadoop.conf`: storage endpoint and Hadoop installation.
- `conf/workloads`: workload-specific sizes, generator distributions/seeds and algorithm settings.
- `benchmarks.lst`: default workload selection.

**Parametrization** separates Spark configuration, HiBench configuration and
Workloads. Inspect and edit each workload's size definitions, generator controls
and algorithm parameters, with effective values and parameter help. **Save and
update target** saves the canonical files and updates the selected destination;
a failed connection preserves the local save and offers a retry. Shared changes
are blocked during active runs.

**Experiments** separates execution target from workload selection, operation,
size, repetitions and resources. It applies run-only overrides and exposes an
optional execution group ID; workload/size metadata is generated automatically.
Advanced Spark properties include examples and help. Native `spark.*` keys can be
added to a size or an experiment; precedence is common base, selected size,
explicit override. Generator partition counts stay separate because they can
change random streams. No runtime recipe/configs directory is needed.
Download an editable preset from Runs, or edit a recorded run as a new experiment;
import the preset to reuse a selection of multiple workloads. The API exposes
the same shared services, configuration editing, resolution, submission and exports.
See configuration model (local record) and
API guide (local record).

## Three interfaces, one execution model

- **Web:** configure an experiment, prepare data and run workloads, inspect
  progress/logs/resource graphs, open historical runs and download reusable presets.
- **REST API:** integrate configuration inspection/editing, validation/resolution,
  submission, status, logs, cancellation and exports into external tooling. Inspect
  the Pydantic request/response contracts in [Swagger UI](http://127.0.0.1:8000/docs),
  [ReDoc](http://127.0.0.1:8000/redoc) and the [OpenAPI schema](http://127.0.0.1:8000/openapi.json).
  Use **Authorize** when HIBENCH_API_TOKEN is set. These URLs use the default API port.
- **CLI:** run the same experiment services from scripts or a terminal using the
  `hibench` command. The CLI example above includes installation and diagnostics.

All three use the shared controller and domain validation. Benchmarks execute in
workers outside HTTP requests and Streamlit reruns, while recorded configurations
and provenance remain associated with their run IDs.

The API includes shared Spark/HiBench configuration, workload sizes and parameters,
profiles, target diagnostics/installation, cluster operations, experiment import
resolution, runs, logs, metrics, monitoring samples/artifacts, reusable presets,
exports and Spark History links. Run history accepts `limit` and `offset`. Full submission models require explicit
resources and monitoring: obtain them from `GET /experiments/defaults` or resolve
an imported override through `POST /experiments/resolve`. The API does not invent
a second set of resource defaults.
`PUT /configuration/*` saves locally; `POST /diagnostics/install` updates the
selected target. Use both for the web's Save and update target workflow. Installation
is rejected while queued or active runs exist; a failed target update leaves the
local configuration saved. The comparison API describes configuration differences,
not statistical performance equivalence; its web view remains planned.

The web uses shared Python services directly; it does not require internal HTTP
requests to itself. **About** provides version, attribution, license notices and
API links. Generator parameter changes can change random streams and dataset
meaning; retain those settings in exported experiment conditions.

## Results and reproducibility

Runs provides durable history, console output, application IDs, metrics, resource
graphs and HTML reports, JSONL raw samples, provenance and downloadable bundles.
Spark History Server links/event-log ZIP downloads complement these reports.
Resource samples describe the configured Linux workers and may share host
counters in Docker; they are not isolated Spark task CPU measurements.
State normally lives in `report/control`, configurable with `HIBENCH_STATE_DIR`.
The manifest includes effective parameters, artifacts and configuration hashes.

All catalog implementations are migrated, but experimental validation has limits:
named tiny acceptance, 22 small workload successes and eight selected large
successes are recorded with exceptions and integrity-check scope in
standard-scale validation (local record).
A successful small run is not proof of numerical identity with legacy algorithms,
repeated performance or capacity at larger scales. See the
migration ledger (local record),
micro (local record), SQL (local record) and
ML (local record), final ML (local record),
trees (local record), SVD (local record) and
NWeight (local record) contracts. `docs/` and `docs_internal/` retain local documentation, internal reports
and verified archives of retired source; historical evidence is not runtime input.

## Development, contribution and license

The automated Python suite covers configuration, controller/adapters, API, UI,
monitoring and workload/evidence contracts. It uses temporary state and mocked
submission and does not require a running Spark cluster or Docker. Run it from
the repository root in a Python 3.11+ virtual environment:

```sh
python -m pip install -e ".[test]"
python -m unittest discover -s tests -v
```

Compile the JVM workloads separately with JDK 11 and Maven 3.9:

```sh
mvn -B clean package
```

Compilation does not validate workload outputs. Explicit integration checks and
scientific campaigns require the compatible cluster, built/installed artifacts
and independent result checks. They are not run by the default Python suite.

See [test scope and integration checks](tests/README.md) and [CONTRIBUTING](CONTRIBUTING.md). The project remains **Apache-2.0**; original
copyright and third-party notices are retained. HiBench Next modernization and
extensions: **Mariano Garralda-Barrio, 2025–2026**. See [LICENSE](LICENSE),
[NOTICE](NOTICE) and [CITATION.cff](CITATION.cff). Use the latter to cite the software;
no SoftwareX publication or universal scalability certification is claimed.

Scientific campaign and evidence tools live in [tests/tools](tests/tools/README.md);
`bin/` contains build scripts, maintained workload launchers and runtime/report
helpers. They are separate from the installed `hibench` application package.

Current v2.0.0 release checks, cluster-mode runs and explicit remaining gates: release hardening (local record).

Final SSH acceptance: canonical tiny cluster-mode validation (local record), including worker monitoring and Windows/Linux test results.

The maintainer's `docs/` and `docs_internal/` folders are excluded from Git
and distribution packages. The public test guides, fixtures and reference checks
remain available under `tests/`; references to local records above do not imply
that those records or raw campaign archives are included in this release.
