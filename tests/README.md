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
builds Python packages, checks server help and compiles the Maven reactor on
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
