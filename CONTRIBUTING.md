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

# Contributing to HiBench Next

HiBench Next maintains Spark batch workloads and reproducible generators. Contributions must retain meaningful data volume and workload profiles or explicitly version a changed contract. Application workload/generator code must not use RDD APIs, legacy Spark MLlib or silent legacy fallbacks. Internal implementation details of Spark itself are outside this restriction.

Use Python 3.11+ for the controller, Java 11 and the documented Spark 3.5/Scala 2.12 build. Install `pip install -e ".[test]"`, run `python -m unittest discover -s tests -v` and `mvn -B clean package`. CI builds the Python distribution and Java modules. Integration tests require the complementary Docker cluster and are recorded separately from unit tests.

Keep validation in domain/shared services; UI and API should call the same implementation. Add Pydantic contracts for new HTTP operations. Durable execution belongs in detached workers, not HTTP requests or Streamlit reruns. Document API/configuration compatibility and preserve existing saved records.

Follow the [Python package organization](README.md#python-package-organization):
parameter resolution and validation belong in `hibench/parameters`, web pages
in `hibench/ui`, HTTP contracts in `hibench/api`, and target execution support
in `hibench/adapters`. Use `python -m hibench.launcher --with-ui` for local
API/web startup. Update runtime resources when relocating code used by the
installed Bash launchers; targets do not install the full controller package.

For workload changes, record seeds, distributions, dimensions, cardinalities, logical volume, format and algorithm semantics; distinguish exact references, structural checks and untested scaling. Preserve failing evidence. Update the migration record and report with links to tests and receipts. Never substitute reduced sizes and label them original presets.

Retain upstream attribution and Apache-2.0 notices, and identify new dependencies/licenses. Open a focused pull request explaining behavior and validation; do not claim statistical performance improvements from single functional executions.
