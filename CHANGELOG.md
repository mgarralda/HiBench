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

# Changelog

## 2.5.0

Functional scope is frozen. Publication/tagging is separate from local validation.

Software archive: [Zenodo DOI: 10.5281/zenodo.23400663](https://doi.org/10.5281/zenodo.23400663).

### Added

- Individual namespaced PySpark implementations for all 24 batch workloads;
  shared Scala generators, workload settings and existing launch infrastructure.
- Scala, PySpark or both selection through Web UI, REST API and CLI, with effective
  client-mode enforcement for PySpark and separate application observations.
- Saved comparisons: condition summaries, duration statistics, B/A ratios,
  charts and JSON/CSV/HTML/ZIP downloads. Optional SVD phase measurements remain
  distinct from application/job/task timing boundaries.

### Changed

- Python modules grouped under parameters, API, UI, adapters and services;
  `python -m hibench.launcher --with-ui` starts API and web together.
- Experiments emphasizes target, workload/scale, implementation and execution;
  monitoring offers inspection, cancellation and downloads, with configuration
  reuse returning to Experiments.
- One authoritative `conf/` base and portable imported/exported overrides.
- Workload scales recalibrated by family. Dense ML Small/Large uses 400k/4m rows
  with 1000 features; SVD retains its separate documented shape ladder.
- Controller dependencies consolidated in `pyproject.toml`; deployment preserves
  host configuration, SQLite/run state and reports.
- Build, metadata, citation, public documentation and source packaging aligned
  with 2.5.0; local scientific exports and attachments excluded from releases.

### Validation scope and limitations

The supported reference is Spark 3.5.9 / Scala 2.12.18 / Java 11 / Hadoop 3.3.6;
controller Python is 3.11+. Spark 4 / Scala 2.13 is outside this release.
Automated controller tests and compilation do not certify all workload outputs.
Selected Scala/PySpark pilots check specified numerical observables. Calibration
of Linear/PCA Small/Large and SVD Large does not certify durations or capacity for
all 24 workloads or Huge and larger tiers. Single-pair timing observations are
exploratory. BLAS providers, native-thread limits and resources affect comparisons.
Dataset manifest keys identify preparation conditions rather than content checksums;
runtime versions and output equivalence are not automatically certified.

Historical evidence remains recoverable and is not rewritten to match new defaults.
See [tests](tests/README.md), [configuration](docs/configuration-model.md) and
[scale contracts](docs/workload-scales.md) for maintained instructions.
