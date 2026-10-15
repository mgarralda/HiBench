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

# Configuration model

This short reference explains configuration resolution. For the illustrated web
workflow, installation, API/CLI examples and scientific interpretation, see the
[Technical and scientific user manual (PDF)](HiBench-Next-2.5.0-Technical-Scientific-Manual-EN.pdf).

`conf/` is the sole maintained configuration base for CLI, API and web experiments.

## Resolution

The controller reads common `.conf` files, selects the named scale and resolves
its Spark resource settings. Workload-specific defaults come from
`conf/workloads/<family>/<workload>.conf`. Explicit workload parameters override
those defaults for the selected experiment only.

An imported YAML/JSON experiment or a submitted draft overrides the resolved base.
Resource, Spark, monitoring and partial target dictionaries merge with base values;
a complete target replaces the base target, preserving explicit field removals.
The final specification is validated before submission and retained in the run.
No separate runtime recipe/preset directory is maintained. Exported experiment
overrides are user-owned files; importing them applies them to the current base.

Scale names are not dataset identities. See [workload scales](workload-scales.md)
for input-volume contracts, calibrated cases and untested tiers.

## Editing and execution

- General configuration sets the common target and experiment defaults.
- Spark infrastructure configures Spark properties and resources by scale.
- Workload parametrization edits shared Scala/PySpark workload settings.
- Experiments selects target, workloads, scale and implementation, with optional
  overrides. The resolved settings should be reviewed before execution.
- **Update target** synchronizes built artifacts, launch helpers and configuration
  to the selected target. Rebuild Scala changes before updating. Updates are
  unavailable while controller runs are queued or active.
- **Check connection** diagnoses the configured target; it does not execute a benchmark.

PySpark runs in client deploy mode. Mixed Scala/PySpark selection retains separate
application identities and observations; Scala may use cluster mode while PySpark
uses client. Effective deployment differences must be considered when comparing.

## Persistence and comparisons

Configuration stays under `conf/`; run state, SQLite, logs, reports and comparison
snapshots stay under the configurable report/state directory. Docker Compose binds
configuration and reports to the host. Dataset manifests describe preparation
conditions, not content checksums. Comparisons retain independent observations
and flag known differences or unknown metadata; they do not certify numerical
output equivalence or provide causal/statistical equivalence claims.

Historical runs retain their recorded specifications even when defaults change.
Reusing a historical specification creates a new experiment; monitoring and
comparison views do not submit benchmarks.
