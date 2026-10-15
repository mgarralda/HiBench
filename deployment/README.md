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

# HiBench Next deployment

This Compose stack runs the FastAPI API and Streamlit UI together, supervised by
FastAPI lifespan. It uses the existing Spark/Hadoop Docker cluster; it does not
create a second cluster. Build images include Java 11 benchmark JARs, Python
3.11, the Docker CLI/Compose plugin and SSH client. Java/Spark/Hadoop execution
and resource monitoring run in the configured cluster container.

The image starts `python -m hibench.launcher --host 0.0.0.0 --with-ui`.
The REST application is `hibench.api.app:app`; its lifespan starts the packaged
Streamlit entry point `hibench/ui/app.py` and stops that child process on shutdown.
Detached benchmark workers keep their own lifecycle.

## Start

The host needs Docker with Compose and Git. Java, Maven and Python dependencies
are provided by the image build; benchmark JARs are compiled from repository
sources rather than committed binaries.

Use a companion cluster revision/image for Spark **3.5.9**, Scala **2.12.18**,
Java **11** and Hadoop **3.3.6**, the validated environment for this release.
The companion repository's current Spark 4.2 stack requires the planned HiBench
migration; this release's target diagnosis rejects Spark 4 / Scala 2.13.

1. Download or check out the companion [hadoop-spark-cluster Spark 3.5.9 release](https://github.com/mgarralda/hadoop-spark-cluster/releases#release-spark-3.5.9) (`spark-3.5.9` tag) and start its cluster. Verify the container name and paths in `../conf/hibench.conf` and the storage settings in `../conf/hadoop.conf`.
2. Copy `.env.example` to `.env`. Generate a token without installing Python locally: `docker run --rm python:3.11-slim python -c "import secrets; print(secrets.token_urlsafe(32))"`. Use the generated value in `.env`.
3. From this directory: `docker compose --env-file .env -f compose.yml up -d --build --wait`.
4. Open the UI at http://127.0.0.1:8501 and API docs at http://127.0.0.1:8000/docs. Use **Authorize** with the token for API requests.
5. In Experiments, select the Existing Docker target, use **Update target**, then **Check connection** before submitting an experiment. Configuration pages also provide **Save and update target** after editing settings. Controller defaults use the running container `spark-cluster-master`.

The API and UI ports can be changed in `.env`. Stop any host processes already
using those ports. Commands below also run from this directory:

```sh
docker compose --env-file .env -f compose.yml logs -f
docker compose --env-file .env -f compose.yml down
docker compose --env-file .env -f compose.yml up -d --build --wait
```

## Persistent state

| Host path | Container path | Contents |
| --- | --- | --- |
| `../conf` | `/opt/hibench/conf` | Canonical configuration; UI edits persist on the host |
| `../report` | `/opt/hibench/report` | SQLite run registry, profiles, run snapshots, logs, metrics, HTML reports and raw samples |

Configure these bind mounts with `HIBENCH_CONF_HOST_DIR` and
`HIBENCH_REPORT_HOST_DIR` in `.env`; Windows paths can use forward slashes
(for example `E:/projects/HiBench/report`). Relative paths resolve from this
Compose directory. The configuration directory must contain the existing `conf/`
files. SQLite lives at `report/control/runs.sqlite`; saved analysis snapshots live
at `report/control/comparisons/<comparison_id>/comparison.json`. Keep
`HIBENCH_STATE_DIR` beneath `/opt/hibench/report` so state remains mounted.
There is no separate SQLite volume: the report mount persists database, WAL,
dataset manifests and comparison evidence together.

In **Runs → Comparisons**, select completed runs and one implementation per side.
The same run may supply both Scala and PySpark observations. Analyze all workloads
or one workload, inspect conditions, sample durations, medians and B/A ratios,
then optionally save a snapshot. Downloads include JSON, raw observations CSV,
summary CSV, an offline HTML report and a ZIP containing all four files. Reports
remain independent from original workload reports and resource monitoring.
API clients use `POST /api/v1/comparisons/analyze`, `POST /api/v1/comparisons`
to save, and `GET /api/v1/comparisons/{id}/export` to download. Dataset manifest
keys identify preparation conditions, not verified data content. Runtime versions
and output equivalence are shown as uncertified; comparisons do not establish
causality or statistical equivalence. Duration includes submission and startup.

Rebuilding images or recreating this service does not remove bind-mounted state.
Back up both directories together while no run is active and the service is
stopped. Preserve SQLite database/WAL files as a set. Imported experiment files
belong to users and are downloaded rather than kept as runtime recipes.
The cluster's HDFS datasets and Spark event logs belong to **its own volumes**;
back up those volumes in the companion project as well. Removing cluster volumes
can lose datasets even if HiBench reports remain. Container recreation preserves
records, not an interrupted worker process; reconcile affected runs after recovery.

The Docker socket grants control over the Docker host. This is a trusted local
administration deployment, bound to loopback; do not expose the UI publicly. The
API requires a bearer token, but Streamlit has no user authentication. Use a
separately secured proxy before any shared/network deployment. The same socket
lets the Cluster assistant invoke Docker/Compose. Its clone workspace is currently
inside the controller container unless an operator mounts that chosen path too;
the recommended existing companion checkout stays managed on the host.

The health check covers both API and UI. No Redis, external database or second
execution service is introduced. Workload validation and scalability limitations
are described in the root README and scientific records.

## SSH execution

Choose SSH in Experiments to use an existing Linux submission host. Mount your already authorized private key and independently verified known_hosts file read-only into the controller with a local Compose override; set the corresponding container paths in the target. Keep that override and credentials outside Git. OpenSSH uses non-interactive authentication and strict host-key verification. For the companion cluster, its published SSH port is normally 2222 on the host; from a controller container use a reachable host address or shared Docker network hostname, rather than that container's own 127.0.0.1. No key is created or copied automatically.

`deployment/.env` supplies optional `HIBENCH_SSH_HOST`, `HIBENCH_SSH_USERNAME`, `HIBENCH_SSH_PORT`, `HIBENCH_SSH_IDENTITY_FILE` and `HIBENCH_SSH_KNOWN_HOSTS_FILE`. Compose passes these to the controller. Explicit experiment fields take precedence; resolved connection values are retained in run snapshots. Native launches use exported environment variables (they do not automatically read the Compose .env).

Password authentication is supported through `HIBENCH_SSH_PASSWORD` in the ignored deployment `.env` or controller environment. Leave `identity_file` empty to select it; an explicit key takes precedence. Mount an independently verified known_hosts file and configure its container path. Passwords are not accepted in REST target models or experiment files, and are not placed in command arguments. Restart the controller after changing credential environment variables.

## Deployment validation

The normal image build compiles workloads and installs the application; it does
not run the Python suite or real Spark acceptance. The health check tests API
and UI availability, not cluster compatibility or workload correctness.

See [the test guide](../tests/README.md) for automated checks and explicit Compose
acceptance, including its image prerequisite, shared-state effects and compatible
cluster requirement. Dated release receipts are linked from the root README.

### Shared scale baselines

Resource and numerical thread settings live in `conf/spark.conf`; see the
[scale baseline table](../sparkbench/README.md#shared-scale-baselines) for values
and validation limits. HiBench also applies numerical `spark.yarn.appMasterEnv`
variables to Client submission processes, avoiding shell-profile edits.

The companion validation Compose master has a four-CPU quota; workers retain
six CPU each. Keep these external CPU limits recorded with experiments:
`spark.driver.cores` does not enforce Client CPU capacity. New experiments inherit
the scale base; existing saved experiments retain their explicit overrides.
