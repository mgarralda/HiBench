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
5. In the UI, select the Existing Docker target in Experiments, then use **Save and update target** in Spark configuration. Check the target before submitting an experiment. Controller defaults use the running container `spark-cluster-master`.

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
