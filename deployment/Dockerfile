# syntax=docker/dockerfile:1
# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM maven:3.9.9-eclipse-temurin-11 AS workloads
WORKDIR /build
COPY pom.xml LICENSE NOTICE ./
COPY sparkbench ./sparkbench
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

FROM docker:28-cli AS dockercli
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends bash openssh-client git ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=dockercli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose
WORKDIR /opt/hibench
COPY pyproject.toml README.md LICENSE NOTICE ./
COPY hibench ./hibench
COPY .streamlit ./.streamlit
COPY bin ./bin
COPY conf ./conf
COPY --from=workloads /build/sparkbench/assembly/target/sparkbench-assembly-2.0.0-dist.jar ./sparkbench/assembly/target/
COPY sparkbench/graph/src/main/resources/nweight-user-features ./sparkbench/graph/src/main/resources/
RUN find bin -name '*.sh' -exec sed -i 's/\r$//' {} +
RUN python -m pip install --no-cache-dir -e .
ENV PYTHONUNBUFFERED=1 HIBENCH_STATE_DIR=/opt/hibench/report/control HIBENCH_UI_HOST=0.0.0.0
EXPOSE 8000 8501
CMD ["python", "-m", "hibench.server", "--host", "0.0.0.0", "--with-ui"]
