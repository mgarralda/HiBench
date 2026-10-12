# Modifications Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

param([switch]$SkipTests)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$taskMavenCachePath = Join-Path $env:USERPROFILE '.m2/repository'
New-Item -ItemType Directory -Force -Path $taskMavenCachePath | Out-Null
$skipValue = if ($SkipTests) { '1' } else { '0' }
$buildScript = @'
set -eu
mkdir -p /tmp/hibench-build
tar --exclude=target --exclude=__pycache__ -C /workspace -cf - pom.xml LICENSE NOTICE sparkbench | tar -C /tmp/hibench-build -xf -
cd /tmp/hibench-build
if [ "$HIBENCH_SKIP_TESTS" = 1 ]; then
  mvn -B -DskipTests clean package
else
  mvn -B clean package
fi
for module in sparkbench/common sparkbench/micro sparkbench/ml sparkbench/graph sparkbench/sql sparkbench/websearch sparkbench/assembly; do
  mkdir -p /workspace/$module/target
  cp $module/target/*.jar /workspace/$module/target/
done
'@
$buildScript = $buildScript.Replace("`r`n", "`n")
& docker run --rm --mount "type=bind,source=$projectRoot,target=/workspace" --mount "type=bind,source=$taskMavenCachePath,target=/root/.m2/repository" -e "HIBENCH_SKIP_TESTS=$skipValue" maven:3.9.9-eclipse-temurin-11 bash -lc $buildScript
if ($LASTEXITCODE -ne 0) { throw "Java 11 build failed (exit $LASTEXITCODE)" }
