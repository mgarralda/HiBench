param([switch]$SkipTests)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$taskMavenCachePath = Join-Path $env:USERPROFILE '.m2/repository'
New-Item -ItemType Directory -Force -Path $taskMavenCachePath | Out-Null
$skipValue = if ($SkipTests) { '1' } else { '0' }
$buildScript = @'
set -eu
mkdir -p /tmp/hibench-build
tar --exclude=target --exclude=__pycache__ -C /workspace -cf - pom.xml common autogen sparkbench | tar -C /tmp/hibench-build -xf -
cd /tmp/hibench-build
if [ "$HIBENCH_SKIP_TESTS" = 1 ]; then
  mvn -B -DskipTests clean package
else
  mvn -B clean package
fi
for module in common autogen sparkbench/common sparkbench/micro sparkbench/ml sparkbench/graph sparkbench/sql sparkbench/websearch sparkbench/assembly; do
  mkdir -p /workspace/$module/target
  cp $module/target/*.jar /workspace/$module/target/
done
'@
& docker run --rm --mount "type=bind,source=$projectRoot,target=/workspace" --mount "type=bind,source=$taskMavenCachePath,target=/root/.m2/repository" -e "HIBENCH_SKIP_TESTS=$skipValue" maven:3.9.9-eclipse-temurin-11 bash -lc $buildScript
if ($LASTEXITCODE -ne 0) { throw "Java 11 build failed (exit $LASTEXITCODE)" }
