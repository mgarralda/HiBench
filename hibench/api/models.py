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

"""Pydantic v2 request and response contracts."""
from typing import Any, Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, model_validator
from hibench import config


class Model(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class Workload(Model):
    id: str
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)


class WorkloadDefaultsUpdate(Model):
    scale: Literal['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata']
    parameters: dict[str, str | int | float | bool]
    expected_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class WorkloadDefaultsResult(Model):
    changed: bool
    configuration_file: str
    properties: dict[str, str] = Field(default_factory=dict)


class SparkBaseUpdate(Model):
    benchmark_text: str | None = None
    benchmark_sha256: str | None = Field(default=None, pattern=r'^[0-9a-f]{64}$')
    text: str
    expected_sha256: str = Field(pattern=r'^[0-9a-f]{64}$')


class Target(Model):
    kind: Literal['docker', 'local', 'ssh']
    workspace: str
    spark_home: str
    hadoop_home: str
    master: str = Field(description='Spark master: yarn, spark://host:port for a Standalone cluster, or local[2] for two threads on the submission host.')
    hdfs_uri: str
    deploy_mode: Literal['client', 'cluster'] = Field(default='client', description='HiBench supports cluster deployment only on YARN for Scala. PySpark-only experiments, Standalone and Local require client; both uses client for PySpark.')
    workers: list[str] = Field(default_factory=list)
    container: str | None = None
    host: str | None = None
    username: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    identity_file: str | None = None
    known_hosts_file: str | None = None
    hadoop_conf_dir: str | None = None
    report_base: str | None = None
    dataset_base: str | None = None
    output_base: str | None = None


class Resources(Model):
    executor_instances: int = Field(ge=1, le=10000)
    executor_cores: int = Field(ge=1, le=10000)
    executor_memory: str
    driver_memory: str
    generation_partitions: int = Field(ge=1, le=10000)
    generation_shuffle_partitions: int = Field(ge=1, le=10000)
    shuffle_partitions: int = Field(ge=1, le=10000)


class MonitoringSettings(Model):
    enabled: bool
    interval_s: int = Field(ge=1, le=60)


class Experiment(Model):
    schema_version: Literal[1] = 1
    name: str
    execution_group: str = Field(default='', max_length=128)
    target: Target
    implementation: Literal['scala', 'pyspark', 'both'] = 'scala'
    mode: Literal['prepare', 'run', 'prepare_and_run']
    scale: Literal['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata']
    repetitions: int = Field(ge=1, le=100)
    timeout_s: int = Field(ge=1, le=604800)
    resources: Resources = Field(description='Complete effective resources. Obtain canonical defaults through /experiments/defaults or /experiments/resolve.')
    spark_conf: dict[str, str | int | float | bool] = Field(default_factory=dict)
    monitoring: MonitoringSettings
    workloads: list[Workload] = Field(min_length=1)

    @model_validator(mode='after')
    def domain_validation(self):
        config.validate(self.model_dump(exclude_none=True))
        return self


class ResolveRequest(Model):
    experiment: dict[str, Any]
    profile: str | None = None


class Submission(Model):
    experiment: Experiment
    request_key: str | None = Field(default=None, max_length=200)


class SubmissionResult(Model):
    run_id: str
    created: bool


class ProfileSettings(Model):
    target: Target
    resources: Resources
    spark_conf: dict[str, str | int | float | bool] = Field(default_factory=dict)


class Profile(Model):
    name: str
    settings: ProfileSettings
    updated: float


class RunSummary(Model):
    id: str
    state: Literal['queued', 'waiting', 'running', 'succeeded', 'failed', 'cancelled']
    created: float
    updated: float
    detail: str


class Run(RunSummary):
    request_key: str
    spec: Experiment
    cancel: int
    worker_pid: int | None


class RunResults(Model):
    run: Run
    provenance: dict[str, Any]
    events: list[dict[str, Any]]
    metrics: list[dict[str, Any]]


class LogChunk(Model):
    offset: int
    next_offset: int
    text: str


class MonitoringReport(Model):
    id: str
    workload: str
    phase: Literal['prepare', 'run']
    index: int
    status: Literal['available', 'partial', 'unavailable']
    interval_s: int
    schema_version: int
    collector_version: str
    scope: str
    expected_nodes: list[str]
    observed_nodes: list[str]
    sample_count: int
    errors: list[str]
    collected_at: float
    report_available: bool
    units: dict[str, str] = Field(default_factory=dict)


class MonitoringResults(Model):
    status: Literal['available', 'partial', 'unavailable', 'pending', 'disabled']
    scope: str
    reports: list[MonitoringReport]


class ResourceSample(Model):
    schema_version: int
    timestamp: float
    node: str
    scope: Literal['system']
    metrics: dict[str, dict[str, str | int | float]]


class MonitoringSamples(Model):
    offset: int
    next_offset: int
    samples: list[ResourceSample]


class ComparisonSelection(Model):
    left: str
    right: str
    left_implementation: Literal['scala', 'pyspark'] | None = None
    right_implementation: Literal['scala', 'pyspark'] | None = None
    workload: str | None = None

    @model_validator(mode='after')
    def validate_ids(self):
        self.left = str(UUID(self.left))
        self.right = str(UUID(self.right))
        return self


class ComparativeAnalysis(Model):
    schema_version: int
    analysis_version: int
    created_at: float
    selection: ComparisonSelection
    source_sha256: list[str]
    source_states: list[str]
    source_errors: list[list[dict[str, Any]]]
    conditions: list[dict[str, Any]]
    observations: dict[str, list[dict[str, Any]]]
    summary: list[dict[str, Any]]
    phase_summary: list[dict[str, Any]] = Field(default_factory=list,
        description='Explicit instrumented workload timers grouped by workload, protocol and phase. Missing measurements are not zero; these are not Spark SQL durations.')
    scope: str
    comparison_id: str | None = None


class Comparison(Model):
    left: str
    right: str
    configuration_equivalent: bool
    differences: list[dict[str, Any]]
    left_metrics: list[dict[str, Any]]
    right_metrics: list[dict[str, Any]]
    scope: str


class Adapter(Model):
    kind: str
    label: str
    implemented: bool


class WorkloadParameter(Model):
    property: str
    kind: Literal['integer', 'number', 'boolean', 'string']
    default: str
    presets: dict[str, str]
    unit: str | None = None
    note: str | None = None


class CatalogItem(Model):
    id: str
    category: str
    description: str
    parameters: dict[str, WorkloadParameter]
    requires_input: bool
    requires_classic: bool
    validation_status: str
    execution_available: bool
    implementations: list[Literal['scala', 'pyspark']]
    prepare_capabilities: list[str]
    preset_table: list[dict[str, str]]
    configuration_properties: list[dict[str, str]]
    configuration_file: str
    configuration_sha256: str


class SparkBase(Model):
    benchmark_text: str
    benchmark_sha256: str
    configuration_file: str
    text: str
    sha256: str
    defaults: Experiment


class ClusterProfile(Model):
    directory: str
    project: str
    ref: str


class ClusterAction(Model):
    profile: ClusterProfile
    action: Literal['download', 'build', 'up', 'stop', 'down']


class Message(Model):
    message: str


class Operation(Model):
    id: str


class Diagnostic(Model):
    output: str


class Health(Model):
    status: Literal['ok']
    api_version: str


class ClusterStatus(Model):
    services: list[dict[str, Any]]


class ClusterOperationStatus(Model):
    task: dict[str, Any] | None
    log: str


class ConfigurationUpdate(Model):
    text: str = Field(description='Complete canonical configuration text, not experiment overrides.')
    expected_sha256: str = Field(pattern=r'^[0-9a-f]{64}$', description='Revision returned by GET. Stale revisions are rejected.')


class BenchmarkBase(Model):
    configuration_file: str
    text: str
    sha256: str
    defaults: Experiment


class ExperimentPreset(Model):
    """Compact differences from conf/. Import with /experiments/resolve."""
    schema_version: Literal[1] = 1
    name: str | None = None
    execution_group: str | None = None
    target: Target | None = None
    implementation: Literal['scala', 'pyspark', 'both'] | None = None
    mode: Literal['prepare', 'run', 'prepare_and_run'] | None = None
    scale: Literal['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'] | None = None
    repetitions: int | None = None
    timeout_s: int | None = None
    resources: dict[str, str | int] | None = None
    spark_conf: dict[str, str | int | float | bool] | None = None
    monitoring: dict[str, bool | int] | None = None
    workloads: list[Workload] | None = None


class SparkHistoryApplication(Model):
    application_id: str
    history_url: str
    event_logs_url: str


class SparkHistoryLinks(Model):
    applications: list[SparkHistoryApplication]
    scope: str


class ApiError(Model):
    detail: str | list[dict[str, Any]] = Field(description='Actionable domain error text or Pydantic validation details.')
