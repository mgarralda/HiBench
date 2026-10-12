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

"""Small HTTP routes calling shared services; synchronous I/O runs in threadpool."""
from dataclasses import asdict
from uuid import UUID
from typing import Literal
from fastapi import APIRouter, Query, Response
from fastapi.responses import FileResponse
from .models import MonitoringResults, MonitoringSamples
from .models import WorkloadDefaultsUpdate, WorkloadDefaultsResult
from .models import SparkBaseUpdate, SparkBase, ConfigurationUpdate, BenchmarkBase, ExperimentPreset, SparkHistoryLinks, ApiError
from .models import (Adapter, CatalogItem, ClusterAction, ClusterOperationStatus,
                     ClusterProfile, ClusterStatus, Comparison, Diagnostic,
                     Experiment, LogChunk, Message, Operation, Profile,
                     ProfileSettings, ResolveRequest, Run, RunResults,
                     RunSummary, Submission, SubmissionResult)
from hibench import store
from hibench.catalog import workloads
from hibench.adapters import REGISTRY, create_adapter
from hibench.services import configuration, experiments, clusters
from hibench.services import monitoring

router = APIRouter(prefix='/api/v1', responses={401: {'model': ApiError}, 404: {'model': ApiError}, 422: {'model': ApiError}, 503: {'model': ApiError}, 504: {'model': ApiError}})


@router.get('/configuration/spark', response_model=SparkBase, response_model_exclude_none=True, tags=['Configuration'])
def shared_spark_configuration(scale: Literal['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'] | None = Query(default=None)):
    from hibench.spark_configuration import shared
    return shared(scale=scale)


@router.put('/configuration/spark', response_model=SparkBase, response_model_exclude_none=True, tags=['Configuration'])
def save_spark_configuration(request: SparkBaseUpdate):
    from hibench.spark_configuration import save_shared
    return save_shared(request.text, request.expected_sha256, request.benchmark_text, request.benchmark_sha256)


@router.get('/experiments/defaults', response_model=Experiment, response_model_exclude_none=True, tags=['Experiments'])
def defaults(scale: Literal['tiny', 'small', 'large', 'huge', 'gigantic', 'bigdata'] | None = Query(default=None)):
    from hibench import config
    return config.default_experiment(scale=scale)


@router.get('/adapters', response_model=list[Adapter], tags=['Infrastructure'])
def adapters():
    return [asdict(entry) for entry in REGISTRY]


@router.get('/workloads', response_model=list[CatalogItem], response_model_exclude_none=True, tags=['Configuration'])
def catalog():
    import hashlib
    from hibench.workload_parameters import details
    items = []
    for workload in workloads():
        item, presets, properties, path = details(workload['id'])
        items.append({**item, 'preset_table': presets, 'configuration_properties': properties,
                      'configuration_file': str(path)})
        items[-1]['configuration_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    return items


@router.put('/workloads/{workload_id}/defaults', response_model=WorkloadDefaultsResult, tags=['Configuration'])
def save_workload_defaults(workload_id: str, request: WorkloadDefaultsUpdate):
    from hibench.workload_parameters import save_defaults
    return save_defaults(workload_id, request.scale, request.parameters, request.expected_sha256)


@router.post('/experiments/resolve', response_model=Experiment, response_model_exclude_none=True, tags=['Experiments'])
def resolve(request: ResolveRequest):
    settings = configuration.get_profile(request.profile)['settings'] if request.profile else None
    return configuration.resolve(request.experiment, settings)


@router.post('/experiments/validate', response_model=Experiment, response_model_exclude_none=True, tags=['Experiments'])
def validate(experiment: Experiment):
    return experiment


@router.get('/profiles', response_model=list[Profile], response_model_exclude_none=True, tags=['Configuration'])
def profiles():
    return configuration.profiles()


@router.put('/profiles/{name}', response_model=Profile, response_model_exclude_none=True, tags=['Configuration'])
def save_profile(name: str, settings: ProfileSettings):
    return configuration.save_profile(name, settings.model_dump(exclude_none=True))


@router.delete('/profiles/{name}', response_model=Message, tags=['Configuration'])
def delete_profile(name: str):
    configuration.delete_profile(name)
    return {'message': 'Profile deleted; saved runs retain their snapshots'}


@router.post('/runs', response_model=SubmissionResult, status_code=202, tags=['Runs'])
def submit(request: Submission):
    return experiments.submit(request.experiment.model_dump(exclude_none=True), request.request_key)


@router.get('/runs', response_model=list[RunSummary], tags=['Runs'])
def runs(limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return store.recent(limit, offset)


@router.get('/runs/{run_id}', response_model=Run, response_model_exclude_none=True, tags=['Runs'])
def run(run_id: UUID):
    return store.get(str(run_id))


@router.get('/runs/{run_id}/results', response_model=RunResults, response_model_exclude_none=True, tags=['Runs'])
def results(run_id: UUID):
    return experiments.results(str(run_id))


@router.get('/runs/{run_id}/logs', response_model=LogChunk, tags=['Runs'])
def logs(run_id: UUID, offset: int = Query(0, ge=0), limit: int = Query(40000, ge=1, le=1000000)):
    return experiments.logs(str(run_id), offset, limit)


@router.get('/runs/{run_id}/export', response_class=Response, responses={200: {'content': {'application/zip': {'schema': {'type': 'string', 'format': 'binary'}}}}}, tags=['Runs'])
def export(run_id: UUID):
    return Response(experiments.export(str(run_id)), media_type='application/zip',
                    headers={'Content-Disposition': f'attachment; filename="{run_id}.zip"'})


@router.get('/runs/{run_id}/monitoring', response_model=MonitoringResults, tags=['Runs'])
def monitoring_reports(run_id: UUID):
    return monitoring.reports(str(run_id))


@router.get('/runs/{run_id}/monitoring/{report_id}/samples', response_model=MonitoringSamples, tags=['Runs'])
def monitoring_samples(run_id: UUID, report_id: str, offset: int = Query(0, ge=0),
                       limit: int = Query(1000, ge=1, le=10000)):
    return monitoring.samples(str(run_id), report_id, offset, limit)


@router.get('/runs/{run_id}/monitoring/{report_id}/artifacts/{filename}', response_class=FileResponse, responses={200: {'description': 'HTML report, JSON metadata or NDJSON raw samples', 'content': {mime: {'schema': {'type': 'string', 'format': 'binary'}} for mime in ('text/html', 'application/json', 'application/x-ndjson')}}}, tags=['Runs'])
def monitoring_artifact(run_id: UUID, report_id: str, filename: str):
    path = monitoring.artifact(str(run_id), report_id, filename)
    media = {'report.html': 'text/html', 'samples.jsonl': 'application/x-ndjson', 'metadata.json': 'application/json'}
    return FileResponse(path, media_type=media[filename], filename=report_id+'-'+filename,
                        headers={'Content-Security-Policy': "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'"})


@router.post('/runs/{run_id}/cancel', response_model=Message, status_code=202, tags=['Runs'])
def cancel(run_id: UUID):
    store.cancel(str(run_id))
    return {'message': 'Cancellation requested'}


@router.post('/runs/{run_id}/duplicate', response_model=Experiment, response_model_exclude_none=True, tags=['Runs'])
def duplicate(run_id: UUID):
    return experiments.duplicate(str(run_id))


@router.post('/runs/{run_id}/reconcile', response_model=Run, response_model_exclude_none=True, tags=['Runs'])
def reconcile(run_id: UUID):
    from hibench.recovery import reconcile as recover
    return recover(str(run_id))


@router.get('/comparison', response_model=Comparison, tags=['Runs'])
def compare(left: UUID, right: UUID):
    return experiments.compare(str(left), str(right))


@router.post('/diagnostics/doctor', response_model=Diagnostic, tags=['Infrastructure'])
def doctor(experiment: Experiment):
    return {'output': create_adapter(experiment.target.model_dump(exclude_none=True)).doctor()}


@router.post('/diagnostics/install', response_model=Diagnostic, tags=['Infrastructure'])
def install(experiment: Experiment):
    from hibench.services.runtime import install as update_target
    return {'output': update_target(experiment.target.model_dump(exclude_none=True))}


@router.get('/cluster/profile', response_model=ClusterProfile, tags=['Infrastructure'])
def cluster_profile():
    return clusters.profile()


@router.put('/cluster/profile', response_model=ClusterProfile, tags=['Infrastructure'])
def save_cluster(profile: ClusterProfile):
    clusters.save(profile.model_dump())
    return clusters.profile()


@router.post('/cluster/operations', response_model=Operation, status_code=202, tags=['Infrastructure'])
def cluster_operation(request: ClusterAction):
    return {'id': clusters.start(request.profile.model_dump(), request.action)}


@router.get('/cluster/status', response_model=ClusterStatus, tags=['Infrastructure'])
def cluster_status():
    return {'services': clusters.status(clusters.profile())}


@router.get('/cluster/operations/latest', response_model=ClusterOperationStatus, tags=['Infrastructure'])
def cluster_latest():
    latest = clusters.latest()
    return {'task': latest[0] if latest else None, 'log': latest[1] if latest else ''}


@router.get('/runs/{run_id}/preset', response_model=ExperimentPreset, response_model_exclude_unset=True, tags=['Runs'])
def run_preset(run_id: UUID):
    return experiments.preset(str(run_id))


@router.get('/configuration/hibench', response_model=BenchmarkBase, response_model_exclude_none=True, tags=['Configuration'])
def benchmark_configuration():
    from hibench.spark_configuration import shared
    state = shared()
    return {'configuration_file': 'conf/hibench.conf', 'text': state['benchmark_text'], 'sha256': state['benchmark_sha256'], 'defaults': state['defaults']}


@router.put('/configuration/hibench', response_model=BenchmarkBase, response_model_exclude_none=True, tags=['Configuration'])
def save_benchmark_configuration(request: ConfigurationUpdate):
    from hibench.spark_configuration import shared, save_shared
    state = shared()
    save_shared(state['text'], state['sha256'], request.text, request.expected_sha256)
    return benchmark_configuration()


@router.get('/runs/{run_id}/spark-history', response_model=SparkHistoryLinks, tags=['Runs'])
def spark_history(run_id: UUID, base_url: str = Query(description='Browser-accessible HTTP(S) Spark History Server base URL.')):
    """Return the same application and event-log links used by the web UI.

    Downloads come directly from the History Server; availability depends on
    retained Spark event logs and ingestion delay. No server-side URL fetch.
    """
    from hibench.spark_history import application_links
    return application_links(base_url, experiments.results(str(run_id))['events'])
