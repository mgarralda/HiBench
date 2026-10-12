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

"""Interpret Spark event timings without claiming exclusive algorithm time."""


def summarize_events(events):
    start = end = None
    jobs = {}
    properties = {}
    task_run = task_cpu = gc = spill = 0
    task_attempts = failed_attempts = 0
    executors = {}
    active_executors = set()
    peak_executors = 0
    for event in events:
        kind = event.get('Event')
        if kind == 'SparkListenerApplicationStart':
            start = event['Timestamp']
        elif kind == 'SparkListenerApplicationEnd':
            end = event['Timestamp']
        elif kind == 'SparkListenerEnvironmentUpdate':
            properties = dict(event.get('Spark Properties', {}))
        elif kind == 'SparkListenerExecutorAdded' and event['Executor ID'] != 'driver':
            identifier = event['Executor ID']
            executors[identifier] = event.get('Executor Info', {}).get('Total Cores')
            active_executors.add(identifier)
            peak_executors = max(peak_executors, len(active_executors))
        elif kind == 'SparkListenerExecutorRemoved':
            active_executors.discard(event['Executor ID'])
        elif kind == 'SparkListenerJobStart':
            jobs.setdefault(event['Job ID'], {})['start_ms'] = event['Submission Time']
        elif kind == 'SparkListenerJobEnd':
            jobs.setdefault(event['Job ID'], {}).update(
                end_ms=event['Completion Time'], result=event.get('Job Result'))
        elif kind == 'SparkListenerTaskEnd':
            task_attempts += 1
            failed_attempts += event.get('Task End Reason', {}).get('Reason') != 'Success'
            metrics = event.get('Task Metrics', {})
            task_run += metrics.get('Executor Run Time', 0)
            task_cpu += metrics.get('Executor CPU Time', 0)
            gc += metrics.get('JVM GC Time', 0)
            spill += metrics.get('Disk Bytes Spilled', 0)
    intervals = sorted((j['start_ms'], j['end_ms']) for j in jobs.values()
                       if 'start_ms' in j and 'end_ms' in j and j['end_ms'] >= j['start_ms'])
    union = 0
    previous_end = None
    for begin, finish in intervals:
        if previous_end is None:
            union += finish - begin
        elif finish > previous_end:
            union += finish - max(begin, previous_end)
        previous_end = max(finish, previous_end) if previous_end is not None else finish
    allowed = ('spark.executor.instances', 'spark.executor.cores', 'spark.executor.memory',
               'spark.driver.memory', 'spark.sql.shuffle.partitions',
               'spark.sql.adaptive.enabled', 'spark.dynamicAllocation.enabled',
               'spark.speculation', 'spark.master', 'spark.submit.deployMode')
    return {
        'complete': start is not None and end is not None and end >= start
                    and all('start_ms' in j and 'end_ms' in j and j['end_ms'] >= j['start_ms'] for j in jobs.values()),
        'application_start_ms': start, 'application_end_ms': end,
        'application_duration_s': (end - start) / 1000 if start is not None and end is not None and end >= start else None,
        'job_interval_union_s': union / 1000,
        'jobs': jobs,
        'task_attempts': task_attempts, 'failed_task_attempts': failed_attempts,
        'observed_executor_cores': executors, 'peak_observed_executors': peak_executors,
        'executor_run_time_sum_s': task_run / 1000,
        'executor_cpu_time_sum_s': task_cpu / 1e9,
        'jvm_gc_time_sum_s': gc / 1000, 'disk_spill_bytes_sum': spill,
        'effective_properties': {k: properties[k] for k in allowed if k in properties},
    }
