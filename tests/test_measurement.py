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

import unittest
from tests.tools.measurement import summarize_events


class MeasurementTests(unittest.TestCase):
    def test_overlapping_jobs_and_task_attempts(self):
        events = [
            {'Event': 'SparkListenerApplicationStart', 'Timestamp': 1000},
            {'Event': 'SparkListenerJobStart', 'Job ID': 1, 'Submission Time': 2000},
            {'Event': 'SparkListenerJobStart', 'Job ID': 2, 'Submission Time': 3000},
            {'Event': 'SparkListenerJobEnd', 'Job ID': 1, 'Completion Time': 4000},
            {'Event': 'SparkListenerJobEnd', 'Job ID': 2, 'Completion Time': 5000},
            {'Event': 'SparkListenerApplicationEnd', 'Timestamp': 7000},
            {'Event': 'SparkListenerTaskEnd', 'Task End Reason': {'Reason': 'ExceptionFailure'},
             'Task Metrics': {'Executor Run Time': 200, 'Executor CPU Time': 100000000}},
            {'Event': 'SparkListenerTaskEnd', 'Task End Reason': {'Reason': 'Success'},
             'Task Metrics': {'Executor Run Time': 300, 'Executor CPU Time': 200000000}},
            {'Event': 'SparkListenerEnvironmentUpdate', 'Spark Properties':
             {'spark.sql.adaptive.enabled': 'false', 'spark.hadoop.secret': 'hidden'}},
        ]
        result = summarize_events(events)
        self.assertTrue(result['complete'])
        self.assertEqual(result['application_duration_s'], 6)
        self.assertEqual(result['job_interval_union_s'], 3)
        self.assertEqual(result['failed_task_attempts'], 1)
        self.assertEqual(result['executor_run_time_sum_s'], .5)
        self.assertEqual(result['effective_properties'], {'spark.sql.adaptive.enabled': 'false'})

    def test_truncated_log_is_not_complete(self):
        result = summarize_events([{'Event': 'SparkListenerApplicationStart', 'Timestamp': 1000}])
        self.assertFalse(result['complete'])
        self.assertIsNone(result['application_duration_s'])

    def test_invalid_clock_is_not_a_duration(self):
        result = summarize_events([{'Event': 'SparkListenerApplicationStart', 'Timestamp': 2000},
                                   {'Event': 'SparkListenerApplicationEnd', 'Timestamp': 1000}])
        self.assertFalse(result['complete'])
        self.assertIsNone(result['application_duration_s'])

    def test_missing_or_reversed_job_end_is_incomplete(self):
        events = [{'Event': 'SparkListenerApplicationStart', 'Timestamp': 1000},
                  {'Event': 'SparkListenerApplicationEnd', 'Timestamp': 4000},
                  {'Event': 'SparkListenerJobStart', 'Job ID': 1, 'Submission Time': 2000}]
        self.assertFalse(summarize_events(events)['complete'])
        events.append({'Event': 'SparkListenerJobEnd', 'Job ID': 1, 'Completion Time': 1500})
        self.assertFalse(summarize_events(events)['complete'])
