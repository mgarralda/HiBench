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

import unittest
from unittest.mock import patch, Mock
from hibench import recovery


class RecoveryTests(unittest.TestCase):
    def test_live_worker_is_never_reconciled(self):
        row = {'state': 'running', 'worker_pid': 123}
        with patch.object(recovery.store, 'get', return_value=row), patch.object(recovery, 'process_alive', return_value=True), patch.object(recovery, 'create_adapter') as adapter:
            with self.assertRaisesRegex(ValueError, 'still alive'):
                recovery.reconcile('run')
            adapter.assert_not_called()

    def test_active_remote_job_keeps_lease(self):
        row = {'state': 'running', 'worker_pid': 123, 'spec': {'target': {'kind': 'docker', 'master': 'yarn', 'hadoop_home': '/hadoop'}}}
        adapter = Mock()
        adapter.command.return_value = 'application_123_0001 RUNNING'
        with patch.object(recovery.store, 'get', return_value=row), patch.object(recovery, 'process_alive', return_value=False), patch.object(recovery, 'create_adapter', return_value=adapter), patch.object(recovery.worker, 'events', return_value=[]), patch.object(recovery.store, 'release') as release:
            with self.assertRaisesRegex(RuntimeError, 'still active'):
                recovery.reconcile('run')
            adapter.cancel.assert_called_once()
            release.assert_not_called()

    def test_confirmed_remote_cleanup_marks_failure_before_unlock(self):
        row = {'state': 'running', 'worker_pid': 123, 'spec': {'target': {'kind': 'docker', 'master': 'yarn', 'hadoop_home': '/hadoop'}}}
        adapter = Mock()
        adapter.command.return_value = 'Total applications: 0'
        with patch.object(recovery.store, 'get', return_value=row), patch.object(recovery, 'process_alive', return_value=False), patch.object(recovery, 'create_adapter', return_value=adapter), patch.object(recovery.worker, 'events', return_value=[]), patch.object(recovery.worker, 'append'), patch.object(recovery.store, 'update') as update, patch.object(recovery.store, 'release') as release:
            recovery.reconcile('run')
            self.assertEqual(update.call_args.args[1], 'failed')
            release.assert_called_once_with('run')
