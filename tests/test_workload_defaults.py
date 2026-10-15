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

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench.parameters.workloads import details, save_defaults


class WorkloadDefaultsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.item, rows, other, source = details('micro.wordcount')
        self.path = Path(self.temp.name) / 'wordcount.conf'
        self.path.write_bytes(source.read_bytes())
        self.original = self.path.read_bytes()
        self.digest = hashlib.sha256(self.original).hexdigest()
        self.patcher = patch('hibench.parameters.workloads.details', return_value=(self.item, rows, other, self.path))
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        active = patch('hibench.store.has_active_runs', return_value=False)
        active.start()
        self.addCleanup(active.stop)

    def test_scale_and_fixed_values_preserve_other_sizes_and_expressions(self):
        result = save_defaults('micro.wordcount', 'tiny', {'datasize': 12345, 'seed': 99}, self.digest)
        self.assertTrue(result['changed'])
        text = self.path.read_text()
        self.assertIn('hibench.wordcount.tiny.datasize 12345', text)
        self.assertIn('hibench.wordcount.seed 99', text)
        self.assertIn('${hibench.wordcount.${hibench.scale.profile}.datasize}', text)
        self.assertIn('320000000', text)
        self.assertIn('# Spark SQL text generator v1', text)

    def test_invalid_or_stale_changes_do_not_modify_file(self):
        for values, digest in (({'datasize': 1}, self.digest), ({'unknown': 2}, self.digest), ({'seed': 99}, '0'*64)):
            with self.assertRaises(ValueError):
                save_defaults('micro.wordcount', 'tiny', values, digest)
            self.assertEqual(self.path.read_bytes(), self.original)
        with patch('hibench.store.has_active_runs', return_value=True):
            with self.assertRaisesRegex(ValueError, 'active runs'):
                save_defaults('micro.wordcount', 'tiny', {'seed': 99}, self.digest)
        self.assertEqual(self.path.read_bytes(), self.original)

    def test_api_uses_same_writer(self):
        from fastapi.testclient import TestClient
        from hibench.api.app import create_app
        client = TestClient(create_app())
        response = client.put('/api/v1/workloads/micro.wordcount/defaults',
                              json={'scale': 'tiny', 'parameters': {'seed': 99}, 'expected_sha256': self.digest})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()['changed'])
