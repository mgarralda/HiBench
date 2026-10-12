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

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase, mock
from tests.tools.scale_verification import verify


class ScaleVerificationTests(TestCase):
    def test_running_campaign_cannot_launch_reference_jobs(self):
        with TemporaryDirectory() as folder:
            Path(folder, 'campaign.json').write_text('{}')
            with mock.patch('tests.tools.scale_verification.subprocess.run') as run:
                with self.assertRaises(ValueError):
                    verify(folder)
                run.assert_not_called()

    def test_zero_exit_does_not_hide_failed_integrity_check(self):
        with TemporaryDirectory() as folder:
            destination = Path(folder)
            destination.joinpath('campaign.json').write_text(json.dumps({
                'finished_utc': '2026-10-09', 'runs': [{}, {}], 'experiment': {'target': {
                    'container': 'test', 'hadoop_home': '/hadoop', 'spark_home': '/spark'}}}))

            def call(command, **kwargs):
                if command[-1].endswith('scale-results.json'):
                    destination.joinpath('scale-results.json').write_text(json.dumps([
                        {'status': 'passed'}, {'status': 'failed'}]))
                return SimpleNamespace(returncode=0)

            with mock.patch('tests.tools.scale_verification.subprocess.run', side_effect=call):
                verify(folder)
            self.assertFalse(json.loads(destination.joinpath('scale-verification-receipt.json').read_text())['checks_passed'])
