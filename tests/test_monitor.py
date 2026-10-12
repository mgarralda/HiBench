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

"""Legacy monitoring log compatibility and safe parsing."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "legacy_monitor", Path(__file__).resolve().parents[1] / "bin/functions/monitor.py")
monitor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(monitor)


class MonitorTests(unittest.TestCase):
    def test_template_is_standalone_with_visible_raw_data(self):
        template = (Path(__file__).resolve().parents[1] / "bin/functions/chart-template.html").read_text(encoding="utf-8")
        self.assertNotIn('<script src=', template)
        self.assertNotIn('display: none', template)
        self.assertIn('<noscript>', template)

    def test_namedtuple_roundtrip(self):
        sample = {"hostname": "worker", "cpu": monitor.CPU("cpu", 1, 0, 2, 3, 0, 0, 0)}
        self.assertEqual(monitor.parse_monitor_sample(repr(sample)), sample)

    def test_rejects_executable_content(self):
        with self.assertRaises(ValueError):
            monitor.parse_monitor_sample("__import__('os').system('echo unsafe')")
