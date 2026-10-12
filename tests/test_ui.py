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

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from streamlit.testing.v1 import AppTest


class UIReviewTests(unittest.TestCase):
    def test_all_navigation_pages_render_and_planned_adapter_cannot_submit(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'HIBENCH_STATE_DIR': directory}):
            app = AppTest.from_file(str(Path('hibench/web.py').resolve())).run(timeout=20)
            self.assertFalse(app.exception)
            for key in ('SparkConfiguration', 'HiBenchConfiguration', 'Workloads', 'General', 'Clusters', 'Help', 'About', 'Runs', 'Experiments'):
                app.button(key='nav-' + key).click().run()
                self.assertFalse(app.exception, key)
            adapter = next(item for item in app.selectbox if item.label == 'Execution adapter')
            adapter.set_value('dataproc').run()
            self.assertFalse(app.exception)
            self.assertTrue(next(button for button in app.button if button.label == 'Run').disabled)
