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

import copy
import importlib.util
from pathlib import Path
from unittest import TestCase, mock
from hibench import config
from hibench.catalog import IDS, SCALES

module_spec = importlib.util.spec_from_file_location('preset_campaign', Path('tests/tools/run_preset_campaign.py'))
runner = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(runner)


class PresetCampaignTests(TestCase):
    def test_all_named_presets_cover_catalog_without_overrides(self):
        for scale in SCALES:
            spec = runner.default_spec('tests/fixtures/experiments/preset-' + scale + '.yaml')
            self.assertEqual(spec['scale'], scale)
            self.assertEqual({w['id'] for w in spec['workloads']}, set(IDS))
            self.assertTrue(all(w['parameters'] == {} for w in spec['workloads']))

    def test_custom_size_cannot_be_mislabelled_as_preset_validation(self):
        spec = copy.deepcopy(config.load('tests/fixtures/experiments/preset-tiny.yaml'))
        spec['workloads'][0]['parameters'] = {'mapper.seconds': 1}
        with mock.patch.object(runner.config, 'load', return_value=spec):
            with self.assertRaisesRegex(ValueError, 'forbids'):
                runner.default_spec('irrelevant.yaml')
