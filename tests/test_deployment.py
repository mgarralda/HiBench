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
from pathlib import Path
import yaml
from hibench.backend import ARTIFACTS
from hibench.catalog import IDS


class DeploymentTests(unittest.TestCase):
    def test_release_artifact_version_is_consistent(self):
        import tomllib
        import xml.etree.ElementTree as ET
        version = tomllib.loads(Path('pyproject.toml').read_text())['project']['version']
        from hibench import __version__
        from hibench.api.app import create_app
        self.assertEqual(__version__, version)
        self.assertEqual(create_app().openapi()['info']['version'], version)
        ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
        for path in [Path('pom.xml'), *Path('sparkbench').rglob('pom.xml')]:
            if 'target' in path.parts:
                continue
            root = ET.parse(path).getroot()
            for tag in ('m:version', 'm:parent/m:version'):
                value = root.find(tag, ns)
                if value is not None:
                    self.assertEqual(value.text, version, str(path))
        jar = f'sparkbench-assembly-{version}-dist.jar'
        self.assertEqual(ARTIFACTS, ('sparkbench/assembly/target/' + jar,))
        self.assertIn(jar, Path('deployment/Dockerfile').read_text())
        for path in Path('conf').glob('hibench.conf*'):
            self.assertIn(jar, path.read_text(), str(path))

    def test_stack_enables_ui_api_and_host_persistence(self):
        service = yaml.safe_load(Path('deployment/compose.yml').read_text())['services']['hibench']
        self.assertIn('../conf:/opt/hibench/conf', service['volumes'])
        self.assertIn('../report:/opt/hibench/report', service['volumes'])
        self.assertEqual(len(service['ports']), 2)
        self.assertTrue(all(port.startswith('127.0.0.1:') for port in service['ports']))
        self.assertIn('HIBENCH_API_TOKEN', service['environment'])
        dockerfile = Path('deployment/Dockerfile').read_text()
        self.assertIn('--with-ui', dockerfile)
        self.assertNotIn('COPY common ', dockerfile)
        self.assertNotIn('autogen', dockerfile)

    def test_retired_components_are_absent_and_catalog_retained(self):
        self.assertEqual(len(IDS), 24)
        self.assertEqual(len(ARTIFACTS), 1)
        for path in ('autogen', 'common', 'bin/workloads/graph/pagerank', 'bin/workloads/ml/xgboost'):
            self.assertFalse(Path(path).exists(), path)
        for path in Path('bin').rglob('*.sh'):
            self.assertNotIn(b'\r\n', path.read_bytes(), str(path))

    def test_source_has_license_and_data_has_adjacent_metadata(self):
        for folder in ('hibench', 'bin', 'sparkbench', 'deployment'):
            for path in Path(folder).rglob('*'):
                if not path.is_file() or 'target' in path.parts or '__pycache__' in path.parts:
                    continue
                if path.suffix in ('.py', '.sh', '.ps1', '.scala', '.java', '.xml', '.yml') or path.name == 'Dockerfile':
                    source = path.read_text(encoding='utf-8')[:5000]
                    self.assertTrue('Apache License' in source or 'SPDX-License-Identifier:' in source, str(path))
        self.assertTrue(Path('sparkbench/common/src/main/resources/hibench-words.license').exists())
