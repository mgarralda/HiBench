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

"""Evidence attribution is an acceptance requirement, not a success heuristic."""
import importlib.util
import unittest
import json
import shutil
from tempfile import TemporaryDirectory
from pathlib import Path

spec = importlib.util.spec_from_file_location("acceptance_matrix", Path("tests/tools/build_acceptance_matrix.py"))
matrix = importlib.util.module_from_spec(spec)
spec.loader.exec_module(matrix)


class AcceptanceMatrixTests(unittest.TestCase):
    def test_custom_size_cannot_certify_named_tiny_preset(self):
        entry = {'workload': 'micro.terasort'}
        campaign = {'runs': [{'workload': 'micro.terasort', 'state': 'succeeded',
                              'spec': {'scale': 'tiny', 'workloads': [{'parameters': {'datasize': 1000000}}]}}]}
        matrix.apply_preset_evidence(entry, campaign, [{'workload': 'micro.terasort', 'status': 'passed'}],
                                    [{'case': 'actual', 'checks_passed': True}],
                                    {'workloads': [{'workload': 'micro.terasort', 'complete': True}]})
        self.assertNotIn('default_preset_acceptance', entry)

    def test_preset_execution_without_checks_is_not_acceptance(self):
        entry = {'workload': 'micro.terasort'}
        campaign = {'runs': [{'workload': 'micro.terasort', 'state': 'succeeded',
                              'spec': {'scale': 'tiny', 'workloads': [{'parameters': {}}]}}]}
        matrix.apply_preset_evidence(entry, campaign, [], [], {})
        self.assertNotIn('default_preset_acceptance', entry)

    def test_intermediate_success_is_attributed_per_workload(self):
        measurements = {'workloads': [{'workload': 'ml.svd', 'complete': True}]}
        checks = [{'workload': 'ml.svd', 'status': 'passed'}]
        selected = {'workload': 'ml.svd'}
        unrelated = {'workload': 'ml.pca'}
        matrix.apply_intermediate_evidence(selected, measurements, checks)
        matrix.apply_intermediate_evidence(unrelated, measurements, checks)
        self.assertIn('intermediate_timing', selected)
        self.assertNotIn('intermediate_timing', unrelated)

    def test_incomplete_timing_or_failed_check_is_not_acceptance(self):
        for complete, status in ((False, 'passed'), (True, 'failed')):
            entry = {'workload': 'ml.svd'}
            matrix.apply_intermediate_evidence(entry, {'workloads': [{'workload': 'ml.svd', 'complete': complete}]},
                                               [{'workload': 'ml.svd', 'status': status}])
            self.assertNotIn('intermediate_timing', entry)

    def test_catalog_coverage_and_no_shared_success_certification(self):
        fixture = matrix.ROOT / 'tests/fixtures/acceptance'
        with TemporaryDirectory(dir=matrix.ROOT) as folder:
            root = Path(folder)
            shutil.copytree(fixture, root, dirs_exist_ok=True)
            shutil.copytree(fixture / 'references', root / 'docs', dirs_exist_ok=True)
            ledger = json.loads((root / 'docs/reproducibility/migrations.json').read_text())
            # The frozen ledger keeps historical paths. This temporary ledger
            # assesses current source files after package reorganization.
            relocated = {
                'hibench/backend.py': 'hibench/adapters/runtime.py',
                'hibench/workload_parameters.py': 'hibench/parameters/workloads.py',
                'hibench/ml_parameters.py': 'hibench/parameters/ml.py',
                'hibench/svd_parameters.py': 'hibench/parameters/svd.py',
                'hibench/nweight_parameters.py': 'hibench/parameters/nweight.py',
            }
            for record in ledger['records']:
                record['implementation'] = [relocated.get(name, name) for name in record.get('implementation', [])]
            (root / 'docs/reproducibility/migrations.json').write_text(json.dumps(ledger), encoding='utf-8')
            required = set()
            for record in ledger['records']:
                if record['workload'] not in matrix.IDS:
                    continue
                for field in ('verifiers', 'implementation', 'scale_or_profile_reference'):
                    required.update(record.get(field, []))
            required.update('bin/workloads/' + name.replace('.', '/') + '/prepare/prepare.sh' for name in matrix.IDS)
            for relative in required:
                path = root / relative
                if not path.exists():
                    path.parent.mkdir(parents=True, exist_ok=True)
                    source = matrix.ROOT / relative
                    if relative == 'tests/test_pending_ml.py':
                        source = matrix.ROOT / 'tests/test_ml_contracts.py'
                    shutil.copy2(source, path)
            data = matrix.build(root)
        self.assertEqual({w["workload"] for w in data["workloads"]}, set(matrix.IDS))
        self.assertEqual(len(data["workloads"]), 24)
        for entry in data["workloads"]:
            self.assertIn(entry["current_revision_cluster_acceptance"], ('pending', 'execution_recorded; independent output acceptance pending', 'bounded checks recorded; performance/external pending'))
            self.assertIn(entry["receipt"], data["receipts"])
            self.assertTrue(entry["evidence_sha256"])

    def test_observations_retain_shared_receipt_json_pointers(self):
        data = {"campaigns": [{"state": "succeeded", "spec": {"workloads": [{"id": "ml.svd"}]}}]}
        observations = matrix.observations(data)
        self.assertIn({"pointer": "/campaigns/0/state", "value": "succeeded"}, observations)
        self.assertIn({"pointer": "/campaigns/0/spec", "value": data["campaigns"][0]["spec"]}, observations)
