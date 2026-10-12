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

import copy
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench import config
from hibench.catalog import IDS, SCALES
from hibench.workload_parameters import customize, details, typed_value

class WorkloadParameterTests(unittest.TestCase):
    def test_every_workload_and_scale_can_be_inspected_and_applied(self):
        base=config.default_experiment()
        for workload in IDS:
            item,rows,other,path=details(workload)
            before=path.read_bytes()
            self.assertEqual(len(rows),len(item["parameters"]))
            for scale in SCALES:
                values={k:typed_value(s,s["presets"][scale]) for k,s in item["parameters"].items()}
                if not item.get("execution_available", True):
                    with self.assertRaisesRegex(ValueError, "unavailable"):
                        customize(base,workload,scale,values)
                    continue
                effective_base=copy.deepcopy(base)
                if workload=="graph.nweight" and scale=="bigdata":
                    effective_base['resources']['generation_shuffle_partitions']=4
                result=customize(effective_base,workload,scale,values)
                self.assertEqual(result["scale"],scale)
                self.assertEqual(next(w for w in result["workloads"] if w["id"]==workload)["parameters"],{})
            self.assertEqual(path.read_bytes(),before)

    def test_customizations_are_validated_without_mutating_reference_or_draft(self):
        base=config.default_experiment();original=copy.deepcopy(base)
        result=customize(base,"sql.scan","small",{"pages":120,"uservisits":1234})
        self.assertEqual(base,original)
        self.assertEqual(next(w for w in result["workloads"] if w["id"]=="sql.scan")["parameters"],{"pages":120,"uservisits":1234})
        for params in ({"pages":1},{"uservisits":1000.0},{"unknown":1}):
            with self.assertRaises(ValueError):customize(base,"sql.scan","tiny",params)

    def test_all_workload_pages_render_without_execution(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        with patch("hibench.worker.launch") as launch:
            app.button(key="nav-Workloads").click().run()
            for workload in IDS:
                family=workload.split(".")[0]
                next(w for w in app.selectbox if w.label=="Workload family").set_value(family).run()
                next(w for w in app.selectbox if w.label=="Workload").set_value(workload).run()
                self.assertFalse(app.exception,workload)
                self.assertTrue(app.dataframe)
            launch.assert_not_called()

    def test_ui_saves_shared_defaults_without_changing_experiment(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-Workloads").click().run()
        next(w for w in app.selectbox if w.label=="Workload family").set_value("sql").run()
        next(w for w in app.selectbox if w.label=="Workload").set_value("sql.scan").run()
        next(w for w in app.checkbox if w.label=="Edit parameters").check().run()
        next(w for w in app.number_input if w.label=="uservisits").set_value(1234)
        before = copy.deepcopy(app.session_state.experiment)
        with patch('hibench.services.runtime.create_adapter'), patch('hibench.ui.workloads.save_defaults', return_value={'changed': False}) as save:
            next(w for w in app.button if w.label=="Save and update target").click().run()
            self.assertEqual(save.call_args.args[0], 'sql.scan')
            self.assertEqual(save.call_args.args[2]['uservisits'], 1234)
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment, before)
        app.button(key="nav-Experiments").click().run()
        self.assertFalse(app.exception)
        self.assertIn("sql.scan",app.multiselect[0].value)
        self.assertEqual(app.session_state.experiment, before)

    def test_experiment_draft_survives_navigation_without_submission(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        with patch("hibench.worker.launch") as launch:
            next(w for w in app.text_input if w.label=="Name").set_value("Unsubmitted draft").run()
            app.button(key="nav-Workloads").click().run()
            app.button(key="nav-Experiments").click().run()
            self.assertFalse(app.exception)
            self.assertEqual(next(w for w in app.text_input if w.label=="Name").value,"Unsubmitted draft")
            launch.assert_not_called()
