import copy
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench import config
from hibench.catalog import IDS, SCALES
from hibench.workload_parameters import customize, details, typed_value

class WorkloadParameterTests(unittest.TestCase):
    def test_every_workload_and_scale_can_be_inspected_and_applied(self):
        base=config.load("configs/docker-yarn.yaml")
        for workload in IDS:
            item,rows,other,path=details(workload)
            before=path.read_bytes()
            self.assertEqual(len(rows),len(item["parameters"]))
            for scale in SCALES:
                values={k:typed_value(s,s["presets"][scale]) for k,s in item["parameters"].items()}
                result=customize(base,workload,scale,values)
                self.assertEqual(result["scale"],scale)
                self.assertEqual(next(w for w in result["workloads"] if w["id"]==workload)["parameters"],{})
            self.assertEqual(path.read_bytes(),before)

    def test_customizations_are_validated_without_mutating_reference_or_draft(self):
        base=config.load("configs/docker-yarn.yaml");original=copy.deepcopy(base)
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

    def test_ui_applies_overrides_and_returns_to_experiment(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-Workloads").click().run()
        next(w for w in app.selectbox if w.label=="Workload family").set_value("sql").run()
        next(w for w in app.selectbox if w.label=="Workload").set_value("sql.scan").run()
        next(w for w in app.checkbox if w.label=="Edit parameters").check().run()
        next(w for w in app.number_input if w.label=="uservisits").set_value(1234)
        next(w for w in app.button if w.label=="Use in experiment").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(next(w for w in app.session_state.experiment["workloads"] if w["id"]=="sql.scan")["parameters"],{"uservisits":1234})
        app.button(key="nav-Experiments").click().run()
        self.assertFalse(app.exception)
        self.assertIn("sql.scan",app.multiselect[0].value)
        self.assertEqual(next(w for w in app.number_input if w.key=="sql.scan:uservisits:tiny").value,1234)

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
