import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench import config
from hibench.adapters import create_adapter, PhaseJob
from hibench.backend import dataset_key

class GeneralSettingsTests(unittest.TestCase):
    def test_storage_and_runtime_settings_reach_staging_and_dataset_identity(self):
        original=config.load("configs/docker-yarn.yaml")
        spec=copy.deepcopy(original)
        spec["target"].update(dataset_base="hdfs://spark-cluster-master:9000/custom/data",output_base="hdfs://spark-cluster-master:9000/custom/output",hadoop_conf_dir="/custom/hadoop-conf",report_base="/custom/reports")
        spec=config.validate(spec);adapter=create_adapter(spec["target"]);item=spec["workloads"][0]
        self.assertNotEqual(dataset_key(spec,item),dataset_key(original,item))
        root=adapter.dataset_root("test")
        self.assertTrue(root.startswith(spec["target"]["dataset_base"]+"/"))
        with tempfile.TemporaryDirectory() as temp, patch.object(adapter,"write_file"):
            job=PhaseJob("test-run",spec,item,"run",1,root,Path(temp))
            staged=adapter.stage(job)
            props=next(Path(temp).glob("*.conf")).read_text()
            self.assertIn("hibench.hadoop.configure.dir /custom/hadoop-conf",props)
            self.assertIn("/custom/output/test-run/",props)
            self.assertIn("/custom/reports/test-run/",staged["pidfile"])
            self.assertIn("HADOOP_CONF_DIR=/custom/hadoop-conf"," ".join(staged["argv"]))

    def test_invalid_roots_are_rejected_and_ha_nameservice_is_accepted(self):
        base=config.load("configs/docker-yarn.yaml")
        for uri in ("hdfs://other:9000/data","hdfs://spark-cluster-master:9000/","hdfs://spark-cluster-master:9000/data/../other","s3a://bucket/data"):
            spec=copy.deepcopy(base);spec["target"]["dataset_base"]=uri
            with self.assertRaises(ValueError):config.validate(spec)
        base["target"]["hdfs_uri"]="hdfs://nameservice1"
        self.assertEqual(config.validate(base)["target"]["hdfs_uri"],"hdfs://nameservice1")

    def test_general_ui_saves_and_preserves_custom_paths(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        self.assertFalse(app.exception)
        next(w for w in app.text_input if w.label=="Dataset base URI").set_value("hdfs://spark-cluster-master:9000/custom/data")
        next(w for w in app.button if w.label=="Apply general settings").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment["target"]["dataset_base"],"hdfs://spark-cluster-master:9000/custom/data")
        app.button(key="nav-Experiments").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment["target"]["dataset_base"],"hdfs://spark-cluster-master:9000/custom/data")
        app.button(key="nav-General").click().run()
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("Amazon S3 (soon)").run()
        self.assertTrue(next(w for w in app.button if w.label=="Apply general settings").disabled)

    def test_azure_event_logs_are_independent_from_hdfs_datasets(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        uri="wasbs://hdp@sparkaccount.blob.core.windows.net/spark3-events"
        next(w for w in app.text_input if w.label=="Event log URI").set_value(uri)
        next(w for w in app.button if w.label=="Apply general settings").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment["spark_conf"]["spark.eventLog.dir"],uri)
        self.assertTrue(app.session_state.experiment["target"]["hdfs_uri"].startswith("hdfs://"))
        self.assertNotIn("spark.history.fs.logDirectory",app.session_state.experiment["spark_conf"])

    def test_wasbs_dataset_storage_can_be_applied(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("Azure Blob Storage (WASB/WASBS)").run()
        endpoint="wasbs://hdp@account.blob.core.windows.net"
        for label,value in (("Storage endpoint",endpoint),("Dataset base URI",endpoint+"/bench/data"),("Output base URI",endpoint+"/bench/results")):
            next(w for w in app.text_input if w.label==label).set_value(value)
        next(w for w in app.button if w.label=="Apply general settings").click().run()
        self.assertFalse(app.exception)
        self.assertEqual(app.session_state.experiment["target"]["hdfs_uri"],endpoint)
        adapter=create_adapter(app.session_state.experiment["target"])
        self.assertTrue(adapter.dataset_root("test").startswith(endpoint+"/bench/data/"))

    def test_provider_switch_replaces_uri_prefixes_without_saving(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        original={label:next(w for w in app.text_input if w.label==label).value for label in ("Storage endpoint","Dataset base URI","Output base URI","Event log URI")}
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("Azure Blob Storage (WASB/WASBS)").run()
        self.assertFalse(app.exception)
        for label,value in original.items():
            self.assertEqual(next(w for w in app.text_input if w.label==label).value,value.replace("hdfs://","wasbs://",1))
        self.assertTrue(app.session_state.experiment["target"]["hdfs_uri"].startswith("hdfs://"))
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("HDFS").run()
        for label,value in original.items():
            self.assertEqual(next(w for w in app.text_input if w.label==label).value,value)
