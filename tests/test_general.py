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
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench import config
from hibench.adapters import create_adapter, PhaseJob
from hibench.adapters.runtime import dataset_key

class GeneralSettingsTests(unittest.TestCase):
    @patch('hibench.adapters.runtime.ARTIFACTS', ())
    def test_storage_and_runtime_settings_reach_staging_and_dataset_identity(self):
        original=config.default_experiment()
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
        base=config.default_experiment()
        for uri in ("hdfs://other:9000/data","hdfs://spark-cluster-master:9000/","hdfs://spark-cluster-master:9000/data/../other","s3a://bucket/data"):
            spec=copy.deepcopy(base);spec["target"]["dataset_base"]=uri
            with self.assertRaises(ValueError):config.validate(spec)
        base["target"]["hdfs_uri"]="hdfs://nameservice1"
        self.assertEqual(config.validate(base)["target"]["hdfs_uri"],"hdfs://nameservice1")

    def test_general_ui_saves_and_preserves_custom_paths(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/ui/app.py").resolve())).run(timeout=20)
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

    def test_general_no_longer_duplicates_shared_spark_settings(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/ui/app.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        self.assertFalse(any(w.label == 'Event log URI' for w in app.text_input))
        app.button(key='nav-SparkConfiguration').click().run()
        self.assertFalse(app.exception)
        self.assertTrue(any(w.label.startswith('Spark properties for ') for w in app.text_area))
        self.assertTrue(app.session_state.experiment["target"]["hdfs_uri"].startswith("hdfs://"))
        self.assertNotIn("spark.history.fs.logDirectory",app.session_state.experiment["spark_conf"])

    def test_wasbs_dataset_storage_can_be_applied(self):
        from streamlit.testing.v1 import AppTest
        app=AppTest.from_file(str(Path("hibench/ui/app.py").resolve())).run(timeout=20)
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
        app=AppTest.from_file(str(Path("hibench/ui/app.py").resolve())).run(timeout=20)
        app.button(key="nav-General").click().run()
        original={label:next(w for w in app.text_input if w.label==label).value for label in ("Storage endpoint","Dataset base URI","Output base URI")}
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("Azure Blob Storage (WASB/WASBS)").run()
        self.assertFalse(app.exception)
        for label,value in original.items():
            self.assertEqual(next(w for w in app.text_input if w.label==label).value,value.replace("hdfs://","wasbs://",1))
        self.assertTrue(app.session_state.experiment["target"]["hdfs_uri"].startswith("hdfs://"))
        next(w for w in app.selectbox if w.label=="Dataset storage backend").set_value("HDFS").run()
        for label,value in original.items():
            self.assertEqual(next(w for w in app.text_input if w.label==label).value,value)

    def test_general_fields_follow_a_replaced_experiment_target(self):
        from streamlit.testing.v1 import AppTest
        import copy
        app = AppTest.from_file(str(Path('hibench/ui/app.py').resolve())).run(timeout=20)
        app.button(key='nav-General').click().run()
        draft = copy.deepcopy(app.session_state.experiment)
        endpoint = 'wasbs://benchmark@account.blob.core.windows.net'
        draft['target']['hdfs_uri'] = endpoint
        draft['target']['dataset_base'] = endpoint + '/new-data'
        draft['target']['output_base'] = endpoint + '/new-output'
        app.session_state.experiment = draft
        app.run()
        self.assertFalse(app.exception)
        self.assertEqual(next(w for w in app.text_input if w.label == 'Storage endpoint').value, endpoint)
        self.assertEqual(next(w for w in app.text_input if w.label == 'Dataset base URI').value, endpoint + '/new-data')
        self.assertEqual(next(w for w in app.selectbox if w.label == 'Dataset storage backend').value, 'Azure Blob Storage (WASB/WASBS)')
