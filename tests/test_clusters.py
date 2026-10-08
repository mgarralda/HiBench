import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from hibench.services import clusters

class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {"HIBENCH_STATE_DIR": self.tmp.name})
        self.env.start()
        self.root = Path(self.tmp.name) / "lab"
        self.root.mkdir()
        (self.root / "docker-compose.yml").write_text("services: {}")
        self.profile = dict(directory=str(self.root), project="spark-cluster", ref="main")
    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()
    def test_lifecycle_plans_preserve_volumes_and_project(self):
        cmd = clusters.plan(self.profile, "down")[0]
        self.assertNotIn("-v", cmd)
        self.assertEqual(cmd[cmd.index("-p") + 1], "spark-cluster")
        self.assertEqual(clusters.plan(self.profile, "stop")[0][-1], "stop")
        self.assertIn("--wait", clusters.plan(self.profile, "up")[0])
        self.assertEqual(len(clusters.plan(self.profile, "build")), 4)
        with self.assertRaises(ValueError): clusters.plan(self.profile, "download")
    def test_profile_and_download_arguments(self):
        clusters.save(self.profile)
        self.assertEqual(clusters.profile(), clusters.validate(self.profile))
        profile = {**self.profile, "directory": str(Path(self.tmp.name) / "new lab")}
        cmd = clusters.plan(profile, "download")[0]
        self.assertEqual(cmd[-1], clusters.validate(profile)["directory"])
        self.assertIn(clusters.REPOSITORY, cmd)
        with self.assertRaises(ValueError): clusters.validate({**profile, "ref": "--upload-pack=bad"})
    def test_detached_operation_and_duplicate_protection(self):
        with patch("hibench.services.clusters.subprocess.Popen") as launch:
            operation = clusters.start(self.profile, "up")
            launch.assert_called_once()
            with self.assertRaises(ValueError): clusters.start(self.profile, "up")
        with patch("hibench.services.clusters.subprocess.run") as execute:
            clusters.work(operation)
            execute.assert_called_once()
        task, log = clusters.latest()
        self.assertEqual(task["state"], "succeeded")
        self.assertFalse((clusters.home() / "operation.lock").exists())
        with patch("hibench.services.clusters.active_benchmarks", return_value=1):
            with self.assertRaises(ValueError): clusters.start(self.profile, "stop")
    def test_navigation_pages_do_not_change_cluster_automatically(self):
        from streamlit.testing.v1 import AppTest
        app = AppTest.from_file(str(Path("hibench/web.py").resolve())).run(timeout=20)
        with patch("hibench.services.clusters.subprocess.run") as command:
            for page in ("Runs", "Clusters", "Workloads", "Help", "Experiments"):
                app.button(key="nav-" + page).click().run()
                self.assertFalse(app.exception)
            command.assert_not_called()
