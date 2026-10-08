"""Classic Spark launchers: all Bash/HDFS/YARN details stay in this adapter."""
import queue
import re
import subprocess
import threading
import time
import uuid
import json
from .base import ExecutionAdapter, JobReference
from ..backend import ClassicBackend, properties, dataset_key

APP_ID = re.compile(r"(?:application_\d+_\d+|app-\d+-\d+|local-\d+)")

class ClassicAdapter(ClassicBackend, ExecutionAdapter):
    def validate_target(self):
        target = self.target
        if target.get("kind") == "docker" and not re.fullmatch(r"[\w.-]+", target.get("container", "")):
            raise ValueError("Invalid Docker container name")
        for key in ("workspace", "spark_home", "hadoop_home", *[k for k in ("hadoop_conf_dir", "report_base") if k in target]):
            path = target.get(key, "")
            if not isinstance(path, str) or not re.fullmatch(r"/[\w./-]+", path) or ".." in path.split("/") or path == "/":
                raise ValueError(f"{key} must be an absolute Linux path without traversal or spaces")
        if target.get("master") not in ("yarn", "local[2]") and not re.fullmatch(r"spark://[\w.-]+:\d+", target.get("master", "")):
            raise ValueError("master must be yarn, local[2] or spark://host:port")
        target.setdefault("deploy_mode", "client")
        if target["deploy_mode"] not in ("client", "cluster"):
            raise ValueError("deploy_mode must be client or cluster")
        if target["deploy_mode"] == "cluster" and target["master"] != "yarn":
            raise ValueError("MVP cluster deploy mode is supported on YARN only")
        if not re.fullmatch(r"(?:hdfs://[\w.-]+(?::\d+)?|wasbs?://[\w.@-]+)", target.get("hdfs_uri", "")):
            raise ValueError("This classic adapter requires an HDFS or WASB/WASBS endpoint")
        for key in ("dataset_base", "output_base"):
            if key in target:
                value = target[key]
                prefix = target["hdfs_uri"] + "/"
                if not isinstance(value, str) or not value.startswith(prefix) or not re.fullmatch(r"/[\w./-]+", value[len(target["hdfs_uri"]):]) or ".." in value[len(prefix):].split("/") or value.rstrip("/") == target["hdfs_uri"]:
                    raise ValueError(f"{key} must be a non-root directory on the configured storage endpoint")
                target[key] = value.rstrip("/")
        if not isinstance(target.get("workers", []), list) or any(not re.fullmatch(r"[\w.-]+", str(x)) for x in target.get("workers", [])):
            raise ValueError("workers must be a list of hostnames")

    def capabilities(self):
        return frozenset({"spark_batch", "mapreduce", "classic_spark", "hdfs"})

    def validate(self, spec):
        from ..catalog import workloads
        catalog = {item["id"]: item for item in workloads()}
        for item in spec["workloads"]:
            required = {"spark_batch"} if spec["mode"] != "prepare" else set()
            if spec["mode"] != "run":
                required.update(catalog[item["id"]]["prepare_capabilities"])
            if catalog[item["id"]]["requires_classic"] and spec["mode"] != "prepare":
                required.add("classic_spark")
            missing = required - self.capabilities()
            if missing:
                raise ValueError(f"{item['id']} requires adapter capabilities: {sorted(missing)}")

    def dataset_key(self, spec, item):
        return dataset_key(spec, item)

    def dataset_root(self, key):
        return self.target.get("dataset_base", self.target["hdfs_uri"] + "/HiBench-Control/datasets") + "/" + key + "-" + uuid.uuid4().hex[:8]

    def stage(self, job):
        remote_dir = self.target.get("report_base", self.root + "/report/control") + f"/{job.run_id}/{job.workload['id']}/{job.index}"
        output = self.target.get("output_base", self.target["hdfs_uri"] + "/HiBench-Control/runs") + f"/{job.run_id}/{job.workload['id']}/{job.index}"
        conf = job.directory / (job.workload['id'] + f"-{job.index}.conf")
        conf.write_text(properties(job.spec, job.workload, job.run_id, job.dataset_root, remote_dir, output), encoding="utf-8")
        remote_conf = remote_dir + "/override.conf"
        self.write_file(conf, remote_conf)
        category, name = job.workload["id"].split(".")
        script = self.root + f"/bin/workloads/{category}/{name}/" + ("prepare/prepare.sh" if job.phase == "prepare" else "spark/run.sh")
        return dict(job=job, argv=self.script_argv(job.run_id, remote_conf, script, remote_dir), pidfile=remote_dir + "/process.pid")

    def submit(self, staged):
        job = staged["job"]
        proc = subprocess.Popen(staged["argv"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace", bufsize=1)
        return JobReference(self.target["kind"], job.run_id, job.phase, str(proc.pid),
                            dict(pidfile=staged["pidfile"], argv=staged["argv"], application_ids=[],
                                 workload=job.workload["id"], index=job.index,
                                 log_path=str(job.directory / "output.log")), proc)

    def _app_ids(self, reference):
        return set(reference.metadata["application_ids"])

    def status(self, reference):
        applications = []
        for app in reference.metadata["application_ids"]:
            if app.startswith("application_") and self.target["master"] == "yarn":
                output = self.command([self.target["hadoop_home"] + "/bin/yarn", "application", "-status", app])
                applications.append(dict(id=app, status=output))
        return_code = reference.runtime.poll() if reference.runtime else None
        state = "unknown" if reference.runtime is None else "running"
        if return_code is not None:
            state = "succeeded" if return_code == 0 else "failed"
        if any("Final-State : FAILED" in app["status"] for app in applications):
            state = "failed"
        elif any("Final-State : KILLED" in app["status"] for app in applications):
            state = "cancelled"
        return dict(state=state, return_code=return_code, applications=applications)

    def logs(self, reference, cursor=0):
        # The collector writes an append-only local log; offsets are bytes.
        from pathlib import Path
        path = Path(reference.metadata["log_path"])
        if not path.exists():
            return dict(cursor=cursor, text="")
        with path.open("rb") as stream:
            stream.seek(cursor)
            text = stream.read().decode("utf-8", errors="replace")
            return dict(cursor=stream.tell(), text=text)

    def cancel(self, reference):
        super().cancel(reference.metadata["pidfile"], self._app_ids(reference), reference.run_id)

    def wait(self, reference, timeout, cancelled, emit, log_path):
        reference.metadata["log_path"] = str(log_path)
        proc = reference.runtime
        app_ids = self._app_ids(reference)
        messages = queue.Queue()
        def read():
            try:
                for line in proc.stdout:
                    messages.put(line)
            finally:
                messages.put(None)
        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        deadline = time.monotonic() + timeout
        ended = False
        stopped = False
        with log_path.open("a", encoding="utf-8") as log:
            try:
                while not ended or proc.poll() is None:
                    try:
                        line = messages.get(timeout=0.25)
                        if line is None:
                            ended = True
                        else:
                            log.write(line)
                            log.flush()
                            app_ids.update(APP_ID.findall(line))
                            if sorted(app_ids) != reference.metadata["application_ids"]:
                                reference.metadata["application_ids"] = sorted(app_ids)
                                emit({"kind": "job_reference", "reference": reference.snapshot()})
                            stripped = line.strip()
                            if stripped.startswith("HIBENCH_RESULT="):
                                emit({"kind": "result", "metrics": json.loads(stripped.split("=", 1)[1])})
                            if stripped.startswith("HIBENCH_INPUT_PATH="):
                                emit({"kind": "input", "path": stripped.split("=", 1)[1]})
                    except queue.Empty:
                        pass
                    timed_out = time.monotonic() > deadline
                    is_cancelled = cancelled()
                    if not stopped and proc.poll() is None and (is_cancelled or timed_out):
                        stopped = True
                        self.cancel(reference)
                        try:
                            proc.wait(timeout=15)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=10)
                        if timed_out:
                            raise TimeoutError("Phase exceeded timeout; cancellation was requested")
                        raise InterruptedError("Cancelled by user")
                return_code = proc.wait()
                if return_code != 0:
                    raise RuntimeError(f"Workload process failed with exit code {return_code}; see output.log")
                if cancelled():
                    raise InterruptedError("Cancelled by user")
                for application in self.status(reference)["applications"]:
                    emit({"kind": "application", **application})
                    if "Final-State : SUCCEEDED" not in application["status"]:
                        raise RuntimeError(f"YARN application {application['id']} did not finish SUCCEEDED")
            finally:
                if proc.poll() is None:
                    self.cancel(reference)
                    proc.terminate()
                    try:
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                reader.join(timeout=5)
                proc.stdout.close()

class DockerAdapter(ClassicAdapter):
    pass

class LocalAdapter(ClassicAdapter):
    pass
