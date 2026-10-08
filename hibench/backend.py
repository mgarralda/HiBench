"""Classic local/Docker adapter; argv boundaries are preserved through submission."""
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import re
from .catalog import repository_root, workloads
from .config import scalar

ARTIFACTS = (
    "autogen/target/autogen-8.0-SNAPSHOT-jar-with-dependencies.jar",
    "common/target/hibench-common-8.0-SNAPSHOT-jar-with-dependencies.jar",
    "sparkbench/assembly/target/sparkbench-assembly-8.0-SNAPSHOT-dist.jar",
)


class ClassicBackend:
    def __init__(self, target):
        self.target = target
        self.root = target["workspace"]

    def argv(self, command):
        return (["docker", "exec", self.target["container"], *command]
                if self.target["kind"] == "docker" else command)

    def command(self, command, timeout=60):
        if command[0].endswith(("/bin/hdfs", "/bin/yarn")) and "--config" not in command:
            command = [command[0], "--config", self.target.get("hadoop_conf_dir", self.target["hadoop_home"] + "/etc/hadoop"), *command[1:]]
        result = subprocess.run(self.argv(command), text=True, encoding="utf-8", errors="replace",
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode:
            raise subprocess.CalledProcessError(result.returncode, result.args, output=result.stdout)
        return result.stdout

    def doctor(self):
        target = self.target
        script = "set -eu; export HADOOP_CONF_DIR=" + shlex.quote(target.get("hadoop_conf_dir", target["hadoop_home"] + "/etc/hadoop")) + "; java -version; python3 --version; "
        script += shlex.quote(target["spark_home"] + "/bin/spark-submit") + " --version; "
        script += shlex.quote(target["hadoop_home"] + "/bin/hadoop") + " version; "
        for artifact in ARTIFACTS:
            script += "test -f " + shlex.quote(self.root + "/" + artifact) + "; "
            expected = hashlib.sha256((repository_root() / artifact).read_bytes()).hexdigest()
            script += "actual=$(sha256sum " + shlex.quote(self.root + "/" + artifact) + " | cut -d' ' -f1); "
            script += "test \"$actual\" = " + shlex.quote(expected) + " || { echo 'Artifact mismatch: run hibench install'; exit 1; }; "
        repo = repository_root()
        files = {path.relative_to(repo).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
                 for folder in ("bin", "conf") for path in (repo / folder).rglob("*")
                 if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"}
        probe = ("import hashlib,json,sys; from pathlib import Path; root=Path(sys.argv[1]); "
                 "expected=json.loads(sys.argv[2]); "
                 "bad=[name for name,digest in expected.items() if not (root/name).is_file() "
                 "or hashlib.sha256((root/name).read_bytes()).hexdigest()!=digest]; "
                 "assert not bad, 'Runtime files differ; run hibench install: '+', '.join(bad); "
                 "print('HIBENCH_RUNTIME_FILES_OK')")
        script += shlex.join(["python3", "-c", probe, self.root, json.dumps(files)]) + "; "
        script += shlex.quote(target["hadoop_home"] + "/bin/hdfs") + " dfs -ls " + shlex.quote(target["hdfs_uri"] + "/") + "; "
        if target["master"] == "yarn":
            script += shlex.quote(target["hadoop_home"] + "/bin/yarn") + " node -list; "
        script += "echo HIBENCH_DOCTOR_OK"
        output = self.command(["bash", "-lc", script], timeout=120)
        java = re.search(r'(?:openjdk|java) version "(\d+)', output)
        python = re.search(r"Python 3\.(\d+)\.", output)
        if (not re.search(r"version 3\.5\.\d+", output) or "Using Scala version 2.12." not in output
                or not java or int(java.group(1)) not in (11, 17)
                or not python or int(python.group(1)) < 10):
            raise ValueError("This adapter requires Spark 3.5 / Scala 2.12 / Java 11 or 17 / Python 3.10+.\n" + output)
        return output

    def install(self):
        repo = repository_root()
        if self.target["kind"] != "docker":
            if repo != Path(self.root).resolve():
                raise ValueError("Local target must point to the checkout; build it there")
            return "Local checkout is used directly"
        missing = [x for x in ARTIFACTS if not (repo / x).exists()]
        if missing:
            raise ValueError("Build Maven first; missing: " + ", ".join(missing))
        container = self.target["container"]
        # Docker named volumes and docker cp can create root-owned directories.
        subprocess.run(["docker", "exec", "--user", "root", container, "mkdir", "-p", self.root], check=True)
        # Install only runtime sources/config and built JARs; do not copy .git or datasets.
        for directory in ("bin", "conf"):
            subprocess.run(["docker", "cp", str(repo / directory), container + ":" + self.root + "/"], check=True)
        for artifact in ARTIFACTS:
            destination = self.root + "/" + artifact
            subprocess.run(["docker", "exec", "--user", "root", container, "mkdir", "-p", destination.rsplit("/", 1)[0]], check=True)
            subprocess.run(["docker", "cp", str(repo / artifact), container + ":" + destination], check=True)
        identity = self.command(["id", "-u"]).strip() + ":" + self.command(["id", "-g"]).strip()
        subprocess.run(["docker", "exec", "--user", "root", container, "chown", "-R", identity, self.root], check=True)
        self.command(["bash", "-lc", "find " + shlex.quote(self.root + "/bin") + " -name '*.sh' -exec chmod +x {} +"])
        return "Installed batch scripts, configs and JARs in " + container + ":" + self.root

    def write_file(self, local, remote):
        self.command(["mkdir", "-p", remote.rsplit("/", 1)[0]])
        if self.target["kind"] == "docker":
            subprocess.run(["docker", "cp", str(local), self.target["container"] + ":" + remote], check=True)
        else:
            import shutil
            shutil.copyfile(local, remote)

    def dataset_available(self, manifest):
        if not manifest.get("requires_input", True):
            return True
        command = [self.target["hadoop_home"] + "/bin/hdfs", "dfs", "-test", "-e", manifest["input_path"]]
        try:
            self.command(command)
            return True
        except subprocess.CalledProcessError:
            return False

    def script_argv(self, run_id, config_path, script, report_dir):
        # A pid file lets cancellation terminate the actual remote process group,
        # rather than merely closing the Docker client connection.
        pidfile = report_dir + "/process.pid"
        command = "mkdir -p " + shlex.quote(report_dir) + "; cd " + shlex.quote(report_dir)
        command += "; echo $$ > " + shlex.quote(pidfile)
        command += "; exec env " + shlex.join([
            "HIBENCH_PYTHON=python3", "HIBENCH_MONITOR_ENABLED=0", "HIBENCH_RUN_ID=" + run_id,
            "HIBENCH_CONF_FOLDER=" + self.root + "/conf", "HIBENCH_OVERRIDE_FILE=" + config_path,
            "HADOOP_HOME=" + self.target["hadoop_home"],
            "HADOOP_CONF_DIR=" + self.target.get("hadoop_conf_dir", self.target["hadoop_home"] + "/etc/hadoop"),
            "SPARK_HOME=" + self.target["spark_home"], "bash", script])
        return self.argv(["setsid", "--wait", "bash", "-lc", command])

    def cancel(self, pidfile, app_ids, run_id=None):
        errors = []
        app_ids = set(app_ids)
        if run_id and self.target["master"] == "yarn":
            import re
            try:
                listed = self.command([self.target["hadoop_home"] + "/bin/yarn", "application", "-list",
                                       "-appStates", "SUBMITTED,ACCEPTED,RUNNING", "-appTags", "hibench-" + run_id])
                app_ids.update(re.findall(r"application_\d+_\d+", listed))
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                errors.append(str(exc))
        for app in app_ids:
            if app.startswith("application_"):
                try:
                    self.command([self.target["hadoop_home"] + "/bin/yarn", "application", "-kill", app])
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    errors.append(str(exc))
        script = "if test -f " + shlex.quote(pidfile) + "; then "
        script += "pid=$(cat " + shlex.quote(pidfile) + "); case $pid in ''|*[!0-9]*) exit 1;; esac; "
        script += "kill -TERM -- -\"$pid\" 2>/dev/null || true; fi"
        try:
            self.command(["bash", "-lc", script])
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            errors.append(str(exc))
        if errors:
            raise RuntimeError("Cancellation requires attention: " + "; ".join(errors))


def dataset_key(spec, item):
    # Includes actual source artifacts and config, not only a mutable git branch name.
    digest = hashlib.sha256()
    digest.update(json.dumps({"target": spec["target"], "scale": spec["scale"], "item": item,
                              "spark_conf": spec["spark_conf"],
                              "maps": spec["resources"]["map_partitions"],
                              "shuffle": spec["resources"]["shuffle_partitions"]}, sort_keys=True).encode())
    repo = repository_root()
    digest.update((repo / "conf" / "workloads" / Path(*item["id"].split(".")).with_suffix(".conf")).read_bytes())
    for name in ("hadoop.conf", "spark.conf", "hibench.conf"):
        digest.update((repo / "conf" / name).read_bytes())
    category, name = item["id"].split(".")
    digest.update((repo / "bin" / "workloads" / category / name / "prepare" / "prepare.sh").read_bytes())
    for artifact in ARTIFACTS:
        digest.update((repo / artifact).read_bytes())
    return digest.hexdigest()[:24]


def properties(spec, item, run_id, dataset_root, report_dir, output_path):
    target, resources = spec["target"], spec["resources"]
    values = {
        "hibench.spark.home": target["spark_home"], "hibench.spark.master": target["master"],
        "hibench.hadoop.home": target["hadoop_home"],
        "hibench.hadoop.executable": target["hadoop_home"] + "/bin/hadoop",
        "hibench.hadoop.configure.dir": target.get("hadoop_conf_dir", target["hadoop_home"] + "/etc/hadoop"),
        "hibench.hdfs.master": target["hdfs_uri"], "hibench.hdfs.data.dir": dataset_root,
        "hibench.scale.profile": spec["scale"], "hibench.report.dir": report_dir,
        "hibench.report.name": "hibench.csv", "hibench.workload.output": output_path,
        "hibench.default.map.parallelism": resources["map_partitions"],
        "hibench.default.shuffle.parallelism": resources["shuffle_partitions"],
        "hibench.yarn.executor.num": resources["executor_instances"],
        "hibench.yarn.executor.cores": resources["executor_cores"],
        "hibench.yarn.deploy.mode": target["deploy_mode"],
        "hibench.masters.hostnames": target["container"] if target["kind"] == "docker" else "localhost",
        "hibench.slaves.hostnames": " ".join(target.get("workers", [])) or "localhost",
        "spark.executor.instances": resources["executor_instances"],
        "spark.executor.cores": resources["executor_cores"],
        "spark.executor.memory": resources["executor_memory"], "spark.driver.memory": resources["driver_memory"],
        "spark.default.parallelism": resources["map_partitions"],
        "spark.sql.shuffle.partitions": resources["shuffle_partitions"],
        "spark.yarn.tags": "hibench-" + run_id, "spark.app.benchmark.group.id": run_id,
        "spark.yarn.submit.waitAppCompletion": True,
        "spark.extraListeners": "org.hibench.sparkbench.common.BenchmarkListener",
        "mapreduce.map.memory.mb": 1024, "mapreduce.reduce.memory.mb": 1024,
    }
    values.update(spec["spark_conf"])
    schema = next(x for x in workloads() if x["id"] == item["id"])["parameters"]
    values.update({schema[key]["property"]: value for key, value in item["parameters"].items()})
    return "\n".join(key + " " + scalar(value) for key, value in sorted(values.items())) + "\n"
