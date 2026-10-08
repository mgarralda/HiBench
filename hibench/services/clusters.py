"""Cluster lifecycle service, independent of Streamlit and benchmark submission."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid
from hibench import store
from hibench.catalog import repository_root

REPOSITORY = "https://github.com/mgarralda/hadoop-spark-cluster.git"
IMAGES = (("Dockerfile", "hadoop_spark_base_image"), ("master/Dockerfile", "spark_master"),
          ("slave/Dockerfile", "spark_slave"), ("Dockerfile.jupyter", "spark_jupyter"))

def home():
    path = store.home() / "clusters"
    path.mkdir(exist_ok=True)
    return path

def profile():
    path = home() / "profile.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else dict(
        directory=str(repository_root().parent / "hadoop-spark-cluster"), project="spark-cluster", ref="main")

def validate(profile):
    directory = Path(profile["directory"]).expanduser()
    if not directory.is_absolute() or directory == Path(directory.anchor):
        raise ValueError("The directory must be absolute and cannot be a filesystem root")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", profile["project"]):
        raise ValueError("Invalid Compose project name")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", profile["ref"]):
        raise ValueError("Invalid branch or tag")
    return {**profile, "directory": str(directory.resolve())}

def save(profile):
    profile = validate(profile)
    path = home() / "profile.json"
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    os.replace(temporary, path)

def compose(profile):
    profile = validate(profile)
    path = Path(profile["directory"]) / "docker-compose.yml"
    if not path.is_file():
        raise ValueError("docker-compose.yml is missing; download or select the project first")
    return ["docker", "compose", "--project-directory", profile["directory"], "-f", str(path), "-p", profile["project"]]

def plan(profile, action):
    profile = validate(profile)
    if action == "download":
        destination = Path(profile["directory"])
        if destination.exists():
            raise ValueError("Download requires a new directory; existing projects are not overwritten")
        return [["git", "clone", "--depth", "1", "--branch", profile["ref"], REPOSITORY, str(destination)]]
    cmd = compose(profile)
    if action == "build":
        return [["docker", "build", "-f", str(Path(profile["directory"]) / file), "-t", tag, profile["directory"]]
                for file, tag in IMAGES]
    commands = {"up": ["up", "-d", "--wait", "--wait-timeout", "300"],
                "stop": ["stop"], "down": ["down"], "status": ["ps", "--all", "--format", "json"]}
    if action not in commands:
        raise ValueError("Unknown cluster operation")
    return [cmd + commands[action]]

def active_benchmarks():
    with store.connect() as db:
        return db.execute("SELECT COUNT(*) FROM runs WHERE state IN ('queued','waiting','running')").fetchone()[0]

def status(profile):
    result = subprocess.run(plan(profile, "status")[0], capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=30)
    if result.returncode:
        raise RuntimeError(result.stderr or result.stdout)
    output = result.stdout.strip()
    if not output:
        return []
    rows = json.loads(output) if output.startswith("[") else [json.loads(line) for line in output.splitlines()]
    return [{key: row.get(key, "") for key in ("Name", "Service", "State", "Health", "Status")} for row in rows]

def start(profile, action):
    if action in ("stop", "down", "build", "up") and active_benchmarks():
        raise ValueError("Benchmarks are queued or running; finish them before modifying the cluster")
    commands = plan(profile, action)
    lock = home() / "operation.lock"
    try:
        lock.mkdir()
    except FileExistsError:
        raise ValueError("A cluster operation is already in progress; check its monitoring panel")
    operation = uuid.uuid4().hex
    directory = home() / operation
    try:
        directory.mkdir()
        task = dict(id=operation, action=action, profile=validate(profile), commands=commands, state="queued", created=time.time())
        (directory / "task.json").write_text(json.dumps(task, indent=2), encoding="utf-8")
        (home() / "latest.txt").write_text(operation, encoding="utf-8")
        env = os.environ.copy()
        env["HIBENCH_STATE_DIR"] = str(store.home())
        env["PYTHONPATH"] = str(repository_root()) + os.pathsep + env.get("PYTHONPATH", "")
        kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {"start_new_session": True}
        with (directory / "output.log").open("a", encoding="utf-8") as log:
            subprocess.Popen([sys.executable, "-m", "hibench.services.clusters", operation], env=env,
                             stdin=subprocess.DEVNULL, stdout=log, stderr=log, **kwargs)
        return operation
    except Exception:
        lock.rmdir()
        raise

def latest():
    path = home() / "latest.txt"
    if not path.exists():
        return None
    directory = home() / path.read_text(encoding="utf-8").strip()
    task = json.loads((directory / "task.json").read_text(encoding="utf-8"))
    log = directory / "output.log"
    return task, log.read_text(encoding="utf-8", errors="replace")[-40000:] if log.exists() else ""

def work(operation):
    if not re.fullmatch(r"[a-f0-9]{32}", operation):
        raise ValueError("Invalid operation ID")
    directory = home() / operation
    path = directory / "task.json"
    task = json.loads(path.read_text(encoding="utf-8"))
    def update(state, detail=""):
        task.update(state=state, detail=detail, updated=time.time())
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(task, indent=2), encoding="utf-8")
        os.replace(tmp, path)
    try:
        update("running")
        # Revalidate the selected action instead of executing arbitrary stored commands.
        for command in plan(task["profile"], task["action"]):
            print(subprocess.list2cmdline(command), flush=True)
            result = subprocess.run(command, cwd=task["profile"]["directory"] if task["action"] != "download" else None,
                                    timeout=7200, check=True)
        update("succeeded")
    except Exception as exc:
        print(str(exc), flush=True)
        update("failed", str(exc))
    finally:
        (home() / "operation.lock").rmdir()

if __name__ == "__main__":
    work(sys.argv[1])
