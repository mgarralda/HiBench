"""Detached worker with durable status, logs, dataset manifests and cancellation."""
import json
import os
import subprocess
import sys
import time
from . import store
from .adapters import create_adapter, PhaseJob
from .catalog import requires_input as workload_requires_input



def launch(run_id):
    env = os.environ.copy()
    from .catalog import repository_root
    env["PYTHONPATH"] = str(repository_root()) + os.pathsep + env.get("PYTHONPATH", "")
    env["HIBENCH_STATE_DIR"] = str(store.home())
    stream = (store.directory(run_id) / "worker.log").open("a", encoding="utf-8")
    kwargs = {"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {"start_new_session": True}
    with stream:
        proc = subprocess.Popen([sys.executable, "-m", "hibench.worker", run_id], env=env,
                                stdin=subprocess.DEVNULL, stdout=stream, stderr=stream, **kwargs)
    return proc.pid


def append(run_id, value):
    with (store.directory(run_id) / "events.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"at": time.time(), **value}, ensure_ascii=False) + "\n")


def events(run_id):
    path = store.directory(run_id) / "events.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()] if path.exists() else []


def work(run_id):
    if not store.claim(run_id):
        return
    try:
        spec = store.get(run_id)["spec"]
        backend = create_adapter(spec["target"])
        backend.validate(spec)
        waiting_since = time.monotonic()
        while not store.acquire(run_id, spec["target"]):
            if store.get(run_id)["cancel"]:
                raise InterruptedError("Cancelled before starting")
            if time.monotonic() - waiting_since > spec["timeout_s"]:
                raise TimeoutError("Timed out waiting for another experiment on this destination")
            time.sleep(1)
        if store.get(run_id)["cancel"]:
            raise InterruptedError("Cancelled before starting")
        store.update(run_id, "running", "Checking destination")
        append(run_id, {"kind": "doctor", "output": backend.doctor()})
        directory = store.directory(run_id)
        for item in spec["workloads"]:
            requires_input = workload_requires_input(item)
            key = backend.dataset_key(spec, item)
            manifest_dir = store.home() / "datasets"
            manifest_dir.mkdir(exist_ok=True)
            manifest_path = manifest_dir / (key + ".json")
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
            except json.JSONDecodeError:
                manifest = None
                append(run_id, {"kind": "invalid_dataset_manifest", "path": str(manifest_path)})
            available = manifest and backend.dataset_available(manifest)
            if spec["mode"] == "run" and requires_input and not available:
                raise ValueError(f"No completed matching dataset for {item['id']}; choose preparation first")
            dataset_root = manifest["root"] if available else backend.dataset_root(key)
            phases = ([] if available or spec["mode"] == "run" else ["prepare"])
            if spec["mode"] != "prepare":
                phases += ["run"] * spec["repetitions"]
            if available:
                append(run_id, {"kind": "dataset_reused", "workload": item["id"], "manifest": manifest})
            for phase_index, phase in enumerate(phases):
                if store.get(run_id)["cancel"]:
                    raise InterruptedError("Cancelled between phases")
                label = f"{item['id']}/{phase}/{phase_index}"
                store.update(run_id, detail=label)
                append(run_id, {"kind": "phase_started", "workload": item["id"], "phase": phase, "index": phase_index})
                job = PhaseJob(run_id, spec, item, phase, phase_index, dataset_root, directory)
                staged = backend.stage(job)
                reference = backend.submit(staged)
                append(run_id, {"kind": "job_reference", "reference": reference.snapshot()})
                backend.wait(reference, spec["timeout_s"], lambda: bool(store.get(run_id)["cancel"]),
                             lambda event: append(run_id, event), directory / "output.log")
                if phase == "prepare":
                    inputs = [x["path"] for x in events(run_id) if x["kind"] == "input"]
                    if not inputs and requires_input:
                        raise RuntimeError("Preparation did not identify its effective dataset input")
                    manifest = dict(schema_version=1, key=key, root=dataset_root, input_path=inputs[-1] if inputs else None,
                                    requires_input=requires_input,
                                    workload=item, scale=spec["scale"], prepared_by=run_id, completed_at=time.time())
                    if not backend.dataset_available(manifest):
                        raise RuntimeError("Prepared dataset does not exist")
                    temporary = manifest_path.with_suffix(".tmp-" + run_id)
                    temporary.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                    os.replace(temporary, manifest_path)
                    append(run_id, {"kind": "dataset_prepared", "manifest": manifest})
                else:
                    phase_events = events(run_id)
                    last_start = max(i for i, e in enumerate(phase_events) if e["kind"] == "phase_started")
                    if not any(e["kind"] == "result" for e in phase_events[last_start:]):
                        raise RuntimeError("Job exited successfully but did not produce benchmark metrics")
                append(run_id, {"kind": "phase_finished", "workload": item["id"], "phase": phase, "index": phase_index})
        store.update(run_id, "succeeded", "All requested phases completed")
    except InterruptedError as exc:
        store.update(run_id, "cancelled", str(exc))
    except Exception as exc:
        detail = str(exc) + ("\n" + exc.output if getattr(exc, "output", None) else "")
        append(run_id, {"kind": "error", "message": detail})
        store.update(run_id, "failed", detail)
        raise
    finally:
        store.release(run_id)


if __name__ == "__main__":
    work(sys.argv[1])
