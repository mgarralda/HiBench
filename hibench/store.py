"""Durable runs and destination leases: reloads do not restart jobs."""
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid
from contextlib import contextmanager
from .catalog import repository_root


def home():
    path = Path(os.environ.get("HIBENCH_STATE_DIR", repository_root() / "report" / "control")).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def connect():
    db = sqlite3.connect(home() / "runs.sqlite", timeout=30)
    db.row_factory = sqlite3.Row
    schema = """
      CREATE TABLE IF NOT EXISTS runs (
        id TEXT PRIMARY KEY, request_key TEXT UNIQUE NOT NULL,
        state TEXT NOT NULL, spec TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
        cancel INTEGER NOT NULL DEFAULT 0, detail TEXT NOT NULL DEFAULT '', worker_pid INTEGER);
      CREATE TABLE IF NOT EXISTS leases (target TEXT PRIMARY KEY, run_id TEXT UNIQUE NOT NULL);
    """
    try:
        for attempt in range(6):
            try:
                # Switching a newly created database to WAL can return SQLITE_BUSY
                # immediately when another process is initializing it too.
                if db.execute("PRAGMA journal_mode").fetchone()[0] != "wal":
                    db.execute("PRAGMA journal_mode=WAL")
                db.executescript(schema)
                break
            except sqlite3.OperationalError as exc:
                if attempt == 5 or not any(word in str(exc).lower() for word in ("locked", "busy")):
                    raise
                time.sleep(0.05 * (2 ** attempt))
        with db:
            yield db
    finally:
        db.close()


def create(spec, request_key=None):
    from .config import validate
    spec = validate(spec)
    request_key = request_key or str(uuid.uuid4())
    with connect() as db:
        db.execute("BEGIN IMMEDIATE")
        old = db.execute("SELECT * FROM runs WHERE request_key=?", (request_key,)).fetchone()
        if old:
            if json.loads(old["spec"]) != spec:
                raise ValueError("This request key belongs to a different experiment")
            return old["id"], False
        run_id = str(uuid.uuid4())
        now = time.time()
        db.execute("INSERT INTO runs(id,request_key,state,spec,created,updated) VALUES(?,?,?,?,?,?)",
                   (run_id, request_key, "queued", json.dumps(spec, sort_keys=True), now, now))
        directory(run_id).mkdir(parents=True, exist_ok=True)
        (directory(run_id) / "experiment.json").write_text(json.dumps(spec, indent=2), encoding="utf-8")
    return run_id, True


def directory(run_id):
    uuid.UUID(run_id)
    return home() / "runs" / run_id


def get(run_id):
    with connect() as db:
        row = db.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
    if not row:
        raise ValueError("Unknown run")
    result = dict(row)
    result["spec"] = json.loads(result["spec"])
    return result


def recent():
    with connect() as db:
        return [dict(row) for row in db.execute("SELECT id,state,created,updated,detail FROM runs ORDER BY created DESC LIMIT 100")]


def update(run_id, state=None, detail=None, **fields):
    allowed = {"cancel", "worker_pid"}
    if set(fields) - allowed:
        raise ValueError("Invalid run field")
    if state is not None:
        fields["state"] = state
    if detail is not None:
        fields["detail"] = detail
    fields["updated"] = time.time()
    with connect() as db:
        db.execute("UPDATE runs SET " + ",".join(k + "=?" for k in fields) + " WHERE id=?",
                   (*fields.values(), run_id))


def claim(run_id):
    with connect() as db:
        changed = db.execute("UPDATE runs SET state='waiting',worker_pid=?,updated=? WHERE id=? AND state='queued'",
                             (os.getpid(), time.time(), run_id)).rowcount
    return bool(changed)


def acquire(run_id, target):
    key = json.dumps({k: target.get(k) for k in ("kind", "container", "workspace")}, sort_keys=True)
    with connect() as db:
        try:
            db.execute("INSERT INTO leases(target,run_id) VALUES(?,?)", (key, run_id))
            return True
        except sqlite3.IntegrityError:
            return False


def release(run_id):
    with connect() as db:
        db.execute("DELETE FROM leases WHERE run_id=?", (run_id,))


def cancel(run_id):
    row = get(run_id)
    if row["state"] in ("succeeded", "failed", "cancelled"):
        return
    update(run_id, cancel=1, detail="Cancellation requested")
