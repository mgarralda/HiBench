#!/usr/bin/env python3
"""Run argv, preserve output and status, and capture IDs independently of TTY."""
import json
from pathlib import Path
import re
import subprocess
import sys
import time

APPLICATION_ID = re.compile(r"(?:application_\d+_\d+|app-\d+-\d+|local-\d+)")


def execute(workload_result_file, command_lines):
    log_path = Path(workload_result_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path = Path(str(log_path) + ".execution.json")
    metadata = {"application_id": None, "command": command_lines,
                "started_at": time.time(), "returncode": None}
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
    proc = subprocess.Popen(command_lines, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True,
                            encoding="utf-8", errors="replace", bufsize=1)
    try:
        with log_path.open("w", encoding="utf-8") as log:
            for line in proc.stdout:
                log.write(line)
                log.flush()
                sys.stdout.write(line)
                sys.stdout.flush()
                match = APPLICATION_ID.search(line)
                if match and not metadata["application_id"]:
                    metadata["application_id"] = match.group()
                    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        code = proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        code = 130
    finally:
        proc.stdout.close()
        metadata.update(returncode=proc.returncode, finished_at=time.time())
        metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return code, metadata["application_id"]


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit("Usage: execute_with_log.py <log path> <command> [arguments...]")
    code, _ = execute(sys.argv[1], sys.argv[2:])
    sys.exit(code)
