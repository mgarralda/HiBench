#!/usr/bin/env python3
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

"""Run argv, preserve output and status, and capture IDs independently of TTY."""
import json
import os
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
    environment = os.environ.copy()
    if Path(command_lines[0]).name == 'spark-submit' and '--master' in command_lines and command_lines[command_lines.index('--master') + 1] == 'yarn':
        # Cluster-mode drivers do not print to this console. Keep the submission
        # client's application ID visible even when the cluster uses ERROR logs.
        logging_path = log_path.with_suffix('.submit-log4j2.properties').resolve()
        logging_path.write_text('''status = error
appender.console.type = Console
appender.console.name = console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %m%n
rootLogger.level = error
rootLogger.appenderRef.console.ref = console
logger.yarnclient.name = org.apache.spark.deploy.yarn.Client
logger.yarnclient.level = info
''', encoding='utf-8')
        environment['SPARK_SUBMIT_OPTS'] = environment.get('SPARK_SUBMIT_OPTS', '') + ' -Dlog4j.configurationFile=' + str(logging_path)
    proc = subprocess.Popen(command_lines, stdout=subprocess.PIPE, env=environment,
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
