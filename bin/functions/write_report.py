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

"""Versioned, named benchmark metrics (input bytes, not replicated storage)."""
import csv
import json
from pathlib import Path
import sys
import uuid


def write_report(path, workload, scale, start, end, size, application_id="", run_id=""):
    start, end, size = int(start), int(end), int(size)
    if end < start or size < 0:
        raise ValueError("Invalid duration or input size")
    duration = (end - start) / 1000
    row = dict(schema_version=2, run_id=run_id or str(uuid.uuid4()),
               application_id=application_id, workload=workload, scale=scale,
               started_at_ms=start, finished_at_ms=end, duration_s=duration,
               input_bytes=size, throughput_bytes_s=size / duration if duration else None)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    # Legacy scripts run on Linux; serialize writers to this report across jobs.
    import fcntl
    with target.open("a+", encoding="utf-8", newline="") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.seek(0, 2)
        writer = csv.DictWriter(stream, fieldnames=list(row))
        if stream.tell() == 0:
            writer.writeheader()
        writer.writerow(row)
        stream.flush()
    print("HIBENCH_RESULT=" + json.dumps(row, sort_keys=True), flush=True)
    return row


if __name__ == "__main__":
    if len(sys.argv) != 9:
        sys.exit("Usage: write_report.py <csv> <workload> <scale> <start> <end> <bytes> <app id> <run id>")
    write_report(*sys.argv[1:])
