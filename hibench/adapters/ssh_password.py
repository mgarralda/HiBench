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

"""Password-only SSH subprocess bridge; credentials come exclusively from env."""
import json
import os
from pathlib import Path
import shlex
import sys
import threading
import time
import paramiko


def execute(target, command, stream_input=False):
    password = os.environ.get('HIBENCH_SSH_PASSWORD')
    if not password:
        raise ValueError('HIBENCH_SSH_PASSWORD is required for password authentication')
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    known = target.get('known_hosts_file')
    if known:
        client.load_host_keys(str(Path(known).expanduser()))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    try:
        client.connect(hostname=target['host'], port=target.get('port', 22),
                       username=target['username'], password=password,
                       allow_agent=False, look_for_keys=False,
                       timeout=10, auth_timeout=10, banner_timeout=10)
        transport = client.get_transport()
        transport.set_keepalive(15)
        channel = transport.open_session(timeout=10)
        channel.set_combine_stderr(True)
        channel.exec_command('exec ' + shlex.join(command))
        errors = []
        def upload():
            try:
                while chunk := sys.stdin.buffer.read(65536):
                    channel.sendall(chunk)
                channel.shutdown_write()
            except Exception as exc:
                errors.append(exc)
        uploader = None
        if stream_input:
            uploader = threading.Thread(target=upload, daemon=True)
            uploader.start()
        else:
            channel.shutdown_write()
        while True:
            if channel.recv_ready():
                chunk = channel.recv(65536)
                if not chunk:
                    break
                sys.stdout.buffer.write(chunk)
                sys.stdout.buffer.flush()
            elif channel.exit_status_ready():
                break
            elif channel.closed:
                break
            else:
                time.sleep(0.02)
        status = channel.recv_exit_status()
        if uploader:
            uploader.join(timeout=5)
        if errors and status == 0:
            raise RuntimeError('SSH input transfer failed')
        return status if status >= 0 else 1
    finally:
        client.close()


def main():
    password = os.environ.get('HIBENCH_SSH_PASSWORD', '')
    try:
        payload = json.loads(sys.argv[1])
        return execute(payload['target'], payload['command'], payload.get('stream_input', False))
    except Exception as exc:
        message = str(exc)
        if password:
            message = message.replace(password, '[redacted]')
        print('SSH connection/command failed: ' + message, file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
