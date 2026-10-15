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

"""OpenSSH transport for the same Linux Spark/HDFS/YARN runtime contract."""
from pathlib import Path
import os
import json
import re
import shlex
import subprocess
import tarfile
import tempfile
import sys
from .classic import ClassicAdapter
from .runtime import ARTIFACTS, RUNTIME_RESOURCES
from ..catalog import repository_root


def connection_defaults():
    """Optional deployment settings; explicit experiment values take precedence."""
    values = {key: os.environ['HIBENCH_SSH_' + key.upper()] for key in
              ('host', 'username', 'port', 'identity_file', 'known_hosts_file')
              if os.environ.get('HIBENCH_SSH_' + key.upper())}
    if 'port' in values:
        values['port'] = int(values['port'])
    return values


class SSHAdapter(ClassicAdapter):
    def validate_target(self):
        super().validate_target()
        target = self.target
        for key, value in connection_defaults().items():
            target.setdefault(key, value)
        if not isinstance(target.get('host'), str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.-]{0,252}', target['host']):
            raise ValueError('SSH host must be a hostname or IPv4 address without shell characters')
        if not isinstance(target.get('username'), str) or not re.fullmatch(r'[a-z_][a-z0-9_.-]*', target['username']):
            raise ValueError('SSH username must be a Linux account name')
        target.setdefault('port', 22)
        if type(target['port']) is not int or not 1 <= target['port'] <= 65535:
            raise ValueError('SSH port must be an integer from 1 to 65535')
        for key in ('identity_file', 'known_hosts_file'):
            if key in target:
                value = target[key]
                if not isinstance(value, str) or not value.strip() or any(ord(c) < 32 for c in value):
                    raise ValueError(key + ' must be a controller file path')

    def argv(self, command, stream_input=False):
        target = self.target
        if os.environ.get('HIBENCH_SSH_PASSWORD') and not target.get('identity_file'):
            connection = {key: target[key] for key in ('host', 'username', 'port', 'known_hosts_file') if key in target}
            return [sys.executable, '-m', 'hibench.adapters.ssh_password',
                    json.dumps(dict(target=connection, command=command, stream_input=stream_input))]
        args = ['ssh', '-T', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
                '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=15',
                '-o', 'ServerAliveCountMax=3', '-p', str(target.get('port', 22))]
        if target.get('identity_file'):
            args += ['-i', str(Path(target['identity_file']).expanduser()), '-o', 'IdentitiesOnly=yes']
        if target.get('known_hosts_file'):
            args += ['-o', 'UserKnownHostsFile=' + str(Path(target['known_hosts_file']).expanduser())]
        return args + ['-l', target['username'], target['host'], 'exec ' + shlex.join(command)]

    def _send(self, command, stream, timeout=120):
        result = subprocess.run(self.argv(command, stream_input=True), stdin=stream, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode:
            raise subprocess.CalledProcessError(result.returncode, result.args,
                                               output=result.stdout.decode('utf-8', errors='replace'))

    def write_file(self, local, remote):
        self.command(['mkdir', '-p', remote.rsplit('/', 1)[0]])
        with Path(local).open('rb') as stream:
            self._send(['sh', '-c', 'cat > ' + shlex.quote(remote)], stream)

    def install(self):
        repo = repository_root()
        missing = [name for name in ARTIFACTS + RUNTIME_RESOURCES if not (repo / name).is_file()]
        if missing:
            raise ValueError('Build Maven first; missing: ' + ', '.join(missing))
        self.command(['mkdir', '-p', self.root])
        def filtered(member):
            return None if '__pycache__' in Path(member.name).parts or member.name.endswith('.pyc') else member
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as stream:
            with tarfile.open(fileobj=stream, mode='w') as archive:
                for name in ('bin', 'conf', 'pysparkbench', *ARTIFACTS, *RUNTIME_RESOURCES):
                    archive.add(repo / name, arcname=name, filter=filtered)
            stream.seek(0)
            self._send(['tar', '--no-same-owner', '-xf', '-', '-C', self.root], stream)
        self.command(['bash', '-lc', 'find ' + shlex.quote(self.root + '/bin') + " -name '*.sh' -exec chmod +x {} +"])
        return 'Installed batch runtime over SSH in ' + self.target['host'] + ':' + self.root

