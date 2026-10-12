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

"""Explicit fail-safe reconciliation after a confirmed worker exit."""
import os
import re
from . import store, worker
from .adapters import create_adapter
from .adapters.base import JobReference


def process_alive(pid):
    if not pid:
        raise ValueError('Worker identity unavailable; manual inspection required')
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            if ctypes.get_last_error() == 87:
                return False
            raise RuntimeError('Cannot determine worker liveness safely')
        try:
            code = wintypes.DWORD()
            if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
                raise RuntimeError('Cannot read worker exit state')
            return code.value == 259
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def reconcile(run_id):
    row = store.get(run_id)
    if row['state'] in ('succeeded', 'failed', 'cancelled'):
        return row
    if process_alive(row['worker_pid']):
        raise ValueError('Worker is still alive; use normal cancellation')
    adapter = create_adapter(row['spec']['target'])
    if row['spec']['target']['master'] != 'yarn':
        raise ValueError('Crash reconciliation currently requires YARN')
    references = [e['reference'] for e in worker.events(run_id) if e['kind'] == 'job_reference']
    # Backend cancellation also discovers applications by the exact run tag.
    if references:
        adapter.cancel(JobReference(**references[-1]))
    else:
        # Never remove a lease merely because no application ID was persisted.
        adapter.cancel(JobReference(row['spec']['target']['kind'], run_id, 'recovery', '',
                                    {'pidfile': '/tmp/hibench-no-pid-' + run_id,
                                     'application_ids': []}))
    live = adapter.command([row['spec']['target']['hadoop_home'] + '/bin/yarn',
                            'application', '-list', '-appStates', 'SUBMITTED,ACCEPTED,RUNNING',
                            '-appTags', 'hibench-' + run_id])
    if re.search(r'application_\d+_\d+', live):
        raise RuntimeError('Tagged YARN applications still active; lease retained; retry reconciliation')
    worker.append(run_id, {'kind': 'crash_reconciled', 'worker_pid': row['worker_pid'],
                           'policy': 'cancel tagged jobs, mark failed, release lease; no automatic resubmit',
                           'yarn_active_listing': live})
    store.update(run_id, 'failed', 'Worker exited; remote jobs reconciled; safe to submit a new run')
    store.release(run_id)
    return store.get(run_id)
