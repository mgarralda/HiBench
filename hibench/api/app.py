# Copyright 2026 Mariano Garralda-Barrio (HiBench Next).
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

"""Application factory and lifespan: initialize state; optionally own the UI process."""
import asyncio
from contextlib import asynccontextmanager
import os
import secrets
import subprocess
import sys
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import JSONResponse
from hibench.services import configuration
from hibench.catalog import repository_root
from hibench import __version__
from .models import Health
from .routes import router

bearer = HTTPBearer(auto_error=False)


def authorize(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
    token = os.environ.get('HIBENCH_API_TOKEN')
    if token and (credentials is None or not secrets.compare_digest(credentials.credentials, token)):
        raise HTTPException(401, 'Invalid or missing bearer token', headers={'WWW-Authenticate': 'Bearer'})


@asynccontextmanager
async def lifespan(app):
    configuration.initialize()
    process = None
    log = None
    try:
        if os.environ.get('HIBENCH_START_UI') == '1':
            from hibench import store
            log = (store.home() / 'streamlit.log').open('a', encoding='utf-8')
            port = int(os.environ.get('HIBENCH_UI_PORT', '8501'))
            kwargs = {'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}
            process = subprocess.Popen([sys.executable, '-m', 'streamlit', 'run', str(repository_root() / 'hibench/web.py'),
                                        '--server.address=' + os.environ.get('HIBENCH_UI_HOST', '127.0.0.1'), f'--server.port={port}', '--server.headless=true'],
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=log, **kwargs)
            await asyncio.sleep(1)
            if process.poll() is not None:
                raise RuntimeError('Streamlit failed to start; inspect streamlit.log (port may already be in use)')
        app.state.ui_process = process
        yield
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                await asyncio.to_thread(process.wait, timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                await asyncio.to_thread(process.wait)
        if log:
            log.close()
        # Detached benchmark workers and cluster operations retain their own lifecycle.


def create_app():
    app = FastAPI(title='HiBench Next API', version=__version__, lifespan=lifespan,
                  description='Spark batch experiment control. Configuration comes from conf/; run overrides and snapshots remain separate. Use /experiments/resolve for imported presets, then /runs to submit. Cloud adapters marked soon are not enabled.',
                  dependencies=[Depends(authorize)])
    app.include_router(router)

    @app.get('/health', response_model=Health)
    def health():
        return {'status': 'ok', 'api_version': __version__}

    @app.exception_handler(ValueError)
    async def value_error(request, exc):
        return JSONResponse(status_code=404 if str(exc) == 'Unknown run' else 422, content={'detail': str(exc)})

    @app.exception_handler(KeyError)
    async def not_found(request, exc):
        return JSONResponse(status_code=404, content={'detail': str(exc)})

    @app.exception_handler(subprocess.CalledProcessError)
    async def runtime_failure(request, exc):
        output = str(exc.output or '').strip()[-3000:]
        return JSONResponse(status_code=503, content={'detail': 'Target command failed. Check the execution adapter, connectivity, authentication, target paths and runtime installation.' + ('\n' + output if output else '')})

    @app.exception_handler(subprocess.TimeoutExpired)
    async def runtime_timeout(request, exc):
        return JSONResponse(status_code=504, content={'detail': 'Target operation timed out. Check target connectivity and service health before retrying.'})

    @app.exception_handler(OSError)
    async def filesystem_or_process_error(request, exc):
        return JSONResponse(status_code=503, content={'detail': 'Required file or executable is unavailable: ' + str(exc)})

    return app


app = create_app()
