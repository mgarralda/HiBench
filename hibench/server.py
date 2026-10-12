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

"""Unified local launcher: API plus optional Streamlit supervised by lifespan."""
import argparse
import os
import uvicorn


def main():
    parser = argparse.ArgumentParser(description='HiBench Next API and optional web UI')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8000)
    parser.add_argument('--with-ui', action='store_true')
    parser.add_argument('--ui-port', type=int, default=8501)
    args = parser.parse_args()
    if args.host not in ('127.0.0.1', 'localhost', '::1') and not os.environ.get('HIBENCH_API_TOKEN'):
        parser.error('Set HIBENCH_API_TOKEN before binding to a non-loopback address')
    os.environ['HIBENCH_START_UI'] = '1' if args.with_ui else '0'
    os.environ['HIBENCH_UI_PORT'] = str(args.ui_port)
    uvicorn.run('hibench.api.app:app', host=args.host, port=args.port, workers=1)


if __name__ == '__main__':
    main()
