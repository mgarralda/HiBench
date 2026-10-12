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

"""History links shared by browser UI and REST API; no remote fetch."""
from urllib.parse import quote, urlsplit


def links(base, application_id):
    parsed = urlsplit(base)
    if parsed.scheme not in ('http', 'https') or not parsed.netloc or parsed.query or parsed.fragment:
        raise ValueError('Use an HTTP(S) History Server base URL without query or fragment.')
    base = base.rstrip('/')
    app = quote(application_id, safe='')
    return base + '/history/' + app + '/', base + '/api/v1/applications/' + app + '/logs'



def application_links(base, events):
    applications = []
    seen = set()
    for event in events:
        ids = event.get('reference', {}).get('metadata', {}).get('application_ids', [])
        app = event.get('metrics', {}).get('application_id')
        for value in [*ids, app]:
            if value and value not in seen:
                seen.add(value)
                history, download = links(base, value)
                applications.append({'application_id': value, 'history_url': history, 'event_logs_url': download})
    # Validate even while no application has been reported yet.
    links(base, '')
    return {'applications': applications, 'scope': 'History Server links; ingestion may be delayed and event logs must remain available.'}
