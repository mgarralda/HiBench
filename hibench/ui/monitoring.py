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

"""Render persisted telemetry; no dependency on a running cluster."""
import streamlit as st
from hibench.services import monitoring


def render(run_id):
    result = monitoring.reports(run_id)
    st.caption('Resource monitoring · ' + result['status'].title())
    if not result['reports']:
        st.info({'disabled': 'Resource monitoring was disabled for this run.',
                 'pending': 'Reports will appear after each monitored phase finishes.',
                 'unavailable': 'No monitoring report was collected for this run.'}.get(result['status'], 'No reports.'))
        return
    selected = st.selectbox('Workload / phase / execution index', [r['id'] for r in result['reports']], key='telemetry-'+run_id)
    metadata = next(r for r in result['reports'] if r['id'] == selected)
    st.write(f"**{metadata['status'].title()}** · {metadata['sample_count']} samples · {len(metadata['observed_nodes'])} nodes · {metadata['interval_s']} s interval")
    st.caption(metadata['scope'])
    if metadata['errors']:
        with st.expander('Collection diagnostics'):
            st.json(metadata['errors'])
    with st.expander('Coverage and metadata'):
        st.json(metadata)
    for filename, label, mime in [('samples.jsonl', 'Download raw samples', 'application/x-ndjson'),
                                  ('metadata.json', 'Download monitoring metadata', 'application/json')]:
        try:
            st.download_button(label, monitoring.artifact(run_id, selected, filename).read_bytes(),
                               selected+'-'+filename, mime, key=selected+filename)
        except KeyError:
            pass
    if metadata['report_available']:
        report = monitoring.artifact(run_id, selected, 'report.html').read_text(encoding='utf-8')
        st.download_button('Download resource report', report, selected+'.html', 'text/html', key=selected+'html')
        with st.expander('View resource report', expanded=True):
            st.iframe(report, height=850, alt='Worker resource report')
