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

"""Browser links to Spark's application UI and original event-log archive."""
from hibench.services.spark_history import links, application_links
import streamlit as st


def render(run_id, spec, events):
    with st.expander('Spark application history'):
        configured = str(spec.get('spark_conf', {}).get('spark.yarn.historyServer.address', ''))
        default = 'http://127.0.0.1:18080' if spec.get('target', {}).get('kind') == 'docker' else configured
        if default and '://' not in default:
            default = 'http://' + default
        base = st.text_input('Browser-accessible Spark History Server URL', default, key='history-url-'+run_id,
                             help='Use the published host address, not an internal Docker hostname. This session-only setting does not change Spark configuration.')
        st.caption('History may appear after a delay. Event logs describe Spark jobs, stages and tasks; they are separate from driver console logs and system resource samples. Downloads come from the History Server as ZIP archives and require it to remain available.')
        if not any(event.get('metrics', {}).get('application_id') or event.get('reference', {}).get('metadata', {}).get('application_ids') for event in events):
            st.info('Links will appear once Spark reports an application ID.')
            return
        try:
            for app in application_links(base, events)['applications']:
                history, download = app['history_url'], app['event_logs_url']
                st.write(app['application_id'])
                left, right = st.columns(2)
                left.link_button('Open in Spark History', history)
                right.link_button('Download Spark event logs (ZIP)', download)
        except ValueError as error:
            st.info(str(error))
