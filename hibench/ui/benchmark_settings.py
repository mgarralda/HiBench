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

"""Canonical benchmark/controller settings, kept separate from Spark properties."""
import uuid
import streamlit as st
from hibench import config, spark_configuration
from hibench.ui.spark_settings import SAVE_HELP, update_saved_target, target_update_status


def render():
    st.title('HiBench configuration')
    st.caption('Shared benchmark defaults, generator partitions, launch paths and monitoring. Source: conf/hibench.conf. Spark properties and scale sections belong in Spark configuration; dataset sizes belong in Workloads.')
    state = spark_configuration.shared()
    target_update_status()
    with st.form('shared-benchmark-' + state['benchmark_sha256']):
        text = st.text_area('Shared HiBench configuration', state['benchmark_text'], height=650,
                            help='Use hibench.* or sparkbench.* properties separated from values by whitespace. Generator partition changes can change random streams and datasets. Comments begin with #.')
        saved = st.form_submit_button('Save and update target', type='primary', help=SAVE_HELP)
    if saved:
        try:
            overrides = config.overrides(st.session_state.experiment)
            spark_configuration.save_shared(state['text'], state['sha256'], text, state['benchmark_sha256'])
            st.session_state.experiment = config.resolve(overrides)
            st.session_state.request_key = str(uuid.uuid4())
            update_saved_target()
            st.rerun()
        except (ValueError, TypeError, OSError) as exc:
            st.error(str(exc))
