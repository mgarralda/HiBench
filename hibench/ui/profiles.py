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

"""Infrastructure profile management shared with the HTTP interface."""
import uuid
import streamlit as st
from hibench.services import configuration


def render():
    with st.expander('Infrastructure profiles'):
        st.caption('Profiles store the execution target, storage, resources and Spark properties. Workload parameters stay in the experiment. Loading a profile replaces infrastructure settings; submitted runs retain their snapshots.')
        profiles = configuration.profiles()
        if profiles:
            selected = st.selectbox('Saved infrastructure profile', [p['name'] for p in profiles])
            if st.button('Load infrastructure profile'):
                st.session_state.experiment = configuration.apply_profile(st.session_state.experiment, configuration.get_profile(selected)['settings'])
                st.session_state.request_key = str(uuid.uuid4())
                for key in list(st.session_state):
                    if key.startswith('general-'):
                        del st.session_state[key]
                st.rerun()
            if st.button('Delete infrastructure profile'):
                configuration.delete_profile(selected)
                st.rerun()
        name = st.text_input('Profile name', help='Save applied settings. This does not submit a benchmark.')
        if st.button('Save infrastructure profile', disabled=not name.strip()):
            try:
                experiment = st.session_state.experiment
                configuration.save_profile(name.strip(), {key: experiment[key] for key in configuration.SECTIONS})
                st.success('Infrastructure profile saved.')
            except ValueError as exc:
                st.error(str(exc))
