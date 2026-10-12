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

"""Streamlit entry point: navigation only; services own execution and cluster operations."""


import uuid
import streamlit as st
from hibench import config
from hibench.catalog import repository_root
from hibench.ui import experiments, executions, clusters, help_page, workloads as workloads_page, general
from hibench.ui import spark_settings, benchmark_settings, about

st.set_page_config(page_title="HiBench Next", page_icon=str(repository_root() / 'hibench/ui/assets/hibench-next-icon.ico'), layout="wide", initial_sidebar_state="expanded")
st.markdown("""
<style>
[data-testid="stSidebar"] {border-right: 1px solid rgba(128,128,128,.16);}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {gap:.4rem;}
[data-testid="stSidebar"] hr {margin:.8rem 0;}
[data-testid="stSidebar"] .stButton > button {
    justify-content:flex-start; padding:.55rem .8rem; border-radius:.4rem;
    margin:.08rem 0; transition:background .15s ease;
}
[data-testid="stSidebar"] .stButton > button:hover {
    border-color:rgba(59,130,246,.45);
}
[data-testid="stSidebar"] .stButton > button p {font-weight:500;}
[data-testid="stSidebar"] .stButton > button > div {justify-content:flex-start;}
.block-container {max-width:1280px; padding-top:2rem;}
[data-testid="stCaptionContainer"] {line-height:1.5;}
</style>
""", unsafe_allow_html=True)
if "experiment" not in st.session_state:
    try:
        st.session_state.experiment = config.default_experiment()
    except (ValueError, OSError) as exc:
        st.error('The shared configuration could not be loaded. Check conf/ and restart this page. ' + str(exc))
        st.stop()
if "request_key" not in st.session_state:
    st.session_state.request_key = str(uuid.uuid4())
if st.query_params.get('run'):
    from hibench import store
    try:
        selected_run = str(uuid.UUID(st.query_params['run']))
        store.get(selected_run)
        st.session_state.active_run = selected_run
        st.session_state.ui_page = 'Runs'
    except (ValueError, KeyError):
        st.warning('The linked run is unavailable.')
with st.sidebar:
    st.image(str(repository_root() / 'hibench/ui/assets/hibench-next-web.png'), width='stretch')
    if "ui_page" not in st.session_state:
        st.session_state.ui_page = "Experiments"
    def navigate(destination):
        st.session_state.ui_page = destination
        if destination != 'Runs' and 'run' in st.query_params:
            del st.query_params['run']
    groups = [
        ('WORKSPACE', [('Experiments', 'Experiments', 'science'), ('Runs', 'Runs', 'monitoring'), ('Help', 'Help', 'help_outline')]),
        ('PARAMETRIZATION', [('SparkConfiguration', 'Spark configuration', 'tune'), ('HiBenchConfiguration', 'HiBench configuration', 'settings'), ('Workloads', 'Workloads', 'dataset')]),
        ('SPARK INFRASTRUCTURE', [('General', 'General', 'storage'), ('Clusters', 'Cluster', 'dns')]),
        ('PROJECT', [('About', 'About', 'info')]),
    ]
    for index, (heading, entries) in enumerate(groups):
        if index:
            st.divider()
        st.caption(heading)
        for destination, label, icon in entries:
            st.button(label, icon=':material/' + icon + ':', key='nav-' + destination, width='stretch',
                      type='primary' if st.session_state.ui_page == destination else 'secondary',
                      on_click=navigate, args=(destination,))
    st.divider()
    page = st.session_state.ui_page
    st.caption('Reproducible benchmarks for Apache Spark')
    st.caption('Apache-2.0 · © 2025–2026 Mariano Garralda-Barrio')

if page == "Experiments":
    st.title("Experiments")
    st.caption("Select a target and configure a reproducible benchmark.")
    experiments.render()
elif page == "Runs":
    executions.render()
elif page == "General":
    general.render()
elif page == "Workloads":
    workloads_page.render()
elif page == 'HiBenchConfiguration':
    benchmark_settings.render()
elif page == 'SparkConfiguration':
    spark_settings.render()
elif page == "Clusters":
    clusters.render()
elif page == "About":
    about.render()
else:
    help_page.render()


# Launch this UI with `python -m streamlit run hibench/web.py`.
# The REST application entry point is hibench.api.app:app; hibench.server can launch both.
