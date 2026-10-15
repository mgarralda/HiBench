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

"""Project identity, attribution and support entry points."""
from importlib.metadata import version
import streamlit as st
from hibench.catalog import workloads, repository_root


def render():
    st.title('About HiBench Next')
    st.caption('Reproducible benchmarks for Apache Spark')
    st.write('An open-source modernization of HiBench focused on Spark batch benchmarking: DataFrame-based workloads and generators, current Spark ML APIs, reproducible configuration and shared web, REST and CLI services.')
    left, right = st.columns(2)
    left.metric('Controller version', version('hibench-next'))
    right.metric('Batch workloads', len(workloads()))
    st.subheader('License and attribution')
    st.write('Licensed under Apache License 2.0. HiBench Next builds on the original HiBench work by Intel and its contributors. Original copyright notices are retained.')
    st.write('HiBench Next contributions © 2025–2026 Mariano Garralda-Barrio. See LICENSE and NOTICE in the repository for full attribution.')
    with st.expander('Project notices'):
        st.code((repository_root() / 'NOTICE').read_text(encoding='utf-8'), language='text')
    st.subheader('Documentation and programmatic access')
    st.markdown('[Cite HiBench Next v2.5.0 — Zenodo DOI](https://doi.org/10.5281/zenodo.23400663) · [Citation formats](https://github.com/mgarralda/HiBench-Next#citing-hibench-next)')
    st.markdown('[Project repository](https://github.com/mgarralda/HiBench-Next) · [Original HiBench](https://github.com/Intel-bigdata/HiBench) · [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)')
    st.markdown('[REST API / Swagger](http://127.0.0.1:8000/docs) · [OpenAPI schema](http://127.0.0.1:8000/openapi.json)')
    st.caption('API links use the default local port. For a remote deployment, use the API host and port configured by your administrator. Cloud execution adapters marked soon are not implemented. Public validation scope is documented in README.md, pysparkbench/README.md and tests/README.md.')
