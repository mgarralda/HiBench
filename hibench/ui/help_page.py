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

import streamlit as st

def render():
    st.title("Help")
    st.subheader("Prepare and run a benchmark")
    st.markdown("1. In **Spark Infrastructure → Cluster**, select an existing project or use the assistant to download, build and start it.\n"
                "2. In **Experiments**, select an adapter, check the target and configure your workloads.\n"
                "3. Click Run and open **Runs** to view logs, metrics and history.")
    st.subheader("Clusters and adapters")
    st.write("The cluster hosts Spark, Hadoop and your data. The adapter determines how jobs are submitted. "
             "Existing Docker uses docker exec on a running container; it does not create a cluster. "
             "SSH connects to an existing Linux submission host using a key/agent or HIBENCH_SSH_PASSWORD from the controller environment, with verified host keys. "
             "Configure credential paths or environment variables on the controller; passwords and private keys are not uploaded through the UI or saved in experiments.")
    st.subheader("Data and persistence")
    st.write("Preparation creates data; execution uses a compatible prepared dataset. "
             "The controller stores configurations, logs and history. Stopping or removing containers preserves "
             "volumes but interrupts running jobs. Use the reconcile operation after a worker crash to check the recorded application state; jobs are not automatically restarted.")
    st.subheader("Saved configuration")
    st.write("Save reusable infrastructure profiles in General or Experiments. Profiles contain target, storage, resources and Spark properties; workloads remain part of the experiment. Runs provides duplication and reproducibility ZIP exports; the comparison view is planned.")
    st.write("Download YAML from Experiments and import it to reuse your configuration. "
             "Loading it does not start jobs or import datasets. The lab profile stores its directory and Compose project; "
             "it does not replace the experiment connection settings.")
    st.subheader("Troubleshooting")
    st.write("Check that Docker is running, the Compose project matches the existing cluster, and services are healthy. "
             "Use Check connection to verify the HiBench runtime. After compiling Scala/Java changes, update the target using Save and update target in Parametrization, or hibench install with your experiment file.")
    st.subheader("REST API")
    st.code("python -m hibench.server\n# Optional unified launcher:\npython -m hibench.server --with-ui", language="bash")
    st.write("The API defaults to http://127.0.0.1:8000 with interactive documentation at /docs. Stop the existing UI before using the unified launcher on its port, or choose --ui-port. API shutdown leaves detached benchmark workers running.")
