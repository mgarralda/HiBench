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
                "2. In **Spark Infrastructure → Target & storage**, apply connection, runtime and storage settings.\n"
                "3. In **Experiments**, check or update the target, select workloads, size and implementation, then review and Run.\n"
                "4. In **Runs**, use History to find runs, Monitoring for progress and downloads, and Comparisons for analysis.")
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
    st.write("Manage reusable infrastructure profiles in Target & storage; load them from Experiments. Profiles contain target, storage, resources and Spark properties; workloads remain part of the experiment. Parametrization owns shared defaults; optional Experiments overrides affect only the current draft. Reuse configuration in Runs opens a draft without submitting jobs.")
    st.subheader("Scala, PySpark and comparisons")
    st.write("Select Scala, PySpark or both in Experiments. Workload settings and datasets are shared; generators remain Scala. PySpark runs in Client mode. Runs → Comparisons compares one implementation per side, within a run or between completed runs. Inspect conditions, duration medians, samples and B/A ratios, then save or download JSON, CSV, HTML or ZIP. Unknown conditions and failed runs are flagged; comparisons do not certify statistical or output equivalence.")
    st.write("Download YAML from Experiments and import it to reuse your configuration. "
             "Loading it does not start jobs or import datasets. The lab profile stores its directory and Compose project; "
             "it does not replace the experiment connection settings.")
    st.subheader("Troubleshooting")
    st.write("Check that Docker is running, the Compose project matches the existing cluster, and services are healthy. "
             "Use Check connection to verify the HiBench runtime. After compiling Scala/Java changes, use Update target in Experiments, Save and update target in Parametrization, or hibench install with your experiment file. Target updates are unavailable while runs are queued or active.")
    st.subheader("REST API")
    st.code("python -m hibench.launcher\n# Optional unified launcher:\npython -m hibench.launcher --with-ui", language="bash")
    st.write("The API defaults to http://127.0.0.1:8000 with interactive documentation at /docs. Stop the existing UI before using the unified launcher on its port, or choose --ui-port. API shutdown leaves detached benchmark workers running.")
