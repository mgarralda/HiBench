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

"""General experiment storage, runtime and logging settings."""
import copy
import hashlib
import json
import os
import uuid
import streamlit as st
from hibench import config, store
from hibench.adapters import create_adapter, REGISTRY


def _change_storage_prefix():
    azure = st.session_state["general-provider"] == "Azure Blob Storage (WASB/WASBS)"
    prefixes = ("hdfs://",) if azure else ("wasb://", "wasbs://")
    replacement = "wasbs://" if azure else "hdfs://"
    if st.session_state["general-provider"] not in ("HDFS", "Azure Blob Storage (WASB/WASBS)"):
        return
    for key in ("general-endpoint", "general-datasets", "general-outputs"):
        value = st.session_state.get(key, "")
        for prefix in prefixes:
            if value.startswith(prefix):
                st.session_state[key] = replacement + value[len(prefix):]
                break


def target_settings(target):
    """Edit the existing draft target; permanent connection controls live here."""
    kinds = [entry.kind for entry in REGISTRY]
    target['kind'] = st.selectbox('Execution adapter', kinds, index=kinds.index(target['kind']),
        format_func=lambda kind: next(entry.label + (' (soon)' if not entry.implemented else '') for entry in REGISTRY if entry.kind == kind))
    implemented = next(entry.implemented for entry in REGISTRY if entry.kind == target['kind'])
    if not implemented:
        st.info('This adapter is coming soon. Connection and execution are not implemented yet.')
        return False
    if target["kind"] == "docker":
        target["container"] = st.text_input("Master container", target.get("container", "spark-cluster-master"))
    elif target['kind'] == 'ssh':
        from hibench.adapters.ssh import connection_defaults
        for key, value in connection_defaults().items():
            target.setdefault(key, value)
        target.pop('container', None)
        target['host'] = st.text_input('SSH host', target.get('host', ''), help='Linux submission host. For the companion Docker cluster from the host machine, use 127.0.0.1 and its published SSH port.')
        target['username'] = st.text_input('SSH username', target.get('username', ''), help='Account with access to Spark, Hadoop and the target HiBench directory.')
        target['port'] = int(st.number_input('SSH port', min_value=1, max_value=65535, value=target.get('port', 22)))
        for key, label in (('identity_file', 'SSH private key path'), ('known_hosts_file', 'SSH known hosts path')):
            value = st.text_input(label, target.get(key, ''), help='File on the controller machine/container. Leave blank to use OpenSSH defaults or your agent. Private keys are never uploaded or included in run bundles. The server host key must already be trusted.')
            if value.strip():
                target[key] = value.strip()
            else:
                target.pop(key, None)
        password_auth = bool(os.environ.get('HIBENCH_SSH_PASSWORD')) and not target.get('identity_file')
        st.caption(('Authentication: password from the controller environment (HIBENCH_SSH_PASSWORD). ' if password_auth
                    else 'Authentication: OpenSSH key or agent. ')
                   + 'Server host keys must be verified. An explicit private key path takes precedence over the environment password. Spark runs on the remote Linux host.')
    else:
        target.pop("container", None)
        st.caption("The controller and runtime must run on the same Linux machine.")
    for key, label in (("workspace", "HiBench directory on the target"), ("spark_home", "Spark home"),
                       ("hadoop_home", "Hadoop home")):
        target[key] = st.text_input(label, target[key])
    target["workers"] = st.text_input("Workers (space-separated)", " ".join(target.get("workers", []))).split()
    return True


def render():
    st.title("Target & storage")
    st.caption("Configure the submission target once, then use Experiments to select workloads and run. Shared benchmark and Spark defaults are under Parametrization.")
    from hibench.ui import profiles
    with st.expander('Manage infrastructure profiles'):
        profiles.render()
    base = copy.deepcopy(st.session_state.experiment)
    target = base["target"]
    with st.expander("Target connection and runtime", expanded=True):
        supported_target = target_settings(target)
        if st.button("Apply target settings", type="primary", disabled=not supported_target):
            try:
                st.session_state.experiment = config.validate(base)
                st.session_state.request_key = str(uuid.uuid4())
                st.success("Target settings applied. Return to Experiments to check or update the target.")
            except (ValueError, TypeError) as exc:
                st.error(str(exc))
    # Storage edits always start from applied settings, not unapplied connection fields.
    base = copy.deepcopy(st.session_state.experiment)
    target = base['target']
    target_revision = hashlib.sha256(json.dumps(target, sort_keys=True).encode()).hexdigest()
    if st.session_state.get('general-target-revision') != target_revision:
        endpoint = target.get('hdfs_uri', '')
        st.session_state['general-endpoint'] = endpoint
        st.session_state['general-datasets'] = target.get('dataset_base', endpoint + '/HiBench-Control/datasets')
        st.session_state['general-outputs'] = target.get('output_base', endpoint + '/HiBench-Control/runs')
        st.session_state['general-provider'] = 'Azure Blob Storage (WASB/WASBS)' if endpoint.startswith(('wasb://', 'wasbs://')) else 'HDFS'
        st.session_state['general-target-revision'] = target_revision
    initial_endpoint = target.get("hdfs_uri", "")
    for key, value in (("general-endpoint", initial_endpoint),
                       ("general-datasets", target.get("dataset_base", initial_endpoint + "/HiBench-Control/datasets")),
                       ("general-outputs", target.get("output_base", initial_endpoint + "/HiBench-Control/runs"))):
        st.session_state.setdefault(key, value)
    st.subheader("Benchmark data storage")
    providers = ["HDFS", "Azure Blob Storage (WASB/WASBS)", "Amazon S3 (soon)", "Azure ADLS (ABFS/ABFSS) (soon)", "Google Cloud Storage (soon)"]
    provider = st.selectbox("Dataset storage backend", providers, index=1 if target.get("hdfs_uri", "").startswith(("wasb://", "wasbs://")) else 0, key="general-provider", on_change=_change_storage_prefix)
    supported = provider in providers[:2] and target["kind"] in ("docker", "local", "ssh")
    if not supported:
        st.info("This dataset storage backend is not enabled in the current controller. Event logs may independently use cloud filesystem URIs configured on the runtime.")
    if provider == "Azure Blob Storage (WASB/WASBS)":
        st.caption("WASBS prefixes are filled automatically. Replace the endpoint authority with your Azure container/account and update the base URIs accordingly. The runtime must provide the WASB connector and authentication.")
    with st.form("general-settings"):
        endpoint = st.text_input("Storage endpoint", key="general-endpoint", help="HDFS: hdfs://host:port or hdfs://nameservice. Azure Blob: wasbs://container@account.blob.core.windows.net (or a runtime-configured shorthand). The runtime must provide the connector and authentication.")
        datasets = st.text_input("Dataset base URI", key="general-datasets")
        outputs = st.text_input("Output base URI", key="general-outputs")
        st.caption("The controller creates isolated subdirectories for datasets and each run. Changing the storage location prevents reuse of datasets from the previous location.")
        st.subheader("Runtime configuration")
        hadoop_conf = st.text_input("Hadoop configuration directory", target.get("hadoop_conf_dir", target.get("hadoop_home", "") + "/etc/hadoop"))
        reports = st.text_input("Remote report directory", target.get("report_base", target.get("workspace", "") + "/report/control"))
        apply = st.form_submit_button("Apply general settings", type="primary", disabled=not supported)
    if apply:
        try:
            expected_schemes = ("hdfs://",) if provider == "HDFS" else ("wasb://", "wasbs://")
            if not endpoint.startswith(expected_schemes):
                raise ValueError("Enter a storage endpoint matching the selected backend and update both base URIs to use that endpoint.")
            target["hdfs_uri"] = endpoint
            for key, value, default in (("dataset_base", datasets, endpoint + "/HiBench-Control/datasets"),
                                        ("output_base", outputs, endpoint + "/HiBench-Control/runs"),
                                        ("hadoop_conf_dir", hadoop_conf, target["hadoop_home"] + "/etc/hadoop"),
                                        ("report_base", reports, target["workspace"] + "/report/control")):
                if value.rstrip("/") == default:
                    target.pop(key, None)
                else:
                    target[key] = value
            st.session_state.experiment = config.validate(base)
            st.session_state.request_key = str(uuid.uuid4())
            st.success("General settings applied. Review or export the experiment in Experiments.")
        except (ValueError, TypeError) as exc:
            st.error(str(exc))
    if st.button("Check configured endpoint", disabled=not supported):
        try:
            with st.spinner("Checking the saved runtime and storage endpoint..."):
                st.code(create_adapter(st.session_state.experiment["target"]).doctor())
        except Exception as exc:
            st.error(str(exc))
    st.caption("The check uses applied settings. Apply form changes before checking.")
    st.subheader("Controller state and execution defaults")
    st.write("Local history, logs and dataset manifests: " + str(store.home()))
    st.caption("Controller state is an application setting (HIBENCH_STATE_DIR), separate from distributed data and remote reports. Resources and partitions are configured in Experiments. Matching prepared datasets are reused automatically.")
