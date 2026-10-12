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

import copy
import json
import os
import uuid
from datetime import datetime
import yaml
import streamlit as st
from hibench import config, store
from hibench.adapters import create_adapter, REGISTRY
from hibench.catalog import SCALES, repository_root, workloads

def render():
    from hibench.ui import profiles
    profiles.render()
    base = copy.deepcopy(st.session_state.experiment)
    imported = st.file_uploader("Load saved configuration (YAML or JSON)", type=["yaml", "yml", "json"], help="Import a downloaded experiment override or full run configuration. Loading validates the file and updates this draft; it does not submit jobs.")
    if imported is not None and st.button("Load configuration"):
        try:
            st.session_state.experiment = config.resolve(yaml.safe_load(imported.getvalue()))
            st.session_state.request_key = str(uuid.uuid4())
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    target = copy.deepcopy(base["target"])
    st.subheader("1. Execution target")
    with st.container(border=True):
        execution_group = st.text_input("Execution group ID", base.get("execution_group", ""), max_chars=128,
            help="Optional label shared by related runs, saved in your experiment and passed as spark.app.benchmark.group.id. Leave blank to use the unique run ID. Workload and data size are generated automatically for each job.")
        kinds = [entry.kind for entry in REGISTRY]
        target["kind"] = st.selectbox("Execution adapter", kinds, index=kinds.index(target["kind"]),
                                      format_func=lambda kind: next(entry.label + (" (soon)" if not entry.implemented else "")
                                                                   for entry in REGISTRY if entry.kind == kind))
        implemented = next(entry.implemented for entry in REGISTRY if entry.kind == target["kind"])
        if not implemented:
            target_valid = False
            st.info("This adapter is coming soon. Connection and execution are not implemented yet.")
        else:
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
                               ("hadoop_home", "Hadoop home"), ("master", "Spark master"), ("hdfs_uri", "Storage endpoint (HDFS or WASB/WASBS)")):
                target[key] = st.text_input(label, target[key])
            target["deploy_mode"] = st.selectbox("Deploy mode", ["client", "cluster"], index=["client", "cluster"].index(target["deploy_mode"]), help="Client keeps the driver on the submission target. Cluster launches it through the cluster manager; console output may be incomplete, so use Runs and Spark History for application details.")
            target["workers"] = st.text_input("Workers (space-separated)", " ".join(target.get("workers", []))).split()
            try:
                destination_adapter = create_adapter(target)
                destination_adapter.validate_target()
                target_valid = True
            except ValueError as exc:
                target_valid = False
                st.error(str(exc))
            if st.button("Check connection", disabled=not target_valid):
                try:
                    with st.spinner("Checking connection and runtime…"):
                        st.code(destination_adapter.doctor())
                except Exception as exc:
                    st.error(str(exc))
            if target_valid:
                caps = destination_adapter.capabilities()
                st.caption("Capabilities: " + ", ".join(sorted(caps)))
    st.subheader("2. Experiment and workloads")
    name = st.text_input("Name", base.get("name", "Batch experiment"))
    catalog = {item["id"]: item for item in workloads()}
    selected = st.multiselect("Batch workloads", list(catalog), default=[x["id"] for x in base["workloads"]])
    left, right = st.columns(2)
    with left:
        scale = st.selectbox("Scale", SCALES, index=SCALES.index(base["scale"]))
        modes = {"Prepare and run": "prepare_and_run", "Prepare only": "prepare", "Run prepared data": "run"}
        mode = st.selectbox("Operation", list(modes), index=list(modes.values()).index(base["mode"]))
    with right:
        repetitions = st.number_input("Benchmark repetitions", 1, 100, base["repetitions"], help="Number of workload executions after preparation. Prepared data may be reused; keep cache behavior and repetitions consistent when comparing results.")
        timeout = st.number_input("Phase timeout (seconds)", 1, 604800, base["timeout_s"], help="Maximum time for each prepare or run phase, including Spark startup. Exceeding it fails that phase and requests application termination.")
    resources = copy.deepcopy(base["resources"])
    if scale != base['scale']:
        previous_base = config.default_experiment(scale=base['scale'], selected_workloads=['micro.sleep'])
        selected_base = config.default_experiment(scale=scale, selected_workloads=['micro.sleep'])
        resources = {key: selected_base['resources'][key] if value == previous_base['resources'][key] else value
                     for key, value in resources.items()}
        st.caption('Spark settings inherited from the selected scale have been refreshed. Explicit resource overrides are retained.')
    with st.expander("Requested experiment resources"):
        st.caption("These resources are requested for each benchmark; they do not describe the total cluster capacity.")
        columns = st.columns(2)
        for index, (key, label) in enumerate((
            ("executor_instances", "Executors"), ("executor_cores", "Cores per executor"),
            ("shuffle_partitions", "Spark SQL shuffle partitions"),
            ("generation_partitions", "Generation partitions"), ("generation_shuffle_partitions", "Generator shuffle partitions"))):
            resources[key] = columns[index % 2].number_input(label, 1, 10000, resources[key])
        resources["executor_memory"] = columns[0].text_input("Executor memory", resources["executor_memory"])
        resources["driver_memory"] = columns[1].text_input("Driver memory", resources["driver_memory"])
    with st.expander("Resource monitoring"):
        monitor_enabled = st.checkbox("Collect worker resource metrics", value=base.get('monitoring', {}).get('enabled', False),
                                      help="Collect Linux system observations over SSH on the configured workers. Host counters can overlap in Docker. Monitoring adds overhead; keep it consistent across compared runs.")
        monitor_interval = st.number_input("Sampling interval (seconds)", 1, 60, base.get('monitoring', {}).get('interval_s', 5))
        st.caption("Reports and raw samples are retained with each phase and included in the experiment bundle. Available on Docker, SSH and local Linux adapters; collector failure does not fail the benchmark.")
    prior = {x["id"]: x["parameters"] for x in base["workloads"]}
    chosen = []
    for workload_id in selected:
        overrides = copy.deepcopy(prior.get(workload_id, {}))
        with st.expander("Parameters · " + workload_id):
            st.caption(catalog[workload_id]["description"])
            customize = st.checkbox("Customize parameters", value=bool(overrides), key="custom-" + workload_id)
            if customize:
                for key, schema in catalog[workload_id]["parameters"].items():
                    label = key + (" (" + schema["unit"] + ")" if schema.get("unit") else "")
                    initial = overrides.get(key, schema["presets"].get(scale, schema["default"]))
                    widget_key = workload_id + ":" + key + ":" + scale
                    if schema["kind"] in ("number", "integer"):
                        if schema["kind"] == "number":
                            overrides[key] = st.number_input(label, min_value=0.0, value=float(initial), key=widget_key)
                        else:
                            overrides[key] = st.number_input(label, min_value=0, value=int(initial), key=widget_key)
                    elif schema["kind"] == "boolean":
                        overrides[key] = st.checkbox(key, value=str(initial).lower() == "true", key=widget_key)
                    else:
                        overrides[key] = st.text_input(key, str(initial), key=widget_key)
            else:
                overrides = {}
                st.caption("The preset for the selected scale will be used.")
        chosen.append({"id": workload_id, "parameters": overrides})
    with st.expander("Advanced Spark configuration"):
        st.caption('Defaults come from common properties and the selected scale in conf/spark.conf. The editor below contains only experiment overrides. Remove a property to inherit the common default; use {} to inherit all defaults. Saved profiles and imported experiments may contain explicit overrides.')
        if st.button('Use shared Spark defaults', help='Clear Spark property overrides for this draft experiment. Saved runs are unchanged.'):
            st.session_state.experiment['spark_conf'] = {}
            st.session_state.request_key = str(uuid.uuid4())
            st.rerun()
        st.caption("Add any supported Spark property using its exact name and a YAML value. These settings are saved in the experiment and passed to spark-submit through its properties file. The selected Spark runtime must support the property; unknown names may be ignored by Spark.")
        conf_text = st.text_area("Spark properties", yaml.safe_dump(base["spark_conf"], sort_keys=True), height=260,
                                help="Use key: value, for example spark.sql.adaptive.enabled: true. Add dependent settings as separate properties. Lines starting with # are comments and are not applied. Executor count, cores, memory, master and parallelism use the dedicated fields above.")
        try:
            from hibench.spark_configuration import resolve
            overrides = yaml.safe_load(conf_text) or {}
            if not isinstance(overrides, dict):
                raise ValueError('Spark properties must be a YAML mapping.')
            effective, sources = resolve(overrides, scale=scale)
            st.dataframe([{'Property': key, 'Effective value': config.scalar(value), 'Source': sources[key]}
                          for key, value in sorted(effective.items())], hide_index=True, width='stretch')
            st.caption('Dedicated resource fields and launcher-managed identifiers are added at submission. Defaults are re-read from the repository on each render; submitted runs retain a frozen snapshot.')
        except (ValueError, yaml.YAMLError) as exc:
            st.error(str(exc))
        st.caption("Save experiment includes these properties in YAML. Review their effects before comparing benchmark results.")
        with st.expander("Property examples and dependencies"):
            st.markdown("**Fixed benchmark resources**")
            st.code("spark.dynamicAllocation.enabled: false\nspark.sql.adaptive.enabled: false\nspark.executor.extraJavaOptions: '-XX:+UseG1GC'", language="yaml")
            st.markdown("**Dynamic Allocation** — requires a supported shuffle-preservation mechanism on the runtime. This example uses shuffle tracking; an external shuffle service is another option when configured on the cluster.")
            st.code("spark.dynamicAllocation.enabled: true\nspark.dynamicAllocation.shuffleTracking.enabled: true\nspark.dynamicAllocation.minExecutors: 2\nspark.dynamicAllocation.initialExecutors: 2\nspark.dynamicAllocation.maxExecutors: 10", language="yaml")
            st.markdown("**Adaptive Query Execution (AQE)** — dependent options take effect when AQE is enabled.")
            st.code("spark.sql.adaptive.enabled: true\nspark.sql.adaptive.coalescePartitions.enabled: true\nspark.sql.adaptive.advisoryPartitionSizeInBytes: 64m\nspark.sql.adaptive.skewJoin.enabled: true", language="yaml")
            st.markdown("[Spark configuration reference](https://spark.apache.org/docs/3.5.9/configuration.html) · [AQE settings](https://spark.apache.org/docs/3.5.9/sql-performance-tuning.html#adaptive-query-execution)")
    try:
        spec = config.validate(dict(schema_version=1, name=name, execution_group=execution_group, target=target, mode=modes[mode],
                                    scale=scale, repetitions=int(repetitions), timeout_s=int(timeout),
                                    resources=resources, spark_conf=yaml.safe_load(conf_text) or {}, workloads=chosen,
                                    monitoring={'enabled': monitor_enabled, 'interval_s': int(monitor_interval)}))
    except Exception as exc:
        spec = None
        if implemented:
            st.error(str(exc))
    if not implemented:
        st.button("Run", type="primary", disabled=True)
    if spec:
        st.session_state.experiment = spec
        st.download_button("Save experiment", yaml.safe_dump(config.overrides(spec), sort_keys=False), "experiment.yaml", "application/yaml")
        st.caption('Download contains only changes from conf/. Importing it applies those changes to the current common base. Submitted runs separately retain their complete configuration for exact reproduction.')
        st.subheader("Review execution")
        with st.container(border=True):
            destination_name = target.get("container") if target["kind"] == "docker" else target.get('host', target['workspace'])
            st.write(f"**Target:** {target['kind']} · {destination_name} · {target['master']}")
            st.write("**Workloads:** " + ", ".join(selected))
            st.write(f"**Operation:** {mode} · scale {scale} · {repetitions} repetition(s)")
            st.write(f"**Resources:** {resources['executor_instances']} executor(s), "
                     f"{resources['executor_cores']} core(s) and {resources['executor_memory']} per executor; "
                     f"driver {resources['driver_memory']}")
            st.caption(f"Phase timeout: {timeout} seconds. Custom parameters are included in the downloadable configuration.")
        if st.button("Run", type="primary"):
            try:
                from hibench.services.experiments import submit
                run_id = submit(spec, st.session_state.request_key)['run_id']
                st.session_state.active_run = run_id
                st.session_state.experiment = spec
                st.success("Run submitted: " + run_id)
            except Exception as exc:
                st.error(str(exc))
        if st.session_state.get('active_run'):
            def view_submitted_run():
                from hibench.ui.executions import open_run
                open_run(st.session_state.active_run)
                st.session_state.ui_page = 'Runs'
            st.button('Open latest run', icon=':material/arrow_forward:', on_click=view_submitted_run,
                      help='Open monitoring, logs and results for the most recently submitted or selected run.')
        if st.button("New run", help="Create a fresh submission key. Repeated clicks on Run otherwise refer to the same submission, preventing accidental duplicate jobs."):
            st.session_state.request_key = str(uuid.uuid4())
            st.info("The next click on Run will create a new execution.")
