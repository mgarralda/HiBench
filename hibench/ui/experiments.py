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
import uuid
import yaml
import streamlit as st
from hibench import config, store
from hibench.adapters import create_adapter, REGISTRY
from hibench.catalog import SCALES, repository_root, workloads

def target_actions(target, valid):
    """Explicit runtime actions; never submit a benchmark."""
    signature = (json.dumps(target, sort_keys=True), tuple(
        (str(path), path.stat().st_mtime_ns, path.stat().st_size)
        for folder in ('conf', 'bin', 'pysparkbench', 'sparkbench/assembly/target')
        for path in sorted((repository_root() / folder).rglob('*'))
        if path.is_file() and '__pycache__' not in path.parts and path.suffix != '.pyc'))
    status = st.session_state.get('experiment-target-check')
    if not status or status['signature'] != signature:
        st.info('Target readiness has not been checked for this configuration. Check connection before running.')
    elif status['ok']:
        st.success('Last check passed: connection available and HiBench runtime matches this checkout.')
    else:
        st.warning('Target check failed. Review the details or update the target.')
    check, update = st.columns(2)
    check_clicked = check.button('Check connection', disabled=not valid)
    update_clicked = update.button('Update target', disabled=not valid or store.has_active_runs(),
        help='Copy saved configuration, launch scripts, PySpark modules and the compiled JAR to the target. No benchmark is started.')
    if check_clicked or update_clicked:
        try:
            with st.spinner('Updating and checking target…' if update_clicked else 'Checking connection and runtime…'):
                if update_clicked:
                    from hibench.services.runtime import install
                    install(target)
                output = create_adapter(target).doctor()
            st.session_state['experiment-target-check'] = {'signature': signature, 'ok': True}
            st.success('Target check passed.' + (' Saved runtime installed.' if update_clicked else ''))
            with st.expander('Target check details'):
                st.code(output)
        except Exception as exc:
            st.session_state['experiment-target-check'] = {'signature': signature, 'ok': False}
            st.error(str(exc))
            if getattr(exc, 'output', None):
                st.code(exc.output)

def render():
    from hibench.ui import profiles
    with st.expander("Load configuration or infrastructure profile"):
        profiles.render(manage=False)
        imported = st.file_uploader("Load saved configuration (YAML or JSON)", type=["yaml", "yml", "json"], help="Import a downloaded experiment override or full run configuration. Loading validates the file and updates this draft; it does not submit jobs.")
        if imported is not None and st.button("Load configuration"):
            try:
                st.session_state.experiment = config.resolve(yaml.safe_load(imported.getvalue()))
                st.session_state.request_key = str(uuid.uuid4())
                st.rerun()
            except Exception as exc:
                st.error(str(exc))
    base = copy.deepcopy(st.session_state.experiment)
    if st.session_state.get('experiment-saved-implementation') != base.get('implementation', 'scala'):
        st.session_state.pop('experiment-implementation', None)
    target = copy.deepcopy(base["target"])
    st.subheader("1. Execution target")
    with st.container(border=True):
        destination_name = target.get("container") if target["kind"] == "docker" else target.get('host', target['workspace'])
        st.write(f"**{target['kind'].upper()}** · {destination_name}")
        st.caption("Connection, runtime paths and storage are configured in Spark Infrastructure → Target & storage.")
        st.button("Edit target & storage", on_click=lambda: st.session_state.update(ui_page='General'))
        implemented = next(entry.implemented for entry in REGISTRY if entry.kind == target["kind"])
        implementation = st.session_state.get('experiment-implementation', base.get('implementation', 'scala'))
        if not implemented:
            target_valid = False
            st.info("This adapter is coming soon. Configure a supported target in Spark Infrastructure.")
        else:
            execution_modes = ['yarn', 'standalone', 'local']
            saved_selection = (target['master'], target['deploy_mode'])
            if st.session_state.get('experiment-spark-target') != saved_selection:
                st.session_state.pop('experiment-spark-mode', None)
                st.session_state.pop('experiment-deploy-mode', None)
            current_mode = 'yarn' if target['master'] == 'yarn' else ('standalone' if target['master'].startswith('spark://') else 'local')
            execution_mode = st.selectbox('Spark execution mode', execution_modes,
                index=execution_modes.index(current_mode), key='experiment-spark-mode',
                format_func=lambda value: {'yarn': 'YARN', 'standalone': 'Standalone cluster', 'local': 'Local (2 threads)'}[value],
                help='Local runs on the submission host only; it does not use cluster workers.')
            if execution_mode == 'standalone':
                target['master'] = st.text_input('Standalone master URL', target['master'] if target['master'].startswith('spark://') else '', placeholder='spark://host:7077')
            else:
                target['master'] = 'yarn' if execution_mode == 'yarn' else 'local[2]'
            deploy_modes = ['client', 'cluster'] if execution_mode == 'yarn' and implementation != 'pyspark' else ['client']
            target['deploy_mode'] = st.selectbox('Deploy mode', deploy_modes,
                index=deploy_modes.index(target['deploy_mode']) if target['deploy_mode'] in deploy_modes else 0,
                key='experiment-deploy-mode', disabled=len(deploy_modes) == 1,
                help='HiBench supports YARN Cluster for Scala. PySpark and Standalone use Client.')
            try:
                destination_adapter = create_adapter(target)
                destination_adapter.validate_target()
                target_valid = True
            except ValueError as exc:
                target_valid = False
                st.error(str(exc))
            target_actions(target, target_valid)
    st.subheader("2. Workloads and data")
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

    st.subheader("3. Implementation")
    implementation = st.selectbox("Implementation", ["scala", "pyspark", "both"],
        key='experiment-implementation',
        index=["scala", "pyspark", "both"].index(base.get("implementation", "scala")),
        format_func=lambda value: {"scala": "Scala", "pyspark": "PySpark", "both": "Scala and PySpark"}[value],
        help="Both prepares once and runs Scala then PySpark per repetition. Generators stay in Scala.")
    if implementation == 'pyspark':
        target['deploy_mode'] = 'client'
    if implementation == 'both':
        st.caption("Scala uses the selected deploy mode; PySpark uses Client. Both share the dataset and workload settings.")
    st.caption("Defaults come from Parametrization. Optional overrides below apply only to this experiment.")
    with st.expander("Execution options"):
        execution_group = st.text_input("Execution group ID", base.get("execution_group", ""), max_chars=128,
            help="Optional label shared by related runs. Leave blank to use the unique run ID.")
        timeout = st.number_input("Phase timeout (seconds)", 1, 604800, base["timeout_s"],
            help="Maximum duration per phase, including Spark startup.")
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
    with st.expander("Workload overrides for this experiment"):
        st.caption("Shared sizes and algorithm defaults are edited in Parametrization → Workloads.")
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
            from hibench.parameters.spark import resolve
            overrides = yaml.safe_load(conf_text) or {}
            if not isinstance(overrides, dict):
                raise ValueError('Spark properties must be a YAML mapping.')
            resolve(overrides, scale=scale)
        except (ValueError, yaml.YAMLError) as exc:
            st.error(str(exc))
        st.caption("Shared properties and effective scale settings are available in Parametrization → Spark configuration.")
    try:
        spec = config.validate(dict(schema_version=1, name=name, execution_group=execution_group, target=target, implementation=implementation, mode=modes[mode],
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
        st.session_state['experiment-saved-implementation'] = implementation
        st.session_state['experiment-spark-target'] = (target['master'], target['deploy_mode'])
        st.subheader("4. Review and run")
        with st.container(border=True):
            destination_name = target.get("container") if target["kind"] == "docker" else target.get('host', target['workspace'])
            st.write(f"**Target:** {target['kind']} · {destination_name} · {target['master']}")
            deployments = 'Scala: ' + target['deploy_mode'] + '; PySpark: client' if implementation == 'both' else {'scala': 'Scala', 'pyspark': 'PySpark'}[implementation] + ': ' + target['deploy_mode']
            st.write('**Driver deployment:** ' + deployments)
            st.write("**Workloads:** " + ", ".join(selected))
            st.write(f"**Operation:** {mode} · scale {scale} · {repetitions} repetition(s)")
            st.write(f"**Resources:** {resources['executor_instances']} executor(s), "
                     f"{resources['executor_cores']} core(s) and {resources['executor_memory']} per executor; "
                     f"driver {resources['driver_memory']}")
            st.caption(f"Phase timeout: {timeout} seconds. Custom parameters are included in the downloadable configuration.")
            if target['master'].startswith('local'):
                st.caption('Local mode runs on the submission host. Requested executor resources do not provision distributed workers.')
            st.caption('Monitoring: ' + ('enabled' if monitor_enabled else 'disabled') +
                       f" · Workload overrides: {sum(bool(w['parameters']) for w in chosen)} · Spark property overrides: {len(spec['spark_conf'])}")
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
        st.download_button("Save experiment", yaml.safe_dump(config.overrides(spec), sort_keys=False), "experiment.yaml", "application/yaml",
            help="Download overrides relative to conf/. Runs retain the complete frozen configuration.")
        if st.button("New run", help="Create a fresh submission key. Repeated clicks on Run otherwise refer to the same submission, preventing accidental duplicate jobs."):
            st.session_state.request_key = str(uuid.uuid4())
            st.info("The next click on Run will create a new execution.")
