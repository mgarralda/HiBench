import copy
import json
import uuid
from datetime import datetime
import yaml
import streamlit as st
from hibench import config, store
from hibench.adapters import create_adapter, REGISTRY
from hibench.catalog import SCALES, repository_root, workloads
from hibench import worker

def render():
    base = copy.deepcopy(st.session_state.experiment)
    imported = st.file_uploader("Load saved configuration (YAML or JSON)", type=["yaml", "yml", "json"])
    if imported is not None and st.button("Load configuration"):
        try:
            st.session_state.experiment = config.validate(yaml.safe_load(imported.getvalue()))
            st.session_state.request_key = str(uuid.uuid4())
            st.rerun()
        except Exception as exc:
            st.error(str(exc))
    target = copy.deepcopy(base["target"])
    st.subheader("1. Execution target")
    with st.container(border=True):
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
            else:
                target.pop("container", None)
                st.caption("The controller and runtime must run on the same Linux machine.")
            for key, label in (("workspace", "HiBench directory on the target"), ("spark_home", "Spark home"),
                               ("hadoop_home", "Hadoop home"), ("master", "Spark master"), ("hdfs_uri", "Storage endpoint (HDFS or WASB/WASBS)")):
                target[key] = st.text_input(label, target[key])
            target["deploy_mode"] = st.selectbox("Deploy mode", ["client", "cluster"], index=["client", "cluster"].index(target["deploy_mode"]))
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
        repetitions = st.number_input("Benchmark repetitions", 1, 100, base["repetitions"])
        timeout = st.number_input("Phase timeout (seconds)", 1, 604800, base["timeout_s"])
    resources = copy.deepcopy(base["resources"])
    with st.expander("Requested experiment resources"):
        st.caption("These resources are requested for each benchmark; they do not describe the total cluster capacity.")
        columns = st.columns(2)
        for index, (key, label) in enumerate((
            ("executor_instances", "Executors"), ("executor_cores", "Cores per executor"),
            ("map_partitions", "Generation partitions"), ("shuffle_partitions", "Shuffle partitions"))):
            resources[key] = columns[index % 2].number_input(label, 1, 10000, resources[key])
        resources["executor_memory"] = columns[0].text_input("Executor memory", resources["executor_memory"])
        resources["driver_memory"] = columns[1].text_input("Driver memory", resources["driver_memory"])
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
        st.caption("Add any supported Spark property using its exact name and a YAML value. These settings are saved in the experiment and passed to spark-submit through its properties file. The selected Spark runtime must support the property; unknown names may be ignored by Spark.")
        conf_text = st.text_area("Spark properties", yaml.safe_dump(base["spark_conf"], sort_keys=True), height=260,
                                help="Use key: value, for example spark.sql.adaptive.enabled: true. Add dependent settings as separate properties. Lines starting with # are comments and are not applied. Executor count, cores, memory, master and parallelism use the dedicated fields above.")
        st.caption("Save experiment includes these properties in YAML. Review their effects before comparing benchmark results.")
        with st.expander("Property examples and dependencies"):
            st.markdown("**Fixed benchmark resources**")
            st.code("spark.dynamicAllocation.enabled: false\nspark.sql.adaptive.enabled: false\nspark.executor.extraJavaOptions: '-XX:+UseG1GC'", language="yaml")
            st.markdown("**Dynamic Allocation** — requires a supported shuffle-preservation mechanism on the runtime. This example uses shuffle tracking; an external shuffle service is another option when configured on the cluster.")
            st.code("spark.dynamicAllocation.enabled: true\nspark.dynamicAllocation.shuffleTracking.enabled: true\nspark.dynamicAllocation.minExecutors: 2\nspark.dynamicAllocation.initialExecutors: 2\nspark.dynamicAllocation.maxExecutors: 10", language="yaml")
            st.markdown("**Adaptive Query Execution (AQE)** — dependent options take effect when AQE is enabled.")
            st.code("spark.sql.adaptive.enabled: true\nspark.sql.adaptive.coalescePartitions.enabled: true\nspark.sql.adaptive.advisoryPartitionSizeInBytes: 64m\nspark.sql.adaptive.skewJoin.enabled: true", language="yaml")
            st.markdown("[Spark configuration reference](https://spark.apache.org/docs/3.5.7/configuration.html) · [AQE settings](https://spark.apache.org/docs/3.5.7/sql-performance-tuning.html#adaptive-query-execution)")
    try:
        spec = config.validate(dict(schema_version=1, name=name, target=target, mode=modes[mode],
                                    scale=scale, repetitions=int(repetitions), timeout_s=int(timeout),
                                    resources=resources, spark_conf=yaml.safe_load(conf_text) or {}, workloads=chosen))
    except Exception as exc:
        spec = None
        if implemented:
            st.error(str(exc))
    if not implemented:
        st.button("Run", type="primary", disabled=True)
    if spec:
        st.session_state.experiment = spec
        st.download_button("Save experiment", yaml.safe_dump(spec, sort_keys=False), "experiment.yaml", "application/yaml")
        st.subheader("Review execution")
        with st.container(border=True):
            destination_name = target.get("container") if target["kind"] == "docker" else target["workspace"]
            st.write(f"**Target:** {target['kind']} · {destination_name} · {target['master']}")
            st.write("**Workloads:** " + ", ".join(selected))
            st.write(f"**Operation:** {mode} · scale {scale} · {repetitions} repetition(s)")
            st.write(f"**Resources:** {resources['executor_instances']} executor(s), "
                     f"{resources['executor_cores']} core(s) and {resources['executor_memory']} per executor; "
                     f"driver {resources['driver_memory']}")
            st.caption(f"Phase timeout: {timeout} seconds. Custom parameters are included in the downloadable configuration.")
        if st.button("Run", type="primary"):
            try:
                run_id, created = store.create(spec, st.session_state.request_key)
                if created:
                    worker.launch(run_id)
                st.session_state.active_run = run_id
                st.session_state.experiment = spec
                st.success("Run submitted: " + run_id)
            except Exception as exc:
                st.error(str(exc))
        if st.button("New run"):
            st.session_state.request_key = str(uuid.uuid4())
            st.info("The next click on Run will create a new execution.")
