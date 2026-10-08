"""General experiment storage, runtime and logging settings."""
import copy
import uuid
import streamlit as st
from hibench import config, store
from hibench.adapters import create_adapter


def _change_storage_prefix():
    azure = st.session_state["general-provider"] == "Azure Blob Storage (WASB/WASBS)"
    prefixes = ("hdfs://",) if azure else ("wasb://", "wasbs://")
    replacement = "wasbs://" if azure else "hdfs://"
    if st.session_state["general-provider"] not in ("HDFS", "Azure Blob Storage (WASB/WASBS)"):
        return
    for key in ("general-endpoint", "general-datasets", "general-outputs", "general-events"):
        value = st.session_state.get(key, "")
        for prefix in prefixes:
            if value.startswith(prefix):
                st.session_state[key] = replacement + value[len(prefix):]
                break


def render():
    st.title("General settings")
    st.caption("Storage and logging settings for the current experiment. Existing runs retain their saved configuration.")
    base = copy.deepcopy(st.session_state.experiment)
    target = base["target"]
    initial_endpoint = target.get("hdfs_uri", "")
    for key, value in (("general-endpoint", initial_endpoint),
                       ("general-datasets", target.get("dataset_base", initial_endpoint + "/HiBench-Control/datasets")),
                       ("general-outputs", target.get("output_base", initial_endpoint + "/HiBench-Control/runs")),
                       ("general-events", str(base["spark_conf"].get("spark.eventLog.dir", "")))):
        st.session_state.setdefault(key, value)
    st.subheader("Benchmark data storage")
    providers = ["HDFS", "Azure Blob Storage (WASB/WASBS)", "Amazon S3 (soon)", "Azure ADLS (ABFS/ABFSS) (soon)", "Google Cloud Storage (soon)"]
    provider = st.selectbox("Dataset storage backend", providers, index=1 if target.get("hdfs_uri", "").startswith(("wasb://", "wasbs://")) else 0, key="general-provider", on_change=_change_storage_prefix)
    supported = provider in providers[:2] and target["kind"] in ("docker", "local")
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
        st.subheader("Spark event logs")
        st.caption("Independent from dataset storage and execution adapter. WASBS, ABFSS, S3A and GS URIs can be passed through when their connector and authentication are configured on the runtime.")
        conf = base["spark_conf"]
        enabled = st.checkbox("Enable Spark event logs", value=str(conf.get("spark.eventLog.enabled", True)).lower() == "true")
        event_uri = st.text_input("Event log URI", key="general-events", help="Examples: hdfs://host:9000/spark-events or wasbs://container@account.blob.core.windows.net/spark-events. Leave blank to inherit runtime settings. Spark must be able to write to the directory.")
        history = st.text_input("YARN History Server address", str(conf.get("spark.yarn.historyServer.address", "")), help="Optional host:port for the Spark History Server link.")
        st.caption("Configure spark.history.fs.logDirectory separately on the History Server to read the event-log location. Setting it on a submitted job does not configure the running History Server.")
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
            conf["spark.eventLog.enabled"] = enabled
            for key, value in (("spark.eventLog.dir", event_uri), ("spark.yarn.historyServer.address", history)):
                if value.strip(): conf[key] = value.strip()
                else: conf.pop(key, None)
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
