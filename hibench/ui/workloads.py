"""Reference scales and workload-specific experiment parameters."""
import uuid
import streamlit as st
from hibench.catalog import SCALES, workloads
from hibench.workload_parameters import details, customize, typed_value, MEANINGS


def render():
    st.title("Workloads")
    st.caption("Inspect reference scales and workload behavior. Customize values for a reproducible experiment.")
    catalog = workloads()
    left, right = st.columns([1, 2])
    category = left.selectbox("Workload family", list(dict.fromkeys(w["category"] for w in catalog)))
    selected = right.selectbox("Workload", [w["id"] for w in catalog if w["category"] == category])
    item, rows, other, path = details(selected)
    st.caption(item["description"])
    st.subheader("Reference sizes")
    st.caption("Original scale definitions, including parameters that vary with size. Storage bytes depend on the data format.")
    if rows:
        st.dataframe(rows, hide_index=True, width="stretch")
    else:
        st.info("This workload does not define configurable scale parameters.")
    fixed = [{"Parameter": name, "Default": schema["presets"]["tiny"],
              "Meaning": schema.get("note", MEANINGS.get(name, schema.get("unit", "")))}
             for name, schema in item["parameters"].items() if len(set(schema["presets"].values())) == 1]
    if fixed:
        st.subheader("Behavior and fixed parameters")
        st.dataframe(fixed, hide_index=True, width="stretch")
    with st.expander("Other workload configuration"):
        st.caption("Configuration expressions include derived settings and runtime paths. Generation and shuffle partitions are configured in Experiments.")
        if other:
            st.dataframe(other, hide_index=True, width="stretch")
        st.caption("Source: " + str(path.relative_to(path.parents[3])))
    resources = st.session_state.experiment["resources"]
    st.info(f"Shared execution settings: {resources['map_partitions']} map partitions · "
            f"{resources['shuffle_partitions']} shuffle partitions. Configure these in Experiments; "
            "they can affect data layout and generator content.")
    if selected == "ml.linear":
        st.caption("Linear generation defaults to the shuffle partition count. Changing it changes the random streams and dataset. Metrics are measured on the training split.")
    st.subheader("Customize for an experiment")
    base = st.session_state.experiment
    scale = st.selectbox("Reference scale", SCALES, index=SCALES.index(base["scale"]))
    active = next((w["parameters"] for w in base["workloads"] if w["id"] == selected), {}) if scale == base["scale"] else {}
    st.caption("Applying adds this workload to the current experiment and sets the experiment scale for all selected workloads. Reference definitions stay unchanged. Save the experiment as YAML from Experiments.")
    editing = st.checkbox("Edit parameters", key="workload-edit-" + selected)
    values = {}
    with st.form("workload-parameters-" + selected + "-" + scale):
        for name, schema in item["parameters"].items():
            initial = typed_value(schema, active.get(name, schema["presets"][scale]))
            kwargs = {"key": "preset-" + selected + "-" + scale + "-" + name,
                      "disabled": not editing or bool(schema.get("note")), "help": schema.get("note", MEANINGS.get(name, schema.get("unit")))}
            if schema["kind"] == "boolean":
                values[name] = st.checkbox(name, value=initial, **kwargs)
            elif schema["kind"] in ("integer", "number"):
                values[name] = st.number_input(name, min_value=0 if schema["kind"] == "integer" else 0.0, value=initial, **kwargs)
            else:
                values[name] = st.text_input(name, value=initial, **kwargs)
        applied = st.form_submit_button("Use in experiment", type="primary")
    if applied:
        try:
            st.session_state.experiment = customize(base, selected, scale, values)
            st.session_state.request_key = str(uuid.uuid4())
            st.success("Workload settings applied. Open Experiments to review, save or run them.")
        except (ValueError, TypeError) as exc:
            st.error(str(exc))
