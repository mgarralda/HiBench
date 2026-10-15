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

"""Reference scales and workload-specific experiment parameters."""
import hashlib
import streamlit as st
from hibench.catalog import SCALES, workloads
from hibench.ui.spark_settings import SAVE_HELP, update_saved_target, target_update_status
from hibench.parameters.workloads import details, save_defaults, typed_value, MEANINGS


def render():
    st.title("Workloads")
    target_update_status()
    st.caption("View and edit shared workload defaults in conf/. Use Experiments for changes limited to one execution.")
    catalog = workloads()
    left, right = st.columns([1, 2])
    category = left.selectbox("Workload family", list(dict.fromkeys(w["category"] for w in catalog)))
    selected = right.selectbox("Workload", [w["id"] for w in catalog if w["category"] == category])
    item, rows, other, path = details(selected)
    st.caption(item["description"])
    st.subheader("Configured sizes")
    st.caption("Current scale definitions in conf/, including generator parameters. Editing them changes the shared benchmark base. Storage bytes depend on the data format.")
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
        st.caption("Derived expressions and runtime paths are shown for reference. Shared generation/shuffle settings come from conf/hibench.conf; Experiments can override them.")
        if other:
            st.dataframe(other, hide_index=True, width="stretch")
        st.caption("Source: " + str(path.relative_to(path.parents[3])))
    from hibench import config
    resources = config.default_experiment()["resources"]
    st.info(f"Shared execution settings: {resources['generation_partitions']} map partitions · "
            f"{resources['generation_shuffle_partitions']} generator shuffle partitions. Shared values come from conf/; Experiments can override them. "
            "they can affect data layout and generator content.")
    if selected == "ml.linear":
        st.caption("Linear generation defaults to the generator shuffle partition count. Changing it changes the random streams and dataset. Metrics are measured on the training split.")
    st.subheader("Edit shared defaults")
    scale = st.selectbox("Scale", SCALES, index=SCALES.index(config.default_experiment()['scale']))
    revision = hashlib.sha256(path.read_bytes()).hexdigest()
    st.caption("Save writes the workload configuration file. Scale-specific changes affect only the chosen size; fixed parameters affect all sizes. Existing experiment overrides remain unchanged. Saving also updates the target selected in Experiments.")
    editing = st.checkbox("Edit parameters", key="workload-edit-" + selected)
    values = {}
    with st.form("workload-parameters-" + selected + "-" + scale):
        for name, schema in item["parameters"].items():
            initial = typed_value(schema, schema["presets"][scale])
            kwargs = {"key": "preset-" + selected + "-" + scale + "-" + revision + "-" + name,
                      "disabled": not editing or 'Legacy setting' in schema.get('note', '') or 'Historical unused property' in schema.get('note', ''),
                      "help": schema.get("note", MEANINGS.get(name, schema.get("unit")))}
            if schema["kind"] == "boolean":
                values[name] = st.checkbox(name, value=initial, **kwargs)
            elif schema["kind"] in ("integer", "number"):
                values[name] = st.number_input(name, min_value=0 if schema["kind"] == "integer" else 0.0, value=initial, **kwargs)
            else:
                values[name] = st.text_input(name, value=initial, **kwargs)
        applied = st.form_submit_button("Save and update target", type="primary", disabled=not editing, help=SAVE_HELP)
    if applied:
        try:
            result = save_defaults(selected, scale, values, revision)
            update_saved_target()
            st.rerun()
        except (ValueError, TypeError) as exc:
            st.error(str(exc))
