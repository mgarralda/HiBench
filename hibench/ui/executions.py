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

import json
from datetime import datetime
import streamlit as st
from hibench import store

def open_run(run_id):
    """Button callback runs before tabs are constructed, so selection can change."""
    store.get(run_id)
    st.session_state.active_run = run_id
    st.session_state['runs-tab'] = 'Monitoring'
    st.query_params['run'] = run_id


def render():
    st.title("Runs")
    current, history = st.tabs(["Monitoring", "History"], key='runs-tab', on_change='rerun')
    with current:
        @st.fragment(run_every="2s")
        def run_panel():
            run_id = st.session_state.get("active_run")
            if not run_id:
                st.info("Submit an experiment or open a run from history.")
                return
            try:
                row = store.get(run_id)
            except ValueError:
                st.error('This run is unavailable. Open an existing run from History.')
                return
            labels = {"queued": "Queued", "waiting": "Waiting for target", "running": "Running",
                      "succeeded": "Completed", "failed": "Failed", "cancelled": "Cancelled"}
            st.write("**" + labels.get(row["state"], row["state"]) + "** · " + row["detail"])
            st.caption(run_id)
            if row["state"] == "failed":
                st.error(row["detail"] or "The run failed. Inspect the phase logs below before retrying.")
            if row["state"] not in ("succeeded", "failed", "cancelled") and st.button("Cancel run", help="Request cancellation of the active phase and associated Spark application. It can take time to stop; prepared datasets and saved reports are retained."):
                store.cancel(run_id)
                st.info("Cancellation requested; the status will update when the worker processes it.")
            path = store.directory(run_id) / "output.log"
            if path.exists():
                with path.open("rb") as stream:
                    stream.seek(max(0, path.stat().st_size - 40000))
                    st.code(stream.read().decode("utf-8", errors="replace"), language="text", height=360)
            event_path = store.directory(run_id) / "events.jsonl"
            metrics = []
            events = []
            if event_path.exists():
                for line in event_path.read_text(encoding="utf-8").splitlines():
                    try:
                        event = json.loads(line)
                        events.append(event)
                        if event["kind"] == "result":
                            metrics.append(event["metrics"])
                    except json.JSONDecodeError:
                        pass
            if metrics:
                columns = ("workload", "scale", "duration_s", "input_bytes", "throughput_bytes_s", "application_id")
                st.dataframe([{key: result.get(key) for key in columns} for result in metrics], width="stretch")
                st.caption("Duration includes submission and Spark startup.")
                st.download_button("Download results", json.dumps(metrics, indent=2), run_id + ".json", "application/json")
            st.download_button("Run configuration", json.dumps(row["spec"], indent=2), run_id + "-experiment.json", "application/json")
            from hibench.ui.spark_history import render as render_history
            render_history(run_id, row['spec'], events)
            from hibench.services import experiments
            with st.expander("Resource monitoring", expanded=True):
                from hibench.ui.monitoring import render as render_monitoring
                render_monitoring(run_id)
            with st.expander("Reproducibility and export"):
                result = experiments.results(run_id)
                st.json(result['provenance'])
                st.download_button("Download experiment bundle", lambda: experiments.export(run_id), run_id + ".zip", "application/zip", on_click="ignore", help="Build a ZIP on demand with configuration, provenance, console logs, metrics and resource reports. Spark event logs are downloaded separately through Spark History.")
                try:
                    preset = experiments.preset(run_id)
                    st.download_button("Download editable preset", json.dumps(preset, indent=2), run_id + "-preset.json", "application/json")
                except ValueError as exc:
                    st.info("This archived configuration cannot be reused with the current schema: " + str(exc))
                st.caption("Import this preset in Experiments to edit resources, Spark properties and individual workloads before submitting another run.")
                if st.button("Edit as new experiment"):
                    import uuid
                    st.session_state.experiment = experiments.duplicate(run_id)
                    st.session_state.request_key = str(uuid.uuid4())
                    st.session_state.ui_page = 'Experiments'
                    st.query_params.pop('run', None)
                    st.rerun()
        run_panel()
    with history:
        page = st.number_input('History page', min_value=1, value=1, step=1, help='Each page shows up to 100 runs, newest first. Saved runs on older pages are retained.')
        rows = store.recent(limit=100, offset=(page - 1) * 100)
        if rows:
            st.caption('Select a row and click Open run to view its results, logs and resource report.')
            table = []
            for row in rows:
                spec = store.get(row['id'])['spec']
                table.append({'name': spec.get('name', 'Experiment'), 'state': row['state'],
                              'scale': spec['scale'], 'workloads': ', '.join(w['id'] for w in spec['workloads']),
                              'created': datetime.fromtimestamp(row['created']).isoformat(timespec='seconds'),
                              'id': row['id']})
            # A new row set gets a new selection widget, avoiding stale row-index mappings.
            import hashlib
            key = 'run-history-' + hashlib.sha256('|'.join(row['id'] for row in rows).encode()).hexdigest()[:12]
            selected = st.dataframe(table, width='stretch', hide_index=True, key=key,
                                    on_select='rerun', selection_mode='single-row',
                                    column_order=['name', 'state', 'scale', 'workloads', 'created', 'id'],
                                    column_config={'name': 'Experiment', 'state': 'Status', 'scale': 'Scale',
                                                   'workloads': 'Workloads', 'created': 'Created', 'id': 'Run ID'})
            indices = selected.selection.rows
            selected_run = rows[indices[0]]['id'] if indices and indices[0] < len(rows) else None
            st.button('Open run', disabled=selected_run is None, on_click=open_run, args=(selected_run,))
        else:
            st.info('No runs on this page. Return to an earlier page or submit an experiment.')
        st.divider()
        st.button('Compare configurations and results (soon)', disabled=True,
                  help='A useful comparison view will be added in a future version.')
