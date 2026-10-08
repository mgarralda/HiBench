import json
from datetime import datetime
import streamlit as st
from hibench import store

def render():
    st.title("Runs")
    current, history = st.tabs(["Monitoring", "History"])
    with current:
        @st.fragment(run_every="2s")
        def run_panel():
            run_id = st.session_state.get("active_run")
            if not run_id:
                st.info("Submit an experiment or open a run from history.")
                return
            row = store.get(run_id)
            labels = {"queued": "Queued", "waiting": "Waiting for target", "running": "Running",
                      "succeeded": "Completed", "failed": "Failed", "cancelled": "Cancelled"}
            st.write("**" + labels.get(row["state"], row["state"]) + "** · " + row["detail"])
            st.caption(run_id)
            if row["state"] not in ("succeeded", "failed", "cancelled") and st.button("Cancel run"):
                store.cancel(run_id)
                st.info("Cancellation requested; the status will update when the worker processes it.")
            path = store.directory(run_id) / "output.log"
            if path.exists():
                with path.open("rb") as stream:
                    stream.seek(max(0, path.stat().st_size - 40000))
                    st.code(stream.read().decode("utf-8", errors="replace"), language="text", height=360)
            event_path = store.directory(run_id) / "events.jsonl"
            metrics = []
            if event_path.exists():
                for line in event_path.read_text(encoding="utf-8").splitlines():
                    try:
                        event = json.loads(line)
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
        run_panel()
    with history:
        rows = store.recent()
        st.dataframe([{**row, "created": datetime.fromtimestamp(row["created"]).isoformat(timespec="seconds"),
                      "updated": datetime.fromtimestamp(row["updated"]).isoformat(timespec="seconds")} for row in rows], width="stretch")
        if rows:
            selected_run = st.selectbox("Open run", [x["id"] for x in rows])
            if st.button("View run"):
                st.session_state.active_run = selected_run
                st.rerun()
