"""Cluster management page: explicit actions delegate to lifecycle services."""
import streamlit as st
from hibench.services import clusters

def render():
    st.title("Clusters")
    st.caption("Manage the existing Docker lab or install a copy of the project.")
    profile = clusters.profile()
    with st.container(border=True):
        st.subheader("Spark / Hadoop lab")
        directory = st.text_input("Cluster project directory", profile["directory"])
        project = st.text_input("Docker Compose project", profile["project"], help="Use the name used to create the cluster; the lab uses spark-cluster.")
        ref = st.text_input("Branch or tag to download", profile["ref"])
        selected = dict(directory=directory, project=project, ref=ref)
        if st.button("Save cluster profile"):
            try:
                clusters.save(selected)
                st.success("Profile saved")
            except Exception as exc:
                st.error(str(exc))
        if st.button("Check status"):
            try:
                rows = clusters.status(selected)
                if rows:
                    st.dataframe(rows, width="stretch", hide_index=True)
                else:
                    st.info("No containers have been created for this project.")
            except Exception as exc:
                st.error(str(exc))
    st.subheader("Setup and management assistant")
    st.markdown("1. Select an existing checkout or download the repository into a new directory.\n"
                "2. Build the images if they are not available yet.\n"
                "3. Start and verify the services. Startup waits for their healthchecks.")
    st.caption("Repository: " + clusters.REPOSITORY)
    st.info("The lab requires Docker with Linux containers, Docker Compose and Git for downloading. "
            "Its configured limits total about 26 GiB of RAM, plus disk space for images and data.")
    choices = {"Download repository": "download", "Build images": "build",
               "Start and verify": "up", "Stop cluster": "stop", "Remove containers": "down"}
    choice = st.selectbox("Cluster operation", list(choices))
    action = choices[choice]
    valid = True
    try:
        commands = clusters.plan(selected, action)
        st.code("\n".join(" ".join(command) for command in commands), language="text")
    except Exception as exc:
        valid = False
        st.warning(str(exc))
    if action in ("stop", "down"):
        st.warning("This operation interrupts cluster services. Volumes and datasets are preserved.")
        valid = st.checkbox("I confirm that I want to stop cluster services") and valid
    if st.button(choice, disabled=not valid, type="primary"):
        try:
            operation = clusters.start(selected, action)
            st.success("Operation started: " + operation)
        except Exception as exc:
            st.error(str(exc))
    @st.fragment(run_every="2s")
    def progress():
        result = clusters.latest()
        if result:
            task, log = result
            labels = {"queued": "Queued", "running": "Running", "succeeded": "Completed", "failed": "Failed"}
            st.subheader("Latest operation")
            st.write(labels.get(task["state"], task["state"]) + " · " + task["action"])
            if task.get("detail"):
                st.error(task["detail"])
            st.code(log or "Waiting for output…", language="text", height=300)
    progress()
    st.subheader("Local lab services")
    st.markdown("[YARN](http://127.0.0.1:8088) · [HDFS](http://127.0.0.1:9870) · "
                "[Spark History](http://127.0.0.1:18080) · [Spark Master](http://127.0.0.1:8080)")
