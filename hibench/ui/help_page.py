import streamlit as st

def render():
    st.title("Help")
    st.subheader("Prepare and run a benchmark")
    st.markdown("1. In **Clusters**, select an existing project or use the assistant to download, build and start it.\n"
                "2. In **Experiments**, select an adapter, check the target and configure your workloads.\n"
                "3. Click Run and open **Runs** to view logs, metrics and history.")
    st.subheader("Clusters and adapters")
    st.write("The cluster hosts Spark, Hadoop and your data. The adapter determines how jobs are submitted. "
             "Existing Docker uses docker exec on a running container; it does not create a cluster. "
             "SSH will support submission to a remote master when implemented.")
    st.subheader("Data and persistence")
    st.write("Preparation creates data; execution uses a compatible prepared dataset. "
             "The controller stores configurations, logs and history. Stopping or removing containers preserves "
             "volumes but interrupts running jobs. Automatic recovery after a worker crash is not implemented.")
    st.subheader("Saved configuration")
    st.write("Download YAML from Experiments and import it to reuse your configuration. "
             "Loading it does not start jobs or import datasets. The lab profile stores its directory and Compose project; "
             "it does not replace the experiment connection settings.")
    st.subheader("Troubleshooting")
    st.write("Check that Docker is running, the Compose project matches the existing cluster, and services are healthy. "
             "Use Check connection to verify the HiBench runtime. After changing scripts or JARs, run hibench install with your configuration.")
