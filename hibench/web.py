"""Streamlit entry point: navigation only; services own execution and cluster operations."""
import uuid
import streamlit as st
from hibench import config
from hibench.catalog import repository_root
from hibench.ui import experiments, executions, clusters, help_page, workloads as workloads_page, general

st.set_page_config(page_title="HiBench Next", page_icon="⚡", layout="wide")
st.markdown("""
<style>
[data-testid="stSidebar"] {border-right: 1px solid rgba(128,128,128,.16);}
[data-testid="stSidebar"] [data-testid="stButton"] button {
    justify-content:flex-start; padding:.8rem 1rem; border-radius:.75rem;
    margin:.08rem 0; transition:background .15s ease;
}
[data-testid="stSidebar"] [data-testid="stButton"] button:hover {
    border-color:rgba(56,189,248,.5);
}
[data-testid="stSidebar"] [data-testid="stButton"] button p {font-weight:600;}
.hibench-brand {display:flex; align-items:center; gap:.85rem; margin:.5rem 0 1.4rem;}
.hibench-mark {background:linear-gradient(135deg,#0284c7,#0f766e); color:white;
    border-radius:14px; padding:.7rem .85rem; font-weight:800; font-size:1.2rem;}
.hibench-name {font-size:1.65rem; font-weight:750; letter-spacing:-.04em;}
.hibench-tagline {font-size:.8rem; opacity:.65; margin-top:.1rem;}
</style>
""", unsafe_allow_html=True)
if "experiment" not in st.session_state:
    st.session_state.experiment = config.load(repository_root() / "configs" / "docker-yarn.yaml")
if "request_key" not in st.session_state:
    st.session_state.request_key = str(uuid.uuid4())
with st.sidebar:
    st.markdown('<div class="hibench-brand"><div class="hibench-mark">HB</div>'
                '<div><div class="hibench-name">HiBench Next</div>'
                '<div class="hibench-tagline">Reproducible benchmarks for Apache Spark</div></div></div>', unsafe_allow_html=True)
    if "ui_page" not in st.session_state:
        st.session_state.ui_page = "Experiments"
    def navigate(destination):
        st.session_state.ui_page = destination
    st.caption("WORKSPACE")
    labels = {"Experiments": "🧪  Experiments", "Runs": "📊  Runs",
              "Clusters": "🖥️  Clusters", "Help": "📖  Help"}
    for destination, label in labels.items():
        st.button(label, key="nav-" + destination, width="stretch",
                  type="primary" if st.session_state.ui_page == destination else "secondary",
                  on_click=navigate, args=(destination,))
    st.divider()
    st.caption("PARAMETRIZATION")
    st.button("🛠️  General", key="nav-General", width="stretch",
              type="primary" if st.session_state.ui_page == "General" else "secondary",
              on_click=navigate, args=("General",))
    st.button("⚙️  Workloads", key="nav-Workloads", width="stretch",
              type="primary" if st.session_state.ui_page == "Workloads" else "secondary",
              on_click=navigate, args=("Workloads",))
    st.divider()
    page = st.session_state.ui_page
    st.caption("BATCH LAB")
    st.caption("Spark 3.5 · Python 3\n\nConfigure. Run. Compare.")
if page == "Experiments":
    st.title("Experiments")
    st.caption("Select a target and configure a reproducible benchmark.")
    experiments.render()
elif page == "Runs":
    executions.render()
elif page == "General":
    general.render()
elif page == "Workloads":
    workloads_page.render()
elif page == "Clusters":
    clusters.render()
else:
    help_page.render()
