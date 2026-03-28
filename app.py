import streamlit as st

st.set_page_config(
    page_title="Argus Platform",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

dashboard = st.Page("views/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True)
cases = st.Page("views/cases.py", title="Cases", icon=":material/work:")
integrations = st.Page("views/integrations.py", title="Integrations", icon=":material/extension:")
reports = st.Page("views/reports.py", title="Reports", icon=":material/analytics:")
debugger = st.Page("views/debugger.py", title="Debugger", icon=":material/bug_report:")

st.logo("logo.svg", icon_image="icon.svg", size="large")

pg = st.navigation([dashboard, cases, integrations, reports, debugger])

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

from views.components import render_topbar, render_breadcrumbs
render_topbar()
render_breadcrumbs(pg.title)

pg.run()
