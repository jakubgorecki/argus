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
db_editor = st.Page("views/db_editor.py", title="DB Admin", icon=":material/database:")
debugger = st.Page("views/debugger.py", title="Debugger", icon=":material/bug_report:")

st.logo("logo.svg", icon_image="icon.svg", size="large")

pg = st.navigation([dashboard, cases, integrations, reports, db_editor, debugger])

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

from views import components
components.render_topbar()

if "selected_case" not in st.session_state:
    st.session_state["selected_case"] = None

if "action" in st.query_params and st.query_params["action"] == "clear_case":
    st.session_state["selected_case"] = None
    del st.query_params["action"]
    
if "selected_case" in st.query_params:
    st.session_state["selected_case"] = st.query_params["selected_case"]
    del st.query_params["selected_case"]

if pg.title == "Cases" and st.session_state['selected_case'] is not None:
    case_id = st.session_state['selected_case']
    components.render_breadcrumbs([
        ("ARGUS", "/"),
        ("Cases", "/cases?action=clear_case"),
        (case_id, None)
    ])
else:
    components.render_breadcrumbs(pg.title)

pg.run()
