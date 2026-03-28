import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Argus Platform",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

session = get_active_session()

def load_case_ids():
    try:
        return session.sql("SELECT RESULT_ID AS ID FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS").to_pandas()['ID'].tolist()
    except Exception:
        return []

dashboard = st.Page("views/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True)
cases_list = st.Page("views/cases.py", title="Cases", icon=":material/work:", url_path="cases")
integrations = st.Page("views/integrations.py", title="Integrations", icon=":material/extension:")
reports = st.Page("views/reports.py", title="Reports", icon=":material/analytics:")
db_editor = st.Page("views/db_editor.py", title="DB Admin", icon=":material/database:")
debugger = st.Page("views/debugger.py", title="Debugger", icon=":material/bug_report:")

st.logo("logo.svg", icon_image="icon.svg", size="large")

pg = st.navigation({
    "Main": [dashboard, cases_list, integrations, reports],
    "System": [db_editor, debugger]
})

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

from views import components
components.render_topbar()

if "selected_case" in st.query_params:
    if st.query_params.get("selected_case") == "":
        del st.query_params["selected_case"]
        st.session_state.pop("selected_case", None)
        st.rerun()
    else:
        st.session_state["selected_case"] = st.query_params["selected_case"]
else:
    st.session_state.pop("selected_case", None)

if pg.title == "Cases" and st.session_state.get('selected_case'):
    case_id = st.session_state.get('selected_case')
    components.render_breadcrumbs([
        ("ARGUS", "/"),
        ("Cases", "?page=cases"),
        (case_id, None)
    ])
else:
    components.render_breadcrumbs(pg.title)

pg.run()
