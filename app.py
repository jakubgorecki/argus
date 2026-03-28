import streamlit as st
import pandas as pd
import sqlite3
import os

st.set_page_config(
    page_title="Argus Platform",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database helper for routing
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "argus.db")
    return sqlite3.connect(db_path)

def load_case_ids():
    try:
        conn = get_db_connection()
        ids = pd.read_sql("SELECT ID FROM cases", conn)['ID'].tolist()
        conn.close()
        return ids
    except Exception:
        return []

# Define standard pages
dashboard = st.Page("views/dashboard.py", title="Dashboard", icon=":material/dashboard:", default=True)
cases_list = st.Page("views/cases.py", title="Cases", icon=":material/work:", url_path="cases")
integrations = st.Page("views/integrations.py", title="Integrations", icon=":material/extension:")
reports = st.Page("views/reports.py", title="Reports", icon=":material/analytics:")
db_editor = st.Page("views/db_editor.py", title="DB Admin", icon=":material/database:")
debugger = st.Page("views/debugger.py", title="Debugger", icon=":material/bug_report:")

# Handle Selection Logic
# Reverting to Query Parameters based routing as Streamlit 1.55 doesn't support nested url_paths
# like 'cases/ID'. To ensure a clean experience, we'll use st.query_params.

st.logo("logo.svg", icon_image="icon.svg", size="large")

pg = st.navigation({
    "Main": [dashboard, cases_list, integrations, reports],
    "System": [db_editor, debugger]
})

with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

from views import components
components.render_topbar()

# Sync current selection: Priority to Query Params
if "selected_case" in st.query_params:
    st.session_state["selected_case"] = st.query_params["selected_case"]

if pg.title == "Cases" and st.session_state.get('selected_case'):
    case_id = st.session_state.get('selected_case')
    components.render_breadcrumbs([
        ("ARGUS", "/"),
        ("Cases", "/cases"),
        (case_id, None)
    ])
else:
    components.render_breadcrumbs(pg.title)

pg.run()
