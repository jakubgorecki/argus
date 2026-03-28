import streamlit as st
import pandas as pd
import sqlite3
import os

# Database connection helper
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "..", "argus.db")
    return sqlite3.connect(db_path)

@st.cache_data
def load_cases_data():
    conn = get_db_connection()
    cases = pd.read_sql("SELECT * FROM cases", conn)
    metrics = pd.read_sql("SELECT * FROM case_metrics", conn)
    conn.close()
    return cases, metrics

cases_df, metrics_df = load_cases_data()

st.title("Case Management")

# Top Metrics Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    val = metrics_df[metrics_df['metric'] == 'Active Cases']['value'].values[0]
    delta = metrics_df[metrics_df['metric'] == 'Active Cases']['delta'].values[0]
    st.metric("Active Cases", val, delta=delta, delta_color="normal")

with col2:
    val = metrics_df[metrics_df['metric'] == 'Pending Review (High Risk)']['value'].values[0]
    delta = metrics_df[metrics_df['metric'] == 'Pending Review (High Risk)']['delta'].values[0]
    st.metric("Pending Review (High Risk)", val, delta=delta, delta_color="inverse")

with col3:
    val = metrics_df[metrics_df['metric'] == 'AI Auto-Cleared (24H)']['value'].values[0]
    delta = metrics_df[metrics_df['metric'] == 'AI Auto-Cleared (24H)']['delta'].values[0]
    st.metric("AI Auto-Cleared (24H)", val, delta=delta, delta_color="normal")

with col4:
    val = metrics_df[metrics_df['metric'] == 'Avg. Resolution Time']['value'].values[0]
    delta = metrics_df[metrics_df['metric'] == 'Avg. Resolution Time']['delta'].values[0]
    # Unit is hrs
    st.metric("Avg. Resolution Time", f"{val} hrs", delta=delta if delta else None)

st.markdown("<br>", unsafe_allow_html=True)

# Filters Row
f_col1, f_col2, f_col3, f_col_empty, f_col4 = st.columns([2, 2, 2, 5, 3])
with f_col1:
    st.selectbox("Filters", ["All Statuses", "Requires Review", "In Progress"], label_visibility="collapsed")
with f_col2:
    st.selectbox("Risk", ["Risk: All Levels", "High", "Medium", "Low"], label_visibility="collapsed")
with f_col3:
    st.selectbox("Entity", ["Entity: All", "Corporate", "Individual"], label_visibility="collapsed")
with f_col4:
    st.button("+ New Case", type="primary", use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# Responsive Case Cards replacing rigid Dataframe
for idx, row in cases_df.iterrows():
    with st.container(border=True):
        col1, col2, col3, col4, col5, col6 = st.columns([1.5, 3, 2, 2, 2, 2], vertical_alignment="center")
        
        with col1:
            st.markdown(f"<div style='font-weight:700; color:#4A192C;'>{row['ID']}</div>", unsafe_allow_html=True)
            st.caption(f"{row['LAST_ACTIVITY']}")
            
        with col2:
            st.markdown(f"<div style='font-weight:600; font-size:15px; color:#2D1A22;'>{row['COUNTRY']} {row['ENTITY_NAME']}</div>", unsafe_allow_html=True)
            st.caption(row['TYPE'])
            
        with col3:
            # Simple progress bar replacement
            st.markdown(f"<div style='font-size:12px; margin-bottom:4px; font-weight:600;'>RISK SCORE: {row['RISK_SCORE']:.1f}</div>", unsafe_allow_html=True)
            st.progress(int(row['RISK_SCORE']))
            
        with col4:
            st.markdown(f"<div style='font-size:12px; font-weight:600; color:#8C7C83;'>AI CONFIDENCE</div><div style='font-weight:600;'>{row['AI_CONFIDENCE']}</div>", unsafe_allow_html=True)
            
        with col5:
            color = "#E53E3E" if row['STATUS'] == "Pending Review" else ("#D69E2E" if "Investigation" in row['STATUS'] else "#38A169")
            st.markdown(f"<div style='background-color:{color}; color:white; padding:4px 8px; border-radius:4px; font-size:12px; font-weight:600; text-align:center; display:inline-block;'>{row['STATUS']}</div>", unsafe_allow_html=True)
            
        with col6:
            if st.button("Open Case", key=f"case_btn_{row['ID']}", use_container_width=True):
                st.session_state['selected_case'] = row['ID']
                
if 'selected_case' in st.session_state:
    st.success(f"Case {st.session_state['selected_case']} selected! (Ready for details view binding)")

