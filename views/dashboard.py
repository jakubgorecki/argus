import streamlit as st
import pandas as pd
import sqlite3
import os

# Database connection helper
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "..", "argus.db")
    return sqlite3.connect(db_path)

@st.cache_data
def fetch_dash_cases():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM cases", conn)
    conn.close()
    return df

@st.cache_data
def get_chart_data():
    conn = get_db_connection()
    df = pd.read_sql("SELECT * FROM ai_metrics", conn)
    conn.close()
    return df

st.title("Surveillance Overview")
st.caption("Real-time risk orchestration and case intelligence for Enfuce financial networks.")

st.markdown("<br>", unsafe_allow_html=True)

col_charts, col_metrics = st.columns([1.5, 1])

with col_charts:
    with st.container(border=True):
        st.markdown("<h4 style='display:inline-block; margin:0;'>AI Noise Reduction</h4>  <span style='background:#EFEBEB; color:#2D1A22; padding:4px 12px; border-radius:12px; font-weight:600; font-size:12px; margin-left:12px;'>+14.2%</span>", unsafe_allow_html=True)
        st.caption("EFFICIENCY METRICS • LAST 24H")
        
        df_chart = get_chart_data()
        st.bar_chart(df_chart, x="Day", y="Noise_Removed", color="#8C7C83", height=250)

with col_metrics:
    with st.container(border=True):
        st.markdown("<h4 style='margin:0; color:#4A192C;'>Pending Review</h4>", unsafe_allow_html=True)
        st.markdown("<div style='display:flex; justify-content:space-between; align-items:flex-start;'><h1 style='margin:0; padding:0; color:#4A192C;'>124</h1> <span class='material-symbols-rounded' style='color:#4A192C; font-size:24px; padding-top:8px;'>assignment_turned_in</span></div>", unsafe_allow_html=True)
        st.caption("Priority cases awaiting officer verification.")
        st.markdown("""
            <div style='display:flex; margin-top:12px;'>
                <div style="background:#2D1A22; color:white; width:28px; height:28px; border-radius:50%; display:flex; justify-content:center; align-items:center; font-size:10px; margin-left:-0px; border:2px solid #FAFAFA;">AB</div>
                <div style="background:#4A192C; color:white; width:28px; height:28px; border-radius:50%; display:flex; justify-content:center; align-items:center; font-size:10px; margin-left:-8px; border:2px solid #FAFAFA;">CD</div>
                <div style="background:#E53E3E; color:white; width:28px; height:28px; border-radius:50%; display:flex; justify-content:center; align-items:center; font-size:10px; margin-left:-8px; border:2px solid #FAFAFA; font-weight:bold;">+8</div>
            </div>
        """, unsafe_allow_html=True)

    with st.container(border=True):
        st.markdown("<h5 style='margin:0; margin-bottom: 8px;'><span class='material-symbols-rounded' style='font-size:18px; color:#4A192C; vertical-align:middle;'>bolt</span> Argus AI Insights</h5>", unsafe_allow_html=True)
        st.write("Anomaly detected in Nordics settlement cluster. Velocity spike of 240% above baseline.")

st.markdown("<br><br>", unsafe_allow_html=True)

with st.container(border=True):
    col_qtitle, col_qbuttons = st.columns([1, 1])
    with col_qtitle:
        st.subheader("Priority Case Queue")
        st.caption("MANAGED INTELLIGENCE FEED")
    with col_qbuttons:
        st.markdown("<div style='display:flex; justify-content:flex-end; gap:8px;'>", unsafe_allow_html=True)
        bc0, bc1, bc2 = st.columns([4, 1.5, 1.5])
        with bc1:
            st.button("FILTER", use_container_width=True)
        with bc2:
            st.button("EXPORT", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)
        
    df = fetch_dash_cases().head(5) # Limit Dashboard to top 5 cases
    for idx, row in df.iterrows():
        with st.container(border=True):
            c_id, c_ent, c_risk, c_stat, c_act = st.columns([1, 3, 2, 2, 1], vertical_alignment="center")
            with c_id:
                st.markdown(f"<span style='font-weight:700; color:#4A192C;'>{row['ID']}</span>", unsafe_allow_html=True)
            with c_ent:
                st.markdown(f"**{row['COUNTRY']} {row['ENTITY_NAME']}**<br/><span style='font-size:12px; color:#8C7C83;'>{row['TYPE']}</span>", unsafe_allow_html=True)
            with c_risk:
                st.markdown(f"<div style='font-size:12px; margin-bottom:4px; font-weight:600;'>RISK SCORE: {row['RISK_SCORE']:.1f}</div>", unsafe_allow_html=True)
                st.progress(int(row['RISK_SCORE']))
            with c_stat:
                color = "#E53E3E" if row['STATUS'] == "Pending Review" else ("#D69E2E" if "Investigation" in row['STATUS'] else "#38A169")
                st.markdown(f"<div style='background-color:{color}; color:white; padding:4px 8px; border-radius:4px; font-size:12px; font-weight:600; text-align:center; display:inline-block;'>{row['STATUS']}</div>", unsafe_allow_html=True)
            with c_act:
                if st.button("Review", key=f"dash_btn_{row['ID']}", use_container_width=True):
                    st.session_state['selected_case'] = row['ID']
                    
    if 'selected_case' in st.session_state:
        st.success(f"Case {st.session_state['selected_case']} selected from Dashboard queue.")
        
    st.markdown("<div style='text-align:center; margin-top:16px; font-size:14px; font-weight:600;'><a href='/cases' style='color:#2D1A22; text-decoration:none;'>View All Active Cases</a></div>", unsafe_allow_html=True)
