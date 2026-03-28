import streamlit as st
import pandas as pd
import sqlite3
import os

# Database connection helper
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "..", "argus.db")
    return sqlite3.connect(db_path)

# Removed cache to ensure live DB reads
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
    # Custom "Pending Review" Card matching user screenshot
    conn = get_db_connection()
    employees_df = pd.read_sql("SELECT * FROM employees LIMIT 3", conn)
    conn.close()
    
    avatar_html = ""
    for idx, e_row in employees_df.iterrows():
        ml = "-8px" if idx > 0 else "0px"
        avatar_html += f"<img src='{e_row['AVATAR_URL']}' style='width:36px; height:36px; border-radius:50%; border:2px solid #4A192C; margin-left:{ml}; object-fit:cover;' />"
    avatar_html += "<div style='background:#2D1A22; color:white; width:36px; height:36px; border-radius:50%; display:flex; justify-content:center; align-items:center; font-size:11px; margin-left:-8px; border:2px solid #4A192C; font-weight:bold;'>+8</div>"

    st.markdown(f"""
        <div style='background-color: #4A192C; padding: 32px; border-radius: 12px; color: white; font-family: "Inter", sans-serif; position: relative;'>
            <div style='display: flex; justify-content: space-between; align-items: flex-start;'>
                <h3 style='margin: 0; font-size: 24px; font-weight: 600; color: white;'>Pending Review</h3>
                <span class='material-symbols-rounded' style='font-size: 32px; color: white;'>assignment_late</span>
            </div>
            <h1 style='margin: 16px 0 8px 0; font-size: 64px; font-weight: 700; color: white;'>124</h1>
            <p style='margin: 0; font-size: 16px; color: #D3C9CB;'>Priority cases awaiting officer verification.</p>
            <div style='display: flex; margin-top: 24px; align-items: center;'>
                {avatar_html}
            </div>
        </div>
    """, unsafe_allow_html=True)
    

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
        
    st.markdown("""<style>.dash-card { border:1px solid #EFEBEB; border-radius:8px; padding:16px 24px; background-color:#ffffff; transition:box-shadow 0.2s ease, background-color 0.2s ease; margin-bottom:12px; display:block; text-decoration:none !important; color:inherit !important; } .dash-card:hover { background-color:#fafafa; box-shadow:0 4px 12px rgba(0,0,0,0.05); }</style>""", unsafe_allow_html=True)
    df = fetch_dash_cases().head(5) # Limit Dashboard to top 5 cases
    for idx, row in df.iterrows():
        color = "#E53E3E" if row['STATUS'] == "Pending Review" else ("#D69E2E" if "Investigation" in row['STATUS'] else "#38A169")
        card_html = f"""<a href="cases?selected_case={row['ID']}" target="_self" class="dash-card">
<div style="display: flex; align-items: center; justify-content: space-between; font-family: 'Inter', sans-serif;">
<div style="display: flex; flex-direction: column; width: 40%;">
<div style="display: flex; align-items: center; gap: 12px; margin-bottom: 4px;">
<img src="https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/72x72/{row['FLAG_URL']}" style="width: 24px; height: 24px;" alt="Flag" />
<span style="font-weight: 700; font-size: 16px; color: #1a1c1d;">{row['ENTITY_NAME']}</span>
</div>
<span style="font-size: 11px; color: #8C7C83; font-weight: 700; letter-spacing: 0.5px; margin-left: 36px;">{row['TYPE']}</span>
</div>
<div style="width: 25%;">
<div style="font-size: 10px; font-weight: 700; color: #524346; margin-bottom: 6px;">RISK SCORE: {row['RISK_SCORE']:.1f}</div>
<div style="width: 100%; height: 6px; background-color: #f3f3f5; border-radius: 3px; overflow: hidden;">
<div style="width: {row['RISK_SCORE']}%; height: 100%; background-color: #4A192C; border-radius: 3px;"></div>
</div>
</div>
<div style="width: 15%;">
<div style="font-size: 10px; font-weight: 700; color: #8C7C83; margin-bottom: 2px;">AI CONFIDENCE</div>
<div style="font-weight: 700; font-size: 14px; color: #1a1c1d;">{row['AI_CONFIDENCE']}</div>
</div>
<div style="width: 15%; text-align: right;">
<span style="background-color: {color}; color: white; padding: 6px 14px; border-radius: 4px; font-size: 12px; font-weight: 700; display: inline-block;">{row['STATUS']}</span>
</div>
</div>
</a>"""
        st.markdown(card_html, unsafe_allow_html=True)
        
    st.markdown("<div style='text-align:center; margin-top:16px; font-size:14px; font-weight:600;'><a href='/cases' style='color:#2D1A22; text-decoration:none;'>View All Active Cases</a></div>", unsafe_allow_html=True)
