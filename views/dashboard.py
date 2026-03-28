import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

COUNTRY_FLAGS = {
    'AF':'1f1e6-1f1eb','AL':'1f1e6-1f1f1','DZ':'1f1e9-1f1ff','AR':'1f1e6-1f1f7',
    'AU':'1f1e6-1f1fa','AT':'1f1e6-1f1f9','BD':'1f1e7-1f1e9','BR':'1f1e7-1f1f7',
    'CA':'1f1e8-1f1e6','CN':'1f1e8-1f1f3','CO':'1f1e8-1f1f4','CU':'1f1e8-1f1fa',
    'EG':'1f1ea-1f1ec','FR':'1f1eb-1f1f7','DE':'1f1e9-1f1ea','GB':'1f1ec-1f1e7',
    'HK':'1f1ed-1f1f0','IN':'1f1ee-1f1f3','IR':'1f1ee-1f1f7','IQ':'1f1ee-1f1f6',
    'JP':'1f1ef-1f1f5','KP':'1f1f0-1f1f5','KR':'1f1f0-1f1f7','LB':'1f1f1-1f1e7',
    'MY':'1f1f2-1f1fe','MX':'1f1f2-1f1fd','NG':'1f1f3-1f1ec','PK':'1f1f5-1f1f0',
    'RU':'1f1f7-1f1fa','SA':'1f1f8-1f1e6','ZA':'1f1ff-1f1e6','SE':'1f1f8-1f1ea',
    'SY':'1f1f8-1f1fe','TR':'1f1f9-1f1f7','AE':'1f1e6-1f1ea','US':'1f1fa-1f1f8',
    'VE':'1f1fb-1f1ea',
}

def fetch_dash_cases():
    df = session.sql("""
        SELECT
            r.RESULT_ID AS ID,
            r.FULL_NAME_SCREENED AS ENTITY_NAME,
            CASE WHEN i.GENDER IS NOT NULL THEN 'INDIVIDUAL' ELSE 'ENTITY' END AS TYPE,
            COALESCE(i.COUNTRY, 'N/A') AS COUNTRY,
            r.DISPOSITION AS STATUS,
            ROUND(r.COMPOSITE_SCORE * 100, 1) AS RISK_SCORE,
            ROUND(r.NAME_SIMILARITY_SCORE * 100, 0) || '%' AS AI_CONFIDENCE,
            r.MATCHED_ENTITY_NAME,
            r.MATCHED_LIST_ABBREVIATION,
            r.SCREENED_AT
        FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS r
        LEFT JOIN AML_SCREENING.PIPELINE.INCOMING_SCREENINGS i
            ON r.SCREENING_REQUEST_ID = i.SCREENING_REQUEST_ID
        ORDER BY r.COMPOSITE_SCORE DESC
    """).to_pandas()
    df['FLAG_URL'] = df['COUNTRY'].map(COUNTRY_FLAGS).fillna('1f3f3-fe0f') + '.png'
    return df

@st.cache_data(ttl=300)
def get_chart_data():
    return get_active_session().sql("""
        SELECT
            TO_CHAR(SCREENED_AT, 'YYYY-MM-DD') AS DAY,
            COUNT(*) AS NOISE_REMOVED
        FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
        WHERE DISPOSITION = 'AUTO_DISMISSED'
        GROUP BY DAY
        ORDER BY DAY
    """).to_pandas()

st.title("Surveillance Overview")
st.caption("Real-time risk orchestration and case intelligence for AML screening pipeline.")

st.markdown("<br>", unsafe_allow_html=True)

col_charts, col_metrics = st.columns([1.5, 1])

with col_charts:
    with st.container(border=True):
        st.markdown("""
            <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 8px;'>
                <h4 style='margin:0;'>AI Noise Reduction</h4>
                <div style='background:#F8F5F5; color:#4A192C; padding:6px 14px; border-radius:100px; font-weight:700; font-size:13px; border: 1px solid #EFEBEB;'>+14.2%</div>
            </div>
        """, unsafe_allow_html=True)
        st.caption("AUTO-DISMISSED SCREENINGS PER DAY")
        
        df_chart = get_chart_data()
        st.bar_chart(df_chart, x="DAY", y="NOISE_REMOVED", color="#9B8B91", height=335)

with col_metrics:
    pending_count = session.sql("SELECT COUNT(*) AS C FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS WHERE DISPOSITION IN ('PENDING_HUMAN_REVIEW','CRITICAL_MATCH')").to_pandas()['C'].iloc[0]
    employees_df = session.sql("SELECT * FROM AML_SCREENING.ARGUS.EMPLOYEES LIMIT 3").to_pandas()
    
    avatar_html = ""
    for idx, e_row in employees_df.iterrows():
        ml = "-12px" if idx > 0 else "0px"
        avatar_html += f"<img src='{e_row['AVATAR_URL']}' style='width:36px; height:36px; border-radius:50%; border:2px solid #4A192C; margin-left:{ml}; object-fit:cover;' />"
    avatar_html += "<div style='background:#2D1A22; color:white; width:36px; height:36px; border-radius:50%; display:flex; justify-content:center; align-items:center; font-size:11px; margin-left:-12px; border:2px solid #4A192C; font-weight:bold;'>+8</div>"

    st.markdown(f"""
        <div style='background-color: #4A192C; padding: 24px 36px; border-radius: 12px; color: white; font-family: "Inter", sans-serif; height: 440px; display: flex; flex-direction: column; justify-content: space-between;'>
            <div>
                <div style='display: flex; justify-content: space-between; align-items: flex-start;'>
                    <h3 style='margin: 0; font-size: 20px; font-weight: 600; color: white; opacity: 0.9;'>Pending Review</h3>
                    <span class='material-symbols-rounded' style='font-size: 28px; color: white;'>assignment_late</span>
                </div>
                <h1 style='margin: 24px 0 8px 0; font-size: 64px; font-weight: 700; color: white;'>{pending_count}</h1>
                <p style='margin: 0; font-size: 14px; color: #D3C9CB; line-height: 1.4;'>Priority cases awaiting officer verification and manual risk adjudication.</p>
            </div>
            <div style='display: flex; align-items: center;'>
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
    df = fetch_dash_cases().head(5)
    for idx, row in df.iterrows():
        color = "#E53E3E" if row['STATUS'] in ('CRITICAL_MATCH','PENDING_HUMAN_REVIEW') else ("#D69E2E" if row['STATUS'] == 'NO_MATCH' else "#38A169")
        card_html = f"""<a href="cases?selected_case={row['ID']}" target="_self" class="dash-card">
<div style="display: flex; align-items: center; justify-content: space-between; font-family: 'Inter', sans-serif;">
<div style="display: flex; flex-direction: column; width: 40%;">
<div style="display: flex; align-items: center; gap: 12px; margin-bottom: 4px;">
<img src="https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/72x72/{row['FLAG_URL']}" style="width: 24px; height: 24px;" alt="Flag" />
<span style="font-weight: 700; font-size: 16px; color: var(--argus-text-dark);">{row['ENTITY_NAME']}</span>
</div>
<span style="font-size: 11px; color: var(--argus-text-muted); font-weight: 700; letter-spacing: 0.5px; margin-left: 36px;">{row['TYPE']}</span>
</div>
<div style="width: 25%;">
<div style="font-size: 10px; font-weight: 700; color: var(--argus-text-muted); opacity: 0.8; margin-bottom: 6px;">RISK SCORE: {row['RISK_SCORE']:.1f}</div>
<div style="width: 100%; height: 6px; background-color: var(--argus-accent-light); border-radius: 3px; overflow: hidden;">
<div style="width: {row['RISK_SCORE']}%; height: 100%; background-color: var(--argus-primary); border-radius: 3px;"></div>
</div>
</div>
<div style="width: 15%;">
<div style="font-size: 10px; font-weight: 700; color: var(--argus-text-muted); margin-bottom: 2px;">AI CONFIDENCE</div>
<div style="font-weight: 700; font-size: 14px; color: var(--argus-text-dark);">{row['AI_CONFIDENCE']}</div>
</div>
<div style="width: 15%; text-align: right;">
<span style="background-color: {color}; color: white; padding: 6px 14px; border-radius: 4px; font-size: 12px; font-weight: 700; display: inline-block;">{row['STATUS']}</span>
</div>
</div>
</a>"""
        st.markdown(card_html, unsafe_allow_html=True)
        
    st.markdown("<div style='text-align:center; margin-top:16px; font-size:14px; font-weight:600;'><a href='/cases' style='color:#2D1A22; text-decoration:none;'>View All Active Cases</a></div>", unsafe_allow_html=True)
