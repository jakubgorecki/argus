import streamlit as st
import altair as alt
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def _country_flag_code(iso2):
    if not iso2 or len(iso2) != 2 or not iso2.isalpha() or iso2.upper() == 'NA':
        return ''
    a, b = iso2.upper()
    return f"{0x1F1E6 + ord(a) - ord('A'):x}-{0x1F1E6 + ord(b) - ord('A'):x}"

def fetch_dash_cases():
    df = session.sql("""
        SELECT
            r.RESULT_ID AS ID,
            r.FULL_NAME_SCREENED AS ENTITY_NAME,
            CASE WHEN i.GENDER IS NOT NULL THEN 'INDIVIDUAL' ELSE 'ENTITY' END AS TYPE,
            COALESCE(i.COUNTRY, 'N/A') AS COUNTRY,
            r.DISPOSITION AS STATUS,
            ROUND(r.COMPOSITE_SCORE * 100, 1) AS RISK_SCORE,
            ROUND(r.NAME_SIMILARITY_SCORE * 100, 0) || '%' AS NAME_SIMILARITY,
            r.MATCHED_ENTITY_NAME,
            r.MATCHED_LIST_ABBREVIATION,
            r.SCREENED_AT
        FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS r
        LEFT JOIN AML_SCREENING.PIPELINE.INCOMING_SCREENINGS i
            ON r.SCREENING_REQUEST_ID = i.SCREENING_REQUEST_ID
        ORDER BY r.COMPOSITE_SCORE DESC
    """).to_pandas()
    df['FLAG_URL'] = df['COUNTRY'].apply(_country_flag_code) + '.png'
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
        st.caption("**Auto-Dismissed Screenings Per Day**")
        
        df_chart = get_chart_data()
        chart = alt.Chart(df_chart).mark_bar(
            color='#9B8B91',
            cornerRadiusTopLeft=3,
            cornerRadiusTopRight=3
        ).encode(
            x=alt.X('DAY:N', title='Date', axis=alt.Axis(labelAngle=-45, titleFontWeight='bold')),
            y=alt.Y('NOISE_REMOVED:Q', title='Number of cases', axis=alt.Axis(titleFontWeight='bold'))
        ).properties(
            height=310,
            background='transparent'
        ).configure_view(
            strokeWidth=0
        )
        st.altair_chart(chart, use_container_width=True)

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
