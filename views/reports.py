import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()

STATUS_LABELS = {
    'CRITICAL_MATCH': 'Critical Match',
    'PENDING_HUMAN_REVIEW': 'Review Required',
    'AUTO_DISMISSED': 'Auto-Dismissed',
    'HUMAN_DISMISSED': 'Human-Dismissed',
    'NO_MATCH': 'No Match',
    'DISMISS_OVERRIDDEN': 'Dismiss Overridden',
    'PENDING_AI_ADJUDICATION': 'Pending AI',
}

STATUS_COLORS = {
    'Critical Match': '#E53E3E',
    'Review Required': '#f57c00',
    'Auto-Dismissed': '#0088a3',
    'Human-Dismissed': '#38A169',
    'No Match': '#8C7C83',
    'Dismiss Overridden': '#e65100',
    'Pending AI': '#7B61FF',
}

st.title("Reports")
st.caption("Pipeline analytics, disposition intelligence, and exportable compliance reports.")

st.markdown("<br>", unsafe_allow_html=True)

metrics_df = session.sql("""
    SELECT
        COUNT(*) AS TOTAL,
        COUNT(CASE WHEN DISPOSITION IN ('PENDING_HUMAN_REVIEW','CRITICAL_MATCH') THEN 1 END) AS PENDING,
        COUNT(CASE WHEN DISPOSITION IN ('AUTO_DISMISSED','HUMAN_DISMISSED') THEN 1 END) AS DISMISSED,
        ROUND(AVG(COMPOSITE_SCORE) * 100, 1) AS AVG_SCORE,
        COUNT(CASE WHEN AI_DECISION IS NOT NULL THEN 1 END) AS AI_TOTAL,
        COUNT(CASE WHEN AI_DECISION = 'DISMISS' AND DISPOSITION IN ('AUTO_DISMISSED','HUMAN_DISMISSED') THEN 1
              WHEN AI_DECISION = 'ESCALATE' AND DISPOSITION IN ('PENDING_HUMAN_REVIEW','CRITICAL_MATCH') THEN 1
              END) AS AI_UPHELD
    FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
""").to_pandas()

total = int(metrics_df['TOTAL'].iloc[0] or 0)
pending = int(metrics_df['PENDING'].iloc[0] or 0)
dismissed = int(metrics_df['DISMISSED'].iloc[0] or 0)
avg_score = float(pd.to_numeric(metrics_df['AVG_SCORE'].iloc[0], errors='coerce') or 0)
ai_total = int(metrics_df['AI_TOTAL'].iloc[0] or 0)
ai_upheld = int(metrics_df['AI_UPHELD'].iloc[0] or 0)
ai_accuracy = round((ai_upheld / ai_total * 100), 1) if ai_total > 0 else 0

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.metric("Total Screenings", f"{total:,}")
with m2:
    st.metric("Pending Review", pending)
with m3:
    st.metric("Dismissed", dismissed)
with m4:
    st.metric("AI Accuracy", f"{ai_accuracy}%")
with m5:
    st.metric("Avg Risk Score", f"{avg_score}%")

st.markdown("<br>", unsafe_allow_html=True)

chart_col, donut_col = st.columns([3, 2])

with chart_col:
    with st.container(border=True):
        st.markdown("<h4 style='margin:0 0 16px 0;'>Screening Volume by Disposition</h4>", unsafe_allow_html=True)

        volume_df = session.sql("""
            SELECT
                TO_CHAR(SCREENED_AT, 'YYYY-MM-DD') AS DAY,
                DISPOSITION,
                COUNT(*) AS CNT
            FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
            GROUP BY DAY, DISPOSITION
            ORDER BY DAY
        """).to_pandas()

        if not volume_df.empty:
            volume_df['LABEL'] = volume_df['DISPOSITION'].map(STATUS_LABELS).fillna(volume_df['DISPOSITION'])

            color_domain = list(STATUS_COLORS.keys())
            color_range = list(STATUS_COLORS.values())

            chart = alt.Chart(volume_df).mark_bar(
                cornerRadiusTopLeft=3,
                cornerRadiusTopRight=3
            ).encode(
                x=alt.X('DAY:N', title='Date', axis=alt.Axis(labelAngle=-45)),
                y=alt.Y('CNT:Q', title='Cases', stack='zero'),
                color=alt.Color('LABEL:N', title='Disposition',
                    scale=alt.Scale(domain=color_domain, range=color_range)),
                tooltip=['DAY:N', 'LABEL:N', 'CNT:Q']
            ).properties(
                height=350,
                background='transparent'
            ).configure_view(strokeWidth=0)

            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No screening data available.")

with donut_col:
    with st.container(border=True):
        st.markdown("<h4 style='margin:0 0 16px 0;'>Disposition Breakdown</h4>", unsafe_allow_html=True)

        disp_df = session.sql("""
            SELECT DISPOSITION, COUNT(*) AS CNT
            FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
            GROUP BY DISPOSITION
            ORDER BY CNT DESC
        """).to_pandas()

        if not disp_df.empty:
            disp_df['LABEL'] = disp_df['DISPOSITION'].map(STATUS_LABELS).fillna(disp_df['DISPOSITION'])
            disp_df['PCT'] = (disp_df['CNT'] / disp_df['CNT'].sum() * 100).round(1)

            color_domain = list(STATUS_COLORS.keys())
            color_range = list(STATUS_COLORS.values())

            donut = alt.Chart(disp_df).mark_arc(innerRadius=60, outerRadius=100).encode(
                theta=alt.Theta('CNT:Q'),
                color=alt.Color('LABEL:N', title='Disposition',
                    scale=alt.Scale(domain=color_domain, range=color_range)),
                tooltip=['LABEL:N', 'CNT:Q', 'PCT:Q']
            ).properties(
                height=300,
                background='transparent'
            ).configure_view(strokeWidth=0)

            st.altair_chart(donut, use_container_width=True)

            for _, r in disp_df.iterrows():
                c = STATUS_COLORS.get(r['LABEL'], '#8C7C83')
                st.markdown(
                    "<div style='display:flex; justify-content:space-between; align-items:center; padding:6px 0; border-bottom:1px solid var(--argus-border);'>"
                    "<div style='display:flex; align-items:center; gap:8px;'>"
                    f"<div style='width:10px; height:10px; border-radius:50%; background:{c};'></div>"
                    f"<span style='font-size:13px; color:var(--argus-text-dark);'>{r['LABEL']}</span>"
                    "</div>"
                    f"<span style='font-size:13px; font-weight:700; color:var(--argus-text-dark);'>{r['CNT']} ({r['PCT']}%)</span>"
                    "</div>",
                    unsafe_allow_html=True
                )
        else:
            st.info("No data available.")

st.markdown("<br>", unsafe_allow_html=True)

with st.container(border=True):
    st.markdown("<h4 style='margin:0 0 16px 0;'>Compliance Export</h4>", unsafe_allow_html=True)
    st.caption("Generate a downloadable report of all screening results within a date range.")

    ex_col1, ex_col2, ex_col3 = st.columns([3, 3, 2], vertical_alignment="bottom")
    with ex_col1:
        start_date = st.date_input("From", value=datetime.now().date() - timedelta(days=30))
    with ex_col2:
        end_date = st.date_input("To", value=datetime.now().date())
    with ex_col3:
        generate_btn = st.button("Generate Report", type="primary", use_container_width=True, icon=":material/download:")

    if generate_btn:
        export_df = session.sql(f"""
            SELECT
                r.RESULT_ID AS CASE_ID,
                r.FULL_NAME_SCREENED AS ENTITY_NAME,
                COALESCE(i.COUNTRY, 'N/A') AS COUNTRY,
                COALESCE(i.DATE_OF_BIRTH::VARCHAR, 'N/A') AS DOB,
                r.DISPOSITION,
                ROUND(r.COMPOSITE_SCORE * 100, 1) AS RISK_SCORE_PCT,
                ROUND(r.NAME_SIMILARITY_SCORE * 100, 1) AS NAME_SIM_PCT,
                r.MATCHED_ENTITY_NAME,
                r.MATCHED_LIST_ABBREVIATION AS MATCHED_LIST,
                r.AI_DECISION,
                r.SCREENED_AT,
                COALESCE(i.SOURCE_SYSTEM, 'N/A') AS SOURCE_SYSTEM,
                COALESCE(i.CARD_REQUESTED, 'N/A') AS CARD_REQUESTED
            FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS r
            LEFT JOIN AML_SCREENING.PIPELINE.INCOMING_SCREENINGS i
                ON r.SCREENING_REQUEST_ID = i.SCREENING_REQUEST_ID
            WHERE r.SCREENED_AT >= '{start_date.strftime('%Y-%m-%d')}'
              AND r.SCREENED_AT < '{(end_date + timedelta(days=1)).strftime('%Y-%m-%d')}'
            ORDER BY r.SCREENED_AT DESC
        """).to_pandas()

        if export_df.empty:
            st.warning("No results found for the selected date range.")
        else:
            st.markdown(f"**{len(export_df)}** record(s) found.")
            st.dataframe(export_df, use_container_width=True, hide_index=True, height=300)

            csv_data = export_df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                data=csv_data,
                file_name=f"argus_report_{start_date}_{end_date}.csv",
                mime="text/csv",
                icon=":material/download:",
            )
