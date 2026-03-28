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

STATUS_LABELS = {
    'CRITICAL_MATCH': 'Critical Match',
    'PENDING_HUMAN_REVIEW': 'Review Required',
    'AUTO_DISMISSED': 'Auto-Dismissed',
    'NO_MATCH': 'No Match',
    'DISMISS_OVERRIDDEN': 'Dismiss Overridden',
}

STATUS_BG = {
    'CRITICAL_MATCH': '#ffdad6',
    'PENDING_HUMAN_REVIEW': '#fff3e0',
    'AUTO_DISMISSED': '#b3ebff',
    'NO_MATCH': '#e8e8e8',
    'DISMISS_OVERRIDDEN': '#fff3e0',
}

STATUS_FG = {
    'CRITICAL_MATCH': '#93000a',
    'PENDING_HUMAN_REVIEW': '#e65100',
    'AUTO_DISMISSED': '#004e5f',
    'NO_MATCH': '#4c4547',
    'DISMISS_OVERRIDDEN': '#e65100',
}

CASE_QUERY = """
    SELECT
        r.RESULT_ID AS ID,
        r.FULL_NAME_SCREENED AS ENTITY_NAME,
        CASE WHEN i.GENDER IS NOT NULL THEN 'INDIVIDUAL' ELSE 'ENTITY' END AS TYPE,
        COALESCE(i.COUNTRY, 'N/A') AS COUNTRY,
        r.DISPOSITION AS STATUS,
        ROUND(r.COMPOSITE_SCORE * 100, 1) AS RISK_SCORE,
        ROUND(r.NAME_SIMILARITY_SCORE * 100, 0) || '%' AS NAME_SIMILARITY,
        r.MATCHED_ENTITY_NAME,
        r.MATCHED_ENTITY_ALIASES,
        r.MATCHED_LIST_NAME,
        r.MATCHED_LIST_ABBREVIATION,
        r.MATCHED_COUNTRY,
        r.MATCHED_DOB,
        r.MATCHED_POB,
        r.AI_DECISION,
        r.AI_REASONING,
        r.AI_ERROR,
        COALESCE(i.DATE_OF_BIRTH::VARCHAR, 'N/A') AS DOB,
        COALESCE(i.PLACE_OF_BIRTH, 'N/A') AS POB,
        r.DOB_SCORE,
        r.DOB_MATCH_TYPE,
        r.COUNTRY_SCORE,
        r.POB_SCORE,
        r.POB_MATCH_TYPE,
        r.NAME_SIMILARITY_SCORE,
        r.COMPOSITE_SCORE,
        r.LOGICAL_EXCLUSION,
        r.EXCLUSION_REASON,
        r.CANDIDATE_COUNT,
        r.SCREENED_AT,
        COALESCE(i.GENDER, 'N/A') AS GENDER,
        COALESCE(i.SOURCE_SYSTEM, 'N/A') AS SOURCE_SYSTEM
    FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS r
    LEFT JOIN AML_SCREENING.PIPELINE.INCOMING_SCREENINGS i
        ON r.SCREENING_REQUEST_ID = i.SCREENING_REQUEST_ID
"""

selected_case = st.query_params.get("selected_case", None)
if selected_case is None:
    st.session_state.pop("selected_case", None)

if selected_case is not None:
    case_id = selected_case
    st.session_state['selected_case'] = case_id

    case_data = session.sql(CASE_QUERY + f" WHERE r.RESULT_ID = '{case_id}'").to_pandas()

    if case_data.empty:
        st.error("Case data not found.")
        st.stop()

    row = case_data.iloc[0]

    st.markdown("<div style='margin-top: -32px;'></div>", unsafe_allow_html=True)

    flag_color = STATUS_BG.get(row["STATUS"], "#ffdad6")
    text_color = STATUS_FG.get(row["STATUS"], "#93000a")
    flag_label = STATUS_LABELS.get(row["STATUS"], row["STATUS"])

    st.markdown(f"""
        <div style='display:flex; align-items:center; gap: 12px; margin-bottom: 8px;'>
            <span style='background-color: {flag_color}; color: {text_color}; padding: 4px 12px; border-radius: 16px; font-size: 11px; font-weight: bold; display:inline-flex; align-items:center; gap:4px;'>
                <span class='material-symbols-rounded' style='font-size:14px;'>flag</span> {flag_label}
            </span>
            <span style='color: #8C7C83; font-size: 13px; white-space: nowrap;'>Screened {row['SCREENED_AT']}</span>
        </div>
        <h1 style='margin:0; padding:0; color: #2c0210; font-size: 48px; line-height: 1.1;'>{row['ENTITY_NAME']}</h1>
    """, unsafe_allow_html=True)

    det_col1, det_col2 = st.columns([7, 3], vertical_alignment="center")

    with det_col1:
        st.markdown(f"""
            <div style='display:flex; align-items:center; gap: 16px; margin-top: 12px; color: #524346; font-size: 14px;'>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>fingerprint</span> <b>ID:</b> {row['ID'][:18]}...</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>location_on</span> <b>Country:</b> {row['COUNTRY']}</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>calendar_today</span> <b>DOB:</b> {row['DOB']}</div>
            </div>
        """, unsafe_allow_html=True)

    with det_col2:
        btn_col1, btn_col2 = st.columns([1, 1])
        with btn_col1:
            if st.button("Share Case", icon=":material/share:", use_container_width=True):
                st.toast("Case link copied to clipboard!")
        with btn_col2:
            if st.button("Export PDF", icon=":material/print:", use_container_width=True):
                import streamlit.components.v1 as components
                components.html("<script>window.parent.print()</script>", height=0)

    st.markdown(f"""
        <div style='display:flex; align-items:center; gap: 0px; margin-top: 16px; margin-bottom: 24px; padding-top: 16px; border-top: 1px solid #EFEBEB;'>
            <div style='display:flex; align-items:center; gap:8px; padding-right: 20px; border-right: 1px solid #EFEBEB;'>
                <span class='material-symbols-rounded' style='font-size:20px; color:#524346;'>business</span>
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>SOURCE:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row['SOURCE_SYSTEM']}</span>
            </div>
            <div style='display:flex; align-items:center; gap:8px; padding-left: 20px;'>
                <span class='material-symbols-rounded' style='font-size:20px; color:#524346;'>payment</span>
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>MATCHED LIST:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row.get('MATCHED_LIST_NAME', 'N/A') or 'N/A'}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    col_left, col_right = st.columns([8, 3])

    with col_left:
        with st.container(border=True):
            st.markdown("<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Match Comparison</h4><span style='font-size:10px; font-weight:600; color:#524346; letter-spacing: 0.5px; text-transform: uppercase;'>Source: {}</span></div>".format(row.get('MATCHED_LIST_ABBREVIATION', 'N/A') or 'N/A'), unsafe_allow_html=True)

            comparison_data = [
                ("Full Name", row['ENTITY_NAME'], row.get('MATCHED_ENTITY_NAME', 'N/A') or 'N/A',
                 "MATCH" if row.get('NAME_SIMILARITY_SCORE', 0) and row['NAME_SIMILARITY_SCORE'] >= 0.85 else ("PARTIAL" if row.get('NAME_SIMILARITY_SCORE', 0) and row['NAME_SIMILARITY_SCORE'] >= 0.5 else "MISMATCH")),
                ("Date of Birth", row['DOB'], row.get('MATCHED_DOB', 'N/A') or 'N/A',
                 "MATCH" if row.get('DOB_SCORE', 0) and row['DOB_SCORE'] >= 0.85 else ("PARTIAL" if row.get('DOB_MATCH_TYPE') else "MISSING")),
                ("Country", row['COUNTRY'], row.get('MATCHED_COUNTRY', 'N/A') or 'N/A',
                 "MATCH" if row.get('COUNTRY_SCORE', 0) and row['COUNTRY_SCORE'] >= 0.85 else "MISMATCH"),
                ("Place of Birth", row['POB'], row.get('MATCHED_POB', 'N/A') or 'N/A',
                 "MATCH" if row.get('POB_SCORE', 0) and row['POB_SCORE'] >= 0.85 else ("PARTIAL" if row.get('POB_MATCH_TYPE') else "MISSING")),
            ]

            if row.get('MATCHED_ENTITY_ALIASES') and pd.notna(row['MATCHED_ENTITY_ALIASES']):
                comparison_data.append(("Aliases", "—", row['MATCHED_ENTITY_ALIASES'], "INFO"))

            table_html = """<div style="overflow-x: auto; width: 100%; margin-bottom: 24px;">
<table style="width: 100%; text-align: left; border-collapse: collapse; font-family: 'Inter', sans-serif; min-width: 600px; border: none;">
<thead style="border-bottom: 2px solid #EFEBEB;">
<tr>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Attribute</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Screened Data</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Sanctions Match</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Status</th>
</tr>
</thead>
<tbody style="background-color: var(--argus-card-bg);">"""
            for idx_h, (attr, subj, wl, stat) in enumerate(comparison_data):
                bg_col = "#b3ebff" if stat == "MATCH" else ("#cfc4c6" if stat == "MISMATCH" else ("#e8dddf" if stat in ("PARTIAL","INFO") else "#f5f5f5"))
                txt_col = "#001f27" if stat == "MATCH" else "#4c4547"
                bb_style = f'border-bottom: 1px solid var(--argus-border);' if idx_h < len(comparison_data) - 1 else ''
                table_html += f"""
<tr style="{bb_style}">
<td style="padding: 16px 24px; font-weight: 600; font-size: 14px; color: var(--argus-text-dark);">{attr}</td>
<td style="padding: 16px 24px; font-size: 14px; color: var(--argus-text-muted);">{subj}</td>
<td style="padding: 16px 24px; font-weight: 700; font-size: 14px; color: var(--argus-text-dark);">{wl}</td>
<td style="padding: 16px 24px;"><span style="background-color: {bg_col}; color: {txt_col}; padding: 4px 8px; border-radius: 4px; font-size: 10px; font-weight: 700;">{stat}</span></td>
</tr>"""
            table_html += """</tbody>
</table>
</div>"""
            st.markdown(table_html, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Score Breakdown</h4></div>", unsafe_allow_html=True)

            scores = [
                ("Name Similarity", row.get('NAME_SIMILARITY_SCORE', 0) or 0),
                ("DOB", row.get('DOB_SCORE', 0) or 0),
                ("Country", row.get('COUNTRY_SCORE', 0) or 0),
                ("Place of Birth", row.get('POB_SCORE', 0) or 0),
                ("Composite", row.get('COMPOSITE_SCORE', 0) or 0),
            ]
            score_html = "<div style='display:flex; flex-direction:column; gap:12px;'>"
            for s_name, s_val in scores:
                pct = round(s_val * 100, 1)
                bar_color = "#E53E3E" if pct >= 85 else ("#f57c00" if pct >= 50 else "#38A169")
                score_html += f"""
                <div>
                    <div style='display:flex; justify-content:space-between; margin-bottom:4px;'>
                        <span style='font-size:13px; font-weight:600; color:var(--argus-text-dark);'>{s_name}</span>
                        <span style='font-size:13px; font-weight:700; color:var(--argus-text-dark);'>{pct}%</span>
                    </div>
                    <div style='width:100%; height:8px; background:var(--argus-accent-light); border-radius:4px; overflow:hidden;'>
                        <div style='width:{pct}%; height:100%; background:{bar_color}; border-radius:4px;'></div>
                    </div>
                </div>"""
            score_html += "</div>"
            st.markdown(score_html, unsafe_allow_html=True)

            if row.get('LOGICAL_EXCLUSION'):
                st.warning(f"Logical Exclusion: {row.get('EXCLUSION_REASON', 'N/A')}")

        with st.container(border=True):
            st.markdown("<h4 style='margin:0 0 16px 0;'>Evidence & Files</h4>", unsafe_allow_html=True)
            st.markdown("<div style='padding:24px; background-color:#f3f3f5; border-radius:12px; font-size:14px; color:#524346;'>No evidence files attached to this screening result. Upload files below to attach evidence.</div>", unsafe_allow_html=True)
            st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)
            st.file_uploader("Drop additional evidence", accept_multiple_files=True, label_visibility="collapsed")

        with st.form(key=f"review_form_{case_id}", clear_on_submit=True, border=True):
            st.markdown("<h4 style='margin-bottom:16px;'>Record Review Decision</h4>", unsafe_allow_html=True)
            new_note = st.text_area("Rationale", label_visibility="collapsed", placeholder="Provide rationale for your decision...")

            st.markdown("<hr style='margin: 16px 0; border: none; border-top: 1px solid #EFEBEB;'>", unsafe_allow_html=True)

            rc1, rc2, rc3 = st.columns([5, 4, 3], vertical_alignment="center")
            with rc1:
                st.markdown("""
                <div style='display:flex; align-items:center; gap:8px;'>
                    <span class='material-symbols-rounded' style='color:#2c0210; font-size:20px;'>verified_user</span>
                    <span style='font-size:14px; font-weight:600; color:#1a1c1d;'>Your Review Recommendation</span>
                </div>
                """, unsafe_allow_html=True)
            with rc2:
                decision = st.selectbox("Decision", ["Cleared", "Escalate", "Reject & Block"], label_visibility="collapsed")
            with rc3:
                submit_review = st.form_submit_button("Submit Review", type="primary", use_container_width=True)

            if submit_review:
                disposition_map = {"Cleared": "AUTO_DISMISSED", "Escalate": "PENDING_HUMAN_REVIEW", "Reject & Block": "CRITICAL_MATCH"}
                new_disp = disposition_map[decision]
                rationale = (new_note.strip() if new_note.strip() else "No additional rationale provided.").replace("'", "''")

                session.sql(f"""
                    UPDATE AML_SCREENING.PIPELINE.SCREENING_RESULTS
                    SET DISPOSITION = '{new_disp}',
                        AI_REASONING = COALESCE(AI_REASONING, '') || ' | HUMAN REVIEW: {decision} - {rationale}'
                    WHERE RESULT_ID = '{case_id}'
                """).collect()

                session.sql(f"""
                    INSERT INTO AML_SCREENING.PIPELINE.AUDIT_LOG (EVENT_TYPE, DETAILS)
                    SELECT 'HUMAN_REVIEW', OBJECT_CONSTRUCT(
                        'result_id', '{case_id}',
                        'decision', '{decision}',
                        'new_disposition', '{new_disp}',
                        'rationale', '{rationale}',
                        'completed_at', CURRENT_TIMESTAMP()::VARCHAR
                    )
                """).collect()

                st.rerun()

    with col_right:
        with st.container(border=True):
            ai_title = "AI Assessment"
            if row.get('AI_DECISION'):
                ai_title = f"AI Decision: {row['AI_DECISION']}"
            ai_desc = row.get('AI_REASONING', '') or 'No AI analysis has been performed on this screening result yet.'
            if row.get('AI_ERROR') and pd.notna(row['AI_ERROR']):
                ai_desc += f"\n\n⚠️ Error: {row['AI_ERROR']}"

            st.markdown(f"""
                <div style='display:flex; flex-direction:column; gap:16px;'>
                    <div style='background-color:#2c0210; color:white; padding:12px; border-radius:12px; width: fit-content;'>
                        <span class='material-symbols-rounded'>auto_awesome</span>
                    </div>
                    <div>
                        <h4 style='margin:0; color:#2c0210; font-size: 18px;'>{ai_title}</h4>
                        <p style='margin-top:12px; font-size:14px; color:#1a1c1d; line-height: 1.5;'>{ai_desc}</p>
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with st.container(border=True):
            st.markdown("<h4 style='margin:0 0 16px 0;'>Screening Metadata</h4>", unsafe_allow_html=True)
            meta_items = [
                ("Gender", row.get('GENDER', 'N/A')),
                ("Candidates Found", str(row.get('CANDIDATE_COUNT', 0))),
                ("DOB Match Type", row.get('DOB_MATCH_TYPE', 'N/A') or 'N/A'),
                ("POB Match Type", row.get('POB_MATCH_TYPE', 'N/A') or 'N/A'),
            ]
            for m_label, m_val in meta_items:
                st.markdown(f"""
                    <div style='display:flex; justify-content:space-between; padding:8px 0; border-bottom:1px solid var(--argus-border);'>
                        <span style='font-size:12px; font-weight:600; color:var(--argus-text-muted);'>{m_label}</span>
                        <span style='font-size:12px; font-weight:700; color:var(--argus-text-dark);'>{m_val}</span>
                    </div>
                """, unsafe_allow_html=True)


else:
    cases_df = session.sql(CASE_QUERY + """
        WHERE r.DISPOSITION != 'AUTO_DISMISSED'
           OR r.SCREENED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY r.COMPOSITE_SCORE DESC
    """).to_pandas()
    cases_df['FLAG_URL'] = cases_df['COUNTRY'].map(COUNTRY_FLAGS).fillna('1f3f3-fe0f') + '.png'

    st.title("Case Management")

    total = len(cases_df)
    pending = len(cases_df[cases_df['STATUS'].isin(['PENDING_HUMAN_REVIEW', 'CRITICAL_MATCH'])])
    dismissed = len(cases_df[cases_df['STATUS'] == 'AUTO_DISMISSED'])
    no_match = len(cases_df[cases_df['STATUS'] == 'NO_MATCH'])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Screened", total)
    with col2:
        st.metric("Pending Review", pending, delta_color="inverse")
    with col3:
        st.metric("Auto-Dismissed", dismissed, delta_color="normal")
    with col4:
        st.metric("No Match", no_match)

    st.markdown("<br>", unsafe_allow_html=True)

    f_col1, f_col2, f_col3 = st.columns([2, 2, 2])
    with f_col1:
        status_options = ["All Statuses"] + sorted(cases_df['STATUS'].unique().tolist())
        status_filter = st.selectbox("Status", status_options, index=0, label_visibility="collapsed")
    with f_col2:
        risk_filter = st.selectbox("Risk", ["Risk: All Levels", "High", "Medium", "Low"], label_visibility="collapsed")
    with f_col3:
        entity_filter = st.selectbox("Entity", ["Entity: All", "INDIVIDUAL", "ENTITY"], label_visibility="collapsed")

    filtered_df = cases_df.copy()
    filtered_df = filtered_df.sort_values(by="RISK_SCORE", ascending=False)

    if status_filter != "All Statuses":
        filtered_df = filtered_df[filtered_df['STATUS'] == status_filter]

    if risk_filter != "Risk: All Levels":
        if risk_filter == "High": filtered_df = filtered_df[filtered_df['RISK_SCORE'] >= 70]
        elif risk_filter == "Medium": filtered_df = filtered_df[(filtered_df['RISK_SCORE'] >= 30) & (filtered_df['RISK_SCORE'] < 70)]
        elif risk_filter == "Low": filtered_df = filtered_df[filtered_df['RISK_SCORE'] < 30]

    if entity_filter != "Entity: All":
        filtered_df = filtered_df[filtered_df['TYPE'] == entity_filter]

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
<style>
.case-btn button {
    background: #fff !important;
    border: 1px solid #EFEBEB !important;
    border-radius: 8px !important;
    color: var(--argus-text-dark) !important;
    font-family: 'Courier New', monospace !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    text-align: left !important;
    padding: 14px 20px !important;
    white-space: pre !important;
    transition: all 0.2s ease !important;
    margin-bottom: 2px !important;
}
.case-btn button:hover {
    background: #fafafa !important;
    border-left: 4px solid #4A192C !important;
}
</style>
""", unsafe_allow_html=True)

    header = f"{'Entity':<30} {'Risk':>6}  {'Similarity':>10}  {'Status':<18}"
    st.markdown(f"<div style='padding:8px 20px; border-bottom:2px solid #EFEBEB; margin-bottom:4px; font-family:Courier New,monospace; font-size:13px; font-weight:700; color:#8C7C83; white-space:pre;'>{header}</div>", unsafe_allow_html=True)

    if filtered_df.empty:
        st.info("No cases match the selected filters.")
    else:
        for idx, row in filtered_df.iterrows():
            label = STATUS_LABELS.get(row["STATUS"], row["STATUS"])
            name = row['ENTITY_NAME'][:28].ljust(28)
            risk = f"{row['RISK_SCORE']:>6.1f}"
            sim = f"{row['NAME_SIMILARITY']:>10}"
            status = label[:18].ljust(18)
            btn_label = f"{name}  {risk}  {sim}  {status}"

            st.markdown("<div class='case-btn'>", unsafe_allow_html=True)
            if st.button(btn_label, key=f"c_{row['ID']}", use_container_width=True):
                st.query_params["selected_case"] = row["ID"]
                st.rerun()
            st.markdown("</div>", unsafe_allow_html=True)
