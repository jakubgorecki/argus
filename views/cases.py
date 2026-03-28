import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def load_cases_data():
    cases = session.sql("SELECT * FROM AML_SCREENING.ARGUS.CASES").to_pandas()
    metrics = session.sql("SELECT * FROM AML_SCREENING.ARGUS.CASE_METRICS").to_pandas()
    return cases, metrics

cases_df, metrics_df = load_cases_data()

if 'selected_case' in st.session_state and st.session_state['selected_case'] is not None:
    case_id = st.session_state['selected_case']
        
    case_data = session.sql(f"SELECT * FROM AML_SCREENING.ARGUS.CASES WHERE ID = '{case_id}'").to_pandas()

    if case_data.empty:
        st.error("Case data not found.")
        st.stop()
        
    row = case_data.iloc[0]

    st.markdown("<div style='margin-top: -32px;'></div>", unsafe_allow_html=True)
    
    flag_color = "#ffdad6" if row["STATUS"] != "AUTO-CLEARED" else "#b3ebff"
    text_color = "#93000a" if row["STATUS"] != "AUTO-CLEARED" else "#004e5f"
    flag_label = "HIGH RISK HIT" if row["RISK_SCORE"] > 70 else ("REVIEW TRIGGERED" if row["RISK_SCORE"] > 40 else "CLEARED")
    
    st.markdown(f"""
        <div style='display:flex; align-items:center; gap: 12px; margin-bottom: 8px;'>
            <span style='background-color: {flag_color}; color: {text_color}; padding: 4px 12px; border-radius: 16px; font-size: 11px; font-weight: bold; display:inline-flex; align-items:center; gap:4px;'>
                <span class='material-symbols-rounded' style='font-size:14px;'>flag</span> {flag_label}
            </span>
            <span style='color: #8C7C83; font-size: 13px; white-space: nowrap;'>Updated {row['LAST_ACTIVITY']}</span>
        </div>
        <h1 style='margin:0; padding:0; color: #2c0210; font-size: 48px; line-height: 1.1;'>{row['ENTITY_NAME']}</h1>
    """, unsafe_allow_html=True)

    det_col1, det_col2 = st.columns([7, 3], vertical_alignment="center")
    
    with det_col1:
        st.markdown(f"""
            <div style='display:flex; align-items:center; gap: 16px; margin-top: 12px; color: #524346; font-size: 14px;'>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>fingerprint</span> <b>ID:</b> {row['ID']}</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>location_on</span> <b>Location:</b> {row['COUNTRY']}</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>calendar_today</span> <b>DOB:</b> {row['DOB']}</div>
            </div>
        """, unsafe_allow_html=True)

    with det_col2:
        st.markdown("""
        <style>
        div[data-testid="stHorizontalBlock"] .stButton > button {
            background-color: white !important;
            border: 1px solid #EFEBEB !important;
            border-radius: 8px !important;
            color: #2c0210 !important;
            font-size: 13px !important;
            font-weight: 600 !important;
            padding: 8px 16px !important;
            height: auto !important;
            width: auto !important;
            transition: all 0.2s ease !important;
            display: flex !important;
            align-items: center !important;
            gap: 8px !important;
        }
        div[data-testid="stHorizontalBlock"] .stButton > button:hover {
            background-color: #fafafa !important;
            border-color: #d1d1d1 !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
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
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>CLIENT:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row['CLIENT']}</span>
            </div>
            <div style='display:flex; align-items:center; gap:8px; padding-left: 20px;'>
                <span class='material-symbols-rounded' style='font-size:20px; color:#524346;'>payment</span> 
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>PRODUCT:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row['PRODUCT']}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    col_left, col_right = st.columns([8, 3])

    with col_left:
        with st.container(border=True):
            wl_hits_df = session.sql(f"SELECT * FROM AML_SCREENING.ARGUS.WATCHLIST_HITS WHERE CASE_ID = '{case_id}'").to_pandas()

            st.markdown("<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Watchlist Hit Details</h4><span style='font-size:10px; font-weight:600; color:#524346; letter-spacing: 0.5px; text-transform: uppercase;'>Source: World-Check Global</span></div>", unsafe_allow_html=True)
            
            if not wl_hits_df.empty:
                table_html = """<div style="overflow-x: auto; width: 100%; margin-bottom: 24px;">
<table style="width: 100%; text-align: left; border-collapse: collapse; font-family: 'Inter', sans-serif; min-width: 600px; border: none;">
<thead style="border-bottom: 2px solid #EFEBEB;">
<tr>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Attribute</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Subject Data</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Watchlist Hit</th>
<th style="padding: 12px 24px; font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #524346; font-weight: 700;">Status</th>
</tr>
</thead>
<tbody style="background-color: var(--argus-card-bg);">"""
                for idx, h_row in wl_hits_df.iterrows():
                    match_stat = h_row['MATCH_STATUS']
                    bg_col = "#b3ebff" if match_stat == "MATCH" else ("#cfc4c6" if match_stat == "MISMATCH" else "#e8dddf")
                    txt_col = "#001f27" if match_stat == "MATCH" else "#4c4547"
                    bb_style = f'border-bottom: 1px solid var(--argus-border);' if idx < len(wl_hits_df) - 1 else ''
                    table_html += f"""
<tr style="{bb_style}">
<td style="padding: 16px 24px; font-weight: 600; font-size: 14px; color: var(--argus-text-dark);">{h_row['ATTRIBUTE']}</td>
<td style="padding: 16px 24px; font-size: 14px; color: var(--argus-text-muted);">{h_row['SUBJECT_DATA']}</td>
<td style="padding: 16px 24px; font-weight: 700; font-size: 14px; color: var(--argus-text-dark);">{h_row['WATCHLIST_DATA']}</td>
<td style="padding: 16px 24px;"><span style="background-color: {bg_col}; color: {txt_col}; padding: 4px 8px; border-radius: 4px; font-size: 10px; font-weight: 700;">{match_stat}</span></td>
</tr>"""
                table_html += """</tbody>
</table>
</div>"""
                st.markdown(table_html, unsafe_allow_html=True)
            else:
                st.markdown("<div style='padding:24px; background-color:#f3f3f5; border-radius:12px; font-size:14px; color:#524346; margin: 8px 0 24px 0;'>No watchlist attributes flagged for this entity.</div>", unsafe_allow_html=True)

        with st.container(border=True):
            ev_df = session.sql(f"SELECT * FROM AML_SCREENING.ARGUS.EVIDENCE_FILES WHERE CASE_ID = '{case_id}'").to_pandas()
            
            st.markdown(f"<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Evidence & Files</h4><span style='background-color:#e8dddf; color:#696163; padding:2px 8px; border-radius:4px; font-size:10px; font-weight:bold;'>{len(ev_df)} FILES</span></div>", unsafe_allow_html=True)
            
            grid_html = "<div style='display:grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap:12px;'>"
            for idx, e_row in ev_df.iterrows():
                fname = e_row['FILE_NAME']
                fdate = e_row['UPLOAD_DATE']
                icon = "receipt_long" if ".pdf" in fname else "image"
                grid_html += f"""
                <div style='display:flex; align-items:center; gap:12px; padding:12px; background-color:#f8f9fa; border-radius:8px; border: 1px solid #EFEBEB;'>
                    <div style='background-color:#fff; color:#1a1c1d; width:32px; height:32px; border-radius:6px; display:flex; align-items:center; justify-content:center;'><span class='material-symbols-rounded' style='font-size:20px;'>{icon}</span></div>
                    <div style='overflow:hidden;'>
                        <div style='font-size:12px; font-weight:bold; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;'>{fname}</div>
                        <div style='font-size:10px; color:#524346;'>{fdate}</div>
                    </div>
                </div>"""
            grid_html += "</div>"
            st.markdown(grid_html, unsafe_allow_html=True)
            
            st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)
            st.file_uploader("Drop additional evidence", accept_multiple_files=True, label_visibility="collapsed")

        with st.container(border=True):
            st.markdown("<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Decision History</h4><span style='font-size: 13px; font-weight: 600; color: #471524; cursor: pointer;'>View Audit Log</span></div>", unsafe_allow_html=True)
            
            history_df = session.sql(f"SELECT * FROM AML_SCREENING.ARGUS.DECISION_HISTORY WHERE CASE_ID = '{case_id}' ORDER BY SORT_ORDER DESC").to_pandas()
            
            timeline_parts = ["<div style='margin-left: 20px; position: relative; padding: 24px 0 24px 0; font-family: \"Inter\", sans-serif;'>"]
            for idx, h_row in history_df.iterrows():
                title = h_row['TITLE']
                desc = h_row['DESCRIPTION']
                date = h_row['TIMESTAMP']
                dot_color = "#2c0210" if idx == 0 else "#e2e2e4" 
                margin_bottom = "24px" if idx < len(history_df) - 1 else "0"
                
                line_segment = ""
                if idx < len(history_df) - 1:
                    line_segment = f"<div style='position: absolute; left: -1px; top: 16px; bottom: -24px; border-left: 2px solid #e2e2e4;'></div>"
                
                part_html = f"<div style='position: relative; padding-left: 24px; margin-bottom: {margin_bottom};'>{line_segment}"
                part_html += f"<div style='position: absolute; left: -9px; top: 0px; width: 16px; height: 16px; border-radius: 50%; background-color: {dot_color}; box-shadow: 0 0 0 4px #fff;'></div>"
                part_html += f"<div style='display: flex; flex-direction: column;'>"
                part_html += f"<span style='font-size: 10px; text-transform: uppercase; color: #524346; font-weight: 600; letter-spacing: 0.5px;'>{date}</span>"
                part_html += f"<p style='margin: 4px 0 2px 0; font-size: 14px; font-weight: 600; color: #1a1c1d;'>{title}</p>"
                part_html += f"<p style='margin: 0; font-size: 12px; color: #524346;'>{desc}</p>"
                part_html += "</div></div>"
                timeline_parts.append(part_html)
                
            timeline_parts.append("</div>")
            full_html = "".join(timeline_parts)
            st.markdown(full_html, unsafe_allow_html=True)

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
                from datetime import datetime
                now_str = datetime.now().strftime("Today, %I:%M %p")
                
                desc_str = new_note if new_note.strip() else "No additional rationale provided."
                title_str = f"Analyst Review: {decision}"
                
                cur_hist = session.sql(f"SELECT MAX(SORT_ORDER) as M FROM AML_SCREENING.ARGUS.DECISION_HISTORY WHERE CASE_ID='{case_id}'").to_pandas()
                max_order = int(cur_hist['M'].iloc[0]) if not cur_hist.empty and pd.notna(cur_hist['M'].iloc[0]) else 0
                
                full_desc = f"Analyst Julian Thome ({decision}): " + desc_str
                session.sql(f"INSERT INTO AML_SCREENING.ARGUS.DECISION_HISTORY (CASE_ID, SORT_ORDER, TIMESTAMP, TITLE, DESCRIPTION) VALUES ('{case_id}', {max_order + 1}, '{now_str}', '{title_str}', '{full_desc}')").collect()
                
                new_stat = "AUTO-CLEARED" if decision == "Cleared" else ("Investigation" if decision == "Escalate" else "Blocked")
                session.sql(f"UPDATE AML_SCREENING.ARGUS.CASES SET STATUS = '{new_stat}' WHERE ID = '{case_id}'").collect()
                
                st.rerun()


    with col_right:
        with st.container(border=True):
            st.markdown(f"""
                <div style='display:flex; flex-direction:column; gap:16px;'>
                    <div style='background-color:#2c0210; color:white; padding:12px; border-radius:12px; width: fit-content;'>
                        <span class='material-symbols-rounded'>auto_awesome</span>
                    </div>
                    <div>
                        <h4 style='margin:0; color:#2c0210; font-size: 18px;'>{row['AI_INSIGHT_TITLE']}</h4>
                        <p style='margin-top:12px; font-size:14px; color:#1a1c1d; line-height: 1.5;'>{row['AI_INSIGHT_DESC']}</p>
                    </div>
                </div>
            """, unsafe_allow_html=True)



else:
    st.title("Case Management")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        val = metrics_df[metrics_df['METRIC'] == 'Active Cases']['VALUE'].values[0]
        delta = metrics_df[metrics_df['METRIC'] == 'Active Cases']['DELTA'].values[0]
        st.metric("Active Cases", val, delta=delta, delta_color="normal")

    with col2:
        val = metrics_df[metrics_df['METRIC'] == 'Pending Review (High Risk)']['VALUE'].values[0]
        delta = metrics_df[metrics_df['METRIC'] == 'Pending Review (High Risk)']['DELTA'].values[0]
        st.metric("Pending Review (High Risk)", val, delta=delta, delta_color="inverse")

    with col3:
        val = metrics_df[metrics_df['METRIC'] == 'AI Auto-Cleared (24H)']['VALUE'].values[0]
        delta = metrics_df[metrics_df['METRIC'] == 'AI Auto-Cleared (24H)']['DELTA'].values[0]
        st.metric("AI Auto-Cleared (24H)", val, delta=delta, delta_color="normal")

    with col4:
        val = metrics_df[metrics_df['METRIC'] == 'Avg. Resolution Time']['VALUE'].values[0]
        delta = metrics_df[metrics_df['METRIC'] == 'Avg. Resolution Time']['DELTA'].values[0]
        st.metric("Avg. Resolution Time", f"{val} hrs", delta=delta if delta else None)

    st.markdown("<br>", unsafe_allow_html=True)

    f_col1, f_col2, f_col3, f_col_empty, f_col4 = st.columns([2, 2, 2, 5, 3])
    with f_col1:
        status_filter = st.selectbox("Filters", ["All Statuses", "Pending Review", "Investigation", "AUTO-CLEARED"], index=1, label_visibility="collapsed")
    with f_col2:
        risk_filter = st.selectbox("Risk", ["Risk: All Levels", "High", "Medium", "Low"], label_visibility="collapsed")
    with f_col3:
        entity_filter = st.selectbox("Entity", ["Entity: All", "Corporate", "Individual"], label_visibility="collapsed")
    with f_col4:
        st.button("+ New Case", type="primary", use_container_width=True)

    filtered_df = cases_df.copy()
    
    filtered_df = filtered_df.sort_values(by="RISK_SCORE", ascending=False)
    if status_filter != "All Statuses":
        filtered_df = filtered_df[filtered_df['STATUS'] == status_filter]
    
    if risk_filter != "Risk: All Levels":
        if risk_filter == "High": filtered_df = filtered_df[filtered_df['RISK_SCORE'] >= 70]
        elif risk_filter == "Medium": filtered_df = filtered_df[(filtered_df['RISK_SCORE'] >= 30) & (filtered_df['RISK_SCORE'] < 70)]
        elif risk_filter == "Low": filtered_df = filtered_df[filtered_df['RISK_SCORE'] < 30]
        
    if entity_filter != "Entity: All":
        filtered_df = filtered_df[filtered_df['TYPE'] == entity_filter.upper()]

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("""
<div style="display: flex; align-items: center; justify-content: space-between; padding: 0 24px 12px 24px; border-bottom: 2px solid #EFEBEB; margin-bottom: 8px; font-family: 'Inter', sans-serif;">
    <div style="width: 40%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Entity Name</div>
    <div style="width: 25%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Risk Score</div>
    <div style="width: 15%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">AI Confidence</div>
    <div style="width: 15%; text-align: right; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Status</div>
</div>
""", unsafe_allow_html=True)

    st.markdown("""
<style>
.case-row {
    border: 1px solid #EFEBEB;
    border-top: none; 
    padding: 16px 24px;
    background-color: #ffffff;
    transition: all 0.2s ease;
    display: block;
    text-decoration: none !important;
    color: inherit !important;
}
.case-row:first-of-type {
    border-top: 1px solid #EFEBEB;
    border-top-left-radius: 12px;
    border-top-right-radius: 12px;
}
.case-row:last-of-type {
    border-bottom-left-radius: 12px;
    border-bottom-right-radius: 12px;
}
.case-row:hover {
    background-color: #fafafa;
    transform: translateX(4px);
    border-left: 4px solid #4A192C;
}
</style>
""", unsafe_allow_html=True)
    
    if filtered_df.empty:
        st.info("No cases match the selected filters.")
    else:
        for idx, row in filtered_df.iterrows():
            status_labels = {'CRITICAL_MATCH': 'Critical Match', 'PENDING_HUMAN_REVIEW': 'Review Required', 'AUTO_DISMISSED': 'Auto-Dismissed', 'NO_MATCH': 'No Match', 'AUTO-CLEARED': 'Auto-Cleared', 'Pending Review': 'Pending Review', 'Investigation': 'Investigation', 'Blocked': 'Blocked'}
            status_bg = {'CRITICAL_MATCH': '#ffdad6', 'PENDING_HUMAN_REVIEW': '#fff3e0', 'AUTO_DISMISSED': '#b3ebff', 'NO_MATCH': '#e8e8e8', 'AUTO-CLEARED': '#b3ebff', 'Pending Review': '#ffdad6', 'Investigation': '#ffdad6', 'Blocked': '#ffdad6'}
            status_fg = {'CRITICAL_MATCH': '#93000a', 'PENDING_HUMAN_REVIEW': '#e65100', 'AUTO_DISMISSED': '#004e5f', 'NO_MATCH': '#4c4547', 'AUTO-CLEARED': '#004e5f', 'Pending Review': '#93000a', 'Investigation': '#93000a', 'Blocked': '#93000a'}
            color = status_bg.get(row["STATUS"], "#ffdad6")
            text_color = status_fg.get(row["STATUS"], "#93000a")
            label = status_labels.get(row["STATUS"], row["STATUS"])
            
            card_html = f"""<a href="?selected_case={row['ID']}" target="_self" class="case-row">
<div style="display: flex; align-items: center; justify-content: space-between; font-family: 'Inter', sans-serif;">
<div style="display: flex; flex-direction: column; width: 40%;">
<div style="display: flex; align-items: center; gap: 12px; margin-bottom: 2px;">
<img src="https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/72x72/{row['FLAG_URL']}" style="width: 20px; height: 20px;" alt="Flag" />
<span style="font-weight: 600; font-size: 15px; color: var(--argus-text-dark);">{row['ENTITY_NAME']}</span>
</div>
<span style="font-size: 10px; color: var(--argus-text-muted); font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; margin-left: 32px;">{row['TYPE']}</span>
</div>
<div style="width: 25%;">
<div style="width: 100%; max-width: 140px; height: 6px; background-color: var(--argus-accent-light); border-radius: 3px; overflow: hidden; margin-bottom: 4px;">
<div style="width: {row['RISK_SCORE']}%; height: 100%; background-color: var(--argus-primary); border-radius: 3px;"></div>
</div>
<div style="font-size: 11px; font-weight: 700; color: var(--argus-text-muted);">{row['RISK_SCORE']:.1f}</div>
</div>
<div style="width: 15%;">
<div style="font-weight: 700; font-size: 14px; color: var(--argus-text-dark);">{row['AI_CONFIDENCE']}</div>
</div>
<div style="width: 15%; text-align: right;">
<span style="background-color: {color}; color: {text_color}; padding: 6px 14px; border-radius: 4px; font-size: 11px; font-weight: 700; display: inline-block; min-width: 120px; text-align: center;">{label}</span>
</div>
</div>
</a>"""
            st.markdown(card_html, unsafe_allow_html=True)
