import streamlit as st
import pandas as pd
import json
from io import BytesIO
from fpdf import FPDF
from snowflake.snowpark.context import get_active_session
from datetime import datetime

session = get_active_session()


def fetch_case_audit_trail(case_id, screening_request_id, row):
    events = []

    events.append({
        "icon": "shield",
        "title": "Screening Initiated",
        "detail": "Batch screening processed against sanctions snapshot",
        "user": "SYSTEM",
        "timestamp": row['SCREENED_AT'],
    })

    ai_val = str(row.get('AI_DECISION') or '').strip()
    ai_reasoning = str(row.get('AI_REASONING') or '').strip()

    if ai_val and ai_reasoning:
        original_disposition = 'PENDING_AI_ADJUDICATION'
    else:
        original_disposition = str(row.get('STATUS') or '').strip()

    coarse_details = {
        'CRITICAL_MATCH': 'High name similarity with corroborating data. Flagged for immediate review.',
        'PENDING_AI_ADJUDICATION': 'Grey zone match routed to AI adjudicator for secondary analysis.',
        'PENDING_HUMAN_REVIEW': 'Elevated name similarity. Escalated directly to human review.',
        'NO_MATCH': 'Below screening thresholds. No match identified.',
        'AUTO_DISMISSED': 'Low composite score. Automatically dismissed by rule-based filter.',
    }
    coarse_label = STATUS_LABELS.get(original_disposition, original_disposition)
    coarse_detail = coarse_details.get(original_disposition, 'Disposition assigned by coarse filter.')

    events.append({
        "icon": "rule",
        "title": "Coarse Filter: " + coarse_label,
        "detail": coarse_detail,
        "user": "SYSTEM",
        "timestamp": row['SCREENED_AT'],
    })

    if ai_val and ai_reasoning:
        ai_ts = row['SCREENED_AT']
        events.append({
            "icon": "auto_awesome",
            "title": "AI Adjudication: " + ai_val,
            "detail": ai_reasoning[:200] if ai_reasoning else 'No reasoning provided.',
            "user": "AI ADJUDICATOR",
            "timestamp": ai_ts,
        })

    audit_df = session.sql(f"""
        SELECT
            DETAILS:"decision"::VARCHAR AS DECISION,
            DETAILS:"rationale"::VARCHAR AS RATIONALE,
            DETAILS:"new_disposition"::VARCHAR AS NEW_DISPOSITION,
            CREATED_AT,
            CREATED_BY
        FROM AML_SCREENING.PIPELINE.AUDIT_LOG
        WHERE EVENT_TYPE = 'HUMAN_REVIEW'
          AND DETAILS:"result_id"::VARCHAR = '{case_id}'
        ORDER BY CREATED_AT ASC
    """).to_pandas()

    icon_map = {"Clear": "check_circle", "Escalate": "arrow_upward"}
    for _, a_row in audit_df.iterrows():
        decision = str(a_row.get('DECISION', 'Unknown') or 'Unknown')
        rationale = str(a_row.get('RATIONALE', '') or '')
        new_disp = str(a_row.get('NEW_DISPOSITION', '') or '')
        disp_label = STATUS_LABELS.get(new_disp, new_disp)
        detail_text = (disp_label + ". " + rationale) if rationale else disp_label

        events.append({
            "icon": icon_map.get(decision, "rate_review"),
            "title": "Human Review: " + decision,
            "detail": detail_text,
            "user": str(a_row['CREATED_BY']),
            "timestamp": a_row['CREATED_AT'],
        })

    return events


def render_audit_trail(events):
    if not events:
        st.caption("No activity recorded yet.")
        return

    for i, ev in enumerate(events):
        is_last = i == len(events) - 1
        ts = ev['timestamp']
        if isinstance(ts, str):
            ts_display = ts[:16]
        elif hasattr(ts, 'strftime'):
            ts_display = ts.strftime('%Y-%m-%d %H:%M')
        else:
            ts_display = str(ts)[:16]

        border_css = "border-left:2px solid #EFEBEB; margin-left:11px; padding-left:20px; padding-bottom:16px;" if not is_last else "margin-left:11px; padding-left:20px;"

        st.markdown(
            "<div style='display:flex; align-items:center; gap:8px;'>"
            "<span class='material-symbols-rounded' style='font-size:18px; color:#4A192C; background:#F8F5F5; border:1px solid #EFEBEB; border-radius:50%; padding:4px;'>" + ev['icon'] + "</span>"
            "<span style='font-size:15px; font-weight:700; color:var(--argus-text-dark);'>" + ev['title'] + "</span>"
            "</div>",
            unsafe_allow_html=True
        )
        st.markdown(
            "<div style='" + border_css + "'>"
            "<div style='font-size:14px; color:var(--argus-text-muted); line-height:1.5;'>" + ev['detail'] + "</div>"
            "<div style='font-size:12px; color:#8C7C83; margin-top:4px; font-weight:600;'>" + ev['user'] + " &middot; " + ts_display + "</div>"
            "</div>",
            unsafe_allow_html=True
        )

def _safe_latin(text):
    from unidecode import unidecode
    return unidecode(str(text))

def generate_case_pdf(row, audit_events):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=20)

    pw = pdf.w - pdf.l_margin - pdf.r_margin

    pdf.set_font("Helvetica", "B", 18)
    pdf.set_text_color(44, 2, 16)
    pdf.cell(pw, 10, "ARGUS - Case Report", align="C", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)
    pdf.set_draw_color(239, 235, 235)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + pw, pdf.get_y())
    pdf.ln(6)

    disp_label = STATUS_LABELS.get(row.get('STATUS', ''), row.get('STATUS', ''))
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(pw, 8, _safe_latin(row.get('ENTITY_NAME', 'N/A')), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(82, 67, 70)
    pdf.cell(pw, 6, _safe_latin("Status: " + disp_label + "   |   Risk Score: " + str(row.get('RISK_SCORE', 'N/A')) + "%"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    fields = [
        ("Case ID", str(row.get('ID', 'N/A'))),
        ("Entity Type", str(row.get('TYPE', 'N/A'))),
        ("Country", str(row.get('COUNTRY', 'N/A'))),
        ("Date of Birth", str(row.get('DOB', 'N/A'))),
        ("Place of Birth", str(row.get('POB', 'N/A'))),
        ("Source System", str(row.get('SOURCE_SYSTEM', 'N/A'))),
        ("Screened At", str(row.get('SCREENED_AT', 'N/A'))[:19]),
    ]

    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(44, 2, 16)
    pdf.cell(pw, 8, "Case Details", new_x="LMARGIN", new_y="NEXT")
    pdf.set_draw_color(239, 235, 235)

    for label, val in fields:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(140, 124, 131)
        pdf.cell(50, 6, _safe_latin(label))
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(26, 28, 29)
        pdf.cell(pw - 50, 6, _safe_latin(val), new_x="LMARGIN", new_y="NEXT")

    pdf.ln(6)

    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(44, 2, 16)
    pdf.cell(pw, 8, "Match Comparison", new_x="LMARGIN", new_y="NEXT")

    col_w = [pw * 0.2, pw * 0.28, pw * 0.28, pw * 0.24]
    headers = ["Attribute", "Screened Data", "Sanctions Match", "Status"]
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(82, 67, 70)
    pdf.set_fill_color(248, 245, 245)
    for i, h in enumerate(headers):
        pdf.cell(col_w[i], 7, h, border=0, fill=True)
    pdf.ln()

    comparison_data = [
        ("Full Name", str(row.get('ENTITY_NAME', 'N/A')), str(row.get('MATCHED_ENTITY_NAME', 'N/A') or 'N/A')),
        ("Date of Birth", str(row.get('DOB', 'N/A')), str(row.get('MATCHED_DOB', 'N/A') or 'N/A')),
        ("Country", str(row.get('COUNTRY', 'N/A')), str(row.get('MATCHED_COUNTRY', 'N/A') or 'N/A')),
        ("Place of Birth", str(row.get('POB', 'N/A')), str(row.get('MATCHED_POB', 'N/A') or 'N/A')),
    ]

    score_map = {
        "Full Name": row.get('NAME_SIMILARITY_SCORE', 0) or 0,
        "Date of Birth": row.get('DOB_SCORE', 0) or 0,
        "Country": row.get('COUNTRY_SCORE', 0) or 0,
        "Place of Birth": row.get('POB_SCORE', 0) or 0,
    }

    pdf.set_font("Helvetica", "", 8)
    for attr, screened, matched in comparison_data:
        sc = score_map.get(attr, 0)
        stat = "MATCH" if sc >= 0.85 else ("PARTIAL" if sc >= 0.5 else "MISMATCH")
        pdf.set_text_color(26, 28, 29)
        pdf.cell(col_w[0], 6, attr)
        pdf.set_text_color(82, 67, 70)
        pdf.cell(col_w[1], 6, _safe_latin(screened[:35]))
        pdf.set_text_color(26, 28, 29)
        pdf.cell(col_w[2], 6, _safe_latin(matched[:35]))
        if stat == "MATCH":
            pdf.set_text_color(0, 78, 95)
        else:
            pdf.set_text_color(76, 69, 71)
        pdf.cell(col_w[3], 6, stat, new_x="LMARGIN", new_y="NEXT")

    pdf.ln(6)

    pdf.set_font("Helvetica", "B", 11)
    pdf.set_text_color(44, 2, 16)
    pdf.cell(pw, 8, "Score Breakdown", new_x="LMARGIN", new_y="NEXT")

    scores = [
        ("Name Similarity", row.get('NAME_SIMILARITY_SCORE', 0) or 0),
        ("DOB", row.get('DOB_SCORE', 0) or 0),
        ("Country", row.get('COUNTRY_SCORE', 0) or 0),
        ("Place of Birth", row.get('POB_SCORE', 0) or 0),
        ("Composite", row.get('COMPOSITE_SCORE', 0) or 0),
    ]
    bar_max_w = pw * 0.55
    for s_name, s_val in scores:
        pct = round(s_val * 100, 1)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(26, 28, 29)
        pdf.cell(55, 6, s_name)
        x_bar = pdf.get_x()
        y_bar = pdf.get_y() + 1.5
        pdf.set_fill_color(232, 221, 223)
        pdf.rect(x_bar, y_bar, bar_max_w, 3, "F")
        if pct >= 85:
            pdf.set_fill_color(229, 62, 62)
        elif pct >= 50:
            pdf.set_fill_color(245, 124, 0)
        else:
            pdf.set_fill_color(56, 161, 105)
        pdf.rect(x_bar, y_bar, bar_max_w * (pct / 100), 3, "F")
        pdf.set_x(x_bar + bar_max_w + 4)
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(20, 6, str(pct) + "%", new_x="LMARGIN", new_y="NEXT")

    ai_val = str(row.get('AI_DECISION') or '').strip()
    ai_reasoning = str(row.get('AI_REASONING') or '').strip()
    if ai_val and ai_reasoning:
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(44, 2, 16)
        pdf.cell(pw, 8, _safe_latin("AI Decision: " + ai_val), new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(26, 28, 29)
        pdf.multi_cell(pw, 5, _safe_latin(ai_reasoning[:500]))

    if audit_events:
        pdf.ln(4)
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(44, 2, 16)
        pdf.cell(pw, 8, "Activity Log", new_x="LMARGIN", new_y="NEXT")
        for ev in audit_events:
            ts = ev['timestamp']
            if isinstance(ts, str):
                ts_str = ts[:16]
            elif hasattr(ts, 'strftime'):
                ts_str = ts.strftime('%Y-%m-%d %H:%M')
            else:
                ts_str = str(ts)[:16]
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(44, 2, 16)
            pdf.cell(pw, 5, _safe_latin(ev['title']), new_x="LMARGIN", new_y="NEXT")
            pdf.set_font("Helvetica", "", 8)
            pdf.set_text_color(82, 67, 70)
            pdf.cell(pw, 4, _safe_latin(ev['user'] + "  |  " + ts_str), new_x="LMARGIN", new_y="NEXT")
            if ev.get('detail'):
                pdf.set_text_color(26, 28, 29)
                pdf.multi_cell(pw, 4, _safe_latin(str(ev['detail'])[:300]))
            pdf.ln(2)

    pdf.ln(6)
    pdf.set_draw_color(239, 235, 235)
    pdf.line(pdf.l_margin, pdf.get_y(), pdf.l_margin + pw, pdf.get_y())
    pdf.ln(4)
    pdf.set_font("Helvetica", "I", 7)
    pdf.set_text_color(140, 124, 131)
    pdf.cell(pw, 4, "Generated by Argus Compliance Platform  |  " + datetime.now().strftime('%Y-%m-%d %H:%M UTC'), align="C")

    buf = BytesIO()
    pdf.output(buf)
    buf.seek(0)
    return buf.getvalue()

def _country_flag_code(iso2):
    if not iso2 or len(iso2) != 2 or not iso2.isalpha() or iso2.upper() == 'NA':
        return ''
    a, b = iso2.upper()
    return f"{0x1F1E6 + ord(a) - ord('A'):x}-{0x1F1E6 + ord(b) - ord('A'):x}"

STATUS_LABELS = {
    'CRITICAL_MATCH': 'Critical Match',
    'PENDING_HUMAN_REVIEW': 'Review Required',
    'AUTO_DISMISSED': 'Auto-Dismissed',
    'HUMAN_DISMISSED': 'Human-Dismissed',
    'NO_MATCH': 'No Match',
    'DISMISS_OVERRIDDEN': 'Dismiss Overridden',
}

STATUS_BG = {
    'CRITICAL_MATCH': '#ffdad6',
    'PENDING_HUMAN_REVIEW': '#fff3e0',
    'AUTO_DISMISSED': '#b3ebff',
    'HUMAN_DISMISSED': '#d4edda',
    'NO_MATCH': '#e8e8e8',
    'DISMISS_OVERRIDDEN': '#fff3e0',
}

STATUS_FG = {
    'CRITICAL_MATCH': '#93000a',
    'PENDING_HUMAN_REVIEW': '#e65100',
    'AUTO_DISMISSED': '#004e5f',
    'HUMAN_DISMISSED': '#155724',
    'NO_MATCH': '#4c4547',
    'DISMISS_OVERRIDDEN': '#e65100',
}

CASE_QUERY = """
    SELECT
        r.RESULT_ID AS ID,
        r.SCREENING_REQUEST_ID,
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
        COALESCE(i.SOURCE_SYSTEM, 'N/A') AS SOURCE_SYSTEM,
        COALESCE(i.CARD_REQUESTED, 'N/A') AS CARD_REQUESTED
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

    safe_case_id = case_id.replace("'", "''")
    case_data = session.sql(CASE_QUERY + f" WHERE r.RESULT_ID = '{safe_case_id}'").to_pandas()

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
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>fingerprint</span> <b>ID:</b> {row['ID']}</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>location_on</span> <b>Country:</b> {row['COUNTRY']}</div>
                <div style='display:flex; align-items:center; gap:6px;'><span class='material-symbols-rounded' style='font-size:18px; color:#8C7C83;'>calendar_today</span> <b>DOB:</b> {row['DOB']}</div>
            </div>
        """, unsafe_allow_html=True)

    with det_col2:
        audit_events_for_pdf = fetch_case_audit_trail(case_id, row.get('SCREENING_REQUEST_ID', ''), row)
        pdf_bytes = generate_case_pdf(row, audit_events_for_pdf)
        btn_col1, btn_col2 = st.columns([1, 1])
        with btn_col1:
            if st.button("Share Case", icon=":material/share:", use_container_width=True):
                import streamlit.components.v1 as components
                components.html(f"<script>navigator.clipboard.writeText('{case_id}');</script>", height=0)
                st.toast(f"Case ID copied: {case_id}")
        with btn_col2:
            st.download_button(
                "Export PDF",
                data=pdf_bytes,
                file_name=f"case_{case_id}.pdf",
                mime="application/pdf",
                icon=":material/print:",
                use_container_width=True,
            )

    st.markdown(f"""
        <div style='display:flex; align-items:center; gap: 0px; margin-top: 16px; margin-bottom: 24px; padding-top: 16px; border-top: 1px solid #EFEBEB;'>
            <div style='display:flex; align-items:center; gap:8px; padding-right: 20px; border-right: 1px solid #EFEBEB;'>
                <span class='material-symbols-rounded' style='font-size:20px; color:#524346;'>business</span>
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>SOURCE:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row['SOURCE_SYSTEM']}</span>
            </div>
            <div style='display:flex; align-items:center; gap:8px; padding-left: 20px;'>
                <span class='material-symbols-rounded' style='font-size:20px; color:#524346;'>credit_card</span>
                <span style='font-size:11px; font-weight:700; color:#8C7C83; text-transform:uppercase; letter-spacing:0.5px;'>CARD REQUESTED:</span>
                <span style='font-size:14px; font-weight:600; color:#2c0210;'>{row.get('CARD_REQUESTED', 'N/A') or 'N/A'}</span>
            </div>
        </div>
    """, unsafe_allow_html=True)

    col_left, col_right = st.columns([8, 3])

    with col_left:
        with st.container(border=True):
            matched_list_label = row.get('MATCHED_LIST_NAME', 'N/A') or 'N/A'
            matched_list_abbr = row.get('MATCHED_LIST_ABBREVIATION', '') or ''
            list_display = matched_list_abbr if matched_list_abbr else matched_list_label
            st.markdown(f"""<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'>
<h4 style='margin:0;'>Match Comparison</h4>
<span style='font-size:10px; font-weight:700; color:#524346; letter-spacing:0.5px; text-transform:uppercase; background:#F8F5F5; padding:6px 14px; border-radius:100px; border:1px solid #EFEBEB;'>{list_display}</span>
</div>""", unsafe_allow_html=True)

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
            st.markdown("<h4 style='margin:0 0 16px 0;'>Evidence & Files</h4>", unsafe_allow_html=True)

            evidence_df = session.sql(f"""
                SELECT EVIDENCE_ID, FILE_NAME, FILE_TYPE, FILE_SIZE, UPLOADED_AT, UPLOADED_BY
                FROM AML_SCREENING.PIPELINE.CASE_EVIDENCE
                WHERE RESULT_ID = '{case_id}'
                ORDER BY UPLOADED_AT DESC
            """).to_pandas()

            if evidence_df.empty:
                st.markdown("<div style='padding:24px; background-color:#f3f3f5; border-radius:12px; font-size:14px; color:#524346;'>No evidence files attached to this screening result.</div>", unsafe_allow_html=True)
            else:
                for _, ef in evidence_df.iterrows():
                    size_kb = round(ef['FILE_SIZE'] / 1024, 1) if ef['FILE_SIZE'] else 0
                    ts = str(ef['UPLOADED_AT'])[:16]
                    fc1, fc2, fc3 = st.columns([7, 2, 1], vertical_alignment="center")
                    with fc1:
                        st.markdown(
                            "<div style='display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid var(--argus-border);'>"
                            "<span class='material-symbols-rounded' style='font-size:20px; color:#4A192C;'>description</span>"
                            "<div>"
                            f"<div style='font-size:14px; font-weight:600; color:var(--argus-text-dark);'>{ef['FILE_NAME']}</div>"
                            f"<div style='font-size:11px; color:var(--argus-text-muted);'>{ef['FILE_TYPE']} &middot; {size_kb} KB &middot; {ef['UPLOADED_BY']} &middot; {ts}</div>"
                            "</div></div>",
                            unsafe_allow_html=True
                        )
                    with fc2:
                        try:
                            stage_path = f"@AML_SCREENING.PIPELINE.EVIDENCE_STAGE/{case_id}/{ef['FILE_NAME']}"
                            file_stream = session.file.get_stream(stage_path)
                            file_bytes = file_stream.read()
                            st.download_button(
                                "Download",
                                data=file_bytes,
                                file_name=ef['FILE_NAME'],
                                icon=":material/download:",
                                key=f"dl_{ef['EVIDENCE_ID']}",
                                use_container_width=True,
                            )
                        except Exception:
                            st.button("Unavailable", disabled=True, key=f"dl_{ef['EVIDENCE_ID']}", use_container_width=True)
                    with fc3:
                        if st.button("", icon=":material/delete:", key=f"rm_{ef['EVIDENCE_ID']}", use_container_width=True):
                            eid = str(ef['EVIDENCE_ID']).replace("'", "''")
                            fname = str(ef['FILE_NAME']).replace("'", "''")
                            session.sql(f"DELETE FROM AML_SCREENING.PIPELINE.CASE_EVIDENCE WHERE EVIDENCE_ID = '{eid}'").collect()
                            try:
                                session.sql(f"REMOVE @AML_SCREENING.PIPELINE.EVIDENCE_STAGE/{case_id}/{fname}").collect()
                            except Exception:
                                pass
                            st.rerun()

            st.markdown("<div style='margin-top:16px;'></div>", unsafe_allow_html=True)
            uploaded_files = st.file_uploader("Upload evidence files", accept_multiple_files=True, label_visibility="collapsed", key=f"evidence_upload_{case_id}")

            if uploaded_files:
                for uf in uploaded_files:
                    file_bytes = uf.read()
                    file_name = uf.name.replace("'", "''")
                    file_type = uf.type or "unknown"
                    file_size = len(file_bytes)
                    stage_path = f"{case_id}/{uf.name}"

                    try:
                        input_stream = BytesIO(file_bytes)
                        session.file.put_stream(
                            input_stream,
                            f"@AML_SCREENING.PIPELINE.EVIDENCE_STAGE/{stage_path}",
                            auto_compress=False,
                            overwrite=True,
                        )

                        session.sql(f"""
                            INSERT INTO AML_SCREENING.PIPELINE.CASE_EVIDENCE
                                (RESULT_ID, FILE_NAME, FILE_TYPE, FILE_SIZE, STAGE_PATH)
                            VALUES ('{case_id}', '{file_name}', '{file_type}', {file_size}, '{stage_path}')
                        """).collect()

                        st.success(f"Uploaded **{uf.name}** ({round(file_size/1024, 1)} KB)")
                    except Exception as e:
                        st.error(f"Failed to upload {uf.name}: {e}")

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
                decision = st.selectbox("Decision", ["Clear", "Escalate"], label_visibility="collapsed")
            with rc3:
                submit_review = st.form_submit_button("Submit Review", type="primary", use_container_width=True)

            if submit_review:
                disposition_map = {"Clear": "HUMAN_DISMISSED", "Escalate": "CRITICAL_MATCH"}
                new_disp = disposition_map[decision]
                rationale = (new_note.strip() if new_note.strip() else "No additional rationale provided.").replace("'", "''")

                session.sql(f"""
                    UPDATE AML_SCREENING.PIPELINE.SCREENING_RESULTS
                    SET DISPOSITION = '{new_disp}'
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

        with st.container(border=True):
            st.markdown("<h4 style='margin:0 0 16px 0;'>Activity Log</h4>", unsafe_allow_html=True)
            audit_events = fetch_case_audit_trail(case_id, row.get('SCREENING_REQUEST_ID', ''), row)
            render_audit_trail(audit_events)

    with col_right:
        ai_val_display = str(row.get('AI_DECISION') or '').strip()
        ai_reasoning_display = str(row.get('AI_REASONING') or '').strip()
        if ai_val_display and ai_reasoning_display:
            with st.container(border=True):
                ai_title = f"AI Decision: {row['AI_DECISION']}"
                ai_desc = row.get('AI_REASONING', '') or 'No reasoning provided.'
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
            st.markdown("<div style='display:flex; justify-content:space-between; align-items:center; margin-bottom: 24px;'><h4 style='margin:0;'>Score Breakdown</h4></div>", unsafe_allow_html=True)

            scores = [
                ("Name Similarity", row.get('NAME_SIMILARITY_SCORE', 0) or 0),
                ("DOB", row.get('DOB_SCORE', 0) or 0),
                ("Country", row.get('COUNTRY_SCORE', 0) or 0),
                ("Place of Birth", row.get('POB_SCORE', 0) or 0),
                ("Composite", row.get('COMPOSITE_SCORE', 0) or 0),
            ]
            score_html = "<div style='display:flex; flex-direction:column; gap:12px; padding-bottom:16px;'>"
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
            st.markdown("<div style='padding-bottom:8px;'></div>", unsafe_allow_html=True)

else:
    cases_df = session.sql(CASE_QUERY + """
        WHERE r.DISPOSITION != 'AUTO_DISMISSED'
           OR r.SCREENED_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY r.COMPOSITE_SCORE DESC
    """).to_pandas()
    cases_df['FLAG_URL'] = cases_df['COUNTRY'].apply(_country_flag_code) + '.png'

    st.title("Case Management")

    total = len(cases_df)
    pending = len(cases_df[cases_df['STATUS'].isin(['PENDING_HUMAN_REVIEW', 'CRITICAL_MATCH'])])
    dismissed = len(cases_df[cases_df['STATUS'].isin(['AUTO_DISMISSED', 'HUMAN_DISMISSED'])])
    no_match = len(cases_df[cases_df['STATUS'] == 'NO_MATCH'])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Screened", total)
    with col2:
        st.metric("Pending Review", pending, delta_color="inverse")
    with col3:
        st.metric("Dismissed", dismissed, delta_color="normal")
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
.case-row-btn > div > div > div > div > div > button {
    height: 84px !important;
    width: 100% !important;
    border: 1px solid #EFEBEB !important;
    border-radius: 8px !important;
    background: #fff !important;
    padding: 0 24px !important;
    margin: 0 !important;
    text-align: left !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}
.case-row-btn > div > div > div > div > div > button:hover {
    border-color: #4A192C !important;
    box-shadow: 0 2px 8px rgba(74, 25, 44, 0.08) !important;
}
.case-row-btn > div > div > div > div > div > button > div {
    display: flex !important;
    align-items: center !important;
    width: 100% !important;
}
</style>
""", unsafe_allow_html=True)

    st.markdown("""
<div style="display: flex; align-items: center; padding: 0 24px 12px 24px; border-bottom: 2px solid #EFEBEB; margin-bottom: 8px; font-family: 'Inter', sans-serif;">
    <div style="width: 42%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Entity Name</div>
    <div style="width: 24%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Risk Score</div>
    <div style="width: 14%; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Name Similarity</div>
    <div style="width: 20%; text-align: center; font-size: 11px; font-weight: 700; color: #8C7C83; text-transform: uppercase; letter-spacing: 0.5px;">Status</div>
</div>
""", unsafe_allow_html=True)

    if filtered_df.empty:
        st.info("No cases match the selected filters.")
    else:
        for idx, row in filtered_df.iterrows():
            color = STATUS_BG.get(row["STATUS"], "#ffdad6")
            txt_color = STATUS_FG.get(row["STATUS"], "#93000a")
            label = STATUS_LABELS.get(row["STATUS"], row["STATUS"])
            rid = row['ID']
            flag_img = f"<img src=\"https://cdnjs.cloudflare.com/ajax/libs/twemoji/14.0.2/72x72/{row['FLAG_URL']}\" style=\"width:20px; height:20px;\" />" if row['FLAG_URL'] else ""

            btn_label = f"""<div style="display:flex; align-items:center; width:100%; font-family:'Inter',sans-serif;">
<div style="width:42%; display:flex; flex-direction:column;">
<div style="display:flex; align-items:center; gap:12px; margin-bottom:2px;">
{flag_img}<span style="font-weight:600; font-size:15px; color:var(--argus-text-dark);">{row['ENTITY_NAME']}</span>
</div>
<span style="font-size:10px; color:var(--argus-text-muted); font-weight:600; text-transform:uppercase; letter-spacing:0.5px; margin-left:32px;">{row['TYPE']}</span>
</div>
<div style="width:24%;">
<div style="width:100%; max-width:140px; height:6px; background:var(--argus-accent-light); border-radius:3px; overflow:hidden; margin-bottom:4px;">
<div style="width:{row['RISK_SCORE']}%; height:100%; background:var(--argus-primary); border-radius:3px;"></div>
</div>
<div style="font-size:11px; font-weight:700; color:var(--argus-text-muted);">{row['RISK_SCORE']:.1f}</div>
</div>
<div style="width:14%;">
<div style="font-weight:700; font-size:14px; color:var(--argus-text-dark);">{row['NAME_SIMILARITY']}</div>
</div>
<div style="width:20%; text-align:center;">
<span style="background:{color}; color:{txt_color}; padding:6px 14px; border-radius:4px; font-size:11px; font-weight:700; display:inline-block; min-width:120px; text-align:center;">{label}</span>
</div>
<div style="width:4%; text-align:right; color:var(--argus-text-muted); font-size:18px;">›</div>
</div>"""

            with st.container():
                st.markdown("<div class='case-row-btn'>", unsafe_allow_html=True)
                if st.button(btn_label, key=f"c_{rid}", use_container_width=True):
                    st.query_params["selected_case"] = rid
                    st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)
