import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

EXPECTED_COLUMNS = {
    "FIRST_NAME": {"required": True, "type": "VARCHAR"},
    "LAST_NAME": {"required": True, "type": "VARCHAR"},
    "MIDDLE_NAME": {"required": False, "type": "VARCHAR"},
    "DATE_OF_BIRTH": {"required": False, "type": "DATE (YYYY-MM-DD)"},
    "PLACE_OF_BIRTH": {"required": False, "type": "VARCHAR"},
    "GENDER": {"required": False, "type": "VARCHAR"},
    "COUNTRY": {"required": False, "type": "VARCHAR (ISO-2)"},
    "CARD_REQUESTED": {"required": False, "type": "VARCHAR"},
    "SOURCE_SYSTEM": {"required": False, "type": "VARCHAR"},
}

st.title("Integrations")
st.caption("Submit individual screenings or bulk-upload datasets into the AML screening pipeline.")

pipe_col1, pipe_col2 = st.columns([8, 2], vertical_alignment="center")
with pipe_col1:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:10px;'>
            <span class='material-symbols-rounded' style='font-size:20px; color:#4A192C;'>sync</span>
            <span style='font-size:14px; color:var(--argus-text-muted);'>Trigger the screening pipeline manually to process any pending records.</span>
        </div>
    """, unsafe_allow_html=True)
with pipe_col2:
    if st.button("Run Pipeline Now", icon=":material/play_arrow:", type="primary", use_container_width=True):
        with st.spinner("Running pipeline..."):
            batch_result = session.sql("CALL AML_SCREENING.PIPELINE.SCREEN_BATCH()").collect()
            st.toast(f"Batch: {batch_result[0][0]}")
            ai_result = session.sql("CALL AML_SCREENING.PIPELINE.RUN_AI_ADJUDICATOR()").collect()
            st.toast(f"AI: {ai_result[0][0]}")
        st.success("Pipeline completed.")

st.markdown("<br>", unsafe_allow_html=True)

tab_manual, tab_bulk = st.tabs(["Manual Screening", "Bulk CSV Upload"])

with tab_manual:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>person_add</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Submit Individual Screening</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Enter entity details to create a new screening request.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    with st.form("manual_screening_form", clear_on_submit=True, border=True):
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            first_name = st.text_input("First Name *", placeholder="e.g. Mohammed")
        with col_b:
            middle_name = st.text_input("Middle Name", placeholder="e.g. Ali")
        with col_c:
            last_name = st.text_input("Last Name *", placeholder="e.g. Hassan")

        col_d, col_e, col_f = st.columns(3)
        with col_d:
            dob = st.date_input("Date of Birth", value=None)
        with col_e:
            country = st.text_input("Country (ISO-2)", placeholder="e.g. LB", max_chars=2)
        with col_f:
            gender = st.selectbox("Gender", ["Male", "Female", "Unknown"])

        col_g, col_h = st.columns(2)
        with col_g:
            pob = st.text_input("Place of Birth", placeholder="e.g. Beirut")
        with col_h:
            card_requested = st.text_input("Card Requested", placeholder="e.g. Loyalty Card")

        source_system = st.text_input("Source System", value="MANUAL", placeholder="e.g. MANUAL, CRM, KYC_PLATFORM")

        st.markdown("<hr style='margin:16px 0; border:none; border-top:1px solid #EFEBEB;'>", unsafe_allow_html=True)

        sc1, sc2, sc3 = st.columns([4, 3, 3], vertical_alignment="center")
        with sc1:
            run_pipeline = st.checkbox("Run pipeline after submission", value=True)
        with sc3:
            submitted = st.form_submit_button("Submit Screening", type="primary", use_container_width=True)

        if submitted:
            if not first_name or not first_name.strip():
                st.error("First Name is required.")
            elif not last_name or not last_name.strip():
                st.error("Last Name is required.")
            else:
                fn = first_name.strip().replace("'", "''")
                ln = last_name.strip().replace("'", "''")
                mn = f"'{middle_name.strip().replace(chr(39), chr(39)+chr(39))}'" if middle_name and middle_name.strip() else "NULL"
                dob_val = f"'{dob.strftime('%Y-%m-%d')}'" if dob else "NULL"
                country_val = f"'{country.strip().upper()}'" if country and country.strip() else "NULL"
                pob_val = f"'{pob.strip().replace(chr(39), chr(39)+chr(39))}'" if pob and pob.strip() else "NULL"
                card_val = f"'{card_requested.strip().replace(chr(39), chr(39)+chr(39))}'" if card_requested and card_requested.strip() else "NULL"
                src_val = f"'{source_system.strip().replace(chr(39), chr(39)+chr(39))}'" if source_system and source_system.strip() else "'MANUAL'"

                session.sql(f"""
                    INSERT INTO AML_SCREENING.PIPELINE.INCOMING_SCREENINGS
                        (FIRST_NAME, LAST_NAME, MIDDLE_NAME, DATE_OF_BIRTH, COUNTRY, GENDER, PLACE_OF_BIRTH, CARD_REQUESTED, SOURCE_SYSTEM)
                    VALUES ('{fn}', '{ln}', {mn}, {dob_val}, {country_val}, '{gender}', {pob_val}, {card_val}, {src_val})
                """).collect()

                st.success(f"Screening request submitted for **{first_name.strip()} {last_name.strip()}**.")

                if run_pipeline:
                    with st.spinner("Running screening pipeline..."):
                        batch_result = session.sql("CALL AML_SCREENING.PIPELINE.SCREEN_BATCH()").collect()
                        st.info(f"Batch: {batch_result[0][0]}")
                        ai_result = session.sql("CALL AML_SCREENING.PIPELINE.RUN_AI_ADJUDICATOR()").collect()
                        st.info(f"AI Adjudicator: {ai_result[0][0]}")

with tab_bulk:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>upload_file</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Bulk CSV Upload</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Upload a CSV file to batch-submit screening requests.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    with st.container(border=True):
        st.markdown("<h5 style='margin:0 0 12px 0;'>Expected CSV Format</h5>", unsafe_allow_html=True)
        fmt_html = "<div style='display:flex; flex-wrap:wrap; gap:8px;'>"
        for col_name, meta in EXPECTED_COLUMNS.items():
            badge_bg = "#ffdad6" if meta["required"] else "#F8F5F5"
            badge_fg = "#93000a" if meta["required"] else "#524346"
            req_tag = " *" if meta["required"] else ""
            fmt_html += f"<span style='background:{badge_bg}; color:{badge_fg}; padding:4px 10px; border-radius:6px; font-size:11px; font-weight:700; font-family:monospace;'>{col_name}{req_tag}</span>"
        fmt_html += "</div>"
        st.markdown(fmt_html, unsafe_allow_html=True)
        st.caption("Columns marked with * are required. Other columns are optional and can be omitted.")

    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"], label_visibility="collapsed")

    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

            missing_required = []
            for col_name, meta in EXPECTED_COLUMNS.items():
                if meta["required"] and col_name not in df.columns:
                    missing_required.append(col_name)

            if missing_required:
                st.error(f"Missing required columns: **{', '.join(missing_required)}**")
            else:
                valid_cols = [c for c in df.columns if c in EXPECTED_COLUMNS]
                extra_cols = [c for c in df.columns if c not in EXPECTED_COLUMNS]

                if extra_cols:
                    st.warning(f"Ignoring unrecognized columns: {', '.join(extra_cols)}")

                upload_df = df[valid_cols].copy()

                empty_fn = upload_df['FIRST_NAME'].isna() | (upload_df['FIRST_NAME'].astype(str).str.strip() == '')
                empty_ln = upload_df['LAST_NAME'].isna() | (upload_df['LAST_NAME'].astype(str).str.strip() == '')
                invalid_rows = empty_fn | empty_ln

                if invalid_rows.any():
                    st.warning(f"{invalid_rows.sum()} row(s) have empty FIRST_NAME or LAST_NAME and will be skipped.")
                    upload_df = upload_df[~invalid_rows]

                if upload_df.empty:
                    st.error("No valid rows to upload after validation.")
                else:
                    st.markdown(f"**Preview** — {len(upload_df)} valid row(s)")
                    st.dataframe(upload_df.head(50), use_container_width=True, hide_index=True)

                    bc1, bc2, bc3 = st.columns([4, 3, 3])
                    with bc1:
                        run_pipeline_bulk = st.checkbox("Run pipeline after upload", value=True, key="bulk_pipeline")
                    with bc3:
                        upload_btn = st.button("Upload & Insert", type="primary", use_container_width=True)

                    if upload_btn:
                        for col_name in EXPECTED_COLUMNS:
                            if col_name not in upload_df.columns:
                                upload_df[col_name] = None

                        if upload_df['SOURCE_SYSTEM'].isna().all():
                            upload_df['SOURCE_SYSTEM'] = 'CSV_UPLOAD'

                        if 'DATE_OF_BIRTH' in upload_df.columns:
                            upload_df['DATE_OF_BIRTH'] = pd.to_datetime(upload_df['DATE_OF_BIRTH'], errors='coerce').dt.strftime('%Y-%m-%d')

                        ordered_cols = list(EXPECTED_COLUMNS.keys())
                        insert_df = upload_df[ordered_cols]

                        for _, r in insert_df.iterrows():
                            vals = []
                            for c in ordered_cols:
                                v = r[c]
                                if pd.isna(v) or v is None:
                                    vals.append("NULL")
                                else:
                                    vals.append("'" + str(v).replace("'", "''") + "'")
                            session.sql(f"""
                                INSERT INTO AML_SCREENING.PIPELINE.INCOMING_SCREENINGS
                                    ({', '.join(ordered_cols)})
                                VALUES ({', '.join(vals)})
                            """).collect()

                        st.success(f"**{len(upload_df)}** screening request(s) inserted successfully.")

                        if run_pipeline_bulk:
                            with st.spinner("Running screening pipeline..."):
                                batch_result = session.sql("CALL AML_SCREENING.PIPELINE.SCREEN_BATCH()").collect()
                                st.info(f"Batch: {batch_result[0][0]}")
                                ai_result = session.sql("CALL AML_SCREENING.PIPELINE.RUN_AI_ADJUDICATOR()").collect()
                                st.info(f"AI Adjudicator: {ai_result[0][0]}")

        except Exception as e:
            st.error(f"Error reading CSV: {e}")

st.markdown("<br>", unsafe_allow_html=True)

with st.container(border=True):
    st.markdown("<h4 style='margin:0 0 16px 0;'>Recent Submissions</h4>", unsafe_allow_html=True)
    recent_df = session.sql("""
        SELECT
            SCREENING_REQUEST_ID AS ID,
            FIRST_NAME, MIDDLE_NAME, LAST_NAME,
            DATE_OF_BIRTH AS DOB,
            COUNTRY, GENDER,
            CARD_REQUESTED,
            SOURCE_SYSTEM,
            SUBMITTED_AT
        FROM AML_SCREENING.PIPELINE.INCOMING_SCREENINGS
        ORDER BY SUBMITTED_AT DESC
        LIMIT 10
    """).to_pandas()

    if recent_df.empty:
        st.caption("No screening requests submitted yet.")
    else:
        st.dataframe(recent_df, use_container_width=True, hide_index=True)
