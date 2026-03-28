import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("DB Admin")
st.caption("Pipeline configuration, sanctions management, audit trail, and table inspection.")

tab_settings, tab_snapshot, tab_audit, tab_browser = st.tabs([
    "Pipeline Settings", "Sanctions Snapshot", "Audit Log", "Table Browser"
])

with tab_settings:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>tune</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Pipeline Configuration</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Adjust screening thresholds and AI adjudicator parameters.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    settings_df = session.sql("""
        SELECT SETTING_KEY, SETTING_VALUE, DESCRIPTION, UPDATED_AT, UPDATED_BY
        FROM AML_SCREENING.PIPELINE.PIPELINE_SETTINGS
        ORDER BY SETTING_KEY
    """).to_pandas()

    if settings_df.empty:
        st.warning("No pipeline settings found.")
    else:
        with st.form("settings_form", border=True):
            new_values = {}
            threshold_keys = [
                'NAME_SIM_NO_MATCH_THRESHOLD', 'NAME_SIM_HIGH_THRESHOLD',
                'NAME_SIM_ONE_WAY_GATE', 'DOB_CORROBORATION_THRESHOLD',
                'COUNTRY_CORROBORATION_THRESHOLD'
            ]
            numeric_keys = [
                'DOB_YEAR_GAP_MAX', 'MIN_TOKEN_PAIR_PENALTY_THRESHOLD',
                'TOP_N_MATCHES', 'AI_TEMPERATURE', 'AI_MAX_TOKENS'
            ]
            text_keys = ['AI_MODEL']

            st.markdown("##### Screening Thresholds")
            cols = st.columns(3)
            for idx, key in enumerate(threshold_keys):
                row = settings_df[settings_df['SETTING_KEY'] == key]
                if not row.empty:
                    cur_val = float(row['SETTING_VALUE'].iloc[0])
                    desc = row['DESCRIPTION'].iloc[0]
                    with cols[idx % 3]:
                        new_values[key] = st.number_input(
                            key.replace('_', ' ').title(),
                            value=cur_val, min_value=0.0, max_value=1.0, step=0.05,
                            help=desc
                        )

            st.markdown("##### Numeric Parameters")
            cols2 = st.columns(3)
            for idx, key in enumerate(numeric_keys):
                row = settings_df[settings_df['SETTING_KEY'] == key]
                if not row.empty:
                    cur_val = row['SETTING_VALUE'].iloc[0]
                    desc = row['DESCRIPTION'].iloc[0]
                    with cols2[idx % 3]:
                        if key in ('TOP_N_MATCHES', 'AI_MAX_TOKENS'):
                            new_values[key] = str(st.number_input(
                                key.replace('_', ' ').title(),
                                value=int(float(cur_val)), min_value=1, step=1,
                                help=desc
                            ))
                        else:
                            new_values[key] = str(st.number_input(
                                key.replace('_', ' ').title(),
                                value=float(cur_val), min_value=0.0, step=0.01,
                                help=desc
                            ))

            st.markdown("##### AI Model")
            for key in text_keys:
                row = settings_df[settings_df['SETTING_KEY'] == key]
                if not row.empty:
                    cur_val = row['SETTING_VALUE'].iloc[0]
                    desc = row['DESCRIPTION'].iloc[0]
                    new_values[key] = st.text_input(
                        key.replace('_', ' ').title(),
                        value=cur_val, help=desc
                    )

            st.markdown("<hr style='margin:16px 0; border:none; border-top:1px solid #EFEBEB;'>", unsafe_allow_html=True)
            save_settings = st.form_submit_button("Save Settings", type="primary")

        if save_settings:
            changed = 0
            for key, new_val in new_values.items():
                row = settings_df[settings_df['SETTING_KEY'] == key]
                if not row.empty:
                    old_val = row['SETTING_VALUE'].iloc[0]
                    try:
                        changed_val = float(new_val) != float(old_val)
                    except (ValueError, TypeError):
                        changed_val = str(new_val).strip() != str(old_val).strip()
                    if changed_val:
                        safe_val = str(new_val).replace("'", "''")
                        session.sql(f"""
                            UPDATE AML_SCREENING.PIPELINE.PIPELINE_SETTINGS
                            SET SETTING_VALUE = '{safe_val}', UPDATED_AT = CURRENT_TIMESTAMP(), UPDATED_BY = CURRENT_USER()
                            WHERE SETTING_KEY = '{key}'
                        """).collect()
                        changed += 1
            if changed > 0:
                st.success(f"Updated {changed} setting(s).")
                st.rerun()
            else:
                st.info("No changes detected.")

        with st.expander("Current Settings (raw)"):
            st.dataframe(settings_df, use_container_width=True, hide_index=True)

with tab_snapshot:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>security</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Sanctions List Snapshot</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Current sanctions data and snapshot management.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    snap_df = session.sql("""
        SELECT SNAPSHOT_VERSION, SNAPSHOT_HASH, MAX(SNAPSHOT_TIMESTAMP) AS SNAPSHOT_TIMESTAMP, COUNT(*) AS ENTITY_COUNT
        FROM AML_SCREENING.PIPELINE.SANCTIONS_LIST_SNAPSHOT
        GROUP BY SNAPSHOT_VERSION, SNAPSHOT_HASH
        ORDER BY SNAPSHOT_TIMESTAMP DESC
        LIMIT 1
    """).to_pandas()

    unprocessed = session.sql("""
        SELECT COUNT(*) AS CNT
        FROM AML_SCREENING.PIPELINE.INCOMING_SCREENINGS i
        WHERE i.SCREENING_REQUEST_ID NOT IN (
            SELECT SCREENING_REQUEST_ID FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
        )
    """).to_pandas()['CNT'].iloc[0]

    last_batch = session.sql("""
        SELECT CREATED_AT, DETAILS
        FROM AML_SCREENING.PIPELINE.AUDIT_LOG
        WHERE EVENT_TYPE = 'BATCH_SCREENING_COMPLETED'
        ORDER BY CREATED_AT DESC LIMIT 1
    """).to_pandas()

    with st.container(border=True):
        if not snap_df.empty:
            s = snap_df.iloc[0]
            sc1, sc2, sc3, sc4 = st.columns(4)
            with sc1:
                st.metric("Snapshot Version", s['SNAPSHOT_VERSION'])
            with sc2:
                st.metric("Entities", int(s['ENTITY_COUNT']))
            with sc3:
                ts = str(s['SNAPSHOT_TIMESTAMP'])[:19]
                st.metric("Last Updated", ts)
            with sc4:
                st.metric("Pending Records", int(unprocessed))
        else:
            st.warning("No sanctions snapshot found.")

    if not last_batch.empty:
        lb = last_batch.iloc[0]
        st.caption(f"Last pipeline run: **{str(lb['CREATED_AT'])[:19]}**")

    snap_col1, snap_col2 = st.columns([1, 1])
    with snap_col1:
        if st.button("Refresh Sanctions Snapshot", icon=":material/refresh:", use_container_width=True):
            with st.spinner("Refreshing sanctions snapshot..."):
                result = session.sql("CALL AML_SCREENING.PIPELINE.REFRESH_SANCTIONS_SNAPSHOT()").collect()
                st.success(f"Result: {result[0][0]}")
                st.rerun()
    with snap_col2:
        if st.button("Run Pipeline Now", icon=":material/play_arrow:", type="primary", use_container_width=True):
            with st.spinner("Running pipeline..."):
                batch = session.sql("CALL AML_SCREENING.PIPELINE.SCREEN_BATCH()").collect()
                st.toast(f"Batch: {batch[0][0]}")
                ai = session.sql("CALL AML_SCREENING.PIPELINE.RUN_AI_ADJUDICATOR()").collect()
                st.toast(f"AI: {ai[0][0]}")
            st.success("Pipeline completed.")

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("Browse Sanctions Entities"):
        sanctions_preview = session.sql("""
            SELECT ENTITY_NAME, ENTITY_ALIASES, DOB, POB, LISTING_COUNTRY, LIST_NAME, LIST_ABBREVIATION
            FROM AML_SCREENING.PIPELINE.SANCTIONS_LIST_SNAPSHOT
            ORDER BY ENTITY_NAME
        """).to_pandas()
        st.dataframe(sanctions_preview, use_container_width=True, hide_index=True)

with tab_audit:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>history</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Audit Log</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Full event history for compliance and debugging.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    event_types = session.sql("""
        SELECT DISTINCT EVENT_TYPE FROM AML_SCREENING.PIPELINE.AUDIT_LOG ORDER BY EVENT_TYPE
    """).to_pandas()['EVENT_TYPE'].tolist()

    af1, af2 = st.columns([2, 2])
    with af1:
        selected_event = st.selectbox("Event Type", ["All Events"] + event_types)
    with af2:
        audit_limit = st.selectbox("Show", [25, 50, 100, 250], index=1)

    where_clause = ""
    if selected_event != "All Events":
        where_clause = f"WHERE EVENT_TYPE = '{selected_event}'"

    audit_df = session.sql(f"""
        SELECT AUDIT_ID, EVENT_TYPE, DETAILS, CREATED_AT, CREATED_BY
        FROM AML_SCREENING.PIPELINE.AUDIT_LOG
        {where_clause}
        ORDER BY CREATED_AT DESC
        LIMIT {audit_limit}
    """).to_pandas()

    if audit_df.empty:
        st.info("No audit log entries found.")
    else:
        st.markdown(f"**{len(audit_df)}** entries shown.")
        for _, row in audit_df.iterrows():
            evt = row['EVENT_TYPE']
            ts = str(row['CREATED_AT'])[:19]
            user = row['CREATED_BY']
            icon_map = {
                'BATCH_SCREENING_COMPLETED': 'batch_prediction',
                'AI_ADJUDICATOR_RUN': 'auto_awesome',
                'HUMAN_REVIEW': 'rate_review',
                'SANCTIONS_SNAPSHOT_SKIPPED': 'skip_next',
                'SANCTIONS_SNAPSHOT_REFRESHED': 'refresh',
            }
            icon = icon_map.get(evt, 'event_note')

            with st.expander(f"**{evt}** — {ts} — {user}"):
                details = row['DETAILS']
                if isinstance(details, str):
                    try:
                        import json
                        details = json.loads(details)
                        st.json(details)
                    except Exception:
                        st.code(details)
                elif isinstance(details, dict):
                    st.json(details)
                else:
                    st.code(str(details))

with tab_browser:
    st.markdown("""
        <div style='display:flex; align-items:center; gap:12px; margin-bottom:8px;'>
            <div style='background-color:#4A192C; color:white; padding:10px; border-radius:10px; width:fit-content;'>
                <span class='material-symbols-rounded' style='font-size:20px;'>database</span>
            </div>
            <div>
                <h4 style='margin:0; font-size:18px; color:var(--argus-text-dark);'>Table Browser</h4>
                <p style='margin:0; font-size:13px; color:var(--argus-text-muted);'>Read-only inspection of all AML Screening tables.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)

    tables_info = session.sql("""
        SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT
        FROM AML_SCREENING.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_SCHEMA, TABLE_NAME
    """).to_pandas()

    if tables_info.empty:
        st.warning("No tables found.")
    else:
        table_options = [f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']} ({int(r['ROW_COUNT'])} rows)" for _, r in tables_info.iterrows()]
        table_keys = [f"AML_SCREENING.{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}" for _, r in tables_info.iterrows()]

        selected_idx = st.selectbox("Select Table", range(len(table_options)), format_func=lambda i: table_options[i])
        selected_fqn = table_keys[selected_idx]

        browse_limit = st.selectbox("Row Limit", [50, 100, 500, 1000], index=0, key="browse_limit")

        browse_df = session.sql(f"SELECT * FROM {selected_fqn} LIMIT {browse_limit}").to_pandas()
        st.markdown(f"**{len(browse_df)}** row(s) displayed from `{selected_fqn}`")
        st.dataframe(browse_df, use_container_width=True, hide_index=True)

        csv_browse = browse_df.to_csv(index=False)
        st.download_button(
            "Download as CSV",
            data=csv_browse,
            file_name=f"{selected_fqn.replace('.', '_')}.csv",
            mime="text/csv",
            icon=":material/download:",
        )
