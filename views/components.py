import streamlit as st
from snowflake.snowpark.context import get_active_session

def _do_search(query):
    session = get_active_session()
    q = query.strip()
    if not q:
        return None, "Please enter a search term."

    exact = session.sql(f"SELECT RESULT_ID FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS WHERE RESULT_ID = '{q.replace(chr(39), chr(39)+chr(39))}'").to_pandas()
    if len(exact) == 1:
        return exact['RESULT_ID'].iloc[0], None

    like_q = q.replace("'", "''")
    matches = session.sql(f"""
        SELECT RESULT_ID, FULL_NAME_SCREENED
        FROM AML_SCREENING.PIPELINE.SCREENING_RESULTS
        WHERE RESULT_ID ILIKE '%{like_q}%'
           OR FULL_NAME_SCREENED ILIKE '%{like_q}%'
        ORDER BY SCREENED_AT DESC
        LIMIT 5
    """).to_pandas()

    if len(matches) == 1:
        return matches['RESULT_ID'].iloc[0], None
    elif len(matches) > 1:
        return matches['RESULT_ID'].iloc[0], None
    else:
        return None, f"No cases found for \"{q}\"."

def render_topbar():
    st.markdown("<div id='argus-topbar'></div>", unsafe_allow_html=True)

    if st.session_state.get("_search_navigate"):
        case_id = st.session_state.pop("_search_navigate")
        st.query_params["selected_case"] = case_id
        st.query_params["page"] = "cases"
        st.rerun()
    
    col_search, col_spacer, col_profile = st.columns(
        [30, 40, 30], 
        vertical_alignment="center"
    )
    
    with col_search:
        search_val = st.text_input(
            "Search input",
            placeholder="Search by case ID or entity name",
            label_visibility="collapsed",
            icon=":material/search:",
            key="global_case_search",
        )
        if search_val and search_val.strip() and search_val != st.session_state.get("_last_search"):
            st.session_state["_last_search"] = search_val
            result_id, error = _do_search(search_val)
            if error:
                st.toast(error, icon=":material/error:")
            elif result_id:
                st.session_state["_search_navigate"] = result_id
                st.rerun()
        
    with col_profile:
        st.markdown("""
            <div style='display:flex; justify-content:flex-end; align-items:center; gap:20px;'>
                <div style='display:flex; align-items:center; gap:16px; color:var(--argus-text-muted);'>
                    <span class='material-symbols-rounded' style='font-size:26px;'>check_circle</span>
                    <div style='position:relative; display:flex; align-items:center;'>
                        <span class='material-symbols-rounded' style='font-size:26px;'>notifications</span>
                        <div style='position:absolute; top:2px; right:2px; width:8px; height:8px; background-color:#E53E3E; border-radius:50%; border:1px solid var(--argus-card-bg);'></div>
                    </div>
                    <span class='material-symbols-rounded' style='font-size:26px;'>help</span>
                </div>
                <div style='border-left:1px solid var(--argus-border); height:32px;'></div>
                <div style='text-align:right; display:flex; flex-direction:column; justify-content:center;'>
                    <div style='font-weight:700; font-size:14px; color:var(--argus-text-dark); padding-bottom:2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;'>Julian Thome</div>
                    <div style='font-size:10px; color:var(--argus-text-muted); font-weight:600; letter-spacing:0.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;'>SENIOR COMPLIANCE</div>
                </div>
                <img src='https://res.cloudinary.com/yevhenii-kalashnyk/image/upload/ar_1:1,c_crop,g_face,z_0.9/IMG_9012_what0m.jpg' style='width:36px; height:36px; min-width:36px; min-height:36px; flex-shrink:0; border-radius:50%; object-fit:cover; border:1px solid var(--argus-border);'>
            </div>
        """, unsafe_allow_html=True)
        
    st.markdown("<hr style='margin-top: 8px; margin-bottom: 24px; border-top: 1px solid var(--argus-border);'/>", unsafe_allow_html=True)

def render_breadcrumbs(path_data):
    if isinstance(path_data, str):
        path_data = [("ARGUS", "/"), (path_data, None)]
        
    html_parts = []
    for title, link in path_data:
        if link:
            html_parts.append(f"<a href='{link}' target='_self' style='color: var(--argus-text-muted); text-decoration: none; font-weight: 600; padding: 0px 4px;'>{title}</a>")
        else:
            html_parts.append(f"<span style='color: var(--argus-text-dark); font-weight: 600; padding: 0px 4px;'>{title}</span>")
            
    bc_html = f"<span style='color: var(--argus-text-muted); font-size: 16px;'>›</span>".join(html_parts)
    
    st.markdown(f"""
    <div style="font-size: 11px; font-weight: 700; font-family: 'Inter', sans-serif; text-transform: uppercase; letter-spacing: 1px; color: var(--argus-text-muted); padding-left: 4px; margin-bottom: 8px; display: flex; align-items: center; gap: 4px;">
        {bc_html}
    </div>
    """, unsafe_allow_html=True)
