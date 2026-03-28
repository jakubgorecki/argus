import streamlit as st

def render_topbar():
    st.markdown("<div id='argus-topbar'></div>", unsafe_allow_html=True)
    
    col_search, col_spacer, col_profile = st.columns(
        [30, 40, 30], 
        vertical_alignment="center"
    )
    
    with col_search:
        st.text_input(
            "Search input",
            placeholder="Search cases, entities, or investigators...",
            label_visibility="collapsed",
            icon=":material/search:"
        )
        
    with col_profile:
        st.markdown("""
            <div style='display:flex; justify-content:flex-end; align-items:center; gap:20px;'>
                <div style='display:flex; align-items:center; gap:16px; color:#4A5568;'>
                    <span class='material-symbols-rounded' style='font-size:26px;'>check_circle</span>
                    <div style='position:relative; display:flex; align-items:center;'>
                        <span class='material-symbols-rounded' style='font-size:26px;'>notifications</span>
                        <div style='position:absolute; top:2px; right:2px; width:8px; height:8px; background-color:#E53E3E; border-radius:50%; border:1px solid #FFF;'></div>
                    </div>
                    <span class='material-symbols-rounded' style='font-size:26px;'>help</span>
                </div>
                <div style='border-left:1px solid #EFEBEB; height:32px;'></div>
                <div style='text-align:right; display:flex; flex-direction:column; justify-content:center;'>
                    <div style='font-weight:700; font-size:14px; color:#2D1A22; padding-bottom:2px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;'>Julian Thome</div>
                    <div style='font-size:10px; color:#8C7C83; font-weight:600; letter-spacing:0.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px;'>SENIOR COMPLIANCE</div>
                </div>
                <img src='https://res.cloudinary.com/yevhenii-kalashnyk/image/upload/ar_1:1,c_crop,g_face,z_0.9/IMG_9012_what0m.jpg' style='width:36px; height:36px; min-width:36px; min-height:36px; flex-shrink:0; border-radius:50%; object-fit:cover; border:1px solid #2D1A22;'>
            </div>
        """, unsafe_allow_html=True)
        
    st.markdown("<hr style='margin-top: 8px; margin-bottom: 24px; border-top: 1px solid #EFEBEB;'/>", unsafe_allow_html=True)

def render_breadcrumbs(current_page: str):
    # Native Streamlit SPA routing component ensuring no hard reloads and no new tabs
    st.page_link("views/dashboard.py", label=f"**ARGUS** &nbsp;›&nbsp; {current_page}")
