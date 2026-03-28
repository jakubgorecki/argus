import streamlit as st

def render_topbar():
    # Topbar using streamlit columns
    # [Tool Name]         [ Search Bar ] [ Icons/Profile ]
    st.markdown("""
        <style>
        .topbar { margin-top: -40px; margin-bottom: 20px; }
        .tool-name { font-weight: 800; font-size: 20px; color: #4A192C; }
        .user-block { display: flex; align-items: center; justify-content: flex-end; gap: 12px; }
        .avatar { background-color: #4A192C; color: white; border-radius: 50%; width: 32px; height: 32px; display: flex; justify-content: center; align-items: center; font-size: 14px; font-weight: bold;}
        </style>
    """, unsafe_allow_html=True)
    
    col_logo, col_search, col_profile = st.columns([1.5, 2, 1])
    
    with col_logo:
        st.markdown('<div class="topbar tool-name">👁️ ARGUS PLATFORM</div>', unsafe_allow_html=True)
    
    with col_search:
        # A simple search box utilizing streamlits native text input, hidden label
        st.text_input("Search", placeholder="Search cases, entities, or investigators...", label_visibility="collapsed")
        
    with col_profile:
        st.markdown("""
            <div class="topbar user-block">
                <span>✔️</span> <span>🔔</span>
                <span style="font-size:12px; font-weight:600;">C. Officer</span>
                <div class="avatar">CO</div>
            </div>
        """, unsafe_allow_html=True)

def render_breadcrumbs(current_page: str):
    # Native Streamlit Breadcrumb implementation (visual)
    st.caption(f"Enfuce Compliance / **{current_page}**")
