import streamlit as st
import pandas as pd
import sqlite3
import os

def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "..", "argus.db")
    return sqlite3.connect(db_path)

st.title("Database Browser & Editor")
st.caption("Live access to Argus Compliance SQL tables for validation and debugging.")

conn = get_db_connection()
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]

if not tables:
    st.warning("No tables found in the database.")
else:
    selected_table = st.selectbox("Select Table to View/Edit", tables)
    
    # Load data
    df = pd.read_sql(f"SELECT * FROM {selected_table}", conn)
    
    st.markdown(f"### Table: `{selected_table}`")
    
    # Editable dataframe
    edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True)
    
    col1, col2 = st.columns([1, 4])
    with col1:
        save_btn = st.button("Save Changes", type="primary", use_container_width=True)
    
    if save_btn:
        try:
            # Overwrite the table with edited data
            edited_df.to_sql(selected_table, conn, if_exists="replace", index=False)
            st.success(f"Changes saved to `{selected_table}` successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Error saving changes: {e}")

conn.close()
