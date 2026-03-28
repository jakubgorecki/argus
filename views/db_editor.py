import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Database Browser & Editor")
st.caption("Live access to Argus Compliance SQL tables for validation and debugging.")

tables_df = session.sql("SHOW TABLES IN AML_SCREENING.ARGUS").to_pandas()
tables = tables_df['name'].tolist() if not tables_df.empty else []

if not tables:
    st.warning("No tables found in the database.")
else:
    selected_table = st.selectbox("Select Table to View/Edit", tables)
    
    df = session.sql(f"SELECT * FROM AML_SCREENING.ARGUS.{selected_table}").to_pandas()
    
    st.markdown(f"### Table: `{selected_table}`")
    
    edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True)
    
    col1, col2 = st.columns([1, 4])
    with col1:
        save_btn = st.button("Save Changes", type="primary", use_container_width=True)
    
    if save_btn:
        try:
            session.sql(f"TRUNCATE TABLE AML_SCREENING.ARGUS.{selected_table}").collect()
            snowpark_df = session.create_dataframe(edited_df)
            snowpark_df.write.mode("append").save_as_table(f"AML_SCREENING.ARGUS.{selected_table}")
            st.success(f"Changes saved to `{selected_table}` successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"Error saving changes: {e}")
