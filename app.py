from components.db import init_db, update_db_schema
from components.ui import render_ui
import streamlit as st
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="SQL Server Monitor",
    layout="wide",
    page_icon="🎯"
)


# Initialize the app and update schema if needed
init_db()
update_db_schema()

# Initialize session state
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0
    st.session_state.last_refresh = datetime.now()

render_ui()
