from components.db import init_db
from components.ui import render_ui
import streamlit as st
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="SQL Server Monitor",
    layout="wide",
    page_icon="🎯"
)


# Initialize the app
init_db()

# Initialize session state
if 'refresh_counter' not in st.session_state:
    st.session_state.refresh_counter = 0
    st.session_state.last_refresh = datetime.now()

render_ui()
