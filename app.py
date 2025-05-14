from components.db import init_db
from components.ui import render_ui
import streamlit as st

st.set_page_config(page_title="SQL Server Monitor", layout="wide")


init_db()
render_ui()
