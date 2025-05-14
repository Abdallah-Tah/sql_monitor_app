import streamlit as st
import pandas as pd
from datetime import datetime
from components.sql import (
    get_databases, get_tables, check_selected_tables, get_job_history,
    get_active_jobs, get_all_jobs, get_windows_user, get_job_details, get_job_steps
)
from components.db import (
    save_table_config, load_saved_table_config, log_table_check_result, get_latest_log,
    save_job_config, load_saved_job_config, log_job_check_result
)


def apply_status_colors(df, status_column):
    def color_status(val):
        colors = {
            'Running': 'blue',
            'Succeeded': 'green',
            'Failed': 'red',
            'Disabled': 'gray',
            'Enabled': 'green',
            'Canceled': 'orange',
            'Retry': 'yellow',
            'In Progress': 'blue',
            'OK': 'green',
            'Empty': 'yellow'
        }
        return f'color: {colors.get(val, "black")}'

    return df.style.apply(
        lambda x: [color_status(val) if col == status_column else ''
                   for col, val in x.items()], axis=1
    )


def render_table_monitor():
    st.header("📊 Database Table Monitor")

    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Configuration")
        dbs = get_databases()
        selected_db = st.selectbox(
            "Choose Database", dbs, key="table_db_select")

        if selected_db:
            tables = get_tables(selected_db)
            selected_tables = st.multiselect(
                "Select Tables", tables, key="table_select")
            if st.button("Save Selected Tables", key="save_tables"):
                save_table_config(selected_db, selected_tables)
                st.success("Configuration saved.")

    with col2:
        st.subheader("Monitored Tables Status")
        saved_tables = load_saved_table_config()

        if not saved_tables.empty:
            results = []
            for _, row in saved_tables.iterrows():
                result = check_selected_tables(
                    row["db_name"], [row["table_name"]])
                count = int(
                    result.iloc[0]["Rows"]) if result.iloc[0]["Rows"].isdigit() else 0
                log_table_check_result(
                    row["db_name"], row["table_name"], count, result.iloc[0]["Status"])
                results.append({
                    'Database': row["db_name"],
                    'Table': row["table_name"],
                    'Row Count': count,
                    'Status': result.iloc[0]["Status"],
                    'Last Check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })

            status_df = pd.DataFrame(results)
            st.dataframe(apply_status_colors(status_df, 'Status'),
                         use_container_width=True)
        else:
            st.info(
                "No tables selected for monitoring. Please configure tables above.")


def render_job_details(job_name):
    details = get_job_details(job_name)
    if details:
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Job Information")
            info_df = pd.DataFrame([details])
            st.dataframe(apply_status_colors(
                info_df, 'Current Status'), use_container_width=True)

        with col2:
            st.markdown("#### Job Steps")
            steps_df = get_job_steps(job_name)
            if not steps_df.empty:
                st.dataframe(apply_status_colors(
                    steps_df, 'Last Status'), use_container_width=True)
            else:
                st.info("No steps found for this job")


def render_job_monitor():
    st.header("🔄 SQL Server Job Monitor")

    tab1, tab2, tab3 = st.tabs(
        ["📋 Job Selection", "▶️ Active Jobs", "📈 Job History"])

    with tab1:
        col1, col2 = st.columns([2, 3])

        with col1:
            st.subheader("Select Jobs to Monitor")
            all_jobs = get_all_jobs()
            saved_jobs = load_saved_job_config()
            saved_job_names = saved_jobs['job_name'].tolist(
            ) if not saved_jobs.empty else []

            job_names = all_jobs['Job Name'].tolist()
            selected_jobs = st.multiselect(
                "Select Jobs", job_names, default=saved_job_names)

            if st.button("Save Selected Jobs", key="save_jobs"):
                save_job_config(selected_jobs)
                st.success("Job configuration saved.")

        with col2:
            if saved_job_names:
                st.subheader("Monitored Jobs Overview")
                monitored_jobs = all_jobs[all_jobs['Job Name'].isin(
                    saved_job_names)]
                st.dataframe(apply_status_colors(monitored_jobs,
                             'Status'), use_container_width=True)

    with tab2:
        st.subheader("Currently Running Jobs")
        active_jobs = get_active_jobs()
        if not active_jobs.empty:
            st.dataframe(active_jobs, use_container_width=True)

            # Show details for running jobs
            for _, job in active_jobs.iterrows():
                with st.expander(f"Details: {job['Job Name']}", expanded=True):
                    render_job_details(job['Job Name'])
        else:
            st.info("No jobs are currently running")

    with tab3:
        st.subheader("Job History")
        saved_jobs = load_saved_job_config()
        if not saved_jobs.empty:
            col1, col2 = st.columns([1, 3])

            with col1:
                hours = st.slider("Time Range (hours)", 1, 72, 24)
                job_history = get_job_history(hours)
                filtered_history = job_history[job_history['Job Name'].isin(
                    saved_jobs['job_name'])]

                status_options = filtered_history['Status'].unique().tolist()
                selected_status = st.multiselect(
                    "Filter by Status",
                    status_options,
                    default=status_options,
                    key="job_history_status"
                )

            with col2:
                filtered_history = filtered_history[filtered_history['Status'].isin(
                    selected_status)]
                if not filtered_history.empty:
                    st.dataframe(apply_status_colors(
                        filtered_history, 'Status'), use_container_width=True)
                else:
                    st.info("No job history found for the selected criteria")
        else:
            st.info(
                "No jobs selected for monitoring. Please configure jobs in the Job Selection tab.")


def render_ui():
    # Header with authentication info
    st.title("🎯 SQL Server Monitor Dashboard")
    windows_user = get_windows_user()
    st.info(f"Connected using Windows Authentication (User: {windows_user})")

    # Main navigation
    tab1, tab2 = st.tabs(["📊 Table Monitor", "🔄 Job Monitor"])

    with tab1:
        render_table_monitor()

    with tab2:
        render_job_monitor()
