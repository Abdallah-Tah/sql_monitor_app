import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
from components.sql import (
    get_databases, get_tables, check_selected_tables, get_job_history,
    get_active_jobs, get_all_jobs, get_windows_user, get_job_details, get_job_steps
)
from components.db import (
    save_table_config, load_saved_table_config, log_table_check_result, get_latest_log,
    save_job_config, load_saved_job_config, log_job_check_result, delete_table_config,
    delete_job_config
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


def show_notifications(table_results, job_results):
    # Check for failed or empty tables
    failed_tables = [r for r in table_results if r['Status']
                     in ['Empty', 'Error']] if table_results else []

    # Check for failed jobs
    failed_jobs = [r for r in job_results if r['Status']
                   == 'Failed'] if job_results else []

    if failed_tables or failed_jobs:
        with st.sidebar:
            st.header("🚨 Notifications")

            if failed_tables:
                st.subheader("Table Issues")
                for table in failed_tables:
                    if table['Status'] == 'Empty':
                        st.warning(
                            f"⚠️ Empty Table: {table['Database']}.{table['Table']}")
                    else:
                        st.error(
                            f"❌ Table Error: {table['Database']}.{table['Table']} - {table['Status']}")

            if failed_jobs:
                st.subheader("Failed Jobs")
                for job in failed_jobs:
                    st.error(
                        f"❌ Job Failed: {job['Job Name']} at {job['Run Date']} {job['Run Time']}")
                    if job.get('Message'):
                        with st.expander("Error Details"):
                            st.text(job['Message'])


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

    results = []
    with col2:
        st.subheader("Monitored Tables Status")
        saved_tables = load_saved_table_config()

        if not saved_tables.empty:
            # Create a container for the table list
            with st.container():
                for _, row in saved_tables.iterrows():
                    col_info, col_remove = st.columns([5, 1])
                    result = check_selected_tables(
                        row["db_name"], [row["table_name"]])
                    count = int(
                        result.iloc[0]["Rows"]) if result.iloc[0]["Rows"].isdigit() else 0
                    status = result.iloc[0]["Status"]

                    # Display table information
                    with col_info:
                        st.write(
                            f"{row['db_name']}.{row['table_name']} - {status} (Rows: {count})")

                    # Add remove button
                    with col_remove:
                        if st.button("🗑️", key=f"remove_table_{row['db_name']}_{row['table_name']}"):
                            delete_table_config(
                                row["db_name"], row["table_name"])
                            st.experimental_rerun()

                    results.append({
                        'Database': row["db_name"],
                        'Table': row["table_name"],
                        'Row Count': count,
                        'Status': status,
                        'Last Check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })

            if results:
                st.markdown("### Detailed Status")
                status_df = pd.DataFrame(results)
                st.dataframe(apply_status_colors(
                    status_df, 'Status'), use_container_width=True)
        else:
            st.info(
                "No tables selected for monitoring. Please configure tables above.")

    return results


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

    job_results = []
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
                st.subheader("Monitored Jobs")
                monitored_jobs = all_jobs[all_jobs['Job Name'].isin(
                    saved_job_names)]

                # Create a container for the job list with remove buttons
                with st.container():
                    for idx, (_, row) in enumerate(monitored_jobs.iterrows()):
                        col_info, col_remove = st.columns([5, 1])

                        with col_info:
                            status_color = {
                                'Running': '🔵',
                                'Failed': '🔴',
                                'Succeeded': '🟢',
                                'Enabled': '🟢',
                                'Disabled': '⚪',
                                'Canceled': '🟠',
                                'Retry': '🟡'
                            }.get(row['Status'], '⚪')
                            st.write(
                                f"{status_color} {row['Job Name']} ({row['Status']})")

                        with col_remove:
                            # Using index in the key to ensure uniqueness
                            if st.button("🗑️", key=f"remove_job_{idx}"):
                                delete_job_config(row['Job Name'])
                                st.experimental_rerun()

                st.markdown("### Detailed Status")
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
                job_results = filtered_history.to_dict('records')

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

    return job_results


def render_dashboard_view():
    # Show last refresh time and manual refresh button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown(
            f"Last updated: {st.session_state.last_refresh.strftime('%Y-%m-%d %H:%M:%S')}")
    with col2:
        if st.button("🔄 Refresh Data"):
            st.session_state.last_refresh = datetime.now()
            st.session_state.refresh_counter += 1
            st.cache_data.clear()
            st.experimental_rerun()

    st.markdown("""
        <style>
            .stMetricValue {
                font-size: 4rem !important;
            }
            .stMetricLabel {
                font-size: 1.2rem !important;
            }
            section[data-testid="stSidebar"] {
                width: 350px !important;
            }
            .reportview-container .main .block-container {
                padding-top: 0rem;
                padding-bottom: 0rem;
                max-width: 95%;
            }
        </style>
    """, unsafe_allow_html=True)

    # Get all monitored data
    saved_tables = load_saved_table_config()
    saved_jobs = load_saved_job_config()
    all_jobs = get_all_jobs()
    active_jobs = get_active_jobs()
    job_history = get_job_history(24)  # Last 24 hours

    # Job Statistics
    monitored_jobs = all_jobs[all_jobs['Job Name'].isin(
        saved_jobs['job_name'])] if not saved_jobs.empty else pd.DataFrame()
    running_jobs = len(active_jobs) if not active_jobs.empty else 0

    if not job_history.empty:
        recent_failed = len(job_history[job_history['Status'] == 'Failed'])
        recent_succeeded = len(
            job_history[job_history['Status'] == 'Succeeded'])
    else:
        recent_failed = 0
        recent_succeeded = 0

    # Table Statistics
    table_results = []
    if not saved_tables.empty:
        for _, row in saved_tables.iterrows():
            result = check_selected_tables(row["db_name"], [row["table_name"]])
            count = int(result.iloc[0]["Rows"]
                        ) if result.iloc[0]["Rows"].isdigit() else 0
            status = result.iloc[0]["Status"]
            table_results.append({
                'Database': row["db_name"],
                'Table': row["table_name"],
                'Row Count': count,
                'Status': status
            })

    table_stats = pd.DataFrame(
        table_results) if table_results else pd.DataFrame()
    empty_tables = len(
        table_stats[table_stats['Status'] == 'Empty']) if not table_stats.empty else 0
    error_tables = len(table_stats[table_stats['Status'].str.startswith(
        'Error')]) if not table_stats.empty else 0
    healthy_tables = len(
        table_stats[table_stats['Status'] == 'OK']) if not table_stats.empty else 0

    # Dashboard Layout
    st.markdown("## 📊 System Overview")

    # Job Metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("🏃 Running Jobs", running_jobs, delta=None)
    with col2:
        st.metric("❌ Failed Jobs (24h)", recent_failed,
                  delta=f"{recent_failed} jobs" if recent_failed > 0 else None,
                  delta_color="inverse")
    with col3:
        st.metric("✅ Successful Jobs (24h)", recent_succeeded)
    with col4:
        total_jobs = len(monitored_jobs) if not monitored_jobs.empty else 0
        st.metric("📋 Total Monitored Jobs", total_jobs)

    # Table Metrics
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("✅ Healthy Tables", healthy_tables)
    with col2:
        st.metric("⚠️ Empty Tables", empty_tables,
                  delta=f"{empty_tables} tables" if empty_tables > 0 else None,
                  delta_color="inverse")
    with col3:
        st.metric("❌ Error Tables", error_tables,
                  delta=f"{error_tables} tables" if error_tables > 0 else None,
                  delta_color="inverse")

    # Active Jobs Section
    st.markdown("---")
    st.markdown("### 🔄 Currently Running Jobs")
    if not active_jobs.empty:
        for _, job in active_jobs.iterrows():
            with st.container():
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**{job['Job Name']}**")
                    st.text(f"Step: {job['Step Name'] or 'N/A'}")
                with col2:
                    st.markdown(f"Duration: {job['Duration (mins)']} mins")
    else:
        st.info("No jobs currently running")

    # Recent Problems Section
    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### ❌ Recent Job Failures")
        if not job_history.empty:
            failed_jobs = job_history[job_history['Status'] == 'Failed'].head(
                5)
            if not failed_jobs.empty:
                for _, job in failed_jobs.iterrows():
                    st.error(
                        f"{job['Job Name']} - {job['Run Date']} {job['Run Time']}")
            else:
                st.success("No recent job failures")
        else:
            st.info("No job history available")

    with col2:
        st.markdown("### ⚠️ Table Issues")
        if not table_stats.empty:
            problem_tables = table_stats[
                (table_stats['Status'] == 'Empty') |
                (table_stats['Status'].str.startswith('Error'))
            ]
            if not problem_tables.empty:
                for _, table in problem_tables.iterrows():
                    st.warning(
                        f"{table['Database']}.{table['Table']} - {table['Status']}"
                    )
            else:
                st.success("No table issues detected")
        else:
            st.info("No tables being monitored")


def render_config_view():
    tab1, tab2 = st.tabs(["📊 Table Monitor", "🔄 Job Monitor"])

    with tab1:
        render_table_monitor()

    with tab2:
        render_job_monitor()


def init_session_state():
    if 'table_results' not in st.session_state:
        st.session_state.table_results = []
    if 'job_results' not in st.session_state:
        st.session_state.job_results = []
    if 'view_mode' not in st.session_state:
        st.session_state.view_mode = "📺 Dashboard View"
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    if 'refresh_counter' not in st.session_state:
        st.session_state.refresh_counter = 0


def render_ui():
    # Initialize session state
    init_session_state()

    windows_user = get_windows_user()

    # Create a sidebar for navigation
    with st.sidebar:
        st.title("🎯 SQL Monitor")
        st.info(f"Connected as: {windows_user}")
        st.session_state.view_mode = st.radio(
            "View Mode",
            ["📺 Dashboard View", "⚙️ Configuration"],
            index=0 if st.session_state.view_mode == "📺 Dashboard View" else 1
        )

    if st.session_state.view_mode == "📺 Dashboard View":
        # Get latest data for dashboard
        table_results = get_latest_table_results()
        job_results = get_latest_job_results()

        # Update session state
        st.session_state.table_results = table_results
        st.session_state.job_results = job_results

        # Show notifications and render dashboard
        show_notifications(table_results, job_results)
        render_dashboard_view()
    else:
        render_config_view()


def get_latest_table_results():
    saved_tables = load_saved_table_config()
    results = []

    if not saved_tables.empty:
        for _, row in saved_tables.iterrows():
            result = check_selected_tables(row["db_name"], [row["table_name"]])
            count = int(result.iloc[0]["Rows"]
                        ) if result.iloc[0]["Rows"].isdigit() else 0
            status = result.iloc[0]["Status"]
            results.append({
                'Database': row["db_name"],
                'Table': row["table_name"],
                'Row Count': count,
                'Status': status,
                'Last Check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

    return results


def get_latest_job_results():
    saved_jobs = load_saved_job_config()
    if not saved_jobs.empty:
        job_history = get_job_history(24)  # Last 24 hours
        return job_history[job_history['Job Name'].isin(saved_jobs['job_name'])].to_dict('records')
    return []
