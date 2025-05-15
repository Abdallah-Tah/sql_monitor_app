import streamlit as st
import pandas as pd
import time
from datetime import datetime, timedelta
from components.sql import (
    get_databases, get_tables, check_selected_tables, get_job_history,
    get_active_jobs, get_all_jobs, get_windows_user, get_job_details, get_job_steps,
    get_table_size_info  # Added import
)
from components.db import (
    save_table_config, load_saved_table_config, log_table_check_result, get_latest_log,
    save_job_config, load_saved_job_config, log_job_check_result, delete_table_config,
    delete_job_config, log_alert, get_alerts
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
            'Empty': 'yellow',
            'Warn-LowCount': 'orange',
            'Warn-HighCount': 'purple',
            'Slow': 'orange',
            'Fast': 'purple',
            'Normal': 'green'
        }
        return f'color: {colors.get(val, "black")}'

    return df.style.apply(
        lambda x: [color_status(val) if col == status_column else ''
                   for col, val in x.items()], axis=1
    )


def show_notifications(table_results, job_results):
    # Check for tables with issues
    table_issues = []

    if table_results:
        # Find tables with various issues
        empty_tables = [r for r in table_results if r['Status'] == 'Empty']
        error_tables = [
            r for r in table_results if r['Status'].startswith('Error')]
        low_count_tables = [
            r for r in table_results if r['Status'] == 'Warn-LowCount']
        high_count_tables = [
            r for r in table_results if r['Status'] == 'Warn-HighCount']

        table_issues = empty_tables + error_tables + \
            low_count_tables + high_count_tables

    # Check for failed jobs and duration anomalies
    failed_jobs = []
    slow_jobs = []
    fast_jobs = []

    if job_results:
        failed_jobs = [r for r in job_results if r['Status'] == 'Failed']

        # Look for duration anomalies if available
        if 'Duration Status' in job_results[0] if job_results else {}:
            slow_jobs = [r for r in job_results if r.get(
                'Duration Status') == 'Slow']
            fast_jobs = [r for r in job_results if r.get(
                'Duration Status') == 'Fast']

    if table_issues or failed_jobs or slow_jobs or fast_jobs:
        with st.sidebar:
            st.header("🚨 Notifications")

            if table_issues:
                st.subheader("Table Issues")

                # Group notifications by type for better organization
                if any(t['Status'] == 'Empty' for t in table_issues):
                    st.markdown("#### Empty Tables")
                    for table in [t for t in table_issues if t['Status'] == 'Empty']:
                        st.warning(
                            f"⚠️ Empty Table: {table['Database']}.{table['Table']}")

                if any(t['Status'].startswith('Error') for t in table_issues):
                    st.markdown("#### Error Tables")
                    for table in [t for t in table_issues if t['Status'].startswith('Error')]:
                        st.error(
                            f"❌ Table Error: {table['Database']}.{table['Table']} - {table['Status']}")

                if any(t['Status'] == 'Warn-LowCount' for t in table_issues):
                    st.markdown("#### Low Row Count")
                    for table in [t for t in table_issues if t['Status'] == 'Warn-LowCount']:
                        min_rows = table.get('Min Rows', 'N/A')
                        st.warning(
                            f"⚠️ Low Row Count: {table['Database']}.{table['Table']} - Count: {table['Row Count']}, Min: {min_rows}")

                if any(t['Status'] == 'Warn-HighCount' for t in table_issues):
                    st.markdown("#### High Row Count")
                    for table in [t for t in table_issues if t['Status'] == 'Warn-HighCount']:
                        max_rows = table.get('Max Rows', 'N/A')
                        st.warning(
                            f"⚠️ High Row Count: {table['Database']}.{table['Table']} - Count: {table['Row Count']}, Max: {max_rows}")

            if failed_jobs:
                st.subheader("Failed Jobs")
                for job in failed_jobs:
                    st.error(
                        f"❌ Job Failed: {job['Job Name']} at {job['Run Date']} {job['Run Time']}")
                    if job.get('Message'):
                        with st.expander("Error Details"):
                            st.text(job['Message'])

            # Add new section for duration anomalies
            if slow_jobs or fast_jobs:
                st.subheader("Job Duration Anomalies")

                if slow_jobs:
                    st.markdown("#### Slow Jobs")
                    for job in slow_jobs:
                        st.warning(
                            f"⌛ Slow Job: {job['Job Name']} at {job['Run Date']} {job['Run Time']} - Duration: {job['Duration']}")

                if fast_jobs:
                    st.markdown("#### Fast Jobs")
                    for job in fast_jobs:
                        st.info(
                            f"⚡ Fast Job: {job['Job Name']} at {job['Run Date']} {job['Run Time']} - Duration: {job['Duration']}")


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

            # For each selected table, show threshold configuration
            threshold_settings = {}
            if selected_tables:
                st.subheader("Row Count Thresholds")
                st.info(
                    "Set min/max row count thresholds to monitor. Leave blank for no threshold.")

                # Create dictionaries to store threshold values
                min_rows_dict = {}
                max_rows_dict = {}

                # Get existing thresholds from database for selected tables
                existing_config = load_saved_table_config()

                for table in selected_tables:
                    # Check if table already has thresholds
                    existing_min = None
                    existing_max = None

                    if not existing_config.empty:
                        table_config = existing_config[
                            (existing_config['db_name'] == selected_db) &
                            (existing_config['table_name'] == table)
                        ]

                        if not table_config.empty:
                            existing_min = table_config.iloc[0]['min_rows'] if pd.notna(
                                table_config.iloc[0]['min_rows']) else None
                            existing_max = table_config.iloc[0]['max_rows'] if pd.notna(
                                table_config.iloc[0]['max_rows']) else None

                    # Show threshold inputs for this table
                    st.markdown(f"**{table}**")
                    col_min, col_max = st.columns(2)

                    with col_min:
                        min_val = st.number_input(
                            "Min Rows",
                            min_value=0,
                            value=int(
                                existing_min) if existing_min is not None else None,
                            key=f"min_rows_{table}"
                        )
                        if min_val > 0:  # Only save if user entered a value
                            min_rows_dict[table] = min_val

                    with col_max:
                        max_val = st.number_input(
                            "Max Rows",
                            min_value=0,
                            value=int(
                                existing_max) if existing_max is not None else None,
                            key=f"max_rows_{table}"
                        )
                        if max_val > 0:  # Only save if user entered a value
                            max_rows_dict[table] = max_val

            if st.button("Save Selected Tables", key="save_tables"):
                save_table_config(selected_db, selected_tables,
                                  min_rows_dict, max_rows_dict)
                st.success("Configuration saved.")

    results = []
    with col2:
        st.subheader("Monitored Tables Status")
        saved_tables = load_saved_table_config()

        if not saved_tables.empty:
            # Create threshold dictionaries for the check_selected_tables function
            min_rows_dict = {row['table_name']: row['min_rows']
                             for _, row in saved_tables.iterrows() if pd.notna(row['min_rows'])}
            max_rows_dict = {row['table_name']: row['max_rows']
                             for _, row in saved_tables.iterrows() if pd.notna(row['max_rows'])}

            # Create a container for the table list
            with st.container():
                for _, row in saved_tables.iterrows():
                    col_info, col_remove = st.columns([5, 1])

                    # Get table specific thresholds
                    table_min = row['min_rows'] if pd.notna(
                        row['min_rows']) else None
                    table_max = row['max_rows'] if pd.notna(
                        row['max_rows']) else None
                    table_min_dict = {
                        row['table_name']: table_min} if table_min is not None else {}
                    table_max_dict = {
                        row['table_name']: table_max} if table_max is not None else {}

                    check_result_df = check_selected_tables(
                        row["db_name"], [row["table_name"]], table_min_dict, table_max_dict)

                    count = 0
                    status = "Error"
                    if not check_result_df.empty:
                        count = int(
                            check_result_df.iloc[0]["Rows"]) if check_result_df.iloc[0]["Rows"].isdigit() else 0
                        status = check_result_df.iloc[0]["Status"]

                    # Get table size info
                    size_info = get_table_size_info(
                        row["db_name"], row["table_name"])
                    data_mb = size_info['data_kb'] / 1024
                    index_mb = size_info['index_kb'] / 1024
                    total_mb = data_mb + index_mb

                    # Prepare threshold info to display
                    threshold_info = ""
                    if table_min is not None or table_max is not None:
                        threshold_info = " - Thresholds: "
                        if table_min is not None:
                            threshold_info += f"Min={table_min}"
                        if table_min is not None and table_max is not None:
                            threshold_info += ", "
                        if table_max is not None:
                            threshold_info += f"Max={table_max}"

                    # Display table information
                    with col_info:
                        st.write(
                            f"{row['db_name']}.{row['table_name']} - {status} (Rows: {count}){threshold_info} - Size: {total_mb:.2f} MB (Data: {data_mb:.2f} MB, Index: {index_mb:.2f} MB)")

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
                        'Min Rows': table_min if table_min is not None else "None",
                        'Max Rows': table_max if table_max is not None else "None",
                        'Data MB': round(data_mb, 2),
                        'Index MB': round(index_mb, 2),
                        'Total MB': round(total_mb, 2),
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
                show_anomalies = st.checkbox(
                    "Detect Duration Anomalies", value=True)
                job_history = get_job_history(
                    hours, detect_anomalies=show_anomalies)
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

                if show_anomalies and 'Duration Status' in filtered_history.columns:
                    duration_status_options = filtered_history['Duration Status'].unique(
                    ).tolist()
                    selected_duration_status = st.multiselect(
                        "Filter by Duration",
                        duration_status_options,
                        default=duration_status_options,
                        key="job_duration_status"
                    )
                    filtered_history = filtered_history[
                        filtered_history['Status'].isin(selected_status) &
                        filtered_history['Duration Status'].isin(
                            selected_duration_status)
                    ]
                else:
                    filtered_history = filtered_history[filtered_history['Status'].isin(
                        selected_status)]

            with col2:
                if not filtered_history.empty:
                    # If we have duration anomaly data, display and color-code by that
                    if show_anomalies and 'Duration Status' in filtered_history.columns:
                        # Create a more informative view with both status columns
                        display_df = filtered_history.copy()

                        # For jobs with duration anomalies, add indicators
                        def format_duration(row):
                            duration = row['Duration']
                            status = row['Duration Status']
                            if status == 'Normal':
                                return duration
                            elif status == 'Slow':
                                return f"{duration} ⚠️ (Slow)"
                            elif status == 'Fast':
                                return f"{duration} ⚠️ (Fast)"
                            return duration

                        display_df['Duration'] = display_df.apply(
                            format_duration, axis=1)

                        # Display with both Status and Duration Status coloring
                        status_styled = apply_status_colors(
                            display_df, 'Status')

                        # Now also apply duration status colors
                        def apply_duration_colors(df):
                            def color_duration(val):
                                colors = {
                                    'Slow': 'orange',
                                    'Fast': 'purple',
                                    'Normal': 'green'
                                }
                                return f'color: {colors.get(val, "black")}'

                            if 'Duration Status' in df.columns:
                                return df.style.apply(
                                    lambda x: [color_duration(x['Duration Status']) if col == 'Duration' else ''
                                               for col in x.index], axis=1)
                            return df

                        st.dataframe(status_styled, use_container_width=True)

                        # Add a section highlighting anomalies
                        anomalies = display_df[display_df['Duration Status'] != 'Normal']
                        if not anomalies.empty:
                            st.subheader("⚠️ Duration Anomalies")
                            for _, job in anomalies.iterrows():
                                anomaly_type = "longer" if job['Duration Status'] == 'Slow' else "shorter"
                                st.warning(
                                    f"{job['Job Name']} ran {anomaly_type} than usual on {job['Run Date']} {job['Run Time']} - Duration: {job['Duration']}"
                                )
                    else:
                        # Original display logic without duration anomaly detection
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

    # Job Statistics - Updated to only count monitored jobs
    monitored_jobs = all_jobs[all_jobs['Job Name'].isin(
        saved_jobs['job_name'])] if not saved_jobs.empty else pd.DataFrame()
    running_jobs = len(active_jobs[active_jobs['Job Name'].isin(
        saved_jobs['job_name'])]) if not active_jobs.empty else 0

    if not job_history.empty and not saved_jobs.empty:
        # Filter job history to only include monitored jobs
        monitored_history = job_history[job_history['Job Name'].isin(
            saved_jobs['job_name'])]
        recent_failed = len(
            monitored_history[monitored_history['Status'] == 'Failed'])
        recent_succeeded = len(
            monitored_history[monitored_history['Status'] == 'Succeeded'])
    else:
        recent_failed = 0
        recent_succeeded = 0

    # Table Statistics
    table_results_for_stats = []
    if not saved_tables.empty:
        for _, row in saved_tables.iterrows():
            result_df = check_selected_tables(
                row["db_name"], [row["table_name"]])
            count = 0
            status = "Error"
            if not result_df.empty:
                count = int(
                    result_df.iloc[0]["Rows"]) if result_df.iloc[0]["Rows"].isdigit() else 0
                status = result_df.iloc[0]["Status"]

            size_info = get_table_size_info(row["db_name"], row["table_name"])
            data_mb = size_info['data_kb'] / 1024
            index_mb = size_info['index_kb'] / 1024
            total_mb = data_mb + index_mb

            table_results_for_stats.append({
                'Database': row["db_name"],
                'Table': row["table_name"],
                'Row Count': count,
                'Status': status,
                'Data MB': round(data_mb, 2),
                'Index MB': round(index_mb, 2),
                'Total MB': round(total_mb, 2)
            })

    table_stats = pd.DataFrame(
        table_results_for_stats) if table_results_for_stats else pd.DataFrame()
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
        # Filter to only show monitored jobs
        if not saved_jobs.empty:
            active_jobs = active_jobs[active_jobs['Job Name'].isin(
                saved_jobs['job_name'])]

        if not active_jobs.empty:
            for _, job in active_jobs.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.markdown(f"**{job['Job Name']}**")
                        # Handle step information more safely
                        current_step = '0'
                        step_name = 'Starting'

                        if 'Current Step' in job and pd.notna(job['Current Step']):
                            current_step = str(job['Current Step'])
                        if 'Step Name' in job and pd.notna(job['Step Name']):
                            step_name = job['Step Name']

                        st.text(f"Step {current_step}: {step_name}")
                    with col2:
                        duration = job['Duration (mins)'] if 'Duration (mins)' in job and pd.notna(
                            job['Duration (mins)']) else 0
                        st.markdown(f"Duration: {duration} mins")
        else:
            st.info("No monitored jobs are currently running")
    else:
        st.info("No jobs are currently running")

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


def render_alert_log():
    st.header("🚨 Alert Log")

    # Filters for the alert log
    col1, col2, col3 = st.columns(3)
    with col1:
        alert_type_filter = st.selectbox(
            "Alert Type",
            ["All", "Table", "Job"],
            key="alert_type_filter"
        )
    with col2:
        status_filter = st.selectbox(
            "Status",
            ["All", "Empty", "Error", "Failed", "Warn-LowCount",
                "Warn-HighCount", "Slow", "Fast"],
            key="status_filter"
        )
    with col3:
        time_filter = st.selectbox(
            "Time Range",
            ["All Time", "Last 24 Hours", "Last 7 Days", "Last 30 Days"],
            key="time_filter"
        )

    # Convert filter selections to parameters for get_alerts
    alert_type = None if alert_type_filter == "All" else alert_type_filter
    status = None if status_filter == "All" else status_filter

    hours_back = None
    if time_filter == "Last 24 Hours":
        hours_back = 24
    elif time_filter == "Last 7 Days":
        hours_back = 24 * 7
    elif time_filter == "Last 30 Days":
        hours_back = 24 * 30

    # Get alerts based on filters
    alerts = get_alerts(
        limit=100,
        alert_type=alert_type,
        status=status,
        hours_back=hours_back
    )

    if alerts.empty:
        st.info("No alerts found for the selected filters.")
    else:
        # Format the dataframe for display
        display_df = alerts.copy()

        # Convert alert_time to a more readable format
        display_df['alert_time'] = pd.to_datetime(
            display_df['alert_time']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # First ensure we have all the required columns
        required_columns = ['id', 'alert_time', 'alert_type',
                            'source_type', 'source_name', 'status', 'message']
        if all(col in display_df.columns for col in required_columns):
            # Display the alert log with original column names
            display_df = display_df[required_columns].copy()

            # Then rename the columns
            display_df.columns = ['ID', 'Time', 'Type',
                                  'Source Type', 'Source', 'Status', 'Message']

            # Display the formatted DataFrame
            st.dataframe(
                apply_status_colors(display_df, 'Status'),
                use_container_width=True
            )

            # Allow user to view details for a specific alert
            if 'details' in alerts.columns and not all(alerts['details'].isna()):
                alert_ids = alerts['id'].tolist()
                selected_alert = st.selectbox(
                    "View Alert Details",
                    ["Select an alert..."] + alert_ids,
                    format_func=lambda x: f"Alert #{x}" if isinstance(
                        x, int) else x
                )

                if selected_alert != "Select an alert...":
                    alert_details = alerts[alerts['id'] ==
                                           selected_alert]['details'].iloc[0]
                    if alert_details:
                        with st.expander("Alert Details", expanded=True):
                            st.text(alert_details)
        else:
            missing_columns = [
                col for col in required_columns if col not in display_df.columns]
            st.error(
                f"Missing required columns in alert data: {', '.join(missing_columns)}")


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


def render_config_view():
    st.header("⚙️ Configuration")

    tab1, tab2, tab3 = st.tabs(
        ["📊 Table Monitor", "🔄 Job Monitor", "🚨 Alert Log"])

    with tab1:
        table_results = render_table_monitor()
        st.session_state.table_results = table_results

    with tab2:
        job_results = render_job_monitor()
        st.session_state.job_results = job_results

    with tab3:
        render_alert_log()


def get_latest_table_results():
    saved_tables = load_saved_table_config()
    results = []

    if not saved_tables.empty:
        for _, row in saved_tables.iterrows():
            # Get table specific thresholds
            table_min = row['min_rows'] if pd.notna(row['min_rows']) else None
            table_max = row['max_rows'] if pd.notna(row['max_rows']) else None
            table_min_dict = {row['table_name']
                : table_min} if table_min is not None else {}
            table_max_dict = {row['table_name']
                : table_max} if table_max is not None else {}

            check_result_df = check_selected_tables(
                row["db_name"], [row["table_name"]], table_min_dict, table_max_dict)
            count = 0
            status = "Error"
            if not check_result_df.empty:
                count = int(
                    check_result_df.iloc[0]["Rows"]) if check_result_df.iloc[0]["Rows"].isdigit() else 0
                status = check_result_df.iloc[0]["Status"]

                # Log alerts for table issues
                if status != "OK":
                    source_type = ""
                    if status == "Empty":
                        source_type = "Empty Table"
                    elif status.startswith("Error"):
                        source_type = "Table Error"
                    elif status == "Warn-LowCount":
                        source_type = "Low Row Count"
                    elif status == "Warn-HighCount":
                        source_type = "High Row Count"

                    if source_type:
                        details = f"Database: {row['db_name']}\n"
                        details += f"Table: {row['table_name']}\n"
                        details += f"Row Count: {count}\n"

                        if status == "Warn-LowCount" and table_min is not None:
                            details += f"Min Threshold: {table_min}\n"
                        elif status == "Warn-HighCount" and table_max is not None:
                            details += f"Max Threshold: {table_max}\n"

                        log_alert(
                            alert_type="Table",
                            source_type=source_type,
                            source_name=f"{row['db_name']}.{row['table_name']}",
                            status=status,
                            message=f"Table {row['db_name']}.{row['table_name']} has {status} status",
                            details=details
                        )

            size_info = get_table_size_info(row["db_name"], row["table_name"])
            data_mb = size_info['data_kb'] / 1024
            index_mb = size_info['index_kb'] / 1024
            total_mb = data_mb + index_mb

            results.append({
                'Database': row["db_name"],
                'Table': row["table_name"],
                'Row Count': count,
                'Status': status,
                'Min Rows': table_min if table_min is not None else "None",
                'Max Rows': table_max if table_max is not None else "None",
                'Data MB': round(data_mb, 2),
                'Index MB': round(index_mb, 2),
                'Total MB': round(total_mb, 2),
                'Last Check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            # Log check result to table_check_log
            log_table_check_result(
                row["db_name"],
                row["table_name"],
                count,
                status
            )

    return results


def get_latest_job_results():
    saved_jobs = load_saved_job_config()
    results = []

    if not saved_jobs.empty:
        # Last 24 hours with anomaly detection
        job_history = get_job_history(24, detect_anomalies=True)
        filtered_history = job_history[job_history['Job Name'].isin(
            saved_jobs['job_name'])]

        # Log alerts for job issues
        for _, job in filtered_history.iterrows():
            # Log failed jobs
            if job['Status'] == 'Failed':
                details = f"Job Name: {job['Job Name']}\n"
                details += f"Run Date: {job['Run Date']}\n"
                details += f"Run Time: {job['Run Time']}\n"
                details += f"Duration: {job['Duration']}\n"
                details += f"Message: {job['Message']}\n"

                log_alert(
                    alert_type="Job",
                    source_type="Failed Job",
                    source_name=job['Job Name'],
                    status="Failed",
                    message=f"Job {job['Job Name']} failed at {job['Run Date']} {job['Run Time']}",
                    details=details
                )

            # Log duration anomalies if present
            if 'Duration Status' in job and job['Duration Status'] in ['Slow', 'Fast']:
                details = f"Job Name: {job['Job Name']}\n"
                details += f"Run Date: {job['Run Date']}\n"
                details += f"Run Time: {job['Run Time']}\n"
                details += f"Duration: {job['Duration']}\n"
                details += f"Normal Duration: {job['Duration Status']}\n"
                if 'Duration Seconds' in job:
                    details += f"Duration in seconds: {job['Duration Seconds']}\n"

                log_alert(
                    alert_type="Job",
                    source_type="Duration Anomaly",
                    source_name=job['Job Name'],
                    status=job['Duration Status'],
                    message=f"Job {job['Job Name']} had abnormal duration ({job['Duration Status']}) at {job['Run Date']} {job['Run Time']}",
                    details=details
                )

        return filtered_history.to_dict('records')
    return []
