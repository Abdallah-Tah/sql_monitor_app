import streamlit as st
import pandas as pd
import time
import os  # Added import
from datetime import datetime, timedelta
from components.sql import (
    get_databases, get_tables, check_selected_tables, get_table_size_info,
    get_job_history, get_job_details, get_job_steps, get_all_jobs, get_active_jobs, get_table_columns,
    get_rows_for_processed_today, get_connection  # Added get_connection
)
from components.db import (
    save_table_config, load_saved_table_config, log_table_check_result, get_latest_log,
    save_job_config, load_saved_job_config, log_job_check_result, delete_table_config,
    # Added imports
    delete_job_config, log_alert, get_alerts, save_column_config, load_column_config
)
from streamlit_autorefresh import st_autorefresh


def get_windows_user():
    try:
        return os.getlogin()
    except OSError:
        # Fallback if os.getlogin() fails (e.g., not run from a real terminal)
        import getpass
        try:
            return getpass.getuser()
        except Exception:
            return "Unknown User"


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

    if table_results:  # table_results is a list of dicts
        for table_data in table_results:  # Renamed to avoid conflict
            status = table_data['Status']
            is_issue = False
            if status == 'Empty':
                is_issue = True
            elif status.startswith('Error'):
                is_issue = True
            elif status == 'Warn-LowCount':  # Exact match for simple low count
                is_issue = True
            elif status == 'Warn-HighCount':  # Exact match for simple high count
                is_issue = True
            # Catch column condition issues that are explicitly warnings or combined with other warnings
            elif 'ColumnConditionNotMet' in status and ('Warn' in status or ';ColCondNotMet' in status):
                is_issue = True

            if is_issue:
                table_issues.append(table_data)

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

                # Handle Empty Tables
                current_display_list = [
                    t for t in table_issues if t['Status'] == 'Empty']
                if current_display_list:
                    st.markdown("#### Empty Tables")
                    for table_item in current_display_list:
                        st.warning(
                            f"⚠️ Empty Table: {table_item['Database']}.{table_item['Table']}")

                # Handle Error Tables
                current_display_list = [
                    t for t in table_issues if t['Status'].startswith('Error')]
                if current_display_list:
                    st.markdown("#### Error Tables")
                    for table_item in current_display_list:
                        st.error(
                            f"❌ Table Error: {table_item['Database']}.{table_item['Table']} - {table_item['Status']}")

                # Handle Low Row Count (exact status match)
                current_display_list = [
                    t for t in table_issues if t['Status'] == 'Warn-LowCount']
                if current_display_list:
                    st.markdown("#### Low Row Count")
                    for table_item in current_display_list:
                        min_rows = table_item.get('Min Rows', 'N/A')
                        st.warning(
                            f"⚠️ Low Row Count: {table_item['Database']}.{table_item['Table']} - Count: {table_item.get('Row Count', 'N/A')}, Min: {min_rows}")

                # Handle High Row Count (exact status match)
                current_display_list = [
                    t for t in table_issues if t['Status'] == 'Warn-HighCount']
                if current_display_list:
                    st.markdown("#### High Row Count")
                    for table_item in current_display_list:
                        max_rows = table_item.get('Max Rows', 'N/A')
                        st.warning(
                            f"⚠️ High Row Count: {table_item['Database']}.{table_item['Table']} - Count: {table_item.get('Row Count', 'N/A')}, Max: {max_rows}")

                # Handle Column Condition Issues
                # This will catch statuses like 'Warn-ColumnConditionNotMet'
                # or combined statuses like 'Warn-LowCount;ColCondNotMet'
                current_display_list = [t for t in table_issues if 'ColumnConditionNotMet' in t['Status'] and (
                    'Warn' in t['Status'] or ';ColCondNotMet' in t['Status'])]
                if current_display_list:
                    st.markdown("#### Column Condition Issues")
                    for table_item in current_display_list:
                        st.warning(
                            f"⚠️ Condition Issue: {table_item['Database']}.{table_item['Table']} - Status: {table_item['Status']}")

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

    # Handling edit state reset if user manually changes selection
    # This might be better handled by how defaults are set or by a "Cancel Edit" button
    current_table_db_selection = st.session_state.get("table_db_select")
    current_table_selection = st.session_state.get("table_select")

    if st.session_state.edit_trigger and \
       (st.session_state.edit_selected_db != current_table_db_selection or
            (current_table_selection is not None and st.session_state.edit_selected_tables != current_table_selection)):
        # User manually changed selection after edit was triggered, so reset edit mode
        # This logic is a bit complex due to widget state. Simpler to reset edit_trigger after one use.
        pass

    col1, col2 = st.columns([2, 3])

    with col1:
        st.subheader("Configuration")
        dbs = get_databases()

        db_default_index = 0
        if st.session_state.edit_trigger and st.session_state.edit_selected_db and st.session_state.edit_selected_db in dbs:
            db_default_index = dbs.index(st.session_state.edit_selected_db)

        selected_db = st.selectbox(
            "Choose Database", dbs, key="table_db_select", index=db_default_index
        )

        if selected_db:
            tables = get_tables(selected_db)
            table_default_selection = []
            if st.session_state.edit_trigger and selected_db == st.session_state.edit_selected_db:
                table_default_selection = [
                    t for t in st.session_state.edit_selected_tables if t in tables]

            # Use a different key for the multiselect if we are setting its value programmatically due to edit_trigger
            # This helps Streamlit correctly update the widget.
            multiselect_key = "table_select_edit" if st.session_state.edit_trigger and table_default_selection else "table_select"

            selected_tables_val = st.multiselect(
                "Select Tables", tables, key=multiselect_key, default=table_default_selection
            )

            # If edit was triggered, and we've now rendered the widgets with defaults, turn off the trigger.
            if st.session_state.edit_trigger:
                st.session_state.edit_trigger = False
                # Force selected_tables to be what was intended by the edit action for this render pass
                if selected_db == st.session_state.edit_selected_db:
                    selected_tables_val = table_default_selection

            # For each selected table, show threshold configuration
            threshold_settings = {}
            if selected_tables_val:  # Use selected_tables_val which has the edit default
                st.subheader("Monitoring Configuration")
                tab_row_count, tab_column_cond = st.tabs(
                    ["Row Count Thresholds", "Column Conditions"])

                with tab_row_count:
                    st.info(
                        "Set min/max row count thresholds to monitor. Leave blank for no threshold.")

                    # Create dictionaries to store threshold values
                    min_rows_dict = {}
                    max_rows_dict = {}

                    # Get existing thresholds from database for selected tables
                    existing_config = load_saved_table_config()

                    for table in selected_tables_val:
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

                        st.markdown(f"**{table}**")
                        col_min, col_max = st.columns(2)

                        with col_min:
                            min_val = st.number_input(
                                "Min Rows",
                                min_value=0,
                                value=int(
                                    existing_min) if existing_min is not None else 0,
                                key=f"min_rows_{table}"
                            )
                            if min_val > 0:
                                min_rows_dict[table] = min_val
                            # Allow setting back to 0 (no threshold)
                            elif table in min_rows_dict:
                                min_rows_dict[table] = None

                        with col_max:
                            max_val = st.number_input(
                                "Max Rows",
                                min_value=0,
                                value=int(
                                    existing_max) if existing_max is not None else 0,
                                key=f"max_rows_{table}"
                            )
                            if max_val > 0:
                                max_rows_dict[table] = max_val
                            # Allow setting back to 0 (no threshold)
                            elif table in max_rows_dict:
                                max_rows_dict[table] = None

                with tab_column_cond:
                    st.info(
                        "Configure conditions for specific columns to monitor (optional)")
                    for table in selected_tables_val:
                        st.markdown(f"**{table}**")
                        columns = get_table_columns(selected_db, table)

                        # Input for minimum match count for column conditions
                        # Load existing config to get the current min_match_count for this table
                        current_table_monitor_config_df = load_saved_table_config()
                        current_min_match_val = 1  # Default
                        if not current_table_monitor_config_df.empty:
                            specific_table_config = current_table_monitor_config_df[
                                (current_table_monitor_config_df['db_name'] == selected_db) &
                                (current_table_monitor_config_df['table_name'] == table)
                            ]
                            if not specific_table_config.empty and 'column_min_match_count' in specific_table_config.columns:
                                val_from_db = specific_table_config['column_min_match_count'].iloc[0]
                                if pd.notna(val_from_db):
                                    current_min_match_val = int(val_from_db)

                        min_match_count_for_cols = st.number_input(
                            "Minimum rows to meet all column conditions (0 or blank for 'all rows')",
                            min_value=0,
                            value=current_min_match_val,
                            # Ensure key is unique
                            key=f"min_match_count_cols_{selected_db}_{table}",
                            help="For conditions to pass, at least this many rows must satisfy ALL defined column conditions. If 0 or blank, ALL rows in the table must satisfy them. For UploadLogs, this is typically 1 (existence check)."
                        )
                        # Store this value in session state or pass it directly to save logic
                        if table not in threshold_settings:
                            threshold_settings[table] = {}
                        # Store the direct value
                        threshold_settings[table]["column_min_match_count"] = min_match_count_for_cols

                        if columns:
                            # Add a checkbox to enable/disable column monitoring for this table
                            # Default value for enable_column_monitoring checkbox
                            default_enable_column_monitoring = False
                            existing_column_config_for_table = load_column_config(
                                selected_db, table)
                            if not existing_column_config_for_table.empty:
                                default_enable_column_monitoring = True

                            enable_column_monitoring = st.checkbox(
                                "Enable Column Monitoring",
                                value=default_enable_column_monitoring,
                                key=f"enable_columns_{table}"
                            )

                            if enable_column_monitoring:
                                # existing_config = load_column_config(selected_db, table) # already loaded
                                selected_columns_for_conditions = st.multiselect(
                                    "Select columns to monitor",
                                    options=[col["name"] for col in columns],
                                    default=[cfg["column_name"] for _, cfg in existing_column_config_for_table.iterrows(
                                    )] if not existing_column_config_for_table.empty else None,
                                    key=f"columns_{table}"
                                )

                                column_configs = []
                                for col_name_to_configure in selected_columns_for_conditions:
                                    col_type = next(
                                        (c["type"] for c in columns if c["name"] == col_name_to_configure), None)
                                    st.markdown(
                                        f"***{col_name_to_configure}*** ({col_type})")

                                    existing_col_config_series = None
                                    if not existing_column_config_for_table.empty and col_name_to_configure in existing_column_config_for_table["column_name"].values:
                                        existing_col_config_series = existing_column_config_for_table[
                                            existing_column_config_for_table["column_name"] == col_name_to_configure
                                        ].iloc[0]

                                    condition_options = ["equals", "not_equals", "greater_than",
                                                         "less_than", "in", "date_equals_today",
                                                         "date_greater_than", "date_less_than"]
                                    default_condition_index = 0
                                    if existing_col_config_series is not None and existing_col_config_series["condition_type"] in condition_options:
                                        default_condition_index = condition_options.index(
                                            existing_col_config_series["condition_type"])

                                    condition = st.selectbox(
                                        "Condition",
                                        options=condition_options,
                                        index=default_condition_index,
                                        key=f"condition_{table}_{col_name_to_configure}"
                                    )

                                    default_value_text = ""
                                    if existing_col_config_series is not None:
                                        default_value_text = existing_col_config_series["condition_value"]

                                    if condition != "date_equals_today":
                                        value = st.text_input(
                                            "Value" +
                                            (" (comma-separated for IN)" if condition == "in" else
                                             " (YYYY-MM-DD for date conditions)" if condition.startswith("date_") else ""),
                                            value=default_value_text,
                                            key=f"value_{table}_{col_name_to_configure}"
                                        )
                                    else:
                                        value = "CURRENT_DATE"

                                    if value:
                                        column_configs.append({
                                            "column_name": col_name_to_configure,
                                            "condition_type": condition,
                                            "condition_value": value
                                        })

                                # Store column configs in the table's threshold settings
                                if column_configs:
                                    if table not in threshold_settings:
                                        threshold_settings[table] = {}
                                    threshold_settings[table]["column_configs"] = column_configs
                            else:
                                # If monitoring is disabled, clear any existing configurations
                                save_column_config(selected_db, table, [])

            if st.button("Save Selected Tables", key="save_tables"):
                for table_to_save in selected_tables_val:  # Iterate over the tables actually selected in the UI
                    # Row count thresholds
                    min_r = min_rows_dict.get(table_to_save)
                    max_r = max_rows_dict.get(table_to_save)

                    # Column min match count
                    # Retrieve from the number_input's current value via session_state
                    col_min_match_c = st.session_state.get(
                        f"min_match_count_cols_{selected_db}_{table_to_save}", 1)
                    if col_min_match_c is None:  # Ensure it has a default if somehow not set
                        col_min_match_c = 1  # Default to 1 if not found

                    # Column configs
                    enable_column_monitoring_for_save = st.session_state.get(
                        f"enable_columns_{table_to_save}", False)  # Get current state of checkbox

                    current_column_configs_for_table = []
                    if enable_column_monitoring_for_save:
                        # Reconstruct column_configs for THIS table based on session_state values of widgets
                        # This is complex because threshold_settings[table]["column_configs"] was built during render
                        # It's safer to rebuild from st.session_state if possible, or ensure threshold_settings is accurate.
                        # For simplicity, we'll rely on threshold_settings which should be up-to-date from the render pass.
                        current_column_configs_for_table = threshold_settings.get(
                            table_to_save, {}).get("column_configs", [])

                    save_table_config(selected_db, [table_to_save],
                                      {table_to_save: min_r} if min_r is not None else {},
                                      {table_to_save: max_r} if max_r is not None else {},
                                      # Pass the new dict here
                                      {table_to_save: col_min_match_c}
                                      )

                    if enable_column_monitoring_for_save and current_column_configs_for_table:
                        save_column_config(
                            selected_db, table_to_save, current_column_configs_for_table)
                    elif not enable_column_monitoring_for_save:  # If checkbox is off, clear existing
                        save_column_config(selected_db, table_to_save, [])

                st.success("Configuration saved.")
                # Reset edit state after save
                st.session_state.edit_selected_db = None
                st.session_state.edit_selected_tables = []
                # Should already be false, but good to be sure
                st.session_state.edit_trigger = False
                st.experimental_rerun()  # Rerun to reflect saved changes and clear selections if needed

    results = []
    with col2:
        st.subheader("Monitored Tables Status")
        saved_tables = load_saved_table_config()

        if not saved_tables.empty:
            # Create a container for the table list
            with st.container():
                for idx, row in saved_tables.iterrows():
                    # Define columns for info and each action button directly
                    info_col, details_col, edit_col, delete_col = st.columns([
                                                                             7, 1, 1, 1])

                    with info_col:  # INFO PART
                        table_min = row['min_rows'] if pd.notna(
                            row['min_rows']) else None
                        table_max = row['max_rows'] if pd.notna(
                            row['max_rows']) else None
                        table_col_min_match = row['column_min_match_count'] if pd.notna(
                            row['column_min_match_count']) else 1  # Default to 1 if not set

                        table_min_dict = {
                            row['table_name']: table_min} if table_min is not None else {}
                        table_max_dict = {
                            row['table_name']: table_max} if table_max is not None else {}
                        table_col_min_match_dict = {
                            row['table_name']: table_col_min_match
                        }

                        check_result_df = check_selected_tables(
                            row["db_name"], [row["table_name"]], table_min_dict, table_max_dict, table_col_min_match_dict)
                        count = 0
                        status = "Error"
                        if not check_result_df.empty:
                            count = int(
                                check_result_df.iloc[0]["Rows"]) if check_result_df.iloc[0]["Rows"].isdigit() else 0
                            if count == 0:  # Explicitly check for empty tables first
                                status = "Empty"
                            else:
                                status = check_result_df.iloc[0]["Status"]

                        size_info = get_table_size_info(
                            row["db_name"], row["table_name"])
                        data_mb = size_info['data_kb'] / 1024
                        index_mb = size_info['index_kb'] / 1024
                        total_mb = data_mb + index_mb

                        threshold_info = ""
                        if table_min is not None or table_max is not None:
                            threshold_info = " - Thresholds: "
                            if table_min is not None:
                                threshold_info += f"Min={int(table_min) if pd.notna(table_min) else 'N/A'}"
                            if table_min is not None and table_max is not None:
                                threshold_info += ", "
                            if table_max is not None:
                                threshold_info += f"Max={int(table_max) if pd.notna(table_max) else 'N/A'}"

                        st.write(
                            f"{row['db_name']}.{row['table_name']} - {status} (Rows: {count}){threshold_info} - Size: {total_mb:.2f} MB (Data: {data_mb:.2f} MB, Index: {index_mb:.2f} MB)")

                    # ACTIONS PART - Buttons in their own columns
                    with details_col:
                        if st.button("ℹ️", key=f"details_table_{idx}", help="View table details", use_container_width=True):
                            # Toggle detail view for this specific table
                            if st.session_state.get(f"expanded_table_detail_{idx}"):
                                st.session_state[f"expanded_table_detail_{idx}"] = False
                            else:
                                st.session_state[f"expanded_table_detail_{idx}"] = True
                    with edit_col:
                        if st.button("✏️", key=f"edit_table_{idx}", help="Edit table configuration", use_container_width=True):
                            st.session_state.edit_selected_db = row['db_name']
                            st.session_state.edit_selected_tables = [
                                row['table_name']]
                            st.session_state.edit_trigger = True  # Signal to set defaults in config section
                            st.experimental_rerun()  # Rerun to repopulate config with selected table
                    with delete_col:
                        if st.button("🗑️", key=f"delete_table_{idx}", help="Delete table configuration", use_container_width=True):
                            delete_table_config(
                                row['db_name'], row['table_name'])
                            st.success(
                                f"Configuration for {row['db_name']}.{row['table_name']} deleted.")
                            st.experimental_rerun()

                    # After the buttons, check if details should be shown for this table
                    if st.session_state.get(f"expanded_table_detail_{idx}"):
                        with st.expander(f"Details for {row['db_name']}.{row['table_name']}", expanded=True):
                            # Display column conditions if any
                            column_configs_df = load_column_config(
                                row["db_name"], row["table_name"])
                            if not column_configs_df.empty:
                                st.markdown("**Monitored Column Conditions:**")
                                # Convert DataFrame to a more readable list of strings or styled display
                                for _, cfg_row in column_configs_df.iterrows():
                                    st.info(
                                        f"- Column: `{cfg_row['column_name']}`, Condition: `{cfg_row['condition_type']}`, Value: `{cfg_row['condition_value']}`")
                            else:
                                st.markdown(
                                    "No specific column conditions configured for this table.")

                            # Display raw check_result_df for this table if available
                            if not check_result_df.empty and 'Column Conditions' in check_result_df.columns:
                                st.markdown("**Detailed Check Result:**")
                                # check_result_df.iloc[0]['Column Conditions'] is a dict
                                detailed_col_conds = check_result_df.iloc[0]['Column Conditions']
                                if detailed_col_conds and isinstance(detailed_col_conds, dict):
                                    # Display the dict as JSON
                                    st.json(detailed_col_conds)
                                elif not check_result_df.empty:
                                    # Fallback to dataframe if not a dict
                                    st.dataframe(check_result_df)
                                else:
                                    st.write(
                                        "No detailed check result available.")

                    results.append({
                        'Database': row["db_name"],
                        'Table': row["table_name"],
                        'Row Count': count,
                        'Status': status,
                        'Min Rows': str(table_min) if table_min is not None else "None",
                        'Max Rows': str(table_max) if table_max is not None else "None",
                        'Data MB': round(data_mb, 2),
                        'Index MB': round(index_mb, 2),
                        'Total MB': round(total_mb, 2),
                        'Last Check': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    })
        else:
            st.info(
                "No tables selected for monitoring. Please configure tables above.")

    # "Detailed Status" DataFrame display is now outside and after col1 and col2
    if results:
        st.markdown("### Detailed Status")
        # Ensure all expected columns are present before trying to set astype
        df_columns = ['Database', 'Table', 'Row Count', 'Status', 'Min Rows',
                      'Max Rows', 'Data MB', 'Index MB', 'Total MB', 'Last Check']
        # Ensure column order and presence
        status_df = pd.DataFrame(results, columns=df_columns)

        # Convert types, handling potential errors if a column is missing or data is not convertible
        try:
            status_df = status_df.astype({
                'Database': str,
                'Table': str,
                'Row Count': int,
                'Status': str,
                'Min Rows': str,
                'Max Rows': str,
                'Data MB': float,
                'Index MB': float,
                'Total MB': float,
                'Last Check': str
            })
        except KeyError as e:
            st.error(
                f"Detailed Status DataFrame is missing an expected column: {e}. Please check data population.")
        except ValueError as e:
            st.error(
                f"Detailed Status DataFrame has a column with an unexpected data type: {e}. Please check data population.")

        st.dataframe(apply_status_colors(
            status_df, 'Status'), use_container_width=True)

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
            # Check if steps_df is a DataFrame and not empty
            if isinstance(steps_df, pd.DataFrame) and not steps_df.empty:
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
                if isinstance(all_jobs, pd.DataFrame) and 'Job Name' in all_jobs.columns:
                    monitored_jobs_df = all_jobs[all_jobs['Job Name'].isin(
                        saved_job_names)]
                else:
                    monitored_jobs_df = pd.DataFrame()

                with st.container():
                    for idx, (_, row) in enumerate(monitored_jobs_df.iterrows()):
                        # Define columns for info and each action button directly
                        # Old: main_cols = st.columns([0.7, 0.3])
                        # New: direct columns for info and 3 buttons. Ratio [7,1,1,1].
                        info_col, details_col, edit_col, delete_col = st.columns([
                                                                                 7, 1, 1, 1])

                        with info_col:  # INFO PART
                            status_color = {
                                'Running': '🔵',
                                'Failed': '🔴',
                                'Succeeded': '🟢',
                                'Enabled': '🟢',
                                'Disabled': '⚪',
                                'Canceled': '🟠',
                                'Retry': '🟡'
                            }.get(row.get('Status', 'N/A'), '⚪')
                            job_name_display = row.get(
                                'Job Name', 'Unknown Job')
                            job_status_display = row.get('Status', 'N/A')
                            st.write(
                                f"{status_color} {job_name_display} ({job_status_display})")

                        # ACTIONS PART - Buttons in their own columns
                        with details_col:
                            if st.button("ℹ️", key=f"details_job_{idx}", help="View job details", use_container_width=True):
                                # Toggle detail view for this specific job
                                # Consistent key generation: using job_name_display and idx for uniqueness
                                job_display_name_for_key = row.get(
                                    'Job Name', 'Unknown Job')
                                unique_job_session_key = f"expanded_job_detail_{job_display_name_for_key}_{idx}"

                                if st.session_state.get(unique_job_session_key):
                                    st.session_state[unique_job_session_key] = False
                                else:
                                    st.session_state[unique_job_session_key] = True

                        with edit_col:
                            if st.button("✏️", key=f"edit_job_{idx}", help="Edit job monitoring configuration", use_container_width=True):
                                st.info(
                                    f"Edit for job {job_name_display} is not currently applicable/implemented.")
                        with delete_col:
                            if st.button("🗑️", key=f"remove_job_{idx}", help="Remove job from monitoring", use_container_width=True):
                                delete_job_config(job_name_display)
                                st.experimental_rerun()

                # After the buttons, check if details should be shown for this job
                # Corrected line for syntax and consistent key logic:
                job_display_name_for_expander = row.get(
                    'Job Name', 'Unknown Job')  # Get the display name
                # Create the consistent session key
                current_job_session_key_for_expander = f"expanded_job_detail_{job_display_name_for_expander}_{idx}"

                if st.session_state.get(current_job_session_key_for_expander, False):
                    with st.expander(f"Details for Job: {job_display_name_for_expander}", expanded=True):
                        # render_job_details uses the actual job name or None
                        render_job_details(row.get('Job Name'))
                        if st.button("Close Details", key=f"close_details_job_{idx}"):
                            st.session_state[current_job_session_key_for_expander] = False
                            st.experimental_rerun()

                st.markdown("### Detailed Status")
                if not monitored_jobs_df.empty:
                    st.dataframe(apply_status_colors(monitored_jobs_df,
                                 'Status'), use_container_width=True)
                else:
                    st.info("No job data to display for monitored jobs.")
            else:
                st.info("No jobs currently selected for monitoring.")

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
    # --- Auto-refresh interval configuration ---
    if 'refresh_interval' not in st.session_state:
        st.session_state.refresh_interval = 30  # default 5 seconds
    col_refresh, _, _ = st.columns([1, 1, 8])
    with col_refresh:
        refresh_interval = st.number_input(
            "Auto-refresh interval (seconds)",
            min_value=1,
            max_value=3600,
            value=st.session_state.refresh_interval,
            step=1,
            key="refresh_interval_input"
        )
        st.session_state.refresh_interval = refresh_interval

    st_autorefresh(interval=st.session_state.refresh_interval *
                   1000, key="dashboard_autorefresh")

    # --- Professional Last Updated Display ---
    last_updated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.markdown(f"""
        <div style='display: flex; align-items: center; margin-bottom: 0.5rem;'>
            <span style='font-size: 1.2rem; color: #888; font-weight: 500; margin-right: 0.5rem;'>🕒 Last updated:</span>
            <span style='font-size: 1.3rem; color: #2b9348; font-weight: bold;'>{last_updated_time}</span>
        </div>
    """, unsafe_allow_html=True)

    # --- Removed countdown and auto-refresh info for silent refresh ---
    # Keep the rest of the dashboard as is

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
                if count == 0:  # Explicitly check for empty tables first
                    status = "Empty"
                else:
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

    # Define status categories
    ok_statuses = ['OK', 'OK-ColumnConditionMet']
    # Keywords to identify warning/empty statuses
    warning_keywords = ['Warn', 'Empty']

    healthy_tables_count = 0
    warning_tables_count = 0
    error_tables_count = 0

    if not table_stats.empty:
        healthy_tables_count = len(
            table_stats[table_stats['Status'].isin(ok_statuses)])

        # Count warning tables: contains a warning keyword, is not an error, and is not healthy
        warning_tables_count = len(table_stats[
            table_stats['Status'].apply(lambda x: isinstance(x, str) and any(keyword in x for keyword in warning_keywords)) &
            ~table_stats['Status'].str.startswith('Error', na=False) &
            ~table_stats['Status'].isin(ok_statuses)
        ])
        # Ensure 'Empty' tables that are not also 'Error' or 'OK' are counted as warnings if not already.
        # The above logic should catch 'Empty' if it's not an error or OK.
        # If an 'Empty' table could also be 'OK' or 'Error' by some logic, this might need refinement.
        # For now, assuming 'Empty' is a type of warning if not an error.

        error_tables_count = len(
            table_stats[table_stats['Status'].str.startswith('Error', na=False)])

        # Adjust healthy_tables_count if a table is counted in multiple categories (e.g. an empty table that is not an error or warning otherwise)
        # The current logic tries to make categories exclusive.
        # Total monitored tables
        total_monitored_tables = len(table_stats)
        # Sanity check: healthy + warning + error should ideally be <= total_monitored_tables
        # If a table is e.g. 'Empty' and also 'Warn-ColumnConditionNotMet', it's one warning.
        # The current warning_tables_count logic should handle this by checking for keywords.

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
        st.metric("✅ Healthy Tables", healthy_tables_count)
    with col2:
        st.metric("⚠️ Warning Tables", warning_tables_count,
                  delta=f"{warning_tables_count} tables" if warning_tables_count > 0 else None,
                  delta_color="inverse")
    with col3:
        st.metric("❌ Error Tables", error_tables_count,
                  delta=f"{error_tables_count} tables" if error_tables_count > 0 else None,
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
    # Removed st.columns(2) for vertical layout

    # Recent Job Failures
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
        st.info("No recent job failures to display.")

    st.markdown("---")  # Adding a separator for clarity

    # Table Issues
    st.markdown("### ⚠️ Table Issues")
    if not table_stats.empty:
        # Filter for tables that are not 'OK' or 'OK-ColumnConditionMet'
        issue_tables = table_stats[~table_stats['Status'].isin(ok_statuses)]
        if not issue_tables.empty:
            # Add filters at the top of the Table Issues section
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=['Empty', 'Warn-LowCount', 'Warn-HighCount',
                             'Warn-ColumnConditionNotMet', 'Error'],
                    default=[],
                    key="table_issues_status_filter"
                )
            with filter_col2:
                db_filter = st.multiselect(
                    "Filter by Database",
                    options=issue_tables['Database'].unique(
                    ).tolist(),  # Convert to list
                    default=[],
                    key="table_issues_db_filter"
                )

            # Apply filters
            filtered_tables = issue_tables.copy()
            if status_filter:
                filtered_tables = filtered_tables[filtered_tables['Status'].isin(
                    status_filter)]
            if db_filter:
                filtered_tables = filtered_tables[filtered_tables['Database'].isin(
                    db_filter)]

            for _, table in filtered_tables.iterrows():
                warning_message = f"{table['Database']}.{table['Table']} - {table['Status']}"

                # Get affected rows based on status type
                if table['Table'] == "MoveFrames" and "ColumnCondition" in table['Status']:
                    conn = get_connection(table['Database'])
                    cursor = conn.cursor()
                    try:
                        # First show the count with proper date comparison
                        query_count = """
                        SELECT COUNT(*) 
                        FROM [{0}].[dbo].[{1}] 
                        WHERE CAST(MoveDate AS DATE) = CAST(GETDATE() AS DATE)
                        AND Processed = 0
                        """.format(table['Database'], table['Table'])
                        cursor.execute(query_count)
                        affected_count = cursor.fetchone()[0]
                        warning_message += f" ({affected_count} unprocessed records)"

                        # Then get the actual rows with correct columns
                        if affected_count > 0:
                            query_rows = """
                            SELECT MoveFramesID, FrameNumber, ShopOrderNumber, MoveDate, 
                                   CAST(MoveDate AS DATE) as MoveDateOnly, Processed
                            FROM [{0}].[dbo].[{1}] 
                            WHERE CAST(MoveDate AS DATE) = CAST(GETDATE() AS DATE)
                            AND Processed = 0
                            ORDER BY MoveDate DESC
                            """.format(table['Database'], table['Table'])
                            df = pd.read_sql(query_rows, conn)

                            # Add column filtering
                            if not df.empty:
                                st.warning(warning_message)

                                # Add column filters
                                col_filters = st.expander(
                                    "Column Filters", expanded=False)
                                with col_filters:
                                    filter_cols = st.multiselect(
                                        "Filter by columns",
                                        options=df.columns.tolist(),  # Convert to list
                                        default=[]
                                    )

                                    filtered_df = df.copy()
                                    for col in filter_cols:
                                        if col in df.columns:
                                            # Convert unique values to list and handle non-string types
                                            unique_vals = df[col].unique()
                                            if not isinstance(unique_vals[0], str):
                                                unique_vals = [
                                                    str(val) for val in unique_vals]
                                            else:
                                                unique_vals = unique_vals.tolist()

                                            selected_vals = st.multiselect(
                                                f"Select {col} values",
                                                options=unique_vals,
                                                default=unique_vals
                                            )

                                            # Convert selected values to appropriate type for comparison
                                            if not isinstance(df[col].iloc[0], str):
                                                selected_vals = [type(df[col].iloc[0])(
                                                    val) for val in selected_vals]

                                            filtered_df = filtered_df[filtered_df[col].isin(
                                                selected_vals)]

                                    st.dataframe(
                                        filtered_df, use_container_width=True)
                    finally:
                        if cursor:
                            cursor.close()
                        if conn:
                            conn.close()
                elif table['Status'] == "Empty":
                    warning_message += " (0 rows)"
                    st.warning(warning_message)
                elif "LowCount" in table['Status'] or "HighCount" in table['Status']:
                    conn = get_connection(table['Database'])
                    try:
                        query = f"""
                        SELECT * 
                        FROM [{table['Database']}].[dbo].[{table['Table']}]
                        {'TOP 1000' if 'HighCount' in table['Status'] else ''}
                        """
                        df = pd.read_sql(query, conn)

                        if "LowCount" in table['Status']:
                            warning_message += f" (Current: {table['Row Count']} rows, Required: {table['Min Rows']} rows)"
                        else:
                            warning_message += f" (Current: {table['Row Count']} rows, Maximum: {table['Max Rows']} rows)"

                        st.warning(warning_message)

                        # Add column filters
                        if not df.empty:
                            col_filters = st.expander(
                                "Column Filters", expanded=False)
                            with col_filters:
                                filter_cols = st.multiselect(
                                    "Filter by columns",
                                    options=df.columns.tolist(),  # Convert to list
                                    default=[]
                                )

                                filtered_df = df.copy()
                                for col in filter_cols:
                                    if col in df.columns:
                                        # Convert unique values to list and handle non-string types
                                        unique_vals = df[col].unique()
                                        if not isinstance(unique_vals[0], str):
                                            unique_vals = [
                                                str(val) for val in unique_vals]
                                        else:
                                            unique_vals = unique_vals.tolist()

                                        selected_vals = st.multiselect(
                                            f"Select {col} values",
                                            options=unique_vals,
                                            default=unique_vals
                                        )

                                        # Convert selected values to appropriate type for comparison
                                        if not isinstance(df[col].iloc[0], str):
                                            selected_vals = [type(df[col].iloc[0])(
                                                val) for val in selected_vals]

                                        filtered_df = filtered_df[filtered_df[col].isin(
                                            selected_vals)]

                            st.dataframe(
                                filtered_df, use_container_width=True)

                            if "HighCount" in table['Status'] and int(table['Row Count']) > 1000:
                                st.info("Showing first 1000 rows only")
                    finally:
                        if conn:
                            conn.close()
                elif "Error" in table['Status']:
                    warning_message += f" (Error accessing table)"
                    st.error(warning_message)
                else:
                    warning_message += f" (Affected rows: {table['Row Count']})"
                    st.warning(warning_message)
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
    if 'auto_refresh' not in st.session_state:
        st.session_state.auto_refresh = False
    if 'last_auto_refresh' not in st.session_state:
        st.session_state.last_auto_refresh = time.time()
    # For table editing
    if 'edit_selected_db' not in st.session_state:
        st.session_state.edit_selected_db = None
    if 'edit_selected_tables' not in st.session_state:
        st.session_state.edit_selected_tables = []
    if 'edit_trigger' not in st.session_state:
        st.session_state.edit_trigger = False
    # For managing expanded details sections (optional, expanders manage their own state by key)
    # if 'expanded_table_detail_key' not in st.session_state:
    #     st.session_state.expanded_table_detail_key = None
    # if 'expanded_job_detail_key' not in st.session_state:
    #     st.session_state.expanded_job_detail_key = None


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
            try:
                # Get table specific thresholds
                table_min = row['min_rows'] if pd.notna(
                    row['min_rows']) else None
                table_max = row['max_rows'] if pd.notna(
                    row['max_rows']) else None
                table_col_min_match = row['column_min_match_count'] if pd.notna(
                    row['column_min_match_count']) else 1

                table_min_dict = {
                    row['table_name']: table_min} if table_min is not None else {}
                table_max_dict = {
                    row['table_name']: table_max} if table_max is not None else {}
                table_col_min_match_dict = {
                    row['table_name']: table_col_min_match}

                # Get table status
                check_result_df = check_selected_tables(
                    row["db_name"], [row["table_name"]], table_min_dict, table_max_dict, table_col_min_match_dict)

                count = 0
                status = "Error"
                if not check_result_df.empty:
                    count = int(
                        check_result_df.iloc[0]["Rows"]) if check_result_df.iloc[0]["Rows"].isdigit() else 0
                    if count == 0:  # Explicitly check for empty tables first
                        status = "Empty"
                    else:
                        status = check_result_df.iloc[0]["Status"]

                # Get size info
                size_info = get_table_size_info(
                    row["db_name"], row["table_name"])
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

                # Special handling for MoveFrames unprocessed records
                if row["table_name"] == "MoveFrames":
                    table_column_configs = load_column_config(
                        row["db_name"], row["table_name"])
                    if not table_column_configs.empty:
                        # Check if we're monitoring Processed=0
                        processed_config = table_column_configs[
                            (table_column_configs["column_name"] == "Processed") &
                            (table_column_configs["condition_value"] == "0")
                        ]
                        if not processed_config.empty:
                            conn = get_connection(row["db_name"])
                            cursor = conn.cursor()
                            try:
                                # Count unprocessed records for today
                                query = """
                                SELECT COUNT(*) 
                                FROM [{0}].[dbo].[MoveFrames] 
                                WHERE CAST(MoveDate AS DATE) = CAST(GETDATE() AS DATE)
                                AND Processed = 0
                                """.format(row["db_name"])
                                cursor.execute(query)
                                unprocessed_count = cursor.fetchone()[0]

                                if unprocessed_count > 0:
                                    status = "Warn-UnprocessedRecords"
                                    details = f"Database: {row['db_name']}\n"
                                    details += f"Table: {row['table_name']}\n"
                                    details += f"Unprocessed Records: {unprocessed_count}\n"
                                    details += f"Date: {datetime.now().strftime('%Y-%m-%d')}\n"

                                    log_alert(
                                        alert_type="Table",
                                        source_type="Unprocessed Records",
                                        source_name=f"{row['db_name']}.{row['table_name']}",
                                        status=status,
                                        message=f"Found {unprocessed_count} unprocessed records in {row['table_name']} for today",
                                        details=details
                                    )
                            finally:
                                if cursor:
                                    cursor.close()
                                if conn:
                                    conn.close()

                # Log alerts for other table issues
                elif status != "OK":
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
            except Exception as e:
                print(
                    f"Error processing table {row['db_name']}.{row['table_name']}: {str(e)}")
                continue

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
