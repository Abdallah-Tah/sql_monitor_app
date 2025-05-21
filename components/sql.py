import streamlit as st
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os
from components.db import load_column_config


def get_windows_user():
    return os.getenv('USERNAME')


# Removed @st.cache_resource(ttl=300) to ensure a fresh connection is always provided
def get_connection(db=None):
    """Get a database connection. Each call returns a new connection to avoid sharing cached closed connections."""
    conn = None  # Initialize conn
    try:
        # Simplified connection string for diagnostics
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=10.1.1.88;"
            "Trusted_Connection=yes;"
            # "TrustServerCertificate=yes;" # Temporarily removed
            # "KeepAlive=30;" # Temporarily removed
            # "KeepAliveInterval=1;" # Temporarily removed
        )
        if db:
            conn_str += f"DATABASE={db};"

        # DEBUG
        print(
            f"DEBUG: Attempting to connect with simplified string: {conn_str}")
        conn = pyodbc.connect(conn_str)
        # DEBUG
        print(f"DEBUG: Connected with simplified string. conn object: {conn}")

        # Test the newly created connection
        try:
            print(f"DEBUG: Testing connection {conn}")  # DEBUG
            with conn.cursor() as test_cursor:
                test_cursor.execute("SELECT 1")
            print(f"DEBUG: Connection test PASSED for {conn}")  # DEBUG
        except pyodbc.Error as test_e:
            # DEBUG
            print(
                f"DEBUG: Connection test FAILED for {conn} (simplified string): {test_e}")
            if conn:
                try:
                    conn.close()
                except pyodbc.Error:
                    pass
            raise  # Re-raise the error from the connection test

        # DEBUG
        print(f"DEBUG: Returning OPEN connection from get_connection: {conn}")
        return conn  # Return the tested, open connection

    except pyodbc.Error as e:
        # DEBUG
        print(
            f"DEBUG: Connection Error in get_connection (simplified string) for DB '{db if db else 'default'}': {str(e)}")
        if conn:  # If connection object exists, try to close it
            try:
                conn.close()
            except pyodbc.Error as close_e:
                # DEBUG
                print(
                    f"DEBUG: Error closing connection in except block: {close_e}")
        raise


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_databases():
    conn = None
    cursor = None
    try:
        conn = get_connection()  # Updated to expect a single connection object
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
        result = [row[0] for row in cursor.fetchall()]
        return result
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_tables(db):
    conn = None
    cursor = None
    try:
        conn = get_connection(db)  # Updated
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
        result = [row[0] for row in cursor.fetchall()]
        return result
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass

# Removed @st.cache_data decorator to prevent caching of database connections


def check_selected_tables(db, tables, min_rows_dict=None, max_rows_dict=None, column_min_match_count_dict=None):
    if not tables:
        return pd.DataFrame([])

    results = []
    conn = None
    cursor = None

    try:
        conn = get_connection(db)
        cursor = conn.cursor()

        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cursor.fetchone()[0]

                min_rows = min_rows_dict.get(table) if min_rows_dict else None
                max_rows = max_rows_dict.get(table) if max_rows_dict else None
                min_match_count_for_column_conditions = column_min_match_count_dict.get(
                    table, 1) if column_min_match_count_dict else 1

                # Determine status based on row count thresholds first
                if count == 0:
                    status = "Empty"
                elif min_rows is not None and count < min_rows:
                    status = "Warn-LowCount"
                elif max_rows is not None and count > max_rows:
                    status = "Warn-HighCount"
                else:
                    status = "OK"

                # Check column conditions if configured
                column_conditions_met = True  # Assume true if no conditions or all met
                column_condition_details = {}
                # Load column configurations for the current table
                table_column_configs_df = load_column_config(db, table)
                if not table_column_configs_df.empty:
                    # Convert DataFrame to list of dicts for check_column_conditions
                    table_column_configs_list = table_column_configs_df.to_dict(
                        'records')
                    column_condition_results = check_column_conditions(
                        db, table, table_column_configs_list, min_match_count_for_column_conditions)

                    column_condition_details = column_condition_results  # Store detailed results
                    # Overall status based on column conditions
                    # For UploadLogs, it's if ANY condition set (e.g. status+date) is met.
                    # For others, it's if ALL configured conditions are met.
                    if table == "UploadLogs":
                        # If any condition (or combined set) is true, it's met.
                        # The check_column_conditions for UploadLogs returns True for a column if its specific rule (or combined rule) is met.
                        # So, if any of the results are True, the overall column condition is considered met for UploadLogs.
                        column_conditions_met = any(
                            column_condition_results.values())
                    else:
                        # For other tables, all individual conditions must be met.
                        column_conditions_met = all(
                            column_condition_results.values())

                    if not column_conditions_met and status == "OK":  # Only override if row count was OK
                        status = "Warn-ColumnConditionNotMet"
                    elif column_conditions_met and status == "OK":
                        status = "OK-ColumnConditionMet"  # More specific OK status
                    elif not column_conditions_met and status.startswith("Warn"):
                        status += ";ColCondNotMet"  # Append to existing warning

                results.append({
                    "Database": db,
                    "Table": table,
                    "Rows": str(count),
                    "Status": status,
                    "Min Rows": str(min_rows) if min_rows is not None else "None",
                    "Max Rows": str(max_rows) if max_rows is not None else "None",
                    "Column Conditions": column_condition_details  # Store detailed results
                })
            except Exception as e:
                print(f"Error checking table {db}.{table}: {str(e)}")
                results.append({
                    "Database": db,
                    "Table": table,
                    "Rows": "-",
                    "Status": f"Error: {str(e)}",
                    "Min Rows": "None",
                    "Max Rows": "None",
                    "Column Conditions": {}
                })
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

    return pd.DataFrame(results)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_table_size_info(db, table_name):
    conn = None
    cursor = None
    try:
        conn = get_connection(db)
        cursor = conn.cursor()
        # Using sp_spaceused to get table size information
        # Ensure the database context is correct for sp_spaceused
        cursor.execute(f"USE {db};")
        query = f"""
            EXEC sp_spaceused N'{table_name}';
        """
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            # sp_spaceused returns size like '123 KB'. Need to parse it.
            data_size_str = row[3]
            index_size_str = row[4]

            data_kb = float(data_size_str.split(
                ' ')[0]) if data_size_str else 0
            index_kb = float(index_size_str.split(
                ' ')[0]) if index_size_str else 0
            return {"data_kb": data_kb, "index_kb": index_kb}
        return {"data_kb": 0, "index_kb": 0}
    except pyodbc.Error as e:
        # Handle cases where the table might not exist or other SQL errors
        print(f"Error getting size for table {db}.{table_name}: {str(e)}")
        return {"data_kb": 0, "index_kb": 0}  # Return default/error state
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=1)  # Cache for just 1 second to ensure fresh data
def get_job_history(hours_back=24, detect_anomalies=True):
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')  # Updated
        cursor = conn.cursor()
        excluded_jobs = get_excluded_jobs()
        placeholders = ','.join('?' * len(excluded_jobs))

        query = f"""
        SELECT 
            j.name AS job_name,
            h.run_date,
            h.run_time,
            h.run_duration,
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'Running'
            END AS status,
            h.message
        FROM sysjobs j 
        INNER JOIN sysjobhistory h ON j.job_id = h.job_id 
        WHERE h.step_id = 0
        AND j.name NOT IN ({placeholders})
        AND DATEDIFF(HOUR, 
            DATEADD(S, 
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 1, 2)) * 3600 +
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 3, 2)) * 60 +
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 5, 2)),
                CONVERT(DATETIME, CAST(h.run_date AS VARCHAR))
            ), 
            GETDATE()) <= ?
        ORDER BY h.run_date DESC, h.run_time DESC
        """

        params = excluded_jobs + [hours_back]
        cursor.execute(query, params)
        columns = ['Job Name', 'Run Date', 'Run Time',
                   'Duration', 'Status', 'Message']
        results = []

        # Create a dictionary to store duration stats by job name
        job_stats = {}
        # Keep track of job occurrences to avoid calculating stats multiple times
        processed_jobs = set()

        for row in cursor.fetchall():
            job_name = row[0]
            run_date = str(row[1])
            date = datetime(
                year=int(run_date[0:4]),
                month=int(run_date[4:6]),
                day=int(run_date[6:8])
            )

            run_time = str(row[2]).zfill(6)
            time = datetime.strptime(run_time, '%H%M%S').time()

            duration = str(row[3]).zfill(6)
            hours = int(duration[0:2])
            minutes = int(duration[2:4])
            seconds = int(duration[4:6])
            duration_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            # Convert duration to seconds for comparison
            duration_seconds = hours * 3600 + minutes * 60 + seconds

            # Calculate duration anomaly status
            duration_status = 'Normal'
            # Only check anomalies for successful jobs
            if detect_anomalies and row[4] == 'Succeeded':
                if job_name not in job_stats and job_name not in processed_jobs:
                    # Fetch stats for this job
                    job_stats[job_name] = get_job_duration_stats(job_name)
                    processed_jobs.add(job_name)

                if job_name in job_stats and job_stats[job_name]['sample_count'] > 0:
                    stats = job_stats[job_name]
                    # Check if duration is an outlier (> 2 std deviations from mean)
                    if stats['std_seconds'] > 0:  # Avoid division by zero
                        z_score = abs(
                            duration_seconds - stats['avg_seconds']) / stats['std_seconds']
                        if z_score > 2:
                            if duration_seconds > stats['avg_seconds']:
                                duration_status = 'Slow'
                            else:
                                duration_status = 'Fast'

            results.append({
                'Job Name': job_name,
                'Run Date': date.strftime('%Y-%m-%d'),
                'Run Time': time.strftime('%H:%M:%S'),
                'Duration': duration_str,
                'Duration Seconds': duration_seconds,
                'Duration Status': duration_status,
                'Status': row[4],
                'Message': row[5] or ''
            })

        return pd.DataFrame(results)
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_job_details(job_name):
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')  # Updated
        cursor = conn.cursor()
        query = """
        SELECT 
            j.name AS job_name,
            SUSER_SNAME(j.owner_sid) as job_owner,
            j.description,
            j.enabled,
            j.date_created,
            j.date_modified,
            CASE 
                WHEN ja.start_execution_date IS NOT NULL AND ja.stop_execution_date IS NULL THEN 'Running'
                WHEN j.enabled = 1 THEN 'Enabled'
                ELSE 'Disabled'
            END as current_status
        FROM sysjobs j
        LEFT JOIN sysjobactivity ja ON j.job_id = ja.job_id
            AND ja.session_id = (SELECT MAX(session_id) FROM sysjobactivity)
        WHERE j.name = ?
        """

        cursor.execute(query, job_name)
        row = cursor.fetchone()

        if row:
            return {
                'Job Name': row[0],
                'Owner': row[1],
                'Description': row[2],
                'Enabled': 'Yes' if row[3] == 1 else 'No',
                'Created': row[4].strftime('%Y-%m-%d %H:%M:%S'),
                'Last Modified': row[5].strftime('%Y-%m-%d %H:%M:%S'),
                'Current Status': row[6]
            }
        return None
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_job_steps(job_name):
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')  # Updated
        cursor = conn.cursor()
        query = """
        SELECT 
            s.step_id,
            s.step_name,
            s.subsystem,
            CASE 
                WHEN h.run_status = 0 THEN 'Failed'
                WHEN h.run_status = 1 THEN 'Succeeded'
                WHEN h.run_status = 2 THEN 'Retry'
                WHEN h.run_status = 3 THEN 'Canceled'
                WHEN h.run_status = 4 THEN 'In Progress'
                ELSE 'Unknown'
            END as last_run_status,
            CONVERT(VARCHAR, DATEADD(S, 
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 1, 2)) * 3600 +
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 3, 2)) * 60 +
                CONVERT(INT, SUBSTRING(CAST(h.run_time AS VARCHAR), 5, 2)),
                CONVERT(DATETIME, CAST(h.run_date AS VARCHAR))
            ), 120) as last_run_time,
            h.run_duration as duration_seconds
        FROM sysjobs j
        INNER JOIN sysjobsteps s ON j.job_id = s.job_id
        LEFT JOIN sysjobhistory h ON j.job_id = h.job_id 
            AND s.step_id = h.step_id
            AND h.instance_id = (
                SELECT MAX(instance_id)
                FROM sysjobhistory h2
                WHERE h2.job_id = j.job_id 
                AND h2.step_id = s.step_id
                AND DATEADD(YEAR, 1, 
                    DATEADD(S, 
                        CONVERT(INT, SUBSTRING(CAST(h2.run_time AS VARCHAR), 1, 2)) * 3600 +
                        CONVERT(INT, SUBSTRING(CAST(h2.run_time AS VARCHAR), 3, 2)) * 60 +
                        CONVERT(INT, SUBSTRING(CAST(h2.run_time AS VARCHAR), 5, 2)),
                        CONVERT(DATETIME, CAST(h2.run_date AS VARCHAR))
                    )
                ) > GETDATE()
            )
        WHERE j.name = ?
        ORDER BY s.step_id
        """

        cursor.execute(query, job_name)
        results = []

        for row in cursor.fetchall():
            duration = str(row[5]).zfill(6) if row[5] else "000000"
            hours = int(duration[0:2])
            minutes = int(duration[2:4])
            seconds = int(duration[4:6])

            results.append({
                'Step ID': row[0],
                'Step Name': row[1],
                'Type': row[2],
                'Last Status': row[3],
                'Last Run Time': row[4] if row[4] else '',
                'Duration': f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            })

        return pd.DataFrame(results)
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


def get_excluded_jobs():
    return [
        'BSQL08-SurgeCurrentTemperatu-SurgeCurrentTemperatu-SQL-DEVELOP-43',
        'BSQL08-PP685Data-PP685Data-SQL-DEVELOP-27',
        'BSQL08-EngineeringTestingOnly-10',
        'BSQL08-TBC_AccessDB-12',
        'BSQL08-PP685Data-9',
        'BSQL08-EngineeringTestingOnl-EngineeringTestingOnl-SQL-DEVELOP-31',
        'BSQL08-ProcessData-ProcessData-SQL-DEVELOP',
        'BSQL08-Weibull-15',
        'BSQL08-ProcessData',
        'BSQL08-Weibull-Weibull-SQL-DEVELOP-48',
        'BSQL08-SurgeCurrentTemperatureTester-14',
        'BSQL08-TBC_AccessDB-TBC_-SQL-DEVELOP-37'
    ]


@st.cache_data(ttl=1)  # Cache for just 1 second to ensure fresh data
def get_all_jobs():
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')  # Updated
        cursor = conn.cursor()
        excluded_jobs = get_excluded_jobs()

        placeholders = ','.join('?' * len(excluded_jobs))

        query = f"""
        SELECT 
            j.name AS job_name,
            SUSER_SNAME(j.owner_sid) as job_owner,
            CASE 
                WHEN ja.start_execution_date IS NOT NULL AND ja.stop_execution_date IS NULL THEN 'Running'
                WHEN j.enabled = 1 THEN 'Enabled'
                ELSE 'Disabled'
            END as current_status,
            CONVERT(VARCHAR, h.run_date, 120) as last_run,
            CASE h.run_status
                WHEN 0 THEN 'Failed'
                WHEN 1 THEN 'Succeeded'
                WHEN 2 THEN 'Retry'
                WHEN 3 THEN 'Canceled'
                WHEN 4 THEN 'Running'
            END AS last_status,
            CONVERT(VARCHAR, js.next_run_date, 120) as next_run
        FROM sysjobs j
        LEFT JOIN sysjobactivity ja ON j.job_id = ja.job_id
            AND ja.session_id = (SELECT MAX(session_id) FROM sysjobactivity)
        LEFT JOIN sysjobhistory h ON j.job_id = h.job_id 
            AND h.instance_id = (
                SELECT MAX(instance_id)
                FROM sysjobhistory h2
                WHERE h2.job_id = j.job_id
            )
        LEFT JOIN sysjobschedules js ON j.job_id = js.job_id
        WHERE j.name NOT IN ({placeholders})
        AND (
            DATEADD(YEAR, 1, 
                DATEADD(S, 
                    CONVERT(INT, SUBSTRING(CAST(COALESCE(h.run_time, '000000') AS VARCHAR), 1, 2)) * 3600 +
                    CONVERT(INT, SUBSTRING(CAST(COALESCE(h.run_time, '000000') AS VARCHAR), 3, 2)) * 60 +
                    CONVERT(INT, SUBSTRING(CAST(COALESCE(h.run_time, '000000') AS VARCHAR), 5, 2)),
                    CONVERT(DATETIME, CAST(COALESCE(h.run_date, '19000101') AS VARCHAR))
                )
            ) > GETDATE()
            OR ja.start_execution_date IS NOT NULL
        )
        ORDER BY j.name
        """

        cursor.execute(query, excluded_jobs)
        results = []

        for row in cursor.fetchall():
            results.append({
                'Job Name': row[0],
                'Owner': row[1],
                'Status': row[2],
                'Last Run': row[3] if row[3] else '',
                'Last Run Status': row[4] if row[4] else '',
                'Next Run': row[5] if row[5] else ''
            })

        return pd.DataFrame(results)
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=1)  # Cache for just 1 second to ensure fresh data
def get_active_jobs():
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')  # Updated
        cursor = conn.cursor()
        excluded_jobs = get_excluded_jobs()
        placeholders = ','.join('?' * len(excluded_jobs))

        query = f"""
        SELECT 
            j.name AS job_name,
            ja.start_execution_date,
            DATEDIFF(MINUTE, ja.start_execution_date, GETDATE()) as duration_minutes,
            h.step_id as current_step,
            s.step_name
        FROM sysjobs j 
        INNER JOIN sysjobactivity ja ON j.job_id = ja.job_id
        LEFT JOIN sysjobhistory h ON j.job_id = h.job_id 
            AND h.instance_id = (
                SELECT MAX(instance_id)
                FROM sysjobhistory h2
                WHERE h2.job_id = j.job_id
            )
        LEFT JOIN sysjobsteps s ON j.job_id = s.job_id 
            AND h.step_id = s.step_id
        WHERE ja.stop_execution_date IS NULL 
        AND ja.start_execution_date IS NOT NULL
        AND ja.session_id = (SELECT MAX(session_id) FROM sysjobactivity)
        AND j.name NOT IN ({placeholders})
        ORDER BY ja.start_execution_date DESC
        """

        cursor.execute(query, excluded_jobs)
        results = []

        for row in cursor.fetchall():
            results.append({
                'Job Name': row[0],
                'Start Time': row[1].strftime('%Y-%m-%d %H:%M:%S') if row[1] else '',
                'Duration (mins)': row[2] if row[2] else 0,
                'Current Step': row[3] if row[3] else 0,
                'Step Name': row[4] if row[4] else ''
            })

        return pd.DataFrame(results)
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_job_duration_stats(job_name, sample_size=10):
    conn = None
    cursor = None
    try:
        conn = get_connection('msdb')
        cursor = conn.cursor()
        query = f"""
        SELECT TOP {sample_size}
            h.run_duration
        FROM sysjobs j 
        INNER JOIN sysjobhistory h ON j.job_id = h.job_id 
        WHERE h.step_id = 0
        AND j.name = ?
        AND h.run_status = 1
        ORDER BY h.run_date DESC, h.run_time DESC
        """

        cursor.execute(query, [job_name])

        durations = []

        for row in cursor.fetchall():
            duration = str(row[0]).zfill(6)
            hours = int(duration[0:2])
            minutes = int(duration[2:4])
            seconds = int(duration[4:6])
            total_seconds = hours * 3600 + minutes * 60 + seconds
            durations.append(total_seconds)

        if durations:
            import numpy as np
            return {
                'avg_seconds': np.mean(durations),
                'std_seconds': np.std(durations),
                'sample_count': len(durations),
                'min_seconds': min(durations),
                'max_seconds': max(durations)
            }

        return {
            'avg_seconds': 0,
            'std_seconds': 0,
            'sample_count': 0,
            'min_seconds': 0,
            'max_seconds': 0
        }

    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_table_columns(db, table):
    conn = None
    cursor = None
    try:
        conn = get_connection(db)
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
        """, [table])
        return [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
    finally:
        if cursor:
            try:
                cursor.close()
            except pyodbc.Error:
                pass
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass


def check_column_conditions(db, table, column_configs, min_match_count=1):
    """
    Check if table data meets the column conditions.
    Returns: dict with column names as keys and boolean results (met/not met) as values.
    """
    if not column_configs:
        return {}

    # Initialize all to False
    results = {config["column_name"]: False for config in column_configs}
    conn = None
    cursor = None

    try:
        conn = get_connection(db)
        cursor = conn.cursor()

        # Special handling for MoveFrames table to check for unprocessed records
        if table == "MoveFrames":
            # First check if we have both Processed and MoveDate conditions
            processed_config = next(
                (cfg for cfg in column_configs if cfg["column_name"] == "Processed"), None)
            date_config = next(
                (cfg for cfg in column_configs if cfg["column_name"] == "MoveDate"), None)

            if processed_config and date_config:
                # Build query to check for records matching the conditions
                query = """
                DECLARE @Today DATE = CAST(GETDATE() AS DATE);
                SELECT COUNT(*) 
                FROM [{0}].[dbo].[MoveFrames] 
                WHERE CAST(MoveDate AS DATE) = @Today
                AND Processed = ?
                """.format(db)

                cursor.execute(query, [processed_config["condition_value"]])
                matching_count = cursor.fetchone()[0]

                # For MoveFrames, finding records with Processed = 0 means the condition is NOT met
                # So if we're checking for Processed = 0 and we find any records, that's a failure state
                condition_met = False if (
                    processed_config["condition_value"] == "0" and matching_count > 0) else True

                # Update results for both columns to reflect unprocessed records state
                results[processed_config["column_name"]] = condition_met
                results[date_config["column_name"]] = condition_met

                return results

        # Standard handling for other tables
        where_clauses = []
        all_params = []

        for config in column_configs:
            column = config["column_name"]
            cond_type = config["condition_type"]
            value = config["condition_value"]

            if cond_type == "equals":
                where_clauses.append(f"{column} = ?")
                all_params.append(value)
            elif cond_type == "not_equals":
                where_clauses.append(f"{column} <> ?")
                all_params.append(value)
            elif cond_type == "in":
                values = [v.strip() for v in value.split(",")]
                placeholders = ",".join("?" * len(values))
                where_clauses.append(f"{column} IN ({placeholders})")
                all_params.extend(values)
            elif cond_type == "date_equals_today":
                where_clauses.append(
                    f"CAST({column} AS DATE) = CAST(GETDATE() AS DATE)")
            elif cond_type == "date_greater_than":
                where_clauses.append(
                    f"CAST({column} AS DATE) > CAST(? AS DATE)")
                all_params.append(value)
            elif cond_type == "date_less_than":
                where_clauses.append(
                    f"CAST({column} AS DATE) < CAST(? AS DATE)")
                all_params.append(value)

        if where_clauses:
            combined_where = " AND ".join(where_clauses)
            query = f"""
            DECLARE @Today DATE = CAST(GETDATE() AS DATE);
            SELECT COUNT(*) FROM [{table}] WHERE {combined_where}
            """

            if all_params:
                cursor.execute(query, all_params)
            else:
                cursor.execute(query)

            count = cursor.fetchone()[0]

            total_query = f"SELECT COUNT(*) FROM [{table}]"
            cursor.execute(total_query)
            total = cursor.fetchone()[0]

            # Determine if conditions are met based on min_match_count
            condition_met = False  # Default to False
            is_date_equals_today_present = any(
                cfg["condition_type"] == "date_equals_today" for cfg in column_configs)

            if table == "UploadLogs" and is_date_equals_today_present and count == 0:
                condition_met = True  # Special case: 0 matches for today's UploadLogs is OK
            else:
                # Standard logic using the configured min_match_count
                if min_match_count == 0:
                    # If 0, all rows in the table must match the conditions
                    # An empty table (total=0) cannot satisfy this
                    condition_met = (count == total and total > 0)
                else:  # min_match_count > 0
                    # If > 0, at least 'min_match_count' rows must match
                    condition_met = (count >= min_match_count)

            for config_item in column_configs:
                results[config_item["column_name"]] = condition_met

    except Exception as e:
        print(f"Error in check_column_conditions for {table}: {str(e)}")
        for config in column_configs:
            results[config["column_name"]] = False
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

    return results


def get_rows_for_processed_today(db, table_name, date_column_name, processed_column_name="Processed"):
    """
    Fetches all rows from a table where the date_column matches today's date
    and the processed_column is 1.
    """
    conn = None
    # cursor = None # Not needed as pd.read_sql handles cursor management
    try:
        conn = get_connection(db)
        # Basic validation for column/table names to ensure they are somewhat reasonable
        # This is not a full SQL injection proofing but a basic check.
        # Assumes names are simple identifiers, possibly with underscores.
        # SQL Server specific quoting with [] handles spaces or keywords if names are passed correctly.
        if not all(name.replace('_', '').replace('[', '').replace(']', '').isalnum() for name in [table_name, date_column_name, processed_column_name]):
            st.error(
                f"Invalid table or column name format provided: {table_name}, {date_column_name}, {processed_column_name}")
            return pd.DataFrame([])

        query = f"""
            SELECT *
            FROM [{table_name}]
            WHERE CAST([{date_column_name}] AS DATE) = CAST(GETDATE() AS DATE)
              AND [{processed_column_name}] = 1
        """
        df = pd.read_sql(query, conn)
        return df
    except pyodbc.Error as e:
        st.error(f"SQL Error fetching rows for {db}.{table_name}: {str(e)}")
        return pd.DataFrame([])
    except Exception as e:
        st.error(f"An unexpected error occurred while fetching rows: {str(e)}")
        return pd.DataFrame([])
    finally:
        # pd.read_sql usually handles connection closing, but explicit close is safer
        if conn:
            try:
                conn.close()
            except pyodbc.Error:
                pass
