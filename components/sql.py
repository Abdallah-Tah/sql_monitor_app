import streamlit as st
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def retry_on_connection_error(max_retries=3, delay=1):
    """Decorator to retry database operations on connection errors"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except pyodbc.Error as e:
                    if isinstance(e, pyodbc.OperationalError) and e.args[0] == '08S01':
                        retries += 1
                        if retries == max_retries:
                            logger.error(
                                f"Failed to execute {func.__name__} after {max_retries} retries: {str(e)}")
                            raise
                        logger.warning(
                            f"Connection error in {func.__name__}, retrying ({retries}/{max_retries})")
                        time.sleep(delay * retries)  # Exponential backoff
                    else:
                        logger.error(
                            f"Database error in {func.__name__}: {str(e)}")
                        raise
            return None
        return wrapper
    return decorator


def get_windows_user():
    return os.getenv('USERNAME')


@st.cache_resource  # Removed ttl=300, connection will be cached per session
def get_connection(db=None):
    try:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=10.1.1.88;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
            "KeepAlive=30;"  # Send keep-alive probe every 30 seconds of inactivity
            # Interval in seconds between keep-alive retransmissions if no response
            "KeepAliveInterval=1;"
            "Connection Timeout=30;"  # Added connection timeout
            "ConnectRetryCount=3;"    # Added retry count
            "ConnectRetryInterval=1;"  # Added retry interval
        )
        if db:
            conn_str += f"DATABASE={db};"
        conn = pyodbc.connect(conn_str)
        return conn  # Ensure only the connection object is returned
    except pyodbc.Error as e:
        logger.error(
            f"Connection Error on DB {db if db else 'default'}: {str(e)}")
        raise


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_databases():
    conn = get_connection()  # Updated to expect a single connection object
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return result


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_tables(db):
    conn = get_connection(db)  # Updated
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return result


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def check_selected_tables(db, tables, min_rows_dict=None, max_rows_dict=None):
    results = []
    conn = get_connection(db)  # Updated
    cursor = conn.cursor()
    try:
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cursor.fetchone()[0]

                # Get thresholds for this table
                min_rows = min_rows_dict.get(table) if min_rows_dict else None
                max_rows = max_rows_dict.get(table) if max_rows_dict else None

                # Determine status based on row count and thresholds
                if count == 0:
                    status = "Empty"
                elif min_rows is not None and count < min_rows:
                    status = "Warn-LowCount"
                elif max_rows is not None and count > max_rows:
                    status = "Warn-HighCount"
                else:
                    status = "OK"

                results.append({
                    "Database": db,
                    "Table": table,
                    "Rows": str(count),
                    "Status": status,
                    "Min Rows": str(min_rows) if min_rows is not None else "None",
                    "Max Rows": str(max_rows) if max_rows is not None else "None"
                })
            except Exception as e:
                results.append({
                    "Database": db,
                    "Table": table,
                    "Rows": "-",
                    "Status": f"Error: {str(e)}",
                    "Min Rows": "None",
                    "Max Rows": "None"
                })
    finally:
        cursor.close()
    return pd.DataFrame(results)


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_table_size_info(db, table_name):
    conn = get_connection(db)
    cursor = conn.cursor()
    try:
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
        logger.error(
            f"Error getting size for table {db}.{table_name}: {str(e)}")
        return {"data_kb": 0, "index_kb": 0}  # Return default/error state
    finally:
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_job_history(hours_back=24, detect_anomalies=True):
    conn = get_connection('msdb')  # Updated
    cursor = conn.cursor()
    try:
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
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_job_details(job_name):
    conn = get_connection('msdb')  # Updated
    cursor = conn.cursor()
    try:
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
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_job_steps(job_name):
    conn = get_connection('msdb')  # Updated
    cursor = conn.cursor()
    try:
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
        cursor.close()


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


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_all_jobs():
    conn = get_connection('msdb')  # Updated
    cursor = conn.cursor()
    try:
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
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_active_jobs():
    conn = get_connection('msdb')  # Updated
    cursor = conn.cursor()
    try:
        excluded_jobs = get_excluded_jobs()
        placeholders = ','.join('?' * len(excluded_jobs))

        query = f"""
        SELECT 
            j.name AS job_name,
            ja.start_execution_date,
            DATEDIFF(MINUTE, ja.start_execution_date, GETDATE()) as duration_minutes,
            COALESCE(h.step_id, 0) as current_step,
            COALESCE(s.step_name, 'Starting') as step_name
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
                # Convert to string and ensure consistent type
                'Current Step': str(row[3]) if pd.notna(row[3]) else '0',
                'Step Name': row[4] or 'Starting'
            })

        return pd.DataFrame(results)
    finally:
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
@retry_on_connection_error()
def get_job_duration_stats(job_name, sample_size=10):
    conn = get_connection('msdb')
    cursor = conn.cursor()
    try:
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
        cursor.close()
