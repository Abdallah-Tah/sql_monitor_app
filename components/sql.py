import streamlit as st
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os


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
        )
        if db:
            conn_str += f"DATABASE={db};"
        conn = pyodbc.connect(conn_str)
        return conn  # Ensure only the connection object is returned
    except pyodbc.Error as e:
        print(f"Connection Error on DB {db if db else 'default'}: {str(e)}")
        raise


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_databases():
    conn = get_connection()  # Updated to expect a single connection object
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return result


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_tables(db):
    conn = get_connection(db)  # Updated
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    result = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return result


@st.cache_data(ttl=300)  # Cache for 5 minutes
def check_selected_tables(db, tables):
    results = []
    conn = get_connection(db)  # Updated
    cursor = conn.cursor()
    try:
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
                count = cursor.fetchone()[0]
                results.append({"Database": db, "Table": table, "Rows": str(
                    count), "Status": "Empty" if count == 0 else "OK"})
            except Exception as e:
                results.append({"Database": db, "Table": table,
                               "Rows": "-", "Status": f"Error: {str(e)}"})
    finally:
        cursor.close()
    return pd.DataFrame(results)


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_job_history(hours_back=24):
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

        for row in cursor.fetchall():
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

            results.append({
                'Job Name': row[0],
                'Run Date': date.strftime('%Y-%m-%d'),
                'Run Time': time.strftime('%H:%M:%S'),
                'Duration': duration_str,
                'Status': row[4],
                'Message': row[5] or ''
            })

        return pd.DataFrame(results)
    finally:
        cursor.close()


@st.cache_data(ttl=300)  # Cache for 5 minutes
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
        cursor.close()
