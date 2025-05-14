import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os


def get_windows_user():
    return os.getenv('USERNAME')


def get_connection(db=None):
    try:
        windows_user = get_windows_user()
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=10.1.1.88;"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )
        if db:
            conn_str += f"DATABASE={db};"
        conn = pyodbc.connect(conn_str)
        return conn, windows_user
    except pyodbc.Error as e:
        print(f"Connection Error: {str(e)}")
        raise


def get_databases():
    conn, _ = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sys.databases WHERE database_id > 4")
    return [row[0] for row in cursor.fetchall()]


def get_tables(db):
    conn, _ = get_connection(db)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [row[0] for row in cursor.fetchall()]


def check_selected_tables(db, tables):
    results = []
    conn, _ = get_connection(db)
    cursor = conn.cursor()
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            results.append({"Database": db, "Table": table, "Rows": str(
                count), "Status": "Empty" if count == 0 else "OK"})
        except Exception as e:
            results.append({"Database": db, "Table": table,
                           "Rows": "-", "Status": f"Error: {str(e)}"})
    return pd.DataFrame(results)


def get_job_history(hours_back=24):
    conn, _ = get_connection('msdb')
    cursor = conn.cursor()

    query = """
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
    INNER JOIN sysjobhistory h 
    ON j.job_id = h.job_id 
    WHERE h.step_id = 0
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

    cursor.execute(query, hours_back)
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


def get_job_details(job_name):
    conn, _ = get_connection('msdb')
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


def get_job_steps(job_name):
    conn, _ = get_connection('msdb')
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
            WHERE h2.job_id = j.job_id AND h2.step_id = s.step_id
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


def get_all_jobs():
    conn, _ = get_connection('msdb')
    cursor = conn.cursor()

    query = """
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
    ORDER BY j.name
    """

    cursor.execute(query)
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


def get_active_jobs():
    conn, _ = get_connection('msdb')
    cursor = conn.cursor()

    query = """
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
    ORDER BY ja.start_execution_date DESC
    """

    cursor.execute(query)
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
