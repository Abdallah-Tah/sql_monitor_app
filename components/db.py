from sqlalchemy import create_engine, text
import pandas as pd
import os
import sqlite3

DB_PATH = "sqlite:///data/job_monitor.db"
engine = create_engine(DB_PATH)


def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS table_monitor_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            db_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            min_rows INTEGER DEFAULT NULL,
            max_rows INTEGER DEFAULT NULL,
            UNIQUE(db_name, table_name)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS table_check_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            db_name TEXT,
            table_name TEXT,
            check_time TEXT,
            row_count INTEGER,
            status TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS job_monitor_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT NOT NULL,
            UNIQUE(job_name)
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS job_monitor_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_name TEXT,
            check_time TEXT,
            status TEXT,
            last_run TEXT,
            next_run TEXT,
            message TEXT
        );
        """))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS alert_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_time TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_name TEXT NOT NULL,
            status TEXT NOT NULL,
            message TEXT,
            details TEXT
        );
        """))


def update_db_schema():
    """
    Check and update database schema to ensure all required columns exist.
    This handles cases where the database was created with an older schema.
    """
    # Connect directly with sqlite3 to check schema
    db_file = os.path.join(os.path.dirname(
        os.path.dirname(__file__)), "data", "job_monitor.db")
    if not os.path.exists(db_file):
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Update: Check and add columns for alert_log
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_log'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(alert_log)")
        columns = {row[1] for row in cursor.fetchall()}

        if 'alert_time' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN alert_time TEXT")
        if 'alert_type' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN alert_type TEXT")
        if 'source_type' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN source_type TEXT")
        if 'source_name' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN source_name TEXT")
        if 'status' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN status TEXT")
        if 'message' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN message TEXT")
        if 'details' not in columns:
            cursor.execute("ALTER TABLE alert_log ADD COLUMN details TEXT")

        conn.commit()

    # Also run the same existing logic for table_monitor_config
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='table_monitor_config'")
    if cursor.fetchone():
        cursor.execute("PRAGMA table_info(table_monitor_config)")
        columns = {row[1] for row in cursor.fetchall()}

        if 'min_rows' not in columns:
            cursor.execute(
                "ALTER TABLE table_monitor_config ADD COLUMN min_rows INTEGER DEFAULT NULL")
        if 'max_rows' not in columns:
            cursor.execute(
                "ALTER TABLE table_monitor_config ADD COLUMN max_rows INTEGER DEFAULT NULL")

        conn.commit()

    conn.close()


def save_table_config(db, tables, min_rows_dict=None, max_rows_dict=None):
    with engine.begin() as conn:
        for table in tables:
            min_r = min_rows_dict.get(table) if min_rows_dict else None
            max_r = max_rows_dict.get(table) if max_rows_dict else None

            conn.execute(text("""
            INSERT OR REPLACE INTO table_monitor_config (db_name, table_name, min_rows, max_rows)
            VALUES (:db, :table, :min_r, :max_r)
            """), {"db": db, "table": table, "min_r": min_r, "max_r": max_r})


def load_saved_table_config():
    return pd.read_sql("SELECT db_name, table_name, min_rows, max_rows FROM table_monitor_config", con=engine)


def log_table_check_result(db, table, count, status):
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO table_check_log (db_name, table_name, check_time, row_count, status)
        VALUES (:db, :table, datetime('now'), :count, :status)
        """), {"db": db, "table": table, "count": count, "status": status})


def get_latest_log():
    return pd.read_sql("SELECT * FROM table_check_log ORDER BY check_time DESC LIMIT 100", con=engine)


def save_job_config(jobs):
    with engine.begin() as conn:
        for job in jobs:
            conn.execute(text("""
            INSERT OR IGNORE INTO job_monitor_config (job_name)
            VALUES (:job)
            """), {"job": job})


def load_saved_job_config():
    return pd.read_sql("SELECT job_name FROM job_monitor_config", con=engine)


def log_job_check_result(job_name, status, last_run, next_run, message):
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO job_monitor_log (job_name, check_time, status, last_run, next_run, message)
        VALUES (:job, datetime('now'), :status, :last_run, :next_run, :message)
        """), {
            "job": job_name,
            "status": status,
            "last_run": last_run,
            "next_run": next_run,
            "message": message
        })


def delete_table_config(db_name, table_name):
    with engine.begin() as conn:
        conn.execute(text("""
        DELETE FROM table_monitor_config
        WHERE db_name = :db AND table_name = :table
        """), {"db": db_name, "table": table_name})


def delete_job_config(job_name):
    with engine.begin() as conn:
        conn.execute(text("""
        DELETE FROM job_monitor_config
        WHERE job_name = :job
        """), {"job": job_name})


def log_alert(alert_type, source_type, source_name, status, message=None, details=None):
    """
    Log an alert to the alert_log table

    Parameters:
    - alert_type: Type of alert (e.g., 'Table', 'Job')
    - source_type: Type of source (e.g., 'Empty Table', 'Failed Job', 'Duration Anomaly')
    - source_name: Name of the source (e.g., table name, job name))
    - status: Status of the alert (e.g., 'Empty', 'Failed', 'Slow', 'Fast', 'Warn-LowCount')
    - message: Alert message
    - details: Additional details (can be JSON or formatted text)
    """
    with engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO alert_log (alert_time, alert_type, source_type, source_name, status, message, details)
        VALUES (datetime('now'), :alert_type, :source_type, :source_name, :status, :message, :details)
        """), {
            "alert_type": alert_type,
            "source_type": source_type,
            "source_name": source_name,
            "status": status,
            "message": message,
            "details": details
        })


def get_alerts(limit=100, alert_type=None, source_type=None, status=None, hours_back=None):
    """
    Retrieve alerts from the alert_log table with optional filtering
    """
    query = "SELECT * FROM alert_log"
    wheres = []
    params = {}

    if alert_type:
        wheres.append("alert_type = :alert_type")
        params["alert_type"] = alert_type

    if source_type:
        wheres.append("source_type = :source_type")
        params["source_type"] = source_type

    if status:
        wheres.append("status = :status")
        params["status"] = status

    if hours_back:
        wheres.append("alert_time > datetime('now', :time_back)")
        params["time_back"] = f'-{hours_back} hours'

    if wheres:
        query += " WHERE " + " AND ".join(wheres)

    query += " ORDER BY alert_time DESC LIMIT :limit"
    params["limit"] = limit

    return pd.read_sql(query, con=engine, params=params)
