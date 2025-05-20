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
            column_min_match_count INTEGER DEFAULT 1, -- Added new column
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
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS column_monitor_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            db_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            condition_type TEXT NOT NULL,
            condition_value TEXT NOT NULL,
            UNIQUE(db_name, table_name, column_name)
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
        init_db()  # Ensure DB and tables are created if db file doesn't exist
        # Re-connect after init_db creates the file
        if not os.path.exists(db_file):  # Still doesn't exist, something is wrong
            print(
                f"ERROR: Database file {db_file} could not be created by init_db().")
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

    # Update: Check and add columns for table_monitor_config
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
        if 'column_min_match_count' not in columns:  # Add check for the new column
            cursor.execute(
                "ALTER TABLE table_monitor_config ADD COLUMN column_min_match_count INTEGER DEFAULT 1")

        conn.commit()

    conn.close()


def save_table_config(db, tables, min_rows_dict=None, max_rows_dict=None, column_min_match_count_dict=None):
    with engine.begin() as conn:
        for table in tables:
            min_r = min_rows_dict.get(table) if min_rows_dict else None
            max_r = max_rows_dict.get(table) if max_rows_dict else None
            # Get the min match count for the current table, default to 1 if not provided
            min_match_c = column_min_match_count_dict.get(
                table, 1) if column_min_match_count_dict else 1

            # Ensure min_match_c is an int, handle None or non-int values if necessary
            if not isinstance(min_match_c, int):
                try:
                    min_match_c = int(
                        min_match_c) if min_match_c is not None else 1
                except ValueError:
                    min_match_c = 1  # Default if conversion fails

            conn.execute(text("""
            INSERT OR REPLACE INTO table_monitor_config 
            (db_name, table_name, min_rows, max_rows, column_min_match_count)
            VALUES (:db, :table, :min_r, :max_r, :min_match_c)
            """), {
                "db": db,
                "table": table,
                "min_r": min_r,
                "max_r": max_r,
                "min_match_c": min_match_c
            })


def load_saved_table_config():
    return pd.read_sql("SELECT db_name, table_name, min_rows, max_rows, column_min_match_count FROM table_monitor_config", con=engine)


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


def save_column_config(db_name, table_name, column_configs):
    """
    Save column monitoring configuration
    column_configs: list of dicts with keys: column_name, condition_type, condition_value
    """
    with engine.begin() as conn:
        # First delete existing config for this table
        conn.execute(text("""
        DELETE FROM column_monitor_config
        WHERE db_name = :db AND table_name = :table
        """), {"db": db_name, "table": table_name})

        # Insert new configurations
        for config in column_configs:
            conn.execute(text("""
            INSERT INTO column_monitor_config 
            (db_name, table_name, column_name, condition_type, condition_value)
            VALUES (:db, :table, :column, :cond_type, :cond_value)
            """), {
                "db": db_name,
                "table": table_name,
                "column": config["column_name"],
                "cond_type": config["condition_type"],
                "cond_value": config["condition_value"]
            })


def load_column_config(db_name=None, table_name=None):
    """Load column monitoring configuration with optional filtering"""
    query = "SELECT * FROM column_monitor_config"
    params = {}
    wheres = []

    if db_name:
        wheres.append("db_name = :db")
        params["db"] = db_name
    if table_name:
        wheres.append("table_name = :table")
        params["table"] = table_name

    if wheres:
        query += " WHERE " + " AND ".join(wheres)

    return pd.read_sql(query, con=engine, params=params)
