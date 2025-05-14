from sqlalchemy import create_engine, text
import pandas as pd
import os

DB_PATH = "sqlite:///data/job_monitor.db"
engine = create_engine(DB_PATH)


def init_db():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS table_monitor_config (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            db_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
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


def save_table_config(db, tables):
    with engine.begin() as conn:
        for table in tables:
            conn.execute(text("""
            INSERT OR IGNORE INTO table_monitor_config (db_name, table_name)
            VALUES (:db, :table)
            """), {"db": db, "table": table})


def load_saved_table_config():
    return pd.read_sql("SELECT db_name, table_name FROM table_monitor_config", con=engine)


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
