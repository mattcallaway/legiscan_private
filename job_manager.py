import sqlite3
import os
import uuid
import datetime
import json
import logging

logger = logging.getLogger(__name__)

class JobManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_jobs (
                        job_id TEXT PRIMARY KEY,
                        job_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        start_time TEXT NOT NULL,
                        end_time TEXT,
                        duration_sec REAL,
                        jurisdiction TEXT,
                        session_scope TEXT,
                        records_processed INTEGER DEFAULT 0,
                        new_items INTEGER DEFAULT 0,
                        updated_items INTEGER DEFAULT 0,
                        api_calls INTEGER DEFAULT 0,
                        error_summary TEXT,
                        initiated_by TEXT
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to initialize jobs DB: {e}")

    def start_job(self, job_type: str, jurisdiction: str = "", session_scope: str = "", initiated_by: str = "system") -> str:
        job_id = str(uuid.uuid4())
        now = datetime.datetime.utcnow().isoformat()
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO system_jobs (job_id, job_type, status, start_time, jurisdiction, session_scope, initiated_by)
                    VALUES (?, ?, 'RUNNING', ?, ?, ?, ?)
                """, (job_id, job_type, now, jurisdiction, session_scope, initiated_by))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to start job {job_id}: {e}")
        return job_id

    def update_job_progress(self, job_id: str, records_processed: int, api_calls: int = 0):
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE system_jobs 
                    SET records_processed = ?, api_calls = ?
                    WHERE job_id = ?
                """, (records_processed, api_calls, job_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")

    def finish_job(self, job_id: str, status: str, new_items: int = 0, updated_items: int = 0, records_processed: int = 0, api_calls: int = 0, error_summary: str = ""):
        now = datetime.datetime.utcnow()
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get start time to calc duration
                row = conn.execute("SELECT start_time FROM system_jobs WHERE job_id = ?", (job_id,)).fetchone()
                duration_sec = 0.0
                if row and row[0]:
                    try:
                        start_dt = datetime.datetime.fromisoformat(row[0])
                        duration_sec = (now - start_dt).total_seconds()
                    except:
                        pass
                
                conn.execute("""
                    UPDATE system_jobs 
                    SET status = ?, end_time = ?, duration_sec = ?, new_items = ?, updated_items = ?, records_processed = ?, api_calls = COALESCE(api_calls, 0) + ?, error_summary = ?
                    WHERE job_id = ?
                """, (status, now.isoformat(), duration_sec, new_items, updated_items, records_processed, api_calls, error_summary, job_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to finish job {job_id}: {e}")

    def get_recent_jobs(self, limit: int = 15) -> list:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("""
                    SELECT * FROM system_jobs 
                    ORDER BY start_time DESC LIMIT ?
                """, (limit,)).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Failed to fetch recent jobs: {e}")
            return []
            
    def get_running_jobs(self) -> list:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                # Safety timeout, consider jobs dead if running > 12 hours
                twelve_hours_ago = (datetime.datetime.utcnow() - datetime.timedelta(hours=12)).isoformat()
                rows = conn.execute("""
                    SELECT * FROM system_jobs 
                    WHERE status = 'RUNNING' AND start_time > ?
                    ORDER BY start_time DESC
                """, (twelve_hours_ago,)).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Failed to fetch running jobs: {e}")
            return []
