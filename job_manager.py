import sqlite3
from contextlib import closing, contextmanager
import os
import uuid
import datetime
import json
import logging

logger = logging.getLogger(__name__)
CORPUS_JOB_TYPES = ("bootstrap_corpus", "incremental_refresh", "keyword_rescan", "current_session_sync", "calendar_refresh")
QUEUED_LEASE_MINUTES = 5
MAX_QUEUE_ATTEMPTS = 2
STALE_JOB_HOURS = 12


class JobConflict(RuntimeError):
    """A sync already owns the shared corpus and keyword workspace."""


class JobManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
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
                columns = {row[1] for row in conn.execute("PRAGMA table_info(system_jobs)")}
                for column, field_type in (
                    ("queue_payload", "TEXT"), ("queued_at", "TEXT"),
                    ("heartbeat_at", "TEXT"), ("attempt", "INTEGER NOT NULL DEFAULT 1"),
                    ("retry_of", "TEXT"), ("progress_message", "TEXT"), ("progress_fraction", "REAL"),
                ):
                    if column not in columns:
                        conn.execute(f"ALTER TABLE system_jobs ADD COLUMN {column} {field_type}")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_system_jobs_queue ON system_jobs(status, queued_at)")
                conn.commit()
        except Exception as e:
            raise RuntimeError("Could not initialize sync job storage.") from e

    @contextmanager
    def worker_ownership(self):
        """Hold a process-scoped writer lock while a worker can mutate shared state."""
        with closing(sqlite3.connect(f"{self.db_path}.worker-lock", timeout=0)) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as error:
                if getattr(error, "sqlite_errorcode", None) not in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                    raise
                yield False
            else:
                try:
                    yield True
                finally:
                    connection.rollback()

    def start_job(self, job_type: str, jurisdiction: str = "", session_scope: str = "", initiated_by: str = "system") -> str:
        job_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            if job_type in CORPUS_JOB_TYPES:
                with self.worker_ownership() as worker_idle:
                    self._recover_interrupted(conn, now, recover_queued=worker_idle)
                existing = self._active_corpus_job(conn)
                if existing:
                    raise JobConflict(f"A {existing['job_type']} job is already running for {existing['jurisdiction'] or 'the corpus'}.")
            conn.execute(
                """INSERT INTO system_jobs (job_id, job_type, status, start_time, jurisdiction, session_scope, initiated_by)
                   VALUES (?, ?, 'RUNNING', ?, ?, ?, ?)""",
                (job_id, job_type, now.isoformat(), jurisdiction, session_scope, initiated_by),
            )
        return job_id

    @staticmethod
    def _active_corpus_job(conn):
        placeholders = ", ".join("?" for _ in CORPUS_JOB_TYPES)
        return conn.execute(
            f"SELECT job_type, jurisdiction FROM system_jobs WHERE status='RUNNING' AND job_type IN ({placeholders}) LIMIT 1",
            CORPUS_JOB_TYPES,
        ).fetchone()

    def _recover_interrupted(self, conn, now, *, recover_queued=True):
        queued_cutoff = (now - datetime.timedelta(minutes=QUEUED_LEASE_MINUTES)).isoformat()
        direct_cutoff = (now - datetime.timedelta(hours=STALE_JOB_HOURS)).isoformat()
        expired = conn.execute(
            """SELECT * FROM system_jobs WHERE status='RUNNING' AND
               ((? AND queue_payload IS NOT NULL AND coalesce(heartbeat_at, start_time)<=?)
               OR (queue_payload IS NULL AND start_time<=?))""",
            (recover_queued, queued_cutoff, direct_cutoff),
        ).fetchall()
        for job in expired:
            conn.execute(
                "UPDATE system_jobs SET status='INTERRUPTED', end_time=?, error_summary=? WHERE job_id=?",
                (now.isoformat(), "Worker stopped reporting progress", job["job_id"]),
            )
            if job["queue_payload"] and job["attempt"] < MAX_QUEUE_ATTEMPTS:
                self._insert_queued(
                    conn, job["job_type"], job["jurisdiction"], job["session_scope"], job["initiated_by"],
                    job["queue_payload"], now.isoformat(), job["attempt"] + 1, job["job_id"],
                )

    @staticmethod
    def _insert_queued(conn, job_type, jurisdiction, scope, actor, payload, now, attempt=1, retry_of=None):
        identifier = str(uuid.uuid4())
        conn.execute(
            """INSERT INTO system_jobs (
                   job_id, job_type, jurisdiction, session_scope, initiated_by, queue_payload,
                   status, start_time, queued_at, attempt, retry_of)
               VALUES (?, ?, ?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?)""",
            (identifier, job_type, jurisdiction, scope, actor, payload, now, now, attempt, retry_of),
        )
        return identifier

    def enqueue_job(self, job_type, jurisdiction, scope, payload, *, initiated_by="system"):
        if job_type not in CORPUS_JOB_TYPES:
            raise ValueError("Unsupported sync job type.")
        serialized = json.dumps(payload, sort_keys=True)
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT job_id FROM system_jobs WHERE job_type=? AND jurisdiction=? AND session_scope=?
                   AND status IN ('QUEUED', 'RUNNING') AND (queue_payload=? OR status='RUNNING')""",
                (job_type, jurisdiction, scope, serialized),
            ).fetchone()
            if existing:
                return existing[0]
            return self._insert_queued(conn, job_type, jurisdiction, scope, initiated_by, serialized, now)

    def claim_next_job(self):
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN IMMEDIATE")
            self._recover_interrupted(conn, now)
            if self._active_corpus_job(conn):
                return None
            job = conn.execute(
                """SELECT * FROM system_jobs WHERE status='QUEUED' AND queue_payload IS NOT NULL
                   ORDER BY queued_at, rowid LIMIT 1"""
            ).fetchone()
            if job is None:
                return None
            conn.execute(
                """UPDATE system_jobs SET status='RUNNING', start_time=?, heartbeat_at=?,
                   progress_message='Starting sync' WHERE job_id=?""",
                (now.isoformat(), now.isoformat(), job["job_id"]),
            )
            return {**dict(job), "status": "RUNNING", "payload": json.loads(job["queue_payload"])}

    def heartbeat(self, job_id, *, fraction=None, message=None):
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat()
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
            updated = conn.execute(
                """UPDATE system_jobs SET heartbeat_at=?, progress_fraction=coalesce(?, progress_fraction),
                   progress_message=coalesce(?, progress_message) WHERE job_id=? AND status='RUNNING'""",
                (now, fraction, message, job_id),
            )
            return updated.rowcount == 1

    def retry_queued_job(self, job_id, *, initiated_by):
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn:
            conn.row_factory = sqlite3.Row
            job = conn.execute(
                "SELECT * FROM system_jobs WHERE job_id=? AND status IN ('FAILED', 'INTERRUPTED') AND queue_payload IS NOT NULL",
                (job_id,),
            ).fetchone()
        if job is None:
            raise ValueError("That job is unavailable for retry.")
        return self.enqueue_job(job["job_type"], job["jurisdiction"], job["session_scope"],
                                json.loads(job["queue_payload"]), initiated_by=initiated_by)

    def update_job_progress(self, job_id: str, records_processed: int, api_calls: int = 0):
        try:
            with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
                conn.execute("""
                    UPDATE system_jobs 
                    SET records_processed = ?, api_calls = ?
                    WHERE job_id = ?
                """, (records_processed, api_calls, job_id))
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")

    def finish_job(self, job_id: str, status: str, new_items: int = 0, updated_items: int = 0, records_processed: int = 0, api_calls: int = 0, error_summary: str = ""):
        now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        try:
            with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
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
                    WHERE job_id = ? AND status = 'RUNNING'
                """, (status, now.isoformat(), duration_sec, new_items, updated_items, records_processed, api_calls, error_summary, job_id))
                conn.commit()
        except Exception as e:
            raise RuntimeError("Could not record the sync job outcome.") from e

    def get_job(self, identifier):
        with closing(sqlite3.connect(self.db_path, timeout=10)) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM system_jobs WHERE job_id=?", (identifier,)).fetchone()
            return dict(row) if row else None

    def get_recent_jobs(self, limit: int = 15) -> list:
        try:
            with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
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
            with closing(sqlite3.connect(self.db_path, timeout=10)) as conn, conn:
                conn.row_factory = sqlite3.Row
                # Safety timeout, consider jobs dead if running > 12 hours
                twelve_hours_ago = (datetime.datetime.utcnow() - datetime.timedelta(hours=STALE_JOB_HOURS)).isoformat()
                rows = conn.execute("""
                    SELECT * FROM system_jobs 
                    WHERE status = 'RUNNING' AND coalesce(heartbeat_at, start_time) > ?
                    ORDER BY start_time DESC
                """, (twelve_hours_ago,)).fetchall()
                return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Failed to fetch running jobs: {e}")
            return []
