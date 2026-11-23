"""Persistent LegiScan request accounting shared by the website and CLI."""

from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import os
from pathlib import Path
import sqlite3

from hearing_calendar import PACIFIC


FILENAME = "legiscan_quota.db"
CORE_WORK = frozenset({"Session discovery", "Bulk import", "Bill refresh"})
MONTHLY_LIMIT_REACHED = "Monthly LegiScan request limit reached"
RESERVE_REACHED = "Remaining LegiScan requests are reserved for bill syncs"
LEDGER_UNAVAILABLE = "LegiScan request budget is temporarily unavailable"


def _credential(key):
    return sha256(key.strip().encode()).hexdigest()


def _local_date(now=None):
    return (now or datetime.now(timezone.utc)).astimezone(PACIFIC).date()


class LegiScanQuota:
    def __init__(self, data_dir):
        self.path = Path(data_dir) / FILENAME
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            pass
        else:
            os.close(descriptor)
        with closing(self._connect()) as conn, conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS quota_policy (
                    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                    monthly_limit INTEGER NOT NULL CHECK (monthly_limit>0),
                    reserve INTEGER NOT NULL CHECK (reserve>=0 AND reserve<monthly_limit),
                    pause_schedule INTEGER NOT NULL CHECK (pause_schedule IN (0, 1))
                );
                INSERT OR IGNORE INTO quota_policy VALUES (1, 10000, 2000, 0);
                CREATE TABLE IF NOT EXISTS quota_usage (
                    credential TEXT NOT NULL, month TEXT NOT NULL, day TEXT NOT NULL,
                    operation TEXT NOT NULL, purpose TEXT NOT NULL, requests INTEGER NOT NULL,
                    PRIMARY KEY (credential, month, day, operation, purpose)
                );
                CREATE TABLE IF NOT EXISTS quota_provider_usage (
                    credential TEXT NOT NULL, month TEXT NOT NULL, extra_requests INTEGER NOT NULL,
                    PRIMARY KEY (credential, month)
                );
                CREATE TABLE IF NOT EXISTS quota_pauses (
                    credential TEXT PRIMARY KEY, until_utc TEXT NOT NULL, reason TEXT NOT NULL
                );
            """)

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _policy(conn):
        return dict(conn.execute("SELECT monthly_limit, reserve, pause_schedule FROM quota_policy").fetchone())

    @staticmethod
    def _provider_extra(conn, credential, month):
        row = conn.execute(
            "SELECT extra_requests FROM quota_provider_usage WHERE credential=? AND month=?", (credential, month),
        ).fetchone()
        return row[0] if row else 0

    @staticmethod
    def _sent(conn, credential, month):
        return conn.execute(
            "SELECT coalesce(sum(requests), 0) FROM quota_usage WHERE credential=? AND month=?", (credential, month),
        ).fetchone()[0]

    @staticmethod
    def _pause(conn, credential, now):
        return conn.execute(
            "SELECT until_utc, reason FROM quota_pauses WHERE credential=? AND until_utc>?",
            (credential, now.isoformat()),
        ).fetchone()

    def reserve_request(self, key, operation, purpose, *, now=None):
        """Atomically reserve one request before network I/O. Return a safe stop reason."""
        credential = _credential(key)
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        day = _local_date(now).isoformat()
        month = day[:7]
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            policy = self._policy(conn)
            pause = self._pause(conn, credential, now)
            if pause:
                return pause["reason"]
            used = self._sent(conn, credential, month) + self._provider_extra(conn, credential, month)
            if used >= policy["monthly_limit"]:
                return MONTHLY_LIMIT_REACHED
            if purpose not in CORE_WORK and used >= policy["monthly_limit"] - policy["reserve"]:
                return RESERVE_REACHED
            conn.execute("""
                INSERT INTO quota_usage VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT (credential, month, day, operation, purpose)
                DO UPDATE SET requests=quota_usage.requests+1
            """, (credential, month, day, operation, purpose))
        return None

    def set_policy(self, monthly_limit, reserve, pause_schedule):
        if monthly_limit < 1 or not 0 <= reserve < monthly_limit:
            raise ValueError("Set a positive monthly limit and a reserve smaller than the limit.")
        with closing(self._connect()) as conn, conn:
            conn.execute("UPDATE quota_policy SET monthly_limit=?, reserve=?, pause_schedule=? WHERE singleton=1",
                         (monthly_limit, reserve, pause_schedule))

    def policy(self):
        with closing(self._connect()) as conn:
            return self._policy(conn)

    def record_provider_usage(self, key, used, *, now=None):
        """Account for calls outside this app without charging locally tracked calls twice."""
        if not key.strip() or used < 0:
            raise ValueError("A key and a nonnegative provider usage total are required.")
        credential = _credential(key)
        month = _local_date(now).isoformat()[:7]
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            difference = max(0, used - self._sent(conn, credential, month))
            conn.execute("""
                INSERT INTO quota_provider_usage VALUES (?, ?, ?)
                ON CONFLICT (credential, month) DO UPDATE SET extra_requests=max(extra_requests, excluded.extra_requests)
            """, (credential, month, difference))

    def pause_key(self, key, reason, minutes, *, now=None):
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        until = now + timedelta(minutes=minutes)
        with closing(self._connect()) as conn, conn:
            conn.execute("""
                INSERT INTO quota_pauses VALUES (?, ?, ?)
                ON CONFLICT (credential) DO UPDATE SET until_utc=excluded.until_utc, reason=excluded.reason
                WHERE excluded.until_utc>quota_pauses.until_utc
            """, (_credential(key), until.isoformat(), reason))

    def summary(self, key, *, now=None):
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        month = _local_date(now).isoformat()[:7]
        credential = _credential(key)
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN")
            policy = self._policy(conn)
            rows = conn.execute("""
                SELECT day, operation, purpose, requests FROM quota_usage
                WHERE credential=? AND month=? ORDER BY day, operation, purpose
            """, (credential, month)).fetchall()
            extra = self._provider_extra(conn, credential, month)
            pause = self._pause(conn, credential, now)
        daily = Counter()
        breakdown = Counter()
        for row in rows:
            daily[row["day"]] += row["requests"]
            breakdown[(row["purpose"], row["operation"])] += row["requests"]
        sent = sum(daily.values())
        return {**policy, "month": month, "sent": sent, "external": extra, "used": sent + extra,
                "daily": dict(daily), "breakdown": breakdown, "pause": dict(pause) if pause else None}
