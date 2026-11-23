# corpus_manager.py
"""
Master Bill Corpus Manager — Two-Layer Legislative Data Model
=============================================================

Layer A  (this module):
  SQLite database (bills.db in DATA_DIR) holding ALL bills for configured
  jurisdictions (California + Federal), NOT just keyword matches.

  Bootstrap strategy:
    1. getDatasetList  → obtain access_key for session  (1 API call)
    2. getDataset      → download bulk ZIP with all bill JSONs  (1 API call)
    3. Parse ZIP locally → upsert into bills table  (0 additional calls)
    Fallback: if getDataset unavailable, use getMasterListRaw + getBill per bill.

  Incremental refresh strategy:
    1. getMasterListRaw  → get all bill_id + change_hash for session  (1 API call)
    2. Compare hashes with local DB
    3. getBill only for new or changed bills  (N calls, typically <50/week)

Layer B  (legiscanner.py):
  Existing keyword-based scan, unchanged.
  Bills discovered there are also recorded in keyword_matches table here.

Public API
----------
  CorpusManager(db_path, api_key, rate_limit_s=0.25)
    .get_active_sessions(jurisdiction)          → list[dict]   (API call)
    .get_cached_sessions(jurisdiction=None)     → list[dict]   (local only)
    .get_dataset_list(jurisdiction)             → list[dict]   (API call)
    .bootstrap_session(session_id, jur, …)      → stats dict
    .refresh_session(session_id, jur, …)        → stats dict
    .record_keyword_match(bill_id, keyword)     → None
    .get_keyword_matches(bill_id)               → list[str]
    .search_bills(query, jur_filter, …)         → pd.DataFrame
    .get_corpus_stats()                         → dict
    .get_all_session_jurisdictions()            → list[str]
    .close()

The DataFrame returned by search_bills() uses the same column names as the
legacy CSV (jurisdiction_level, jurisdiction_name, bill_number, title, …) so
all existing _render_bill_expander() / filter / export code works unchanged.
"""
from __future__ import annotations

import base64
import io
import json
import logging
import os
import sqlite3
import time
import zipfile
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from functools import wraps
from hashlib import sha256
from pathlib import Path
from threading import Lock, local
from typing import Callable, Optional

import requests
from request_budget import MISSING_KEY, admit_legiscan_request, job_stop_reason, observe_legiscan_response
from sqlite_schema import add_missing_columns, apply_ddl, migrate_schema
from hearing_calendar import normalize_calendar, source_link
import pandas as pd

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
BASE_URL        = "https://api.legiscan.com/"

CHAMBER_MAP = {"A": "Assembly", "S": "Senate", "H": "House"}

# Friendly names for supported jurisdictions
JURISDICTION_LABELS = {
    "CA": "California",
    "US": "U.S. Congress",
}

# ── Safe field-flattening helper (mirrors legiscanner._safe_join_list) ─────────
def _safe_join_list(items, key: Optional[str] = None, sep: str = "; ") -> str:
    """Safely join a list that may contain strings OR dicts."""
    if not items:
        return ""
    parts: list[str] = []
    for item in items:
        if isinstance(item, dict):
            parts.append(str(item[key]) if key and key in item else str(item))
        else:
            parts.append(str(item))
    return sep.join(parts)


# ── Bill flattening (independent of legiscanner to avoid circular imports) ──────
def _flatten_bill_to_row(details: dict, jurisdiction: str, session_id: int) -> dict:
    """Convert a raw LegiScan getBill-style dict into a flat DB row dict."""
    committee_info = details.get("committee")
    committee = committee_info.get("name", "") if isinstance(committee_info, dict) else ""

    sponsors_raw = details.get("sponsors", [])
    sponsor_names = _safe_join_list(sponsors_raw, key="name", sep=", ")

    history_raw = details.get("history", [])
    hist_list: list[str] = []
    for h in (history_raw if isinstance(history_raw, list) else []):
        if isinstance(h, dict):
            ch = CHAMBER_MAP.get(h.get("chamber", ""), h.get("chamber", ""))
            action = h.get("action", "").replace("\n", " ")
            date   = h.get("date", "")
            hist_list.append(f"{ch}: {action} ({date})")
        else:
            hist_list.append(str(h))

    ref_list: list[str] = []
    for ref in (details.get("referrals", []) or []):
        if isinstance(ref, dict):
            ch   = CHAMBER_MAP.get(ref.get("chamber", ""), ref.get("chamber", ""))
            name = ref.get("name", "")
            date = ref.get("date", "")
            ref_list.append(f"{ch} - {name} on {date}")
        else:
            ref_list.append(str(ref))

    # Latest text version finding
    texts = details.get("texts", [])
    latest_doc_id = None
    latest_doc_url = ""
    if texts and isinstance(texts, list):
        # Usually ordered by date desc or at least chronological
        latest = texts[-1] 
        latest_doc_id = latest.get("doc_id")
        latest_doc_url = latest.get("url", "")

    return {
        "bill_id":          int(details.get("bill_id", 0)),
        "session_id":       session_id,
        "jurisdiction":     jurisdiction,
        "bill_number":      details.get("bill_number", ""),
        "title":            details.get("title", "").replace("\n", " "),
        "description":      details.get("description", "").replace("\n", " "),
        "status_date":      details.get("status_date", ""),
        "status_stage":     str(details.get("status", "")),
        "url":              details.get("url", ""),
        "committee":        committee,
        "sponsor_names":    sponsor_names,
        "subjects":         _safe_join_list(details.get("subjects", []), key="subject_name"),
        "history":          "; ".join(hist_list),
        "last_action":      hist_list[-1] if hist_list else "",
        "last_action_date": details.get("last_action_date", details.get("status_date", "")),
        "referrals":        "; ".join(ref_list),
        "change_hash":      details.get("change_hash", ""),
        "calendar_feed": details.get("calendar"),
        "calendar_source": source_link(details.get("state_link"), details.get("url")),
        "latest_doc_id":    latest_doc_id,
        "latest_doc_url":   latest_doc_url,
        "last_fetched":     datetime.now(timezone.utc).isoformat(),
    }


# ── DDL ────────────────────────────────────────────────────────────────────────
_SCHEMA_V1 = """
CREATE TABLE IF NOT EXISTS sync_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id     INTEGER PRIMARY KEY,
    jurisdiction   TEXT      NOT NULL,
    session_name   TEXT,
    year_start     INTEGER,
    year_end       INTEGER,
    is_active      INTEGER   DEFAULT 1,
    dataset_hash   TEXT,
    last_masterlist TEXT,
    last_bootstrap  TEXT
);

CREATE TABLE IF NOT EXISTS bills (
    bill_id          INTEGER PRIMARY KEY,
    session_id       INTEGER,
    jurisdiction     TEXT,
    bill_number      TEXT,
    title            TEXT,
    description      TEXT,
    status_date      TEXT,
    status_stage     TEXT,
    url              TEXT,
    committee        TEXT,
    sponsor_names    TEXT,
    subjects         TEXT,
    history          TEXT,
    last_action      TEXT,
    last_action_date TEXT,
    referrals        TEXT,
    change_hash      TEXT,
    latest_doc_id    INTEGER,
    latest_doc_url   TEXT,
    last_fetched     TEXT,
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);

CREATE TABLE IF NOT EXISTS bill_calendar_snapshots (
    bill_id INTEGER PRIMARY KEY,
    jurisdiction TEXT NOT NULL,
    bill_number TEXT NOT NULL,
    title TEXT NOT NULL,
    source_url TEXT NOT NULL,
    events_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bill_calendar_attempts (
    bill_id INTEGER PRIMARY KEY,
    checked_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bill_texts (
    doc_id          INTEGER PRIMARY KEY,
    bill_id         INTEGER NOT NULL,
    mime_type       TEXT,
    content_html    TEXT,
    content_pdf_url TEXT,
    last_fetched    TEXT,
    FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
);

CREATE INDEX IF NOT EXISTS idx_bills_jurisdiction ON bills(jurisdiction);
CREATE INDEX IF NOT EXISTS idx_bills_number       ON bills(bill_number);
CREATE INDEX IF NOT EXISTS idx_bills_status       ON bills(status_stage);
CREATE INDEX IF NOT EXISTS idx_bills_session      ON bills(session_id);
CREATE INDEX IF NOT EXISTS idx_bills_session_status_page ON bills(session_id, status_date DESC, bill_id);
CREATE INDEX IF NOT EXISTS idx_bills_session_action_page ON bills(session_id, last_action_date DESC, bill_id);
CREATE INDEX IF NOT EXISTS idx_bills_status_day ON bills(substr(status_date, 1, 10));

CREATE TABLE IF NOT EXISTS keyword_matches (
    bill_id    INTEGER  NOT NULL,
    keyword    TEXT     NOT NULL,
    matched_at TEXT,
    PRIMARY KEY (bill_id, keyword)
);

CREATE TABLE IF NOT EXISTS roll_calls (
    roll_call_id INTEGER PRIMARY KEY,
    bill_id INTEGER,
    date TEXT,
    desc TEXT,
    yea INTEGER,
    nay INTEGER,
    nv INTEGER,
    absent INTEGER,
    total INTEGER,
    passed INTEGER,
    chamber TEXT,
    chamber_id INTEGER,
    url TEXT,
    state_link TEXT,
    last_fetched TEXT,
    FOREIGN KEY(bill_id) REFERENCES bills(bill_id)
);

CREATE TABLE IF NOT EXISTS legislator_votes (
    roll_call_id INTEGER,
    people_id INTEGER,
    vote_id INTEGER,
    vote_text TEXT,
    PRIMARY KEY(roll_call_id, people_id),
    FOREIGN KEY(roll_call_id) REFERENCES roll_calls(roll_call_id)
);

CREATE TABLE IF NOT EXISTS people (
    people_id INTEGER PRIMARY KEY,
    name TEXT,
    first_name TEXT,
    last_name TEXT,
    party TEXT,
    role_id INTEGER,
    role TEXT,
    district TEXT,
    chamber TEXT
);

CREATE TABLE IF NOT EXISTS people_mapping (
    people_id INTEGER PRIMARY KEY,
    staff_legislator_id TEXT,
    match_quality TEXT,
    FOREIGN KEY(people_id) REFERENCES people(people_id)
);
"""


# ── CorpusManager ──────────────────────────────────────────────────────────────
def corpus_file_revision(db_path):
    """Capture changes to either the main database or its WAL across processes."""
    revision = []
    for filename, is_wal in ((db_path, False), (f"{db_path}-wal", True)):
        try:
            state = os.stat(filename)
        except FileNotFoundError:
            revision.append(None)
            continue
        # Reader connections may create or remove a WAL without any committed frames.
        if is_wal and state.st_size <= 32:
            revision.append(None)
        else:
            revision.append((state.st_ino, state.st_size, state.st_mtime_ns))
    return tuple(revision)


def _canonical_bill_ids(values):
    """Convert only exact representations of SQLite integer bill IDs."""
    for value in values:
        text = str(value)
        digits = text.removeprefix("-")
        if len(text) > 20 or not digits.isascii() or not digits.isdecimal():
            continue
        number = int(text)
        if -(2 ** 63) <= number < 2 ** 63 and str(number) == text:
            yield number


def _corpus_operation(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        with self.connection():
            return method(self, *args, **kwargs)
    return run


class CorpusManager:
    """Manages the local SQLite master bill corpus."""

    def __init__(
        self,
        db_path: str,
        api_key: str,
        rate_limit_s: float = 0.25,
        download_timeout: int = 180,
    ) -> None:
        self.db_path          = db_path
        self.api_key          = api_key.strip()
        self.data_dir         = Path(db_path).parent
        self.rate_limit_s     = rate_limit_s
        self.download_timeout = download_timeout
        self._thread = local()
        self._literal_counts = OrderedDict()
        self._literal_counts_lock = Lock()
        self._version_lists = OrderedDict()
        self._version_lock = Lock()
        self._init_db()

    # ── Connection ────────────────────────────────────────────────────────────

    @property
    def _api_calls_run(self):
        return getattr(self._thread, "api_calls", 0)

    @_api_calls_run.setter
    def _api_calls_run(self, value):
        self._thread.api_calls = value

    def _get_conn(self) -> sqlite3.Connection:
        connection = getattr(self._thread, "connection", None)
        if connection is None:
            connection = sqlite3.connect(self.db_path, timeout=10)
            try:
                connection.row_factory = sqlite3.Row
                connection.create_function("casefold", 1, str.casefold, deterministic=True)
                connection.execute("PRAGMA foreign_keys=ON")
            except BaseException:
                connection.close()
                raise
            self._thread.connection = connection
        return connection

    @contextmanager
    def connection(self):
        """Borrow a thread-owned connection, closing it at the outer scope."""
        owns_connection = getattr(self._thread, "connection", None) is None
        connection = self._get_conn()
        try:
            yield connection
        finally:
            if owns_connection:
                self.close()

    @_corpus_operation
    def _init_db(self) -> None:
        migrate_schema(self._get_conn(), "corpus", (self._migrate_schema_v1,), prepare=self._enable_wal)

    @staticmethod
    def _enable_wal(conn):
        # Switching journal modes can report BUSY without using SQLite's busy timeout.
        deadline = time.monotonic() + 10
        while True:
            try:
                conn.execute("PRAGMA journal_mode=WAL").fetchone()
                return
            except sqlite3.OperationalError as error:
                code = getattr(error, "sqlite_errorcode", 0) & 255
                if code != sqlite3.SQLITE_BUSY or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    @staticmethod
    def _migrate_schema_v1(conn):
        apply_ddl(conn, _SCHEMA_V1)
        add_missing_columns(conn, "bills", (("latest_doc_id", "INTEGER"), ("latest_doc_url", "TEXT")))
        # Old releases look for this text value in sync_meta.
        conn.execute("INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('schema_version', '1')")

    # ── Low-level API helpers ─────────────────────────────────────────────────

    @property
    def last_request_failure(self):
        return getattr(self._thread, "request_failure", None)

    def _api_get(self, params: dict, timeout: Optional[int] = None) -> dict:
        """Make one LegiScan API request (JSON response)."""
        self._thread.request_failure = None
        operation = params["op"]
        purpose = "Session discovery" if operation == "getSessionList" else "Bill research"
        denial = admit_legiscan_request(self.data_dir, self.api_key, operation, purpose)
        if denial:
            self._thread.request_failure = denial
            return {"status": "ERROR", "error": denial, "request_failure": denial}
        p = dict(params)
        p["key"] = self.api_key
        self._api_calls_run += 1
        try:
            r = requests.get(BASE_URL, params=p, timeout=timeout or 30)
            if reason := observe_legiscan_response(self.data_dir, self.api_key, r):
                self._thread.request_failure = reason
                return {"status": "ERROR", "error": reason, "request_failure": reason}
            r.raise_for_status()
            time.sleep(self.rate_limit_s)
            payload = r.json()
            if isinstance(payload, dict):
                # Only local diagnostics may populate this field.
                payload.pop("request_failure", None)
                if reason := observe_legiscan_response(self.data_dir, self.api_key, r, payload):
                    self._thread.request_failure = reason
                    payload["request_failure"] = reason
                return payload
            failure = "LegiScan returned an invalid response"
        except requests.exceptions.Timeout:
            failure = "LegiScan request timed out"
        except requests.exceptions.HTTPError as exc:
            failure = "LegiScan HTTP request failed"
            if exc.response is not None:
                failure = f"LegiScan returned HTTP {exc.response.status_code}"
        except requests.exceptions.ConnectionError:
            failure = "Could not connect to LegiScan"
        except ValueError:
            failure = "LegiScan returned an invalid response"
        except Exception as exc:
            failure = f"LegiScan request failed ({type(exc).__name__})"
        logger.error("LegiScan API error: %s while calling %s", failure, params.get("op", "?"))
        self._thread.request_failure = failure
        return {"status": "ERROR", "error": failure, "request_failure": failure}

    def _meta_get(self, key: str) -> Optional[str]:
        row = self._get_conn().execute(
            "SELECT value FROM sync_meta WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def _meta_set(self, key: str, value: str) -> None:
        self._get_conn().execute(
            "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)",
            (key, value),
        )

    # ── Session discovery ─────────────────────────────────────────────────────

    @_corpus_operation
    def get_active_sessions(self, jurisdiction: str) -> list[dict]:
        """
        Fetch current sessions from LegiScan API and cache in DB.
        Returns up to 5 most-recent sessions (sorted newest first).
        Cost: 1 API call.
        """
        data = self._api_get({"op": "getSessionList", "state": jurisdiction})
        if data.get("status") != "OK":
            logger.warning("getSessionList failed for %s: %s", jurisdiction, data.get("request_failure", "upstream error"))
            return []

        sessions_raw: list[dict] = data.get("sessions", [])
        # Keep all sessions, sort newest first
        sessions_raw.sort(key=lambda s: s.get("year_start", 0), reverse=True)

        conn = self._get_conn()
        for s in sessions_raw[:10]:
            conn.execute(
                """
                INSERT INTO sessions
                    (session_id, jurisdiction, session_name, year_start, year_end, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                ON CONFLICT(session_id) DO UPDATE SET
                    session_name = excluded.session_name,
                    year_start   = excluded.year_start,
                    year_end     = excluded.year_end,
                    is_active    = 1
                """,
                (
                    s["session_id"],
                    jurisdiction,
                    s.get("session_name", ""),
                    s.get("year_start", 0),
                    s.get("year_end", 0),
                ),
            )
        conn.commit()
        return sessions_raw[:10]

    @_corpus_operation
    def get_cached_sessions(self, jurisdiction: Optional[str] = None) -> list[dict]:
        """Return sessions from local DB without any API call."""
        conn = self._get_conn()
        if jurisdiction:
            rows = conn.execute(
                "SELECT * FROM sessions WHERE jurisdiction=? ORDER BY year_start DESC",
                (jurisdiction,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM sessions ORDER BY jurisdiction, year_start DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Bootstrap (getDataset ZIP) ────────────────────────────────────────────

    def get_dataset_list(self, jurisdiction: str) -> list[dict]:
        """
        Fetch available datasets for a jurisdiction.
        Returns list with access_key needed for getDataset.
        Cost: 1 API call.
        """
        data = self._api_get({"op": "getDatasetList", "state": jurisdiction})
        if data.get("status") != "OK":
            logger.warning("getDatasetList failed for %s: %s", jurisdiction, data.get("request_failure", "upstream error"))
            return []
        return data.get("datasetlist", [])

    @_corpus_operation
    def bootstrap_session(
        self,
        session_id: int,
        jurisdiction: str,
        progress_cb: Optional[Callable[[float, str], None]] = None,
        *,
        allow_individual_fallback: bool = True,
    ) -> dict:
        """
        Bootstrap a session using the bulk dataset ZIP.

        Sequence:
          1. getDatasetList → access_key         (1 call)
          2. getDataset ZIP download             (1 call)
          3. Parse all bill JSONs from ZIP       (0 calls)
          Fallback if ZIP unavailable: getMasterListRaw + getBill per bill.

        progress_cb(fraction: float, message: str) is called periodically.
        Returns stats dict: {new, updated, skipped, errors, api_calls}.
        """
        stats: dict[str, int] = {
            "new": 0, "updated": 0, "skipped": 0, "errors": 0, "api_calls": 0
        }
        start_calls = self._api_calls_run

        logger.info(f"Bootstrap start: {jurisdiction} session_id={session_id}")
        if progress_cb:
            progress_cb(0.0, f"Fetching dataset list for {jurisdiction}…")

        datasets = self.get_dataset_list(jurisdiction)
        target_ds = next(
            (d for d in datasets if d.get("session_id") == session_id), None
        )

        if not target_ds and not allow_individual_fallback:
            raise RuntimeError("A bulk dataset is unavailable for this session.")
        if not target_ds:
            logger.warning(
                f"No dataset found for session_id={session_id} ({jurisdiction}); "
                "falling back to getMasterListRaw bootstrap."
            )
            stats.update(
                self._bootstrap_via_masterlist(session_id, jurisdiction, progress_cb)
            )
            stats["api_calls"] = self._api_calls_run - start_calls
            return stats

        access_key = target_ds.get("access_key", "")
        logger.info(
            f"Bootstrap: downloading dataset ZIP for {jurisdiction} session {session_id}"
        )
        if progress_cb:
            progress_cb(0.05, "Downloading dataset ZIP (this may take a moment)…")

        zip_bytes = self._download_dataset_zip(session_id, access_key)
        if zip_bytes is None and not allow_individual_fallback:
            raise RuntimeError("The bulk dataset download failed. Retry the bulk sync.")
        if zip_bytes is None:
            logger.warning("getDataset ZIP unavailable; falling back to getMasterListRaw.")
            stats.update(
                self._bootstrap_via_masterlist(session_id, jurisdiction, progress_cb)
            )
            stats["api_calls"] = self._api_calls_run - start_calls
            return stats

        if progress_cb:
            progress_cb(0.1, "Parsing ZIP contents…")

        self._ingest_zip(zip_bytes, session_id, jurisdiction, progress_cb, stats)
        if not stats["errors"]:
            self._record_bootstrap(session_id, jurisdiction)

        stats["api_calls"] = self._api_calls_run - start_calls
        logger.info(f"Bootstrap complete: {stats}")
        return stats

    def _download_dataset_zip(
        self, session_id: int, access_key: str
    ) -> Optional[bytes]:
        """Download and decode the dataset ZIP. Returns raw bytes or None."""
        if admit_legiscan_request(self.data_dir, self.api_key, "getDataset", "Bulk import"):
            return None
        p = {
            "op":         "getDataset",
            "id":         session_id,
            "access_key": access_key,
            "key":        self.api_key,
        }
        self._api_calls_run += 1
        try:
            r = requests.get(
                BASE_URL, params=p, timeout=self.download_timeout, stream=True
            )
            if observe_legiscan_response(self.data_dir, self.api_key, r):
                return None
            r.raise_for_status()
            time.sleep(self.rate_limit_s)
            data = r.json()
            if observe_legiscan_response(self.data_dir, self.api_key, r, data):
                return None
        except Exception as exc:
            logger.error(f"getDataset download error: {type(exc).__name__}")
            return None

        if data.get("status") != "OK":
            logger.warning(f"getDataset not OK: {data.get('status')}")
            return None

        zip_b64: str = data.get("dataset", {}).get("zip", "")
        if not zip_b64:
            logger.warning("getDataset response had no 'zip' field.")
            return None

        try:
            return base64.b64decode(zip_b64)
        except Exception as exc:
            logger.error(f"Base64 decode failed: {exc}")
            return None

    def _upsert_person(self, conn: sqlite3.Connection, p: dict) -> None:
        conn.execute(
            """
            INSERT INTO people (
                people_id, name, first_name, last_name, party, role_id, role, district, chamber
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(people_id) DO UPDATE SET
                name=excluded.name, first_name=excluded.first_name, last_name=excluded.last_name,
                party=excluded.party, role_id=excluded.role_id, role=excluded.role,
                district=excluded.district, chamber=excluded.chamber
            """,
            (
                p.get("people_id"), p.get("name"), p.get("first_name"), p.get("last_name"),
                p.get("party"), p.get("role_id"), p.get("role"), p.get("district"), str(p.get("chamber", ""))
            )
        )

    def _upsert_rollcall(self, conn: sqlite3.Connection, r: dict, bill_id: int) -> None:
        rc_id = r.get("roll_call_id")
        if not rc_id: return
        conn.execute(
            """
            INSERT INTO roll_calls (
                roll_call_id, bill_id, date, desc, yea, nay, nv, absent, total, passed,
                chamber, chamber_id, url, state_link, last_fetched
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(roll_call_id) DO UPDATE SET
                bill_id=excluded.bill_id, date=excluded.date, desc=excluded.desc, yea=excluded.yea,
                nay=excluded.nay, nv=excluded.nv, absent=excluded.absent, total=excluded.total,
                passed=excluded.passed, chamber=excluded.chamber, chamber_id=excluded.chamber_id,
                url=excluded.url, state_link=excluded.state_link, last_fetched=excluded.last_fetched
            """,
            (
                rc_id, bill_id, r.get("date"), r.get("desc"), r.get("yea"), r.get("nay"),
                r.get("nv"), r.get("absent"), r.get("total"), r.get("passed"),
                r.get("chamber"), r.get("chamber_id"), r.get("url"), r.get("state_link"),
                datetime.now(timezone.utc).isoformat()
            )
        )
        
        votes = r.get("votes", [])
        for v in votes:
            p_id = v.get("people_id")
            if not p_id: continue
            
            p_name = v.get("name")
            if not p_name:
                p_name = f"Unknown Profile (ID {p_id})"
                
            conn.execute(
                """
                INSERT OR IGNORE INTO people (people_id, name, party)
                VALUES (?, ?, ?)
                """,
                (p_id, p_name, v.get("party"))
            )
                
            conn.execute(
                """
                INSERT INTO legislator_votes (roll_call_id, people_id, vote_id, vote_text)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(roll_call_id, people_id) DO UPDATE SET
                    vote_id=excluded.vote_id, vote_text=excluded.vote_text
                """,
                (rc_id, int(p_id), v.get("vote_id"), v.get("vote_text"))
            )

    @_corpus_operation
    def _ingest_zip(
        self,
        zip_bytes: bytes,
        session_id: int,
        jurisdiction: str,
        progress_cb: Optional[Callable],
        stats: dict,
    ) -> None:
        """Walk all JSON files in the dataset ZIP and upsert bills."""
        conn = self._get_conn()
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                all_names = zf.namelist()
                # Prefer paths that clearly contain data by ensuring they have leading slashes
                # to match directories like "/bill/" even if they are at the root "bill/".
                bill_files = [n for n in all_names if n.endswith(".json") and "/bill/" in f"/{n.lower()}"]
                vote_files = [n for n in all_names if n.endswith(".json") and "/vote/" in f"/{n.lower()}"]
                person_files = [n for n in all_names if n.endswith(".json") and "/person/" in f"/{n.lower()}"]
                
                if not bill_files:
                    bill_files = [n for n in all_names if n.endswith(".json") and "masterlist" not in n.lower()]

                total = len(bill_files) + len(vote_files) + len(person_files)
                logger.info(f"ZIP has {len(bill_files)} bills, {len(vote_files)} votes, {len(person_files)} people")

                processed = 0
                for name in person_files:
                    processed += 1
                    try:
                        with zf.open(name) as fh:
                            raw = json.load(fh)
                            p = raw.get("person", raw)
                            if p.get("people_id"):
                                self._upsert_person(conn, p)
                    except Exception as exc:
                        logger.warning("Dataset record could not be imported: %s", type(exc).__name__)
                        stats["errors"] += 1
                
                for i, name in enumerate(bill_files):
                    processed += 1
                    try:
                        with zf.open(name) as fh:
                            raw = json.load(fh)
                    except Exception as exc:
                        logger.debug(f"Skip {name}: {exc}")
                        stats["errors"] += 1
                        continue

                    bill_data = raw.get("bill", raw)
                    bill_id = bill_data.get("bill_id")
                    if not bill_id:
                        continue
                        
                    for sponsor in bill_data.get("sponsors", []):
                        if sponsor.get("people_id"):
                            self._upsert_person(conn, sponsor)

                    row = _flatten_bill_to_row(bill_data, jurisdiction, session_id)
                    row["calendar_from_dataset"] = True
                    self._upsert_bill(conn, row, stats)
                    
                    for r in bill_data.get("votes", []):
                        self._upsert_rollcall(conn, r, bill_id)

                    if processed % 100 == 0:
                        conn.commit()
                        if progress_cb:
                            progress_cb(0.1 + 0.85 * (processed / max(total, 1)), f"Ingested {processed}/{total} files…")

                for name in vote_files:
                    processed += 1
                    try:
                        with zf.open(name) as fh:
                            raw = json.load(fh)
                            r = raw.get("roll_call", raw)
                            rc_bid = r.get("bill_id")
                            if r.get("roll_call_id") and rc_bid:
                                self._upsert_rollcall(conn, r, rc_bid)
                    except Exception as exc:
                        logger.warning("Dataset record could not be imported: %s", type(exc).__name__)
                        stats["errors"] += 1
                    
                    if processed % 100 == 0:
                        conn.commit()
                        if progress_cb:
                            progress_cb(0.1 + 0.85 * (processed / max(total, 1)), f"Ingested {processed}/{total} files…")

                if not stats["new"] and not stats["updated"]:
                    stats["errors"] += 1
                conn.commit()
                if progress_cb:
                    progress_cb(1.0, f"Done — {stats['new']} new, {stats['updated']} updated")
                logger.info(f"ZIP ingest complete: {stats}")

        except zipfile.BadZipFile as exc:
            logger.error(f"Bad ZIP: {exc}")
            stats["errors"] += 1

    # ── Bootstrap fallback (getMasterListRaw + getBill per bill) ──────────────

    @_corpus_operation
    def _bootstrap_via_masterlist(
        self,
        session_id: int,
        jurisdiction: str,
        progress_cb: Optional[Callable],
    ) -> dict:
        """
        Fallback bootstrap when getDataset is unavailable.
        Fetches getMasterListRaw, then getBill for each uncached bill.
        """
        stats: dict[str, int] = {"new": 0, "updated": 0, "skipped": 0, "errors": 0}
        conn = self._get_conn()

        logger.info(f"Fallback bootstrap: getMasterListRaw session {session_id}")
        data = self._api_get({"op": "getMasterListRaw", "id": session_id})
        if data.get("status") != "OK":
            logger.error("Bootstrap master list failed for session %s", session_id)
            stats["errors"] += 1
            return stats

        master = data.get("masterlist", {})
        bills_meta = [
            v for v in master.values()
            if isinstance(v, dict) and "bill_id" in v
        ]
        total = len(bills_meta)
        logger.info(f"Master list: {total} bills")

        # Skip already-cached bills (makes this resumable)
        cached_ids: set[int] = {
            r[0]
            for r in conn.execute(
                "SELECT bill_id FROM bills WHERE session_id=?", (session_id,)
            ).fetchall()
        }

        to_fetch = [m for m in bills_meta if m["bill_id"] not in cached_ids]
        logger.info(
            f"Need to fetch {len(to_fetch)} bills ({len(cached_ids)} already cached)"
        )

        for i, meta in enumerate(to_fetch):
            if job_stop_reason():
                stats["errors"] += 1
                break
            bid = meta["bill_id"]
            detail_data = self._api_get({"op": "getBill", "id": bid})
            if detail_data.get("status") != "OK":
                stats["errors"] += 1
                continue

            bill_detail = detail_data.get("bill", {})
            if not bill_detail:
                stats["errors"] += 1
                continue

            for sponsor in bill_detail.get("sponsors", []):
                if sponsor.get("people_id"):
                    self._upsert_person(conn, sponsor)

            bill_detail["change_hash"] = meta.get("change_hash", "")
            row = _flatten_bill_to_row(bill_detail, jurisdiction, session_id)
            self._upsert_bill(conn, row, stats)
            stats["errors"] += self._process_bill_votes(conn, bill_detail)

            if (i + 1) % 50 == 0:
                conn.commit()
                logger.info(
                    f"Bootstrap progress: {i + 1}/{len(to_fetch)} fetched"
                    f" (total API calls: {self._api_calls_run})"
                )
                if progress_cb:
                    progress_cb(
                        (i + 1) / max(len(to_fetch), 1),
                        f"Fetched {i + 1}/{len(to_fetch)} bills…",
                    )

        conn.commit()
        stats["skipped"] = len(cached_ids)
        if not stats["errors"]:
            self._record_bootstrap(session_id, jurisdiction)
        return stats

    @_corpus_operation
    def _record_bootstrap(self, session_id: int, jurisdiction: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        conn.execute(
            "UPDATE sessions SET last_bootstrap=? WHERE session_id=?",
            (now, session_id),
        )
        self._meta_set(f"last_bootstrap_{jurisdiction}", now)
        conn.commit()

    # ── Incremental refresh (getMasterListRaw diff) ───────────────────────────

    @_corpus_operation
    def refresh_session(
        self,
        session_id: int,
        jurisdiction: str,
        progress_cb: Optional[Callable[[float, str], None]] = None,
    ) -> dict:
        """
        Incremental refresh via getMasterListRaw + getBill for changed bills.

        API calls: 1 (getMasterListRaw) + N (getBill for new/changed bills).
        N is typically <50 in a normal weekly refresh cycle.

        Returns stats dict: {new, updated, skipped, errors, api_calls}.
        """
        stats: dict[str, int] = {
            "new": 0, "updated": 0, "skipped": 0, "errors": 0, "api_calls": 0
        }
        start_calls = self._api_calls_run
        conn = self._get_conn()

        logger.info(
            f"Incremental refresh: jurisdiction={jurisdiction} session={session_id}"
        )
        if progress_cb:
            progress_cb(0.0, "Fetching master bill list…")

        data = self._api_get({"op": "getMasterListRaw", "id": session_id})
        if data.get("status") != "OK" or not isinstance(data.get("masterlist"), dict):
            logger.error("getMasterListRaw failed for session %s", session_id)
            stats["errors"] += 1
            stats["api_calls"] = self._api_calls_run - start_calls
            return stats

        master = data.get("masterlist", {})
        bills_meta = [
            v for v in master.values()
            if isinstance(v, dict) and "bill_id" in v
        ]

        # Load all cached hashes in one query
        cached_hashes: dict[int, str] = {
            r[0]: r[1]
            for r in conn.execute(
                "SELECT bill_id, change_hash FROM bills WHERE session_id=?",
                (session_id,),
            ).fetchall()
        }

        to_fetch: list[dict] = []
        for meta in bills_meta:
            bid      = meta.get("bill_id")
            new_hash = meta.get("change_hash", "")
            if not bid:
                continue
            if bid not in cached_hashes:
                to_fetch.append(meta)              # brand-new bill
            elif new_hash and cached_hashes[bid] != new_hash:
                to_fetch.append(meta)              # hash changed → updated
            else:
                stats["skipped"] += 1              # unchanged

        total_fetch = len(to_fetch)
        logger.info(
            f"Refresh: {stats['skipped']} unchanged, {total_fetch} to fetch"
        )

        updates = []
        for i, meta in enumerate(to_fetch):
            if job_stop_reason():
                break
            bid = meta["bill_id"]
            detail_data = self._api_get({"op": "getBill", "id": bid})
            details = detail_data.get("bill")
            if detail_data.get("status") != "OK" or not isinstance(details, dict) or str(details.get("bill_id")) != str(bid):
                logger.warning("getBill failed for bill %s", bid)
                stats["errors"] += 1
                continue
            details["change_hash"] = meta.get("change_hash", "")
            updates.append((_flatten_bill_to_row(details, jurisdiction, session_id), details))
            if progress_cb and (i + 1) % 25 == 0:
                progress_cb((i + 1) / total_fetch, f"Fetched {i + 1}/{total_fetch} changed bills")

        pending_votes = {
            row["roll_call_id"]: row["bill_id"]
            for row in conn.execute(
                """SELECT r.roll_call_id, r.bill_id FROM roll_calls r
                   JOIN bills b ON b.bill_id=r.bill_id
                   WHERE b.session_id=? AND COALESCE(r.total, 1)>0
                   AND NOT EXISTS (SELECT 1 FROM legislator_votes v WHERE v.roll_call_id=r.roll_call_id)""",
                (session_id,),
            )
        }
        for row, details in updates:
            for roll_call in details.get("votes") or []:
                roll_call_id = roll_call.get("roll_call_id")
                if not roll_call_id or roll_call.get("total") == 0:
                    continue
                if roll_call.get("votes"):
                    pending_votes.pop(roll_call_id, None)
                elif not conn.execute(
                    "SELECT 1 FROM legislator_votes WHERE roll_call_id=? LIMIT 1", (roll_call_id,),
                ).fetchone():
                    pending_votes[roll_call_id] = row["bill_id"]

        vote_updates = []
        stats["vote_errors"] = 0
        for roll_call_id, bill_id in pending_votes.items():
            if job_stop_reason():
                break
            detail = self._fetch_member_votes(roll_call_id)
            if detail is None:
                stats["vote_errors"] += 1
            else:
                vote_updates.append((detail, bill_id))
        stats["errors"] += stats["vote_errors"]

        # A separate connection keeps UI reads and keyword writes from committing this batch.
        writer = sqlite3.connect(self.db_path)
        writer.execute("PRAGMA foreign_keys=ON")
        try:
            with writer:
                for row, details in updates:
                    self._upsert_bill(writer, row, stats)
                    for roll_call in details.get("votes") or []:
                        self._upsert_rollcall(writer, roll_call, row["bill_id"])
                for roll_call, bill_id in vote_updates:
                    self._upsert_rollcall(writer, roll_call, bill_id)
                if not stats["errors"]:
                    now = datetime.now(timezone.utc).isoformat()
                    writer.execute(
                        "UPDATE sessions SET last_masterlist=? WHERE session_id=?",
                        (now, session_id),
                    )
                    writer.execute(
                        "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)",
                        (f"last_incremental_{jurisdiction}", now),
                    )
        finally:
            writer.close()

        stats["api_calls"] = self._api_calls_run - start_calls
        logger.info(f"Incremental refresh complete: {stats}")
        return stats

    # ── Upsert helper ─────────────────────────────────────────────────────────

    def _upsert_bill(
        self, conn: sqlite3.Connection, row: dict, stats: dict
    ) -> None:
        """Insert or update one bill row; increment stats counters."""
        exists = conn.execute(
            "SELECT 1 FROM bills WHERE bill_id=?", (row["bill_id"],)
        ).fetchone()

        if exists:
            conn.execute(
                """
                UPDATE bills SET
                    session_id=?, jurisdiction=?, bill_number=?, title=?,
                    description=?, status_date=?, status_stage=?, url=?,
                    committee=?, sponsor_names=?, subjects=?, history=?,
                    last_action=?, last_action_date=?, referrals=?,
                    change_hash=?, last_fetched=?,
                    latest_doc_id=COALESCE(?, latest_doc_id),
                    latest_doc_url=COALESCE(NULLIF(?, ''), latest_doc_url)
                WHERE bill_id=?
                """,
                (
                    row["session_id"], row["jurisdiction"], row["bill_number"],
                    row["title"], row["description"], row["status_date"],
                    row["status_stage"], row["url"], row["committee"],
                    row["sponsor_names"], row["subjects"], row["history"],
                    row["last_action"], row["last_action_date"], row["referrals"],
                    row["change_hash"], row["last_fetched"], row["latest_doc_id"], row["latest_doc_url"], row["bill_id"],
                ),
            )
            stats["updated"] += 1
        else:
            conn.execute(
                """
                INSERT INTO bills (
                    bill_id, session_id, jurisdiction, bill_number, title,
                    description, status_date, status_stage, url, committee,
                    sponsor_names, subjects, history, last_action,
                    last_action_date, referrals, change_hash, last_fetched, latest_doc_id, latest_doc_url
                ) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?)
                """,
                (
                    row["bill_id"], row["session_id"], row["jurisdiction"],
                    row["bill_number"], row["title"], row["description"],
                    row["status_date"], row["status_stage"], row["url"],
                    row["committee"], row["sponsor_names"], row["subjects"],
                    row["history"], row["last_action"], row["last_action_date"],
                    row["referrals"], row["change_hash"], row["last_fetched"], row["latest_doc_id"], row["latest_doc_url"],
                ),
            )
            stats["new"] += 1
        if not row.get("calendar_from_dataset") or not conn.execute(
            "SELECT 1 FROM bill_calendar_snapshots WHERE bill_id=?", (row["bill_id"],),
        ).fetchone():
            self._save_bill_calendar(conn, row, row.get("calendar_feed"), row.get("calendar_source", ""))

    @staticmethod
    def _save_bill_calendar(connection, bill, feed, source_url):
        events = normalize_calendar(bill["bill_id"], feed)
        if events is None:
            return False
        connection.execute(
            """INSERT INTO bill_calendar_snapshots
               (bill_id, jurisdiction, bill_number, title, source_url, events_json, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(bill_id) DO UPDATE SET jurisdiction=excluded.jurisdiction,
               bill_number=excluded.bill_number, title=excluded.title, source_url=excluded.source_url,
               events_json=excluded.events_json, fetched_at=excluded.fetched_at""",
            (bill["bill_id"],
             *(bill[key] if isinstance(bill.get(key), str) else "" for key in ("jurisdiction", "bill_number", "title")),
             source_url, json.dumps(events), datetime.now(timezone.utc).isoformat()),
        )
        return True

    @_corpus_operation
    def get_bill_calendars(self, bill_ids):
        if not bill_ids:
            return {}
        rows = self._get_conn().execute(
            "SELECT * FROM bill_calendar_snapshots WHERE bill_id IN (SELECT value FROM json_each(?))",
            (json.dumps(list(bill_ids)),),
        )
        calendars = {}
        for row in rows:
            details = dict(row)
            events = json.loads(details.pop("events_json"))
            calendars[str(row["bill_id"])] = {"details": details, "events": [{**details, **event} for event in events]}
        return calendars

    @_corpus_operation
    def calendar_refresh_candidates(self, bills, limit=25):
        if not bills:
            return []
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        ordered = self._get_conn().execute(
            """SELECT j.value AS bill_id,
                      max(coalesce(s.fetched_at, ''), coalesce(a.checked_at, '')) AS last_checked
               FROM json_each(?) j LEFT JOIN bill_calendar_snapshots s ON s.bill_id=j.value
               LEFT JOIN bill_calendar_attempts a ON a.bill_id=j.value
               WHERE max(coalesce(s.fetched_at, ''), coalesce(a.checked_at, '')) < ?
               ORDER BY last_checked, j.key LIMIT ?""",
            (json.dumps(list(bills)), cutoff, limit),
        )
        return [bills[str(row["bill_id"])] for row in ordered]

    @_corpus_operation
    def refresh_tracked_calendars(self, bills, progress_cb=None):
        """Read event feeds for an explicit batch while retaining older snapshots on errors."""
        stats = {"new": 0, "updated": 0, "errors": 0, "unavailable": 0, "unchanged": 0,
                 "deferred": 0, "deferred_reason": None, "failure_reasons": {}}
        start_calls = self._api_calls_run
        connection = self._get_conn()
        for index, bill in enumerate(bills, 1):
            bill_id = bill["bill_id"]
            previous = connection.execute(
                "SELECT fetched_at FROM bill_calendar_snapshots WHERE bill_id=?", (bill_id,),
            ).fetchone()
            if not self.calendar_refresh_candidates({str(bill_id): bill}, limit=1):
                stats["unchanged"] += 1
                continue
            calls_before = self._api_calls_run
            details, failure = self._fetch_bill(int(bill_id))
            if failure and job_stop_reason() and self._api_calls_run == calls_before:
                stats["deferred"] = len(bills) - index + 1
                stats["deferred_reason"] = job_stop_reason()
                break
            if failure:
                stats["errors"] += 1
                reasons = stats["failure_reasons"]
                reasons[failure] = reasons.get(failure, 0) + 1
            with connection:
                if self._api_calls_run > calls_before:
                    connection.execute(
                        """INSERT INTO bill_calendar_attempts VALUES (?, ?)
                           ON CONFLICT(bill_id) DO UPDATE SET checked_at=excluded.checked_at""",
                        (bill_id, datetime.now(timezone.utc).isoformat()),
                    )
                if details:
                    metadata = {
                        **bill, "title": details.get("title") or bill.get("title"),
                        "bill_number": details.get("bill_number") or bill.get("bill_number"),
                    }
                    if self._save_bill_calendar(
                        connection, metadata, details.get("calendar"),
                        source_link(details.get("state_link"), details.get("url"), bill.get("url")),
                    ):
                        stats["updated" if previous else "new"] += 1
                    else:
                        stats["unavailable"] += 1
            if progress_cb:
                progress_cb(index / len(bills), f"Checked {index} of {len(bills)} calendar feeds")
        stats["api_calls"] = self._api_calls_run - start_calls
        return stats

    def _fetch_member_votes(self, roll_call_id):
        data = self._api_get({"op": "getRollCall", "id": roll_call_id})
        detail = data.get("roll_call")
        if data.get("status") == "OK" and isinstance(detail, dict):
            if str(detail.get("roll_call_id")) == str(roll_call_id) and isinstance(detail.get("votes"), list):
                if detail["votes"] or detail.get("total") == 0:
                    return detail
        logger.warning("Individual votes unavailable for roll call %s", roll_call_id)
        return None

    @_corpus_operation
    def keyword_record_from_corpus(self, bill_id, change_hash, jurisdiction):
        if not change_hash:
            return None
        row = self._get_conn().execute("""
            SELECT b.*, s.session_name AS session FROM bills b LEFT JOIN sessions s USING (session_id)
            WHERE b.bill_id=? AND b.change_hash=? AND b.jurisdiction=?
        """, (bill_id, change_hash, jurisdiction)).fetchone()
        return dict(row) if row else None

    def _process_bill_votes(self, conn, bill_detail):
        errors = 0
        bill_id = bill_detail.get("bill_id")
        for roll_call in bill_detail.get("votes") or []:
            roll_call_id = roll_call.get("roll_call_id")
            if not roll_call_id or not bill_id:
                continue
            self._upsert_rollcall(conn, roll_call, bill_id)
            if roll_call.get("total") == 0 or conn.execute(
                "SELECT 1 FROM legislator_votes WHERE roll_call_id=? LIMIT 1", (roll_call_id,),
            ).fetchone():
                continue
            if job_stop_reason():
                errors += 1
                break
            detail = self._fetch_member_votes(roll_call_id)
            if detail is None:
                errors += 1
            else:
                self._upsert_rollcall(conn, detail, bill_id)
        return errors

    # ── Keyword match overlay ─────────────────────────────────────────────────

    @_corpus_operation
    def record_keyword_match(self, bill_id: int, keyword: str) -> None:
        """Record that a bill matched a keyword during a scan (idempotent)."""
        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR IGNORE INTO keyword_matches (bill_id, keyword, matched_at)
            VALUES (?, ?, ?)
            """,
            (bill_id, keyword, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

    @_corpus_operation
    def get_keyword_matches(self, bill_id: int) -> list[str]:
        """Return list of keywords that matched this bill."""
        rows = self._get_conn().execute(
            "SELECT keyword FROM keyword_matches WHERE bill_id=?", (bill_id,)
        ).fetchall()
        return [r[0] for r in rows]

    # ── Search interface (returns DataFrame matching legacy CSV schema) ────────

    @staticmethod
    def _literal_count_key(where, params):
        # Retain a digest, not search text or bill IDs.
        return sha256(repr((where, params)).encode("utf-8")).digest()

    def _cached_literal_count(self, key, revision):
        with self._literal_counts_lock:
            entry = self._literal_counts.get(key)
            if entry and entry[0] == revision and entry[1] > time.monotonic():
                self._literal_counts.move_to_end(key)
                return entry[2]
        return None

    def _save_literal_count(self, key, revision, total):
        if corpus_file_revision(self.db_path) == revision:
            with self._literal_counts_lock:
                self._literal_counts[key] = (revision, time.monotonic() + 60, total)
                self._literal_counts.move_to_end(key)
                if len(self._literal_counts) > 128:
                    self._literal_counts.popitem(last=False)

    def _count_bills(self, connection, where, params, *, cache):
        sql = f"SELECT COUNT(*) FROM bills b {where}"
        if not cache or connection.in_transaction:
            return connection.execute(sql, params).fetchone()[0]
        key = self._literal_count_key(where, params)
        revision = corpus_file_revision(self.db_path)
        total = self._cached_literal_count(key, revision)
        if total is None:
            total = connection.execute(sql, params).fetchone()[0]
            self._save_literal_count(key, revision, total)
        return total

    @staticmethod
    def _probe_search_page(connection, sql, params):
        # Common terms can finish an ordinary ordered page with little work.
        connection.set_progress_handler(lambda: 1, 20_000)
        try:
            return connection.execute(sql, params).fetchall()
        except sqlite3.OperationalError as error:
            if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_INTERRUPT:
                raise
            return None
        finally:
            connection.set_progress_handler(None, 0)

    def _literal_page_and_count(self, connection, where, params, page_sql, page_args, combined_sql):
        key = self._literal_count_key(where, params)
        revision = corpus_file_revision(self.db_path)
        total = self._cached_literal_count(key, revision)
        probe_needed = True
        if total is not None:
            rows = self._probe_search_page(connection, page_sql, page_args) if total else []
            if rows is not None and corpus_file_revision(self.db_path) == revision:
                return total, pd.DataFrame([dict(row) for row in rows])
            probe_needed = rows is not None

        with connection:
            connection.execute("BEGIN")
            rows = self._probe_search_page(connection, page_sql, page_args) if probe_needed else None
            if rows is None:
                combined = connection.execute(combined_sql, page_args).fetchall()
                total = combined[0]["_total"]
                rows = []
                for row in combined:
                    if row["bill_id"] is not None:
                        values = dict(row)
                        del values["_total"]
                        rows.append(values)
            else:
                total = self._count_bills(connection, where, params, cache=False)
        self._save_literal_count(key, revision, total)
        return total, pd.DataFrame([dict(row) for row in rows])

    @_corpus_operation
    def search_bills(
        self,
        query: str = "",
        jurisdiction_filter: Optional[list[str]] = None,
        status_filter: Optional[list[str]] = None,
        keyword_filter: Optional[list[str]] = None,
        limit: int = 500,
        session_id: Optional[int] = None,
        topic_terms: Optional[list[str]] = None,
        *,
        topic_required=None,
        topic_excluded=None,
        date_range=None,
        sponsors=None,
        committees=None,
        bill_ids=None,
        excluded_bill_ids=None,
        sort="Status Date (Newest)",
        offset=0,
        count_only=False,
        include_total=False,
    ) -> pd.DataFrame | int | tuple[int, pd.DataFrame]:
        """
        Search the master corpus.

        Returns a DataFrame with the same column names as the legacy keyword-
        match CSV (jurisdiction_level, jurisdiction_name, bill_number, title,
        description, status_date, status_stage, url, committee, committees,
        sponsors, sponsor_names, subjects, history, last_action,
        last_action_date, referrals, keyword, session) so that all existing
        _render_bill_expander(), filter, and export code works unchanged.

        jurisdiction_filter entries may be friendly names ("California") or
        jurisdiction codes ("CA"). With include_total, return (total, page).
        """
        conn = self._get_conn()
        conditions: list[str] = []
        params: list = []

        if query:
            fields = ("bill_number", "title", "description", "sponsor_names", "subjects", "committee")
            clauses = [f"instr(lower(coalesce(b.{field}, '')), ?) > 0" for field in fields]
            conditions.append("(" + " OR ".join(clauses) + ")")
            params.extend([query.lower()] * len(fields))

        if date_range and len(date_range) == 2:
            conditions.append("substr(b.status_date, 1, 10) BETWEEN ? AND ?")
            params.extend(map(str, date_range))
        if sponsors:
            clauses = [
                "(lower(b.sponsor_names) = lower(?) OR "
                "instr(',' || replace(lower(b.sponsor_names), ', ', ',') || ',', ',' || lower(?) || ',') > 0)"
                for sponsor in sponsors
            ]
            conditions.append("(" + " OR ".join(clauses) + ")")
            params.extend(value for sponsor in sponsors for value in (sponsor, sponsor))
        if committees:
            conditions.append("b.committee IN (SELECT value FROM json_each(?))")
            params.append(json.dumps(list(committees)))
        if bill_ids is not None:
            conditions.append("b.bill_id IN (SELECT value FROM json_each(?))")
            params.append(json.dumps(list(_canonical_bill_ids(bill_ids))))
        if excluded_bill_ids:
            conditions.append("b.bill_id NOT IN (SELECT value FROM json_each(?))")
            params.append(json.dumps(list(_canonical_bill_ids(excluded_bill_ids))))

        def topic_clause(term):
            params.extend([term.casefold()] * 3)
            return (
                "(instr(casefold(coalesce(b.title, '')), ?) > 0 OR "
                "instr(casefold(coalesce(b.description, '')), ?) > 0 OR "
                "instr(casefold(coalesce(b.subjects, '')), ?) > 0)"
            )

        if topic_terms:
            conditions.append("(" + " OR ".join(topic_clause(term) for term in topic_terms) + ")")
        for term in topic_required or ():
            conditions.append(topic_clause(term))
        for term in topic_excluded or ():
            conditions.append("NOT " + topic_clause(term))

        if jurisdiction_filter:
            # Accept both friendly names and raw codes
            label_to_code = {v: k for k, v in JURISDICTION_LABELS.items()}
            codes = [
                label_to_code.get(j, j) for j in jurisdiction_filter
            ]
            placeholders = ",".join("?" * len(codes))
            conditions.append(f"b.jurisdiction IN ({placeholders})")
            params.extend(codes)

        if status_filter:
            placeholders = ",".join("?" * len(status_filter))
            conditions.append(f"b.status_stage IN ({placeholders})")
            params.extend(status_filter)

        if session_id is not None:
            conditions.append("b.session_id = ?")
            params.append(session_id)

        if keyword_filter:
            placeholders = ",".join("?" * len(keyword_filter))
            conditions.append(
                f"EXISTS (SELECT 1 FROM keyword_matches km "
                f"WHERE km.bill_id = b.bill_id AND casefold(km.keyword) IN ({placeholders}))"
            )
            params.extend(keyword.casefold() for keyword in keyword_filter)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        cache_count = bool(query) and bill_ids is None and not excluded_bill_ids
        if count_only:
            return self._count_bills(conn, where, params, cache=cache_count)

        sort_field, direction = {
            "Most Recent Action": ("last_action_date", "DESC"),
            "Status Date (Newest)": ("status_date", "DESC"),
            "Bill Number A→Z": ("bill_number", "COLLATE NOCASE ASC"),
            "Status Stage": ("status_stage", "ASC"),
        }.get(sort, ("status_date", "DESC"))
        order_by = f"b.{sort_field} {direction}, b.bill_id"
        fields = """
                b.bill_id,

                -- ── Jurisdiction columns (same names as legacy CSV) ──
                CASE b.jurisdiction
                    WHEN 'CA' THEN 'State'
                    WHEN 'US' THEN 'Federal'
                    ELSE 'Unknown'
                END                                        AS jurisdiction_level,
                CASE b.jurisdiction
                    WHEN 'CA' THEN 'California'
                    WHEN 'US' THEN 'U.S. Congress'
                    ELSE b.jurisdiction
                END                                        AS jurisdiction_name,

                b.bill_number,
                b.title,
                b.description,
                b.status_date,
                b.status_stage,
                b.url,
                b.committee                                AS committee,
                b.committee                                AS committees,
                b.sponsor_names                            AS sponsor_names,
                b.sponsor_names                            AS sponsors,
                b.subjects,
                b.history,
                b.last_action,
                b.last_action_date,
                b.referrals,
                b.change_hash,
                b.latest_doc_id,
                b.latest_doc_url,
                b.last_fetched,

                -- ── Keyword overlay (comma-joined for display) ──
                COALESCE(
                    (SELECT GROUP_CONCAT(km.keyword, '; ')
                     FROM keyword_matches km
                     WHERE km.bill_id = b.bill_id),
                    ''
                )                                          AS keyword,

                s.session_name                             AS session
        """
        sql = f"""
            SELECT {fields}
            FROM bills b
            LEFT JOIN sessions s ON b.session_id = s.session_id
            {where}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        page_args = [*params, -1 if limit is None else limit, offset]

        if include_total:
            if cache_count and limit is not None and not conn.in_transaction and sqlite3.sqlite_version_info >= (3, 35):
                combined = f"""
                    WITH matches AS MATERIALIZED (
                        SELECT b.bill_id, b.{sort_field} FROM bills b {where}
                    ), page AS MATERIALIZED (
                        SELECT b.bill_id FROM matches b ORDER BY {order_by} LIMIT ? OFFSET ?
                    ), totals AS (SELECT COUNT(*) AS total FROM matches)
                    SELECT totals.total AS _total, {fields}
                    FROM totals LEFT JOIN page ON 1
                    LEFT JOIN bills b ON b.bill_id=page.bill_id
                    LEFT JOIN sessions s ON s.session_id=b.session_id
                    ORDER BY {order_by}
                """
                return self._literal_page_and_count(conn, where, params, sql, page_args, combined)
            total = self._count_bills(conn, where, params, cache=cache_count)
            return total, pd.DataFrame([dict(row) for row in conn.execute(sql, page_args)])

        try:
            rows = conn.execute(sql, page_args).fetchall()
        except Exception as exc:
            logger.error(f"search_bills SQL error: {exc}")
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    # ── Bulk bill lookup by bill_id ───────────────────────────────────────────

    @_corpus_operation
    def indexed_bill_ids(self, bill_ids):
        return {str(row[0]) for row in self._get_conn().execute(
            "SELECT bill_id FROM bills WHERE bill_id IN (SELECT value FROM json_each(?))",
            (json.dumps(list(bill_ids)),),
        )}

    @_corpus_operation
    def get_bills_by_ids(self, bill_ids: list) -> "pd.DataFrame":
        """
        Return a DataFrame of bills whose bill_id is in the provided list.
        Uses the same column schema as search_bills() so all rendering helpers
        work unchanged.  No API calls; local DB only.
        """
        if not bill_ids:
            return pd.DataFrame()
        conn  = self._get_conn()
        placeholders = ",".join("?" * len(bill_ids))
        sql = f"""
            SELECT
                b.bill_id,
                CASE b.jurisdiction WHEN 'CA' THEN 'State'   WHEN 'US' THEN 'Federal' ELSE 'Unknown' END AS jurisdiction_level,
                CASE b.jurisdiction WHEN 'CA' THEN 'California' WHEN 'US' THEN 'U.S. Congress' ELSE b.jurisdiction END AS jurisdiction_name,
                b.bill_number, b.title, b.description, b.status_date, b.status_stage,
                b.url, b.committee AS committee, b.committee AS committees,
                b.sponsor_names AS sponsor_names, b.sponsor_names AS sponsors,
                b.subjects, b.history, b.last_action, b.last_action_date, b.referrals,
                b.change_hash, b.latest_doc_id, b.latest_doc_url, b.last_fetched,
                COALESCE(
                    (SELECT GROUP_CONCAT(km.keyword, '; ')
                     FROM keyword_matches km WHERE km.bill_id = b.bill_id), ''
                ) AS keyword,
                s.session_name AS session
            FROM bills b
            LEFT JOIN sessions s ON b.session_id = s.session_id
            WHERE b.bill_id IN ({placeholders})
            ORDER BY b.status_date DESC
        """
        try:
            # Cast bill_ids to int before query
            int_ids = [int(x) for x in bill_ids]
            rows = conn.execute(sql, int_ids).fetchall()
        except Exception as exc:
            logger.error(f"get_bills_by_ids SQL error: {exc}")
            return pd.DataFrame()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    # ── Stats ─────────────────────────────────────────────────────────────────

    @_corpus_operation
    def get_historical_coverage(self):
        """Report indexed and cached material for each discovered CA or US session."""
        return pd.read_sql_query("""
            WITH text_cache AS (
                SELECT DISTINCT bill_id FROM bill_texts
                WHERE length(content_html)>0 OR length(content_pdf_url)>0
            ),
            bill_coverage AS (
                SELECT b.session_id, COUNT(*) AS bills,
                       SUM(b.latest_doc_id IS NOT NULL OR length(coalesce(b.latest_doc_url, ''))>0
                           OR t.bill_id IS NOT NULL) AS bills_with_text,
                       SUM(t.bill_id IS NOT NULL) AS bills_with_cached_text
                FROM bills b LEFT JOIN text_cache t ON t.bill_id=b.bill_id
                GROUP BY b.session_id
            ),
            roll_call_coverage AS (
                SELECT b.session_id, COUNT(*) AS roll_calls,
                       SUM(EXISTS (SELECT 1 FROM legislator_votes v
                                   WHERE v.roll_call_id=r.roll_call_id)) AS with_individual_votes
                FROM roll_calls r JOIN bills b ON b.bill_id=r.bill_id
                GROUP BY b.session_id
            )
            SELECT s.session_id, s.jurisdiction, s.session_name, s.year_start, s.year_end,
                   s.last_bootstrap, s.last_masterlist,
                   coalesce(b.bills, 0) AS bills, coalesce(b.bills_with_text, 0) AS bills_with_text,
                   coalesce(b.bills_with_cached_text, 0) AS bills_with_cached_text,
                   coalesce(r.roll_calls, 0) AS roll_calls,
                   coalesce(r.with_individual_votes, 0) AS with_individual_votes
            FROM sessions s LEFT JOIN bill_coverage b ON b.session_id=s.session_id
            LEFT JOIN roll_call_coverage r ON r.session_id=s.session_id
            WHERE s.jurisdiction IN ('CA', 'US')
            ORDER BY s.year_start DESC, s.jurisdiction, s.session_id DESC
        """, self._get_conn())

    @_corpus_operation
    def get_filter_options(self):
        conn = self._get_conn()
        sponsors = {
            name.strip()
            for row in conn.execute("SELECT DISTINCT sponsor_names FROM bills WHERE sponsor_names <> ''")
            for name in row[0].split(",") if name.strip()
        }
        committees = {row[0] for row in conn.execute("SELECT DISTINCT committee FROM bills WHERE committee <> ''")}
        return sponsors, committees

    @_corpus_operation
    def get_corpus_stats(self) -> dict:
        """Return summary statistics about the local corpus (no API calls)."""
        conn = self._get_conn()

        def _count(sql, *args):
            return conn.execute(sql, args).fetchone()[0]

        return {
            "total_bills":         _count("SELECT COUNT(*) FROM bills"),
            "ca_bills":            _count("SELECT COUNT(*) FROM bills WHERE jurisdiction='CA'"),
            "us_bills":            _count("SELECT COUNT(*) FROM bills WHERE jurisdiction='US'"),
            "active_sessions":     _count("SELECT COUNT(*) FROM sessions WHERE is_active=1"),
            "keyword_matches":     _count("SELECT COUNT(*) FROM keyword_matches"),
            "last_bootstrap_CA":   self._meta_get("last_bootstrap_CA"),
            "last_bootstrap_US":   self._meta_get("last_bootstrap_US"),
            "last_incremental_CA": self._meta_get("last_incremental_CA"),
            "last_incremental_US": self._meta_get("last_incremental_US"),
            "schema_version":      self._meta_get("schema_version"),
        }

    @_corpus_operation
    def get_all_session_jurisdictions(self) -> list[str]:
        """Return distinct jurisdiction codes that have cached sessions."""
        rows = self._get_conn().execute(
            "SELECT DISTINCT jurisdiction FROM sessions WHERE is_active=1"
        ).fetchall()
        return [r[0] for r in rows]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        connection = getattr(self._thread, "connection", None)
        if connection is not None:
            connection.close()
            del self._thread.connection

    # ── Roll Call & People API ────────────────────────────────────────────────

    @_corpus_operation
    def get_roll_calls_for_bill(self, bill_id: int) -> list[dict]:
        """Fetch all roll call metrics for a given bill."""
        conn = self._get_conn()
        sql = "SELECT * FROM roll_calls WHERE bill_id=? ORDER BY date DESC"
        rows = conn.execute(sql, (bill_id,)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            # attach individual member votes
            v_sql = """
                SELECT lv.*, p.name, p.party, pm.staff_legislator_id
                FROM legislator_votes lv
                LEFT JOIN people p ON lv.people_id = p.people_id
                LEFT JOIN people_mapping pm ON lv.people_id = pm.people_id
                WHERE lv.roll_call_id=?
                ORDER BY p.name
            """
            v_rows = conn.execute(v_sql, (d["roll_call_id"],)).fetchall()
            d["member_votes"] = [dict(vr) for vr in v_rows]
            result.append(d)
        return result

    def get_votes_for_legislator(self, staff_legislator_id: str) -> list[dict]:
        """Fetch recorded votes for a legislator linked to the staff directory."""
        return self._get_legislator_votes(
            "JOIN people_mapping pm ON lv.people_id = pm.people_id",
            "pm.staff_legislator_id = ? AND pm.match_quality IN ('Verified Name and Jurisdiction', 'Manual verified')", (staff_legislator_id,),
        )

    @_corpus_operation
    def get_votes_for_legislator_by_name(self, first_name: str, last_name: str, jurisdiction=None, profile_id=None) -> list[dict]:
        """Use a unique full-name match when an identity link is unavailable."""
        if not first_name or not last_name:
            return []
        matches = self._get_conn().execute(
            """SELECT p.people_id FROM people p
               LEFT JOIN people_mapping m ON m.people_id=p.people_id
               WHERE LOWER(p.first_name)=LOWER(?) AND LOWER(p.last_name)=LOWER(?)
               AND coalesce(m.match_quality, '') NOT IN ('Ambiguous', 'Manual excluded')
               AND (? IS NULL OR m.staff_legislator_id IS NULL OR m.staff_legislator_id=?)""",
            (first_name.strip(), last_name.strip(), profile_id, profile_id),
        ).fetchall()
        if jurisdiction:
            matches = [
                row for row in matches if self._get_conn().execute(
                    """SELECT 1 FROM legislator_votes v JOIN roll_calls r ON v.roll_call_id=r.roll_call_id
                       JOIN bills b ON b.bill_id=r.bill_id WHERE v.people_id=? AND b.jurisdiction=? LIMIT 1""",
                    (row[0], jurisdiction),
                ).fetchone()
            ]
        if len(matches) != 1:
            return []
        where = "lv.people_id = ?"
        params = (matches[0][0],)
        if jurisdiction:
            where += " AND b.jurisdiction = ?"
            params += (jurisdiction,)
        return self._get_legislator_votes("", where, params)

    @_corpus_operation
    def _get_legislator_votes(self, identity_join, condition, params):
        rows = self._get_conn().execute(f"""
            SELECT rc.roll_call_id, lv.vote_text, rc.date AS vote_date,
                   rc.desc AS motion, rc.passed, rc.chamber AS vote_chamber, b.bill_number, b.title,
                   b.jurisdiction, b.bill_id, b.subjects, b.committee,
                   s.session_name AS session
            FROM legislator_votes lv
            JOIN roll_calls rc ON lv.roll_call_id = rc.roll_call_id
            JOIN bills b ON rc.bill_id = b.bill_id
            LEFT JOIN sessions s ON b.session_id = s.session_id
            {identity_join}
            WHERE {condition}
            ORDER BY rc.date DESC, rc.roll_call_id DESC
        """, params).fetchall()
        return [dict(row) for row in rows]

    @_corpus_operation
    def get_staff_cross_reference(self, staff_name: str) -> list[dict]:
        """
        Find all legislators a given staff member works under,
        and what issue areas they cover. Uses staff_manager-side data
        via a direct name search across legislator_votes people names.
        Returns list of {people_id, name, party, chamber, district}.
        """
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM people WHERE LOWER(name) LIKE ? LIMIT 20",
            (f"%{staff_name.lower()}%",)
        ).fetchall()
        return [dict(r) for r in rows]

        
    @_corpus_operation
    def sync_people_mapping(self, staff_df):
        """Link recorded voters to a unique directory identity in the same jurisdiction."""
        from staff_manager import normalize_name_components

        def normalized(value):
            return normalize_name_components(value)["full"]

        def chamber(value):
            value = str(value or "").lower().strip()
            if value in {"s", "sen", "senate", "upper"}:
                return "upper"
            if value in {"a", "h", "assembly", "house", "lower", "house of representatives"}:
                return "lower"
            return ""

        directory = {}
        for row in staff_df.to_dict("records"):
            name = normalized(row.get("name"))
            state = str(row.get("state") or "").upper()
            if name and state and row.get("legislator_id"):
                directory.setdefault((name, state), []).append(row)

        conn = self._get_conn()
        context = {}
        for source in conn.execute(
            """SELECT DISTINCT v.people_id, b.jurisdiction FROM legislator_votes v
               JOIN roll_calls r ON r.roll_call_id=v.roll_call_id
               JOIN bills b ON b.bill_id=r.bill_id"""
        ):
            context.setdefault(source["people_id"], set()).add(source["jurisdiction"])

        manual = {row["people_id"]: row for row in conn.execute(
            "SELECT * FROM people_mapping WHERE match_quality IN ('Manual verified', 'Manual excluded')"
        )}
        active_ids = {row["legislator_id"] for rows in directory.values() for row in rows}
        people = conn.execute("SELECT * FROM people").fetchall()
        matched = 0
        with conn:
            for person in people:
                person_id = person["people_id"]
                decision = manual.get(person_id)
                if decision and (
                    decision["match_quality"] == "Manual excluded" or decision["staff_legislator_id"] in active_ids
                ):
                    matched += bool(decision["staff_legislator_id"])
                    continue
                name = normalized(person["name"] or " ".join(filter(None, (person["first_name"], person["last_name"]))))
                candidates = [row for state in context.get(person_id, ()) for row in directory.get((name, state), ())]
                source_chamber = chamber(person["chamber"])
                if source_chamber:
                    candidates = [row for row in candidates if chamber(row.get("chamber")) == source_chamber]
                staff_id = candidates[0]["legislator_id"] if len(candidates) == 1 else None
                quality = "Verified Name and Jurisdiction" if staff_id else "Ambiguous" if candidates else "Unmatched"
                matched += bool(staff_id)
                conn.execute(
                    """INSERT INTO people_mapping (people_id, staff_legislator_id, match_quality)
                       VALUES (?, ?, ?)
                       ON CONFLICT(people_id) DO UPDATE SET staff_legislator_id=excluded.staff_legislator_id,
                       match_quality=excluded.match_quality""",
                    (person_id, staff_id, quality),
                )
        return {"matched": matched, "unmatched": len(people) - matched, "total": len(people)}

    def get_federal_member_votes(self, people_id):
        return self._get_legislator_votes("", "lv.people_id = ? AND b.jurisdiction = 'US'", (people_id,))

    @_corpus_operation
    def list_vote_identities(self, query="", *, include_linked=False, limit=100, jurisdiction=None):
        return [dict(row) for row in self._get_conn().execute(
            """SELECT p.people_id, p.name, p.chamber, p.district,
                      m.staff_legislator_id, coalesce(m.match_quality, 'Unreviewed') AS quality,
                      GROUP_CONCAT(DISTINCT b.jurisdiction) AS jurisdictions,
                      GROUP_CONCAT(DISTINCT s.session_name) AS sessions,
                      COUNT(DISTINCT r.roll_call_id) AS votes
               FROM people p JOIN legislator_votes v ON v.people_id=p.people_id
               JOIN roll_calls r ON r.roll_call_id=v.roll_call_id
               JOIN bills b ON b.bill_id=r.bill_id
               LEFT JOIN sessions s ON s.session_id=b.session_id
               LEFT JOIN people_mapping m ON m.people_id=p.people_id
               WHERE (instr(lower(coalesce(p.name, '')), lower(?))>0 OR ?='')
               AND (? IS NULL OR b.jurisdiction=?)
               GROUP BY p.people_id
               HAVING ? OR coalesce(m.match_quality, '') NOT IN
                    ('Verified Name and Jurisdiction', 'Manual verified', 'Manual excluded')
               ORDER BY votes DESC, p.name, p.people_id LIMIT ?""",
            (query, query, jurisdiction, jurisdiction, include_linked, limit),
        )]

    @_corpus_operation
    def review_vote_identity(self, people_id, profile=None):
        """Persist an admin decision separately from automated name matching."""
        conn = self._get_conn()
        jurisdictions = {
            row[0] for row in conn.execute(
                """SELECT DISTINCT b.jurisdiction FROM legislator_votes v
                   JOIN roll_calls r ON r.roll_call_id=v.roll_call_id JOIN bills b ON b.bill_id=r.bill_id
                   WHERE v.people_id=?""", (people_id,),
            )
        }
        if not jurisdictions:
            raise ValueError("No recorded votes were found for that identity.")
        if profile and profile.get("state") not in jurisdictions:
            raise ValueError("The profile is outside the jurisdictions in the voter record.")
        with conn:
            conn.execute(
                """INSERT INTO people_mapping (people_id, staff_legislator_id, match_quality) VALUES (?, ?, ?)
                   ON CONFLICT(people_id) DO UPDATE SET staff_legislator_id=excluded.staff_legislator_id,
                   match_quality=excluded.match_quality""",
                (people_id, profile["legislator_id"] if profile else None,
                 "Manual verified" if profile else "Manual excluded"),
            )

    @_corpus_operation
    def get_people_mapping_stats(self) -> dict:
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM people").fetchone()[0]
        matched = conn.execute("SELECT COUNT(*) FROM people_mapping WHERE staff_legislator_id IS NOT NULL").fetchone()[0]
        unmatched = conn.execute("SELECT COUNT(*) FROM people_mapping WHERE staff_legislator_id IS NULL").fetchone()[0]
        return {"total": total, "matched": matched, "unmatched": unmatched}

    def get_bill(self, bill_id: int) -> Optional[dict]:
        """Fetch detailed bill JSON from LegiScan API by bill_id."""
        details, _ = self._fetch_bill(bill_id)
        return details

    def _fetch_bill(self, bill_id):
        if not self.api_key.strip():
            self._thread.request_failure = MISSING_KEY
            return None, MISSING_KEY
        logger.info(f"API: get_bill(bill_id={bill_id})")
        res = self._api_get({"op": "getBill", "id": bill_id})
        if res.get("status") == "OK":
            bill = res.get("bill")
            if isinstance(bill, dict) and str(bill.get("bill_id")) == str(bill_id):
                return bill, None
            return None, "LegiScan did not return the requested bill"
        if failure := res.get("request_failure"):
            return None, failure
        alert = res.get("alert")
        message = str(alert.get("message", "")).casefold() if isinstance(alert, dict) else ""
        if "limit" in message or "quota" in message:
            return None, "LegiScan reported a request limit"
        if "api key" in message or "apikey" in message:
            return None, "LegiScan reported an API key problem"
        if "bill" in message and any(term in message for term in ("not found", "invalid", "unknown")):
            return None, "LegiScan could not find the requested bill"
        return None, "LegiScan rejected the bill request"

    @_corpus_operation
    def get_bill_versions(self, bill_id: int) -> list[dict]:
        self._thread.request_failure = None
        revision = self._get_conn().execute(
            "SELECT change_hash, latest_doc_id FROM bills WHERE bill_id=?", (bill_id,),
        ).fetchone()
        key = (bill_id, *revision) if revision else (bill_id,)
        with self._version_lock:
            cached = self._version_lists.get(key)
            if cached and cached[0] > time.monotonic():
                self._version_lists.move_to_end(key)
                return deepcopy(cached[1])
        details = self.get_bill(bill_id)
        if details is None:
            return []
        versions = {}
        for text in details.get("texts") or []:
            if isinstance(text, dict) and text.get("doc_id"):
                versions[int(text["doc_id"])] = text
        ordered = sorted(versions.values(), key=lambda item: (item.get("date") or "", int(item["doc_id"])))
        with self._version_lock:
            self._version_lists[key] = (time.monotonic() + 3 * 60 * 60, deepcopy(ordered))
            self._version_lists.move_to_end(key)
            if len(self._version_lists) > 256:
                self._version_lists.popitem(last=False)
        return ordered

    @_corpus_operation
    def get_bill_text(self, bill_id: int, doc_id: Optional[int] = None) -> Optional[dict]:
        """
        Get the full text of a bill. Uses local cache in bill_texts table if present.
        Returns a dict: { 'html': str, 'pdf_url': str, 'mime': str }
        """
        self._thread.request_failure = None
        conn = self._get_conn()
        
        # 1. Try cache
        if doc_id:
            cached = conn.execute(
                "SELECT content_html, content_pdf_url, mime_type FROM bill_texts WHERE doc_id=? AND bill_id=?",
                (doc_id, bill_id)
            ).fetchone()
            if cached:
                return {
                    "html": cached["content_html"], 
                    "pdf_url": cached["content_pdf_url"], 
                    "mime": cached["mime_type"]
                }

        # 2. If no doc_id given, find the latest one from the bills table
        if not doc_id:
            row = conn.execute("SELECT latest_doc_id FROM bills WHERE bill_id=?", (bill_id,)).fetchone()
            if row and row["latest_doc_id"]:
                doc_id = row["latest_doc_id"]
            else:
                # Need to refresh bill details to find texts
                details = self.get_bill(bill_id)
                if details:
                    # Update the bill row with latest text markers
                    # (This also ensures subsequent calls skip this check)
                    texts = details.get("texts", [])
                    if texts:
                        latest = texts[-1]
                        doc_id = latest.get("doc_id")
                        conn.execute(
                            "UPDATE bills SET latest_doc_id=?, latest_doc_url=? WHERE bill_id=?",
                            (doc_id, latest.get("url", ""), bill_id)
                        )
                        conn.commit()

        if not doc_id:
            return None

        # 3. Double check cache with resolved doc_id
        cached = conn.execute(
            "SELECT content_html, content_pdf_url, mime_type FROM bill_texts WHERE doc_id=? AND bill_id=?",
            (doc_id, bill_id)
        ).fetchone()
        if cached:
            return {
                "html": cached["content_html"], 
                "pdf_url": cached["content_pdf_url"], 
                "mime": cached["mime_type"]
            }

        # 4. Fetch from API
        logger.info(f"API: get_bill_text(doc_id={doc_id})")
        res = self._api_get({"op": "getBillText", "id": doc_id})
        if res.get("status") == "OK" and "text" in res:
            text_data = res["text"]
            mime = text_data.get("mime", "")
            content_b64 = text_data.get("doc", "")
            
            html_content = ""
            pdf_url = text_data.get("state_link", "")
            
            if "html" in mime.lower() and content_b64:
                try:
                    html_content = base64.b64decode(content_b64).decode("utf-8", errors="replace")
                except:
                    html_content = "Failed to decode HTML content."
            
            # Cache it
            conn.execute(
                """INSERT INTO bill_texts 
                   (doc_id, bill_id, mime_type, content_html, content_pdf_url, last_fetched)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (doc_id, bill_id, mime, html_content, pdf_url, datetime.now(timezone.utc).isoformat())
            )
            conn.commit()
            
            return {"html": html_content, "pdf_url": pdf_url, "mime": mime}

        return None
