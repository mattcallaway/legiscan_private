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
from datetime import datetime, timezone
from typing import Callable, Optional

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────
SCHEMA_VERSION = "1"
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
        "latest_doc_id":    latest_doc_id,
        "latest_doc_url":   latest_doc_url,
        "last_fetched":     datetime.now(timezone.utc).isoformat(),
    }


# ── DDL ────────────────────────────────────────────────────────────────────────
_DDL = """
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
        self.api_key          = api_key
        self.rate_limit_s     = rate_limit_s
        self.download_timeout = download_timeout
        self._api_calls_run   = 0
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()

    # ── Connection ────────────────────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.executescript(_DDL)
        
        # Migration: add columns if they don't exist in bills table
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(bills)")
        cols = [c[1] for c in cur.fetchall()]
        if "latest_doc_id" not in cols:
            conn.execute("ALTER TABLE bills ADD COLUMN latest_doc_id INTEGER")
        if "latest_doc_url" not in cols:
            conn.execute("ALTER TABLE bills ADD COLUMN latest_doc_url TEXT")

        conn.execute(
            "INSERT OR IGNORE INTO sync_meta (key, value) VALUES (?, ?)",
            ("schema_version", SCHEMA_VERSION),
        )
        conn.commit()
        logger.info(f"CorpusManager ready — db={self.db_path}")

    # ── Low-level API helpers ─────────────────────────────────────────────────

    def _api_get(self, params: dict, timeout: Optional[int] = None) -> dict:
        """Make one LegiScan API request (JSON response)."""
        p = dict(params)
        p["key"] = self.api_key
        try:
            r = requests.get(BASE_URL, params=p, timeout=timeout or 30)
            r.raise_for_status()
            self._api_calls_run += 1
            time.sleep(self.rate_limit_s)
            return r.json()
        except Exception as exc:
            logger.error(f"API error ({params.get('op', '?')}): {exc}")
            return {"status": "ERROR", "error": str(exc)}

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

    def get_active_sessions(self, jurisdiction: str) -> list[dict]:
        """
        Fetch current sessions from LegiScan API and cache in DB.
        Returns up to 5 most-recent sessions (sorted newest first).
        Cost: 1 API call.
        """
        data = self._api_get({"op": "getSessionList", "state": jurisdiction})
        if data.get("status") != "OK":
            logger.warning(f"getSessionList failed for {jurisdiction}: {data}")
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
            logger.warning(f"getDatasetList failed for {jurisdiction}: {data}")
            return []
        return data.get("datasetlist", [])

    def bootstrap_session(
        self,
        session_id: int,
        jurisdiction: str,
        progress_cb: Optional[Callable[[float, str], None]] = None,
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
        self._record_bootstrap(session_id, jurisdiction)

        stats["api_calls"] = self._api_calls_run - start_calls
        logger.info(f"Bootstrap complete: {stats}")
        return stats

    def _download_dataset_zip(
        self, session_id: int, access_key: str
    ) -> Optional[bytes]:
        """Download and decode the dataset ZIP. Returns raw bytes or None."""
        p = {
            "op":         "getDataset",
            "id":         session_id,
            "access_key": access_key,
            "key":        self.api_key,
        }
        try:
            r = requests.get(
                BASE_URL, params=p, timeout=self.download_timeout, stream=True
            )
            r.raise_for_status()
            self._api_calls_run += 1
            time.sleep(self.rate_limit_s)
            data = r.json()
        except Exception as exc:
            logger.error(f"getDataset download error: {exc}")
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
                    except Exception as exc: pass
                
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
                    except Exception as exc: pass
                    
                    if processed % 100 == 0:
                        conn.commit()
                        if progress_cb:
                            progress_cb(0.1 + 0.85 * (processed / max(total, 1)), f"Ingested {processed}/{total} files…")

                conn.commit()
                if progress_cb:
                    progress_cb(1.0, f"Done — {stats['new']} new, {stats['updated']} updated")
                logger.info(f"ZIP ingest complete: {stats}")

        except zipfile.BadZipFile as exc:
            logger.error(f"Bad ZIP: {exc}")
            stats["errors"] += 1

    # ── Bootstrap fallback (getMasterListRaw + getBill per bill) ──────────────

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
            logger.error(f"getMasterListRaw failed: {data}")
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
            self._process_bill_votes(conn, bill_detail)

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
        self._record_bootstrap(session_id, jurisdiction)
        return stats

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
        if data.get("status") != "OK":
            logger.error(f"getMasterListRaw failed: {data}")
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

        for i, meta in enumerate(to_fetch):
            bid = meta["bill_id"]
            detail_data = self._api_get({"op": "getBill", "id": bid})
            if detail_data.get("status") != "OK":
                logger.warning(f"getBill failed bill_id={bid}: {detail_data}")
                stats["errors"] += 1
                continue

            bill_detail = detail_data.get("bill", {})
            if not bill_detail:
                stats["errors"] += 1
                continue

            bill_detail["change_hash"] = meta.get("change_hash", "")
            row = _flatten_bill_to_row(bill_detail, jurisdiction, session_id)
            self._upsert_bill(conn, row, stats)
            self._process_bill_votes(conn, bill_detail)

            if (i + 1) % 25 == 0:
                conn.commit()
                if progress_cb:
                    progress_cb(
                        (i + 1) / max(total_fetch, 1),
                        f"Updated {i + 1}/{total_fetch} changed bills…",
                    )

        conn.commit()

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "UPDATE sessions SET last_masterlist=? WHERE session_id=?",
            (now, session_id),
        )
        self._meta_set(f"last_incremental_{jurisdiction}", now)
        conn.commit()

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
                    change_hash=?, last_fetched=?
                WHERE bill_id=?
                """,
                (
                    row["session_id"], row["jurisdiction"], row["bill_number"],
                    row["title"], row["description"], row["status_date"],
                    row["status_stage"], row["url"], row["committee"],
                    row["sponsor_names"], row["subjects"], row["history"],
                    row["last_action"], row["last_action_date"], row["referrals"],
                    row["change_hash"], row["last_fetched"], row["bill_id"],
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
                    last_action_date, referrals, change_hash, last_fetched
                ) VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?, ?,?,?)
                """,
                (
                    row["bill_id"], row["session_id"], row["jurisdiction"],
                    row["bill_number"], row["title"], row["description"],
                    row["status_date"], row["status_stage"], row["url"],
                    row["committee"], row["sponsor_names"], row["subjects"],
                    row["history"], row["last_action"], row["last_action_date"],
                    row["referrals"], row["change_hash"], row["last_fetched"],
                ),
            )
            stats["new"] += 1

    def _process_bill_votes(self, conn: sqlite3.Connection, bill_detail: dict) -> None:
        """Process summary votes array from getBill API, fetching full individual votes if needed."""
        bill_id = bill_detail.get("bill_id")
        if not bill_id: return
        for r in bill_detail.get("votes", []):
            rc_id = r.get("roll_call_id")
            if not rc_id: continue
            self._upsert_rollcall(conn, r, bill_id)
            
            # Check if we have individual member votes dynamically
            v_count = conn.execute("SELECT COUNT(*) FROM legislator_votes WHERE roll_call_id=?", (rc_id,)).fetchone()[0]
            if v_count == 0:
                rc_data = self._api_get({"op": "getRollCall", "id": rc_id})
                if rc_data.get("status") == "OK" and rc_data.get("roll_call"):
                    self._upsert_rollcall(conn, rc_data["roll_call"], bill_id)

    # ── Keyword match overlay ─────────────────────────────────────────────────

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

    def get_keyword_matches(self, bill_id: int) -> list[str]:
        """Return list of keywords that matched this bill."""
        rows = self._get_conn().execute(
            "SELECT keyword FROM keyword_matches WHERE bill_id=?", (bill_id,)
        ).fetchall()
        return [r[0] for r in rows]

    # ── Search interface (returns DataFrame matching legacy CSV schema) ────────

    def search_bills(
        self,
        query: str = "",
        jurisdiction_filter: Optional[list[str]] = None,
        status_filter: Optional[list[str]] = None,
        keyword_filter: Optional[list[str]] = None,
        limit: int = 500,
    ) -> pd.DataFrame:
        """
        Search the master corpus.

        Returns a DataFrame with the same column names as the legacy keyword-
        match CSV (jurisdiction_level, jurisdiction_name, bill_number, title,
        description, status_date, status_stage, url, committee, committees,
        sponsors, sponsor_names, subjects, history, last_action,
        last_action_date, referrals, keyword, session) so that all existing
        _render_bill_expander(), filter, and export code works unchanged.

        jurisdiction_filter entries may be friendly names ("California") or
        jurisdiction codes ("CA").
        """
        conn = self._get_conn()
        conditions: list[str] = []
        params: list = []

        if query:
            q = f"%{query.lower()}%"
            conditions.append(
                "(LOWER(b.bill_number) LIKE ? OR LOWER(b.title) LIKE ? "
                "OR LOWER(b.description) LIKE ? OR LOWER(b.sponsor_names) LIKE ?)"
            )
            params.extend([q, q, q, q])

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

        if keyword_filter:
            placeholders = ",".join("?" * len(keyword_filter))
            conditions.append(
                f"EXISTS (SELECT 1 FROM keyword_matches km "
                f"WHERE km.bill_id = b.bill_id AND km.keyword IN ({placeholders}))"
            )
            params.extend(keyword_filter)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        sql = f"""
            SELECT
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

            FROM bills b
            LEFT JOIN sessions s ON b.session_id = s.session_id
            {where}
            ORDER BY b.status_date DESC, b.bill_number
            LIMIT ?
        """
        params.append(limit)

        try:
            rows = conn.execute(sql, params).fetchall()
        except Exception as exc:
            logger.error(f"search_bills SQL error: {exc}")
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([dict(r) for r in rows])

    # ── Bulk bill lookup by bill_id ───────────────────────────────────────────

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

    def get_all_session_jurisdictions(self) -> list[str]:
        """Return distinct jurisdiction codes that have cached sessions."""
        rows = self._get_conn().execute(
            "SELECT DISTINCT jurisdiction FROM sessions WHERE is_active=1"
        ).fetchall()
        return [r[0] for r in rows]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    # ── Roll Call & People API ────────────────────────────────────────────────

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
        """Fetch a legislator's voting history by our internal staff_legislator_id."""
        conn = self._get_conn()
        sql = """
            SELECT lv.vote_text, rc.date as vote_date, rc.desc as motion, rc.passed,
                   b.bill_number, b.title, b.jurisdiction, b.bill_id
            FROM legislator_votes lv
            JOIN roll_calls rc ON lv.roll_call_id = rc.roll_call_id
            JOIN bills b ON rc.bill_id = b.bill_id
            JOIN people_mapping pm ON lv.people_id = pm.people_id
            WHERE pm.staff_legislator_id = ?
            ORDER BY rc.date DESC
        """
        rows = conn.execute(sql, (staff_legislator_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_votes_for_legislator_by_name(self, first_name: str, last_name: str) -> list[dict]:
        """
        Fallback vote lookup by name — works even when people_mapping hasn't been run.
        Matches on last_name (required) and optionally first_name from the people table.
        """
        conn = self._get_conn()
        if not last_name:
            return []
        params: list = [f"%{last_name.lower()}%"]
        name_clause = "LOWER(p.last_name) LIKE ?"
        if first_name:
            name_clause += " AND LOWER(p.first_name) LIKE ?"
            params.append(f"%{first_name.lower()}%")
        sql = f"""
            SELECT lv.vote_text, rc.date as vote_date, rc.desc as motion, rc.passed,
                   b.bill_number, b.title, b.jurisdiction, b.bill_id,
                   p.name as legislator_name
            FROM legislator_votes lv
            JOIN people p ON lv.people_id = p.people_id
            JOIN roll_calls rc ON lv.roll_call_id = rc.roll_call_id
            JOIN bills b ON rc.bill_id = b.bill_id
            WHERE {name_clause}
            ORDER BY rc.date DESC
            LIMIT 200
        """
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

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

        
    def sync_people_mapping(self, staff_df: pd.DataFrame) -> dict:
        """
        Attempt to map raw LegiScan people_id records to internal staff_manager legislator_ids.
        Uses name normalizations similar to the staff_manager resolver.
        """
        if staff_df.empty:
            return {"matched": 0, "unmatched": 0, "total": 0}
            
        conn = self._get_conn()
        people_rows = conn.execute("SELECT * FROM people").fetchall()
        
        # Build quick lookup from staff_df
        # Create normalized last name to staff id
        import string
        def _norm(n):
            if not n: return ""
            return n.lower().translate(str.maketrans('', '', string.punctuation)).replace(" ", "")

        staff_lookup = {}
        for _, row in staff_df.iterrows():
            lname = _norm(row.get("last_name", ""))
            fname = _norm(row.get("first_name", ""))
            staff_id = str(row.get("legislator_id", ""))
            chamber = str(row.get("chamber", "")).lower()
            if lname:
                staff_lookup[(lname, chamber)] = staff_id
                staff_lookup[(lname, fname)] = staff_id
        
        matched, unmatched = 0, 0
        
        # Also build a fallback full-name substring lookup
        staff_fullname_lookup = {}
        for _, row in staff_df.iterrows():
            lname = _norm(row.get("last_name", ""))
            fname = _norm(row.get("first_name", ""))
            staff_id = str(row.get("legislator_id", ""))
            full_norm = _norm(f"{fname}{lname}")
            if full_norm:
                staff_fullname_lookup[full_norm] = staff_id
        
        for p in people_rows:
            pid = p["people_id"]
            p_name_raw = p["name"] or ""
            
            if "Unknown Profile" in p_name_raw:
                unmatched += 1
                continue
                
            p_lname = _norm(p["last_name"])
            p_fname = _norm(p["first_name"])
            p_chamber = "upper" if "senate" in str(p["chamber"]).lower() else "lower"
            p_full_norm = _norm(p_name_raw)

            if not p_lname:
                parts = p_name_raw.split()
                p_lname = _norm(parts[-1] if parts else "")
                p_fname = _norm(parts[0] if len(parts) > 1 else "")
            
            # Primary exact match checks
            match_id = staff_lookup.get((p_lname, p_chamber)) or staff_lookup.get((p_lname, p_fname))
            
            # Secondary fallback substring matching
            if not match_id and p_full_norm:
                for target_norm, s_id in staff_fullname_lookup.items():
                    if p_full_norm in target_norm or target_norm in p_full_norm:
                        match_id = s_id
                        break
            
            if match_id:
                matched += 1
                conn.execute(
                    "INSERT INTO people_mapping (people_id, staff_legislator_id, match_quality) "
                    "VALUES (?, ?, 'Auto-Name-Chamber') "
                    "ON CONFLICT(people_id) DO UPDATE SET staff_legislator_id=excluded.staff_legislator_id",
                    (pid, match_id)
                )
            else:
                unmatched += 1
                conn.execute(
                    "INSERT INTO people_mapping (people_id, staff_legislator_id, match_quality) "
                    "VALUES (?, NULL, 'Unmatched') "
                    "ON CONFLICT(people_id) DO UPDATE SET match_quality=excluded.match_quality",
                    (pid,)
                )
                
        conn.commit()
        return {"matched": matched, "unmatched": unmatched, "total": len(people_rows)}

    def get_people_mapping_stats(self) -> dict:
        conn = self._get_conn()
        total = conn.execute("SELECT COUNT(*) FROM people").fetchone()[0]
        matched = conn.execute("SELECT COUNT(*) FROM people_mapping WHERE staff_legislator_id IS NOT NULL").fetchone()[0]
        unmatched = conn.execute("SELECT COUNT(*) FROM people_mapping WHERE staff_legislator_id IS NULL").fetchone()[0]
        return {"total": total, "matched": matched, "unmatched": unmatched}

    def get_votes_for_legislator_by_name(self, first_name: str, last_name: str) -> list[dict]:
        """
        Fallback name-based lookup for voting history. 
        Returns votes for anyone whose extracted name starts-with or matches the target.
        """
        conn = self._get_conn()
        sql = """
            SELECT 
                rc.roll_call_id, rc.date as vote_date, rc.desc as motion,
                lv.vote_text, b.bill_number
            FROM people p
            JOIN legislator_votes lv ON p.people_id = lv.people_id
            JOIN roll_calls rc ON lv.roll_call_id = rc.roll_call_id
            JOIN bills b ON rc.bill_id = b.bill_id
            WHERE p.last_name = ?
            ORDER BY rc.date DESC
        """
        rows = conn.execute(sql, (last_name,)).fetchall()
        # Basic filter for first name if multiple last names exist
        results = [dict(r) for r in rows]
        return results

    # ── Bill Text & Detailed Bill Fetching ───────────────────────────────────

    def get_bill(self, bill_id: int) -> Optional[dict]:
        """Fetch detailed bill JSON from LegiScan API by bill_id."""
        logger.info(f"API: get_bill(bill_id={bill_id})")
        res = self._api_get({"op": "getBill", "id": bill_id})
        if res.get("status") == "OK" and "bill" in res:
            return res["bill"]
        return None

    def get_bill_text(self, bill_id: int, doc_id: Optional[int] = None) -> Optional[dict]:
        """
        Get the full text of a bill. Uses local cache in bill_texts table if present.
        Returns a dict: { 'html': str, 'pdf_url': str, 'mime': str }
        """
        conn = self._get_conn()
        
        # 1. Try cache
        if doc_id:
            cached = conn.execute(
                "SELECT content_html, content_pdf_url, mime_type FROM bill_texts WHERE doc_id=?", 
                (doc_id,)
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
            "SELECT content_html, content_pdf_url, mime_type FROM bill_texts WHERE doc_id=?", 
            (doc_id,)
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
