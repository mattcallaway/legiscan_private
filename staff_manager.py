import sqlite3
import pandas as pd
import os
import uuid
import datetime
import logging
import re
import requests
import json
from typing import List, Dict, Any, Tuple

logger = logging.getLogger(__name__)

STAFF_SCHEMA = """
CREATE TABLE IF NOT EXISTS legislators (
    legislator_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    chamber TEXT,
    state TEXT,
    district TEXT,
    party TEXT,
    normalized_name TEXT
);

CREATE TABLE IF NOT EXISTS legislator_staff (
    staff_id TEXT PRIMARY KEY,
    legislator_id TEXT NOT NULL,
    name TEXT NOT NULL,
    role TEXT NOT NULL,
    email TEXT,
    office_type TEXT,
    source_tab TEXT,
    FOREIGN KEY(legislator_id) REFERENCES legislators(legislator_id)
);

CREATE TABLE IF NOT EXISTS legislator_issue_assignments (
    id TEXT PRIMARY KEY,
    legislator_id TEXT NOT NULL,
    issue_area TEXT NOT NULL,
    staff_name TEXT NOT NULL,
    notes TEXT,
    FOREIGN KEY(legislator_id) REFERENCES legislators(legislator_id)
);

CREATE TABLE IF NOT EXISTS committee_staff (
    id TEXT PRIMARY KEY,
    committee_name TEXT NOT NULL,
    chamber TEXT,
    role TEXT NOT NULL,
    staff_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staff_import_jobs (
    job_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    rows_processed INTEGER DEFAULT 0,
    rows_skipped INTEGER DEFAULT 0,
    unmatched_records INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS unmatched_staff_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_row_data TEXT,
    reason_unmatched TEXT,
    timestamp TEXT
);
"""

def normalize_leg_name(name: str) -> str:
    """Normalize legislator names for robust tracking."""
    if pd.isna(name) or not name:
        return ""
    name_str = str(name).strip()
    
    # Aggressively strip titles
    titles_to_strip = [
        "Sen.", "Senator", "Asm.", "Assemblymember", "Assembly Member", 
        "Assemblywoman", "Assemblyman", "Dr.", "Hon.", "Representative", "Rep."
    ]
    # Use regex to strip boundaries
    pattern = r"^(?:" + "|".join([re.escape(t) for t in titles_to_strip]) + r")\b\s*"
    name_str = re.sub(pattern, "", name_str, flags=re.IGNORECASE).strip()
    
    # Remove everything in parentheses (like "D-LA")
    name_str = re.sub(r'\(.*?\)', '', name_str).strip()
    
    # Remove trailing punctuation
    name_str = re.sub(r'[,.]+$', '', name_str).strip()
    
    return name_str.lower()

def exact_or_partial_match(query_name: str, leg_cache: dict) -> str:
    """
    Given a query like "Villapudua" and a cache {"carlos villapudua": leg_id},
    find the best match. 
    leg_cache dict structure: leg_cache[normalized_name] = leg_id
    Returns matching leg_id or empty string.
    """
    query = normalize_leg_name(query_name)
    if not query:
        return ""
        
    # 1. Exact match
    if query in leg_cache:
        return leg_cache[query]
        
    # 2. Last name / partial contains match
    # Example: query "Aghazarian" in "Greg Aghazarian"
    for norm_name, l_id in leg_cache.items():
        if query in norm_name.split() or norm_name in query.split():
            return l_id
            
    # 3. Fuzzy partial substring (risky but catches "Villapudua, Carlos")
    for norm_name, l_id in leg_cache.items():
        if query in norm_name or norm_name in query:
            return l_id
            
    return ""

def safe_split_names(names_str: str) -> List[str]:
    """Split staff names on `and`, `/`, `+`."""
    if pd.isna(names_str) or not str(names_str).strip():
        return []
    s = str(names_str).strip()
    if s.lower() in ["by issue area", "chair", "vacant", "n/a", "-"]:
        return []
    
    # regex split on common delimiters: slash, plus, ampersand, or literal 'and'
    parts = re.split(r'\s*/\s*|\s*\+\s*|\s*&\s*|\s+and\s+', s, flags=re.IGNORECASE)
    clean_parts = []
    for p in parts:
        cp = p.strip()
        # Remove trailing notes in parens "John Doe (temp)"
        cp = re.sub(r'\s*\(.*?\)$', '', cp)
        if cp and cp.lower() not in ["by issue area", "chair", "vacant", "n/a", ""]:
            clean_parts.append(cp)
    return clean_parts

class StaffManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_conn(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        try:
            with self._get_conn() as conn:
                conn.executescript(STAFF_SCHEMA)
        except Exception as e:
            logger.error(f"Failed to initialize staff db: {e}")

    def ingest_spreadsheet(self, filepath: str, state="CA"):
        """Ingests an Excel workbook."""
        job_id = str(uuid.uuid4())
        now = datetime.datetime.utcnow().isoformat()
        
        stats = {
            "processed": 0, "skipped": 0, "unmatched": 0, "errors": 0
        }
        
        try:
            xl = pd.ExcelFile(filepath)
            sheet_names = xl.sheet_names

            with self._get_conn() as conn:
                # Clear raw data (full replacement strategy for active sessions)
                conn.execute("DELETE FROM legislators WHERE state = ?", (state,))
                conn.execute("DELETE FROM legislator_staff")
                conn.execute("DELETE FROM legislator_issue_assignments")
                conn.execute("DELETE FROM committee_staff")
                
                # A. Legislator Office Staff
                for tab, chamber in [("Assembly", "Assembly"), ("Senate", "Senate")]:
                    if tab in sheet_names:
                        df = xl.parse(tab)
                        self._process_office_staff(df, chamber, state, tab, conn, stats)
                
                # B. Issue Assignments
                for tab, chamber in [("Asm Issues", "Assembly"), ("Sen Issues", "Senate")]:
                    if tab in sheet_names:
                        df = xl.parse(tab)
                        self._process_issues(df, chamber, tab, conn, stats)

                # C. Committee Staff
                for tab, chamber in [("Asm Cmte Staff", "Assembly"), ("Sen Cmte Staff", "Senate")]:
                    if tab in sheet_names:
                        df = xl.parse(tab)
                        self._process_committees(df, chamber, tab, conn, stats)

                # Log Job
                conn.execute("""
                    INSERT INTO staff_import_jobs (job_id, timestamp, rows_processed, rows_skipped, unmatched_records, errors)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (job_id, now, stats["processed"], stats["skipped"], stats["unmatched"], stats["errors"]))
                conn.commit()
                
            return True, stats
        except Exception as e:
            logger.error(f"Staff Ingestion failed: {e}", exc_info=True)
            with self._get_conn() as conn:
                conn.execute("INSERT INTO staff_import_jobs (job_id, timestamp, errors) VALUES (?, ?, 1)", (job_id, now))
            return False, str(e)

    def _process_office_staff(self, df: pd.DataFrame, chamber: str, state: str, tab: str, conn: sqlite3.Connection, stats: dict):
        if 'Member' not in df.columns:
            logger.warning(f"Tab '{tab}' missing 'Member' col.")
            return

        roles_mapping = [
            ("Chief of Staff", "chief_of_staff", "COS Email"),
            ("Legislative Director", "legislative_director", "LD Email"),
            ("Scheduler", "scheduler", "Scheduler Email"),
            ("Comms Director", "communications", "Comms Director Email"),
            ("District Director", "district_director", "District Director Email")
        ]
        
        for _, row in df.iterrows():
            stats["processed"] += 1
            member = row.get("Member", "")
            if pd.isna(member) or not str(member).strip():
                stats["skipped"] += 1
                continue
                
            leg_id = str(uuid.uuid4())
            name_val = str(member).strip()
            norm_name = normalize_leg_name(name_val)
            dist_val = str(row.get("District", "")).strip()
            party_val = str(row.get("Party", "")).strip()
            
            conn.execute("""
                INSERT INTO legislators (legislator_id, name, chamber, state, district, party, normalized_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (leg_id, name_val, chamber, state, dist_val, party_val, norm_name))
            
            for src_col, target_role, email_col in roles_mapping:
                if src_col in row and not pd.isna(row[src_col]):
                    for staff_name in safe_split_names(row[src_col]):
                        staff_id = str(uuid.uuid4())
                        email = row.get(email_col, "")
                        email = str(email).strip() if not pd.isna(email) else ""
                        
                        conn.execute("""
                            INSERT INTO legislator_staff (staff_id, legislator_id, name, role, email, office_type, source_tab)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (staff_id, leg_id, staff_name, target_role, email, "capitol", tab))

    def _process_issues(self, df: pd.DataFrame, chamber: str, tab: str, conn: sqlite3.Connection, stats: dict):
        if 'Member' not in df.columns:
            return
            
        leg_cache = {}
        for r in conn.execute("SELECT legislator_id, normalized_name, chamber FROM legislators").fetchall():
            if r[2] == chamber:
                leg_cache[r[1]] = r[0] # norm_name -> id
                
        issue_cols = [c for c in df.columns if c not in ["Member", "District", "Party"] and not str(c).startswith("Unnamed")]
        
        for _, row in df.iterrows():
            stats["processed"] += 1
            member = row.get("Member", "")
            if pd.isna(member) or not str(member).strip():
                continue
                
            norm_name = normalize_leg_name(member)
            leg_id = exact_or_partial_match(member, leg_cache)
            if not leg_id:
                stats["unmatched"] += 1
                conn.execute("INSERT INTO unmatched_staff_rows (raw_row_data, reason_unmatched) VALUES (?, ?)", 
                             (str(row.to_dict()), f"No legislator found for {norm_name}"))
                continue
                
            for issue_area in issue_cols:
                val = row.get(issue_area)
                if pd.isna(val) or not str(val).strip():
                    continue
                    
                for staff_name in safe_split_names(val):
                    uid = str(uuid.uuid4())
                    conn.execute("""
                        INSERT INTO legislator_issue_assignments (id, legislator_id, issue_area, staff_name)
                        VALUES (?, ?, ?, ?)
                    """, (uid, leg_id, str(issue_area).strip(), staff_name))

    def _process_committees(self, df: pd.DataFrame, chamber: str, tab: str, conn: sqlite3.Connection, stats: dict):
        if 'Committee' not in df.columns:
            return
            
        role_cols = [
            ("Chief Consultant", "chief_consultant"),
            ("Consultant", "consultant"),
            ("Consultants", "consultant"),
            ("Republican Consultant", "republican_consultant"),
            ("Vice Chair", "vice_chair"),
            ("Chair", "chair")
        ]
        
        active_role_cols = [(col, role) for col, role in role_cols if col in df.columns]
        
        for _, row in df.iterrows():
            stats["processed"] += 1
            cmte = row.get("Committee", "")
            if pd.isna(cmte) or not str(cmte).strip():
                continue
                
            cmte_name = str(cmte).strip()
            
            for col, role_id in active_role_cols:
                val = row.get(col)
                if not pd.isna(val):
                    for staff_name in safe_split_names(val):
                        uid = str(uuid.uuid4())
                        conn.execute("""
                            INSERT INTO committee_staff (id, committee_name, chamber, role, staff_name)
                            VALUES (?, ?, ?, ?, ?)
                        """, (uid, cmte_name, chamber, role_id, staff_name))

    def sync_live_sheet(self, url: str, temp_dir: str, state="CA"):
        """Downloads a public Google Sheet as an XLSX buffer and ingests it."""
        try:
            # Transform typical /edit Google URL to /export
            if "/edit" in url:
                export_url = url.split("/edit")[0] + "/export?format=xlsx"
            else:
                export_url = url
                
            resp = requests.get(export_url, timeout=15)
            resp.raise_for_status()
            
            tmp_path = os.path.join(temp_dir, f"temp_staff_{uuid.uuid4().hex[:8]}.xlsx")
            with open(tmp_path, "wb") as f:
                f.write(resp.content)
                
            success, stats_or_err = self.ingest_spreadsheet(tmp_path, state)
            
            # Cleanup temp
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
                
            return success, stats_or_err
        except Exception as e:
            logger.error(f"Live sync failed: {e}")
            return False, str(e)
            
    def get_last_import_job(self) -> dict:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM staff_import_jobs ORDER BY timestamp DESC LIMIT 1").fetchone()
            return dict(row) if row else None
            
    def get_job_history(self, limit: int = 10) -> list:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM staff_import_jobs ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
            
    def get_unmatched_rows(self, limit: int = 100) -> list:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM unmatched_staff_rows ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    # --- Query API ---
    
    def get_all_legislators(self) -> pd.DataFrame:
        with self._get_conn() as conn:
            return pd.read_sql("SELECT * FROM legislators ORDER BY chamber, name", conn)
            
    def get_legislator_staff(self, leg_id: str) -> list:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM legislator_staff WHERE legislator_id = ?", (leg_id,)).fetchall()
            return [dict(r) for r in rows]
            
    def get_legislator_issues(self, leg_id: str) -> list:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT issue_area, staff_name FROM legislator_issue_assignments WHERE legislator_id = ? ORDER BY issue_area", (leg_id,)).fetchall()
            return [dict(r) for r in rows]

    def get_committee_staff(self, cmte_name: str) -> list:
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM committee_staff WHERE committee_name LIKE ?", (f"%{cmte_name}%",)).fetchall()
            return [dict(r) for r in rows]
            
    def get_legislator_committee_matrix(self, leg_norm_name: str) -> list:
        """Finds committees where this legislator is Chair or Vice Chair, and returns the consultant staff for those committees."""
        if not leg_norm_name:
            return []
        with self._get_conn() as conn:
            conn.row_factory = sqlite3.Row
            # Step 1: Find committees this legislator leads
            cmtes = conn.execute(
                "SELECT DISTINCT committee_name FROM committee_staff WHERE role IN ('chair', 'vice_chair') AND LOWER(staff_name) LIKE ?", 
                (f"%{leg_norm_name}%",)
            ).fetchall()
            
            res = []
            for c in cmtes:
                c_name = c['committee_name']
                staff = conn.execute("SELECT role, staff_name FROM committee_staff WHERE committee_name = ? AND role NOT IN ('chair', 'vice_chair')", (c_name,)).fetchall()
                if staff:
                    for s in staff:
                        res.append({"committee": c_name, "role": s['role'].replace("_", " ").title(), "staff_name": s['staff_name']})
                else:
                    res.append({"committee": c_name, "role": "No Consultants Mapped", "staff_name": "—"})
            return res
