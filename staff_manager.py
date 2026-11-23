import sqlite3
from contextlib import closing
import pandas as pd
import os
import uuid
import datetime
import logging
import re
import requests
import json
from hashlib import sha256
from collections import Counter
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
    normalized_name TEXT,
    normalized_full_name TEXT,
    normalized_display_name TEXT,
    normalized_last_name TEXT,
    district_code TEXT,
    district_number TEXT,
    canonical_legislator_key TEXT
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
    state TEXT NOT NULL DEFAULT 'CA',
    role TEXT NOT NULL,
    staff_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS legislator_roster_identity (
    state TEXT NOT NULL, chamber TEXT NOT NULL, full_name TEXT NOT NULL, legislator_id TEXT NOT NULL,
    PRIMARY KEY(state, chamber, full_name)
);

CREATE TABLE IF NOT EXISTS office_staff_identity (
    legislator_id TEXT NOT NULL, full_name TEXT NOT NULL, role TEXT NOT NULL, staff_id TEXT NOT NULL,
    PRIMARY KEY(legislator_id, full_name, role)
);

CREATE TABLE IF NOT EXISTS staff_roster_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    roster_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staff_assignment_history (
    history_id INTEGER PRIMARY KEY,
    category TEXT NOT NULL,
    assignment_json TEXT NOT NULL,
    observed_from TEXT NOT NULL,
    observed_until TEXT
);

CREATE TABLE IF NOT EXISTS member_service_terms (
    profile_key TEXT NOT NULL,
    record_key TEXT NOT NULL,
    jurisdiction TEXT NOT NULL,
    chamber TEXT NOT NULL,
    precision TEXT NOT NULL,
    start TEXT NOT NULL,
    end TEXT NOT NULL,
    source_url TEXT NOT NULL,
    congress INTEGER,
    bioguide TEXT,
    PRIMARY KEY (profile_key, record_key)
);

CREATE TABLE IF NOT EXISTS staff_import_jobs (
    job_id TEXT PRIMARY KEY,
    timestamp TEXT NOT NULL,
    rows_processed INTEGER DEFAULT 0,
    rows_skipped INTEGER DEFAULT 0,
    unmatched_records INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    legislators_matched INTEGER DEFAULT 0,
    staff_created INTEGER DEFAULT 0,
    issues_created INTEGER DEFAULT 0,
    committees_created INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS unmatched_staff_rows (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_row_data TEXT,
    reason_unmatched TEXT,
    timestamp TEXT
);
"""

def normalize_district(dist: str, chamber: str) -> tuple:
    if pd.isna(dist) or not dist:
        return "", ""
    s = str(dist).strip().upper()
    num = "".join(filter(str.isdigit, s))
    if not num: 
        return "", ""
    c_prefix = "SD" if chamber.lower() == "senate" else "AD"
    return f"{c_prefix}{num.zfill(2)}", str(int(num))

def normalize_name_components(raw_name: str) -> dict:
    if pd.isna(raw_name) or not raw_name:
        return {"full": "", "display": "", "last": ""}
    
    name_str = str(raw_name).strip()
    titles_to_strip = [
        "State Senator", "State Sen.", "Sen.", "Senator", "Asm.", "Assemblymember", "Assembly Member", 
        "Assemblywoman", "Assemblyman", "Dr.", "Hon.", "Representative", "Rep.", "Member"
    ]
    pattern = r"^(?:" + "|".join([re.escape(t) for t in titles_to_strip]) + r")\b\s*"
    name_str = re.sub(pattern, "", name_str, flags=re.IGNORECASE).strip()
    name_str = re.sub(r'\(.*?\)', '', name_str).strip()
    name_str = re.sub(r'[,.]+$', '', name_str).strip()
    name_str = re.sub(r'\s+', ' ', name_str)
    
    parts = name_str.split(" ")
    last_name = parts[-1] if len(parts) > 0 else name_str
    
    return {
        "full": name_str.lower(),
        "display": name_str,
        "last": last_name.lower()
    }

def canonical_key(state: str, chamber: str, dist_code: str, last_name: str) -> str:
    return f"{state}|{chamber}|{dist_code}|{last_name}".lower()

def resolve_legislator(df: pd.DataFrame, norm_name: str, last_name: str, chamber: str, dist_code: str, state="CA") -> tuple:
    # Tier 1: Exact Canonical Match
    c_key = canonical_key(state, chamber, dist_code, last_name)
    m1 = df[df['canonical_legislator_key'] == c_key]
    if len(m1) == 1: return m1.iloc[0]['legislator_id'], "Tier 1: Canonical Key"
    
    # Tier 2: Chamber + District Unique Match
    if dist_code and chamber:
        m2 = df[(df['chamber'] == chamber) & (df['district_code'] == dist_code)]
        if len(m2) == 1: return m2.iloc[0]['legislator_id'], "Tier 2: Chamber + District"
        
    # Tier 3: Full Normalized Name Exact
    if norm_name:
        m3 = df[df['normalized_full_name'] == norm_name]
        if len(m3) == 1: return m3.iloc[0]['legislator_id'], "Tier 3: Exact Full Name"
        
    # Tier 4: Unique Last Name within Chamber
    if last_name and chamber:
        m4 = df[(df['chamber'] == chamber) & (df['normalized_last_name'] == last_name)]
        if len(m4) == 1: return m4.iloc[0]['legislator_id'], "Tier 4: Unique Last Name in Chamber"
        
    # Tier 4b: Unique Last Name Global
    if last_name:
        m4b = df[df['normalized_last_name'] == last_name]
        if len(m4b) == 1: return m4b.iloc[0]['legislator_id'], "Tier 4b: Unique Last Name Global"
        
    # Ambiguous
    return None, f"Ambiguous/Unmatched (Last Name Hits: {len(df[df['normalized_last_name'] == last_name]) if last_name else 0})"

def normalize_leg_name(name: str) -> str:
    return normalize_name_components(name)['full']

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
            with closing(self._get_conn()) as conn, conn:
                conn.executescript(STAFF_SCHEMA)
                committee_columns = {row[1] for row in conn.execute("PRAGMA table_info(committee_staff)")}
                if "state" not in committee_columns:
                    conn.execute("ALTER TABLE committee_staff ADD COLUMN state TEXT NOT NULL DEFAULT 'CA'")

                # Ensure backwards schema compatibility for new fields
                for c in ["legislators_matched", "staff_created", "issues_created", "committees_created"]:
                    try:
                        conn.execute(f"ALTER TABLE staff_import_jobs ADD COLUMN {c} INTEGER DEFAULT 0")
                    except sqlite3.OperationalError:
                        pass
                
                # Canonical Migration
                for c in ["normalized_full_name", "normalized_display_name", "normalized_last_name", "district_code", "district_number", "canonical_legislator_key"]:
                    try:
                        conn.execute(f"ALTER TABLE legislators ADD COLUMN {c} TEXT")
                    except sqlite3.OperationalError:
                        pass
        except Exception as e:
            logger.error(f"Failed to initialize staff db: {e}")

    _ROSTER_TABLES = ("legislators", "legislator_staff", "legislator_issue_assignments", "committee_staff")

    def _roster_rows(self, connection):
        connection.row_factory = sqlite3.Row
        return {
            table: [dict(row) for row in connection.execute(f"SELECT * FROM {table}")]
            for table in self._ROSTER_TABLES
        }

    def _roster_revision(self, roster):
        ordered = {table: sorted(json.dumps(row, sort_keys=True) for row in rows)
                   for table, rows in roster.items()}
        return sha256(json.dumps(ordered, sort_keys=True).encode()).hexdigest()

    def _roster_diff(self, before, after):
        changes = []
        for table in self._ROSTER_TABLES:
            if table in {"legislators", "legislator_staff"}:
                key = "legislator_id" if table == "legislators" else "staff_id"
                old = {row[key]: row for row in before[table]}
                new = {row[key]: row for row in after[table]}
                added, removed = len(new.keys() - old.keys()), len(old.keys() - new.keys())
                updated = sum(old[identity] != new[identity] for identity in old.keys() & new.keys())
            else:
                def records(rows):
                    return Counter(json.dumps({key: value for key, value in row.items() if key != "id"}, sort_keys=True)
                                   for row in rows)
                old, new = records(before[table]), records(after[table])
                added, removed, updated = sum((new - old).values()), sum((old - new).values()), 0
            changes.append({
                "category": table, "before": len(before[table]), "after": len(after[table]),
                "added": added, "updated": updated, "removed": removed,
            })
        return changes

    def _save_roster_snapshot(self, connection, roster, reason, timestamp):
        snapshot_id = uuid.uuid4().hex
        connection.execute(
            "INSERT INTO staff_roster_snapshots VALUES (?, ?, ?, ?)",
            (snapshot_id, timestamp, reason, json.dumps(roster)),
        )
        return snapshot_id

    def _record_observed_assignments(self, connection, before, after, timestamp):
        def assignments(roster):
            records = set()
            for table, rows in roster.items():
                for row in rows:
                    content = dict(row)
                    content.pop("id", None)
                    if table == "legislators":
                        content = {key: row.get(key) for key in
                                   ("legislator_id", "name", "state", "chamber", "district", "party")}
                    records.add((table, json.dumps(content, sort_keys=True)))
            return records

        open_records = {
            (row["category"], row["assignment_json"]): row["history_id"]
            for row in connection.execute(
                "SELECT history_id, category, assignment_json FROM staff_assignment_history WHERE observed_until IS NULL"
            )
        }
        # On the first recorded import, the existing roster is known as of this observation only.
        if not open_records:
            connection.executemany(
                "INSERT INTO staff_assignment_history (category, assignment_json, observed_from) VALUES (?, ?, ?)",
                [(category, content, timestamp) for category, content in assignments(before)],
            )
            open_records = {
                (row["category"], row["assignment_json"]): row["history_id"]
                for row in connection.execute(
                    "SELECT history_id, category, assignment_json FROM staff_assignment_history WHERE observed_until IS NULL"
                )
            }
        current = assignments(after)
        connection.executemany(
            "UPDATE staff_assignment_history SET observed_until=? WHERE history_id=?",
            [(timestamp, open_records[record]) for record in open_records.keys() - current],
        )
        connection.executemany(
            "INSERT INTO staff_assignment_history (category, assignment_json, observed_from) VALUES (?, ?, ?)",
            [(category, content, timestamp) for category, content in current - open_records.keys()],
        )

    def get_roster_snapshots(self):
        with closing(self._get_conn()) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(
                "SELECT snapshot_id, created_at, reason FROM staff_roster_snapshots ORDER BY created_at DESC"
            )]

    def preview_roster_restore(self, snapshot_id):
        with closing(self._get_conn()) as connection, connection:
            connection.execute("BEGIN")
            before = self._roster_rows(connection)
            saved = connection.execute(
                "SELECT roster_json FROM staff_roster_snapshots WHERE snapshot_id=?", (snapshot_id,),
            ).fetchone()
            if saved is None:
                raise ValueError("Roster snapshot was not found.")
            return self._roster_revision(before), self._roster_diff(before, json.loads(saved[0]))

    def restore_roster_snapshot(self, snapshot_id, expected_revision):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with closing(self._get_conn()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            before = self._roster_rows(connection)
            if self._roster_revision(before) != expected_revision:
                raise ValueError("The staff directory changed after preview. Preview this restore again.")
            saved = connection.execute(
                "SELECT roster_json FROM staff_roster_snapshots WHERE snapshot_id=?", (snapshot_id,),
            ).fetchone()
            if saved is None:
                raise ValueError("Roster snapshot was not found.")
            after = json.loads(saved[0])
            self._save_roster_snapshot(connection, before, "Before roster restore", now)
            for table in reversed(self._ROSTER_TABLES):
                connection.execute(f"DELETE FROM {table}")
            for table in self._ROSTER_TABLES:
                columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
                rows = after[table]
                if rows:
                    fields = [column for column in columns if column in rows[0]]
                    placeholders = ", ".join("?" for field in fields)
                    connection.executemany(
                        f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({placeholders})",
                        [tuple(row[field] for field in fields) for row in rows],
                    )
            self._record_observed_assignments(connection, before, self._roster_rows(connection), now)

    def get_service_terms(self, profile_key):
        with closing(self._get_conn()) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(
                """SELECT * FROM member_service_terms WHERE profile_key=?
                   ORDER BY start DESC, precision, chamber, record_key""", (profile_key,),
            )]

    @staticmethod
    def _save_service_record(connection, profile_key, record_key, term, bioguide=None):
        columns = ("jurisdiction", "chamber", "precision", "start", "end", "source_url", "congress")
        connection.execute(
            """INSERT INTO member_service_terms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(profile_key, record_key) DO UPDATE SET
               end=excluded.end, source_url=excluded.source_url""",
            (profile_key, record_key, *(term[field] for field in columns), bioguide),
        )

    def save_exact_service_term(self, profile_key, jurisdiction, chamber, start, end, source_url):
        from service_terms import validate_exact_term
        term = validate_exact_term(jurisdiction, chamber, start, end, source_url.strip())
        with closing(self._get_conn()) as connection, connection:
            self._save_service_record(connection, profile_key, f'exact:{jurisdiction}:{term["chamber"]}:{term["start"]}', term)

    def save_congress_service_terms(self, profile_key, member):
        if not profile_key.startswith("federal:") or not member["terms"]:
            raise ValueError("Select a federal voter profile and preview its official terms first.")
        identifier = member["bioguide"]
        with closing(self._get_conn()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            other_profile = connection.execute(
                "SELECT 1 FROM member_service_terms WHERE bioguide=? AND profile_key<>?", (identifier, profile_key),
            ).fetchone()
            different_id = connection.execute(
                "SELECT 1 FROM member_service_terms WHERE profile_key=? AND bioguide<>?", (profile_key, identifier),
            ).fetchone()
            if other_profile or different_id:
                raise ValueError("The Bioguide ID or voter profile is already linked. Verify the existing records before relinking.")
            connection.execute("DELETE FROM member_service_terms WHERE profile_key=? AND bioguide=?", (profile_key, identifier))
            for term in member["terms"]:
                self._save_service_record(connection, profile_key, f'congress:{term["congress"]}:{term["chamber"]}', term, identifier)

    def delete_service_term(self, profile_key, record_key):
        with closing(self._get_conn()) as connection, connection:
            connection.execute(
                "DELETE FROM member_service_terms WHERE profile_key=? AND record_key=?", (profile_key, record_key),
            )

    def get_member_assignment_history(self, legislator_id):
        with closing(self._get_conn()) as connection:
            rows = connection.execute(
                """SELECT category, assignment_json, observed_from, observed_until
                   FROM staff_assignment_history
                   WHERE json_extract(assignment_json, '$.legislator_id')=?
                   ORDER BY observed_from DESC, category""", (legislator_id,),
            )
            history = []
            for category, content, start, end in rows:
                assignment = json.loads(content)
                detail = " · ".join(str(assignment[field]) for field in
                                    ("district", "party", "role", "name", "issue_area", "staff_name")
                                    if assignment.get(field))
                history.append({"Assignment": category.replace("_", " ").title(), "Details": detail,
                                "First observed": start, "Last observed active until": end})
            return history

    def ingest_spreadsheet(self, filepath: str, state="CA", *, preview=False, expected_revision=None):
        """Ingests an Excel workbook."""
        job_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        stats = {
            "processed": 0, "skipped": 0, "unmatched": 0, "errors": 0,
            "legislators_matched": 0, "staff_created": 0, "issues_created": 0, "committees_created": 0,
            "warnings": []
        }
        
        try:
            office_tabs = {"Assembly": "Assembly", "Senate": "Senate"}
            issue_tabs = {"Asm Issues": "Assembly", "Sen Issues": "Senate"}
            committee_tabs = {"Asm Cmte Staff": "Assembly", "Sen Cmte Staff": "Senate"}
            with pd.ExcelFile(filepath) as workbook:
                sheets = {
                    tab: pd.read_excel(workbook, sheet_name=tab)
                    for tab in [*office_tabs, *issue_tabs, *committee_tabs]
                    if tab in workbook.sheet_names
                }

            if not sheets:
                raise ValueError("The workbook does not contain any recognized staff directory tabs.")
            for tab, frame in sheets.items():
                required = "Committee" if tab in committee_tabs else "Member"
                if required not in frame or not frame[required].dropna().astype(str).str.strip().ne("").any():
                    raise ValueError(f"{tab} must contain a {required} column with at least one record.")
                if tab in office_tabs:
                    names = [normalize_name_components(value)["full"] for value in frame[required].dropna()]
                    names = [name for name in names if name]
                    if not names:
                        raise ValueError(f"{tab} does not contain a usable member name.")
                    if len(set(names)) != len(names):
                        raise ValueError(f"{tab} has multiple rows for the same member. Review the roster before importing.")
                if tab in committee_tabs and not any(
                    column in frame for column in (
                        "Chief Consultant", "Consultant", "Consultants", "Republican Consultant",
                        "Chair", "Vice Chair",
                    )
                ):
                    raise ValueError(f"{tab} is missing its committee staff columns.")

            with closing(self._get_conn()) as conn, conn:
                conn.execute("BEGIN IMMEDIATE")
                before = self._roster_rows(conn)
                revision = self._roster_revision(before)
                if expected_revision is not None and revision != expected_revision:
                    raise ValueError("The staff directory changed after preview. Preview the workbook again.")
                for tab, chamber in office_tabs.items():
                    if tab not in sheets:
                        continue
                    previous = conn.execute(
                        "SELECT legislator_id, name FROM legislators WHERE state=? AND chamber=?",
                        (state, chamber),
                    ).fetchall()
                    identities = {}
                    for member_id, member_name in previous:
                        identity = normalize_name_components(member_name)["full"]
                        if identity in identities:
                            raise ValueError(f"The existing {chamber} roster has duplicate member names.")
                        identities[identity] = member_id
                    conn.executemany(
                        "INSERT OR REPLACE INTO legislator_roster_identity VALUES (?, ?, ?, ?)",
                        [(state, chamber, identity, member_id) for identity, member_id in identities.items()],
                    )
                    identities = dict(conn.execute(
                        "SELECT full_name, legislator_id FROM legislator_roster_identity WHERE state=? AND chamber=?",
                        (state, chamber),
                    ))
                    seen = self._process_office_staff(sheets[tab], chamber, state, tab, conn, stats, identities)
                    removed = {(member_id,) for member_id, _ in previous if member_id not in seen}
                    for (member_id,) in removed:
                        conn.executemany(
                            "INSERT OR IGNORE INTO office_staff_identity VALUES (?, ?, ?, ?)",
                            [(member_id, normalize_name_components(name)["full"], role, staff_id)
                             for staff_id, name, role in conn.execute(
                                 "SELECT staff_id, name, role FROM legislator_staff WHERE legislator_id=?", (member_id,),
                             )],
                        )
                    for table in ("legislator_staff", "legislator_issue_assignments", "legislators"):
                        conn.executemany(f"DELETE FROM {table} WHERE legislator_id=?", removed)

                for tab, chamber in issue_tabs.items():
                    if tab not in sheets:
                        continue
                    conn.execute(
                        """DELETE FROM legislator_issue_assignments WHERE legislator_id IN
                           (SELECT legislator_id FROM legislators WHERE state=? AND chamber=?)""",
                        (state, chamber),
                    )
                    previous_count = stats["issues_created"]
                    self._process_issues(sheets[tab], chamber, tab, conn, stats, state)
                    if stats["issues_created"] == previous_count:
                        raise ValueError(f"{tab} did not produce any matched issue assignments.")

                for tab, chamber in committee_tabs.items():
                    if tab not in sheets:
                        continue
                    conn.execute("DELETE FROM committee_staff WHERE state=? AND chamber=?", (state, chamber))
                    previous_count = stats["committees_created"]
                    self._process_committees(sheets[tab], chamber, tab, conn, stats, state)
                    if stats["committees_created"] == previous_count:
                        raise ValueError(f"{tab} did not produce any committee staff records.")

                after = self._roster_rows(conn)
                stats["revision"] = revision
                stats["changes"] = self._roster_diff(before, after)
                if preview:
                    conn.rollback()
                    return True, stats
                if any(change["added"] or change["removed"] or change["updated"] for change in stats["changes"]):
                    stats["snapshot_id"] = self._save_roster_snapshot(conn, before, f"Before {state} roster import", now)
                    self._record_observed_assignments(conn, before, after, now)

                # Log Job
                conn.execute("""
                    INSERT INTO staff_import_jobs (job_id, timestamp, rows_processed, rows_skipped, unmatched_records, errors, legislators_matched, staff_created, issues_created, committees_created)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (job_id, now, stats["processed"], stats["skipped"], stats["unmatched"], stats["errors"], stats["legislators_matched"], stats["staff_created"], stats["issues_created"], stats["committees_created"]))
                conn.commit()
                
            return True, stats
        except Exception as e:
            logger.error(f"Staff Ingestion failed: {e}", exc_info=True)
            if not preview:
                with closing(self._get_conn()) as conn, conn:
                    conn.execute("INSERT INTO staff_import_jobs (job_id, timestamp, errors) VALUES (?, ?, 1)", (job_id, now))
            return False, str(e)

    def _process_office_staff(self, df, chamber, state, tab, conn, stats, identities):
        if 'Member' not in df.columns:
            logger.warning(f"Tab '{tab}' missing 'Member' col.")
            return

        seen = set()
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
            if not normalize_name_components(member)["full"]:
                stats["skipped"] += 1
                continue
                
            name_val = str(member).strip()
            norm_name = normalize_leg_name(name_val)
            dist_val = str(row.get("District", "")).strip()
            party_val = str(row.get("Party", "")).strip()
            
            n_comps = normalize_name_components(name_val)
            dist_code, dist_num = normalize_district(dist_val, chamber)
            c_key = canonical_key(state, chamber, dist_code, n_comps['last'])
            leg_id = identities.get(n_comps["full"]) or uuid.uuid4().hex
            seen.add(leg_id)
            conn.execute(
                "INSERT OR IGNORE INTO legislator_roster_identity VALUES (?, ?, ?, ?)",
                (state, chamber, n_comps["full"], leg_id),
            )
            existing_staff = {}
            for stored_id, stored_name, stored_role in conn.execute(
                "SELECT staff_id, name, role FROM legislator_staff WHERE legislator_id=? AND source_tab=?",
                (leg_id, tab),
            ):
                identity = (normalize_name_components(stored_name)["full"], stored_role)
                if identity in existing_staff:
                    raise ValueError(f"Multiple existing staff records for {stored_name} in the same role.")
                existing_staff[identity] = stored_id
            conn.executemany(
                "INSERT OR REPLACE INTO office_staff_identity VALUES (?, ?, ?, ?)",
                [(leg_id, name, role, staff_id) for (name, role), staff_id in existing_staff.items()],
            )
            existing_staff = {
                (name, role): staff_id for name, role, staff_id in conn.execute(
                    "SELECT full_name, role, staff_id FROM office_staff_identity WHERE legislator_id=?", (leg_id,),
                )
            }
            conn.executemany(
                "DELETE FROM legislator_staff WHERE legislator_id=? AND source_tab=? AND role=?",
                [(leg_id, tab, role) for column, role, _ in roles_mapping if column in df],
            )
            inserted_staff = set()

            
            conn.execute("""
                INSERT INTO legislators (legislator_id, name, chamber, state, district, party, normalized_name, normalized_full_name, normalized_display_name, normalized_last_name, district_code, district_number, canonical_legislator_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(legislator_id) DO UPDATE SET
                    name=excluded.name, chamber=excluded.chamber, state=excluded.state,
                    district=excluded.district, party=excluded.party,
                    normalized_name=excluded.normalized_name, normalized_full_name=excluded.normalized_full_name,
                    normalized_display_name=excluded.normalized_display_name, normalized_last_name=excluded.normalized_last_name,
                    district_code=excluded.district_code, district_number=excluded.district_number,
                    canonical_legislator_key=excluded.canonical_legislator_key
            """, (leg_id, name_val, chamber, state, dist_val, party_val, norm_name, n_comps['full'], n_comps['display'], n_comps['last'], dist_code, dist_num, c_key))
            stats["legislators_matched"] += 1
            
            for src_col, target_role, email_col in roles_mapping:
                if src_col in row and not pd.isna(row[src_col]):
                    for staff_name in safe_split_names(row[src_col]):
                        identity = (normalize_name_components(staff_name)["full"], target_role)
                        if identity in inserted_staff:
                            continue
                        inserted_staff.add(identity)
                        staff_id = existing_staff.get(identity) or uuid.uuid4().hex
                        conn.execute(
                            "INSERT OR IGNORE INTO office_staff_identity VALUES (?, ?, ?, ?)",
                            (leg_id, *identity, staff_id),
                        )
                        email = row.get(email_col, "")
                        email = str(email).strip() if not pd.isna(email) else ""
                        
                        conn.execute("""
                            INSERT INTO legislator_staff (staff_id, legislator_id, name, role, email, office_type, source_tab)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (staff_id, leg_id, staff_name, target_role, email, "capitol", tab))
                        stats["staff_created"] += 1

        return seen

    def _process_issues(self, df: pd.DataFrame, chamber: str, tab: str, conn: sqlite3.Connection, stats: dict, state="CA"):
        if 'Member' not in df.columns:
            return
            
        leg_df = pd.read_sql("SELECT * FROM legislators WHERE state=? AND chamber=?", conn, params=(state, chamber))
        issue_cols = [c for c in df.columns if c not in ["Member", "District", "Party"] and not str(c).startswith("Unnamed")]
        
        for _, row in df.iterrows():
            stats["processed"] += 1
            member = row.get("Member", "")
            dist_val = str(row.get("District", "")).strip()
            if pd.isna(member) or not str(member).strip():
                continue
                
            n_comps = normalize_name_components(member)
            d_code, _ = normalize_district(dist_val, chamber)
            
            leg_id, rsn = resolve_legislator(leg_df, n_comps['full'], n_comps['last'], chamber, d_code, state)
            
            if not leg_id:
                stats["unmatched"] += 1
                unmatched_payload = {
                    "raw_member": str(member),
                    "raw_district": dist_val,
                    "inferred_chamber": chamber,
                    "norm_last": n_comps['last'],
                    "norm_dist": d_code
                }
                conn.execute("INSERT INTO unmatched_staff_rows (raw_row_data, reason_unmatched) VALUES (?, ?)", 
                             (json.dumps(unmatched_payload), rsn))
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
                    stats["issues_created"] += 1

    def _process_committees(self, df: pd.DataFrame, chamber: str, tab: str, conn: sqlite3.Connection, stats: dict, state="CA"):
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
                            INSERT INTO committee_staff (id, committee_name, chamber, role, staff_name, state)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (uid, cmte_name, chamber, role_id, staff_name, state))
                        stats["committees_created"] += 1

    def download_live_workbook(self, url):
        export_url = url.split("/edit")[0] + "/export?format=xlsx" if "/edit" in url else url
        response = requests.get(export_url, timeout=15)
        response.raise_for_status()
        return response.content

    def sync_live_sheet(self, url: str, temp_dir: str, state="CA"):
        """Download the configured public roster and import it."""
        from tempfile import TemporaryDirectory
        from pathlib import Path

        try:
            with TemporaryDirectory(dir=temp_dir) as directory:
                path = Path(directory, "roster.xlsx")
                path.write_bytes(self.download_live_workbook(url))
                return self.ingest_spreadsheet(path, state)
        except (OSError, requests.RequestException) as error:
            logger.error("Live staff roster download failed: %s", type(error).__name__)
            return False, str(error)

    def get_last_import_job(self) -> dict:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM staff_import_jobs ORDER BY timestamp DESC LIMIT 1").fetchone()
            return dict(row) if row else None
            
    def get_job_history(self, limit: int = 10) -> list:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM staff_import_jobs ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]
            
    def get_unmatched_rows(self, limit: int = 100) -> list:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM unmatched_staff_rows ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    # --- Query API ---
    
    def get_all_legislators(self) -> pd.DataFrame:
        with closing(self._get_conn()) as conn, conn:
            return pd.read_sql("SELECT * FROM legislators ORDER BY chamber, name", conn)
            
    def get_office_staff_profiles(self):
        with closing(self._get_conn()) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(
                """SELECT s.staff_id, s.name, s.role, l.name AS office
                   FROM legislator_staff s JOIN legislators l ON l.legislator_id=s.legislator_id
                   ORDER BY s.name, l.name, s.role"""
            )]

    def get_legislator_staff(self, leg_id: str) -> list:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM legislator_staff WHERE legislator_id = ?", (leg_id,)).fetchall()
            return [dict(r) for r in rows]
            
    def get_legislator_issues(self, leg_id: str) -> list:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT issue_area, staff_name FROM legislator_issue_assignments WHERE legislator_id = ? ORDER BY issue_area", (leg_id,)).fetchall()
            return [dict(r) for r in rows]

    def get_committee_staff(self, cmte_name: str) -> list:
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM committee_staff WHERE committee_name LIKE ?", (f"%{cmte_name}%",)).fetchall()
            return [dict(r) for r in rows]
            
    def get_legislator_committee_matrix(self, leg_norm_name: str) -> list:
        """Finds committees where this legislator is Chair or Vice Chair, and returns the consultant staff for those committees."""
        if not leg_norm_name:
            return []
        with closing(self._get_conn()) as conn, conn:
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

    def get_staff_by_name(self, name: str) -> dict | None:
        """
        Look up a full staff record from legislator_staff by name.
        Tries exact match first, then case-insensitive partial match.
        Returns a staff dict (with staff_id, legislator_id, role, email, etc.) or None.
        """
        if not name or name == "—":
            return None
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            # Exact match (case-insensitive)
            row = conn.execute(
                "SELECT * FROM legislator_staff WHERE LOWER(name) = ? LIMIT 1",
                (name.lower(),)
            ).fetchone()
            if row:
                return dict(row)
            # Partial match
            row = conn.execute(
                "SELECT * FROM legislator_staff WHERE LOWER(name) LIKE ? LIMIT 1",
                (f"%{name.lower()}%",)
            ).fetchone()
            return dict(row) if row else None

    def build_staff_name_index(self) -> dict:
        """
        Return a mapping of lowercase staff name → staff dict for all staff in the DB.
        Useful for O(1) lookups when rendering many names at once.
        """
        with closing(self._get_conn()) as conn, conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM legislator_staff").fetchall()
        index = {}
        for row in rows:
            d = dict(row)
            key = (d.get("name") or "").lower().strip()
            if key:
                index[key] = d
        return index
