from notifications import STATUS_LABELS
from tracker_pages import (
    render_dashboard, render_email_notifications, render_hearing_calendar, render_historical_coverage,
)
from service_terms import CALIFORNIA_SOURCE, fetch_congress_member_terms, vote_service_status
import requests
from collections import Counter
import streamlit as st
import pandas as pd
import os
import json
from datetime import date, datetime, timedelta, timezone
import time
import logging
import importlib
import sys
import sqlite3
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from bill_workspace import export_workspace, read_workspace, reattach_profile_note, resolve_attachment, update_tracking, update_bill_note, update_workspace
from html import escape
from bill_research import (
    amendment_diff, bill_revision, build_review_queue, changed_fields, parse_topic_rules,
    readable_bill_markup, topic_match_reason,
)
from hearing_calendar import source_link

from job_manager import JobManager
from job_runner import current_session, run_bootstrap_job, run_refresh_job, run_rescan_job, sync_selected_sessions
from staff_manager import StaffManager, resolve_legislator, normalize_name_components

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config               import DATA_DIR, API_KEY
import auth as _auth
from sync_github_repo     import sync_with_remote as _sync_with_remote
import threading
def sync_with_remote():
    """Run github sync asynchronously."""
    threading.Thread(target=_sync_with_remote, daemon=True).start()

from legiscanner          import run_scan, CSV_FILE, KEYWORDS_FILE, load_keywords, normalize_keywords

# ── Corpus manager (Layer A — master bill corpus) ─────────────────────────────
# Guarded import: if corpus_manager.py is absent the app falls back gracefully.
try:
    from corpus_manager import CorpusManager as _CorpusManager
    from corpus_manager import corpus_file_revision
    _CORPUS_AVAILABLE = True
except ImportError:
    _CORPUS_AVAILABLE = False

DATA_FILE    = CSV_FILE
EXPORT_FILE = os.path.join(DATA_DIR, "Tracked_Bills_Export.csv")

st.set_page_config(page_title="UFtW Bill Tracker", layout="wide")

# ─── Authentication wall ──────────────────────────────────────────────────────
# render_auth_page() returns True if already logged in, otherwise renders the
# login/register UI and we must stop here.
if not _auth.render_auth_page():
    st.stop()

_current_user     = _auth.get_current_user()          # {username, role, api_key, is_guest}
_username         = _current_user["username"]

# Guest edits live in this browser session's temporary workspace.
# Registered users get their own subdirectory.
if _auth.is_guest():
    _user_data_dir = _auth.get_guest_data_dir()
else:
    _user_data_dir = _auth.get_user_data_dir(_username)

# Per-user API key: use theirs if set, otherwise fall back to global config key
_effective_api_key = _current_user.get("api_key") or API_KEY

# ─── Constants ────────────────────────────────────────────────────────────────
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam',
    'AS': 'American Samoa', 'MP': 'Northern Mariana Islands'
}

FEDERAL_TYPES = {
    'US_HOUSE':       'U.S. House of Representatives',
    'US_SENATE':      'U.S. Senate',
    'CONGRESS':       'U.S. Congress (Both Chambers)',
    'FEDERAL_AGENCY': 'Federal Agency Rules/Regulations'
}

STATUS_LABEL_FILE = os.path.join(_user_data_dir, "status_labels.json")
STATUS_LEGEND = dict(STATUS_LABELS)

# ─── Session-state defaults ───────────────────────────────────────────────────
_STATE_DEFAULTS = {
    "selected_states":       [],
    "selected_federal_types":[],
    "status_options":        None,   # initialised after CSV load
    "active_view_name":      None,
}
for _k, _v in _STATE_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─── Jurisdiction helpers ─────────────────────────────────────────────────────
def get_jurisdiction_from_bill_number(bill_number):
    """Only infer jurisdiction from an explicit jurisdiction prefix."""
    if bill_number:
        code, separator, _ = str(bill_number).strip().upper().partition("-")
        if separator and code == "US":
            return "Federal", "U.S. Congress"
        if separator and code in US_STATES:
            return "State", US_STATES[code]
    return "Unknown", "Unknown"


def apply_jurisdiction_columns(df, func):
    if 'bill_number' not in df.columns:
        return df
    if 'jurisdiction_level' not in df:
        df['jurisdiction_level'] = None
    if 'jurisdiction_name' not in df:
        df['jurisdiction_name'] = None
    for i, val in df['bill_number'].dropna().items():
        result = func(val)
        if isinstance(result, (list, tuple)) and len(result) == 2:
            if pd.isna(df.at[i, 'jurisdiction_level']):
                df.at[i, 'jurisdiction_level'] = result[0]
            if pd.isna(df.at[i, 'jurisdiction_name']):
                df.at[i, 'jurisdiction_name'] = result[1]
        else:
            df.at[i, 'jurisdiction_level'] = 'Unknown'
            df.at[i, 'jurisdiction_name']  = 'Unknown'
    return df


# ─── Data-loading functions (Layer B — keyword CSV) ───────────────────────────
@st.cache_data
def load_data(snapshot_revision):
    """Load bill data from the keyword-match CSV (Layer B)."""
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE, dtype={"bill_id": "string"})
            logger.info(f"Loaded {len(df)} bills from CSV")
            if 'jurisdiction_level' not in df.columns or 'jurisdiction_name' not in df.columns:
                df = apply_jurisdiction_columns(df, get_jurisdiction_from_bill_number)
            return df
        logger.warning("CSV data file not found")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        st.error(f"Error loading bill data: {e}")
        return pd.DataFrame()


def save_keywords(keywords):
    """Replace the shared keyword file atomically."""
    destination = Path(KEYWORDS_FILE)
    with TemporaryDirectory(dir=destination.parent) as temporary:
        candidate = Path(temporary) / destination.name
        candidate.write_text(json.dumps(normalize_keywords(keywords), indent=2) + "\n", encoding="utf-8")
        candidate.replace(destination)



def load_saved_views():
    try:
        if os.path.exists(VIEWS_FILE):
            with open(VIEWS_FILE, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading saved views: {e}")
        return {}

def save_saved_views(views):
    """Write atomically THEN sync."""
    try:
        tmp_file = VIEWS_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(views, f, indent=2)
        os.replace(tmp_file, VIEWS_FILE)
        return True
    except Exception as e:
        logger.error(f"Error saving views: {e}")
        st.error(f"Error saving views: {e}")
        return False

def load_bill_workspace(path, default):
    try:
        return read_workspace(path, default)
    except (OSError, ValueError, sqlite3.Error) as error:
        st.error(f"Could not open {Path(path).name}: {error}")
        return default


def publish_bill_workspace(*paths):
    """Refresh local JSON mirrors for the workspace backup workflow."""
    status_key = f"bill_save_status:{_username}"
    pending_key = f"bill_pending_sync:{_username}"
    pending = set(st.session_state.get(pending_key, ()))
    pending.update(map(str, paths))
    try:
        supported = {TRACKED_FILE, NOTES_FILE, TOPICS_FILE, REVIEW_QUEUE_FILE,
                     _profile_notes_path("staff"), _profile_notes_path("legislator")}
        for path in pending.intersection(supported):
            export_workspace(path)
    except (OSError, ValueError, sqlite3.Error) as error:
        logger.warning("Bill workspace backup could not be written: %s", error)
        st.session_state[pending_key] = sorted(pending)
        st.session_state[status_key] = "mirror_pending"
        return

    st.session_state[pending_key] = []
    st.session_state[status_key] = "local"


def change_tracking(*, add=None, remove=()):
    try:
        update_tracking(TRACKED_FILE, add=add, remove=remove)
    except (OSError, ValueError, sqlite3.Error) as error:
        st.error(f"Tracking change could not be saved: {error}")
        return False
    publish_bill_workspace(TRACKED_FILE)
    return True


# ─── Profile note helpers (legislators & staff) ───────────────────────────────
def _profile_notes_path(kind: str) -> str:
    """kind = 'legislator' or 'staff'"""
    return os.path.join(_user_data_dir, f"{kind}_notes.json")

def load_profile_notes(kind: str) -> dict:
    return load_bill_workspace(_profile_notes_path(kind), {})


def save_profile_note(kind, profile_id, expected_text, text):
    path = _profile_notes_path(kind)

    def apply(notes):
        if notes.get(profile_id) != expected_text:
            raise ValueError("This profile note changed in another session. Reload it before saving.")
        notes[profile_id] = text

    try:
        update_workspace(path, {}, apply)
    except (OSError, ValueError, sqlite3.Error) as error:
        st.error(f"Could not save profile note: {error}")
        return False
    publish_bill_workspace(path)
    return True


def render_profile_note_recovery(legislators, staff_manager):
    legislator_labels = {
        str(row["legislator_id"]): f'{row["name"]} | {row.get("chamber", "")} {row.get("district", "")}'
        for row in legislators.to_dict("records")
    }
    staff_labels = {
        row["staff_id"]: f'{row["name"]} | {row["office"]} | {row["role"].replace("_", " ")}'
        for row in staff_manager.get_office_staff_profiles()
    }
    for kind, labels in (("legislator", legislator_labels), ("staff", staff_labels)):
        notes = load_profile_notes(kind)
        orphaned = {key: value for key, value in notes.items() if key not in labels}
        if not orphaned:
            continue
        with st.expander(f"Reconnect older {kind} notes"):
            st.caption("These notes have no current profile. Choose a destination only after verifying the identity.")
            source = st.selectbox("Note to reconnect", list(orphaned), key=f"reconnect_{kind}_source",
                                  format_func=lambda key: f"{str(orphaned[key])[:70]} | {key}")
            st.text(str(orphaned[source]))
            available = {key: label for key, label in labels.items() if key not in notes}
            target = st.selectbox("Attach to profile", [None, *available], key=f"reconnect_{kind}_destination",
                                  format_func=lambda key: available.get(key, "Choose a profile"))
            if st.button("Reconnect note", key=f"reconnect_{kind}", disabled=target is None):
                path = _profile_notes_path(kind)
                try:
                    reattach_profile_note(path, source, target, orphaned[source])
                except (OSError, ValueError, sqlite3.Error) as error:
                    st.error(f"Note could not be reconnected: {error}")
                else:
                    publish_bill_workspace(path)
                    st.rerun()


# ─── Search helper ────────────────────────────────────────────────────────────
def search_df(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Search available bill fields for literal text."""
    if not query or df.empty:
        return df
    fields = ["title", "sponsors", "sponsor_names", "description", "bill_number", "subjects", "keyword", "committees"]
    matches = pd.Series(False, index=df.index)
    for column in fields:
        if column in df:
            matches |= df[column].astype("string").str.contains(query, case=False, regex=False, na=False)
    return df[matches]


def filter_bill_keywords(frame, selected):
    selected = {entry.casefold() for entry in normalize_keywords(list(selected))}
    return frame[frame["keyword"].fillna("").map(
        lambda entries: any(entry.casefold() in selected for entry in normalize_keywords(entries.split(";")))
    )]


# ─── Sort helper ──────────────────────────────────────────────────────────────
_SORT_OPTIONS = ["Most Recent Action", "Status Date (Newest)", "Bill Number A→Z", "Status Stage"]
_TRACKED_SORT_OPTIONS = ["Most Recent Action", "Status Date (Newest)", "Bill Number A→Z", "Status Stage", "Priority (High First)", "Last Reviewed (Newest)", "Follow-up (Soonest)"]

def apply_sort(df: pd.DataFrame, sort_key: str) -> pd.DataFrame:
    if df.empty:
        return df
    if sort_key == "Most Recent Action":
        col = 'last_action_date' if 'last_action_date' in df.columns else 'status_date'
        return df.sort_values(col, ascending=False, na_position='last') if col in df else df
    if sort_key == "Status Date (Newest)":
        if 'status_date' in df.columns:
            return df.sort_values('status_date', ascending=False, na_position='last')
    if sort_key == "Bill Number A→Z":
        if 'bill_number' in df.columns:
            return df.sort_values('bill_number', ascending=True, na_position='last')
    if sort_key == "Status Stage":
        if 'status_stage' in df.columns:
            return df.sort_values('status_stage', ascending=True, na_position='last')
    if sort_key == "Priority (High First)":
        if 'priority' in df.columns:
            prio_map = {"High": 1, "Medium": 2, "Low": 3, "": 4}
            df['_prio_sort'] = df['priority'].map(lambda x: prio_map.get(x, 4))
            return df.sort_values('_prio_sort', ascending=True).drop(columns=['_prio_sort'])
    if sort_key == "Last Reviewed (Newest)":
        if 'last_reviewed' in df.columns:
            return df.sort_values('last_reviewed', ascending=False, na_position='last')
    if sort_key == "Follow-up (Soonest)" and "follow_up" in df:
        return df.sort_values("follow_up", key=lambda dates: dates.replace("", pd.NA), na_position="last")
    return df


# ─── Corpus helpers ───────────────────────────────────────────────────────────
@st.cache_data(ttl=60, max_entries=32, show_spinner=False)
def load_corpus_filter_options(db_path, revision, include_facets, _corpus):
    statuses = get_corpus_status_options(_corpus)
    sponsors, committees = _corpus.get_filter_options() if include_facets else ([], [])
    return statuses, sponsors, committees


def get_corpus_status_options(corpus) -> list:
    """Distinct status_stage values from corpus DB (used in All Bills tab)."""
    if corpus is None:
        return []
    try:
        with corpus.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT status_stage FROM bills "
                "WHERE status_stage IS NOT NULL ORDER BY CAST(status_stage AS INTEGER)"
            ).fetchall()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []


def get_tracked_bills_df(tracked_bills, corpus, df_csv: pd.DataFrame) -> pd.DataFrame:
    """
    Return a DataFrame of all tracked bills.
    Prefers corpus data; falls back to CSV rows for any not found in corpus.
    Compatible with _render_bill_card() column expectations.
    """
    if not tracked_bills:
        return pd.DataFrame()

    # Try corpus first
    corpus_df = pd.DataFrame()
    if corpus is not None:
        try:
            ids = [item for item in tracked_bills if str(item).isascii() and str(item).isdecimal()]
            corpus_df = corpus.get_bills_by_ids(ids)
        except Exception as e:
            logger.warning(f"Tracked bills corpus lookup failed: {e}")
            corpus_df = pd.DataFrame()

    found_in_corpus = set(corpus_df['bill_id'].astype(str).tolist()) if not corpus_df.empty else set()

    # Fall back to CSV for any bills not found in corpus
    missing = [b for b in tracked_bills if b not in found_in_corpus]
    csv_fallback = pd.DataFrame()
    if missing and not df_csv.empty and 'bill_id' in df_csv.columns:
        csv_fallback = df_csv[df_csv['bill_id'].astype(str).isin(missing)].copy()

    frames = [f for f in [corpus_df, csv_fallback] if not f.empty]
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    # Preserve tracked_bills ordering
    order = {b: i for i, b in enumerate(tracked_bills)}
    result['_order'] = result['bill_id'].astype(str).map(lambda x: order.get(x, 9999))
    return result.sort_values('_order').drop(columns=['_order'])


def build_export_df(source_df: pd.DataFrame, bill_notes: dict, tracked_bills: list) -> pd.DataFrame:
    if source_df.empty:
        return pd.DataFrame()
    export_df = source_df.copy()
    
    for col in ['jurisdiction_level', 'jurisdiction_name', 'session', 'status_stage', 'status_date', 
                'last_action', 'last_action_date', 'subjects', 'sponsors', 'committees', 'keyword', 'url']:
        if col not in export_df.columns:
            export_df[col] = ''
            
    export_df['tracked'] = export_df['bill_id'].astype(str).apply(lambda x: 'Yes' if x in tracked_bills else 'No')
    export_df['position'] = export_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get("position", ""))
    export_df['priority'] = export_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get("priority", ""))
    export_df['notes_comment'] = export_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get("comment", ""))
    export_df['notes_links'] = export_df['bill_id'].astype(str).apply(lambda x: ", ".join(bill_notes.get(x, {}).get("links", [])))
    export_df["follow_up"] = export_df["bill_id"].astype(str).map(
        lambda bill_id: bill_notes.get(bill_id, {}).get("follow_up", "")
    )
    export_df['my_topics'] = export_df['bill_id'].astype(str).map(
        lambda bill_id: "; ".join(bill_notes.get(bill_id, {}).get("topics", []))
    )
    export_df['last_reviewed'] = export_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get("last_reviewed", ""))
    
    cols = [
        'bill_number', 'title', 'jurisdiction_level', 'jurisdiction_name', 'session',
        'status_stage', 'status_date', 'last_action', 'last_action_date',
        'subjects', 'sponsors', 'committees', 'keyword', 'url',
        'tracked', 'position', 'priority', 'notes_comment', 'notes_links', 'my_topics', 'follow_up', 'last_reviewed'
    ]
    cols = [c for c in cols if c in export_df.columns]
    safe_export = export_df[cols].copy()
    for column in safe_export.select_dtypes(include=["object", "string"]).columns:
        safe_export[column] = safe_export[column].map(
            lambda value: "'" + value
            if isinstance(value, str) and (
                value.startswith(("\t", "\r", "\n"))
                or value.lstrip().startswith(("=", "+", "-", "@"))
            ) else value
        )
    return safe_export

# ─── Dashboard ────────────────────────────────────────────────────────────────
def run_smart_header(df_count, app_mode, corpus, tracked_bills):
    """Name the result scope and show the latest successful refresh."""
    st.subheader(app_mode)
    st.caption(f"{df_count:,} matching bills | {len(tracked_bills):,} tracked")
    if app_mode == "Keyword Matches":
        if os.path.isfile(DATA_FILE):
            modified = datetime.fromtimestamp(os.path.getmtime(DATA_FILE))
            st.caption(f"Keyword snapshot saved {modified:%b %d, %Y at %H:%M} (server time)")
    elif corpus:
        stats = corpus.get_corpus_stats()
        refreshed = stats.get("last_incremental_CA")
        if refreshed:
            updated = datetime.fromisoformat(refreshed)
            st.caption(f"California corpus refreshed {updated:%b %d, %Y at %H:%M %Z}")
        else:
            st.caption("No completed California refresh recorded yet")

    if not df_count:
        st.info("No bills match this view. Clear filters or try a different search.")


def _normalize_note(note: dict) -> dict:
    return {
        "comment":       note.get("comment", ""),
        "links":         note.get("links", []),
        "files":         note.get("files", []),
        "position":      note.get("position", ""),
        "priority":      note.get("priority", ""),
        "last_reviewed": note.get("last_reviewed", ""),
        "topics": note.get("topics", []),
        "follow_up": note.get("follow_up", ""),
    }

@st.dialog("📄 Bill Text", width="large")
def _show_bill_text_modal(bill_id, doc_id, bill_number, bill_url=None):
    st.subheader(f"Text for {bill_number}")
    if not corpus:
        st.error("Corpus metadata not available.")
        return
    
    # Safe conversion from pandas/JSON/SQLite types
    try:
        clean_bill_id = int(bill_id)
        # Check for NaN/None safely
        clean_doc_id = int(doc_id) if (doc_id is not None and str(doc_id).lower() != 'nan') else None
    except (ValueError, TypeError):
        clean_bill_id = 0
        clean_doc_id = None

    with st.spinner("Fetching full text from LegiScan..."):
        res = corpus.get_bill_text(clean_bill_id, doc_id=clean_doc_id)
    
    if not res:
        st.error("Could not retrieve bill text. It may not be available in HTML format yet.")
        if link := source_link(bill_url):
            st.link_button("View on LegiScan", link)
        return
    
    if res.get("html"):
        try:
            markup = readable_bill_markup(res["html"])
        except ValueError as error:
            st.warning(str(error))
            if link := source_link(bill_url):
                st.link_button("View on LegiScan", link)
            return
        # Wrap in a scrolling div for better UX
        st.components.v1.html(
            f"""
            <div style="font-family: sans-serif; line-height: 1.6; color: #333; height: 80vh; overflow-y: scroll; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
                {markup}
            </div>
            """,
            height=850,
            scrolling=False
        )
    elif link := source_link(res.get("pdf_url")):
        st.info("HTML text not available. You can view the official PDF below:")
        st.link_button("📂 View PDF", link, use_container_width=True)
    else:
        st.warning("No text content found.")

def render_amendment_comparison(bill_id, prefix):
    with st.expander("Compare amendments"):
        versions_key = f"{_username}:{prefix}:versions"
        if st.button("Load text versions", key=f"{prefix}_load_versions"):
            try:
                st.session_state[versions_key] = corpus.get_bill_versions(bill_id)
            except (ValueError, OSError) as error:
                st.error(f"Versions could not be loaded: {error}")
        versions = st.session_state.get(versions_key)
        if versions is None:
            return
        if len(versions) < 2:
            st.caption("Fewer than two text versions are available from LegiScan.")
            return
        documents = {int(version["doc_id"]): version for version in versions}

        def label(doc_id):
            version = documents[doc_id]
            return f'{version.get("date") or "Undated"} | {version.get("type") or "Bill text"} | {doc_id}'

        first, second = st.columns(2)
        earlier = first.selectbox("Earlier version", list(documents), index=len(documents) - 2,
                                  format_func=label, key=f"{prefix}_earlier")
        later = second.selectbox("Later version", list(documents), index=len(documents) - 1,
                                 format_func=label, key=f"{prefix}_later")
        for doc_id in dict.fromkeys((earlier, later)):
            url = documents[doc_id].get("state_link") or documents[doc_id].get("url")
            if isinstance(url, str) and url.startswith(("https://", "http://")):
                st.link_button(f"Source: {label(doc_id)}", url)
        if st.button("Compare text", disabled=earlier == later, key=f"{prefix}_compare"):
            before = corpus.get_bill_text(bill_id, earlier) or {}
            after = corpus.get_bill_text(bill_id, later) or {}
            if not before.get("html") or not after.get("html"):
                st.info("Both versions need HTML text for line comparison. Use the source links for other formats.")
                return
            try:
                diff = amendment_diff(before["html"], after["html"])
            except ValueError as error:
                st.info(str(error))
                return
            if diff:
                st.code(diff, language="diff")
                st.download_button("Download changes", diff, file_name=f"bill-{bill_id}-changes.diff",
                                   mime="text/plain", key=f"{prefix}_diff_download")
            else:
                st.info("No differences in the visible text.")


def _render_bill_card(row, raw_note: dict, bill_id: str,
                      bill_notes: dict, tracked_bills: list,
                      key_prefix: str) -> dict:
    """
    Render a single bill as a rich expandable card.
    """
    from datetime import datetime, timedelta, timezone
    note = _normalize_note(raw_note)
    status      = str(row.get('status_stage', ''))
    status_lbl  = STATUS_LEGEND.get(status, f"Status {status}") if status else 'Unknown'
    jur_level   = row.get('jurisdiction_level', '')
    jur_name    = row.get('jurisdiction_name', '')
    jur_icon    = '🏛️' if jur_level == 'Federal' else ('🗺️' if jur_level == 'State' else '🌐')
    kw_raw      = str(row.get('keyword', '') or '')
    kw_tags     = [t.strip() for t in kw_raw.replace(';', ',').split(',') if t.strip()]
    disp_bill_number = row.get('bill_number', bill_id)
    
    with st.container(border=True):
        c1, c2 = st.columns([5, 1])
        with c1:
            st.markdown(f"### {disp_bill_number}: {row.get('title', 'No title')}")
            
            indicator_html = f"<b>{jur_icon} {escape(str(jur_name))}</b> &nbsp;|&nbsp; 📋 {escape(str(status_lbl))}"
            prio = note.get('priority')
            if prio == 'High': indicator_html += " &nbsp;|&nbsp; <span style='color:red;'><b>🔥 High Priority</b></span>"
            elif prio: indicator_html += f" &nbsp;|&nbsp; 🔥 {escape(str(prio))}"
            
            pos = note.get('position')
            if pos: indicator_html += f" &nbsp;|&nbsp; 🏷️ <b>{escape(str(pos))}</b>"
            
            if bool(note.get('comment', '').strip()): indicator_html += " &nbsp;|&nbsp; 📝 Has Notes"
            if bill_id in tracked_bills: indicator_html += " &nbsp;|&nbsp; ⭐ Tracked"
            
            st.markdown(indicator_html, unsafe_allow_html=True)

            last_action = row.get('last_action', '')
            last_action_date = row.get('last_action_date', '')
            staleness_warning = ""
            if last_action_date:
                try:
                    dt = datetime.strptime(str(last_action_date).split()[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                    if dt < (now_utc - timedelta(days=540)):
                        staleness_warning = " ⚠️ *(Warning: Last action is > 18 months old)*"
                except: pass
                
            if last_action or last_action_date:
                st.caption(f"**Last Action:** {last_action} ({last_action_date}){staleness_warning}")
            
            if kw_tags:
                st.write(" ".join([f"`{t}`" for t in kw_tags]))

            sponsors = str(row.get('sponsors', row.get('sponsor_names', '—')))
            if sponsors != '—' and sponsors.strip():
                st.caption("**Sponsors & Coauthors**")
                _sp_list = [s.strip() for s in sponsors.split(',') if s.strip()]
                if _sp_list:
                    sp_cols = st.columns(min(len(_sp_list), 5))
                    for idx, clean_sp in enumerate(_sp_list):
                        with sp_cols[idx % 5]:
                            def jump_prof(sp=clean_sp):
                                st.session_state.app_mode = "👔 Legislator Directory"
                                st.session_state.active_profile = sp
                            st.button(f"{clean_sp[:25]}", key=f"lnk_{key_prefix}_{idx}", on_click=jump_prof, type="tertiary", use_container_width=True)

        with c2:
            if bill_id not in tracked_bills:
                if st.button("⭐ Track", key=f"{key_prefix}_track", use_container_width=True):
                    if change_tracking(add=bill_id):
                        st.toast(f"Tracked {disp_bill_number}")
                        st.rerun()
            else:
                if st.button("❌ Untrack", key=f"{key_prefix}_untrack", use_container_width=True):
                    if change_tracking(remove=(bill_id,)):
                        st.toast(f"Untracked {disp_bill_number}")
                        st.rerun()

            if row.get('url'):
                st.link_button("📄 LegiScan", row['url'], use_container_width=True)
            
            # 📖 In-app Text Reader
            doc_id = row.get("latest_doc_id")
            if st.button("📖 Open Text", key=f"{key_prefix}_text", use_container_width=True):
                _show_bill_text_modal(bill_id, doc_id, disp_bill_number, bill_url=row.get('url'))

        with st.expander("📝 View Details & Edit Notes"):
            info1, info2 = st.columns(2)
            with info1:
                st.write(f"**Session:** {row.get('session', '—')}")
                desc = row.get('description', '')
                if desc: st.write(f"**Summary:** {desc}")
                
            with info2:
                committees = row.get('committees', row.get('committee', '—'))
                st.write(f"**Committee(s):** {committees}")
                subjects = row.get('subjects', '')
                if subjects: st.write(f"**Subjects:** {subjects}")

            st.divider()
            baseline_key = f"{_username}:{key_prefix}_note_baseline"
            if baseline_key not in st.session_state:
                st.session_state[baseline_key] = deepcopy(raw_note)
            nc1, nc2 = st.columns(2)
            with nc1:
                new_comment = st.text_area("💬 Notes / Comments", value=note.get("comment", ""), key=f"{key_prefix}_cmt")
                new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{key_prefix}_lnks")
            with nc2:
                position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch", "Neutral", "No Position"], 
                                        index=["", "Support", "Oppose", "Watch", "Neutral", "No Position"].index(note.get("position", "")), key=f"{key_prefix}_pos")
                priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], 
                                        index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{key_prefix}_prio")
            
            topic_input = st.text_input(
                "My topics", value=", ".join(note["topics"]), key=f"{key_prefix}_topics",
                help="Separate topics with commas. A bill can belong to more than one topic.",
            )
            follow_up = st.date_input(
                "Follow-up date",
                value=datetime.fromisoformat(note["follow_up"]).date() if note["follow_up"] else None,
                key=f"{key_prefix}_follow_up",
            )
            if st.session_state.pop(f"{key_prefix}_clear_decision_reason", False):
                st.session_state.pop(f"{key_prefix}_decision_reason", None)
            decision_reason = st.text_input(
                "Reason for this decision (optional)", max_chars=500, key=f"{key_prefix}_decision_reason",
                help="This is kept in your private decision history for the current bill version.",
            )
            uploaded_file = st.file_uploader("📎 Attach PDF", type=["pdf"], key=f"{key_prefix}_upl")
            
            if st.button("💾 Save Notes", key=f"{key_prefix}_save"):
                attachment = None
                changes = {
                    "comment": new_comment,
                    "links": [link.strip() for link in new_links.split(",") if link.strip()],
                    "position": position,
                    "priority": priority,
                    "topics": list({topic.casefold(): topic for part in topic_input.split(",") if (topic := part.strip())}.values()),
                    "follow_up": follow_up.isoformat() if follow_up else "",
                    "last_reviewed": datetime.now(timezone.utc).isoformat(),
                    "reviewed_revision": bill_revision(row),
                }
                expected = st.session_state[baseline_key]
                try:
                    if uploaded_file:
                        pdf = uploaded_file.getbuffer()
                        if len(pdf) > 20 * 1024 * 1024 or not bytes(pdf[:5]) == b"%PDF-":
                            raise ValueError("Attach a PDF file of up to 20 MB.")
                        target = Path(UPLOAD_DIR, f"{uuid4().hex}_{Path(uploaded_file.name).name}")
                        with target.open("xb") as file:
                            attachment = target
                            file.write(pdf)
                        changes["files"] = [*expected.get("files", []), attachment.name]
                    saved = update_bill_note(
                        NOTES_FILE, bill_id, expected, changes, actor=_username, reason=decision_reason,
                    )
                except (OSError, ValueError, sqlite3.Error) as error:
                    if attachment is not None:
                        attachment.unlink(missing_ok=True)
                    st.error(f"Notes could not be saved: {error}")
                else:
                    note = bill_notes[bill_id] = saved[bill_id]
                    st.session_state[baseline_key] = deepcopy(note)
                    paths = [NOTES_FILE]
                    if attachment is not None:
                        paths.append(attachment)
                    publish_bill_workspace(*paths)
                    st.session_state[f"{key_prefix}_clear_decision_reason"] = True
                    st.toast(f"Notes saved for {disp_bill_number}")
                    st.rerun()

            if note["files"]:
                attachments = {
                    path.name: path for reference in note["files"]
                    if (path := resolve_attachment(UPLOAD_DIR, reference)) is not None
                }
                if attachments:
                    selected = st.selectbox(
                        "Saved attachments", [None, *attachments], key=f"{key_prefix}_attachment",
                        format_func=lambda name: name.split("_", 1)[-1] if name else "Choose a file",
                    )
                    if selected:
                        path = attachments[selected]
                        with path.open("rb") as file:
                            st.download_button("Download attachment", file, file_name=path.name.split("_", 1)[-1],
                                               mime="application/pdf", key=f"{key_prefix}_download_attachment")
                else:
                    st.caption("The recorded attachments are missing from this workspace.")

            if note.get("last_reviewed"):
                try:
                    _lr_dt = datetime.fromisoformat(note["last_reviewed"]).strftime("%Y-%m-%d %H:%M")
                    st.caption(f"Last reviewed: {_lr_dt}")
                except: pass

        if bill_id in tracked_bills:
            revision = bill_revision(row)
            previous = raw_note.get("reviewed_revision")
            changes_since_review = changed_fields(previous, revision)
            if changes_since_review:
                st.caption("Changed since review: " + ", ".join(changes_since_review))
            elif not previous:
                st.caption("No review snapshot yet.")
            if st.button("Mark reviewed", key=f"{key_prefix}_review"):
                try:
                    update_bill_note(NOTES_FILE, bill_id, raw_note, {
                        "reviewed_revision": revision,
                        "last_reviewed": datetime.now(timezone.utc).isoformat(),
                    }, actor=_username)
                except (OSError, ValueError, sqlite3.Error) as error:
                    st.error(f"Review could not be saved: {error}")
                else:
                    st.session_state.pop(baseline_key, None)
                    publish_bill_workspace(NOTES_FILE)
                    st.rerun()

        history = raw_note.get("decision_history", [])
        if history:
            with st.expander("Decision history"):
                labels = {"position": "Position", "priority": "Priority", "follow_up": "Follow-up", "topics": "Topics"}

                def display(value):
                    return ", ".join(value) if isinstance(value, list) and value else str(value) if value else "Unset"

                rows = []
                for event in reversed(history):
                    evidence = event["evidence"]
                    changes = "\n".join(
                        f'{labels[field]}: {display(values["before"])} → {display(values["after"])}'
                        for field, values in event["changes"].items()
                    )
                    rows.append({
                        "Recorded (UTC)": event["recorded_at"][:16].replace("T", " "),
                        "Type": event["kind"],
                        "Actor": event["actor"],
                        "Change": changes,
                        "Reason": event["reason"],
                        "Bill text version": evidence.get("Text version", ""),
                        "Recorded last action": evidence.get("Last action", ""),
                    })
                st.dataframe(pd.DataFrame(rows), hide_index=True)
                versions = list(dict.fromkeys(
                    item["Bill text version"] for item in rows if item["Bill text version"].isascii()
                    and item["Bill text version"].isdecimal()
                ))
                if corpus and versions:
                    selected_version = st.selectbox("Reviewed text version", versions, key=f"{key_prefix}_reviewed_version")
                    if st.button("Open reviewed text", key=f"{key_prefix}_open_reviewed_version"):
                        _show_bill_text_modal(bill_id, selected_version, disp_bill_number, row.get("url"))
                st.download_button(
                    "Download decision history", json.dumps({"bill_id": bill_id, "events": history}, indent=2),
                    file_name=f"bill-{bill_id}-decision-history.json", mime="application/json",
                    key=f"{key_prefix}_decision_history_export",
                )

        if corpus and bill_id.isascii() and bill_id.isdecimal():
            render_amendment_comparison(int(bill_id), key_prefix)

        for heading, field in (("Bill history", "history"), ("Committee referrals", "referrals")):
            entries = row.get(field)
            if isinstance(entries, str) and entries.strip():
                with st.expander(heading):
                    for entry in entries.split("; "):
                        st.text(entry)

        with st.expander("🗳️ Roll Call Votes"):
            if corpus:
                try:
                    rcs = corpus.get_roll_calls_for_bill(int(bill_id))
                    if not rcs:
                        st.info("No recorded roll-call votes available for this bill.")
                    else:
                        for rc in rcs:
                            passed_str = "✅ Passed" if rc.get('passed') else "❌ Failed"
                            st.markdown(f"**{rc.get('date')}** — {rc.get('chamber', 'Chamber')} / {rc.get('desc')} ({passed_str})")
                            st.caption(f"Yea: {rc.get('yea', 0)} | Nay: {rc.get('nay', 0)} | NV: {rc.get('nv', 0)} | Absent: {rc.get('absent', 0)}")
                            m_votes = rc.get('member_votes', [])
                            if m_votes:
                                with st.expander("View Individual Votes"):
                                    # Create small columns for a grid of votes
                                    cols = st.columns(3)
                                    for v_idx, mv in enumerate(m_votes):
                                        v_txt = mv.get('vote_text', 'Unknown')
                                        raw_name = mv.get('name') or ""
                                        
                                        if not raw_name or "Unknown Profile" in raw_name:
                                            p_name = f"<span style='color:red;'>[Unresolved ID {escape(str(mv.get('people_id')))}]</span>"
                                        elif mv.get('staff_legislator_id'):
                                            # If mapped, indicate linkage (we could link to profile here later)
                                            p_name = f"<strong>{escape(str(raw_name))}</strong>"
                                        else:
                                            p_name = escape(str(raw_name))
                                            
                                        color = "green" if v_txt == "Yea" else ("red" if v_txt == "Nay" else "gray")
                                        cols[v_idx % 3].markdown(f"{p_name}: <span style='color:{color}; font-weight:bold;'>{escape(str(v_txt))}</span>", unsafe_allow_html=True)
                            st.divider()
                except Exception as e:
                    st.error(f"Error loading votes: {e}")
            else:
                st.info("Roll-call database is offline.")

    return note


# ── Resolve per-user file paths now that we are authenticated ──────────────────
TRACKED_FILE = os.path.join(_user_data_dir, "tracked_bills.json")
NOTES_FILE   = os.path.join(_user_data_dir, "bill_notes.json")
VIEWS_FILE   = os.path.join(_user_data_dir, "saved_views.json")
TOPICS_FILE  = os.path.join(_user_data_dir, "saved_topics.json")
REVIEW_QUEUE_FILE = os.path.join(_user_data_dir, "review_queue.json")
UPLOAD_DIR   = os.path.join(_user_data_dir, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── Load application data ────────────────────────────────────────────────────
try:
    try:
        keywords_list = load_keywords()
    except (OSError, ValueError) as error:
        st.sidebar.error(f"Could not read shared keywords: {error}")
        keywords_list = []
    STATUS_LEGEND.update(load_bill_workspace(STATUS_LABEL_FILE, {}))
    tracked_bills = [str(item) for item in load_bill_workspace(TRACKED_FILE, [])]
    bill_notes    = load_bill_workspace(NOTES_FILE, {})
    try:
        snapshot = Path(DATA_FILE).stat()
        revision = (snapshot.st_mtime_ns, snapshot.st_size)
    except FileNotFoundError:
        revision = None
    df = load_data(revision)
    saved_views   = load_saved_views()
except Exception as e:
    st.error(f"Critical error loading application data: {e}")
    st.stop()

# ── Initialize CorpusManager ──────────────────────────────────────────────────
@st.cache_resource
def get_job_manager():
    try: return JobManager(os.path.join(DATA_DIR, "jobs.db"))
    except Exception as e:
        logger.error(f"Failed to load JobManager: {e}")
        return None

@st.cache_resource
def get_staff_manager():
    try: return StaffManager(os.path.join(DATA_DIR, "staff.db"))
    except Exception as e:
        logger.error(f"Failed to load StaffManager: {e}")
        return None

@st.cache_resource
def get_corpus_manager(api_key: str = ""):
    """CorpusManager keyed on api_key so a per-user key gets its own instance."""
    if not _CORPUS_AVAILABLE: return None
    effective_key = api_key or API_KEY
    try: return _CorpusManager(os.path.join(DATA_DIR, "bills.db"), effective_key)
    except Exception as _ce:
        logger.warning(f"CorpusManager init failed (non-fatal): {_ce}")
        return None

job_manager   = get_job_manager()
staff_manager = get_staff_manager()
corpus        = get_corpus_manager(_effective_api_key)
# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED SIDEBAR COMMAND CENTER 
# ═══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🏛️ UFtW Bill Tracker")
if st.session_state.get(f"bill_save_status:{_username}") in {"mirror_pending"}:
    st.sidebar.warning("Bill changes are saved locally. Backup is pending.")
    if st.sidebar.button("Retry local workspace copy"):
        publish_bill_workspace()
        st.rerun()
elif st.session_state.get(f"bill_save_status:{_username}") == "synced":
    st.sidebar.caption("Bill workspace changes published to GitHub.")
elif st.session_state.get(f"bill_save_status:{_username}") == "local":
    st.sidebar.caption("Bill workspace changes saved locally.")


# ── User info & logout ────────────────────────────────────────────────────────
if _auth.is_guest():
    _role_badge = "👁️ Guest"
elif _auth.is_admin():
    _role_badge = "👑 Admin"
else:
    _role_badge = "👤 User"
st.sidebar.caption(f"{_role_badge} — **{_username}**")
_signout_label = "🔐 Sign In / Register" if _auth.is_guest() else "🚪 Sign Out"
if st.sidebar.button(_signout_label, use_container_width=True):
    _auth.logout()
    st.rerun()

# Account settings (password + API key)
_auth.render_account_settings_sidebar()

st.sidebar.divider()

# ── A. SEARCH & MODE (Always Visible) ─────────────────────────────────────────
global_search = st.sidebar.text_input(
    "🔍 Global Search",
    value=st.session_state.get("global_search", ""),
    placeholder="bill number, title, sponsor...",
    key="global_search_input"
)
st.session_state.global_search = global_search

view_modes = ["Dashboard", "🔍 All Bills", "🏷️ Keyword Matches", "⭐ Tracked Bills", "Topic Inbox",
              "Hearing Calendar", "Historical Coverage", "👔 Legislator Directory", "Federal Voting Profiles", "🛠️ Staff Analytics"]
queue_preferences = {}
if not _auth.is_guest():
    view_modes.append("Email Notifications")
    queue_preferences = load_bill_workspace(REVIEW_QUEUE_FILE, {})
    queue_toggle = f"review_queue_enabled:{_username}"

    def change_queue_preference():
        enabled = st.session_state[queue_toggle]
        try:
            update_workspace(REVIEW_QUEUE_FILE, {}, lambda current: current.update(enabled=enabled))
        except (OSError, ValueError, sqlite3.Error) as error:
            st.session_state[queue_toggle] = queue_preferences.get("enabled", False)
            st.sidebar.error(f"Queue preference could not be saved: {error}")
        else:
            if not enabled and st.session_state.get("app_mode") == "Daily Review":
                st.session_state["app_mode"] = "⭐ Tracked Bills"
            publish_bill_workspace(REVIEW_QUEUE_FILE)

    st.sidebar.checkbox("Enable Daily Review", value=queue_preferences.get("enabled", False),
                        key=queue_toggle, on_change=change_queue_preference)
    if queue_preferences.get("enabled"):
        view_modes.insert(4, "Daily Review")
if st.session_state.get("app_mode") == "Daily Review" and "Daily Review" not in view_modes:
    st.session_state["app_mode"] = "⭐ Tracked Bills"
app_mode = st.sidebar.radio("View Mode", view_modes, key="app_mode")

st.sidebar.divider()

# ── B. SAVED VIEWS ────────────────────────────────────────────────────────────
st.sidebar.subheader("💾 Saved Views")
BUILTIN_VIEWS = {
    "📋 Tracked — All":            {"app_mode": "⭐ Tracked Bills", "global_sort": "Most Recent Action", "tracked_quick": "All Tracked"},
    "🚨 Tracked — Needs Attention": {"app_mode": "⭐ Tracked Bills", "global_sort": "Priority (High First)", "tracked_quick": "Needs Attention"},
    "📝 Tracked — Needs Notes":    {"app_mode": "⭐ Tracked Bills", "global_sort": "Priority (High First)", "tracked_quick": "No Notes"},
    "🔥 Tracked — High Priority":  {"app_mode": "⭐ Tracked Bills", "global_sort": "Last Reviewed (Newest)", "tracked_quick": "High Priority"},
    "🏷️ Keyword — Recent Action":  {"app_mode": "🏷️ Keyword Matches", "global_sort": "Most Recent Action"},
    "🏛️ CA Bills — Recent":        {"app_mode": "🔍 All Bills", "global_jur": ["California"], "global_sort": "Most Recent Action"},
    "🇺🇸 Federal Bills — Recent":  {"app_mode": "🔍 All Bills", "global_jur": ["U.S. Congress"], "global_sort": "Most Recent Action"},
}

_av = st.session_state.active_view_name
if _av:
    st.sidebar.caption(f"Active: **{_av}**")

_view_tabs1, _view_tabs2 = st.sidebar.tabs(["Presets", "My Views"])
with _view_tabs1:
    _sel_builtin = st.selectbox("Select Preset", list(BUILTIN_VIEWS.keys()), key="sel_builtin")
    if st.button("Load Preset", key="load_builtin_btn"):
        st.session_state.active_view_name = _sel_builtin
        for k, v in BUILTIN_VIEWS[_sel_builtin].items():
            st.session_state[k] = v
        st.rerun()

with _view_tabs2:
    if not saved_views:
        st.caption("No custom saved views.")
    else:
        _sel_custom = st.selectbox("Select View", list(saved_views.keys()), key="sel_custom_view")
        _cvcol1, _cvcol2 = st.columns(2)
        with _cvcol1:
            if st.button("Load View", key="load_custom_btn"):
                st.session_state.active_view_name = _sel_custom
                for k, v in saved_views[_sel_custom].items():
                    if k != "saved_at":
                        st.session_state[k] = v
                        if k == "global_status":
                            st.session_state.pop("global_status_input", None)
                st.rerun()
        with _cvcol2:
            if st.button("Delete Context", key="del_custom_btn"):
                del saved_views[_sel_custom]
                if save_saved_views(saved_views):
                    if st.session_state.active_view_name == _sel_custom:
                        st.session_state.active_view_name = None
                    st.success("Deleted!")
                    st.rerun()
    st.divider()
    _new_v_name = st.text_input("Name current state:")
    if st.button("Save Current View", key="save_custom_btn"):
        if _new_v_name:
            cv = {
                "app_mode": st.session_state.app_mode,
                "global_search": st.session_state.global_search,
                "global_jur": st.session_state.get("global_jur", []),
                "global_status": st.session_state.get("global_status", []),
                "global_sponsors": st.session_state.get("global_sponsors", []),
                "global_committees": st.session_state.get("global_committees", []),
                "global_date_range": st.session_state.get("global_date_range", []),
                "kw_filter": st.session_state.get("kw_filter", []),
                "tracked_pos": st.session_state.get("tracked_pos", []),
                "tracked_prio": st.session_state.get("tracked_prio", []),
                "tracked_quick": st.session_state.get("tracked_quick", "All Tracked"),
                "global_sort": st.session_state.get("global_sort", "Most Recent Action"),
                "saved_at": datetime.utcnow().isoformat()
            }
            saved_views[_new_v_name] = cv
            if save_saved_views(saved_views):
                st.session_state.active_view_name = _new_v_name
                st.success("Saved view!")
                st.rerun()

st.sidebar.divider()

# ── C. PRIMARY FILTERS ────────────────────────────────────────────────────────
st.sidebar.subheader("🎯 Primary Filters")

# Gather global options from df and corpus where applicable
_all_jur_opts = ["California", "U.S. Congress"]
_all_status_opts = []
_all_sponsors = []
_all_committees = []
if corpus:
    corpus_path = os.path.join(DATA_DIR, "bills.db")
    _all_status_opts, _all_sponsors, _all_committees = load_corpus_filter_options(
        corpus_path, corpus_file_revision(corpus_path), "All Bills" in app_mode, corpus,
    )
if not df.empty:
    _csv_stats = df["status_stage"].dropna().astype(str).unique().tolist() if "status_stage" in df.columns else []
    for s in _csv_stats:
        if s not in _all_status_opts:
            _all_status_opts.append(s)
    _all_status_opts = sorted(_all_status_opts)
    if "sponsors" in df.columns:
        _all_sponsors = set(_all_sponsors) | {
            name.strip() for group in df["sponsors"].dropna().astype(str)
            for name in group.split(",") if name.strip()
        }
    if "committees" in df.columns:
        _all_committees = set(_all_committees) | set(df["committees"].dropna().astype(str))

status_options = list(dict.fromkeys([*_all_status_opts, *STATUS_LEGEND]))
st.session_state.status_options = status_options

global_jur = st.sidebar.multiselect(
    "Jurisdiction",
    options=_all_jur_opts,
    default=st.session_state.get("global_jur", []),
    key="global_jur_input"
)
st.session_state.global_jur = global_jur

st.session_state.global_status = st.sidebar.multiselect(
    "Status Stage",
    options=status_options,
    default=[code for code in st.session_state.get("global_status", []) if code in status_options],
    format_func=lambda code: f"{STATUS_LEGEND.get(code, code)} [{code}]",
    key="global_status_input",
)

global_date_range = st.sidebar.date_input(
    "Status Date Range",
    value=st.session_state.get("global_date_range", []),
    key="global_date_range_input"
)
st.session_state.global_date_range = global_date_range

global_sponsors = st.sidebar.multiselect(
    "Sponsors",
    options=sorted(set(_all_sponsors) | set(st.session_state.get("global_sponsors", []))),
    default=st.session_state.get("global_sponsors", []),
    key="global_sponsors_input"
)
st.session_state.global_sponsors = global_sponsors

global_committees = st.sidebar.multiselect(
    "Committees",
    options=sorted(set(_all_committees) | set(st.session_state.get("global_committees", []))),
    default=st.session_state.get("global_committees", []),
    key="global_committees_input"
)
st.session_state.global_committees = global_committees

st.sidebar.divider()

# ── D. ADVANCED FILTERS (Expander) ───────────────────────────────────────────
with st.sidebar.expander("🛠️ Advanced Filters"):
    snapshot_keywords = df["keyword"].dropna().unique().tolist() if not df.empty and "keyword" in df.columns else []
    _avail_kw = sorted(normalize_keywords([*keywords_list, *snapshot_keywords]), key=str.casefold)
    keyword_by_key = {entry.casefold(): entry for entry in _avail_kw}
    selected_keywords = normalize_keywords(list(st.session_state.get("kw_filter", [])))
    active_keywords = [keyword_by_key[entry.casefold()] for entry in selected_keywords if entry.casefold() in keyword_by_key]
    kw_filter = st.multiselect(
        "Keyword Meta-Tags",
        options=_avail_kw,
        default=active_keywords,
        key="kw_filter_input"
    )
    st.session_state.kw_filter = kw_filter

    tracked_pos = st.multiselect(
        "Annotation Position",
        ["Support", "Oppose", "Watch", "Neutral", "No Position"],
        default=st.session_state.get("tracked_pos", []),
        key="tracked_pos_input"
    )
    st.session_state.tracked_pos = tracked_pos

    tracked_prio = st.multiselect(
        "Annotation Priority",
        ["High", "Medium", "Low"],
        default=st.session_state.get("tracked_prio", []),
        key="tracked_prio_input"
    )
    st.session_state.tracked_prio = tracked_prio

    st.write("---")
    with st.form("custom_status_labels"):
        _ns_code = st.text_input("Custom Status Code", key="new_status_code", max_chars=32).strip()
        _ns_label = st.text_input("Custom Status Label", key="new_status_label", max_chars=80).strip()
        if st.form_submit_button("Save Status Label"):
            if not _ns_code or not _ns_label:
                st.error("Enter both a status code and a label.")
            else:
                try:
                    update_workspace(STATUS_LABEL_FILE, {}, lambda labels: labels.update({_ns_code: _ns_label}))
                except (OSError, ValueError, sqlite3.Error) as error:
                    st.error(f"Could not save the status label: {error}")
                else:
                    st.session_state["status_label_saved"] = True
                st.rerun()

    if st.session_state.pop("status_label_saved", False):
        st.success("Status label saved.")


# ── E. MODE-SPECIFIC OVERLAYS ──────────────────────────────────────────────────
if "⭐ Tracked Bills" in app_mode:
    st.sidebar.divider()
    st.sidebar.caption("**Tracked Bills Quick-Filters**")
    tracked_quick = st.sidebar.radio(
        "Status Flag",
        ["All Tracked", "Needs Attention", "Has Notes", "No Notes", "High Priority", "Recently Updated", "Changes Since Review"],
        index=["All Tracked", "Needs Attention", "Has Notes", "No Notes", "High Priority", "Recently Updated", "Changes Since Review"].index(st.session_state.get("tracked_quick", "All Tracked")),
        key="tracked_quick_input"
    )
    st.session_state.tracked_quick = tracked_quick

# ── F. ADMIN / DATA TOOLS (Expander) ──────────────────────────────────────────
st.sidebar.divider()
with st.sidebar.expander("⚙️ Admin & Database Tools"):
    if not _auth.is_admin():
        st.warning("🔒 Admin access required.")
        st.caption("Contact your administrator to request admin privileges.")
    else:
        # ── User Management ──────────────────────────────────────────────────
        _auth.render_admin_user_management()
        st.divider()

        st.header("📊 System Status")
        if job_manager:
            r_jobs = job_manager.get_recent_jobs(1)
            st.caption(f"Last Job: {r_jobs[0]['job_type']} ({r_jobs[0]['status']})" if r_jobs else "Last Job: None")
            running_jobs = job_manager.get_running_jobs()
            if running_jobs:
                st.warning(f"⚠️ {len(running_jobs)} job(s) currently running!")

        st.write(f"⏱️ **Last Render Time:** {st.session_state.get('last_render_ms', 0):.0f} ms")
        if corpus:
            c_stats = corpus.get_corpus_stats()
            st.write(f"Corpus Size: {c_stats['total_bills']:,} bills")
            try:
                m_stats = corpus.get_people_mapping_stats()
                st.write(f"**LegiScan Person Matches:** {m_stats['matched']} matched / {m_stats['unmatched']} unmatched (Total: {m_stats['total']})")
                if st.button("🔄 Auto-Map LegiScan Names to Roster"):
                    if staff_manager:
                        res = corpus.sync_people_mapping(staff_manager.get_all_legislators())
                        st.success(f"Matched {res['matched']}, Unmatched {res['unmatched']} out of {res['total']}")
                        st.rerun()
            except: pass

            st.divider()
            st.header("🛠️ Diagnostic Tools")
            if st.button("📊 Vote-Person Pipeline Diagnostics"):
                st.session_state.show_diagnostics = not st.session_state.get("show_diagnostics", False)

            if st.session_state.get("show_diagnostics", False):
                st.info("Gathering database forensics...")
                with corpus.connection() as conn:
                    db_stats = {
                        "roll_calls":     conn.execute("SELECT COUNT(*) FROM roll_calls").fetchone()[0],
                        "leg_votes":      conn.execute("SELECT COUNT(*) FROM legislator_votes").fetchone()[0],
                        "people_total":   conn.execute("SELECT COUNT(*) FROM people").fetchone()[0],
                        "people_unknown": conn.execute("SELECT COUNT(*) FROM people WHERE name LIKE '%Unknown Profile%'").fetchone()[0],
                        "people_mapped":  conn.execute("SELECT COUNT(*) FROM people_mapping").fetchone()[0],
                    }
                    sample = conn.execute("SELECT people_id, name, party FROM people WHERE name LIKE '%Unknown Profile%' LIMIT 10").fetchall()
                st.write(f"**Roll Calls Stored:** {db_stats['roll_calls']:,}")
                st.write(f"**Individual Votes Stored:** {db_stats['leg_votes']:,}")
                st.write(f"**People Profiles Extracted:** {db_stats['people_total']:,}")
                st.write(f"**Profiles Missing Name (ID Only):** {db_stats['people_unknown']:,}")
                st.write(f"**Profiles Mapped to Staff Roster:** {db_stats['people_mapped']:,}")

                st.caption("Recent Failed Name Extracts & Fallbacks:")
                if sample:
                    st.dataframe(pd.DataFrame([dict(s) for s in sample]), use_container_width=True)
                else:
                    st.success("No recent unknowns!")

        st.divider()

        if corpus and staff_manager:
            with st.expander("Review vote identities"):
                identity_query = st.text_input("Find a voter by name", key="vote_identity_query")
                include_linked = st.checkbox("Include previously reviewed identities", key="vote_include_linked")
                identities = {
                    person["people_id"]: person
                    for person in corpus.list_vote_identities(identity_query, include_linked=include_linked)
                }
                if not identities:
                    st.caption("No matching voter records to review.")
                else:
                    person_id = st.selectbox(
                        "Voter record", list(identities), key="vote_identity_person",
                        format_func=lambda key: (
                            f'{identities[key]["name"] or "Unknown name"} | '
                            f'{identities[key]["votes"]} recorded votes | {identities[key]["quality"]} | {key}'
                        ),
                    )
                    person = identities[person_id]
                    st.caption(f'Jurisdictions: {person["jurisdictions"]} | Sessions: {person["sessions"] or "Unknown"}'
                               f' | Chamber: {person["chamber"] or "Unknown"} | District: {person["district"] or "Unknown"}')
                    states = set((person["jurisdictions"] or "").split(","))
                    matching_profiles = {
                        profile["legislator_id"]: profile
                        for profile in staff_manager.get_all_legislators().to_dict("records")
                        if profile.get("state") in states
                    }
                    target = st.selectbox(
                        "Link to directory profile", [None, *matching_profiles], key="vote_identity_target",
                        format_func=lambda key: (
                            f'{matching_profiles[key]["name"]} | {matching_profiles[key]["chamber"]} '
                            f'{matching_profiles[key]["district"]}' if key else "Choose a profile"
                        ),
                    )
                    link, exclude = st.columns(2)
                    should_link = link.button("Verify link", disabled=target is None, key="verify_vote_identity")
                    should_exclude = exclude.button("Keep this voter unlinked", key="exclude_vote_identity")
                    if should_link or should_exclude:
                        try:
                            corpus.review_vote_identity(person_id, matching_profiles[target] if should_link else None)
                        except (ValueError, sqlite3.Error) as error:
                            st.error(f"Vote decision could not be saved: {error}")
                        else:
                            st.toast("Vote identity decision saved")
                            st.rerun()

        st.header("👔 Staff Intelligence")
        live_sources = [{
            "label": "Live Capitol Matrix",
            "type": "google_sheet_live",
            "url": "https://docs.google.com/spreadsheets/d/1gFeGy72R_-FSFrjXbKCAAvVsvNjyV7t_TUvFoB12vys/export?format=xlsx",
            "enabled": True,
            "state": "CA",
            "chamber": "Both"
        }]

        def show_staff_import_result(success, result):
            if not success:
                st.error(f"Staff import rejected: {result}")
                return
            if corpus:
                try:
                    corpus.sync_people_mapping(staff_manager.get_all_legislators())
                except sqlite3.Error as error:
                    st.warning(f"Roster saved. Vote identity mapping needs a retry: {error}")
            st.session_state.sync_warnings = result.get("warnings", [])
            st.success(
                f'Roster saved. {result["staff_created"]:,} office staff records and '
                f'{result["unmatched"]:,} unmatched rows.'
            )

        def staff_workbook(payload, *, preview=False, expected_revision=None):
            with TemporaryDirectory(dir=DATA_DIR) as directory:
                path = Path(directory, "roster.xlsx")
                path.write_bytes(payload)
                return staff_manager.ingest_spreadsheet(
                    path, preview=preview, expected_revision=expected_revision,
                )

        if staff_manager:
            prim = live_sources[0]
            preview_key = f"staff_import_preview:{_username}"
            restore_key = f"staff_restore_preview:{_username}"

            def stage_staff_workbook(payload, label):
                st.session_state.pop(preview_key, None)
                st.session_state.pop(restore_key, None)
                ok, result = staff_workbook(payload, preview=True)
                if ok:
                    st.session_state[preview_key] = (payload, label, result)
                else:
                    st.error(f"Staff import rejected: {result}")

            def present_roster_changes(changes):
                table = pd.DataFrame(changes)
                table["category"] = table["category"].str.replace("_", " ").str.title()
                st.dataframe(table.rename(columns=str.title), hide_index=True)

            st.write(f"Source: **{prim['label']}**")
            if st.button("Preview live staff roster", key="preview_staff_live"):
                try:
                    with st.spinner("Downloading and validating the roster..."):
                        stage_staff_workbook(staff_manager.download_live_workbook(prim["url"]), prim["label"])
                except (OSError, ValueError, requests.RequestException) as error:
                    st.error(f"Staff roster could not be downloaded: {error}")
            upload = st.file_uploader("Or upload a staff workbook (.xlsx)", type=["xlsx"], key="staff_override")
            if upload and st.button("Preview uploaded workbook", key="preview_staff_upload"):
                stage_staff_workbook(upload.getvalue(), upload.name)

            staged = st.session_state.get(preview_key)
            if staged:
                payload, label, result = staged
                st.caption(f"Preview for {label}")
                present_roster_changes(result["changes"])
                st.caption(f'{result["unmatched"]:,} unmatched rows. The live directory is unchanged.')
                apply_column, cancel_column = st.columns(2)
                if apply_column.button("Apply roster import", type="primary", key="apply_staff_import"):
                    ok, imported = staff_workbook(payload, expected_revision=result["revision"])
                    st.session_state.pop(preview_key, None)
                    show_staff_import_result(ok, imported)
                if cancel_column.button("Discard import preview", key="discard_staff_preview"):
                    st.session_state.pop(preview_key, None)
                    st.rerun()

            with st.expander("Restore a roster snapshot"):
                snapshots = staff_manager.get_roster_snapshots()
                if not snapshots:
                    st.caption("A snapshot is saved before any import that changes the roster.")
                else:
                    snapshot_labels = {
                        item["snapshot_id"]: f'{item["created_at"][:19]} UTC | {item["reason"]}'
                        for item in snapshots
                    }
                    selection = st.selectbox("Roster snapshot", list(snapshot_labels), format_func=snapshot_labels.__getitem__)
                    if st.button("Preview snapshot restore", key="preview_staff_restore"):
                        st.session_state.pop(preview_key, None)
                        st.session_state.pop(restore_key, None)
                        try:
                            revision, changes = staff_manager.preview_roster_restore(selection)
                            st.session_state[restore_key] = (selection, revision, changes)
                        except (ValueError, sqlite3.Error) as error:
                            st.error(f"Restore preview failed: {error}")
                    staged_restore = st.session_state.get(restore_key)
                    if staged_restore and staged_restore[0] == selection:
                        present_roster_changes(staged_restore[2])
                        confirmed = st.checkbox("Replace the current roster with this snapshot", key="confirm_staff_restore")
                        if st.button("Restore selected snapshot", disabled=not confirmed, key="apply_staff_restore"):
                            try:
                                staff_manager.restore_roster_snapshot(selection, staged_restore[1])
                            except (ValueError, sqlite3.Error) as error:
                                st.error(f"Roster restore failed: {error}")
                            else:
                                st.success("Roster restored. A snapshot of the prior roster was saved.")
                                if corpus:
                                    try:
                                        corpus.sync_people_mapping(staff_manager.get_all_legislators())
                                    except sqlite3.Error as error:
                                        st.warning(f"Roster restored. Vote mappings need a retry: {error}")
                            finally:
                                st.session_state.pop(restore_key, None)

        st.divider()

        background_sync = os.environ.get("LEGISCAN_BACKGROUND_JOBS", "").strip().lower() in {"1", "true", "yes"}

        def enqueue_sync(job_type, jurisdiction, scope, payload):
            try:
                job_manager.enqueue_job(job_type, jurisdiction, scope, payload, initiated_by=_username)
            except (ValueError, sqlite3.Error) as error:
                st.error(f"Could not queue the sync: {error}")
            else:
                st.success("Sync request queued. Check Recent Jobs for progress.")

        if background_sync:
            st.caption("Sync requests run in the background using the server's LegiScan key.")

        st.header("🔄 Keyword Rescan")
        st.caption("Scans the API for bills mentioning monitored keywords.")
        with st.form("shared_keywords"):
            edited_keywords = st.text_area("Shared keywords, one per line", value="\n".join(keywords_list))
            if st.form_submit_button("Save shared keywords"):
                try:
                    save_keywords(edited_keywords.splitlines())
                except (OSError, ValueError) as error:
                    st.error(f"Could not save shared keywords: {error}")
                else:
                    st.rerun()
        _rescan_states = st.multiselect(
            "States", options=sorted(US_STATES.values()),
            default=["California"], key="rescan_states"
        )
        _rescan_federal = st.checkbox("Include Federal", value=True, key="rescan_federal")

        _running_jobs_for_lock = job_manager.get_running_jobs() if job_manager else []
        _lock_rescan = bool(_running_jobs_for_lock and any(_j['job_type'] == 'keyword_rescan' for _j in _running_jobs_for_lock))
        if st.button("▶️ Run Keyword Rescan", key="run_rescan", disabled=not job_manager or (_lock_rescan and not background_sync)):
            if not _rescan_states and not _rescan_federal:
                st.warning("Select at least one jurisdiction.")
            else:
                if background_sync:
                    codes = {name: code for code, name in US_STATES.items()}
                    states = [codes[state] for state in _rescan_states] + (["US"] if _rescan_federal else [])
                    enqueue_sync("keyword_rescan", ",".join(states), "ALL", {"states": states})
                else:
                    with st.spinner("Scanning bills by keyword…"):
                        try:
                            _sc2nc = {v: k for k, v in US_STATES.items()}
                            _api_states = [_sc2nc.get(s, s) for s in _rescan_states]
                            if _rescan_federal:
                                _api_states.append("US")
                            scan_stats = run_rescan_job(corpus, _api_states, DATA_DIR, job_manager, initiated_by=_username)
                            sync_with_remote()
                            load_data.clear()
                            if scan_stats["api_status"] == "error":
                                st.warning(
                                    f'Rescan had {scan_stats["search_failures"]} search failures and '
                                    f'{scan_stats["detail_failures"]} bill detail failures. '
                                    'Existing records were kept for bills that could not be refreshed.'
                                )
                            else:
                                st.success("Rescan completed!")
                        except Exception as e:
                            st.error(f"Rescan failed: {e}")

        st.divider()
        st.header("📚 Master Corpus Tasks")
        st.caption("Synchronizes entire session bill details to local SQLite.")
        if corpus:
            st.caption("Sync the current session for either jurisdiction or both.")
            ca_selected = st.checkbox("California Legislature", value=True, key="corpus_scope_ca")
            us_selected = st.checkbox("U.S. Congress", value=False, key="corpus_scope_us")
            jurisdictions = [code for code, selected in (("CA", ca_selected), ("US", us_selected)) if selected]
            jurisdiction_names = {"CA": "California", "US": "U.S. Congress"}
            year = datetime.now().year
            corpus_busy = any(job["job_type"] in {"bootstrap_corpus", "incremental_refresh"}
                              for job in _running_jobs_for_lock)
            for code in jurisdictions:
                session = current_session(corpus.get_cached_sessions(code), year)
                label = jurisdiction_names[code]
                if session is None:
                    st.caption(f"{label}: current session will be discovered at sync.")
                else:
                    action = "Refresh updates" if session.get("last_bootstrap") else "First-time bulk import"
                    st.caption(f'{label}: {session["session_name"]} · {action}')
            if st.button("Sync selected jurisdictions", key="sync_corpus_jurisdictions",
                         disabled=not jurisdictions or not job_manager or (corpus_busy and not background_sync)):
                if background_sync:
                    for code in jurisdictions:
                        enqueue_sync("current_session_sync", code, "CURRENT", {"year": year})
                else:
                    progress = st.progress(0, text="Preparing session sync...")
                    outcomes = sync_selected_sessions(
                        corpus, jurisdictions, year, job_manager,
                        lambda fraction, message: progress.progress(min(fraction, 1.0), text=message),
                    )
                    progress.empty()
                    for code, outcome in outcomes.items():
                        label = jurisdiction_names[code]
                        if "error" in outcome:
                            st.error(f'{label}: {outcome["error"]}')
                        else:
                            stats = outcome["stats"]
                            action = "Bulk import" if outcome["importing"] else "Refresh"
                            message = (
                                f'{label} · {outcome["session"]}: {action} '
                                f'({stats["new"]:,} new, {stats["updated"]:,} updated)'
                            )
                            if stats["errors"]:
                                st.warning(f'{message}. Incomplete: {stats["errors"]} errors.')
                            else:
                                st.success(f"{message} completed.")

            with st.expander("Manual session import and refresh"):
                _cached_sessions = corpus.get_cached_sessions()
                _session_opts = {f"{s['jurisdiction']} — {s['session_name']}": s for s in _cached_sessions}

                if st.button("🔍 Discover Target Sessions", key="corpus_discover"):
                    with st.spinner("Discovering..."):
                        corpus.get_active_sessions("CA")
                        corpus.get_active_sessions("US")
                        st.rerun()

                _sel_label   = st.selectbox("Session", options=list(_session_opts.keys()) if _session_opts else ["(none)"], key="corpus_session_select")
                _sel_session = _session_opts.get(_sel_label)

                st.markdown("**Incremental Refresh** (cheap, periodic)")
                _lock_refresh = bool(_running_jobs_for_lock and any(_j['job_type'] == 'incremental_refresh' for _j in _running_jobs_for_lock))
                if _sel_session and _sel_session.get("last_masterlist"):
                    st.caption(f'Last completed refresh: {_sel_session["last_masterlist"]}')
                if st.button("Refresh Session Updates", key="corpus_refresh", disabled=not _sel_session or not job_manager or (_lock_refresh and not background_sync)):
                    if background_sync:
                        enqueue_sync("incremental_refresh", _sel_session["jurisdiction"], str(_sel_session["session_id"]), {})
                    else:
                        progress = st.progress(0, text="Refreshing...")
                        try:
                            results = run_refresh_job(
                                corpus, _sel_session["session_id"], _sel_session["jurisdiction"], job_manager,
                                lambda fraction, message: progress.progress(min(fraction, 1.0), text=message),
                            )
                        except Exception as error:
                            progress.empty()
                            st.error(f"Session refresh failed: {error}")
                        else:
                            progress.empty()
                            if results["errors"]:
                                st.warning(f'Refresh incomplete. {results["errors"]} upstream requests failed. Existing records were kept for failed updates.')
                            else:
                                st.success("Session refresh completed.")

                st.markdown("**Bulk Bootstrap** (expensive, initial load)")
                _lock_boot    = bool(_running_jobs_for_lock and any(_j['job_type'] == 'bootstrap_corpus' for _j in _running_jobs_for_lock))
                _confirm_boot = st.checkbox("I understand Bootstrap takes several minutes", key="confirm_boot")
                if st.button("Import selected session", key="corpus_bootstrap", disabled=not (_sel_session and _confirm_boot and job_manager) or (_lock_boot and not background_sync)):
                    if background_sync:
                        enqueue_sync("bootstrap_corpus", _sel_session["jurisdiction"], str(_sel_session["session_id"]), {})
                    else:
                        progress = st.progress(0, text="Importing session...")
                        try:
                            result = run_bootstrap_job(
                                corpus, _sel_session["session_id"], _sel_session["jurisdiction"], job_manager,
                                lambda fraction, message: progress.progress(min(fraction, 1.0), text=message),
                                allow_individual_fallback=False,
                            )
                        except Exception as error:
                            st.error(f"Session import failed: {error}")
                        else:
                            if result["errors"]:
                                st.warning(f'Import incomplete: {result["errors"]} errors.')
                            else:
                                st.success("Session import completed.")
                        finally:
                            progress.empty()

        st.divider()
        st.header("📋 Recent Jobs Log")
        if job_manager:
            st.button("Refresh job status", key="refresh_sync_jobs")
            recent = job_manager.get_recent_jobs(15)
            if not recent:
                st.caption("No jobs logged yet.")
            for job in recent:
                st.write(f'**{job["status"].title()}** | {job["job_type"].replace("_", " ").title()} | {job["jurisdiction"] or ""}')
                if job["status"] == "RUNNING" and job.get("progress_message"):
                    st.progress(job.get("progress_fraction") or 0.0, text=job["progress_message"])
                if job["error_summary"]:
                    st.caption(job["error_summary"])
                elif job["status"] == "SUCCESS":
                    st.caption(f'{job["new_items"]:,} added · {job["updated_items"]:,} updated')
                if background_sync and job.get("queue_payload") and job["status"] in {"FAILED", "INTERRUPTED"}:
                    if st.button("Retry sync", key=f'retry_sync_{job["job_id"]}'):
                        try:
                            job_manager.retry_queued_job(job["job_id"], initiated_by=_username)
                        except (ValueError, sqlite3.Error) as error:
                            st.error(f"Could not retry the sync: {error}")
                        else:
                            st.toast("Sync request queued")
                            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN CONTENT / WORKSPACE
# ═══════════════════════════════════════════════════════════════════════════════
st.title("🏛️ Unfuck The World Bill Tracker")
# We will invoke run_smart_header later dynamically inside each mode (All Bills, Keyword Matches, Tracked Bills) because we need the final df_count AFTER filters!

st.divider()

def clear_bill_filters():
    for field in (
        "global_jur", "global_status", "global_sponsors", "global_committees",
        "global_date_range", "kw_filter", "tracked_pos", "tracked_prio", "global_search",
        "global_status_input", "tracked_quick",
    ):
        st.session_state.pop(field, None)
        st.session_state.pop(f"{field}_input", None)
    st.session_state.pop("research_bill_id", None)
    st.session_state.pop("research_session_id", None)
    st.session_state.pop("historical_search_page", None)
    st.session_state.pop("tracked_topic_filter", None)
    for prefix in ("ab", "kw", "tr"):
        st.session_state.pop(f"{prefix}_page", None)


if any(view in app_mode for view in ("All Bills", "Keyword Matches", "Tracked Bills")):
    sort_column, reset_column = st.columns([3, 1], vertical_alignment="bottom")
    with sort_column:
        sort_options = _TRACKED_SORT_OPTIONS if "Tracked" in app_mode else _SORT_OPTIONS
        st.session_state.global_sort = st.selectbox("Sort bills by", sort_options, key="global_sort_input")
    with reset_column:
        st.button("Clear filters", on_click=clear_bill_filters, use_container_width=True)


def show_bill_results(frame, bill_notes, tracked_bills, prefix):
    if frame.empty:
        return
    page_size = 50
    total_pages = (len(frame) + page_size - 1) // page_size
    page_key = f"{prefix}_page"
    page = min(st.session_state.get(page_key, 1), total_pages)
    st.session_state[page_key] = page
    start = (page - 1) * page_size
    end = min(start + page_size, len(frame))
    st.caption(f"Bills {start + 1:,}–{end:,} of {len(frame):,}")
    if total_pages > 1:
        back, jump, forward = st.columns([1, 2, 1], vertical_alignment="bottom")
        back.button("Previous page", key=f"{prefix}_prev", disabled=page == 1,
                    on_click=st.session_state.__setitem__, args=(page_key, page - 1))
        jump.number_input(f"Page (of {total_pages})", min_value=1, max_value=total_pages, step=1, key=page_key)
        forward.button("Next page", key=f"{prefix}_next", disabled=page == total_pages,
                       on_click=st.session_state.__setitem__, args=(page_key, page + 1))
    render_bill_page(frame.iloc[start:end], bill_notes, tracked_bills, prefix)


def render_bill_page(page, bill_notes, tracked_bills, prefix):
    """Show compact results and render just the selected bill's controls."""
    page = page.dropna(subset=["bill_id"]).drop_duplicates(subset=["bill_id"])
    if page.empty:
        return

    rows = {str(row["bill_id"]): row for _, row in page.iterrows()}
    table = pd.DataFrame([
        {
            "Bill": row.get("bill_number", bill_id),
            "Session": row.get("session", ""),
            "Title": row.get("title", ""),
            "Status": STATUS_LEGEND.get(str(row.get("status_stage", "")), row.get("status_stage", "")),
            "Jurisdiction": row.get("jurisdiction_name", ""),
            "Last action": row.get("last_action", ""),
            "Action date": row.get("last_action_date", ""),
            "Committee": row.get("committees", ""),
            "Position": bill_notes.get(bill_id, {}).get("position", ""),
            "Priority": bill_notes.get(bill_id, {}).get("priority", ""),
            "My topics": ", ".join(bill_notes.get(bill_id, {}).get("topics", [])),
            "Follow-up": bill_notes.get(bill_id, {}).get("follow_up", ""),
            "Tracked": bill_id in tracked_bills,
        }
        for bill_id, row in rows.items()
    ])
    if "topic_match_reason" in page:
        table["Why it matched"] = page["topic_match_reason"].tolist()
    st.dataframe(
        table, hide_index=True, height=min(430, 38 + 35 * len(table)),
        column_config={"Tracked": st.column_config.CheckboxColumn(), "Title": st.column_config.TextColumn(width="large")},
    )
    selected = st.selectbox(
        "Open a bill",
        list(rows),
        format_func=lambda bill_id: (
            f'{rows[bill_id].get("bill_number", bill_id)} | '
            f'{rows[bill_id].get("session", "")} | {rows[bill_id].get("title", "")}'
        ),
        key=f"{prefix}_selected_bill",
    )
    if selected is not None:
        _render_bill_card(
            rows[selected], bill_notes.get(selected, {}), selected,
            bill_notes, tracked_bills, key_prefix=f"{prefix}_{selected}",
        )


def find_topic_matches(topic, corpus, keyword_snapshot, seen=()):
    matches = corpus.search_bills(
        topic_terms=topic.get("terms"), topic_required=topic.get("required"), topic_excluded=topic.get("excluded"),
        excluded_bill_ids=seen, limit=500,
    ) if corpus else pd.DataFrame()
    if not keyword_snapshot.empty and "bill_id" in keyword_snapshot:
        cached = keyword_snapshot.dropna(subset=["bill_id"]).copy()
        cached["bill_id"] = cached["bill_id"].astype(str)
        cached = cached.loc[~cached["bill_id"].isin(seen)]
        if not cached.empty:
            explanations = cached.apply(lambda row: topic_match_reason(row, topic), axis=1)
            cached = cached.loc[explanations.ne("")]
            if corpus and not cached.empty:
                cached = cached.loc[~cached["bill_id"].isin(corpus.indexed_bill_ids(cached["bill_id"]))]
        matches = pd.concat([matches, cached], ignore_index=True)
    if matches.empty:
        return matches
    matches = matches.dropna(subset=["bill_id"]).copy()
    matches["bill_id"] = matches["bill_id"].astype(str)
    matches = matches.drop_duplicates("bill_id")
    return matches.assign(topic_match_reason=matches.apply(lambda row: topic_match_reason(row, topic), axis=1))


def topic_rule_fields(prefix, topic=None):
    current = topic or {}
    fields = (
        ("Search terms", "terms", "Match at least one of these phrases."),
        ("Required phrases", "required", "Each phrase must appear somewhere in the bill record."),
        ("Excluded phrases", "excluded", "Drop a bill if any of these phrases appear."),
    )
    return [
        st.text_input(label, value=", ".join(current.get(field, [])), key=f"{prefix}_{field}",
                      help=f"{tip} Separate phrases with commas.")
        for label, field, tip in fields
    ]


def update_review_item(item, *, snooze_until=None):
    bill_id = str(item["bill"]["bill_id"])
    path = REVIEW_QUEUE_FILE

    def save_queue_state(current):
        states = current.setdefault("items", {})
        states[item["key"]] = (
            {"snoozed_until": snooze_until.isoformat()} if snooze_until else {"reviewed": True}
        )

    def review_topic(current):
        entry = current.get(item["topic"])
        if entry is None:
            raise ValueError("This topic was removed in another session.")
        if bill_id not in entry["seen"]:
            entry["seen"].append(bill_id)

    try:
        if snooze_until is None and item["kind"] == "Bill changed":
            path = NOTES_FILE
            update_bill_note(path, bill_id, bill_notes.get(bill_id, {}), {
                "reviewed_revision": bill_revision(item["bill"]),
                "last_reviewed": datetime.now(timezone.utc).isoformat(),
            }, actor=_username)
        elif snooze_until is None and item["kind"] == "Topic match":
            path = TOPICS_FILE
            update_workspace(path, {}, review_topic)
        else:
            update_workspace(path, {}, save_queue_state)
    except (OSError, ValueError, sqlite3.Error) as error:
        st.error(f"Review queue could not be updated: {error}")
    else:
        publish_bill_workspace(path)
        st.rerun()


def service_term_label(term):
    end = term["end"] or "End year unavailable"
    congress = f' | Congress {term["congress"]}' if term["congress"] else ""
    return f'{term["chamber"]} | {term["start"]} to {end}{congress} | {term["precision"]}'


def render_service_terms(manager, key, jurisdiction):
    terms = manager.get_service_terms(key)
    with st.expander("Sourced service terms"):
        if terms:
            for term in terms:
                st.link_button(service_term_label(term), term["source_url"])
        else:
            st.caption("No sourced terms have been saved for this profile.")
        if jurisdiction == "CA":
            st.link_button("Find official California legislative records", CALIFORNIA_SOURCE)
        if _auth.is_admin():
            chambers = ["Assembly", "Senate"] if jurisdiction == "CA" else ["House of Representatives", "Senate"]
            with st.form(f"service_dates:{key}"):
                chamber = st.selectbox("Chamber", chambers)
                a, b = st.columns(2)
                boundaries = {"min_value": date(1789, 1, 1), "max_value": date(date.today().year + 10, 12, 31)}
                start = a.date_input("Verified start date", value=None, **boundaries)
                end = b.date_input("Verified end date", value=None, **boundaries)
                source = st.text_input("Official source URL")
                if st.form_submit_button("Save exact term"):
                    try:
                        manager.save_exact_service_term(key, jurisdiction, chamber, start, end, source)
                    except (ValueError, sqlite3.Error) as error:
                        st.error(f"Could not save the term: {error}")
                    else:
                        st.toast("Sourced term saved")
                        st.rerun()
            if terms:
                records = {term["record_key"]: term for term in terms}
                selected = st.selectbox("Remove a term record", [None, *records],
                                        format_func=lambda value: service_term_label(records[value]) if value else "Choose a record",
                                        key=f"remove_term:{key}")
                if st.button("Remove selected term", key=f"remove_term_button:{key}", disabled=selected is None):
                    manager.delete_service_term(key, selected)
                    st.rerun()
    return terms


def filter_votes_by_service_term(votes, terms, key):
    if not terms or not votes:
        return votes
    labels = {term["record_key"]: term for term in terms}
    term_key = st.selectbox(
        "Service term", [None, *labels],
        format_func=lambda value: service_term_label(labels[value]) if value else "All vote records",
        key=f"service_vote_filter:{key}",
    )
    if not term_key:
        return votes
    evidence = labels[term_key]
    expected = "Within exact dates" if evidence["precision"] == "Exact dates" else "Consistent with year-only record"
    matched = [vote for vote in votes if vote_service_status(vote, [evidence]) == expected]
    st.caption(f"{len(matched):,} of {len(votes):,} vote records match this term's source precision.")
    return matched


def open_profile_bill(bill_id, number):
    st.session_state["research_bill_id"] = str(bill_id)
    st.session_state["global_search_input"] = number
    st.session_state["app_mode"] = "🔍 All Bills"


# Processing helper function
def run_unified_filters(work_df):
    if work_df.empty: return work_df
    
    # Jurisdiction filter
    if st.session_state.global_jur:
        if 'jurisdiction_name' in work_df.columns:
            work_df = work_df[work_df['jurisdiction_name'].isin(st.session_state.global_jur)]
            
    # Status Phase
    if st.session_state.get("global_status"):
        if 'status_stage' in work_df.columns:
            work_df = work_df[work_df['status_stage'].astype(str).isin(st.session_state.global_status)]
            
    # Sponsors & Committees
    if st.session_state.global_sponsors and 'sponsors' in work_df.columns:
        selected = set(st.session_state.global_sponsors)
        work_df = work_df[work_df["sponsors"].fillna("").map(
            lambda value: value in selected or bool(selected.intersection(name.strip() for name in value.split(",")))
        )]
    if st.session_state.global_committees and 'committees' in work_df.columns:
        work_df = work_df[work_df['committees'].isin(st.session_state.global_committees)]
        
    # Dates
    if st.session_state.get("global_date_range") and len(st.session_state.global_date_range) == 2 and 'status_date' in work_df.columns:
        work_df['_sd'] = pd.to_datetime(work_df['status_date'], errors='coerce')
        work_df = work_df[
            (work_df['_sd'] >= pd.to_datetime(st.session_state.global_date_range[0]))
            & (work_df['_sd'] <= pd.to_datetime(st.session_state.global_date_range[1]))
        ].drop(columns=['_sd'])
        
    # Advanced: Keyword categories
    if st.session_state.global_search:
        work_df = search_df(work_df, st.session_state.global_search)
        
    return work_df


# ────────── ALL BILLS ─────────────────────────────────────────────────────────
if app_mode == "Dashboard":
    def open_view(choice):
        st.session_state.app_mode = choice

    render_dashboard(len(tracked_bills), len(load_bill_workspace(TOPICS_FILE, [])), view_modes, open_view)
elif "All Bills" in app_mode:
    if not _CORPUS_AVAILABLE or not corpus:
        st.warning("Master Corpus SQLite not responding.")
    else:
        try:
            sessions = corpus.get_cached_sessions()
            session_labels = {
                item["session_id"]: f'{item["jurisdiction"]} | {item["session_name"]}'
                for item in sessions
            }
            session_id = st.selectbox(
                "Legislative session", [None, *session_labels],
                format_func=lambda value: session_labels.get(value, "All indexed sessions"),
                key="research_session_id",
            )
            focus_bill = st.session_state.get("research_bill_id")
            if focus_bill:
                st.button("Back to session search", on_click=st.session_state.pop, args=("research_bill_id", None))
                bills = corpus.get_bills_by_ids([focus_bill])
                if bills.empty and "bill_id" in df.columns:
                    bills = df[df["bill_id"].astype(str) == focus_bill]
                render_bill_page(bills, bill_notes, tracked_bills, "ab")
            else:
                positions = st.session_state.tracked_pos
                priorities = st.session_state.tracked_prio
                note_ids = None
                if positions or priorities:
                    note_ids = [
                        bill for bill, note in bill_notes.items()
                        if (not positions or note.get("position") in positions)
                        and (not priorities or note.get("priority") in priorities)
                    ]
                filters = dict(
                    session_id=session_id,
                    query=st.session_state.global_search,
                    jurisdiction_filter=st.session_state.global_jur,
                    status_filter=st.session_state.global_status,
                    keyword_filter=st.session_state.kw_filter,
                    date_range=st.session_state.global_date_range,
                    sponsors=st.session_state.global_sponsors,
                    committees=st.session_state.global_committees,
                    bill_ids=note_ids,
                    sort=st.session_state.global_sort,
                )
                total = corpus.search_bills(**filters, count_only=True)
                run_smart_header(total, "All Bills", corpus, tracked_bills)
                if total:
                    page_size = 50
                    page_count = (total + page_size - 1) // page_size
                    page_key = "historical_search_page"
                    st.session_state[page_key] = min(st.session_state.get(page_key, 1), page_count)
                    page = st.number_input("Results page", min_value=1, max_value=page_count, step=1, key=page_key)
                    offset = (page - 1) * page_size
                    bills = corpus.search_bills(**filters, limit=page_size, offset=offset)
                    st.caption(f"Bills {offset + 1:,}–{offset + len(bills):,} of {total:,}")
                    render_bill_page(bills, bill_notes, tracked_bills, "ab")
                    if st.button("Prepare full search export", key="prepare_historical_export"):
                        matching = corpus.search_bills(**filters, limit=None)
                        download = build_export_df(matching, bill_notes, tracked_bills).to_csv(index=False)
                        st.download_button("Download matching bills", download, "all_bills_search.csv", "text/csv")
        except Exception as error:
            st.error(f"Search could not be completed: {error}")


elif app_mode == "Email Notifications" and not _auth.is_guest():
    render_email_notifications(_username, _current_user)


elif app_mode == "Hearing Calendar":
    render_hearing_calendar(
        corpus, tracked_bills, job_manager, _username,
        is_admin=_auth.is_admin(), has_personal_key=bool(_current_user.get("api_key")),
        load_tracked_rows=lambda ids: get_tracked_bills_df(ids, corpus, df),
        open_bill=open_profile_bill,
    )


elif app_mode == "Historical Coverage":
    render_historical_coverage(corpus)


# ────────── KEYWORD MATCHES ───────────────────────────────────────────────────
elif "Keyword Matches" in app_mode:
    if df.empty:
        st.warning("Keyword cache empty. Run a rescan.")
    else:
        kw_df = df.copy()
        kw_df = run_unified_filters(kw_df)
        
        if st.session_state.kw_filter and 'keyword' in kw_df.columns:
            kw_df = filter_bill_keywords(kw_df, st.session_state.kw_filter)
        if st.session_state.tracked_pos:
            kw_df = kw_df[kw_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('position', '') in st.session_state.tracked_pos)]
        if st.session_state.tracked_prio:
            kw_df = kw_df[kw_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('priority', '') in st.session_state.tracked_prio)]
            
        kw_df = apply_sort(kw_df, st.session_state.global_sort)
        run_smart_header(len(kw_df), "Keyword Matches", corpus, tracked_bills)
        
        show_bill_results(kw_df, bill_notes, tracked_bills, "kw")
                
        if not kw_df.empty:
            st.download_button("📥 Export Matches", build_export_df(kw_df, bill_notes, tracked_bills).to_csv(index=False), "match_search.csv", "text/csv")


# ────────── TRACKED BILLS ─────────────────────────────────────────────────────
elif app_mode == "Daily Review":
    st.subheader("Daily Review")
    topics = load_bill_workspace(TOPICS_FILE, {})
    tracked = get_tracked_bills_df(tracked_bills, corpus, df)
    by_topic = [
        (key, topic, find_topic_matches(topic, corpus, df, topic["seen"]).to_dict("records"))
        for key, topic in topics.items()
    ]
    today = date.today()
    schedules = corpus.get_bill_calendars(tracked_bills) if corpus else {}
    upcoming = [
        event for calendar in schedules.values() for event in calendar["events"]
        if today.isoformat() <= event["date"] <= (today + timedelta(days=6)).isoformat()
    ]
    queue = build_review_queue(tracked.to_dict("records"), bill_notes, by_topic, queue_preferences, today, upcoming)
    if not queue:
        st.info("You're caught up. Save topics in Topic Inbox or set follow-ups on tracked bills to see them here.")
    else:
        counts = {kind: sum(item["kind"] == kind for item in queue)
                  for kind in ("Follow-up", "Upcoming event", "Bill changed", "Topic match")}
        st.caption(" · ".join(f"{count:,} {kind.lower()}" for kind, count in counts.items() if count))
        category = st.selectbox("Show", ["All items", *counts], key="review_queue_category")
        visible = [item for item in queue if category == "All items" or item["kind"] == category]
        if not visible:
            st.info("No items in this category.")
        else:
            page_count = (len(visible) + 49) // 50
            page_key = "review_queue_page"
            st.session_state[page_key] = min(st.session_state.get(page_key, 1), page_count)
            page = st.number_input("Queue page", min_value=1, max_value=page_count, step=1, key=page_key)
            start = (page - 1) * 50
            batch = visible[start:start + 50]
            st.caption(f"Items {start + 1:,}–{start + len(batch):,} of {len(visible):,}")
            table = pd.DataFrame([{
                "Type": item["kind"],
                "Bill": item["bill"].get("bill_number", ""),
                "Session": item["bill"].get("session", ""),
                "Title": item["bill"].get("title", ""),
                "Needs review": item["detail"],
            } for item in batch])
            st.dataframe(table, hide_index=True)
            by_key = {item["key"]: item for item in batch}

            def item_label(key):
                item = by_key[key]
                bill = item["bill"]
                return f'{item["kind"]} | {bill.get("bill_number", "")} | {bill.get("session", "")} | {item["detail"]}'

            selected = st.selectbox("Review item", list(by_key), format_func=item_label, key="review_queue_item")
            item = by_key[selected]
            review, snooze_choice, snooze = st.columns([2, 2, 1], vertical_alignment="bottom")
            if review.button("Mark reviewed", key="queue_mark_reviewed"):
                update_review_item(item)
            days = snooze_choice.selectbox("Snooze for", [1, 3, 7],
                                            format_func=lambda count: f"{count} day" if count == 1 else f"{count} days")
            if snooze.button("Snooze", key="queue_snooze"):
                update_review_item(item, snooze_until=today + timedelta(days=days))
            bill = item["bill"]
            bill_id = str(bill["bill_id"])
            _render_bill_card(bill, bill_notes.get(bill_id, {}), bill_id,
                              bill_notes, tracked_bills, key_prefix=f"daily_{bill_id}")


elif app_mode == "Topic Inbox":
    st.subheader("Topic Inbox")
    topics = load_bill_workspace(TOPICS_FILE, {})
    with st.form("new_research_topic", clear_on_submit=True):
        name = st.text_input("Topic name")
        inputs = topic_rule_fields("new_topic")
        if st.form_submit_button("Save topic"):
            name = name.strip()
            try:
                rules = parse_topic_rules(*inputs)
                if not name:
                    raise ValueError("Enter a topic name.")

                def add_topic(current):
                    if name.casefold() in current:
                        raise ValueError("A topic with that name already exists.")
                    current[name.casefold()] = {"name": name, **rules, "seen": []}

                update_workspace(TOPICS_FILE, {}, add_topic)
            except (OSError, ValueError, sqlite3.Error) as error:
                st.error(f"Topic could not be saved: {error}")
            else:
                publish_bill_workspace(TOPICS_FILE)
                st.rerun()
    if topics:
        topic_id = st.selectbox("Saved topic", sorted(topics), format_func=lambda key: topics[key]["name"],
                                key="topic_inbox_selected")
        topic = topics[topic_id]
        rule_summary = [
            f'{label} {", ".join(topic[field])}'
            for label, field in (("Any", "terms"), ("Require", "required"), ("Exclude", "excluded"))
            if topic.get(field)
        ]
        st.caption("Title, summary and subjects | " + " | ".join(rule_summary))
        show_all = st.checkbox("Include reviewed matches", key="topic_include_seen")
        matches = find_topic_matches(topic, corpus, df, () if show_all else topic["seen"])
        if not matches.empty:
            seen = set(topic["seen"])
            new = matches[~matches["bill_id"].astype(str).isin(seen)]
            st.caption(f"{len(new):,} unseen in {len(matches):,} loaded matches")
            if len(matches) >= 500:
                st.caption("Corpus search shows up to 500 matches. Narrow the terms to review a smaller topic.")
            show_bill_results(matches if show_all else new, bill_notes, tracked_bills, "topic")
            if not new.empty and st.button("Mark loaded matches reviewed", key="topic_mark_seen"):
                def mark_seen(current):
                    if topic_id not in current:
                        raise ValueError("This topic was removed in another session.")
                    entry = current[topic_id]
                    entry["seen"] = list(dict.fromkeys([*entry["seen"], *new["bill_id"].astype(str)]))
                try:
                    update_workspace(TOPICS_FILE, {}, mark_seen)
                except (OSError, ValueError, sqlite3.Error) as error:
                    st.error(f"Review could not be saved: {error}")
                else:
                    publish_bill_workspace(TOPICS_FILE)
                    st.rerun()
        else:
            st.info("No indexed bills match this topic.")
        with st.expander("Manage topic"):
            with st.form(f"edit_topic_{topic_id}"):
                edits = topic_rule_fields(f"edit_topic_{topic_id}", topic)
                if st.form_submit_button("Update matching rules"):
                    try:
                        rules = parse_topic_rules(*edits)

                        def update_rules(current):
                            if topic_id not in current:
                                raise ValueError("This topic was removed in another session.")
                            current[topic_id].update(rules)

                        update_workspace(TOPICS_FILE, {}, update_rules)
                    except (OSError, ValueError, sqlite3.Error) as error:
                        st.error(f"Topic could not be updated: {error}")
                    else:
                        publish_bill_workspace(TOPICS_FILE)
                        st.rerun()
            if st.button("Delete saved topic", key="delete_research_topic"):
                try:
                    update_workspace(TOPICS_FILE, {}, lambda current: current.pop(topic_id, None))
                except (OSError, ValueError, sqlite3.Error) as error:
                    st.error(f"Topic could not be deleted: {error}")
                else:
                    publish_bill_workspace(TOPICS_FILE)
                    st.rerun()


elif "Tracked Bills" in app_mode:
    legacy_keys = sorted({
        str(key) for key in [*tracked_bills, *bill_notes]
        if not str(key).isascii() or not str(key).isdecimal()
    })
    if legacy_keys:
        with st.expander("Legacy entries to review"):
            st.caption("These entries use bill labels. Verify their session before applying them to a bill.")
            for legacy_key in legacy_keys:
                st.text(legacy_key)
                if legacy_key in bill_notes:
                    st.json(bill_notes[legacy_key], expanded=False)

    if not tracked_bills:
        st.info("No tracked bills. Flag some bills from the repository first.")
    else:
        tracked_archive = get_tracked_bills_df(tracked_bills, corpus, df)
        tr_df = run_unified_filters(tracked_archive.copy())
        topics_by_bill = {
            bill: {topic.casefold(): topic for topic in bill_notes.get(bill, {}).get("topics", [])}
            for bill in tracked_bills
        }
        topic_labels = {key: label for topics in topics_by_bill.values() for key, label in topics.items()}
        if topic_labels:
            topic_filter = st.multiselect(
                "My topics", sorted(topic_labels), format_func=topic_labels.__getitem__, key="tracked_topic_filter",
            )
            if topic_filter and not tr_df.empty:
                tr_df = tr_df[tr_df["bill_id"].astype(str).map(
                    lambda bill: bool(set(topic_filter).intersection(topics_by_bill.get(bill, {})))
                )]

        if len(tracked_archive) >= 2:
            comparison_rows = {
                str(row["bill_id"]): row for _, row in tracked_archive.drop_duplicates("bill_id").iterrows()
            }
            with st.expander("Compare tracked bills"):
                selected = st.multiselect(
                    "Select up to four bills", list(comparison_rows), max_selections=4, key="compare_tracked",
                    format_func=lambda bill: (
                        f'{comparison_rows[bill].get("bill_number", bill)} | '
                        f'{comparison_rows[bill].get("session", "")} | {comparison_rows[bill].get("title", "")}'
                    ),
                )
                if len(selected) >= 2:
                    comparison = {}
                    for bill in selected:
                        row = comparison_rows[bill]
                        note = bill_notes.get(bill, {})
                        comparison[f'{row.get("bill_number", bill)} | {row.get("session", "")} | {bill}'] = {
                            "Title": row.get("title", ""),
                            "Jurisdiction": row.get("jurisdiction_name", ""),
                            "Status": STATUS_LEGEND.get(str(row.get("status_stage")), str(row.get("status_stage", ""))),
                            "Committee": row.get("committees", ""),
                            "Sponsors": row.get("sponsors", ""),
                            "Last action": row.get("last_action", ""),
                            "Last action date": row.get("last_action_date", ""),
                            "Summary": row.get("description", ""),
                            "My topics": ", ".join(note.get("topics", [])),
                            "Position": note.get("position", ""),
                            "Priority": note.get("priority", ""),
                            "Follow-up": note.get("follow_up", ""),
                        }
                        if isinstance(row.get("url"), str) and row["url"].startswith(("https://", "http://")):
                            st.link_button(f'Open source for {row.get("bill_number", bill)} | {row.get("session", "")}', row["url"])
                    st.dataframe(pd.DataFrame(comparison).fillna(""))
        
        # Apply standard advanced filters
        if st.session_state.kw_filter and 'keyword' in tr_df.columns:
            tr_df = filter_bill_keywords(tr_df, st.session_state.kw_filter)
        if st.session_state.tracked_pos:
            tr_df = tr_df[tr_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('position', '') in st.session_state.tracked_pos)]
        if st.session_state.tracked_prio:
            tr_df = tr_df[tr_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('priority', '') in st.session_state.tracked_prio)]
            
        # Apply Tracked-Specific Quick filter
        _qf = st.session_state.get("tracked_quick", "All Tracked")
        if not tr_df.empty and _qf != "All Tracked":
            if _qf == "Has Notes":
                tr_df = tr_df[tr_df['bill_id'].astype(str).apply(lambda x: bool(bill_notes.get(x, {}).get('comment', '').strip()))]
            elif _qf == "No Notes":
                tr_df = tr_df[tr_df['bill_id'].astype(str).apply(lambda x: not bool(bill_notes.get(x, {}).get('comment', '').strip()))]
            elif _qf == "High Priority":
                tr_df = tr_df[tr_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('priority', '') == 'High')]
            elif _qf == "Needs Attention":
                from datetime import datetime, timedelta, timezone
                now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                week_ago = now_utc - timedelta(days=7)
                def needs_attn(row):
                    b = str(row.get('bill_id'))
                    note = bill_notes.get(b, {})
                    no_note = not bool(note.get('comment', '').strip())
                    no_pos = note.get('position', '') in ('', 'No Position')
                    is_high = note.get('priority', '') == 'High'
                    recent_action = False
                    ad = row.get('last_action_date', '') or row.get('status_date', '')
                    if ad:
                        try:
                            dt = datetime.strptime(str(ad).split()[0], '%Y-%m-%d').replace(tzinfo=timezone.utc)
                            if dt > week_ago: recent_action = True
                        except: pass
                    return no_note or no_pos or is_high or recent_action
                tr_df = tr_df[tr_df.apply(needs_attn, axis=1)]
            elif _qf == "Changes Since Review":
                tr_df = tr_df[tr_df.apply(
                    lambda row: bool(changed_fields(
                        bill_notes.get(str(row["bill_id"]), {}).get("reviewed_revision"), bill_revision(row),
                    )), axis=1,
                )]
            elif _qf == "Recently Updated":
                from datetime import datetime, timedelta, timezone
                now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
                week_ago = now_utc - timedelta(days=7)
                def is_recent(b):
                    lr = bill_notes.get(b, {}).get('last_reviewed', '')
                    if not lr: return False
                    try:
                        dt = datetime.fromisoformat(lr).replace(tzinfo=timezone.utc)
                        return dt > week_ago
                    except: return False
                tr_df = tr_df[tr_df['bill_id'].astype(str).apply(is_recent)]
                
        if not tr_df.empty:
            note_ids = tr_df["bill_id"].astype(str)
            tr_df = tr_df.assign(
                priority=note_ids.map(lambda bill: bill_notes.get(bill, {}).get("priority", "")),
                last_reviewed=note_ids.map(lambda bill: bill_notes.get(bill, {}).get("last_reviewed", "")),
                follow_up=note_ids.map(lambda bill: bill_notes.get(bill, {}).get("follow_up", "")),
            )
        tr_df = apply_sort(tr_df, st.session_state.global_sort)
        run_smart_header(len(tr_df), "Tracked Bills", corpus, tracked_bills)
        
        show_bill_results(tr_df, bill_notes, tracked_bills, "tr")
            
        st.divider()
        if not tracked_archive.empty:
            st.download_button("Export Full Tracked Archive", build_export_df(tracked_archive, bill_notes, tracked_bills).to_csv(index=False), "tracked_archive.csv", "text/csv")
            
        with st.expander("🗑️ Bulk Remove Tracked Bills"):
            _trm = st.multiselect("Select items", [item for item in tracked_bills if str(item) not in legacy_keys])
            if st.button("Delete Tracking", type="primary") and _trm:
                if change_tracking(remove=_trm):
                    st.rerun()

# ────────── STAFF PIPELINE DIAGNOSTICS ──────────────────────────────────────────────
elif "Staff Analytics" in app_mode:
    if not staff_manager:
        st.warning("Staff Intelligence Module offline.")
    else:
        from staff_diagnostics import render_staff_diagnostics
        render_staff_diagnostics(staff_manager)

# ────────── LEGISLATOR DIRECTORY ──────────────────────────────────────────────
elif app_mode == "Federal Voting Profiles":
    st.subheader("Federal Voting Profiles")
    if not corpus or not staff_manager:
        st.info("The corpus and profile databases are required for this view.")
    else:
        name_query = st.text_input("Find a federal voter", key="federal_voter_query")
        candidates = corpus.list_vote_identities(name_query.strip(), include_linked=True, jurisdiction="US")
        if not candidates:
            st.info("No federal voters found. Import a congressional session with individual votes to populate this view.")
        else:
            people = {row["people_id"]: row for row in candidates}
            person_id = st.selectbox(
                "Voter profile", list(people), key="federal_voter",
                format_func=lambda key: f'{people[key]["name"] or "Unknown name"} | {people[key]["sessions"] or "Unknown sessions"} | {key}',
            )
            profile_key = f"federal:{person_id}"
            person = people[person_id]
            st.caption(f'LegiScan voter {person_id} | {person["votes"]:,} recorded federal roll calls')
            if not name_query.strip() and len(candidates) == 100:
                st.caption("Search by name to find additional voters beyond the first 100.")
            if _auth.is_admin():
                with st.expander("Link official Congress.gov terms"):
                    st.link_button("Find the member's Bioguide ID", "https://www.congress.gov/help/field-values/member-bioguide-ids")
                    identifier = st.text_input("Bioguide ID", key=f"congress_bioguide:{profile_key}")
                    preview_key = f"congress_preview:{_username}:{profile_key}"
                    if st.button("Preview official terms", key=f"congress_fetch:{profile_key}"):
                        st.session_state.pop(preview_key, None)
                        try:
                            imported = fetch_congress_member_terms(identifier, os.environ.get("CONGRESS_API_KEY", ""))
                        except (ValueError, RuntimeError) as error:
                            st.error(str(error))
                        else:
                            st.session_state[preview_key] = imported
                    staged = st.session_state.get(preview_key)
                    if staged and staged["bioguide"] == identifier.strip().upper():
                        st.write(f'Congress.gov: {staged["name"]} | {staged["bioguide"]}')
                        st.dataframe(pd.DataFrame(staged["terms"])[["congress", "chamber", "start", "end"]], hide_index=True)
                        st.caption("These records contain years. Compare the official name and chamber with the LegiScan voter before linking.")
                        confirmed = st.checkbox(
                            "I verified that these are the same person",
                            key=f"confirm_congress_identity:{profile_key}:{staged['bioguide']}",
                        )
                        if st.button("Link and save terms", key=f"save_congress:{profile_key}", disabled=not confirmed):
                            try:
                                staff_manager.save_congress_service_terms(profile_key, staged)
                            except (ValueError, sqlite3.Error) as error:
                                st.error(f"Could not link federal terms: {error}")
                            else:
                                st.session_state.pop(preview_key, None)
                                st.toast("Official term record linked")
                                st.rerun()
            terms = render_service_terms(staff_manager, profile_key, "US")
            votes = filter_votes_by_service_term(corpus.get_federal_member_votes(person_id), terms, profile_key)
            if not votes:
                st.info("No vote records match this filter.")
            else:
                table = pd.DataFrame([
                    {"Date": vote["vote_date"], "Session": vote["session"], "Bill": vote["bill_number"],
                     "Title": vote["title"], "Vote": vote["vote_text"], "Motion": vote["motion"],
                     "Term evidence": vote_service_status(vote, terms) if terms else "No sourced term"}
                    for vote in votes
                ])
                st.dataframe(table, hide_index=True)
                bills = {vote["bill_id"]: vote for vote in votes}
                selected_bill = st.selectbox(
                    "Bill to open", list(bills), key=f"federal_vote_bill:{profile_key}",
                    format_func=lambda key: f'{bills[key]["bill_number"]} | {bills[key]["session"]} | {bills[key]["title"]}',
                )
                st.button("Open bill", key=f"open_federal_vote:{profile_key}", on_click=open_profile_bill,
                          args=(selected_bill, bills[selected_bill]["bill_number"]))


elif "Legislator Directory" in app_mode:
    if not staff_manager:
        st.warning("Staff Intelligence Module offline.")
    else:
        st.header("👔 Legislator Staff & Issue Hub")
        try:
            leg_df = staff_manager.get_all_legislators()
        except:
            leg_df = pd.DataFrame()

        if not st.session_state.get("active_profile") and not st.session_state.get("active_staff_profile"):
            render_profile_note_recovery(leg_df, staff_manager)

        # ── Helper: jump to bill view ────────────────────────────────────────────
        def _go_to_bill(bill_number, bill_id=None):
            st.session_state["global_search"] = bill_number
            st.session_state["global_search_input"] = bill_number
            st.session_state["app_mode"] = "🔍 All Bills"
            st.session_state.pop("research_bill_id", None)
            if str(bill_id).isascii() and str(bill_id).isdecimal():
                st.session_state["research_bill_id"] = str(bill_id)
            st.session_state.pop("active_profile", None)
            st.session_state.pop("active_staff_profile", None)

        # ── Helper: render inline vote table ────────────────────────────────────
        def _render_vote_row(v: dict, _idx: int = 0):
            vote_icon = {"Yea": "🟢", "Nay": "🔴", "NV": "⬜", "Absent": "⬛"}.get(v.get("vote_text", ""), "❓")
            bn = v.get("bill_number", "")
            col_a, col_b, col_c, col_d = st.columns([2, 1, 4, 1])
            col_a.markdown(f"**{(v.get('vote_date') or '')[:10]}**")
            col_b.markdown(f"{vote_icon} {v.get('vote_text', '')}")
            col_c.markdown(f"{bn} — {(v.get('motion') or '')[:60]}")
            if bn:
                col_d.button("Open bill", key=f"votelink_{_idx}_{bn}", on_click=_go_to_bill, args=(bn, v.get("bill_id")))

        # ── Staff profile sub-view ───────────────────────────────────────────────
        if st.session_state.get("active_staff_profile"):
            sp = st.session_state.active_staff_profile   # dict: {staff_id, name, role, email, legislator_id}
            back_leg = st.session_state.get("active_profile")

            def _back_from_staff():
                st.session_state.pop("active_staff_profile", None)
            st.button("⬅️ Back to Legislator Profile" if back_leg else "⬅️ Back to Directory",
                      on_click=_back_from_staff)
            st.divider()

            st.subheader(f"🧑‍💼 {sp.get('name', 'Staffer')}")
            st.caption(f"**Role:** {sp.get('role','').replace('_',' ').title()}  |  **Email:** {sp.get('email') or '—'}")

            # Find all legislators this person works for
            st.markdown("#### 🏛️ Works For")
            try:
                all_leg_staff = []
                for _, lrow in leg_df.iterrows():
                    for s in staff_manager.get_legislator_staff(lrow["legislator_id"]):
                        if s["name"].lower() == sp.get("name", "").lower():
                            all_leg_staff.append({
                                "Legislator": lrow.get("name"),
                                "Chamber": lrow.get("chamber"),
                                "District": lrow.get("district"),
                                "Role": s["role"].replace("_", " ").title(),
                            })
                if all_leg_staff:
                    st.dataframe(pd.DataFrame(all_leg_staff), hide_index=True, use_container_width=True)
                else:
                    st.caption("No legislator cross-references found.")
            except Exception as _se:
                st.caption(f"Could not load cross-references: {_se}")

            # Issue areas for this staffer
            st.markdown("#### 📋 Issue Assignments")
            try:
                all_issues = []
                with staff_manager._get_conn() as sc:
                    sc.row_factory = __import__("sqlite3").Row
                    issue_rows = sc.execute(
                        "SELECT lia.issue_area, l.name as legislator FROM legislator_issue_assignments lia "
                        "JOIN legislators l ON lia.legislator_id = l.legislator_id "
                        "WHERE LOWER(lia.staff_name) LIKE ?",
                        (f"%{sp.get('name','').lower()}%",)
                    ).fetchall()
                    all_issues = [dict(r) for r in issue_rows]
                if all_issues:
                    st.dataframe(pd.DataFrame(all_issues).rename(columns={"issue_area": "Issue Area", "legislator": "Legislator"}),
                                 hide_index=True, use_container_width=True)
                else:
                    st.caption("No issue areas assigned.")
            except Exception as _ie:
                st.caption(f"Could not load issue assignments: {_ie}")

            # Committee work
            st.markdown("#### 🏛️ Committee Involvement")
            try:
                with staff_manager._get_conn() as sc:
                    sc.row_factory = __import__("sqlite3").Row
                    cmte_rows = sc.execute(
                        "SELECT committee_name, chamber, role FROM committee_staff WHERE LOWER(staff_name) LIKE ?",
                        (f"%{sp.get('name','').lower()}%",)
                    ).fetchall()
                if cmte_rows:
                    st.dataframe(
                        pd.DataFrame([dict(r) for r in cmte_rows]).rename(
                            columns={"committee_name": "Committee", "chamber": "Chamber", "role": "Role"}
                        ),
                        hide_index=True, use_container_width=True
                    )
                else:
                    st.caption("No committee roles found.")
            except Exception as _ce:
                st.caption(f"Could not load committee data: {_ce}")

            # Notes
            st.divider()
            st.markdown("#### 📝 Your Notes")
            _snotes = load_profile_notes("staff")
            _snote_key = sp.get("staff_id", sp.get("name", "unknown"))
            _current_snote = _snotes.get(_snote_key, "")
            _new_snote = st.text_area("Notes about this staffer", value=_current_snote, height=120,
                                       key=f"snote_{_snote_key}")
            if st.button("💾 Save Staffer Notes", key=f"snote_save_{_snote_key}"):
                if save_profile_note("staff", _snote_key, _snotes.get(_snote_key), _new_snote):
                    st.success("Saved!")

        # ── Legislator profile sub-view ──────────────────────────────────────────
        elif st.session_state.get("active_profile"):
            aptr = st.session_state.active_profile

            def clear_prof():
                st.session_state.pop("active_profile", None)
                st.session_state.pop("active_staff_profile", None)
            st.button("⬅️ Back to Directory", on_click=clear_prof)
            st.divider()

            # Resolve legislator from staff_manager
            match = pd.DataFrame()
            if not leg_df.empty:
                n_comps = normalize_name_components(aptr)
                leg_id, _rsn = resolve_legislator(leg_df, n_comps["full"], n_comps["last"], "", "")
                if leg_id:
                    match = leg_df[leg_df["legislator_id"] == leg_id]

            if match.empty:
                st.info(f"Using Master Corpus profile for: {aptr}")
                st.caption("No staff mapping found.")
                l_id, l_name, l_party, l_dist, l_cham = None, aptr, "?", "?", "Legislator"
                l_norm = aptr
                l_first, l_last = "", aptr.split()[-1] if aptr.split() else aptr
            else:
                lrow  = match.iloc[0]
                l_id  = lrow.get("legislator_id")
                l_name  = lrow.get("name", "Unknown")
                l_party = lrow.get("party", "")
                l_dist  = lrow.get("district", "")
                l_cham  = lrow.get("chamber", "")
                l_norm  = lrow.get("normalized_name", "")
                parts   = l_name.split()
                l_first = parts[0] if len(parts) > 1 else ""
                l_last  = parts[-1] if parts else l_name

            # Party color
            _party_badge = {"R": "🔴", "D": "🔵", "I": "🟡"}.get(l_party[:1].upper() if l_party else "", "⚪")
            st.subheader(f"{_party_badge} {l_cham} {l_name} ({l_party}) — District {l_dist}")

            t_staff, t_issue, t_bills, t_cmte, t_votes = st.tabs([
                "👥 Capitol Staff", "📋 Issue Assignments", "📜 Sponsored Bills",
                "🏛️ Committee Leadership", "🗳️ Voting History"
            ])

            # ── Capitol Staff tab ────────────────────────────────────────────────
            with t_staff:
                if l_id:
                    staff_ls = staff_manager.get_legislator_staff(l_id)
                    if not staff_ls:
                        st.caption("No staff loaded for this legislator.")
                    else:
                        role_labels = {
                            "chief_of_staff": "Chief of Staff", "legislative_director": "Legislative Director",
                            "scheduler": "Scheduler", "communications": "Communications Director",
                            "district_director": "District Director",
                        }
                        for s in staff_ls:
                            s_col1, s_col2, s_col3 = st.columns([3, 2, 1])
                            s_col1.markdown(f"**{s['name']}**  \n{role_labels.get(s['role'], s['role'])}")
                            s_col2.caption(s.get("email") or "")
                            if s_col3.button("View Profile →", key=f"staffbtn_{s['staff_id']}"):
                                st.session_state.active_staff_profile = s
                                st.rerun()
                else:
                    st.caption("No staff mapping for this legislator.")

            # ── Issue Assignments tab ────────────────────────────────────────────
            with t_issue:
                if l_id:
                    iss_ls = staff_manager.get_legislator_issues(l_id)
                    if not iss_ls:
                        st.caption("No issues mapped.")
                    else:
                        # Build name index once — O(1) lookups per row
                        _sname_idx = staff_manager.build_staff_name_index()
                        _last_issue = None
                        for iss in iss_ls:
                            issue = iss.get("issue_area", "")
                            sname = iss.get("staff_name", "")
                            # Print issue area header only when it changes
                            if issue != _last_issue:
                                st.markdown(f"**{issue}**")
                                _last_issue = issue
                            _srec = _sname_idx.get(sname.lower().strip())
                            i_col1, i_col2 = st.columns([5, 1])
                            i_col1.markdown(f"&nbsp;&nbsp;&nbsp;{sname}")
                            if _srec:
                                if i_col2.button("View Profile →",
                                                  key=f"iss_prof_{_srec['staff_id']}_{issue[:20]}"):
                                    st.session_state.active_staff_profile = _srec
                                    st.rerun()
                            else:
                                i_col2.caption("")
                else:
                    st.caption("No issue assignments mapped for this legislator.")

            # ── Sponsored Bills tab ──────────────────────────────────────────────
            with t_bills:
                # Search corpus first for accuracy; fall back to keyword CSV
                s_bills = pd.DataFrame()
                if corpus and l_name:
                    try:
                        s_bills = corpus.search_bills(query=l_last, limit=100)
                        if not s_bills.empty:
                            s_bills = s_bills[s_bills["sponsor_names"].str.contains(l_last, case=False, na=False)]
                    except:
                        s_bills = pd.DataFrame()
                if s_bills.empty and not df.empty and l_norm and "sponsors" in df.columns:
                    s_bills = df[df["sponsors"].astype(str).str.lower().str.contains(
                        str(l_norm).lower(), case=False, na=False)]

                if s_bills.empty:
                    st.caption("No indexed bills found for this legislator.")
                else:
                    st.caption(f"{len(s_bills)} bill(s) found in the corpus.")
                    for _, brow in s_bills.iterrows():
                        b_num = brow.get("bill_number", "")
                        b_col1, b_col2 = st.columns([5, 1])
                        b_col1.markdown(f"**{b_num}** — {brow.get('title','')[:90]}")
                        b_col1.caption(f"Status: {brow.get('status_stage','')} · {brow.get('last_action_date','')[:10]}")
                        b_col2.button("Open bill", key=f"billlink_{brow.get('bill_id')}_{l_id}",
                                      on_click=_go_to_bill, args=(b_num, brow.get("bill_id")))

            # ── Committee Leadership tab ─────────────────────────────────────────
            with t_cmte:
                if l_norm:
                    cmte_ls = staff_manager.get_legislator_committee_matrix(l_norm)
                    if not cmte_ls:
                        st.caption("No primary committee leadership mapped.")
                    else:
                        _sname_idx_c = staff_manager.build_staff_name_index()
                        _last_cmte = None
                        for cr in cmte_ls:
                            cmte = cr.get("committee", "")
                            role = cr.get("role", "")
                            sname = cr.get("staff_name", "")
                            if cmte != _last_cmte:
                                st.markdown(f"**🏛️ {cmte}**")
                                _last_cmte = cmte
                            _srec_c = _sname_idx_c.get(sname.lower().strip())
                            c_col1, c_col2 = st.columns([5, 1])
                            c_col1.markdown(f"&nbsp;&nbsp;&nbsp;*{role}* — {sname}")
                            if _srec_c and sname != "—":
                                if c_col2.button("View Profile →",
                                                  key=f"cmte_prof_{_srec_c['staff_id']}_{cmte[:15]}"):
                                    st.session_state.active_staff_profile = _srec_c
                                    st.rerun()
                            else:
                                c_col2.caption("")
                else:
                    st.caption("Cannot resolve committee status.")

            # ── Voting History tab ───────────────────────────────────────────────
            service_profile = f"directory:{l_id}"
            service_terms = (
                render_service_terms(staff_manager, service_profile, lrow.get("state", "CA"))
                if l_id and lrow.get("state") in {"CA", "US"} else []
            )
            with t_votes:
                if corpus:
                    votes = []
                    # Try via people_mapping first (most precise)
                    if l_id:
                        try:
                            votes = corpus.get_votes_for_legislator(l_id)
                        except:
                            votes = []
                    # Fallback: name-based lookup (works without mapping)
                    if not votes and l_last:
                        try:
                            votes = corpus.get_votes_for_legislator_by_name(l_first, l_last, lrow.get("state"), l_id)
                        except:
                            votes = []

                    with st.expander("Vote coverage for indexed sessions"):
                        coverage = corpus.get_historical_coverage()
                        if lrow.get("state") and not coverage.empty:
                            coverage = coverage[coverage["jurisdiction"] == lrow["state"]]
                        if coverage.empty:
                            st.caption("No sessions are indexed for this jurisdiction.")
                        else:
                            member_counts = Counter(vote.get("session") for vote in votes)
                            coverage = coverage.assign(**{
                                "Recorded votes for this member": coverage["session_name"].map(member_counts).fillna(0).astype(int)
                            })
                            st.dataframe(coverage[[
                                "session_name", "roll_calls", "with_individual_votes", "Recorded votes for this member",
                            ]].rename(columns={
                                "session_name": "Session", "roll_calls": "Indexed roll calls",
                                "with_individual_votes": "With individual votes",
                            }), hide_index=True)
                            st.caption("Session totals describe available records. Member counts use the linked voter identity.")

                    if not votes:
                        st.info("No recorded voting history in local dataset. "
                                "Ensure a corpus bootstrap has been run for the current session.")
                    else:
                        vote_sessions = sorted({v["session"] for v in votes if v.get("session")})
                        selected_session = st.selectbox("Voting session", [None, *vote_sessions],
                                                        format_func=lambda value: value or "All indexed sessions",
                                                        key=f"vote_session_{l_id}")
                        if selected_session:
                            votes = [v for v in votes if v.get("session") == selected_session]
                        votes = filter_votes_by_service_term(votes, service_terms, service_profile)
                        if service_terms and votes:
                            evidence = Counter(vote_service_status(vote, service_terms) for vote in votes)
                            st.caption("Term evidence: " + " | ".join(f"{label}: {count:,}" for label, count in sorted(evidence.items())))
                        voted_bills = {v["bill_id"]: v for v in votes}
                        st.caption(f"{len(votes):,} recorded votes on {len(voted_bills):,} distinct bills")
                        policy_counts = {}
                        for vote_bill in voted_bills.values():
                            for subject in {part.strip() for part in (vote_bill.get("subjects") or "").split(";") if part.strip()}:
                                policy_counts[subject] = policy_counts.get(subject, 0) + 1
                        if policy_counts:
                            policy_table = sorted(policy_counts.items(), key=lambda item: (-item[1], item[0]))
                            with st.expander("Subjects in recorded votes"):
                                st.dataframe(pd.DataFrame(policy_table, columns=["Subject", "Bills"]), hide_index=True)
                        committee_counts = {}
                        for vote_bill in voted_bills.values():
                            committee = (vote_bill.get("committee") or "").strip()
                            if committee:
                                committee_counts[committee] = committee_counts.get(committee, 0) + 1
                        if committee_counts:
                            with st.expander("Committees listed on bills with recorded votes"):
                                rows = sorted(committee_counts.items(), key=lambda item: (-item[1], item[0]))
                                st.dataframe(pd.DataFrame(rows, columns=["Committee", "Bills"]), hide_index=True)
                        _vote_counts = {"Yea": 0, "Nay": 0, "NV": 0, "Absent": 0}
                        for v in votes:
                            _vote_counts[v.get("vote_text", "NV")] = _vote_counts.get(v.get("vote_text", "NV"), 0) + 1
                        vc1, vc2, vc3, vc4 = st.columns(4)
                        vc1.metric("🟢 Yea", _vote_counts.get("Yea", 0))
                        vc2.metric("🔴 Nay", _vote_counts.get("Nay", 0))
                        vc3.metric("⬜ NV", _vote_counts.get("NV", 0))
                        vc4.metric("⬛ Absent", _vote_counts.get("Absent", 0))
                        st.divider()
                        for _vi, v in enumerate(votes):
                            _render_vote_row(v, _vi)
                else:
                    st.caption("Corpus offline — voting history unavailable.")

            # ── Notes section ────────────────────────────────────────────────────
            if l_id:
                with st.expander("Observed assignment history"):
                    history = staff_manager.get_member_assignment_history(l_id)
                    if history:
                        st.caption("Dates show when roster imports observed changes.")
                        st.dataframe(pd.DataFrame(history), hide_index=True)
                    else:
                        st.caption("No assignment history has been recorded for this member yet.")

            st.divider()
            st.markdown("#### 📝 Your Notes on this Legislator")
            _lnotes = load_profile_notes("legislator")
            _lnote_key = l_id or l_name
            _current_lnote = _lnotes.get(str(_lnote_key), "")
            _new_lnote = st.text_area("Notes", value=_current_lnote, height=130,
                                       placeholder="Add your internal notes about this legislator...",
                                       key=f"lnote_{_lnote_key}")
            if st.button("💾 Save Notes", key=f"lnote_save_{_lnote_key}"):
                if save_profile_note("legislator", str(_lnote_key), _lnotes.get(str(_lnote_key)), _new_lnote):
                    st.success("Notes saved!")

        # ── Directory listing ────────────────────────────────────────────────────
        elif leg_df.empty:
            st.info("No legislators ingested yet.")
            st.caption("Use '⚙️ Admin & Database Tools' → '👔 Staff Intelligence' to sync the live roster.")

        else:
            lf_c1, lf_c2 = st.columns(2)
            with lf_c1:
                search_leg = st.text_input("🔍 Search member name, party...", "")
            with lf_c2:
                cham_opt = ["All"] + sorted(leg_df["chamber"].dropna().unique().tolist())
                search_cham = st.selectbox("Chamber", cham_opt)

            if search_cham != "All":
                leg_df = leg_df[leg_df["chamber"] == search_cham]
            if search_leg:
                leg_df = leg_df[
                    leg_df["name"].str.contains(search_leg, case=False, na=False) |
                    leg_df["party"].str.contains(search_leg, case=False, na=False)
                ]

            st.caption(f"Showing {len(leg_df)} active members.")
            _lnotes_dir = load_profile_notes("legislator")
            for _, lrow in leg_df.iterrows():
                l_name = lrow.get("name", "Unknown")
                l_id_dir = str(lrow.get("legislator_id", l_name))
                _party_b = {"R": "🔴", "D": "🔵", "I": "🟡"}.get(
                    str(lrow.get("party", ""))[:1].upper(), "⚪")
                exp_title = f"{_party_b} {lrow.get('chamber')} {l_name} ({lrow.get('party')}) — Dist {lrow.get('district','')}"

                _has_note = bool(_lnotes_dir.get(l_id_dir))
                col1, col2 = st.columns([5, 1])
                col1.markdown(f"**{exp_title}**" + ("  📝" if _has_note else ""))

                def _go_prof(n=l_name):
                    st.session_state.active_profile = n
                col2.button("View Profile →", key=f"dprof_{lrow.get('legislator_id')}", on_click=_go_prof, args=(l_name,))
            st.divider()