import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import time
import logging
import importlib
import sys

from job_manager import JobManager
from job_runner import run_bootstrap_job, run_refresh_job, run_rescan_job
from staff_manager import StaffManager, resolve_legislator, normalize_name_components

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config               import DATA_DIR, API_KEY
import auth as _auth
from sync_github_repo     import ensure_repo, sync_with_remote as _sync_with_remote
import threading
def sync_with_remote():
    """Run github sync asynchronously."""
    threading.Thread(target=_sync_with_remote, daemon=True).start()

from legiscanner          import run_scan, CSV_FILE, KEYWORDS_FILE

# ── Corpus manager (Layer A — master bill corpus) ─────────────────────────────
# Guarded import: if corpus_manager.py is absent the app falls back gracefully.
try:
    from corpus_manager import CorpusManager as _CorpusManager
    _CORPUS_AVAILABLE = True
except ImportError:
    _CORPUS_AVAILABLE = False

if "repo_sync_done" not in st.session_state:
    st.session_state.repo_sync_done = True
    try: ensure_repo()
    except: pass

DATA_FILE    = CSV_FILE
# NOTE: TRACKED_FILE, NOTES_FILE, VIEWS_FILE and UPLOAD_DIR are resolved
# per-user below, after authentication. Placeholders here for reference only;
# they are overwritten after the auth wall.
TRACKED_FILE = os.path.join(DATA_DIR, "tracked_bills.json")   # overwritten post-auth
NOTES_FILE   = os.path.join(DATA_DIR, "bill_notes.json")       # overwritten post-auth
EXPORT_FILE  = os.path.join(DATA_DIR, "Tracked_Bills_Export.csv")
VIEWS_FILE   = os.path.join(DATA_DIR, "saved_views.json")      # overwritten post-auth
UPLOAD_DIR   = os.path.join(DATA_DIR, "uploads")               # overwritten post-auth
os.makedirs(UPLOAD_DIR, exist_ok=True)

st.set_page_config(page_title="UFtW Bill Tracker", layout="wide")

# ─── Authentication wall ──────────────────────────────────────────────────────
# render_auth_page() returns True if already logged in, otherwise renders the
# login/register UI and we must stop here.
if not _auth.render_auth_page():
    st.stop()

_current_user     = _auth.get_current_user()          # {username, role, api_key, is_guest}
_username         = _current_user["username"]

# Guests use the shared DATA_DIR (original single-user behaviour).
# Registered users get their own subdirectory.
if _auth.is_guest():
    _user_data_dir = DATA_DIR
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

STATUS_LEGEND = {
    "1": "Introduced",         "2": "In Committee",          "3": "Reported",
    "4": "Passed One Chamber", "5": "Passed Both Chambers",   "6": "Signed by Governor",
    "7": "Vetoed",             "8": "Failed",                  "9": "Withdrawn",
    "10": "Dead"
}

# ─── Session-state defaults ───────────────────────────────────────────────────
_STATE_DEFAULTS = {
    "selected_states":       [],
    "selected_federal_types":[],
    "status_options":        None,   # initialised after CSV load
    "friendly_status_options": None,
    "friendly_to_code":      None,
    "active_view_name":      None,
}
for _k, _v in _STATE_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─── Status helpers ───────────────────────────────────────────────────────────
def regenerate_friendly_status_options():
    st.session_state.friendly_status_options = [
        f"{STATUS_LEGEND.get(s, s)} [{s}]" for s in st.session_state.status_options
    ]
    st.session_state.friendly_to_code = {
        f"{STATUS_LEGEND.get(s, s)} [{s}]": s for s in st.session_state.status_options
    }


# ─── Jurisdiction helpers ─────────────────────────────────────────────────────
def get_jurisdiction_from_bill_number(bill_number):
    """Extract jurisdiction from bill number (best-effort fallback)."""
    if not bill_number:
        return 'Unknown', 'Unknown'
    bill_number = str(bill_number).upper()
    if any(p in bill_number for p in ['HR', 'S.', 'H.R.', 'HJ', 'SJ', 'HC', 'SC']):
        if bill_number.startswith(('HR', 'H.R.', 'HJ', 'HC')):
            return 'Federal', 'U.S. House of Representatives'
        elif bill_number.startswith(('S.', 'SJ', 'SC')):
            return 'Federal', 'U.S. Senate'
        return 'Federal', 'U.S. Congress'
    for state_code, state_name in US_STATES.items():
        if bill_number.startswith(f'{state_code}-') or bill_number[:len(state_code)] == state_code:
            return 'State', state_name
    ca_prefixes = ('AB', 'ACR', 'SCR', 'AJR', 'SJR', 'SB')
    if bill_number.startswith(ca_prefixes):
        return 'State', 'California'
    return 'Unknown', 'Unknown'


def apply_jurisdiction_columns(df, func):
    if 'bill_number' not in df.columns:
        return df
    df['jurisdiction_level'] = None
    df['jurisdiction_name']  = None
    for i, val in df['bill_number'].dropna().items():
        result = func(val)
        if isinstance(result, (list, tuple)) and len(result) == 2:
            df.at[i, 'jurisdiction_level'] = result[0]
            df.at[i, 'jurisdiction_name']  = result[1]
        else:
            df.at[i, 'jurisdiction_level'] = 'Unknown'
            df.at[i, 'jurisdiction_name']  = 'Unknown'
    return df


# ─── Data-loading functions (Layer B — keyword CSV) ───────────────────────────
@st.cache_data
def load_data():
    """Load bill data from the keyword-match CSV (Layer B)."""
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
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


@st.cache_data
def load_keywords():
    try:
        if os.path.exists(KEYWORDS_FILE):
            with open(KEYWORDS_FILE, "r") as f:
                return json.load(f)
        return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]
    except Exception as e:
        logger.error(f"Error loading keywords: {e}")
        return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]


def save_keywords(keywords):
    """Write atomically THEN sync."""
    try:
        tmp_file = KEYWORDS_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(sorted(set(keywords)), f)
        os.replace(tmp_file, KEYWORDS_FILE)
        sync_with_remote()
        load_keywords.clear()
        return True
    except Exception as e:
        logger.error(f"Error saving keywords: {e}")
        st.error(f"Error saving keywords: {e}")
        return False


@st.cache_data
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
        sync_with_remote()
        load_saved_views.clear()
        return True
    except Exception as e:
        logger.error(f"Error saving views: {e}")
        st.error(f"Error saving views: {e}")
        return False

@st.cache_data
def load_tracked():
    try:
        if os.path.exists(TRACKED_FILE):
            with open(TRACKED_FILE, "r") as f:
                return json.load(f)
        return []
    except Exception as e:
        logger.error(f"Error loading tracked bills: {e}")
        return []


def save_tracked(tracked):
    """Write atomically THEN sync."""
    try:
        tmp_file = TRACKED_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(tracked, f)
        os.replace(tmp_file, TRACKED_FILE)
        sync_with_remote()
        load_tracked.clear()
        return True
    except Exception as e:
        logger.error(f"Error saving tracked bills: {e}")
        st.error(f"Error saving tracked bills: {e}")
        return False


@st.cache_data
def load_notes():
    try:
        if os.path.exists(NOTES_FILE):
            with open(NOTES_FILE, "r") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading notes: {e}")
        return {}


def save_notes(notes):
    """Write atomically THEN sync."""
    try:
        tmp_file = NOTES_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(notes, f, indent=2)
        os.replace(tmp_file, NOTES_FILE)
        sync_with_remote()
        load_notes.clear()
        return True
    except Exception as e:
        logger.error(f"Error saving notes: {e}")
        st.error(f"Error saving notes: {e}")
        return False


# ─── Profile note helpers (legislators & staff) ───────────────────────────────
def _profile_notes_path(kind: str) -> str:
    """kind = 'legislator' or 'staff'"""
    return os.path.join(_user_data_dir, f"{kind}_notes.json")

def load_profile_notes(kind: str) -> dict:
    path = _profile_notes_path(kind)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}
    except Exception as e:
        logger.error(f"Error loading {kind} notes: {e}")
        return {}

def save_profile_notes(kind: str, notes: dict) -> bool:
    path = _profile_notes_path(kind)
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(notes, f, indent=2)
        os.replace(tmp, path)
        sync_with_remote()
        return True
    except Exception as e:
        logger.error(f"Error saving {kind} notes: {e}")
        st.error(f"Error saving {kind} notes: {e}")
        return False


# ─── Search helper ────────────────────────────────────────────────────────────
def search_df(df: pd.DataFrame, query: str) -> pd.DataFrame:
    """Full-text search across standard CSV columns."""
    if not query or df.empty:
        return df
    q = query.lower()
    for col in ['title', 'sponsors', 'sponsor_names', 'description', 'bill_number', 'subjects', 'keyword']:
        if col not in df.columns:
            df[col] = ''
    mask = (
        df['title'].str.lower().str.contains(q, na=False)
        | df['description'].str.lower().str.contains(q, na=False)
        | df['bill_number'].str.lower().str.contains(q, na=False)
        | df['sponsors'].str.lower().str.contains(q, na=False)
        | df['subjects'].str.lower().str.contains(q, na=False)
        | df['keyword'].str.lower().str.contains(q, na=False)
    )
    return df[mask]


# ─── Sort helper ──────────────────────────────────────────────────────────────
_SORT_OPTIONS = ["Most Recent Action", "Status Date (Newest)", "Bill Number A→Z", "Status Stage"]
_TRACKED_SORT_OPTIONS = ["Most Recent Action", "Status Date (Newest)", "Bill Number A→Z", "Status Stage", "Priority (High First)", "Last Reviewed (Newest)"]

def apply_sort(df: pd.DataFrame, sort_key: str) -> pd.DataFrame:
    if df.empty:
        return df
    if sort_key == "Most Recent Action":
        col = 'last_action_date' if 'last_action_date' in df.columns else 'status_date'
        return df.sort_values(col, ascending=False, na_position='last')
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
    return df


# ─── Corpus helpers ───────────────────────────────────────────────────────────
def get_corpus_status_options(corpus) -> list:
    """Distinct status_stage values from corpus DB (used in All Bills tab)."""
    if corpus is None:
        return []
    try:
        rows = corpus._get_conn().execute(
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
            corpus_df = corpus.get_bills_by_ids(tracked_bills)
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
    export_df['last_reviewed'] = export_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get("last_reviewed", ""))
    
    cols = [
        'bill_number', 'title', 'jurisdiction_level', 'jurisdiction_name', 'session',
        'status_stage', 'status_date', 'last_action', 'last_action_date',
        'subjects', 'sponsors', 'committees', 'keyword', 'url',
        'tracked', 'position', 'priority', 'notes_comment', 'notes_links', 'last_reviewed'
    ]
    cols = [c for c in cols if c in export_df.columns]
    return export_df[cols]

# ─── Dashboard ────────────────────────────────────────────────────────────────
def run_smart_header(df_count, app_mode, corpus, tracked_bills):
    """Compact, high-value header area showing mode, row counts, and data freshness."""
    with st.container(border=True):
        hc1, hc2, hc3, hc4 = st.columns(4)
        
        with hc1:
            st.markdown(f"**Current Mode:** `{app_mode}`")
            st.caption(f"Showing **{df_count:,}** results")

        with hc2:
            st.markdown("**Sort Order:**")
            st.caption(st.session_state.get('global_sort', 'Status Date ⬇️'))

        with hc3:
            st.markdown("**Data Vintage:**")
            if corpus:
                cstats = corpus.get_corpus_stats()
                if cstats and cstats.get('last_updated'):
                    lu = cstats['last_updated']
                    st.caption(f"DB: {lu}")
                else:
                    st.caption("DB: Unknown")
            else:
                st.caption("Mode: CSV Cache")

        with hc4:
            st.markdown("**Tracked Scope:**")
            st.caption(f"{len(tracked_bills)} total tracked")
            
    # Add lightweight visual indicator if filters are active
    active_filters = []
    if st.session_state.get('global_jur'): active_filters.append(f"Jurisdictions: {len(st.session_state.global_jur)}")
    if st.session_state.get('global_status'): active_filters.append(f"Statuses: {len(st.session_state.global_status)}")
    if st.session_state.get('kw_filter'): active_filters.append(f"Keywords: {len(st.session_state.kw_filter)}")
    if st.session_state.get('tracked_pos'): active_filters.append(f"Positions: {len(st.session_state.tracked_pos)}")
    if st.session_state.get('global_search'): active_filters.append(f"Search: '{st.session_state.global_search}'")
    
    if active_filters:
        st.info("🔎 **Active Filters:** " + " · ".join(active_filters))


# ─── Bill card renderer ───────────────────────────────────────────────────────
def _normalize_note(note: dict) -> dict:
    return {
        "comment":       note.get("comment", ""),
        "links":         note.get("links", []),
        "files":         note.get("files", []),
        "position":      note.get("position", ""),
        "priority":      note.get("priority", ""),
        "last_reviewed": note.get("last_reviewed", ""),
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
        if bill_url:
            st.link_button("View on LegiScan", bill_url)
        return
    
    if res.get("html"):
        # Wrap in a scrolling div for better UX
        st.components.v1.html(
            f"""
            <div style="font-family: sans-serif; line-height: 1.6; color: #333; height: 80vh; overflow-y: scroll; padding: 20px; border: 1px solid #eee; border-radius: 8px;">
                {res['html']}
            </div>
            """,
            height=850,
            scrolling=False
        )
    elif res.get("pdf_url"):
        st.info("HTML text not available. You can view the official PDF below:")
        st.link_button("📂 View PDF", res['pdf_url'], use_container_width=True)
    else:
        st.warning("No text content found.")

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
            
            indicator_html = f"<b>{jur_icon} {jur_name}</b> &nbsp;|&nbsp; 📋 {status_lbl}"
            prio = note.get('priority')
            if prio == 'High': indicator_html += " &nbsp;|&nbsp; <span style='color:red;'><b>🔥 High Priority</b></span>"
            elif prio: indicator_html += f" &nbsp;|&nbsp; 🔥 {prio}"
            
            pos = note.get('position')
            if pos: indicator_html += f" &nbsp;|&nbsp; 🏷️ <b>{pos}</b>"
            
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
                        staleness_warning = " ⚠️ <i>(Warning: Last action is > 18 months old)</i>"
                except: pass
                
            if last_action or last_action_date:
                st.caption(f"**Last Action:** {last_action} ({last_action_date}){staleness_warning}", unsafe_allow_html=True)
            
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
                    tracked_bills.append(bill_id)
                    save_tracked(tracked_bills)
                    st.toast(f"Tracked {disp_bill_number}")
                    st.rerun()
            else:
                if st.button("❌ Untrack", key=f"{key_prefix}_untrack", use_container_width=True):
                    try:
                        tracked_bills.remove(bill_id)
                        save_tracked(tracked_bills)
                        st.toast(f"Untracked {disp_bill_number}")
                        st.rerun()
                    except: pass
                    
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
            nc1, nc2 = st.columns(2)
            with nc1:
                new_comment = st.text_area("💬 Notes / Comments", value=note.get("comment", ""), key=f"{key_prefix}_cmt")
                new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{key_prefix}_lnks")
            with nc2:
                position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch", "Neutral", "No Position"], 
                                        index=["", "Support", "Oppose", "Watch", "Neutral", "No Position"].index(note.get("position", "")), key=f"{key_prefix}_pos")
                priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], 
                                        index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{key_prefix}_prio")
            
            uploaded_file = st.file_uploader("📎 Attach PDF", type=["pdf"], key=f"{key_prefix}_upl")
            
            if st.button("💾 Save Notes", key=f"{key_prefix}_save"):
                try:
                    note["comment"]  = new_comment
                    note["links"]    = [x.strip() for x in new_links.split(",") if x.strip()]
                    note["position"] = position
                    note["priority"] = priority
                    note["last_reviewed"] = datetime.utcnow().isoformat()
                    if uploaded_file:
                        fp = os.path.join(UPLOAD_DIR, f"{bill_id}_{uploaded_file.name}")
                        with open(fp, "wb") as fh:
                            fh.write(uploaded_file.getbuffer())
                        note.setdefault("files", []).append(fp)
                    bill_notes[bill_id] = note
                    if save_notes(bill_notes):
                        st.success(f"✅ Notes saved for {bill_id}")
                    else:
                        st.error("Failed to save notes")
                except Exception as e:
                    st.error(f"Error saving notes: {e}")
                    logger.error(f"Error saving notes for {bill_id}: {e}")
            
            if note.get("last_reviewed"):
                try:
                    _lr_dt = datetime.fromisoformat(note["last_reviewed"]).strftime("%Y-%m-%d %H:%M")
                    st.caption(f"Last reviewed: {_lr_dt}")
                except: pass

        with st.expander("🗳️ Roll Call Votes"):
            global corpus
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
                                            p_name = f"<span style='color:red;'>[Unresolved ID {mv.get('people_id')}]</span>"
                                        elif mv.get('staff_legislator_id'):
                                            # If mapped, indicate linkage (we could link to profile here later)
                                            p_name = f"**{raw_name}**"
                                        else:
                                            p_name = raw_name
                                            
                                        color = "green" if v_txt == "Yea" else ("red" if v_txt == "Nay" else "gray")
                                        cols[v_idx % 3].markdown(f"{p_name}: <span style='color:{color}; font-weight:bold;'>{v_txt}</span>", unsafe_allow_html=True)
                            st.divider()
                except Exception as e:
                    st.error(f"Error loading votes: {e}")
            else:
                st.info("Roll-call database is offline.")

    return note


# ─── Load application data ────────────────────────────────────────────────────
try:
    keywords_list = load_keywords()
    tracked_bills = load_tracked()
    bill_notes    = load_notes()
    df            = load_data()   # Layer B: keyword-match CSV
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

# ── Resolve per-user file paths now that we are authenticated ──────────────────
TRACKED_FILE = os.path.join(_user_data_dir, "tracked_bills.json")
NOTES_FILE   = os.path.join(_user_data_dir, "bill_notes.json")
VIEWS_FILE   = os.path.join(_user_data_dir, "saved_views.json")
UPLOAD_DIR   = os.path.join(_user_data_dir, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

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

app_mode = st.sidebar.radio(
    "View Mode",
    ["🔍 All Bills", "🏷️ Keyword Matches", "⭐ Tracked Bills", "👔 Legislator Directory", "🛠️ Staff Analytics"],
    key="app_mode"
)

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
    _all_status_opts = get_corpus_status_options(corpus)
if not df.empty:
    _csv_stats = df["status_stage"].dropna().astype(str).unique().tolist() if "status_stage" in df.columns else []
    for s in _csv_stats:
        if s not in _all_status_opts:
            _all_status_opts.append(s)
    _all_status_opts = sorted(_all_status_opts)
    if "sponsors" in df.columns:
        _all_sponsors = sorted(df["sponsors"].dropna().unique())
    if "committees" in df.columns:
        _all_committees = sorted(df["committees"].dropna().unique())

if st.session_state.status_options is None or len(st.session_state.status_options) < len(_all_status_opts):
    st.session_state.status_options = list(_all_status_opts)
    regenerate_friendly_status_options()

global_jur = st.sidebar.multiselect(
    "Jurisdiction",
    options=_all_jur_opts,
    default=st.session_state.get("global_jur", []),
    key="global_jur_input"
)
st.session_state.global_jur = global_jur

_f_opts = [f"{STATUS_LEGEND.get(s, s)} [{s}]" for s in st.session_state.status_options]
_f2c    = {f"{STATUS_LEGEND.get(s, s)} [{s}]": s for s in st.session_state.status_options}
_stat_sel = st.sidebar.multiselect(
    "Status Stage",
    options=_f_opts,
    default=st.session_state.get("global_status_friendly", []),
    key="global_status_friendly"
)
st.session_state.global_status = [_f2c[f] for f in _stat_sel]

global_date_range = st.sidebar.date_input(
    "Status Date Range",
    value=st.session_state.get("global_date_range", []),
    key="global_date_range_input"
)
st.session_state.global_date_range = global_date_range

global_sponsors = st.sidebar.multiselect(
    "Sponsors",
    options=_all_sponsors,
    default=st.session_state.get("global_sponsors", []),
    key="global_sponsors_input"
)
st.session_state.global_sponsors = global_sponsors

global_committees = st.sidebar.multiselect(
    "Committees",
    options=_all_committees,
    default=st.session_state.get("global_committees", []),
    key="global_committees_input"
)
st.session_state.global_committees = global_committees

st.sidebar.divider()

# ── D. ADVANCED FILTERS (Expander) ───────────────────────────────────────────
with st.sidebar.expander("🛠️ Advanced Filters"):
    _avail_kw = sorted(df["keyword"].dropna().unique()) if not df.empty and "keyword" in df.columns else keywords_list
    kw_filter = st.multiselect(
        "Keyword Meta-Tags",
        options=_avail_kw,
        default=st.session_state.get("kw_filter", []),
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
    _ns_code  = st.text_input("New Custom Status Code", key="new_status_code")
    _ns_label = st.text_input("New Status Label",       key="new_status_label")
    if st.button("Add Status", key="add_status_btn"):
        if _ns_code and _ns_code not in st.session_state.status_options:
            st.session_state.status_options.append(_ns_code)
            STATUS_LEGEND[_ns_code] = _ns_label or _ns_code
            regenerate_friendly_status_options()
            st.success(f"Added status {_ns_code}: {_ns_label}")
            st.rerun()

# ── E. MODE-SPECIFIC OVERLAYS ──────────────────────────────────────────────────
if "⭐ Tracked Bills" in app_mode:
    st.sidebar.divider()
    st.sidebar.caption("**Tracked Bills Quick-Filters**")
    tracked_quick = st.sidebar.radio(
        "Status Flag",
        ["All Tracked", "Needs Attention", "Has Notes", "No Notes", "High Priority", "Recently Updated"],
        index=["All Tracked", "Needs Attention", "Has Notes", "No Notes", "High Priority", "Recently Updated"].index(st.session_state.get("tracked_quick", "All Tracked")),
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
                conn = corpus._get_conn()
                db_stats = {
                    "roll_calls":     conn.execute("SELECT COUNT(*) FROM roll_calls").fetchone()[0],
                    "leg_votes":      conn.execute("SELECT COUNT(*) FROM legislator_votes").fetchone()[0],
                    "people_total":   conn.execute("SELECT COUNT(*) FROM people").fetchone()[0],
                    "people_unknown": conn.execute("SELECT COUNT(*) FROM people WHERE name LIKE '%Unknown Profile%'").fetchone()[0],
                    "people_mapped":  conn.execute("SELECT COUNT(*) FROM people_mapping").fetchone()[0],
                }
                st.write(f"**Roll Calls Stored:** {db_stats['roll_calls']:,}")
                st.write(f"**Individual Votes Stored:** {db_stats['leg_votes']:,}")
                st.write(f"**People Profiles Extracted:** {db_stats['people_total']:,}")
                st.write(f"**Profiles Missing Name (ID Only):** {db_stats['people_unknown']:,}")
                st.write(f"**Profiles Mapped to Staff Roster:** {db_stats['people_mapped']:,}")

                st.caption("Recent Failed Name Extracts & Fallbacks:")
                sample = conn.execute("SELECT people_id, name, party FROM people WHERE name LIKE '%Unknown Profile%' LIMIT 10").fetchall()
                if sample:
                    st.dataframe(pd.DataFrame([dict(s) for s in sample]), use_container_width=True)
                else:
                    st.success("No recent unknowns!")

        st.divider()

        st.header("👔 Staff Intelligence")
        live_sources = [{
            "label": "Live Capitol Matrix",
            "type": "google_sheet_live",
            "url": "https://docs.google.com/spreadsheets/d/1gFeGy72R_-FSFrjXbKCAAvVsvNjyV7t_TUvFoB12vys/export?format=xlsx",
            "enabled": True,
            "state": "CA",
            "chamber": "Both"
        }]

        if live_sources and staff_manager:
            prim = live_sources[0]
            st.write(f"Source: **{prim['label']}**")
            if st.button("🔄 Sync Live Capitol Matrix", type="primary", use_container_width=True):
                with st.spinner("Downloading live Google Sheet..."):
                    ok, res = staff_manager.sync_live_sheet(prim['url'], DATA_DIR, prim.get('state', 'CA'))
                    if ok:
                        st.session_state.sync_warnings = res.get('warnings', [])
                        if corpus:
                            try: corpus.sync_people_mapping(staff_manager.get_all_legislators())
                            except: pass
                        st.success("Synced gracefully!")
                        st.rerun()
                    else:
                        st.error(f"Failed: {res}")

            if st.session_state.get('sync_warnings'):
                for w in st.session_state.sync_warnings:
                    st.warning(f"Pipeline Sanity Check: {w}")

            last_job = staff_manager.get_last_import_job()
            if last_job:
                st.caption(f"Last Sync: {last_job['timestamp'][:16].replace('T', ' ')}")

            with st.expander("Or upload manual replacement"):
                staff_file = st.file_uploader("Assy & Senate Roster (.xlsx)", type=['xlsx'], key="staff_override")
                if staff_file and st.button("Ingest Uploaded File"):
                    with st.spinner("Processing..."):
                        tmp_path = os.path.join(DATA_DIR, "temp_staff.xlsx")
                        with open(tmp_path, "wb") as f: f.write(staff_file.getbuffer())
                        staff_manager.ingest_spreadsheet(tmp_path)
                        st.rerun()
        else:
            st.caption("Upload a structured legislative staff roster.")
            staff_file = st.file_uploader("Assy & Senate Roster (.xlsx)", type=['xlsx'])
            if staff_file and staff_manager:
                if st.button("Ingest Staff Hierarchy", type="primary", use_container_width=True):
                    with st.spinner("Extracting roles and issue domains..."):
                        tmp_path = os.path.join(DATA_DIR, "temp_staff.xlsx")
                        with open(tmp_path, "wb") as f:
                            f.write(staff_file.getbuffer())
                        success, results = staff_manager.ingest_spreadsheet(tmp_path)
                        if success:
                            st.success(f"Ingested! Processed: {results['processed']}, Unmatched: {results['unmatched']}")
                        else:
                            st.error(f"Ingestion failed: {results}")
        st.divider()

        st.header("🔄 Keyword Rescan")
        st.caption("Scans the API for bills mentioning monitored keywords.")
        _rescan_states = st.multiselect(
            "States", options=sorted(US_STATES.values()),
            default=["California"], key="rescan_states"
        )
        _rescan_federal = st.checkbox("Include Federal", value=True, key="rescan_federal")

        _running_jobs_for_lock = job_manager.get_running_jobs() if job_manager else []
        _lock_rescan = bool(_running_jobs_for_lock and any(_j['job_type'] == 'keyword_rescan' for _j in _running_jobs_for_lock))
        if st.button("▶️ Run Keyword Rescan", key="run_rescan", disabled=_lock_rescan):
            if not _rescan_states and not _rescan_federal:
                st.warning("Select at least one jurisdiction.")
            else:
                with st.spinner("Scanning bills by keyword…"):
                    try:
                        _sc2nc = {v: k for k, v in US_STATES.items()}
                        _api_states = [_sc2nc.get(s, s) for s in _rescan_states]
                        if _rescan_federal:
                            _api_states.append("US")
                        run_rescan_job(corpus, list(set(_api_states)), DATA_DIR, job_manager, initiated_by=_username)
                        sync_with_remote()
                        load_data.clear()
                        st.success("Rescan completed!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Rescan failed: {e}")

        st.divider()
        st.header("📚 Master Corpus Tasks")
        st.caption("Synchronizes entire session bill details to local SQLite.")
        if corpus:
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
            if st.button("🔄 Refresh Session Updates", key="corpus_refresh", disabled=not _sel_session or _lock_refresh):
                _rb = st.progress(0, text="Refreshing...")
                run_refresh_job(corpus, _sel_session["session_id"], _sel_session["jurisdiction"], job_manager, lambda f, m: _rb.progress(min(f, 1.0), text=m))
                _rb.progress(1.0, text="Done")
                st.rerun()

            st.markdown("**Bulk Bootstrap** (expensive, initial load)")
            _lock_boot    = bool(_running_jobs_for_lock and any(_j['job_type'] == 'bootstrap_corpus' for _j in _running_jobs_for_lock))
            _confirm_boot = st.checkbox("I understand Bootstrap takes several minutes", key="confirm_boot")
            if st.button("⬇️ Execute Bootstrap Dump", key="corpus_bootstrap", disabled=not (_sel_session and _confirm_boot) or _lock_boot):
                _pb = st.progress(0, text="Bootstrapping...")
                run_bootstrap_job(corpus, _sel_session["session_id"], _sel_session["jurisdiction"], job_manager, lambda f, m: _pb.progress(min(f, 1.0), text=m))
                _pb.progress(1.0, text="Done")
                st.rerun()

        st.divider()
        st.header("📋 Recent Jobs Log")
        if job_manager:
            recent = job_manager.get_recent_jobs(5)
            if recent:
                for j in recent:
                    icon = "✅" if j['status'] == 'SUCCESS' else ("❌" if j['status'] == 'FAILED' else "🔄")
                    st.write(f"{icon} **{j['job_type']}** ({j['jurisdiction']})")
                    st.caption(f"Elapsed: {j['duration_sec'] or 0:.1f}s · Added: {j['new_items']} · Updated: {j['updated_items']}")
            else:
                st.caption("No jobs logged yet.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN CONTENT / WORKSPACE
# ═══════════════════════════════════════════════════════════════════════════════
st.title("🏛️ Unfuck The World Bill Tracker")
# We will invoke run_smart_header later dynamically inside each mode (All Bills, Keyword Matches, Tracked Bills) because we need the final df_count AFTER filters!

st.divider()

# Top sorting and workspace bar
wscol1, wscol2, wscol3 = st.columns([1, 4, 1])
with wscol1:
    _sort_opts = _SORT_OPTIONS if "All Bills" not in app_mode else _SORT_OPTIONS
    if "Tracked" in app_mode: _sort_opts = _TRACKED_SORT_OPTIONS
    global_sort = st.selectbox("Sort logic", _sort_opts, key="global_sort_input")
    st.session_state.global_sort = global_sort
with wscol3:
    st.write("") # spacing
    if st.button("🚫 Clear All Filters", use_container_width=True):
        for k in ["global_jur", "global_status", "global_sponsors", "global_committees", "global_date_range", "kw_filter", "tracked_pos", "tracked_prio", "global_search", "global_status_friendly"]:
            if k in st.session_state:
                del st.session_state[k]
        st.session_state.global_search = ""
        st.rerun()

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
        work_df = work_df[work_df['sponsors'].isin(st.session_state.global_sponsors)]
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
if "All Bills" in app_mode:
    if not _CORPUS_AVAILABLE or not corpus:
        st.warning("Master Corpus SQLite not responding.")
    else:
        # Perform DB level filtering for speed, then Pandas filtering for unified fields
        try:
            db_df = corpus.search_bills(
                query=st.session_state.global_search or None,
                jurisdiction_filter=st.session_state.global_jur or None,
                status_filter=st.session_state.get("global_status") or None,
                limit=1000 # Safely bump up so pandas filtering has room
            )
        except Exception as e:
            st.error(f"Search err: {e}")
            db_df = pd.DataFrame()
        
        # Now apply the unified filters in Pandas space safely
        # Note: global_search, jur, and status were ALREADY pushed down to SQLite!
        if not db_df.empty:
            if st.session_state.global_sponsors and 'sponsors' in db_df.columns:
                db_df = db_df[db_df['sponsors'].isin(st.session_state.global_sponsors)]
            if st.session_state.global_committees and 'committees' in db_df.columns:
                db_df = db_df[db_df['committees'].isin(st.session_state.global_committees)]
            if st.session_state.kw_filter and 'keyword' in db_df.columns:
                db_df = db_df[db_df['keyword'].isin(st.session_state.kw_filter)]
            if st.session_state.tracked_pos:
                db_df = db_df[db_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('position', '') in st.session_state.tracked_pos)]
            if st.session_state.tracked_prio:
                db_df = db_df[db_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('priority', '') in st.session_state.tracked_prio)]
                
        db_df = apply_sort(db_df, st.session_state.global_sort)
        run_smart_header(len(db_df), "All Bills", corpus, tracked_bills)
        
        total_bills = len(db_df)
        st.caption(f"Showing {total_bills} bills from Master Archive")
        if total_bills > 0:
            page_size = 50
            total_pages = max(1, (total_bills + page_size - 1) // page_size)
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="all_bills_pg")
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_df = db_df.iloc[start_idx:end_idx]
            if total_pages > 1: st.write(f"Page {page} of {total_pages} (Bills {start_idx+1}-{min(end_idx, total_bills)})")
            
            for _, row in page_df.iterrows():
                bid = str(row.get('bill_id', 'Unknown'))
                _render_bill_card(row, bill_notes.get(bid, {}), bid, bill_notes, tracked_bills, key_prefix=f"ab_{bid}")
            
        if not db_df.empty:
            st.download_button("📥 Export", build_export_df(db_df, bill_notes, tracked_bills).to_csv(index=False), "all_bills_search.csv", "text/csv")


# ────────── KEYWORD MATCHES ───────────────────────────────────────────────────
elif "Keyword Matches" in app_mode:
    if df.empty:
        st.warning("Keyword cache empty. Run a rescan.")
    else:
        kw_df = df.copy()
        kw_df = run_unified_filters(kw_df)
        
        if st.session_state.kw_filter and 'keyword' in kw_df.columns:
            kw_df = kw_df[kw_df['keyword'].isin(st.session_state.kw_filter)]
        if st.session_state.tracked_pos:
            kw_df = kw_df[kw_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('position', '') in st.session_state.tracked_pos)]
        if st.session_state.tracked_prio:
            kw_df = kw_df[kw_df['bill_id'].astype(str).apply(lambda x: bill_notes.get(x, {}).get('priority', '') in st.session_state.tracked_prio)]
            
        kw_df = apply_sort(kw_df, st.session_state.global_sort)
        run_smart_header(len(kw_df), "Keyword Matches", corpus, tracked_bills)
        
        total_bills = len(kw_df)
        st.caption(f"Showing {total_bills} matches from the Rescan Cache")
        if total_bills > 0:
            page_size = 50
            total_pages = max(1, (total_bills + page_size - 1) // page_size)
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="kw_bills_pg")
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_df = kw_df.iloc[start_idx:end_idx]
            if total_pages > 1: st.write(f"Page {page} of {total_pages} (Bills {start_idx+1}-{min(end_idx, total_bills)})")
            
            for _, row in page_df.iterrows():
                bid = str(row.get('bill_id', 'Unknown'))
                _render_bill_card(row, bill_notes.get(bid, {}), bid, bill_notes, tracked_bills, key_prefix=f"kw_{bid}")
                
        if not kw_df.empty:
            st.download_button("📥 Export Matches", build_export_df(kw_df, bill_notes, tracked_bills).to_csv(index=False), "match_search.csv", "text/csv")


# ────────── TRACKED BILLS ─────────────────────────────────────────────────────
elif "Tracked Bills" in app_mode:
    if not tracked_bills:
        st.info("No tracked bills. Flag some bills from the repository first.")
    else:
        tr_df = get_tracked_bills_df(tracked_bills, corpus, df)
        tr_df = run_unified_filters(tr_df)
        
        # Apply standard advanced filters
        if st.session_state.kw_filter and 'keyword' in tr_df.columns:
            tr_df = tr_df[tr_df['keyword'].isin(st.session_state.kw_filter)]
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
                
        tr_df = apply_sort(tr_df, st.session_state.global_sort)
        run_smart_header(len(tr_df), "Tracked Bills", corpus, tracked_bills)
        
        total_bills = len(tr_df)
        st.caption(f"Showing {total_bills} Tracked Bills")
        if total_bills > 0:
            page_size = 50
            total_pages = max(1, (total_bills + page_size - 1) // page_size)
            page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1, key="tr_bills_pg")
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            page_df = tr_df.iloc[start_idx:end_idx]
            if total_pages > 1: st.write(f"Page {page} of {total_pages} (Bills {start_idx+1}-{min(end_idx, total_bills)})")
            
            for _, row in page_df.iterrows():
                bid = str(row.get('bill_id', 'Unknown'))
                _render_bill_card(row, bill_notes.get(bid, {}), bid, bill_notes, tracked_bills, key_prefix=f"tr_{bid}")
            
        st.divider()
        if not tr_df.empty:
            st.download_button("📥 Export Tracked Archive", build_export_df(tr_df, bill_notes, tracked_bills).to_csv(index=False), "tracked_archive.csv", "text/csv")
            
        with st.expander("🗑️ Bulk Remove Tracked Bills"):
            _trm = st.multiselect("Select items", tracked_bills)
            if st.button("Delete Tracking", type="primary") and _trm:
                for t in _trm: tracked_bills.remove(t)
                save_tracked(tracked_bills)
                st.rerun()

# ────────── STAFF PIPELINE DIAGNOSTICS ──────────────────────────────────────────────
elif "Staff Analytics" in app_mode:
    if not staff_manager:
        st.warning("Staff Intelligence Module offline.")
    else:
        from staff_diagnostics import render_staff_diagnostics
        render_staff_diagnostics(staff_manager)

# ────────── LEGISLATOR DIRECTORY ──────────────────────────────────────────────
elif "Legislator Directory" in app_mode:
    if not staff_manager:
        st.warning("Staff Intelligence Module offline.")
    else:
        st.header("👔 Legislator Staff & Issue Hub")
        try:
            leg_df = staff_manager.get_all_legislators()
        except:
            leg_df = pd.DataFrame()

        # ── Helper: jump to bill view ────────────────────────────────────────────
        def _go_to_bill(bill_number: str):
            st.session_state["global_search"] = bill_number
            st.session_state["app_mode_radio"] = "🔍 All Bills"
            st.session_state.pop("active_profile", None)
            st.session_state.pop("active_staff_profile", None)

        # ── Helper: render inline vote table ────────────────────────────────────
        def _render_vote_row(v: dict, _idx: int = 0):
            vote_icon = {"Yea": "🟢", "Nay": "🔴", "NV": "⬜", "Absent": "⬛"}.get(v.get("vote_text", ""), "❓")
            bn = v.get("bill_number", "")
            col_a, col_b, col_c, col_d = st.columns([2, 1, 4, 1])
            col_a.markdown(f"**{v.get('vote_date', '')[:10]}**")
            col_b.markdown(f"{vote_icon} {v.get('vote_text', '')}")
            col_c.markdown(f"{bn} — {v.get('motion', '')[:60]}")
            if bn and col_d.button("→ Bill", key=f"votelink_{_idx}_{bn}"):
                _go_to_bill(bn)
                st.rerun()

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
                _snotes[_snote_key] = _new_snote
                if save_profile_notes("staff", _snotes):
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
                        if b_col2.button("→ View Bill", key=f"billlink_{b_num}_{l_id}"):
                            _go_to_bill(b_num)
                            st.rerun()

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
                            votes = corpus.get_votes_for_legislator_by_name(l_first, l_last)
                        except:
                            votes = []

                    if not votes:
                        st.info("No recorded voting history in local dataset. "
                                "Ensure a corpus bootstrap has been run for the current session.")
                    else:
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
            st.divider()
            st.markdown("#### 📝 Your Notes on this Legislator")
            _lnotes = load_profile_notes("legislator")
            _lnote_key = l_id or l_name
            _current_lnote = _lnotes.get(str(_lnote_key), "")
            _new_lnote = st.text_area("Notes", value=_current_lnote, height=130,
                                       placeholder="Add your internal notes about this legislator...",
                                       key=f"lnote_{_lnote_key}")
            if st.button("💾 Save Notes", key=f"lnote_save_{_lnote_key}"):
                _lnotes[str(_lnote_key)] = _new_lnote
                if save_profile_notes("legislator", _lnotes):
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