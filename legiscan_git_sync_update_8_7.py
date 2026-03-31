import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import logging
import importlib
import sys

from job_manager import JobManager
from job_runner import run_bootstrap_job, run_refresh_job, run_rescan_job
from staff_manager import StaffManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config               import DATA_DIR, API_KEY
from sync_github_repo     import ensure_repo, sync_with_remote
from legiscanner          import run_scan, CSV_FILE, KEYWORDS_FILE

# ── Corpus manager (Layer A — master bill corpus) ─────────────────────────────
# Guarded import: if corpus_manager.py is absent the app falls back gracefully.
try:
    from corpus_manager import CorpusManager as _CorpusManager
    _CORPUS_AVAILABLE = True
except ImportError:
    _CORPUS_AVAILABLE = False

ensure_repo()   # pull latest from remote on startup

DATA_FILE    = CSV_FILE
TRACKED_FILE = os.path.join(DATA_DIR, "tracked_bills.json")
NOTES_FILE   = os.path.join(DATA_DIR, "bill_notes.json")
EXPORT_FILE  = os.path.join(DATA_DIR, "Tracked_Bills_Export.csv")
VIEWS_FILE   = os.path.join(DATA_DIR, "saved_views.json")
UPLOAD_DIR   = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

st.set_page_config(page_title="SCCA Bill Tracker", layout="wide")

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
        return True
    except Exception as e:
        logger.error(f"Error saving keywords: {e}")
        st.error(f"Error saving keywords: {e}")
        return False


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
        return True
    except Exception as e:
        logger.error(f"Error saving views: {e}")
        st.error(f"Error saving views: {e}")
        return False

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
        return True
    except Exception as e:
        logger.error(f"Error saving tracked bills: {e}")
        st.error(f"Error saving tracked bills: {e}")
        return False


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
        return True
    except Exception as e:
        logger.error(f"Error saving notes: {e}")
        st.error(f"Error saving notes: {e}")
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
def create_summary_dashboard(df, tracked_bills, bill_notes, corpus=None):
    """Top-level metrics bar.  Shows corpus total when available."""
    col1, col2, col3, col4, col5 = st.columns(5)
    corpus_stats  = corpus.get_corpus_stats() if corpus else None
    total_bills   = corpus_stats["total_bills"] if corpus_stats else len(df)
    total_label   = "Corpus Bills" if corpus_stats else "Keyword Bills"
    high_priority = sum(1 for b in tracked_bills if bill_notes.get(b, {}).get("priority") == "High")
    support_count = sum(1 for b in tracked_bills if bill_notes.get(b, {}).get("position") == "Support")
    with col1: st.metric(total_label, f"{total_bills:,}")
    with col2: st.metric("Keyword Matches (CSV)", len(df))
    with col3: st.metric("Tracked Bills", len(tracked_bills))
    with col4: st.metric("High Priority", high_priority)
    with col5: st.metric("Support Position", support_count)


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

def _render_bill_card(row, raw_note: dict, bill_id: str,
                      bill_notes: dict, tracked_bills: list,
                      key_prefix: str) -> dict:
    """
    Render a single bill as a rich expandable card.
    Track / Untrack button is INSIDE the expander.
    Returns the (possibly updated) note dict.
    """
    note = _normalize_note(raw_note)
    # Build expander title
    status      = str(row.get('status_stage', ''))
    status_lbl  = STATUS_LEGEND.get(status, f"Status {status}") if status else 'Unknown'
    jur_level   = row.get('jurisdiction_level', '')
    jur_name    = row.get('jurisdiction_name', '')
    jur_icon    = '🏛️' if jur_level == 'Federal' else ('🗺️' if jur_level == 'State' else '🌐')
    kw_raw      = str(row.get('keyword', '') or '')
    kw_tags     = [t.strip() for t in kw_raw.replace(';', ',').split(',') if t.strip()]
    tracked_icon= '⭐ ' if bill_id in tracked_bills else ''
    pos_icon    = f" · 🏷 {note['position']}" if note.get('position') else ''
    prio_icon   = f" · 🔥 {note['priority']}" if note.get('priority') else ''

    disp_bill_number = row.get('bill_number', bill_id)
    title_line = (
        f"{tracked_icon}**{disp_bill_number}** — {row.get('title', 'No title')}  "
        f"· {jur_icon} {jur_name} · 📋 {status_lbl}{pos_icon}{prio_icon}"
    )

    with st.expander(title_line):
        # ── Info columns ────────────────────────────────────────────────────
        info1, info2 = st.columns(2)
        with info1:
            st.write(f"**Jurisdiction:** {jur_level} — {jur_name}")
            st.write(f"**Status:** {status_lbl} [{status}]")
            st.write(f"**Status Date:** {row.get('status_date', '—')}")
            st.write(f"**Session:** {row.get('session', '—')}")
        with info2:
            sponsors = str(row.get('sponsors', row.get('sponsor_names', '—')))
            committees = row.get('committees', row.get('committee', '—'))
            st.write(f"**Committee(s):** {committees}")
            
            # --- Legislative Linking ---
            st.caption("**Sponsors & Coauthors**")
            if sponsors != '—' and sponsors.strip():
                _sp_list = [s.strip() for s in sponsors.split(',') if s.strip()]
                if _sp_list:
                    sp_cols = st.columns(min(len(_sp_list), 3))
                    for idx, clean_sp in enumerate(_sp_list):
                        with sp_cols[idx % 3]:
                            def jump_prof(sp=clean_sp):
                                st.session_state.app_mode = "👔 Legislator Directory"
                                st.session_state.active_profile = sp
                            st.button(f"{clean_sp[:25]}", key=f"lnk_{key_prefix}_{idx}", help=f"View {clean_sp} Profile", on_click=jump_prof, type="tertiary", use_container_width=True)
            if kw_tags:
                badges = ' '.join([f'`{t}`' for t in kw_tags])
                st.write(f"**Keyword Tags:** {badges}")
            if row.get('url'):
                st.markdown(f"[📄 Bill Text & History]({row['url']})")

        # ── Summary / Last Action ────────────────────────────────────────────
        desc = row.get('description', '')
        if desc:
            st.write(f"**Summary:** {desc}")
        last_action = row.get('last_action', '')
        last_action_date = row.get('last_action_date', '')
        if last_action or last_action_date:
            st.write(f"**Last Action:** {last_action} ({last_action_date})")
        subjects = row.get('subjects', '')
        if subjects:
            st.write(f"**Subjects:** {subjects}")
        if note.get("last_reviewed"):
            try:
                _lr_dt = datetime.fromisoformat(note["last_reviewed"]).strftime("%Y-%m-%d %H:%M")
                st.caption(f"Last reviewed: {_lr_dt}")
            except:
                pass

        st.divider()

        # ── Notes & annotations ─────────────────────────────────────────────
        nc1, nc2 = st.columns(2)
        with nc1:
            new_comment = st.text_area(
                "💬 Notes / Comments",
                value=note.get("comment", ""),
                key=f"{key_prefix}_comment"
            )
            new_links = st.text_input(
                "🔗 Related Links (comma-separated)",
                value=", ".join(note.get("links", [])),
                key=f"{key_prefix}_links"
            )
        with nc2:
            position = st.selectbox(
                "🏷 Position", ["", "Support", "Oppose", "Watch", "Neutral", "No Position"],
                index=["", "Support", "Oppose", "Watch", "Neutral", "No Position"].index(note.get("position", "")),
                key=f"{key_prefix}_pos"
            )
            priority = st.selectbox(
                "🔥 Priority", ["", "High", "Medium", "Low"],
                index=["", "High", "Medium", "Low"].index(note.get("priority", "")),
                key=f"{key_prefix}_prio"
            )
        uploaded_file = st.file_uploader("📎 Attach PDF", type=["pdf"], key=f"{key_prefix}_upload")

        # ── Action buttons ──────────────────────────────────────────────────
        ba1, ba2, ba3 = st.columns(3)
        with ba1:
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

        with ba2:
            if bill_id not in tracked_bills:
                if st.button("➕ Track This Bill", key=f"{key_prefix}_track"):
                    try:
                        tracked_bills.append(bill_id)
                        if save_tracked(tracked_bills):
                            st.success(f"✅ Now tracking {bill_id}")
                            st.rerun()
                        else:
                            st.error("Failed to track bill")
                    except Exception as e:
                        st.error(f"Error tracking bill: {e}")
            else:
                if st.button("❌ Untrack", key=f"{key_prefix}_untrack"):
                    try:
                        tracked_bills.remove(bill_id)
                        if save_tracked(tracked_bills):
                            st.success(f"Untracked {bill_id}")
                            st.rerun()
                        else:
                            st.error("Failed to untrack bill")
                    except Exception as e:
                        st.error(f"Error untracking bill: {e}")

        with ba3:
            if row.get('url'):
                st.markdown(f"[🔗 Open on LegiScan]({row['url']})")

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
# Legacy compatibility: corpus is None when corpus_manager.py is absent or DB fails.
try:
    job_manager = JobManager(os.path.join(DATA_DIR, "jobs.db"))
except Exception as e:
    logger.error(f"Failed to load JobManager: {e}")
    job_manager = None

try:
    staff_manager = StaffManager(os.path.join(DATA_DIR, "staff.db"))
except Exception as e:
    logger.error(f"Failed to load StaffManager: {e}")
    staff_manager = None

corpus = None
if _CORPUS_AVAILABLE:
    try:
        corpus = _CorpusManager(os.path.join(DATA_DIR, "bills.db"), API_KEY)
    except Exception as _ce:
        logger.warning(f"CorpusManager init failed (non-fatal): {_ce}")


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# UNIFIED SIDEBAR COMMAND CENTER 
# ═══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🏛️ SCCA Bill Tracker")

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
    
    st.header("📊 System Status")
    if job_manager:
        r_jobs = job_manager.get_recent_jobs(1)
        st.caption(f"Last Job: {r_jobs[0]['job_type']} ({r_jobs[0]['status']})" if r_jobs else "Last Job: None")
        running_jobs = job_manager.get_running_jobs()
        if running_jobs:
            st.warning(f"⚠️ {len(running_jobs)} job(s) currently running!")
    if corpus:
        c_stats = corpus.get_corpus_stats()
        st.write(f"Corpus Size: {c_stats['total_bills']:,} bills")
    st.divider()

    st.header("👔 Staff Intelligence")
    try:
        import json
        with open(os.path.join(DATA_DIR, "staff_sources.json"), "r") as _sf:
            _scf = json.load(_sf)
    except: _scf = {}
    
    live_sources = [s for s in _scf.get('staff_sources', []) if s.get('enabled') and s.get('type') == 'google_sheet_live']
    
    if live_sources and staff_manager:
        prim = live_sources[0]
        st.write(f"Source: **{prim['label']}**")
        if st.button("🔄 Sync Live Capitol Matrix", type="primary", use_container_width=True):
            with st.spinner("Downloading live Google Sheet..."):
                ok, res = staff_manager.sync_live_sheet(prim['url'], DATA_DIR, prim.get('state', 'CA'))
                if ok:
                    st.success("Synced gracefully!")
                    st.rerun()
                else: 
                    st.error(f"Failed: {res}")
                
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
    
    _lock_rescan = bool(running_jobs and any(_j['job_type'] == 'keyword_rescan' for _j in running_jobs))
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
                    stats = run_rescan_job(corpus, list(set(_api_states)), DATA_DIR, job_manager, initiated_by="ui")
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
                
        _sel_label = st.selectbox("Session", options=list(_session_opts.keys()) if _session_opts else ["(none)"], key="corpus_session_select")
        _sel_session = _session_opts.get(_sel_label)
        
        st.markdown("**Incremental Refresh** (cheap, periodic)")
        _lock_refresh = bool(running_jobs and any(_j['job_type'] == 'incremental_refresh' for _j in running_jobs))
        if st.button("🔄 Refresh Session Updates", key="corpus_refresh", disabled=not _sel_session or _lock_refresh):
            _rb = st.progress(0, text="Refreshing...")
            run_refresh_job(corpus, _sel_session["session_id"], _sel_session["jurisdiction"], job_manager, lambda f, m: _rb.progress(min(f, 1.0), text=m))
            _rb.progress(1.0, text="Done")
            st.rerun()
            
        st.markdown("**Bulk Bootstrap** (expensive, initial load)")
        _lock_boot = bool(running_jobs and any(_j['job_type'] == 'bootstrap_corpus' for _j in running_jobs))
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
st.title("🏛️ SCCA Bill Tracker Workspace")
create_summary_dashboard(df, tracked_bills, bill_notes, corpus=corpus)
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
        
        st.caption(f"**ALL BILLS MODE** — Showing {len(db_df)} bills from Master Archive")
        for _, row in db_df.iterrows():
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
        
        st.caption(f"**KEYWORD MODE** — Showing {len(kw_df)} matches from the Rescan Cache")
        for _, row in kw_df.iterrows():
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
        
        st.caption(f"**TRACKED MODE** — Showing {len(tr_df)} Tracked Bills")
        for _, row in tr_df.iterrows():
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
            
        if st.session_state.get('active_profile'):
            aptr = st.session_state.active_profile
            
            def clear_prof():
                if 'active_profile' in st.session_state:
                    del st.session_state['active_profile']
                    
            st.button("⬅️ Back to Directory", on_click=clear_prof)
            st.divider()
            
            # Find the best match
            match = pd.DataFrame()
            if not leg_df.empty:
                match = leg_df[
                    leg_df['name'].str.contains(aptr, case=False, na=False) |
                    leg_df['normalized_name'].str.contains(aptr, case=False, na=False)
                ]
                
            if match.empty:
                st.info(f"Using Master Corpus profile for: {aptr}")
                st.caption("No custom Staff Intelligence mapping found. Fetching bills from master index.")
                l_id = None
                l_name = aptr
                l_party = "?"
                l_dist = "?"
                l_cham = "Legislator"
                l_norm = aptr
            else:
                lrow = match.iloc[0]
                l_id = lrow.get('legislator_id')
                l_name = lrow.get('name', 'Unknown')
                l_party = lrow.get('party', '')
                l_dist = lrow.get('district', '')
                l_cham = lrow.get('chamber', '')
                l_norm = lrow.get('normalized_name', '')
                
            st.subheader(f"{l_cham} {l_name} ({l_party}) — District {l_dist}")
            t_staff, t_issue, t_bills, t_cmte = st.tabs(["Capitol Staff", "Issue Assignments", "Sponsored Bills", "Committee Leadership"])
            
            with t_staff:
                if l_id:
                    staff_ls = staff_manager.get_legislator_staff(l_id)
                    if staff_ls: st.table(pd.DataFrame(staff_ls)[['role', 'name', 'email', 'office_type']])
                    else: st.caption("No staff loaded.")
                else: 
                    st.caption("No staff logic active for unmapped corpus legislators.")
            with t_cmte:
                if l_norm:
                    cmte_ls = staff_manager.get_legislator_committee_matrix(l_norm)
                    if cmte_ls: st.table(pd.DataFrame(cmte_ls))
                    else: st.caption("No primary committee leadership mapped.")
                else:
                    st.caption("Cannot resolve committee status.")
            with t_issue:
                if l_id:
                    iss_ls = staff_manager.get_legislator_issues(l_id)
                    if iss_ls: st.table(pd.DataFrame(iss_ls))
                    else: st.caption("No issues mapped.")
                else:
                    st.caption("No issue assignments mapped for unmapped corpus legislators.")
            with t_bills:
                if not df.empty and l_norm and 'sponsors' in df.columns:
                    s_bills = df[df['sponsors'].astype(str).str.lower().str.contains(str(l_norm).lower(), case=False, na=False)]
                    if s_bills.empty: st.caption("No indexed bills found.")
                    else: st.dataframe(s_bills[['bill_number', 'status_stage', 'title']], hide_index=True)
                        
        elif leg_df.empty:
            st.info("No legislators ingested yet.")
            st.caption("Expand the '⚙️ Admin & Database Tools' menu and use the '👔 Staff Intelligence' section to Sync Live Data from Google.")
            
        else: # Standard Directory View
            lf_c1, lf_c2 = st.columns(2)
            with lf_c1:
                search_leg = st.text_input("🔍 Search member name, party...", "")
            with lf_c2:
                cham_opt = ["All"] + list(leg_df['chamber'].dropna().unique())
                search_cham = st.selectbox("Chamber", cham_opt)
                
            if search_cham != "All": leg_df = leg_df[leg_df['chamber'] == search_cham]
            if search_leg:
                leg_df = leg_df[
                    leg_df['name'].str.contains(search_leg, case=False, na=False) |
                    leg_df['party'].str.contains(search_leg, case=False, na=False)
                ]
                
            st.caption(f"Showing {len(leg_df)} active members.")
            for _, lrow in leg_df.iterrows():
                l_name = lrow.get('name', 'Unknown')
                exp_title = f"{lrow.get('chamber')} {l_name} ({lrow.get('party')})"
                
                col1, col2 = st.columns([4, 1])
                col1.markdown(f"**{exp_title}**")
                
                # Using a callback for profile assignment
                def go_prof(n=l_name):
                    st.session_state.active_profile = n
                    
                col2.button("View Profile", key=f"dprof_{lrow.get('legislator_id')}", on_click=go_prof, args=(l_name,))
            st.divider()