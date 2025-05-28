import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from config               import DATA_DIR
from sync_github_repo     import ensure_repo, sync_with_remote
from legi_scan_scanner    import run_scan, CSV_FILE, KEYWORDS_FILE

ensure_repo()   # still make sure the repo is pulled

DATA_FILE    = CSV_FILE
TRACKED_FILE = os.path.join(DATA_DIR, "tracked_bills.json")
NOTES_FILE   = os.path.join(DATA_DIR, "bill_notes.json")
EXPORT_FILE  = os.path.join(DATA_DIR, "Tracked_Bills_Export.csv")
UPLOAD_DIR   = os.path.join(DATA_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


st.set_page_config(page_title="SCCA Bill Tracker", layout="wide")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# State and Federal Configuration
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
    'US_HOUSE': 'U.S. House of Representatives',
    'US_SENATE': 'U.S. Senate',
    'CONGRESS': 'U.S. Congress (Both Chambers)',
    'FEDERAL_AGENCY': 'Federal Agency Rules/Regulations'
}

STATUS_LEGEND = {
    "1": "Introduced",
    "2": "In Committee",
    "3": "Reported",
    "4": "Passed One Chamber",
    "5": "Passed Both Chambers",
    "6": "Signed by Governor",
    "7": "Vetoed",
    "8": "Failed",
    "9": "Withdrawn",
    "10": "Dead"
}
# Default jurisdiction levels if needed during script runs
jurisdiction_levels = ["All", "State", "Federal"]

# Ensure selected_states is always defined
if "selected_states" not in st.session_state:
    st.session_state.selected_states = []
selected_states = st.session_state.selected_states

# Ensure selected_federal_types is always defined
if "selected_federal_types" not in st.session_state:
    st.session_state.selected_federal_types = []
selected_federal_types = st.session_state.selected_federal_types

def regenerate_friendly_status_options():
    st.session_state.friendly_status_options = [
        f"{STATUS_LEGEND.get(s, s)} [{s}]" for s in st.session_state.status_options
    ]
    st.session_state.friendly_to_code = {
        f"{STATUS_LEGEND.get(s, s)} [{s}]": s for s in st.session_state.status_options
    }


def get_jurisdiction_from_bill_number(bill_number):
    """Extract jurisdiction from bill number"""
    if not bill_number:
        return 'Unknown', 'Unknown'
    
    bill_number = str(bill_number).upper()
    
    # Federal patterns
    if any(pattern in bill_number for pattern in ['HR', 'S.', 'H.R.', 'HJ', 'SJ', 'HC', 'SC']):
        if bill_number.startswith(('HR', 'H.R.', 'HJ', 'HC')):
            return 'Federal', 'U.S. House of Representatives'
        elif bill_number.startswith(('S.', 'SJ', 'SC')):
            return 'Federal', 'U.S. Senate'
        else:
            return 'Federal', 'U.S. Congress'
    
    # State patterns - look for state prefixes
    for state_code, state_name in US_STATES.items():
        if bill_number.startswith(f'{state_code}-') or f'{state_code}' in bill_number[:4]:
            return 'State', state_name
    
    # Common state bill patterns
    state_patterns = {
        'AB': 'California', 'SB': 'California', 'ACR': 'California', 'SCR': 'California',
        'HB': 'Various States', 'SB': 'Various States'
    }
    
    for pattern, state in state_patterns.items():
        if bill_number.startswith(pattern):
            if pattern in ['AB', 'ACR', 'SCR'] and state == 'California':
                return 'State', 'California'
            else:
                return 'State', 'Various States'
    
    return 'Unknown', 'Unknown'

def apply_jurisdiction_columns(df, func):
    if 'bill_number' not in df.columns:
        return df

    # Initialize new columns
    df['jurisdiction_level'] = None
    df['jurisdiction_name'] = None

    # Apply function safely
    for i, val in df['bill_number'].dropna().items():
        result = func(val)
        if isinstance(result, (list, tuple)) and len(result) == 2:
            df.at[i, 'jurisdiction_level'] = result[0]
            df.at[i, 'jurisdiction_name'] = result[1]
        else:
            df.at[i, 'jurisdiction_level'] = 'Unknown'
            df.at[i, 'jurisdiction_name'] = 'Unknown'
    return df

@st.cache_data
def load_data():
    """Load bill data with error handling"""
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            logger.info(f"Loaded {len(df)} bills from data file")
            
            # Add jurisdiction columns if they don't exist
            if 'jurisdiction_level' not in df.columns or 'jurisdiction_name' not in df.columns:
                df = apply_jurisdiction_columns(df, get_jurisdiction_from_bill_number)


            
            return df
        else:
            logger.warning("Data file not found")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        st.error(f"Error loading bill data: {e}")
        return pd.DataFrame()

def load_keywords():
    """Load keywords with error handling"""
    try:
        if os.path.exists(KEYWORDS_FILE):
            with open(KEYWORDS_FILE, "r") as f:
                keywords = json.load(f)
                logger.info(f"Loaded {len(keywords)} keywords")
                return keywords
        else:
            default_keywords = ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]
            logger.info("Using default keywords")
            return default_keywords
    except Exception as e:
        logger.error(f"Error loading keywords: {e}")
        st.error(f"Error loading keywords: {e}")
        return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]

def save_keywords(keywords):
    """Save keywords with error handling"""
    try:
        sync_with_remote()
        with open(KEYWORDS_FILE, "w") as f:
            json.dump(sorted(set(keywords)), f)
        logger.info(f"Saved {len(keywords)} keywords")
        return True
    except Exception as e:
        logger.error(f"Error saving keywords: {e}")
        st.error(f"Error saving keywords: {e}")
        return False

def load_tracked():
    """Load tracked bills with error handling"""
    try:
        if os.path.exists(TRACKED_FILE):
            with open(TRACKED_FILE, "r") as f:
                tracked = json.load(f)
                logger.info(f"Loaded {len(tracked)} tracked bills")
                return tracked
        else:
            logger.info("No tracked bills file found")
            return []
    except Exception as e:
        logger.error(f"Error loading tracked bills: {e}")
        st.error(f"Error loading tracked bills: {e}")
        return []

def save_tracked(tracked):
    """Save tracked bills with error handling"""
    try:
        sync_with_remote()
        with open(TRACKED_FILE, "w") as f:
            json.dump(tracked, f)
        logger.info(f"Saved {len(tracked)} tracked bills")
        return True
    except Exception as e:
        logger.error(f"Error saving tracked bills: {e}")
        st.error(f"Error saving tracked bills: {e}")
        return False

def load_notes():
    """Load bill notes with error handling"""
    try:
        if os.path.exists(NOTES_FILE):
            with open(NOTES_FILE, "r") as f:
                notes = json.load(f)
                logger.info(f"Loaded notes for {len(notes)} bills")
                return notes
        else:
            logger.info("No notes file found")
            return {}
    except Exception as e:
        logger.error(f"Error loading notes: {e}")
        st.error(f"Error loading notes: {e}")
        return {}

def save_notes(notes):
    """Save bill notes with error handling"""
    try:
        sync_with_remote()
        with open(NOTES_FILE, "w") as f:
            json.dump(notes, f, indent=2)
        logger.info(f"Saved notes for {len(notes)} bills")
        return True
    except Exception as e:
        logger.error(f"Error saving notes: {e}")
        st.error(f"Error saving notes: {e}")
        return False

def search_bills(df, search_term):
    """Search bills by title, sponsor, description, or bill number"""
    if not search_term:
        return df
    
    search_term = search_term.lower()
    mask = (
        df['title'].str.lower().str.contains(search_term, na=False) |
        df['sponsors'].str.lower().str.contains(search_term, na=False) |
        df['description'].str.lower().str.contains(search_term, na=False) |
        df['bill_number'].str.lower().str.contains(search_term, na=False) |
        df['committees'].str.lower().str.contains(search_term, na=False)
    )
    return df[mask]

def create_summary_dashboard(df, tracked_bills, bill_notes):
    """Create a summary dashboard with key metrics"""
    st.subheader("📊 Dashboard Summary")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Bills", len(df))
    
    with col2:
        st.metric("Tracked Bills", len(tracked_bills))
    
    with col3:
        high_priority_count = sum(1 for bill_id in tracked_bills 
                                 if bill_notes.get(bill_id, {}).get("priority") == "High")
        st.metric("High Priority", high_priority_count)
    
    with col4:
        support_count = sum(1 for bill_id in tracked_bills 
                           if bill_notes.get(bill_id, {}).get("position") == "Support")
        st.metric("Support Position", support_count)
    
    with col5:
        federal_count = len(df[df['jurisdiction_level'] == 'Federal']) if 'jurisdiction_level' in df.columns else 0
        st.metric("Federal Bills", federal_count)
    
    # Jurisdiction and Status breakdown
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("🏛️ Bills by Jurisdiction")
        if not df.empty and 'jurisdiction_level' in df.columns:
            jurisdiction_counts = df['jurisdiction_level'].value_counts()
            st.bar_chart(jurisdiction_counts)
        else:
            st.info("No jurisdiction data available")
    
    with col2:
        st.subheader("📈 Bills by Status")
        if not df.empty and 'status_stage' in df.columns:
            status_counts = df['status_stage'].value_counts()
            st.bar_chart(status_counts)
        else:
            st.info("No status data available")
    
    with col3:
        st.subheader("🏷️ Tracked Bills by Position")
        if tracked_bills:
            positions = [bill_notes.get(bill_id, {}).get("position", "Unassigned") 
                        for bill_id in tracked_bills]
            position_counts = pd.Series(positions).value_counts()
            st.bar_chart(position_counts)
        else:
            st.info("No tracked bills")

# Load data with error handling
try:
    keywords_list = load_keywords()
    tracked_bills  = load_tracked()
    bill_notes     = load_notes()
    df             = load_data()
except Exception as e:
    st.error(f"Critical error loading application data: {e}")
    st.stop()

# Initialize the working copy
filtered_df = df.copy()

# ─── Sidebar controls ───
st.sidebar.title("SCCA Bill Tracker")
st.sidebar.header("Controls")

# ←– RESCAN BUTTON
if st.sidebar.button("🔄 Rescan"):
    if not selected_states and not selected_federal_types:
        st.sidebar.warning("Please select at least one jurisdiction.")
    else:
        with st.spinner("Rescanning bills…"):
            try:
                run_scan(states=selected_states + selected_federal_types, data_dir=DATA_DIR)
                sync_with_remote()
                st.sidebar.success("Rescan complete!")
                st.experimental_rerun()
            except Exception as e:
                st.sidebar.error(f"Rescan failed: {e}")
                logger.error(f"Error during rescan: {e}")

# If there’s no data yet, prompt to scan
if df.empty:
    st.warning("No data found. Please run a rescan to populate.")
    st.stop()
    
# Main content
st.title("🏛️ SCCA Bill Tracker")

# Dashboard Summary
create_summary_dashboard(df, tracked_bills, bill_notes)

st.divider()

# Search functionality
st.subheader("🔍 Search Bills")
search_term = st.text_input("Search by bill number, title, sponsor, description, or committee:", 
                           placeholder="e.g. AB-123, climate change, John Smith")

# Apply search filter
if search_term:
    df = search_bills(df, search_term)
    st.info(f"Found {len(df)} bills matching '{search_term}'")

# Sidebar filters
st.sidebar.title("🔍 Filter Options")

# Add option to show all tracked bills regardless of other filters
show_all_tracked = st.sidebar.checkbox("Show All Tracked Bills (ignore filters)", value=False)

# NEW: State and Federal Legislation Selectors
st.sidebar.subheader("🏛️ Jurisdiction Filters")

# Jurisdiction Level Selector
jurisdiction_levels = st.sidebar.multiselect(
    "Jurisdiction Level",
    options=["Federal", "State", "Unknown"],
    default=["Federal", "State", "Unknown"],
    help="Filter by federal vs state legislation"
)

# State Selector (only show if State is selected in jurisdiction levels)
selected_states = []
if "State" in jurisdiction_levels:
    # Get available states from data
    available_states = []
    if not df.empty and 'jurisdiction_name' in df.columns:
        state_bills = df[df['jurisdiction_level'] == 'State']
        available_states = sorted(state_bills['jurisdiction_name'].dropna().unique())
    
    selected_states = st.sidebar.multiselect(
        "🗺️ Select States",
        options=available_states,
        default=available_states,
        help="Select specific states to include"
    )

# Federal Legislature Selector (only show if Federal is selected)
selected_federal_types = []
if "Federal" in jurisdiction_levels:
    # Get available federal types from data
    available_federal = []
    if not df.empty and 'jurisdiction_name' in df.columns:
        federal_bills = df[df['jurisdiction_level'] == 'Federal']
        available_federal = sorted(federal_bills['jurisdiction_name'].dropna().unique())
    
    # If no specific federal data, use default options
    if not available_federal:
        available_federal = list(FEDERAL_TYPES.values())
    
    selected_federal_types = st.sidebar.multiselect(
        "🏛️ Federal Legislature",
        options=available_federal,
        default=available_federal,
        help="Select House, Senate, or both"
    )

# Show current jurisdiction summary
with st.sidebar.expander("📊 Current Jurisdiction Summary"):
    if not df.empty and 'jurisdiction_level' in df.columns:
        summary = df['jurisdiction_level'].value_counts()
        for level, count in summary.items():
            st.write(f"• {level}: {count} bills")
    else:
        st.write("No jurisdiction data available")

# Rest of existing filters
if not df.empty:
    keywords = st.sidebar.multiselect("Keyword Category", 
                                     sorted(df["keyword"].dropna().unique()) if "keyword" in df.columns else [],
                                     default=sorted(df["keyword"].dropna().unique()) if "keyword" in df.columns else [])
    

# Initialize session state only once
if "status_options" not in st.session_state:
    all_status_stages_raw = sorted(df["status_stage"].dropna().astype(str).unique())
    st.session_state.status_options = all_status_stages_raw
    regenerate_friendly_status_options()

# UI: Add New Status
with st.sidebar.expander("➕ Add Custom Status Code"):
    new_status_code = st.text_input("Status Code (number)")
    new_status_label = st.text_input("Status Label")
    if st.button("Add Status"):
        if new_status_code and new_status_code not in st.session_state.status_options:
            st.session_state.status_options.append(new_status_code)
            STATUS_LEGEND[new_status_code] = new_status_label or new_status_code
            regenerate_friendly_status_options()
            st.success(f"Added new status {new_status_code}: {new_status_label}")

    friendly_status_options = [f"{STATUS_LEGEND.get(s, s)} [{s}]" for s in st.session_state.status_options]
    friendly_to_code = {f"{STATUS_LEGEND.get(s, s)} [{s}]": s for s in st.session_state.status_options}

selected_statuses_friendly = st.sidebar.multiselect(
    "Status Stage",
    options=friendly_status_options,
    default=friendly_status_options,
    key="status_stage_select_friendly"
)

# Convert selection back to code strings
status_stage = [friendly_to_code[f] for f in selected_statuses_friendly]
# Optional: Add a visible legend below the sidebar filters
with st.sidebar.expander("📘 Status Code Legend"):
    for code, label in STATUS_LEGEND.items():
        st.write(f"**{code}** — {label}")
# ─── 2) Persist status options in session_state ───
if "status_options" not in st.session_state:
    # store everything as strings so custom entries fit in cleanly
    st.session_state.status_options = [str(s) for s in all_status_stages_raw]

# ─── 3) Use that session_state list for your multiselect ───
status_stage = st.sidebar.multiselect(
    "Status Stage",
    options=st.session_state.status_options,
    default=st.session_state.status_options,
    key="status_stage_select"
)

# ─── 4) Allow adding custom statuses ───
st.sidebar.subheader("Add Custom Status")
custom_status = st.sidebar.text_input(
    "Enter custom status names (comma-separated)",
    placeholder="e.g., 2, 3, 4",
    key="custom_status_input"
)
if st.sidebar.button("Add Custom Statuses", key="add_custom_status_button"):
    new_statuses = [s.strip() for s in custom_status.split(",") if s.strip()]
    for s in new_statuses:
        if s not in st.session_state.status_options:
            st.session_state.status_options.append(s)
    st.rerun()

# ─── (Optional) Show current selections ───
with st.sidebar.expander("Current Status Filters"):
    for s in status_stage:
        st.write(f"• {s}")

# ─── (Optional) Reset back to defaults ───
if st.sidebar.button("Reset Status Filters", key="reset_status_filters"):
    st.session_state.status_options = [str(s) for s in all_status_stages_raw]
    st.rerun()

    
    # These need to be at the same indentation level as the other filters above
    sponsors = st.sidebar.multiselect("Sponsors", 
                                     sorted(df["sponsors"].dropna().unique()) if "sponsors" in df.columns else [])
    committees = st.sidebar.multiselect("Committees", 
                                       sorted(df["committees"].dropna().unique()) if "committees" in df.columns else [])
    position_filter = st.sidebar.multiselect("Position", ["Support", "Oppose", "Watch"])
    priority_filter = st.sidebar.multiselect("Priority", ["High", "Medium", "Low"])
    date_range = st.sidebar.date_input("Introduced Date Range", [])

    # Apply filters (but not to tracked bills if show_all_tracked is enabled)
    filtered_df = df.copy()
    
    if not show_all_tracked:
        # Apply jurisdiction filters
        if jurisdiction_levels and 'jurisdiction_level' in df.columns:
            filtered_df = filtered_df[filtered_df["jurisdiction_level"].isin(jurisdiction_levels)]
        
        # Apply state-specific filters
        if "State" in jurisdiction_levels and selected_states and 'jurisdiction_name' in df.columns:
            state_mask = (filtered_df['jurisdiction_level'] == 'State') & (filtered_df['jurisdiction_name'].isin(selected_states))
            non_state_mask = filtered_df['jurisdiction_level'] != 'State'
            filtered_df = filtered_df[state_mask | non_state_mask]
        
        # Apply federal-specific filters
        if "Federal" in jurisdiction_levels and selected_federal_types and 'jurisdiction_name' in df.columns:
            federal_mask = (filtered_df['jurisdiction_level'] == 'Federal') & (filtered_df['jurisdiction_name'].isin(selected_federal_types))
            non_federal_mask = filtered_df['jurisdiction_level'] != 'Federal'
            filtered_df = filtered_df[federal_mask | non_federal_mask]
        
        # Apply other existing filters
        if keywords and "keyword" in df.columns:
            filtered_df = filtered_df[filtered_df["keyword"].isin(keywords)]
        if status_stage and "status_stage" in df.columns:
            filtered_df = filtered_df[
            filtered_df["status_stage"]
                   .astype(str)
                   .isin(status_stage)
    ]
        if sponsors and "sponsors" in df.columns:
            filtered_df = filtered_df[filtered_df["sponsors"].isin(sponsors)]
        if committees and "committees" in df.columns:
            filtered_df = filtered_df[filtered_df["committees"].isin(committees)]
        if position_filter:
            filtered_df = filtered_df[filtered_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("position", "") in position_filter)]
        if priority_filter:
            filtered_df = filtered_df[filtered_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("priority", "") in priority_filter)]
        if len(date_range) == 2 and "introduced_date" in df.columns:
            start, end = date_range
            filtered_df["introduced_date"] = pd.to_datetime(filtered_df["introduced_date"], errors='coerce')
            filtered_df = filtered_df[
                (filtered_df["introduced_date"] >= pd.to_datetime(start)) &
                (filtered_df["introduced_date"] <= pd.to_datetime(end))
            ]
    else:
        # If showing all tracked bills, only apply search filter
        pass

tab1, tab2 = st.tabs(["📋 All Bills", "⭐ Tracked Bills"])

with tab1:
    st.subheader(f"All Bills ({len(filtered_df)} found)")
    
    if filtered_df.empty:
        st.info("No bills match your current filters.")
    else:
        for _, row in filtered_df.iterrows():
            bill_id = row.get('bill_number', 'Unknown')
            note = bill_notes.get(bill_id, {
                "comment": "", "links": [], "files": [],
                "position": "", "priority": ""
            })

            tags = []
            if note.get("position"):
                tags.append(f"🏷 {note['position']}")
            if note.get("priority"):
                tags.append(f"🔥 {note['priority']}")
            
            # Add jurisdiction info to tags
            jurisdiction_level = row.get('jurisdiction_level', 'Unknown')
            jurisdiction_name = row.get('jurisdiction_name', 'Unknown')
            if jurisdiction_level == 'Federal':
                tags.append(f"🏛️ Federal")
            elif jurisdiction_level == 'State':
                tags.append(f"🗺️ {jurisdiction_name}")
            
            tagline = " | ".join(tags)

            with st.expander(f"{bill_id}: {row.get('title', 'No title')} ({tagline})"):
                st.write(f"**Jurisdiction:** {jurisdiction_level} - {jurisdiction_name}")
                st.write(f"**Status Stage:** {row.get('status_stage', 'Unknown')}")
                st.write(f"**Sponsor(s):** {row.get('sponsors', 'Unknown')}")
                st.write(f"**Committee(s):** {row.get('committees', 'Unknown')}")
                st.write(f"**Summary:** {row.get('description', 'No description available')}")
                st.write(f"**Last Action:** {row.get('last_action', 'Unknown')} ({row.get('last_action_date', 'Unknown')})")
                if row.get('url'):
                    st.markdown(f"[📄 Full Text and History]({row['url']})")

                new_comment = st.text_area("💬 Notes/Comments", value=note.get("comment", ""), key=f"{bill_id}_comment")
                new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{bill_id}_links")
                position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch"], index=["", "Support", "Oppose", "Watch"].index(note.get("position", "")), key=f"{bill_id}_pos")
                priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{bill_id}_prio")
                uploaded_file = st.file_uploader("📎 Upload PDF", type=["pdf"], key=f"{bill_id}_upload")

                if st.button(f"💾 Save Notes for {bill_id}", key=f"{bill_id}_save"):
                    try:
                        note["comment"] = new_comment
                        note["links"] = [x.strip() for x in new_links.split(",") if x.strip()]
                        note["position"] = position
                        note["priority"] = priority
                        if uploaded_file:
                            file_path = os.path.join(UPLOAD_DIR, f"{bill_id}_{uploaded_file.name}")
                            with open(file_path, "wb") as f:
                                f.write(uploaded_file.getbuffer())
                            note.setdefault("files", []).append(file_path)
                        bill_notes[bill_id] = note
                        if save_notes(bill_notes):
                            sync_with_remote()
                            st.success(f"Saved notes for {bill_id}")
                        else:
                            st.error(f"Failed to save notes for {bill_id}")
                    except Exception as e:
                        st.error(f"Error saving notes: {e}")
                        logger.error(f"Error saving notes for {bill_id}: {e}")

                if bill_id not in tracked_bills:
                    if st.button(f"➕ Track {bill_id}", key=bill_id):
                        try:
                            tracked_bills.append(bill_id)
                            if save_tracked(tracked_bills):
                                sync_with_remote()
                                st.success(f"Now tracking {bill_id}")
                                st.rerun()
                            else:
                                st.error(f"Failed to track {bill_id}")
                        except Exception as e:
                            st.error(f"Error tracking bill: {e}")
                            logger.error(f"Error tracking {bill_id}: {e}")
                else:
                    st.markdown("✅ Currently Tracked")

with tab2:
    st.subheader("Tracked Bills")
    if tracked_bills:
        # For tracked bills, show all tracked bills if option is selected, otherwise apply filters
        if show_all_tracked:
            # Show all tracked bills regardless of other filters
            tracked_df = df[df["bill_number"].isin(tracked_bills)] if not df.empty else pd.DataFrame()
            st.info("Showing all tracked bills (filters ignored)")
        else:
            # Apply filters to tracked bills
            tracked_df = filtered_df[filtered_df["bill_number"].isin(tracked_bills)] if not filtered_df.empty else pd.DataFrame()
        
        st.write(f"**Tracked Bills Found:** {len(tracked_df)} of {len(tracked_bills)} total tracked bills")
        
        # Show which tracked bills are missing from current data
        if not df.empty:
            missing_bills = [bill for bill in tracked_bills if bill not in df["bill_number"].values]
            if missing_bills:
                with st.expander(f"⚠️ {len(missing_bills)} tracked bills not found in current data"):
                    st.write("These bills may have different status stages or may not be in the current dataset:")
                    for bill in missing_bills:
                        note = bill_notes.get(bill, {})
                        st.write(f"- **{bill}**: {note.get('comment', 'No notes')}")
        
        if tracked_df.empty and tracked_bills:
            st.warning("No tracked bills match your current filters. Try:")
            st.write("- Check the 'Show All Tracked Bills' option above")
            st.write("- Add custom status stages if your bills have different statuses")
            st.write("- Clear some filters to see more results")
        elif not tracked_df.empty:
            for _, row in tracked_df.iterrows():
                bill_id = row.get('bill_number', 'Unknown')
                note = bill_notes.get(bill_id, {
                    "comment": "", "links": [], "files": [],
                    "position": "", "priority": ""
                })

                tags = []
                if note.get("position"):
                    tags.append(f"🏷 {note['position']}")
                if note.get("priority"):
                    tags.append(f"🔥 {note['priority']}")
                
                # Add jurisdiction info to tags
                jurisdiction_level = row.get('jurisdiction_level', 'Unknown')
                jurisdiction_name = row.get('jurisdiction_name', 'Unknown')
                if jurisdiction_level == 'Federal':
                    tags.append(f"🏛️ Federal")
                elif jurisdiction_level == 'State':
                    tags.append(f"🗺️ {jurisdiction_name}")
                
                tagline = " | ".join(tags)

                with st.expander(f"{bill_id}: {row.get('title', 'No title')} ({tagline})"):
                    st.write(f"**Jurisdiction:** {jurisdiction_level} - {jurisdiction_name}")
                    st.write(f"**Status Stage:** {row.get('status_stage', 'Unknown')}")
                    st.write(f"**Sponsor(s):** {row.get('sponsors', 'Unknown')}")
                    st.write(f"**Committee(s):** {row.get('committees', 'Unknown')}")
                    st.write(f"**Summary:** {row.get('description', 'No description available')}")
                    st.write(f"**Last Action:** {row.get('last_action', 'Unknown')} ({row.get('last_action_date', 'Unknown')})")
                    if row.get('url'):
                        st.markdown(f"[📄 Full Text and History]({row['url']})")

                    new_comment = st.text_area("💬 Notes/Comments", value=note.get("comment", ""), key=f"{bill_id}_t_comment")
                    new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{bill_id}_t_links")
                    position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch"], index=["", "Support", "Oppose", "Watch"].index(note.get("position", "")), key=f"{bill_id}_t_pos")
                    priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{bill_id}_t_prio")
                    uploaded_file = st.file_uploader("📎 Upload PDF", type=["pdf"], key=f"{bill_id}_t_upload")

                    if st.button(f"💾 Save Notes for {bill_id}", key=f"{bill_id}_t_save"):
                        try:
                            note["comment"] = new_comment
                            note["links"] = [x.strip() for x in new_links.split(",") if x.strip()]
                            note["position"] = position
                            note["priority"] = priority
                            if uploaded_file:
                                file_path = os.path.join(UPLOAD_DIR, f"{bill_id}_{uploaded_file.name}")
                                with open(file_path, "wb") as f:
                                    f.write(uploaded_file.getbuffer())
                                note.setdefault("files", []).append(file_path)
                            bill_notes[bill_id] = note
                            if save_notes(bill_notes):
                                sync_with_remote()
                                st.success(f"Saved notes for {bill_id}")
                            else:
                                st.error(f"Failed to save notes for {bill_id}")
                        except Exception as e:
                            st.error(f"Error saving notes: {e}")
                            logger.error(f"Error saving notes for {bill_id}: {e}")

        # Export functionality
        if tracked_bills:
            try:
                # For export, use all tracked bills data regardless of filters
                export_df = df[df["bill_number"].isin(tracked_bills)].copy() if not df.empty else pd.DataFrame()
                if not export_df.empty:
                    export_df["comments"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("comment", ""))
                    export_df["links"] = export_df["bill_number"].apply(lambda x: ", ".join(bill_notes.get(x, {}).get("links", [])))
                    export_df["files"] = export_df["bill_number"].apply(lambda x: "; ".join(os.path.basename(f) for f in bill_notes.get(x, {}).get("files", [])))
                    export_df["position"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("position", ""))
                    export_df["priority"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("priority", ""))

                    st.download_button("📥 Export Tracked Bills with Notes to CSV", 
                                     data=export_df.to_csv(index=False), 
                                     file_name="Tracked_Bills_Export.csv", 
                                     mime="text/csv")
                else:
                    st.info("No tracked bill data available for export. Bills may need to be rescanned.")
            except Exception as e:
                st.error(f"Error preparing export: {e}")
                logger.error(f"Error preparing export: {e}")

        # Remove tracked bills
        if tracked_bills:
            to_remove = st.multiselect("Remove from Tracked List", tracked_bills)
            if st.button("Remove Selected") and to_remove:
                try:
                    tracked_bills = [b for b in tracked_bills if b not in to_remove]
                    if save_tracked(tracked_bills):
                        sync_with_remote()
                        st.success("Selected bills removed.")
                        st.rerun()
                    else:
                        st.error("Failed to remove selected bills")
                except Exception as e:
                    st.error(f"Error removing bills: {e}")
                    logger.error(f"Error removing tracked bills: {e}")
    else:
        st.info("No bills currently tracked.")