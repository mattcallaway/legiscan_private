import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import subprocess
import logging

from sync_github_repo import ensure_repo, sync_with_remote

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REPO_DIR = ensure_repo()
DATA_FILE = os.path.join(REPO_DIR, "LegiScan_Enhanced_Full_Tracker.csv")
TRACKED_FILE = os.path.join(REPO_DIR, "tracked_bills.json")
KEYWORDS_FILE = os.path.join(REPO_DIR, "keywords.json")
NOTES_FILE = os.path.join(REPO_DIR, "bill_notes.json")
EXPORT_FILE = os.path.join(REPO_DIR, "Tracked_Bills_Export.csv")
UPLOAD_DIR = "uploads"
LEGISCAN_SCRIPT = "legiscan_comprehensive_tracker.py"

st.set_page_config(page_title="SCCA Bill Tracker", layout="wide")
os.makedirs(UPLOAD_DIR, exist_ok=True)

@st.cache_data
def load_data():
    """Load bill data with error handling"""
    try:
        if os.path.exists(DATA_FILE):
            df = pd.read_csv(DATA_FILE)
            logger.info(f"Loaded {len(df)} bills from data file")
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
    
    col1, col2, col3, col4 = st.columns(4)
    
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
    
    # Status breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Bills by Status")
        if not df.empty and 'status_stage' in df.columns:
            status_counts = df['status_stage'].value_counts()
            st.bar_chart(status_counts)
        else:
            st.info("No status data available")
    
    with col2:
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
    tracked_bills = load_tracked()
    bill_notes = load_notes()
    df = load_data()
except Exception as e:
    st.error(f"Critical error loading application data: {e}")
    st.stop()

# Sidebar
st.sidebar.subheader("➕ Add New Keyword")
new_keyword = st.sidebar.text_input("Enter a keyword")
if st.sidebar.button("Add Keyword"):
    if new_keyword and new_keyword not in keywords_list:
        keywords_list.append(new_keyword)
        if save_keywords(keywords_list):
            sync_with_remote()
            st.sidebar.success(f"Keyword '{new_keyword}' added.")
            st.rerun()
    elif new_keyword in keywords_list:
        st.sidebar.warning("Keyword already exists")
    else:
        st.sidebar.warning("Please enter a keyword")

if st.sidebar.button("🔄 Rescan Keywords and Update Bills"):
    try:
        with st.spinner("Rescanning bills... This may take a moment."):
            result = subprocess.run(["python", LEGISCAN_SCRIPT], capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                st.sidebar.success("Rescan complete. Bill data updated.")
                st.rerun()
            else:
                st.sidebar.error(f"Rescan failed: {result.stderr}")
                logger.error(f"Rescan script failed: {result.stderr}")
    except subprocess.TimeoutExpired:
        st.sidebar.error("Rescan timed out. Please try again.")
        logger.error("Rescan script timed out")
    except Exception as e:
        st.sidebar.error(f"Error running rescan: {e}")
        logger.error(f"Error running rescan script: {e}")

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

if not df.empty:
    keywords = st.sidebar.multiselect("Keyword Category", 
                                     sorted(df["keyword"].dropna().unique()) if "keyword" in df.columns else [],
                                     default=sorted(df["keyword"].dropna().unique()) if "keyword" in df.columns else [])
    
    # More flexible status stage selection
    all_status_stages = sorted(df["status_stage"].dropna().unique()) if "status_stage" in df.columns else []
    status_stage = st.sidebar.multiselect("Status Stage", 
                                         all_status_stages,
                                         default=all_status_stages)  # Default to all statuses
    
    # Add manual status input for cases where bills have statuses not in current data
    custom_status = st.sidebar.text_input("Add Custom Status (comma-separated)", 
                                         placeholder="e.g., Status 2, Status 3")
    if custom_status:
        custom_statuses = [s.strip() for s in custom_status.split(",") if s.strip()]
        status_stage.extend(custom_statuses)
    
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
        if keywords and "keyword" in df.columns:
            filtered_df = filtered_df[filtered_df["keyword"].isin(keywords)]
        if status_stage and "status_stage" in df.columns:
            filtered_df = filtered_df[filtered_df["status_stage"].isin(status_stage)]
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
            tagline = " | ".join(tags)

            with st.expander(f"{bill_id}: {row.get('title', 'No title')} ({tagline})"):
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
                tagline = " | ".join(tags)

                with st.expander(f"{bill_id}: {row.get('title', 'No title')} ({tagline})"):
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