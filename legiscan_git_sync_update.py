import streamlit as st
import pandas as pd
import os
import json
from datetime import datetime
import subprocess

from sync_github_repo import ensure_repo, sync_with_remote

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
    return pd.read_csv(DATA_FILE) if os.path.exists(DATA_FILE) else pd.DataFrame()

def load_keywords():
    if os.path.exists(KEYWORDS_FILE):
        with open(KEYWORDS_FILE, "r") as f:
            return json.load(f)
    return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]

def save_keywords(keywords):
    sync_with_remote()
    with open(KEYWORDS_FILE, "w") as f:
        json.dump(sorted(set(keywords)), f)

def load_tracked():
    if os.path.exists(TRACKED_FILE):
        with open(TRACKED_FILE, "r") as f:
            return json.load(f)
    return []

def save_tracked(tracked):
    sync_with_remote()
    with open(TRACKED_FILE, "w") as f:
        json.dump(tracked, f)

def load_notes():
    if os.path.exists(NOTES_FILE):
        with open(NOTES_FILE, "r") as f:
            return json.load(f)
    return {}

def save_notes(notes):
    sync_with_remote()
    with open(NOTES_FILE, "w") as f:
        json.dump(notes, f, indent=2)

keywords_list = load_keywords()
tracked_bills = load_tracked()
bill_notes = load_notes()

# Sidebar
st.sidebar.subheader("➕ Add New Keyword")
new_keyword = st.sidebar.text_input("Enter a keyword")
if st.sidebar.button("Add Keyword"):
    if new_keyword and new_keyword not in keywords_list:
        keywords_list.append(new_keyword)
        save_keywords(keywords_list)
        sync_with_remote()
        st.sidebar.success(f"Keyword '{new_keyword}' added.")

if st.sidebar.button("🔄 Rescan Keywords and Update Bills"):
    try:
        result = subprocess.run(["python", LEGISCAN_SCRIPT], capture_output=True, text=True)
        st.sidebar.success("Rescan complete. Bill data updated.")
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Error running script: {e}")

df = load_data()
if df.empty:
    st.warning("No data found. Please run a rescan to populate.")
    st.stop()

st.sidebar.title("🔍 Filter Options")
keywords = st.sidebar.multiselect("Keyword Category", sorted(df["keyword"].dropna().unique()), default=df["keyword"].dropna().unique())
status_stage = st.sidebar.multiselect("Status Stage", sorted(df["status_stage"].dropna().unique()), default=df["status_stage"].dropna().unique())
sponsors = st.sidebar.multiselect("Sponsors", sorted(df["sponsors"].dropna().unique()))
committees = st.sidebar.multiselect("Committees", sorted(df["committees"].dropna().unique()))
position_filter = st.sidebar.multiselect("Position", ["Support", "Oppose", "Watch"])
priority_filter = st.sidebar.multiselect("Priority", ["High", "Medium", "Low"])
date_range = st.sidebar.date_input("Introduced Date Range", [])

filtered_df = df[df["keyword"].isin(keywords) & df["status_stage"].isin(status_stage)]
if sponsors:
    filtered_df = filtered_df[filtered_df["sponsors"].isin(sponsors)]
if committees:
    filtered_df = filtered_df[filtered_df["committees"].isin(committees)]
if position_filter:
    filtered_df = filtered_df[filtered_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("position", "") in position_filter)]
if priority_filter:
    filtered_df = filtered_df[filtered_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("priority", "") in priority_filter)]
if len(date_range) == 2:
    start, end = date_range
    filtered_df["introduced_date"] = pd.to_datetime(filtered_df["introduced_date"], errors='coerce')
    filtered_df = filtered_df[
        (filtered_df["introduced_date"] >= pd.to_datetime(start)) &
        (filtered_df["introduced_date"] <= pd.to_datetime(end))
    ]

tab1, tab2 = st.tabs(["📋 All Bills", "⭐ Tracked Bills"])

with tab1:
    st.subheader("All Filtered Bills")
    for _, row in filtered_df.iterrows():
        bill_id = row['bill_number']
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

        with st.expander(f"{bill_id}: {row['title']} ({tagline})"):
            st.write(f"**Status Stage:** {row['status_stage']}")
            st.write(f"**Sponsor(s):** {row['sponsors']}")
            st.write(f"**Committee(s):** {row['committees']}")
            st.write(f"**Summary:** {row['description']}")
            st.write(f"**Last Action:** {row['last_action']} ({row['last_action_date']})")
            st.markdown(f"[📄 Full Text and History]({row['url']})")

            new_comment = st.text_area("💬 Notes/Comments", value=note.get("comment", ""), key=f"{bill_id}_comment")
            new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{bill_id}_links")
            position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch"], index=["", "Support", "Oppose", "Watch"].index(note.get("position", "")), key=f"{bill_id}_pos")
            priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{bill_id}_prio")
            uploaded_file = st.file_uploader("📎 Upload PDF", type=["pdf"], key=f"{bill_id}_upload")

            if st.button(f"💾 Save Notes for {bill_id}", key=f"{bill_id}_save"):
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
                save_notes(bill_notes)
                sync_with_remote()
                st.success(f"Saved notes for {bill_id}")

            if bill_id not in tracked_bills:
                if st.button(f"➕ Track {bill_id}", key=bill_id):
                    tracked_bills.append(bill_id)
                    save_tracked(tracked_bills)
                    sync_with_remote()
                    st.rerun()
            else:
                st.markdown("✅ Currently Tracked")

with tab2:
    st.subheader("Tracked Bills")
    if tracked_bills:
        for _, row in df[df["bill_number"].isin(tracked_bills)].iterrows():
            bill_id = row['bill_number']
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

            with st.expander(f"{bill_id}: {row['title']} ({tagline})"):
                st.write(f"**Status Stage:** {row['status_stage']}")
                st.write(f"**Sponsor(s):** {row['sponsors']}")
                st.write(f"**Committee(s):** {row['committees']}")
                st.write(f"**Summary:** {row['description']}")
                st.write(f"**Last Action:** {row['last_action']} ({row['last_action_date']})")
                st.markdown(f"[📄 Full Text and History]({row['url']})")

                new_comment = st.text_area("💬 Notes/Comments", value=note.get("comment", ""), key=f"{bill_id}_t_comment")
                new_links = st.text_input("🔗 Related Links (comma-separated)", value=", ".join(note.get("links", [])), key=f"{bill_id}_t_links")
                position = st.selectbox("🏷 Position", ["", "Support", "Oppose", "Watch"], index=["", "Support", "Oppose", "Watch"].index(note.get("position", "")), key=f"{bill_id}_t_pos")
                priority = st.selectbox("🔥 Priority", ["", "High", "Medium", "Low"], index=["", "High", "Medium", "Low"].index(note.get("priority", "")), key=f"{bill_id}_t_prio")
                uploaded_file = st.file_uploader("📎 Upload PDF", type=["pdf"], key=f"{bill_id}_t_upload")

                if st.button(f"💾 Save Notes for {bill_id}", key=f"{bill_id}_t_save"):
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
                    save_notes(bill_notes)
                    sync_with_remote()
                    st.success(f"Saved notes for {bill_id}")

        export_df = df[df["bill_number"].isin(tracked_bills)].copy()
        export_df["comments"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("comment", ""))
        export_df["links"] = export_df["bill_number"].apply(lambda x: ", ".join(bill_notes.get(x, {}).get("links", [])))
        export_df["files"] = export_df["bill_number"].apply(lambda x: "; ".join(os.path.basename(f) for f in bill_notes.get(x, {}).get("files", [])))
        export_df["position"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("position", ""))
        export_df["priority"] = export_df["bill_number"].apply(lambda x: bill_notes.get(x, {}).get("priority", ""))

        st.download_button("📥 Export Tracked Bills with Notes to CSV", data=export_df.to_csv(index=False), file_name=EXPORT_FILE, mime="text/csv")

        to_remove = st.multiselect("Remove from Tracked List", tracked_bills)
        if st.button("Remove Selected"):
            tracked_bills = [b for b in tracked_bills if b not in to_remove]
            save_tracked(tracked_bills)
            sync_with_remote()
            st.success("Selected bills removed.")
            st.rerun()
    else:
        st.info("No bills currently tracked.")