import streamlit as st
import pandas as pd
from staff_manager import StaffManager, exact_or_partial_match, normalize_leg_name

def render_staff_diagnostics(staff_manager: StaffManager):
    """
    Renders the dedicated Staff Pipeline Diagnostics view.
    """
    st.header("🛠️ Staff Pipeline Diagnostics")
    st.write("End-to-end tracing for the Live Capitol Matrix ingestion framework.")
    
    # ── Database Integrity Metrics ──
    st.subheader("Global Database Metrics")
    m_col1, m_col2, m_col3, m_col4 = st.columns(4)
    with staff_manager._get_conn() as c:
        m_col1.metric("Ingested Legislators", c.execute("SELECT count(*) FROM legislators").fetchone()[0])
        m_col2.metric("Mapped Capitol Staff", c.execute("SELECT count(*) FROM legislator_staff").fetchone()[0])
        m_col3.metric("Mapped Issues", c.execute("SELECT count(*) FROM legislator_issue_assignments").fetchone()[0])
        m_col4.metric("Committee Staff", c.execute("SELECT count(*) FROM committee_staff").fetchone()[0])
    
    # ── Job History ──
    st.subheader("Ingestion Telemetry")
    jobs = staff_manager.get_job_history(limit=5)
    if not jobs:
        st.warning("No import jobs found on this instance.")
    else:
        j_df = pd.DataFrame(jobs)
        # Reorder columns
        cols = ["timestamp", "rows_processed", "rows_skipped", "unmatched_records", "errors", "job_id"]
        j_df = j_df[[c for c in cols if c in j_df.columns]]
        st.dataframe(j_df, hide_index=True)
        
    st.divider()
    
    # ── Match Profiler Simulator ──
    st.subheader("🔍 Name Resolution Simulator")
    st.caption("Type exactly what you see in the Corpus/Bill Sponsor string to verify how the engine attempts to resolve it against the Staff table.")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        test_str = st.text_input("Raw Corpus Name snippet:", placeholder="e.g. Villapudua")
    
    if test_str:
        # Load the cache dictionary dynamically
        try:
            leg_df = staff_manager.get_all_legislators()
            leg_cache = {row['normalized_name']: row['legislator_id'] for _, row in leg_df.iterrows()}
        except Exception as e:
            st.error(f"Cannot load cache: {e}")
            leg_cache = {}
            
        with col2:
            st.write("**Normalization Yield:**")
            norm_res = normalize_leg_name(test_str)
            st.code(norm_res if norm_res else "<Empty>")
            
        leg_id = exact_or_partial_match(test_str, leg_cache)
        if leg_id:
            st.success("✅ **Resolution Successful!**")
            hit = leg_df[leg_df['legislator_id'] == leg_id].iloc[0]
            st.json({
                "Matched Target": hit['name'],
                "Chamber": hit['chamber'],
                "District": hit['district'],
                "Stored Normalized Key": hit['normalized_name']
            })
        else:
            st.error("❌ **Resolution Failed.** This string will drop to the sparse Corpus-scraped profile.")
            
    st.divider()
    
    # ── Unmatched Rows Drop Log ──
    st.subheader("🗑️ Unmatched Rows Log")
    st.caption("Rows from the Google Sheet missing a mapped Legislator (often typos or cross-chamber sync errors).")
    drops = staff_manager.get_unmatched_rows(limit=50)
    
    if not drops:
        st.success("No dropped rows in the recent log buffer!")
    else:
        st.dataframe(pd.DataFrame(drops)[["id", "timestamp", "reason_unmatched", "raw_row_data"]], hide_index=True)
