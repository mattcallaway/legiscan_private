import streamlit as st
import pandas as pd
from staff_manager import StaffManager, resolve_legislator, normalize_name_components

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
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        test_str = st.text_input("Name snippet:", placeholder="e.g. Cabaldon")
    with col2:
        test_cham = st.selectbox("Inferred Chamber:", ["", "Senate", "Assembly"])
    with col3:
        test_dist = st.text_input("Raw District:", placeholder="e.g. SD03")
        
    if test_str:
        try:
            leg_df = staff_manager.get_all_legislators()
        except:
            leg_df = pd.DataFrame()
            
        with col4:
            st.write("**Normalization Yield:**")
            n_comps = normalize_name_components(test_str)
            st.code(n_comps['full'] if n_comps['full'] else "<Empty>")
            
        leg_id, rsn = resolve_legislator(leg_df, n_comps['full'], n_comps['last'], test_cham, test_dist)
        if leg_id:
            st.success(f"✅ **Resolution Successful!** ({rsn})")
            hit = leg_df[leg_df['legislator_id'] == leg_id].iloc[0]
            st.json({
                "Matched Target": hit['name'],
                "Chamber": hit['chamber'],
                "District": hit['district'],
                "Stored Normalized Key": hit['normalized_name'],
                "Canonical Key": hit.get('canonical_legislator_key', 'N/A')
            })
        else:
            st.error(f"❌ **Resolution Failed.** Reason: {rsn}")
            
    st.divider()
    
    # ── Unmatched Rows Drop Log ──
    st.subheader("🗑️ Unmatched Rows Log")
    st.caption("Rows from the Google Sheet missing a mapped Legislator (often typos or cross-chamber sync errors).")
    drops = staff_manager.get_unmatched_rows(limit=50)
    
    if not drops:
        st.success("No dropped rows in the recent log buffer!")
    else:
        st.dataframe(pd.DataFrame(drops)[["id", "timestamp", "reason_unmatched", "raw_row_data"]], hide_index=True)
