import logging
from typing import Optional, Callable
from job_manager import JobManager
from legiscanner import run_scan

logger = logging.getLogger(__name__)

def run_bootstrap_job(corpus, session_id: int, jurisdiction: str, job_manager: JobManager, progress_cb: Optional[Callable] = None) -> dict:
    job_id = job_manager.start_job("bootstrap_corpus", jurisdiction, str(session_id))
    try:
        if progress_cb: progress_cb(0.0, "Starting bootstrap...")
        stats = corpus.bootstrap_session(session_id, jurisdiction, progress_cb)
        
        job_manager.finish_job(
            job_id,
            status="SUCCESS",
            new_items=stats.get("new", 0),
            updated_items=stats.get("updated", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        logger.error(f"Bootstrap job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise

def run_refresh_job(corpus, session_id: int, jurisdiction: str, job_manager: JobManager, progress_cb: Optional[Callable] = None) -> dict:
    job_id = job_manager.start_job("incremental_refresh", jurisdiction, str(session_id))
    try:
        if progress_cb: progress_cb(0.0, "Starting incremental refresh...")
        stats = corpus.refresh_session(session_id, jurisdiction, progress_cb)
        
        job_manager.finish_job(
            job_id,
            status="SUCCESS",
            new_items=stats.get("new", 0),
            updated_items=stats.get("updated", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        logger.error(f"Refresh job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise

def run_rescan_job(corpus, states: list, data_dir: str, job_manager: JobManager, progress_cb: Optional[Callable] = None, initiated_by="system") -> dict:
    jur_str = ",".join(states)
    job_id = job_manager.start_job("keyword_rescan", jur_str, "ALL", initiated_by=initiated_by)
    try:
        if progress_cb: progress_cb(0.0, "Starting keyword rescan...")
        # run_scan from legiscanner
        stats = run_scan(states=states, data_dir=data_dir, corpus_manager=corpus)
        
        job_manager.finish_job(
            job_id,
            status="SUCCESS",
            new_items=stats.get("new_bills", 0),
            updated_items=stats.get("changed_status", 0),
            records_processed=stats.get("total_found", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        logger.error(f"Rescan job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise
