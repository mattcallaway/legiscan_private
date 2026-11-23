import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from civic_index import rebuild_civic_index
from hearing_calendar import PACIFIC, calendar_refresh_failure_summary, legislative_today
from threading import Event, Thread
from typing import Optional, Callable
from job_manager import JobConflict, JobManager
from legiscanner import run_scan
from request_budget import job_request_budget, quota_for_directory

logger = logging.getLogger(__name__)

def _refresh_civic_tags(corpus, stats, progress_cb):
    path = getattr(corpus, "db_path", None)
    if stats["errors"] or not isinstance(path, (str, Path)):
        return
    path = Path(path)
    try:
        if progress_cb:
            progress_cb(0.95, "Indexing civic tags and historical matches...")
        stats["civic_index"] = rebuild_civic_index(path)
    except (OSError, ValueError, sqlite3.Error) as error:
        stats["civic_index_error"] = type(error).__name__
        logger.warning("Civic tag index refresh failed: %s. Run cli.py --task civic-tags to retry.", type(error).__name__)


def _record_budget(stats, budget):
    stats["api_calls"] = budget.used
    stats["budget_exhausted"] = budget.exhausted
    stats["request_stop_reason"] = budget.stop_reason


def run_bootstrap_job(corpus, session_id: int, jurisdiction: str, job_manager: JobManager, progress_cb: Optional[Callable] = None, *, allow_individual_fallback: bool = True, job_id=None) -> dict:
    job_id = job_id or job_manager.start_job("bootstrap_corpus", jurisdiction, str(session_id))
    budget = None
    try:
        if progress_cb: progress_cb(0.0, "Starting bootstrap...")
        with job_request_budget("Bulk import") as budget:
            stats = corpus.bootstrap_session(
                session_id, jurisdiction, progress_cb,
                allow_individual_fallback=allow_individual_fallback,
            )
        _record_budget(stats, budget)
        _refresh_civic_tags(corpus, stats, progress_cb)

        job_manager.finish_job(
            job_id,
            status="FAILED" if stats["errors"] else "SUCCESS",
            error_summary=budget.stop_reason or (f'{stats["errors"]} bootstrap errors' if stats["errors"] else ""),
            new_items=stats.get("new", 0),
            updated_items=stats.get("updated", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        if budget and budget.stop_reason:
            job_manager.finish_job(job_id, status="FAILED", api_calls=budget.used, error_summary=budget.stop_reason)
            raise RuntimeError(budget.stop_reason) from None
        logger.error(f"Bootstrap job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise

def run_refresh_job(corpus, session_id: int, jurisdiction: str, job_manager: JobManager, progress_cb: Optional[Callable] = None, *, job_id=None) -> dict:
    job_id = job_id or job_manager.start_job("incremental_refresh", jurisdiction, str(session_id))
    try:
        if progress_cb: progress_cb(0.0, "Starting incremental refresh...")
        with job_request_budget() as budget:
            stats = corpus.refresh_session(session_id, jurisdiction, progress_cb)
        _record_budget(stats, budget)
        _refresh_civic_tags(corpus, stats, progress_cb)
        
        failures = stats["errors"]
        job_manager.finish_job(
            job_id,
            status="FAILED" if failures else "SUCCESS",
            error_summary=budget.stop_reason or (f"{failures} upstream refresh failures" if failures else ""),
            new_items=stats.get("new", 0),
            updated_items=stats.get("updated", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        logger.error(f"Refresh job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise

def run_rescan_job(corpus, states: list, data_dir: str, job_manager: JobManager, progress_cb: Optional[Callable] = None, initiated_by="system", *, job_id=None, keywords=None) -> dict:
    jur_str = ",".join(states)
    job_id = job_id or job_manager.start_job("keyword_rescan", jur_str, "ALL", initiated_by=initiated_by)
    try:
        if progress_cb: progress_cb(0.0, "Starting keyword rescan...")
        # run_scan from legiscanner
        with job_request_budget("Keyword scan") as budget:
            stats = run_scan(states=states, data_dir=data_dir, corpus_manager=corpus, selected_keywords=keywords)
        _record_budget(stats, budget)
        
        failed = stats["api_status"] == "error"
        error_summary = (
            f'{stats["search_failures"]} search failures and {stats["detail_failures"]} bill detail failures'
            if failed else ""
        )
        job_manager.finish_job(
            job_id,
            status="FAILED" if failed else "SUCCESS",
            error_summary=budget.stop_reason or error_summary,
            new_items=stats.get("new_bills", 0),
            updated_items=stats.get("updated_bills", 0),
            records_processed=stats.get("total_found", 0),
            api_calls=stats.get("api_calls", 0)
        )
        return stats
    except Exception as e:
        logger.error(f"Rescan job failed: {e}", exc_info=True)
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(e))
        raise

def current_session(sessions, year):
    """Choose a regular session covering the given year, never a past or future one."""
    candidates = [
        session for session in sessions
        if (session.get("year_start") or 0) <= year <= (session.get("year_end") or 0)
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda session: (
        "special" not in (session.get("session_name") or "").casefold(),
        session["year_start"], session["year_end"], session["session_id"],
    ))


def _sync_current_session(corpus, jurisdiction, year, job_manager, progress_cb, job_id=None):
    session = current_session(corpus.get_cached_sessions(jurisdiction), year)
    if session is None:
        with job_request_budget("Session discovery") as budget:
            corpus.get_active_sessions(jurisdiction)
        if budget.stop_reason:
            raise RuntimeError(budget.stop_reason)
        session = current_session(corpus.get_cached_sessions(jurisdiction), year)
    if session is None:
        raise RuntimeError(f"No current session was found for {year}.")

    session_id = session["session_id"]
    importing = not session.get("last_bootstrap")
    if importing:
        stats = run_bootstrap_job(
            corpus, session_id, jurisdiction, job_manager, progress_cb,
            allow_individual_fallback=False, job_id=job_id,
        )
    else:
        stats = run_refresh_job(corpus, session_id, jurisdiction, job_manager, progress_cb, job_id=job_id)
    return {"session": session["session_name"], "importing": importing, "stats": stats}


def sync_selected_sessions(corpus, jurisdictions, year, job_manager, progress_cb=None):
    """Bulk-import a current session once, then refresh it on subsequent syncs."""
    outcomes = {}
    for jurisdiction in jurisdictions:
        try:
            outcomes[jurisdiction] = _sync_current_session(corpus, jurisdiction, year, job_manager, progress_cb)
        except Exception as error:
            outcomes[jurisdiction] = {"error": str(error)}
    return outcomes


def scheduled_current_refresh(corpus, jurisdictions, job_manager, *, today=None):
    """Run daily maintenance on Pacific weekdays, skipping completed work and recess pauses."""
    today = today or legislative_today()
    if today.weekday() >= 5:
        return {code: {"skipped": "Weekend in Pacific time"} for code in jurisdictions}
    if quota_for_directory(corpus.data_dir).policy()["pause_schedule"]:
        return {code: {"skipped": "Scheduled refreshes are paused"} for code in jurisdictions}
    skipped, due = {}, []
    for code in jurisdictions:
        session = current_session(corpus.get_cached_sessions(code), today.year) or {}
        stamp = session.get("last_masterlist")
        if stamp:
            try:
                refreshed = datetime.fromisoformat(stamp)
                if refreshed.tzinfo is None:
                    refreshed = refreshed.replace(tzinfo=timezone.utc)
                if refreshed.astimezone(PACIFIC).date() >= today:
                    skipped[code] = {"skipped": "Current session already refreshed today"}
                    continue
            except ValueError:
                pass
        due.append(code)
    return {**skipped, **sync_selected_sessions(corpus, due, today.year, job_manager)}


def run_calendar_refresh(corpus, bills, job_manager, progress_cb=None, *, job_id=None, initiated_by="system"):
    if not 1 <= len(bills) <= 25:
        raise ValueError("Select up to 25 tracked bills for calendar refresh.")
    job_id = job_id or job_manager.start_job("calendar_refresh", "", "WATCHLIST", initiated_by=initiated_by)
    try:
        with job_request_budget("Hearing refresh") as budget:
            stats = corpus.refresh_tracked_calendars(bills, progress_cb)
        _record_budget(stats, budget)
        failure = calendar_refresh_failure_summary(stats)
        message = failure or (f'{stats["unavailable"]} bills had no usable source calendar' if stats["unavailable"] else "")
        job_manager.finish_job(
            job_id, status="FAILED" if failure else "SUCCESS", error_summary=message,
            new_items=stats["new"], updated_items=stats["updated"], api_calls=stats["api_calls"],
        )
        return stats
    except Exception as error:
        job_manager.finish_job(job_id, status="FAILED", error_summary=str(error))
        raise


def execute_next_job(corpus, data_dir, job_manager):
    """Run one queued job while holding exclusive ownership of worker writes."""
    with job_manager.worker_ownership() as owned:
        if owned:
            return _execute_claimed_job(corpus, data_dir, job_manager)
    return None


def _execute_claimed_job(corpus, data_dir, job_manager):
    job = job_manager.claim_next_job()
    if job is None:
        return None

    identifier = job["job_id"]
    stop = Event()
    lost_claim = Event()

    def keep_claim_alive():
        while not stop.wait(20):
            try:
                if not job_manager.heartbeat(identifier):
                    lost_claim.set()
                    return
            except Exception:
                logger.warning("Unable to update queued job heartbeat", exc_info=True)

    def progress(fraction, message):
        if lost_claim.is_set() or not job_manager.heartbeat(
            identifier, fraction=max(0.0, min(1.0, fraction)), message=message,
        ):
            raise JobConflict("This worker no longer holds the sync job claim.")

    heartbeat = Thread(target=keep_claim_alive, daemon=True)
    heartbeat.start()
    try:
        kind = job["job_type"]
        payload = job["payload"]
        scope = job["session_scope"]
        jurisdiction = job["jurisdiction"]
        if kind == "current_session_sync":
            _sync_current_session(corpus, jurisdiction, payload["year"], job_manager, progress, job_id=identifier)
        elif kind == "bootstrap_corpus":
            run_bootstrap_job(corpus, int(scope), jurisdiction, job_manager, progress,
                              allow_individual_fallback=False, job_id=identifier)
        elif kind == "incremental_refresh":
            run_refresh_job(corpus, int(scope), jurisdiction, job_manager, progress, job_id=identifier)
        elif kind == "calendar_refresh":
            run_calendar_refresh(corpus, payload["bills"], job_manager, progress, job_id=identifier)
        elif kind == "keyword_rescan":
            run_rescan_job(corpus, payload["states"], data_dir, job_manager, progress,
                           job_id=identifier, keywords=payload.get("keywords"))
            if job_manager.get_job(identifier)["status"] == "SUCCESS":
                from sync_github_repo import sync_with_remote

                sync_with_remote()
        else:
            raise ValueError("Unsupported queued sync job.")
    except Exception as error:
        # A runner may already have closed this job. Only RUNNING jobs are updated.
        job_manager.finish_job(identifier, status="FAILED", error_summary=str(error))
        logger.warning("Queued job %s failed: %s", identifier, error)
    finally:
        stop.set()
        heartbeat.join()
    return identifier
