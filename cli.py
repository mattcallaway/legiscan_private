import argparse
import json
import logging
import os
import sqlite3
import sys
import time

from config import DATA_DIR, API_KEY
from corpus_manager import CorpusManager
from hearing_calendar import legislative_today
from legiscanner import US_STATES
from job_manager import JobManager
from job_runner import (execute_next_job, run_bootstrap_job, run_refresh_job, run_rescan_job,
                        scheduled_current_refresh, sync_selected_sessions)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Headless Task Runner for Legiscan Updater")
    parser.add_argument("--task", type=str, required=True,
                        choices=["bootstrap", "refresh", "sync-current", "scheduled-refresh", "rescan", "worker",
                                 "notification-setup", "notifications", "civic-tags"], help="The pipeline job to execute")
    parser.add_argument("--session-id", type=int, help="Target session ID for bootstrap/refresh")
    parser.add_argument("--jurisdiction", type=str, help="Target jurisdiction code for bootstrap/refresh (e.g. CA, US)")
    parser.add_argument("--states", type=str, help="Comma-separated state codes for rescan (e.g., CA,NY,US)")
    parser.add_argument("--keyword", action="append", help="Limit rescan to this configured shared keyword. Repeat to select several.")
    
    parser.add_argument("--once", action="store_true", help="Worker: process at most one queued job and exit.")
    args = parser.parse_args()
    
    if args.task == "worker" and (not DATA_DIR or not API_KEY):
        parser.error("The worker requires DATA_DIR and a server API_KEY.")

    if args.task in {"sync-current", "scheduled-refresh"}:
        jurisdictions = list(dict.fromkeys(code.strip().upper() for code in (args.states or "CA,US").split(",")))
        if any(code not in {"CA", "US"} for code in jurisdictions):
            parser.error("Current-session sync supports CA, US, or CA,US.")
        if not DATA_DIR or not API_KEY.strip():
            parser.error("Current-session sync requires DATA_DIR and a server API_KEY.")

    if args.task == "civic-tags":
        from civic_index import rebuild_civic_index

        if not DATA_DIR:
            parser.error("Civic tag indexing requires the persistent DATA_DIR.")
        try:
            summary = rebuild_civic_index(os.path.join(DATA_DIR, "bills.db"))
        except (OSError, ValueError, sqlite3.Error) as error:
            logger.error("Civic tag indexing failed: %s", type(error).__name__)
            return 1
        logger.info("Civic tags: %(bills_scanned)s scanned, %(bills_tagged)s tagged, %(new_tags)s new tags.", summary)
        return 0

    if args.task in {"notification-setup", "notifications"}:
        from notifications import Notifications

        try:
            service = Notifications.from_environment()
            if args.task == "notification-setup":
                service.initialize()
                logger.info("PostgreSQL notification tables are ready.")
            else:
                if not DATA_DIR:
                    parser.error("Notification delivery requires the persistent DATA_DIR.")
                result = service.process(DATA_DIR)
                if result is None:
                    logger.info("Another notification pass is already running.")
                else:
                    logger.info("Status notifications: %s queued, %s accepted by Resend.", *result)
            return 0
        except Exception as error:
            logger.error("Notification command failed: %s", type(error).__name__)
            return 1

    job_db_path = os.path.join(DATA_DIR, "jobs.db")
    job_manager = JobManager(job_db_path)
    corpus = CorpusManager(os.path.join(DATA_DIR, "bills.db"), API_KEY)
    
    try:
        if args.task in {"sync-current", "scheduled-refresh"}:
            if args.task == "scheduled-refresh":
                outcomes = scheduled_current_refresh(corpus, jurisdictions, job_manager)
            else:
                outcomes = sync_selected_sessions(corpus, jurisdictions, legislative_today().year, job_manager)
            print(json.dumps(outcomes, indent=2))
            return int(any("error" in outcome or outcome.get("stats", {}).get("errors") for outcome in outcomes.values()))

        if args.task == "worker":
            logger.info("Waiting for queued sync jobs.")
            while True:
                job_id = execute_next_job(corpus, DATA_DIR, job_manager)
                if args.once:
                    job = job_manager.get_job(job_id) if job_id else None
                    return int(job is not None and job["status"] != "SUCCESS")
                if job_id is None:
                    time.sleep(2)

        if args.task == "bootstrap":
            if not args.session_id or not args.jurisdiction:
                logger.error("--session-id and --jurisdiction required for bootstrap")
                sys.exit(1)
            logger.info(f"Running Bootstrap for {args.jurisdiction} ({args.session_id})")
            stats = run_bootstrap_job(corpus, args.session_id, args.jurisdiction, job_manager)
            if stats["errors"]:
                return 1
            
        elif args.task == "refresh":
            if not args.session_id or not args.jurisdiction:
                logger.error("--session-id and --jurisdiction required for refresh")
                sys.exit(1)
            logger.info(f"Running Incremental Refresh for {args.jurisdiction} ({args.session_id})")
            stats = run_refresh_job(corpus, args.session_id, args.jurisdiction, job_manager)
            
            if stats["errors"]:
                return 1

        elif args.task == "rescan":
            codes_by_name = {name.casefold(): code for code, name in US_STATES.items()}
            requested = args.states.split(",") if args.states else ["CA"]
            states = []
            for state in requested:
                state = state.strip()
                code = codes_by_name.get(state.casefold(), state.upper())
                if code not in US_STATES and code not in {"ALL", "US"}:
                    parser.error(f"Unknown jurisdiction: {state!r}")
                states.append(code)
            stats = run_rescan_job(corpus, list(dict.fromkeys(states)), DATA_DIR, job_manager,
                                   initiated_by="cli", keywords=args.keyword)
            if stats["api_status"] == "error":
                return 1

        logger.info(f"Task {args.task} completed successfully.")
        
    except KeyboardInterrupt:
        return 130
    except Exception as e:
        logger.error(f"Task {args.task} failed: {e}")
        return 1
    finally:
        corpus.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())
