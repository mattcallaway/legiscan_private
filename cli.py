import argparse
import logging
import os
import sys

from config import DATA_DIR, API_KEY
from corpus_manager import _CorpusManager, _CORPUS_AVAILABLE, US_STATES
from job_manager import JobManager
from job_runner import run_bootstrap_job, run_refresh_job, run_rescan_job

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Headless Task Runner for Legiscan Updater")
    parser.add_argument("--task", type=str, required=True, choices=["bootstrap", "refresh", "rescan"], help="The pipeline job to execute")
    parser.add_argument("--session-id", type=int, help="Target session ID for bootstrap/refresh")
    parser.add_argument("--jurisdiction", type=str, help="Target jurisdiction code for bootstrap/refresh (e.g. CA, US)")
    parser.add_argument("--states", type=str, help="Comma-separated state codes for rescan (e.g., CA,NY,US)")
    
    args = parser.parse_args()
    
    if not _CORPUS_AVAILABLE:
        logger.error("Corpus manager is not available. Cannot run headless jobs.")
        sys.exit(1)
        
    job_db_path = os.path.join(DATA_DIR, "jobs.db")
    job_manager = JobManager(job_db_path)
    corpus = _CorpusManager(os.path.join(DATA_DIR, "bills.db"), API_KEY)
    
    try:
        if args.task == "bootstrap":
            if not args.session_id or not args.jurisdiction:
                logger.error("--session-id and --jurisdiction required for bootstrap")
                sys.exit(1)
            logger.info(f"Running Bootstrap for {args.jurisdiction} ({args.session_id})")
            run_bootstrap_job(corpus, args.session_id, args.jurisdiction, job_manager)
            
        elif args.task == "refresh":
            if not args.session_id or not args.jurisdiction:
                logger.error("--session-id and --jurisdiction required for refresh")
                sys.exit(1)
            logger.info(f"Running Incremental Refresh for {args.jurisdiction} ({args.session_id})")
            run_refresh_job(corpus, args.session_id, args.jurisdiction, job_manager)
            
        elif args.task == "rescan":
            states = [s.strip() for s in args.states.split(",")] if args.states else ["California", "US"]
            # Convert state codes back to names if needed, or assume caller provides exact Legiscanner compat codes.
            # Run scan needs full names for States (e.g. "California"), but uses "US" for Federal
            resolved_states = []
            for s in states:
                if s == "ALL":
                    resolved_states.extend(US_STATES.values())
                    continue
                if s == "US":
                    resolved_states.append("US")
                    continue
                # Match code or full name
                if s in US_STATES: resolved_states.append(US_STATES[s])
                elif s in US_STATES.values(): resolved_states.append(s)
                else: logger.warning(f"Unknown state arg: {s}")
                
            resolved_states = list(set(resolved_states))
            logger.info(f"Running Keyword Rescan for states: {resolved_states}")
            run_rescan_job(corpus, resolved_states, DATA_DIR, job_manager, initiated_by="cli")
            
        logger.info(f"Task {args.task} completed successfully.")
        
    except Exception as e:
        logger.error(f"Task {args.task} failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
