#!/usr/bin/env python3
"""
sync_staff_directory.py
=======================
Weekly cron script — syncs the Capitol Matrix Google Sheet into the local
staff SQLite database, then re-runs the LegiScan people-mapping step
so voting history stays linked to the correct legislators.

Usage (manual):
    python sync_staff_directory.py

Cron (runs every Sunday at 3 AM):
    0 3 * * 0 /srv/policy-trackr/venv/bin/python /srv/policy-trackr/sync_staff_directory.py >> /var/log/policy-trackr/staff_sync.log 2>&1
"""

import os
import sys
import logging
from datetime import datetime, timezone

# ── Ensure we can import from the app directory ──────────────────────────────
APP_DIR = os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_DIR = "/var/log/policy-trackr"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("staff_sync")

# ── Config ────────────────────────────────────────────────────────────────────
try:
    from config import DATA_DIR, API_KEY
except ImportError as e:
    logger.error(f"Cannot import config: {e}")
    sys.exit(1)

STAFF_DB   = os.path.join(DATA_DIR, "staff.db")
CORPUS_DB  = os.path.join(DATA_DIR, "bills.db")
STATE      = "CA"

SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/"
    "1gFeGy72R_-FSFrjXbKCAAvVsvNjyV7t_TUvFoB12vys"
    "/export?format=xlsx"
)


def main() -> int:
    start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info(f"Staff directory sync started at {start.isoformat()}")
    logger.info(f"  DATA_DIR  : {DATA_DIR}")
    logger.info(f"  Staff DB  : {STAFF_DB}")
    logger.info(f"  Sheet URL : {SHEET_URL}")

    exit_code = 0

    # ── 1. Sync live Google Sheet ─────────────────────────────────────────────
    try:
        from staff_manager import StaffManager
        sm = StaffManager(STAFF_DB)
        logger.info("Downloading and ingesting live Capitol Matrix…")
        ok, result = sm.sync_live_sheet(SHEET_URL, DATA_DIR, STATE)
        if ok:
            logger.info(
                f"Staff sync SUCCESS — "
                f"legislators_matched={result.get('legislators_matched', '?')}, "
                f"staff_created={result.get('staff_created', '?')}, "
                f"issues_created={result.get('issues_created', '?')}, "
                f"committees_created={result.get('committees_created', '?')}"
            )
            for w in result.get("warnings", []):
                logger.warning(f"  Pipeline warning: {w}")
        else:
            logger.error(f"Staff sync FAILED: {result}")
            exit_code = 1
    except Exception as e:
        logger.error(f"Staff sync threw an unexpected exception: {e}", exc_info=True)
        exit_code = 1

    # ── 2. Re-sync people mapping (links LegiScan votes to staff legislators) ─
    if os.path.exists(CORPUS_DB):
        try:
            from corpus_manager import CorpusManager
            from staff_manager import StaffManager as SM2
            corpus = CorpusManager(CORPUS_DB, API_KEY)
            sm2    = SM2(STAFF_DB)
            leg_df = sm2.get_all_legislators()
            if not leg_df.empty:
                logger.info(f"Re-mapping LegiScan people → {len(leg_df)} staff legislators…")
                stats = corpus.sync_people_mapping(leg_df)
                logger.info(
                    f"People mapping: matched={stats['matched']}, "
                    f"unmatched={stats['unmatched']}, total={stats['total']}"
                )
            else:
                logger.warning("People mapping skipped — no legislators in staff DB yet.")
        except Exception as e:
            logger.error(f"People mapping failed: {e}", exc_info=True)
            # Non-fatal — staff data is still updated
    else:
        logger.info(f"Corpus DB not found at {CORPUS_DB} — skipping people mapping.")

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"Sync complete in {elapsed:.1f}s  (exit_code={exit_code})")
    logger.info("=" * 60)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
