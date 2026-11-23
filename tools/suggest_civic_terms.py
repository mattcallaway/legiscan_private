"""Report recurring civic phrases from an already indexed, read-only bill corpus."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]


def scan_corpus(database, *, jurisdiction="CA", session_id=None, min_bills=3, max_share=0.40, limit=50):
    sys.path.insert(0, str(ROOT))
    from civic_tags import recurring_phrases

    params = [jurisdiction]
    sql = "SELECT title, description, subjects FROM bills WHERE jurisdiction=?"
    if session_id is not None:
        sql += " AND session_id=?"
        params.append(session_id)
    with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
        connection.row_factory = sqlite3.Row
        report = recurring_phrases(
            (dict(row) for row in connection.execute(sql, params)),
            min_bills=min_bills, max_share=max_share, limit=limit,
        )
    return {"jurisdiction": jurisdiction, "session_id": session_id, **report}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path, help="Path to an existing tracker bills.db.")
    parser.add_argument("--jurisdiction", choices=("CA", "US"), default="CA")
    parser.add_argument("--session-id", type=int)
    parser.add_argument("--min-bills", type=int, default=3)
    parser.add_argument("--max-share", type=float, default=0.40)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()
    if args.min_bills < 2 or args.limit < 1 or not 0 < args.max_share <= 1:
        parser.error("Choose --min-bills of at least 2, a positive --limit, and --max-share between 0 and 1.")
    try:
        report = scan_corpus(
            args.database, jurisdiction=args.jurisdiction, session_id=args.session_id,
            min_bills=args.min_bills, max_share=args.max_share, limit=args.limit,
        )
    except sqlite3.Error as error:
        parser.exit(1, f"Could not read the bill corpus: {error}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
