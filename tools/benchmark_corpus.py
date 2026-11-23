"""Measure the corpus query path against a disposable synthetic SQLite corpus."""

import argparse
import json
import math
import os
import platform
from pathlib import Path
from statistics import median
import sys
from tempfile import TemporaryDirectory
from time import perf_counter
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def fixture_rows(count):
    for index in range(1, count + 1):
        session = 1 + index % 4
        day = f"2025-{1 + index % 12:02d}-{1 + index % 27:02d}"
        topic = "Water supply" if index % 9 == 0 else "Housing policy"
        yield (
            index, session, "CA" if session < 4 else "US", f"AB {index}", topic,
            "Synthetic summary for a legislative policy record", day, str(index % 5),
            day, f"Committee {index % 20}", f"Sponsor {index % 120}", topic,
        )


def measure(action, repetitions):
    action()
    samples = []
    for _ in range(repetitions):
        start = perf_counter()
        action()
        samples.append((perf_counter() - start) * 1000)
    ordered = sorted(samples)
    return {
        "p50_ms": round(median(ordered), 3),
        "p95_ms": round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3),
        "p99_ms": round(ordered[math.ceil(0.99 * len(ordered)) - 1], 3),
    }


def run(count, repetitions):
    sys.path.insert(0, str(ROOT))
    with patch.dict(os.environ, {"DATA_DIR": "", "REPO_DIR": "", "API_KEY": ""}):
        from corpus_manager import CorpusManager

    with TemporaryDirectory() as directory:
        database = Path(directory) / "benchmark.sqlite3"
        corpus = CorpusManager(str(database), "")
        try:
            connection = corpus._get_conn()
            with connection:
                connection.executemany(
                    "INSERT INTO sessions (session_id,jurisdiction,session_name) VALUES (?,?,?)",
                    ((session, "CA" if session < 4 else "US", f"Fixture {session}") for session in range(1, 5)),
                )
                connection.executemany(
                    """INSERT INTO bills (bill_id,session_id,jurisdiction,bill_number,title,description,
                       status_date,status_stage,last_action_date,committee,sponsor_names,subjects)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    fixture_rows(count),
                )
            operations = {
                "session_page": lambda: corpus.search_bills(session_id=1, limit=50),
                "literal_page_and_count": lambda: (
                    corpus.search_bills(query="Water supply", session_id=1, limit=50),
                    corpus.search_bills(query="Water supply", session_id=1, count_only=True),
                ),
                "historical_coverage": corpus.get_historical_coverage,
            }
            plans = {}
            for name, sql in {
                "session_page": "SELECT bill_id FROM bills WHERE session_id=1 ORDER BY status_date DESC,bill_id LIMIT 50",
                "status_date_range": "SELECT bill_id FROM bills WHERE substr(status_date,1,10) BETWEEN '2025-01-01' AND '2025-02-01'",
            }.items():
                plans[name] = [row[3] for row in connection.execute(f"EXPLAIN QUERY PLAN {sql}")]
            return {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "rows": count,
                "sessions": 4,
                "repetitions": repetitions,
                "database_and_wal_bytes": sum(path.stat().st_size for path in Path(directory).iterdir()),
                "measurements": {name: measure(action, repetitions) for name, action in operations.items()},
                "representative_query_plans": plans,
            }
        finally:
            corpus.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=12000)
    parser.add_argument("--repetitions", type=int, default=100)
    args = parser.parse_args()
    if args.rows < 1 or args.repetitions < 1:
        parser.error("--rows and --repetitions must be positive")
    print(json.dumps(run(args.rows, args.repetitions), indent=2))


if __name__ == "__main__":
    main()
