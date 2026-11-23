"""Rebuild the shared, derived civic tag index from the existing bill corpus."""

from contextlib import closing
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

from civic_tags import load_civic_rules, recurring_phrases, suggest_civic_topics

INDEX_NAME = "civic_tags.db"
MAX_LEARNED = 32


def _read_only(path):
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)


def rebuild_civic_index(corpus_path):
    source = Path(corpus_path)
    if not source.is_file():
        raise FileNotFoundError("The bill corpus does not exist.")
    index = source.with_name(INDEX_NAME)
    curated = load_civic_rules().topics_by_phrase
    curated_phrases = tuple(f" {phrase} " for phrase in curated)

    with closing(sqlite3.connect(index, timeout=30)) as target, target:
        target.execute("BEGIN IMMEDIATE")
        target.execute("CREATE TABLE IF NOT EXISTS learned (phrase TEXT PRIMARY KEY)")
        target.execute("""CREATE TABLE IF NOT EXISTS assignments (
            tag TEXT NOT NULL, bill_id INTEGER NOT NULL, evidence TEXT NOT NULL,
            PRIMARY KEY(tag, bill_id)
        )""")
        target.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        with closing(_read_only(source)) as corpus:
            corpus.row_factory = sqlite3.Row
            rows = [dict(row) for row in corpus.execute(
                """SELECT bill_id, jurisdiction, title, description, subjects
                   FROM bills WHERE jurisdiction IN ('CA', 'US')"""
            )]
        learned = {
            phrase for (phrase,) in target.execute("SELECT phrase FROM learned")
            if phrase not in curated
        }
        prior_count = len(learned)
        for jurisdiction in ("CA", "US"):
            report = recurring_phrases(
                (row for row in rows if row["jurisdiction"] == jurisdiction), min_bills=3, limit=100,
            )
            for row in report["phrases"]:
                if len(learned) >= MAX_LEARNED:
                    break
                phrase = row["phrase"]
                if (
                    row["civic_vocabulary"] and row["title_or_subject_bills"] >= 3
                    and not any(f" {phrase} " in known for known in curated_phrases)
                ):
                    learned.add(phrase)
        learned = tuple(sorted(learned))

        target.execute("DELETE FROM learned")
        target.executemany("INSERT INTO learned VALUES (?)", ((phrase,) for phrase in learned))
        target.execute("DELETE FROM assignments")
        target.executemany(
            "INSERT INTO assignments (tag, bill_id, evidence) VALUES (?, ?, ?)",
            (
                (tag["topic"], row["bill_id"], json.dumps(tag["evidence"], ensure_ascii=False))
                for row in rows
                for tag in suggest_civic_topics(row, learned=learned, limit=None)
            ),
        )
        target.execute(
            """INSERT INTO metadata VALUES ('last_indexed', ?)
               ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
            (datetime.now(timezone.utc).isoformat(),),
        )
        assigned = target.execute("SELECT COUNT(DISTINCT bill_id) FROM assignments").fetchone()[0]
    return {"bills_scanned": len(rows), "bills_tagged": assigned, "new_tags": len(learned) - prior_count}


def learned_civic_phrases(data_dir):
    index = Path(data_dir, INDEX_NAME)
    if not index.is_file():
        return ()
    with closing(_read_only(index)) as conn:
        return tuple(row[0] for row in conn.execute("SELECT phrase FROM learned ORDER BY phrase"))


def civic_index_summary(data_dir):
    index = Path(data_dir, INDEX_NAME)
    if not index.is_file():
        return {"counts": {}, "last_indexed": None}
    with closing(_read_only(index)) as conn:
        stamp = conn.execute("SELECT value FROM metadata WHERE key='last_indexed'").fetchone()
        return {
            "counts": dict(conn.execute("SELECT tag, COUNT(*) FROM assignments GROUP BY tag ORDER BY tag")),
            "last_indexed": stamp[0] if stamp else None,
        }


def indexed_civic_bills(data_dir, tag, *, limit=50, offset=0):
    with closing(_read_only(Path(data_dir, INDEX_NAME))) as conn:
        return [
            row[0] for row in conn.execute(
                """SELECT bill_id FROM assignments WHERE tag=?
                   ORDER BY bill_id DESC LIMIT ? OFFSET ?""", (tag, limit, offset),
            )
        ]
