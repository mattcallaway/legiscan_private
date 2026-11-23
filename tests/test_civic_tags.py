from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from unittest import TestCase

from civic_tags import recurring_phrases, suggest_civic_topics
from tools.suggest_civic_terms import scan_corpus


class CivicTopicTests(TestCase):
    def test_suggestions_use_whole_terms_and_show_matching_fields(self):
        bill = {
            "title": "Drinking-water safety and rent control",
            "description": "An act to amend Section 244, relating to drinking water and tenant protection.",
            "subjects": "Medi-Cal; groundwater",
            "last_action": "Public transit",
        }
        suggestions = {item["topic"]: item["evidence"] for item in suggest_civic_topics(bill)}
        self.assertEqual(set(suggestions), {"Water", "Housing", "Health care"})
        self.assertIn({"phrase": "drinking water", "field": "Title"}, suggestions["Water"])
        self.assertIn({"phrase": "groundwater", "field": "Subjects"}, suggestions["Water"])
        self.assertIn({"phrase": "tenant protection", "field": "Summary"}, suggestions["Housing"])
        self.assertEqual(suggest_civic_topics({
            "title": "An act to amend Section 2. Read a first time.",
            "description": "Ordered to the Senate. Referred to the committee. A prewildfire exercise.",
            "subjects": None,
        }), [])

    def test_recurring_report_counts_bills_and_filters_boilerplate_and_common_phrases(self):
        bills = [
            {"title": "Wetland restoration planning", "description": "Public transit. Public transit. Routine filing."},
            {"title": "Wetland restoration review", "subjects": "Wetland restoration", "description": "Routine filing."},
            {"title": "Affordable housing", "description": "This bill would support affordable housing. Routine filing."},
            {"title": "Affordable housing expansion", "description": "Routine filing."},
            {"title": "Consumer protection", "description": "Routine filing."},
            {"title": "Unknown detail", "description": "Routine filing."},
        ]
        report = recurring_phrases(bills, min_bills=2, max_share=0.50)
        phrases = {row["phrase"]: row for row in report["phrases"]}
        self.assertEqual(report["bills_scanned"], 6)
        self.assertEqual(phrases["wetland restoration"]["bills"], 2)
        self.assertEqual(phrases["wetland restoration"]["title_or_subject_bills"], 2)
        self.assertEqual(phrases["wetland restoration"]["dictionary_topics"], ["Environment and public lands"])
        self.assertEqual(phrases["affordable housing"]["dictionary_topics"], ["Housing"])
        self.assertNotIn("public transit", phrases)
        self.assertNotIn("routine filing", phrases)
        self.assertNotIn("this bill", phrases)

    def test_report_respects_jurisdiction_and_session_without_writing(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "bills.db"
            with closing(sqlite3.connect(path)) as conn, conn:
                conn.execute("CREATE TABLE bills(jurisdiction, session_id, title, description, subjects)")
                conn.executemany("INSERT INTO bills VALUES (?, ?, ?, ?, ?)", [
                    ("CA", 40, "Water conservation", None, None),
                    ("CA", 40, "Water conservation rebates", None, None),
                    ("CA", 41, "Water conservation", None, None),
                    ("US", 40, "Water conservation", None, None),
                ])
            before = path.read_bytes()
            report = scan_corpus(path, session_id=40, min_bills=2, max_share=1.0)
            self.assertEqual(report["bills_scanned"], 2)
            self.assertEqual(report["phrases"][0]["phrase"], "water conservation")
            self.assertEqual(report["phrases"][0]["bills"], 2)
            self.assertEqual(path.read_bytes(), before)

    def test_shared_index_learns_and_retags_old_bills_after_a_successful_sync(self):
        from unittest.mock import patch

        from civic_index import civic_index_summary, indexed_civic_bills, learned_civic_phrases, rebuild_civic_index
        from corpus_manager import CorpusManager
        from job_manager import JobManager
        with patch.dict("os.environ", {"DATA_DIR": "", "REPO_DIR": "", "API_KEY": ""}):
            from job_runner import run_refresh_job

        with TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = CorpusManager(str(root / "bills.db"), "")
            with corpus.connection() as conn, conn:
                conn.execute("INSERT INTO sessions(session_id, jurisdiction) VALUES (1,'CA'), (9,'CA')")
                conn.executemany(
                    "INSERT INTO bills (bill_id,jurisdiction,session_id,title) VALUES (?, 'CA', 1, ?)",
                    [(1, "Groundwater recharge"), *[(i, f"Fixture {i}") for i in range(2, 12)]],
                )
            self.assertEqual(rebuild_civic_index(corpus.db_path)["new_tags"], 0)
            self.assertEqual(indexed_civic_bills(root, "Water"), [1])
            with corpus.connection() as conn, conn:
                conn.execute("""INSERT INTO bills (bill_id,jurisdiction,session_id,title) VALUES
                                (12,'CA',9,'Groundwater recharge'),
                                (13,'CA',9,'Groundwater recharge permits')""")
            jobs = JobManager(str(root / "jobs.db"))
            with patch.object(corpus, "refresh_session", return_value={"errors": 0, "new": 2}):
                stats = run_refresh_job(corpus, 9, "CA", jobs)
            self.assertIn("groundwater recharge", learned_civic_phrases(root))
            self.assertEqual(stats["civic_index"]["new_tags"], 1)
            self.assertEqual(civic_index_summary(root)["counts"]["Groundwater recharge"], 3)
            self.assertIsNotNone(civic_index_summary(root)["last_indexed"])
            self.assertEqual(indexed_civic_bills(root, "Groundwater recharge", limit=2), [13, 12])
            self.assertEqual(indexed_civic_bills(root, "Groundwater recharge", offset=2), [1])
            self.assertEqual(rebuild_civic_index(corpus.db_path)["new_tags"], 0)

    def test_private_rules_tag_old_bill_without_mutating_other_accounts_and_edits_revisit_matches(self):
        from bill_research import personal_topic_tags, update_personal_topic

        entry = {"name": "Groundwater portfolio", "terms": ["groundwater"], "seen": ["1"], "auto_tag": True}
        bill = {"title": "Groundwater recharge", "bill_id": 1}
        self.assertEqual(personal_topic_tags(bill, {"water": entry}), ["Groundwater portfolio"])
        self.assertEqual(personal_topic_tags(bill, {}), [])
        update_personal_topic(entry, {"terms": ["groundwater"]}, auto_tag=True, track_matches=True)
        self.assertEqual(entry["seen"], ["1"])
        update_personal_topic(entry, {"terms": ["recharge"]}, auto_tag=True, track_matches=True)
        self.assertEqual(entry["seen"], [])
        self.assertEqual(personal_topic_tags(bill, {"water": entry}), ["Groundwater portfolio"])

    def test_dictionary_pilot_gaps_were_added_as_specific_rules(self):
        for title, expected in [
            ("California Environmental Quality Act: hospital projects", "Environment and public lands"),
            ("CEQA: hospital projects", "Environment and public lands"),
            ("Government use of artificial intelligence", "Technology policy"),
            ("Civil actions: protective orders: workplace violence", "Labor and employment"),
            ("Sales and Use Tax Law: vehicles", "Taxes and public finance"),
        ]:
            with self.subTest(title=title):
                self.assertIn(expected, {item["topic"] for item in suggest_civic_topics({"title": title})})
