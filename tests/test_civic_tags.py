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
        self.assertEqual(phrases["wetland restoration"]["dictionary_topics"], [])
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
