import csv
import json
import sqlite3
import sys
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from bill_workspace import NoteConflict, export_workspace, read_workspace, update_bill_note, update_tracking

# Config creates directories on import. Use fixture locations in this suite.
with patch.dict("os.environ", {"DATA_DIR": "", "REPO_DIR": "", "API_KEY": ""}):
    import legiscanner
    import sync_github_repo
    from corpus_manager import CorpusManager
    from job_runner import run_refresh_job
    import auth
    from staff_manager import StaffManager

import pandas as pd


class BillWorkspaceTests(TestCase):
    def test_concurrent_tracking_and_note_conflicts(self):
        with TemporaryDirectory() as directory:
            tracking = Path(directory, "tracking.json")
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda bill: update_tracking(tracking, add=str(bill)), range(20)))
            self.assertEqual(len(read_workspace(tracking, [])), 20)

            notes = Path(directory, "notes.json")
            same_label_different_sessions = {"1001": "Earlier session", "1002": "Later session"}
            for bill, comment in same_label_different_sessions.items():
                update_bill_note(notes, bill, {}, {"comment": comment})
            with self.assertRaises(NoteConflict):
                update_bill_note(notes, "1001", {}, {"comment": "Stale edit"})
            self.assertEqual(
                {bill: note["comment"] for bill, note in read_workspace(notes, {}).items()},
                same_label_different_sessions,
            )


    def test_first_edit_imports_json_and_recovers_mirror_after_restart(self):
        with TemporaryDirectory() as directory:
            notes = Path(directory, "bill_notes.json")
            legacy = {"AB5": {"comment": "Needs session review"}, "1001": {"files": ["old.pdf"]}}
            notes.write_text(json.dumps(legacy), encoding="utf-8")
            update_bill_note(notes, "1001", legacy["1001"], {"comment": "New observation"})
            self.assertEqual(json.loads(notes.read_text(encoding="utf-8")), legacy)
            after = read_workspace(notes, {})
            self.assertEqual(after["AB5"], legacy["AB5"])
            self.assertEqual(after["1001"]["files"], ["old.pdf"])
            export_workspace(notes)
            self.assertEqual(json.loads(notes.read_text(encoding="utf-8")), after)


class KeywordScannerTests(TestCase):
    def test_failed_detail_preserves_snapshot_hash_and_other_keywords(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "keywords.json").write_text('["water", "energy"]', encoding="utf-8")
            (root / "legiscan_cache.json").write_text(
                json.dumps({"1": {"change_hash": "old"}, "2": {"change_hash": "same"}}),
                encoding="utf-8",
            )
            snapshot = root / "LegiScan_Enhanced_Full_Tracker.csv"
            snapshot.write_text("bill_id,title,status_stage\n1,Keep,1\n2,Existing,2\n", encoding="utf-8")
            matches = [{"bill_id": 1, "change_hash": "new"}, {"bill_id": 2, "change_hash": "same"}]
            corpus = Mock()
            with (
                patch.object(legiscanner.time, "sleep"),
                patch.object(legiscanner, "fetch_search_results", return_value=(matches, 2, 0, True)),
                patch.object(legiscanner, "get_bill_details", return_value=None),
            ):
                stats = legiscanner.run_scan(["CA"], directory, corpus)
            self.assertEqual(stats["detail_failures"], 2)
            self.assertEqual(stats["changed_status"], 0)
            with snapshot.open(encoding="utf-8", newline="") as file:
                rows = {row["bill_id"]: row for row in csv.DictReader(file)}
            self.assertEqual((rows["1"]["title"], rows["1"]["change_hash"]), ("Keep", "old"))
            self.assertEqual(
                [call.args for call in corpus.record_keyword_match.call_args_list],
                [(2, "water"), (2, "energy")],
            )

    def test_rejects_empty_or_invalid_scope_before_network_access(self):
        with patch.object(legiscanner, "fetch_search_results") as request:
            for states in ([], ["California"]):
                with self.subTest(states=states), self.assertRaises(ValueError):
                    legiscanner.run_scan(states)
            request.assert_not_called()


class SessionRefreshTests(TestCase):
    def test_keyword_independent_refresh_and_failure_status(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            jobs = Mock()
            master = {"status": "OK", "masterlist": {"0": {"bill_id": 77, "change_hash": "a"}}}
            detail = {"status": "OK", "bill": {"bill_id": 77, "bill_number": "AB5", "title": "A bill"}}
            try:
                with corpus._get_conn() as connection:
                    connection.execute(
                        "INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (?, ?, ?)",
                        (10, "CA", "Fixture session"),
                    )
                with patch.object(corpus, "_api_get", side_effect=[master, detail]):
                    stats = run_refresh_job(corpus, 10, "CA", jobs)
                self.assertEqual(stats["new"], 1)
                self.assertEqual(corpus.get_bills_by_ids([77]).iloc[0]["title"], "A bill")
                last_success = corpus.get_corpus_stats()["last_incremental_CA"]
                self.assertTrue(last_success)

                with patch.object(corpus, "_api_get", return_value={"status": "ERROR"}):
                    stats = run_refresh_job(corpus, 10, "CA", jobs)
                self.assertEqual(stats["errors"], 1)
                self.assertEqual(jobs.finish_job.call_args.kwargs["status"], "FAILED")
                self.assertEqual(corpus.get_corpus_stats()["last_incremental_CA"], last_success)
                self.assertEqual(corpus.get_bills_by_ids([77]).iloc[0]["title"], "A bill")
            finally:
                corpus.close()

class GitPublicationTests(TestCase):
    def test_failed_push_can_retry_without_staging_unrelated_changes(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            checkout = root / "checkout"
            checkout.mkdir()
            remote = root / "backup.git"
            def git(path, *args):
                return subprocess.check_output(
                    ["git", *args], cwd=path, stderr=subprocess.STDOUT, text=True,
                ).strip()

            git(checkout, "init", "-b", "main")
            git(checkout, "config", "user.email", "fixture@example.invalid")
            git(checkout, "config", "user.name", "Fixture")
            git(checkout, "commit", "--allow-empty", "-m", "Initial")
            git(root, "init", "--bare", str(remote))
            git(checkout, "remote", "add", "origin", str(remote))
            git(checkout, "push", "-u", "origin", "main")
            unrelated = checkout / "unrelated.txt"
            unrelated.write_text("keep staged", encoding="utf-8")
            git(checkout, "add", unrelated.name)
            notes = checkout / "LegiScan_Enhanced_Full_Tracker.csv"
            notes.write_text("bill_id\n1\n", encoding="utf-8")
            credentials = checkout / "users.json"
            credentials.write_text("{}", encoding="utf-8")

            with patch.multiple(sync_github_repo, REPO_DIR=str(checkout), DATA_DIR=str(checkout)):
                self.assertFalse(sync_github_repo.sync_with_remote([credentials]))
                git(checkout, "remote", "set-url", "origin", str(root / "unavailable.git"))
                self.assertFalse(sync_github_repo.sync_with_remote([notes]))
                self.assertEqual(git(checkout, "show", "--format=", "--name-only", "HEAD"), notes.name)
                git(checkout, "remote", "set-url", "origin", str(remote))
                self.assertTrue(sync_github_repo.sync_with_remote([notes]))

            self.assertEqual(git(checkout, "rev-parse", "HEAD"), git(remote, "rev-parse", "refs/heads/main"))
            self.assertEqual(git(checkout, "diff", "--cached", "--name-only"), unrelated.name)
            self.assertNotIn(credentials.name, git(remote, "ls-tree", "--name-only", "refs/heads/main"))
            git(checkout, "commit", "-m", "Unmanaged local commit")
            with patch.multiple(sync_github_repo, REPO_DIR=str(checkout), DATA_DIR=str(checkout)):
                self.assertFalse(sync_github_repo.sync_with_remote())


class HistoricalResearchTests(TestCase):
    def test_session_scope_and_vote_identity(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with corpus._get_conn() as conn:
                    conn.executemany(
                        "INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (?, 'CA', ?)",
                        [(10, "2023-2024"), (20, "2025-2026")],
                    )
                    conn.executemany(
                        """INSERT INTO bills (bill_id, session_id, jurisdiction, bill_number, title)
                        VALUES (?, ?, 'CA', 'AB 5', ?)""",
                        [(1001, 10, "Earlier bill"), (1002, 20, "Later bill")],
                    )
                    conn.executemany(
                        "INSERT INTO people (people_id, first_name, last_name) VALUES (?, ?, 'Rivera')",
                        [(1, "Alex"), (2, "Sam")],
                    )
                    conn.execute("INSERT INTO roll_calls (roll_call_id, bill_id) VALUES (40, 1001)")
                    conn.executemany(
                        "INSERT INTO legislator_votes (roll_call_id, people_id, vote_text) VALUES (40, ?, ?)",
                        [(1, "Yea"), (2, "Nay")],
                    )
                self.assertEqual(corpus.search_bills(session_id=20)["bill_id"].tolist(), [1002])
                votes = corpus.get_votes_for_legislator_by_name("Alex", "Rivera")
                self.assertEqual([(vote["bill_id"], vote["vote_text"], vote["session"]) for vote in votes],
                                 [(1001, "Yea", "2023-2024")])
                with corpus._get_conn() as conn:
                    conn.execute("INSERT INTO people (people_id, first_name, last_name) VALUES (3, 'Alex', 'Rivera')")
                    conn.execute("UPDATE people SET name='Alex Rivera', chamber='A' WHERE people_id=1")
                    conn.execute("UPDATE people SET name='Alex Rivera', chamber='S' WHERE people_id=3")
                self.assertEqual(corpus.get_votes_for_legislator_by_name("Alex", "Rivera"), [])
                staff = pd.DataFrame([{"name": "Alex Rivera", "state": "CA", "chamber": "Assembly", "legislator_id": "staff-001"}])
                self.assertEqual(corpus.sync_people_mapping(staff)["matched"], 1)
                self.assertEqual(corpus.get_votes_for_legislator("staff-001")[0]["vote_text"], "Yea")
                duplicated = pd.concat([staff, staff.assign(legislator_id="other")])
                self.assertEqual(corpus.sync_people_mapping(duplicated)["matched"], 0)
                self.assertEqual(corpus.get_votes_for_legislator("staff-001"), [])
            finally:
                corpus.close()


class ReviewAndAmendmentTests(TestCase):
    def test_review_detects_real_changes_and_ignores_missing_overlay_fields(self):
        from bill_research import bill_revision, changed_fields
        previous = bill_revision({"status_stage": 1, "latest_doc_id": 10.0, "history": "Introduced"})
        same = bill_revision({"status_stage": "1", "latest_doc_id": 10, "history": "Introduced"})
        self.assertEqual(changed_fields(previous, same), [])
        amended = bill_revision({"status_stage": 2, "latest_doc_id": None, "history": "Introduced; Amended"})
        self.assertEqual(changed_fields(previous, amended), ["Status", "History"])
        self.assertEqual(changed_fields({}, amended), [])

    def test_amendment_diff_ignores_markup_and_scripts(self):
        from bill_research import amendment_diff
        before = "<head><title>old</title></head><p>Five days.</p><p>Unchanged.</p>"
        after = "<p><b>Seven</b> days.</p><p>Unchanged.</p><script>internal()</script>"
        diff = amendment_diff(before, after)
        self.assertIn("-Five days.", diff)
        self.assertIn("+Seven days.", diff)
        self.assertNotIn("internal()", diff)
        self.assertEqual(amendment_diff(after, "<div>Seven days.</div><div>Unchanged.</div>"), "")


class JurisdictionSyncTests(TestCase):
    def test_selected_jurisdictions_import_once_and_refresh_existing_corpus(self):
        from job_runner import sync_selected_sessions
        corpus, jobs = Mock(), Mock()
        sessions = {
            "CA": [{"session_id": 5, "session_name": "Regular Session", "year_start": 2025,
                    "year_end": 2026, "last_bootstrap": "already imported"}],
            "US": [
                {"session_id": 7, "session_name": "Previous Congress", "year_start": 2023, "year_end": 2024},
                {"session_id": 8, "session_name": "Current Congress", "year_start": 2025, "year_end": 2026},
                {"session_id": 9, "session_name": "Next Congress", "year_start": 2027, "year_end": 2028},
            ],
        }
        corpus.get_cached_sessions.side_effect = sessions.__getitem__
        corpus.bootstrap_session.return_value = {"new": 4, "updated": 0, "errors": 0}
        corpus.refresh_session.return_value = {"new": 0, "updated": 1, "errors": 0}
        result = sync_selected_sessions(corpus, ["CA", "US"], 2026, jobs)
        corpus.refresh_session.assert_called_once_with(5, "CA", None)
        corpus.bootstrap_session.assert_called_once_with(8, "US", None, allow_individual_fallback=False)
        self.assertFalse(result["CA"]["importing"])
        self.assertTrue(result["US"]["importing"])

    def test_bulk_only_sync_does_not_fall_back_to_individual_requests(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with (
                    patch.object(corpus, "get_dataset_list", return_value=[]),
                    patch.object(corpus, "_bootstrap_via_masterlist") as fallback,
                ):
                    with self.assertRaisesRegex(RuntimeError, "bulk dataset is unavailable"):
                        corpus.bootstrap_session(8, "US", allow_individual_fallback=False)
                    fallback.assert_not_called()
            finally:
                corpus.close()


class DailyReviewQueueTests(TestCase):
    def test_due_changes_unseen_snooze_and_completion_are_independent(self):
        from datetime import date
        from bill_research import bill_revision, build_review_queue
        day = date(2026, 11, 22)
        tracked = {"bill_id": "10", "status_stage": "2"}
        notes = {"10": {"follow_up": "2026-11-21", "reviewed_revision": bill_revision({"status_stage": "1"})}}
        topic = {"name": "Water", "terms": ["water"], "seen": ["11"]}
        topic_rows = [("water", topic, [{"bill_id": "11"}, {"bill_id": "12"}])]
        items = build_review_queue([tracked], notes, topic_rows, {}, day)
        self.assertEqual([item["kind"] for item in items], ["Follow-up", "Bill changed", "Topic match"])
        self.assertEqual(items[-1]["bill"]["bill_id"], "12")
        preferences = {"items": {
            items[0]["key"]: {"reviewed": True},
            items[1]["key"]: {"snoozed_until": "2026-11-23"},
        }}
        self.assertEqual([item["kind"] for item in build_review_queue([tracked], notes, topic_rows, preferences, day)],
                         ["Topic match"])
        self.assertEqual([item["kind"] for item in build_review_queue(
            [tracked], notes, topic_rows, preferences, date(2026, 11, 23))],
            ["Bill changed", "Topic match"])
        notes["10"]["follow_up"] = "2026-11-22"
        self.assertEqual(build_review_queue([tracked], notes, topic_rows, preferences, day)[0]["kind"], "Follow-up")

class StaffImportTests(TestCase):
    def test_repeat_preserves_ids_and_invalid_tabs_preserve_roster(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            staff = StaffManager(str(root / "staff.db"))
            workbook = root / "roster.xlsx"
            with pd.ExcelWriter(workbook) as writer:
                pd.DataFrame([{"Member": "Alex Rivera", "District": 1, "Chief of Staff": "Sam Lee"}]).to_excel(
                    writer, sheet_name="Assembly", index=False,
                )
            self.assertTrue(staff.ingest_spreadsheet(workbook)[0])
            member_id = staff.get_all_legislators().iloc[0]["legislator_id"]
            staff_id = staff.get_legislator_staff(member_id)[0]["staff_id"]
            self.assertTrue(staff.ingest_spreadsheet(workbook)[0])
            self.assertEqual(staff.get_all_legislators().iloc[0]["legislator_id"], member_id)
            self.assertEqual(staff.get_legislator_staff(member_id)[0]["staff_id"], staff_id)
            invalid = root / "invalid.xlsx"
            with pd.ExcelWriter(invalid) as writer:
                pd.DataFrame([{"Note": "No roster"}]).to_excel(writer, sheet_name="Notes", index=False)
            self.assertFalse(staff.ingest_spreadsheet(invalid)[0])
            self.assertEqual(staff.get_all_legislators().iloc[0]["legislator_id"], member_id)


class ScopedSearchAndCoverageTests(TestCase):
    def test_unseen_vote_retry_filters_before_limit_and_coverage(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with corpus._get_conn() as conn:
                    conn.execute("INSERT INTO sessions (session_id,jurisdiction,session_name) VALUES (10,'CA','Fixture')")
                master = {"status": "OK", "masterlist": {
                    "1": {"bill_id": 1, "change_hash": "a"}, "2": {"bill_id": 2, "change_hash": "b"},
                }}
                first = {"status": "OK", "bill": {
                    "bill_id": 1, "title": "Water", "status_date": "2025-01-01",
                    "sponsors": [{"name": "Alex Rivera"}, {"name": "Sam Lee"}],
                    "committee": {"name": "Natural Resources"}, "texts": [{"doc_id": 100}],
                    "votes": [{"roll_call_id": 20, "total": 1}],
                }}
                second = {"status": "OK", "bill": {
                    "bill_id": 2, "title": "Water update", "status_date": "2026-01-01",
                    "sponsors": [{"name": "Another Person"}], "committee": {"name": "Energy"},
                }}
                with patch.object(corpus, "_api_get", side_effect=[master, first, second, {"status": "ERROR"}]):
                    stats = corpus.refresh_session(10, "CA")
                self.assertEqual(stats["vote_errors"], 1)
                self.assertIsNone(corpus.get_corpus_stats()["last_incremental_CA"])
                roll_call = {"status": "OK", "roll_call": {
                    "roll_call_id": 20, "total": 1,
                    "votes": [{"people_id": 3, "vote_text": "Yea"}],
                }}
                with patch.object(corpus, "_api_get", side_effect=[master, roll_call]) as request:
                    self.assertEqual(corpus.refresh_session(10, "CA")["vote_errors"], 0)
                    self.assertEqual(request.call_args.args[0], {"op": "getRollCall", "id": 20})
                filters = dict(date_range=("2025-01-01", "2025-12-31"), sponsors=["Sam Lee"],
                               committees=["Natural Resources"], session_id=10)
                self.assertEqual(corpus.search_bills(**filters, count_only=True), 1)
                self.assertEqual(corpus.search_bills(**filters, limit=1)["bill_id"].tolist(), [1])
                self.assertEqual(corpus.search_bills(topic_terms=["water"], excluded_bill_ids=["2"], limit=1)["bill_id"].tolist(), [1])
                self.assertTrue(corpus.search_bills(bill_ids=[]).empty)
                row = corpus.get_historical_coverage().iloc[0]
                self.assertEqual([row.bills, row.bills_with_text, row.roll_calls, row.with_individual_votes], [2, 1, 1, 1])
            finally:
                corpus.close()


class AccountIsolationTests(TestCase):
    def test_recreated_username_gets_new_workspace_and_invalidates_old_session(self):
        class Session(dict):
            __setattr__ = dict.__setitem__
        with TemporaryDirectory() as directory:
            state = Session()
            root = Path(directory)
            with (
                patch.multiple(auth, DATA_DIR=directory, USERS_FILE=str(root / "users.json"),
                               USERS_SUBDIR=str(root / "users")),
                patch.object(auth.st, "session_state", state),
                patch.object(auth, "_hash_password", side_effect=lambda password: "hashed-" + password),
                patch.object(auth, "_check_password", side_effect=lambda password, stored: stored == "hashed-" + password),
            ):
                self.assertTrue(auth.register_user("owner", "password")[0])
                self.assertTrue(auth.register_user("analyst", "password")[0])
                before = Path(auth.get_user_data_dir("analyst"))
                (before / "bill_notes.json").write_text("{}", encoding="utf-8")
                self.assertTrue(auth.login_user("analyst", "password")[0])
                self.assertTrue(auth.admin_delete_user("analyst")[0])
                self.assertTrue(auth.register_user("analyst", "password")[0])
                self.assertIsNone(auth.get_current_user())
                after = Path(auth.get_user_data_dir("analyst"))
                self.assertNotEqual(before, after)
                self.assertFalse((after / "bill_notes.json").exists())
                self.assertTrue((before / "bill_notes.json").exists())


class StaffRosterRecoveryTests(TestCase):
    def test_preview_is_read_only_stale_previews_fail_and_restore_is_reversible(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            manager = StaffManager(str(root / "staff.db"))
            first, second = root / "first.xlsx", root / "second.xlsx"
            for path, district, staff_name in (
                (first, 1, "Sam Lee"), (second, 2, "Remy Okafor"),
            ):
                with pd.ExcelWriter(path) as writer:
                    pd.DataFrame([{"Member": "Alex Rivera", "District": district,
                                   "Chief of Staff": staff_name}]).to_excel(writer, sheet_name="Assembly", index=False)
            self.assertTrue(manager.ingest_spreadsheet(first)[0])
            identity = manager.get_all_legislators().iloc[0]["legislator_id"]
            prior_snapshots = manager.get_roster_snapshots()
            ok, preview = manager.ingest_spreadsheet(second, preview=True)
            self.assertTrue(ok, preview)
            self.assertEqual(preview["changes"][0]["updated"], 1)
            self.assertEqual(preview["changes"][1]["added"], 1)
            self.assertEqual(preview["changes"][1]["removed"], 1)
            self.assertEqual(manager.get_roster_snapshots(), prior_snapshots)
            self.assertEqual(manager.get_legislator_staff(identity)[0]["name"], "Sam Lee")
            ok, applied = manager.ingest_spreadsheet(second, expected_revision=preview["revision"])
            self.assertTrue(ok, applied)
            self.assertEqual(manager.get_legislator_staff(identity)[0]["name"], "Remy Okafor")
            self.assertFalse(manager.ingest_spreadsheet(first, expected_revision=preview["revision"])[0])
            self.assertTrue(any(item["Last observed active until"] for item in manager.get_member_assignment_history(identity)))
            revision, changes = manager.preview_roster_restore(applied["snapshot_id"])
            self.assertEqual(changes[1]["removed"], 1)
            with self.assertRaisesRegex(ValueError, "changed after preview"):
                manager.restore_roster_snapshot(applied["snapshot_id"], preview["revision"])
            manager.restore_roster_snapshot(applied["snapshot_id"], revision)
            self.assertEqual(manager.get_legislator_staff(identity)[0]["name"], "Sam Lee")
            self.assertEqual(manager.get_roster_snapshots()[0]["reason"], "Before roster restore")


class ContinuedAuditTests(TestCase):
    def test_manually_reviewed_identity_survives_automatic_remapping(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with corpus._get_conn() as conn:
                    conn.execute("INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (1,'CA','Fixture')")
                    conn.execute("INSERT INTO bills (bill_id,session_id,jurisdiction,bill_number) VALUES (2,1,'CA','AB2')")
                    conn.execute("INSERT INTO people (people_id,name) VALUES (3,'A. Rivera')")
                    conn.execute("INSERT INTO roll_calls (roll_call_id,bill_id) VALUES (4,2)")
                    conn.execute("INSERT INTO legislator_votes (roll_call_id,people_id,vote_text) VALUES (4,3,'Yea')")
                staff = {"state":"CA", "name":"Alex Rivera", "chamber":"Assembly", "legislator_id":"staff-3"}
                self.assertEqual(corpus.list_vote_identities()[0]["people_id"], 3)
                corpus.review_vote_identity(3, staff)
                corpus.sync_people_mapping(pd.DataFrame([staff]))
                self.assertEqual(corpus.get_votes_for_legislator("staff-3")[0]["vote_text"], "Yea")
                self.assertEqual(corpus.list_vote_identities(), [])
                corpus.review_vote_identity(3)
                corpus.sync_people_mapping(pd.DataFrame([staff]))
                self.assertEqual(corpus.get_votes_for_legislator("staff-3"), [])
            finally:
                corpus.close()

    def test_reconnect_preserves_destination_and_moves_only_selected_note(self):
        from bill_workspace import reattach_profile_note
        with TemporaryDirectory() as directory:
            notes = Path(directory, "legislator_notes.json")
            notes.write_text('{"old":"Research note","occupied":"Keep"}', encoding="utf-8")
            with self.assertRaises(NoteConflict):
                reattach_profile_note(notes, "old", "occupied", "Research note")
            reattach_profile_note(notes, "old", "current", "Research note")
            self.assertEqual(read_workspace(notes, {}), {"current": "Research note", "occupied": "Keep"})

    def test_two_workers_cannot_claim_shared_sync_at_once(self):
        from job_manager import JobConflict, JobManager
        with TemporaryDirectory() as directory:
            location = str(Path(directory, "jobs.db"))
            first, second = JobManager(location), JobManager(location)
            def attempt(manager):
                try:
                    return manager.start_job("incremental_refresh", "CA", "10")
                except JobConflict:
                    return None
            with ThreadPoolExecutor(max_workers=2) as pool:
                claims = list(pool.map(attempt, (first, second)))
            winners = [claim for claim in claims if claim]
            self.assertEqual(len(winners), 1)
            first.finish_job(winners[0], "SUCCESS")
            self.assertTrue(second.start_job("keyword_rescan", "US", "ALL"))

    def test_request_budget_is_shared_within_job_and_resets_between_jobs(self):
        from request_budget import allow_request, job_request_budget
        with patch.dict("os.environ", {"LEGISCAN_JOB_REQUEST_LIMIT": "2"}):
            with job_request_budget() as budget:
                self.assertTrue(allow_request())
                self.assertTrue(allow_request())
                self.assertFalse(allow_request())
                self.assertEqual((budget.used, budget.exhausted), (2, True))
            with job_request_budget():
                self.assertTrue(allow_request())


class DurableSyncQueueTests(TestCase):
    def test_queue_persists_deduplicates_and_runs_one_claim(self):
        from job_manager import JobManager
        from job_runner import execute_next_job
        with TemporaryDirectory() as directory:
            location = str(Path(directory, "jobs.db"))
            initial = JobManager(location)
            first = initial.enqueue_job("incremental_refresh", "CA", "10", {})
            self.assertEqual(initial.enqueue_job("incremental_refresh", "CA", "10", {}), first)
            second = initial.enqueue_job("bootstrap_corpus", "US", "20", {})
            worker = JobManager(location)
            corpus = Mock()
            corpus.refresh_session.return_value = {"new": 1, "updated": 0, "errors": 0}
            corpus.bootstrap_session.return_value = {"new": 3, "updated": 0, "errors": 0}
            self.assertEqual(worker.get_job(first)["status"], "QUEUED")
            self.assertEqual(execute_next_job(corpus, directory, worker), first)
            self.assertEqual(worker.get_job(first)["status"], "SUCCESS")
            self.assertEqual(worker.get_job(second)["status"], "QUEUED")
            self.assertEqual(execute_next_job(corpus, directory, worker), second)
            self.assertEqual(worker.get_job(second)["new_items"], 3)
            self.assertFalse(corpus.bootstrap_session.call_args.kwargs["allow_individual_fallback"])
            self.assertIsNone(execute_next_job(corpus, directory, worker))

    def test_expired_worker_claim_recovers_once_and_keeps_attempt_history(self):
        import sqlite3
        from contextlib import closing
        from job_manager import JobManager
        with TemporaryDirectory() as directory:
            location = str(Path(directory, "jobs.db"))
            manager = JobManager(location)
            identifier = manager.enqueue_job("incremental_refresh", "CA", "10", {})
            self.assertEqual(manager.claim_next_job()["job_id"], identifier)
            self.assertIsNone(JobManager(location).claim_next_job())
            with closing(sqlite3.connect(location)) as connection, connection:
                connection.execute("UPDATE system_jobs SET heartbeat_at='2000-01-01' WHERE job_id=?", (identifier,))
            resumed = JobManager(location).claim_next_job()
            self.assertEqual(resumed["retry_of"], identifier)
            self.assertEqual(resumed["attempt"], 2)
            self.assertEqual(manager.get_job(identifier)["status"], "INTERRUPTED")
            manager.finish_job(identifier, "SUCCESS")
            self.assertEqual(manager.get_job(identifier)["status"], "INTERRUPTED")
            with closing(sqlite3.connect(location)) as connection, connection:
                connection.execute(
                    "UPDATE system_jobs SET heartbeat_at='2000-01-01' WHERE job_id=?", (resumed["job_id"],)
                )
            self.assertIsNone(manager.claim_next_job())
            self.assertEqual(manager.get_job(resumed["job_id"])["status"], "INTERRUPTED")

    def test_queue_failure_preserves_checkpoint_and_can_be_retried(self):
        from job_manager import JobManager
        from job_runner import execute_next_job
        from request_budget import allow_request
        with TemporaryDirectory() as directory:
            manager = JobManager(str(Path(directory, "jobs.db")))
            identifier = manager.enqueue_job("incremental_refresh", "CA", "10", {})
            corpus = Mock()
            def partial(*args):
                self.assertTrue(allow_request())
                self.assertFalse(allow_request())
                return {"new": 1, "updated": 0, "errors": 1}
            with patch.dict("os.environ", {"LEGISCAN_JOB_REQUEST_LIMIT": "1"}):
                corpus.refresh_session.side_effect = partial
                execute_next_job(corpus, directory, manager)
            self.assertEqual(manager.get_job(identifier)["status"], "FAILED")
            self.assertEqual(manager.get_job(identifier)["new_items"], 1)
            self.assertEqual(manager.get_job(identifier)["api_calls"], 1)
            self.assertIn("budget", manager.get_job(identifier)["error_summary"])
            retried = manager.retry_queued_job(identifier, initiated_by="admin")
            corpus.refresh_session.side_effect = None
            corpus.refresh_session.return_value = {"new": 0, "updated": 1, "errors": 0}
            execute_next_job(corpus, directory, manager)
            self.assertEqual(manager.get_job(retried)["status"], "SUCCESS")


class PrivateRestoreRehearsalTests(TestCase):
    def test_quiesced_data_backup_restores_independent_instance(self):
        from hashlib import sha256
        from bill_workspace import resolve_attachment
        from shutil import copytree
        from job_manager import JobManager

        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            workspace = source / "workspaces" / "fixture-account"
            uploads = workspace / "uploads"
            uploads.mkdir(parents=True)
            (source / "users.json").write_text(
                '{"analyst":{"workspace_id":"fixture-account","role":"user"}}', encoding="utf-8",
            )
            fixture_upload = uploads / "fixture.pdf"
            fixture_upload.write_bytes(b"%PDF-1.0\nRestore fixture")
            checksum = sha256(fixture_upload.read_bytes()).hexdigest()
            update_tracking(workspace / "tracked_bills.json", add="101")
            update_bill_note(workspace / "bill_notes.json", "101", {}, {
                "comment": "Track amendments", "topics": ["Water"], "files": [str(fixture_upload)],
            })
            staff = StaffManager(str(source / "staff.db"))
            workbook = root / "roster.xlsx"
            with pd.ExcelWriter(workbook) as writer:
                pd.DataFrame([{"Member": "Alex Rivera", "District": 1, "Chief of Staff": "Sam Lee"}]).to_excel(
                    writer, sheet_name="Assembly", index=False,
                )
            self.assertTrue(staff.ingest_spreadsheet(workbook)[0])
            member_id = staff.get_all_legislators().iloc[0]["legislator_id"]
            staff_id = staff.get_legislator_staff(member_id)[0]["staff_id"]
            corpus = CorpusManager(str(source / "bills.db"), "")
            try:
                with corpus._get_conn() as conn:
                    conn.execute("""INSERT INTO sessions (session_id, jurisdiction, session_name, last_bootstrap)
                                    VALUES (10, 'CA', 'Archive', '2025-01-01')""")
                    conn.execute("INSERT INTO bills (bill_id, session_id, jurisdiction, bill_number) VALUES (101,10,'CA','AB101')")
            finally:
                corpus.close()
            jobs = JobManager(str(source / "jobs.db"))
            queued = jobs.enqueue_job("incremental_refresh", "CA", "10", {})

            backup = copytree(source, root / "backup")
            restored = copytree(backup, root / "restored")
            restored_workspace = restored / "workspaces" / "fixture-account"
            self.assertEqual(read_workspace(restored_workspace / "tracked_bills.json", []), ["101"])
            note = read_workspace(restored_workspace / "bill_notes.json", {})["101"]
            self.assertEqual(note["comment"], "Track amendments")
            restored_attachment = resolve_attachment(restored_workspace / "uploads", note["files"][0])
            self.assertEqual(restored_attachment.parent, (restored_workspace / "uploads").resolve())
            self.assertEqual(sha256(restored_attachment.read_bytes()).hexdigest(), checksum)
            self.assertIsNone(resolve_attachment(restored_workspace / "uploads", str(restored / "users.json")))
            restored_staff = StaffManager(str(restored / "staff.db"))
            self.assertEqual(restored_staff.get_all_legislators().iloc[0]["legislator_id"], member_id)
            self.assertEqual(restored_staff.get_legislator_staff(member_id)[0]["staff_id"], staff_id)
            restored_corpus = CorpusManager(str(restored / "bills.db"), "")
            try:
                coverage = restored_corpus.get_historical_coverage().iloc[0]
                self.assertEqual((coverage["session_name"], coverage["bills"]), ("Archive", 1))
            finally:
                restored_corpus.close()
            self.assertEqual(JobManager(str(restored / "jobs.db")).get_job(queued)["status"], "QUEUED")
            update_tracking(restored_workspace / "tracked_bills.json", add="102")
            self.assertEqual(read_workspace(workspace / "tracked_bills.json", []), ["101"])


class OfficialServiceTermTests(TestCase):
    def test_exact_and_year_only_terms_preserve_precision(self):
        from datetime import date
        from service_terms import fetch_congress_member_terms, vote_service_status
        response = Mock(status_code=200)
        response.json.return_value = {"member": {
            "bioguideId": "A000001", "directOrderName": "Alex Rivera",
            "terms": {"item": [{"congress": 119, "chamber": "House of Representatives",
                                "startYear": 2025, "endYear": 2027}]},
        }}
        with patch("service_terms.requests.get", return_value=response) as request:
            member = fetch_congress_member_terms("a000001", "fixture")
        self.assertEqual(request.call_args.args[0], "https://api.congress.gov/v3/member/A000001")
        self.assertEqual(member["terms"][0]["start"], "2025")
        with TemporaryDirectory() as directory:
            staff = StaffManager(str(Path(directory, "staff.db")))
            staff.save_congress_service_terms("federal:7", member)
            with self.assertRaises(ValueError):
                staff.save_congress_service_terms("federal:9", member)
            staff.save_exact_service_term(
                "federal:7", "US", "House", date(2025, 2, 1), date(2027, 1, 3),
                "https://www.congress.gov/member/example/A000001",
            )
            terms = staff.get_service_terms("federal:7")
            base = {"jurisdiction": "US", "vote_chamber": "H", "session": "119th Congress"}
            self.assertEqual(vote_service_status({**base, "vote_date": "2025-03-01"}, terms), "Within exact dates")
            self.assertEqual(vote_service_status({**base, "vote_date": "2025-01-01"}, terms),
                             "Consistent with year-only record")
            self.assertEqual(vote_service_status({**base, "vote_date": "2025-01-01", "session": "118th Congress"}, terms),
                             "No matching saved term")
            self.assertEqual(vote_service_status({**base, "vote_date": "2025-03-01", "vote_chamber": "S"}, terms),
                             "No matching saved term")
            with self.assertRaises(ValueError):
                staff.save_exact_service_term(
                    "directory:member", "CA", "Assembly", date(2024, 12, 1), date(2026, 12, 1),
                    "https://senate.ca.gov.example.org/record",
                )

    def test_federal_profile_queries_do_not_mix_in_state_votes(self):
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with corpus._get_conn() as conn:
                    conn.executemany(
                        "INSERT INTO sessions (session_id,jurisdiction,session_name) VALUES (?,?,?)",
                        [(10, "US", "119th Congress"), (11, "CA", "2025-2026")],
                    )
                    conn.executemany(
                        "INSERT INTO bills (bill_id,session_id,jurisdiction,bill_number) VALUES (?,?,?,?)",
                        [(20, 10, "US", "HR1"), (21, 11, "CA", "AB1")],
                    )
                    conn.execute("INSERT INTO people (people_id,name) VALUES (7,'Alex Rivera')")
                    conn.executemany("INSERT INTO roll_calls (roll_call_id,bill_id,chamber) VALUES (?,?,?)",
                                     [(30, 20, "H"), (31, 21, "A")])
                    conn.executemany("INSERT INTO legislator_votes (roll_call_id,people_id,vote_text) VALUES (?,7,'Yea')",
                                     [(30,), (31,)])
                profile = corpus.list_vote_identities("Rivera", include_linked=True, jurisdiction="US")[0]
                self.assertEqual((profile["jurisdictions"], profile["votes"]), ("US", 1))
                self.assertEqual(corpus.get_federal_member_votes(7)[0]["vote_chamber"], "H")
                self.assertEqual(len(corpus.get_federal_member_votes(7)), 1)
            finally:
                corpus.close()


class DecisionHistoryTests(TestCase):
    def test_history_is_atomic_and_keeps_evidence_for_each_bill(self):
        with TemporaryDirectory() as directory:
            notes = Path(directory, "bill_notes.json")
            reviewed = {"Text version": "101", "Status": "1", "History": "Initial history"}
            first = update_bill_note(
                notes, "10", {}, {"reviewed_revision": reviewed, "position": "Watch"},
                actor="analyst", reason="Monitor the introduced version",
            )["10"]
            self.assertEqual(first["decision_history"][0]["evidence"], {"Text version": "101", "Status": "1"})
            with self.assertRaises(NoteConflict):
                update_bill_note(notes, "10", {}, {"position": "Oppose"}, actor="other")
            self.assertEqual(read_workspace(notes, {})["10"]["decision_history"], first["decision_history"])
            second = update_bill_note(
                notes, "10", first, {"last_reviewed": "2026-01-01", "reviewed_revision": reviewed}, actor="analyst",
            )["10"]
            self.assertEqual(len(second["decision_history"]), 1)
            amend = {"Text version": "102", "Status": "2"}
            final = update_bill_note(
                notes, "10", second, {"position": "Support", "reviewed_revision": amend}, actor="analyst",
            )["10"]
            self.assertEqual(final["decision_history"][1]["changes"]["position"]["before"], "Watch")
            self.assertEqual(final["decision_history"][1]["evidence"]["Text version"], "102")
            self.assertEqual(final["decision_history"][0]["reason"], "Monitor the introduced version")


class AdvancedTopicTests(TestCase):
    def test_sql_and_visible_evidence_use_same_literal_rules(self):
        from bill_research import parse_topic_rules, topic_match_reason
        rules = parse_topic_rules("water, sewer", "CAFÉ, funding", "pilot")
        rows = [
            {"bill_id": 1, "title": "Water funding", "description": "Café connection", "subjects": "Utilities"},
            {"bill_id": 2, "title": "Sewer funding pilot", "description": "Café testing", "subjects": ""},
            {"bill_id": 3, "title": "Water infrastructure", "description": "Café", "subjects": ""},
            {"bill_id": 4, "title": "Housing funding", "description": "Café", "subjects": ""},
        ]
        self.assertEqual(topic_match_reason(rows[0], rules), "café (Summary) | funding (Title) | water (Title)")
        self.assertFalse(any(topic_match_reason(row, rules) for row in rows[1:]))
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                with corpus._get_conn() as conn:
                    conn.executemany(
                        "INSERT INTO bills (bill_id, title, description, subjects) VALUES (:bill_id,:title,:description,:subjects)",
                        rows,
                    )
                result = corpus.search_bills(
                    topic_terms=rules["terms"], topic_required=rules["required"], topic_excluded=rules["excluded"],
                )
                self.assertEqual(result["bill_id"].tolist(), [1])
                self.assertEqual(corpus.indexed_bill_ids(["1", "999"]), {"1"})
            finally:
                corpus.close()
        with self.assertRaises(ValueError):
            parse_topic_rules("", "water", "water")


class HearingCalendarTests(TestCase):
    def test_calendar_export_uses_date_markers_and_excludes_explicit_cancellations(self):
        from hearing_calendar import export_calendar, normalize_calendar
        events = normalize_calendar("10", [
            {"date": "2026-11-25", "type": "Committee Hearing", "time": "13:30:00",
             "location": "Room A; Capitol", "description": "Hearing on " + "é" * 75},
            {"date": "2026-11-26", "type": "Hearing", "description": "Hearing canceled by the committee"},
        ])
        self.assertTrue(events[1]["cancelled"])
        calendar = export_calendar([{**event, "bill_number": "AB10", "title": "Water, funding",
                                     "source_url": "https://example.gov/bill"} for event in events])
        self.assertEqual(calendar.count("BEGIN:VEVENT"), 1)
        self.assertTrue(all(len(line.encode("utf-8")) <= 75 for line in calendar.split("\r\n")))
        unfolded = calendar.replace("\r\n ", "")
        self.assertIn("DTSTART;VALUE=DATE:20261125", unfolded)
        self.assertIn("13:30:00 (time zone unavailable)", unfolded)
        self.assertIn("Room A\\; Capitol", unfolded)
        self.assertNotIn("20261126", unfolded)

    def test_valid_empty_feed_clears_snapshot_while_unavailable_feed_preserves_it(self):
        from corpus_manager import _flatten_bill_to_row
        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "")
            try:
                metadata = {"bill_id": "10", "jurisdiction": "CA", "bill_number": "AB10", "title": "Water"}
                details = {"bill_id": 10, "bill_number": "AB10", "title": "Water", "calendar": [
                    {"date": "2026-11-25", "time": "13:30:00", "type": "Committee hearing"},
                ]}
                with corpus._get_conn() as conn:
                    conn.execute("INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (1,'CA','Fixture')")
                    corpus._upsert_bill(conn, _flatten_bill_to_row(details, "CA", 1), {"new": 0, "updated": 0})
                    conn.execute("UPDATE bill_calendar_snapshots SET fetched_at='2000-01-01'")
                self.assertEqual(len(corpus.get_bill_calendars(["10"])["10"]["events"]), 1)
                self.assertEqual(corpus.calendar_refresh_candidates({"10": metadata}), [metadata])
                with patch.object(corpus, "get_bill", return_value={"bill_id": 10}):
                    result = corpus.refresh_tracked_calendars([metadata])
                self.assertEqual(result["unavailable"], 1)
                self.assertEqual(len(corpus.get_bill_calendars(["10"])["10"]["events"]), 1)
                self.assertEqual(corpus.calendar_refresh_candidates({"10": metadata}), [])
                with corpus._get_conn() as conn:
                    conn.execute("UPDATE bill_calendar_attempts SET checked_at='2000-01-01'")
                with patch.object(corpus, "get_bill", return_value={"bill_id": 10, "calendar": []}):
                    result = corpus.refresh_tracked_calendars([metadata])
                self.assertEqual(result["updated"], 1)
                self.assertEqual(corpus.get_bill_calendars(["10"])["10"]["events"], [])
            finally:
                corpus.close()


class CorrectnessFollowupTests(TestCase):
    def test_keyword_loader_normalizes_and_rejects_bad_shapes(self):
        from legiscanner import DEFAULT_KEYWORDS, load_keywords

        with TemporaryDirectory() as directory:
            path = Path(directory) / "keywords.json"
            self.assertEqual(load_keywords(path), list(DEFAULT_KEYWORDS))
            path.write_text('[" PFAS ", "pfas", "water   quality", ""]', encoding="utf-8")
            self.assertEqual(load_keywords(path), ["PFAS", "water quality"])
            path.write_text('{"invalid": "shape"}', encoding="utf-8")
            with self.assertRaises(ValueError):
                load_keywords(path)

    def test_running_worker_owns_expired_job_until_it_releases_lock(self):
        from job_manager import JobConflict, JobManager
        from job_runner import execute_next_job

        with TemporaryDirectory() as directory:
            path = str(Path(directory) / "jobs.db")
            manager = JobManager(path)
            job = manager.enqueue_job("keyword_rescan", "CA", "ALL", {"states": ["CA"]})
            with manager.worker_ownership() as owned:
                self.assertTrue(owned)
                manager.claim_next_job()
                with closing(sqlite3.connect(path)) as connection, connection:
                    connection.execute("UPDATE system_jobs SET heartbeat_at='2000-01-01' WHERE job_id=?", (job,))
                self.assertIsNone(execute_next_job(None, directory, JobManager(path)))
                with self.assertRaises(JobConflict):
                    manager.start_job("keyword_rescan", "CA")
                self.assertEqual(manager.get_job(job)["status"], "RUNNING")
            self.assertEqual(manager.claim_next_job()["retry_of"], job)


class CrossProcessAccountTests(TestCase):
    def test_registration_waits_for_independent_writer_and_preserves_its_record(self):
        script = """
import os
import sys
from pathlib import Path
os.environ.update(DATA_DIR=sys.argv[1], REPO_DIR="", API_KEY="")
from auth import register_user
Path(sys.argv[1], "child-ready").touch()
ok, message = register_user("second", "fixture-password")
if not ok:
    raise SystemExit(message)
"""
        with TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = root / "users.json"
            existing = {"first": {"role": "admin", "password_hash": "fixture-existing-hash",
                                  "workspace_id": "existing-workspace"}}
            with closing(sqlite3.connect(str(accounts) + ".lock.sqlite3")) as owner:
                owner.execute("BEGIN IMMEDIATE")
                child = subprocess.Popen(
                    [sys.executable, "-B", "-c", script, directory],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                )
                try:
                    deadline = time.monotonic() + 15
                    while not (root / "child-ready").exists() and time.monotonic() < deadline:
                        self.assertIsNone(child.poll())
                        time.sleep(0.02)
                    self.assertTrue((root / "child-ready").exists(), "Child did not reach registration")
                    with self.assertRaises(subprocess.TimeoutExpired):
                        child.communicate(timeout=1.5)
                    self.assertFalse(accounts.exists())
                    accounts.write_text(json.dumps(existing), encoding="utf-8")
                    owner.commit()
                    stdout, stderr = child.communicate(timeout=15)
                    self.assertEqual(child.returncode, 0, stdout + stderr)
                finally:
                    if child.poll() is None:
                        child.kill()
                    child.communicate()
            current = json.loads(accounts.read_text(encoding="utf-8"))
            self.assertEqual(current["first"], existing["first"])
            self.assertEqual(current["second"]["role"], "user")


class BillMarkupTests(TestCase):
    def test_reader_keeps_structure_and_escapes_text_without_source_attributes(self):
        from bill_research import readable_bill_markup

        source = (
            '<head><title>Metadata</title></head><section class="external">'
            '<h2>Section 1</h2><p>A &lt; B &amp; C</p>'
            '<p><del>Old</del><ins>New</ins></p>'
            '<table><tr><td>2025</td></tr></table></section>'
        )
        self.assertEqual(
            readable_bill_markup(source),
            '<section><h2>Section 1</h2><p>A &lt; B &amp; C</p>'
            '<p><del>Old</del><ins>New</ins></p><table><tr><td>2025</td></tr></table></section>',
        )
        with self.assertRaises(ValueError):
            readable_bill_markup("x" * 2_000_001)
