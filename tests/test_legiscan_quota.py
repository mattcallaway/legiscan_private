from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock, patch

from legiscan_quota import LegiScanQuota, MONTHLY_LIMIT_REACHED, RESERVE_REACHED
from request_budget import admit_legiscan_request, job_request_budget, quota_for_directory

# The application configuration creates directories at import time.
with patch.dict("os.environ", {"DATA_DIR": "", "REPO_DIR": "", "API_KEY": ""}):
    import legiscanner
    from job_runner import run_refresh_job, run_rescan_job, scheduled_current_refresh


def api_reply(payload, status=200, headers=None):
    reply = Mock(status_code=status, headers=headers or {})
    reply.json.return_value = payload
    return reply


class SharedQuotaTests(TestCase):
    def test_concurrent_database_handles_share_reserve_and_hard_limit(self):
        with TemporaryDirectory() as directory:
            first = LegiScanQuota(directory)
            second = LegiScanQuota(directory)
            first.set_policy(8, 2, False)
            with ThreadPoolExecutor(max_workers=8) as workers:
                outcomes = list(workers.map(
                    lambda i: (first, second)[i % 2].reserve_request("fixture-server-key", "getSearchRaw", "Keyword scan"),
                    range(25),
                ))
            self.assertEqual(outcomes.count(None), 6)
            self.assertEqual(outcomes.count(RESERVE_REACHED), 19)
            self.assertIsNone(second.reserve_request("fixture-server-key", "getMasterListRaw", "Bill refresh"))
            self.assertIsNone(first.reserve_request(" fixture-server-key ", "getRollCall", "Bill refresh"))
            self.assertEqual(second.reserve_request("fixture-server-key", "getBill", "Bill refresh"), MONTHLY_LIMIT_REACHED)
            self.assertEqual(first.summary("fixture-server-key")["sent"], 8)
            self.assertEqual(first.summary("other-key")["sent"], 0)
            self.assertNotIn(b"fixture-server-key", first.path.read_bytes())

    def test_provider_total_and_pacific_month_rollover(self):
        november = datetime(2026, 12, 1, 7, 59, tzinfo=timezone.utc)
        december = datetime(2026, 12, 1, 8, 0, tzinfo=timezone.utc)
        with TemporaryDirectory() as directory:
            quota = LegiScanQuota(directory)
            quota.set_policy(10, 2, False)
            quota.reserve_request("server", "getBill", "Bill research", now=november)
            quota.record_provider_usage("server", 8, now=november)
            quota.record_provider_usage("server", 3, now=november)
            self.assertEqual(quota.summary("server", now=november)["external"], 7)
            self.assertEqual(quota.reserve_request("server", "getBill", "Hearing refresh", now=november), RESERVE_REACHED)
            self.assertIsNone(quota.reserve_request("server", "getBill", "Bill refresh", now=november))
            self.assertEqual(quota.summary("server", now=november)["used"], 9)
            self.assertIsNone(quota.reserve_request("server", "getBill", "Hearing refresh", now=december))
            self.assertEqual(quota.summary("server", now=december)["used"], 1)

    def test_denied_monthly_request_does_not_use_the_job_budget(self):
        with TemporaryDirectory() as directory:
            quota_for_directory(directory).set_policy(2, 1, False)
            self.assertIsNone(admit_legiscan_request(directory, "server", "getBill"))
            with job_request_budget("Keyword scan") as budget:
                self.assertEqual(admit_legiscan_request(directory, "server", "getSearchRaw"), RESERVE_REACHED)
                self.assertEqual(budget.stop_reason, RESERVE_REACHED)
                self.assertEqual(budget.used, 0)

    def test_provider_pause_stops_the_current_job_and_other_callers(self):
        from corpus_manager import CorpusManager
        from job_manager import JobManager

        with TemporaryDirectory() as directory:
            corpus = CorpusManager(str(Path(directory, "bills.db")), "server", rate_limit_s=0)
            jobs = JobManager(str(Path(directory, "jobs.db")))
            master = {"status": "OK", "masterlist": {
                str(i): {"bill_id": i, "change_hash": "new"} for i in range(1, 5)
            }}
            refused = {"status": "ERROR", "alert": {"message": "Monthly query limit reached for key server"}}
            with patch("corpus_manager.requests.get", side_effect=[api_reply(master), api_reply(refused)]) as request:
                result = run_refresh_job(corpus, 10, "CA", jobs)
                self.assertIsNone(corpus.get_bill(3))
                self.assertEqual(request.call_count, 2)
            self.assertEqual(result["api_calls"], 2)
            self.assertIn("request limit", result["request_stop_reason"])
            self.assertEqual(jobs.get_recent_jobs()[0]["status"], "FAILED")
            snapshot = LegiScanQuota(directory).summary("server")
            self.assertIn("request limit", snapshot["pause"]["reason"])
            self.assertEqual(snapshot["breakdown"][("Bill refresh", "getBill")], 1)
            self.assertIsNone(LegiScanQuota(directory).summary("another-key")["pause"])

    def test_keyword_scan_reuses_matching_corpus_revision(self):
        from corpus_manager import CorpusManager, _flatten_bill_to_row
        from job_manager import JobManager

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "keywords.json").write_text('["water", "transit"]', encoding="utf-8")
            corpus = CorpusManager(str(root / "bills.db"), "server")
            with corpus.connection() as conn, conn:
                conn.execute("INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (10, 'CA', 'Fixture')")
                row = _flatten_bill_to_row({"bill_id": 5, "title": "Water policy", "change_hash": "same"}, "CA", 10)
                corpus._upsert_bill(conn, row, {"new": 0, "updated": 0})
            response = api_reply({"status": "OK", "searchresult": {"results": [
                {"bill_id": 5, "change_hash": "same", "relevance": 100},
            ]}})
            with (patch.object(legiscanner, "API_KEY", "server"), patch.object(legiscanner.time, "sleep"),
                  patch("legiscanner.requests.get", return_value=response) as request):
                result = run_rescan_job(corpus, ["CA"], directory, JobManager(str(root / "jobs.db")),
                                        keywords=[" water ", "WATER"])
            request.assert_called_once()
            self.assertEqual(request.call_args.kwargs["params"]["query"], "water")
            self.assertEqual(result["api_calls"], 1)
            self.assertIn("Water policy", (root / "LegiScan_Enhanced_Full_Tracker.csv").read_text(encoding="utf-8"))
            self.assertEqual(quota_for_directory(root).summary("server")["breakdown"], {("Keyword scan", "getSearchRaw"): 1})

    def test_version_lookup_reuses_metadata_until_corpus_changes(self):
        from corpus_manager import CorpusManager, _flatten_bill_to_row

        with TemporaryDirectory() as directory:
            root = Path(directory)
            corpus = CorpusManager(str(root / "bills.db"), "server", rate_limit_s=0)
            with corpus.connection() as conn, conn:
                conn.execute("INSERT INTO sessions (session_id, jurisdiction, session_name) VALUES (10, 'CA', 'Fixture')")
                row = _flatten_bill_to_row({"bill_id": 5, "change_hash": "old"}, "CA", 10)
                corpus._upsert_bill(conn, row, {"new": 0, "updated": 0})
            old = api_reply({"status": "OK", "bill": {"bill_id": 5, "texts": [{"doc_id": 101}]}})
            new = api_reply({"status": "OK", "bill": {"bill_id": 5, "texts": [{"doc_id": 102}]}})
            with patch("corpus_manager.requests.get", side_effect=[old, new]) as request:
                self.assertEqual(corpus.get_bill_versions(5), [{"doc_id": 101}])
                self.assertEqual(corpus.get_bill_versions(5), [{"doc_id": 101}])
                with corpus.connection() as conn, conn:
                    conn.execute("UPDATE bills SET change_hash='new' WHERE bill_id=5")
                self.assertEqual(corpus.get_bill_versions(5), [{"doc_id": 102}])
                self.assertEqual(request.call_count, 2)
            self.assertEqual(quota_for_directory(root).summary("server")["sent"], 2)

    def test_weekday_schedule_honors_pause_and_skips_refreshed_sessions(self):
        from corpus_manager import CorpusManager
        from job_manager import JobManager

        with TemporaryDirectory() as directory:
            root = Path(directory)
            quota = quota_for_directory(root)
            corpus = CorpusManager(str(root / "bills.db"), "server")
            jobs = JobManager(str(root / "jobs.db"))
            monday = date(2026, 11, 23)
            with corpus.connection() as conn, conn:
                conn.execute("""
                    INSERT INTO sessions (session_id, jurisdiction, session_name, year_start, year_end, last_masterlist)
                    VALUES (10, 'CA', 'Fixture', 2025, 2026, '2026-11-23T19:30:00+00:00')
                """)
            with patch("job_runner.sync_selected_sessions", return_value={"US": {"session": "Congress"}}) as sync:
                self.assertIn("skipped", scheduled_current_refresh(corpus, ["US"], jobs, today=date(2026, 11, 22))["US"])
                quota.set_policy(10, 2, True)
                self.assertIn("paused", scheduled_current_refresh(corpus, ["US"], jobs, today=monday)["US"]["skipped"])
                quota.set_policy(10, 2, False)
                result = scheduled_current_refresh(corpus, ["CA", "US"], jobs, today=monday)
                self.assertIn("already refreshed", result["CA"]["skipped"])
                sync.assert_called_once_with(corpus, ["US"], 2026, jobs)

    def test_worker_claims_core_refresh_before_queued_optional_work(self):
        from job_manager import JobManager

        with TemporaryDirectory() as directory:
            jobs = JobManager(str(Path(directory, "jobs.db")))
            optional = jobs.enqueue_job("keyword_rescan", "CA", "ALL", {"states": ["CA"]})
            core = jobs.enqueue_job("current_session_sync", "CA", "CURRENT", {"year": 2026})
            self.assertEqual(jobs.claim_next_job()["job_id"], core)
            jobs.finish_job(core, "SUCCESS")
            self.assertEqual(jobs.claim_next_job()["job_id"], optional)
