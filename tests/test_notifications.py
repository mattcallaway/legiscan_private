import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, skipUnless
from unittest.mock import Mock, patch
from uuid import uuid4

from notifications import (
    MailError, Notifications, normalize_email, preferences_sync, read_notification_watchlists,
    send_resend, snapshot_statuses,
)


class NotificationTests(TestCase):
    def test_email_addresses_validate_local_and_domain_labels(self):
        self.assertEqual(normalize_email(" susan+tracker@EXAMPLE.COM "), "susan+tracker@example.com")
        for value in ("a b@example.com", "a\nb@example.com", "a@example/com", "a@-example.com",
                      "a@example..com", "a..b@example.com", "a" * 65 + "@example.com"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_email(value)

    def test_status_baseline_changes_and_removal(self):
        old = {"1": "0", "2": "1", "3": "2"}
        current = {"1": {"status": 1}, "2": {"status": "Unknown"}, "4": {"status": "5"}}
        state, changes = preferences_sync(old, current, {"1", "2", "4"})
        self.assertEqual(state, {"1": "1", "2": "1", "4": "5"})
        self.assertEqual([(bill, before, after) for bill, before, after, _ in changes], [("1", "0", "1")])
        self.assertEqual(preferences_sync(state, current, {"1", "2", "4"})[1], [])

    def test_resend_retries_can_use_same_idempotency_key(self):
        response = Mock(status_code=429)
        with patch("notifications.requests.post", return_value=response) as post:
            with self.assertRaises(MailError) as raised:
                send_resend(recipient="analyst@example.com", subject="Change", body="Body",
                            key="bill-status/fixture", api_key="test-key", sender="test@example.com")
            self.assertTrue(raised.exception.retryable)
            self.assertEqual(post.call_args.kwargs["headers"]["Idempotency-Key"], "bill-status/fixture")
            self.assertEqual(post.call_args.kwargs["json"]["to"], ["analyst@example.com"])
            self.assertNotIn("test-key", str(raised.exception))

    def test_only_current_account_workspaces_are_read(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "users.json").write_text('{"analyst":{"workspace_id":"new"}}', encoding="utf-8")
            for relative, values in (("users/analyst", ["123"]), ("workspaces/new", ["456"])):
                location = root / relative
                location.mkdir(parents=True)
                (location / "tracked_bills.json").write_text(json.dumps(values), encoding="utf-8")
            self.assertEqual(read_notification_watchlists(root), {"account:new": {"456"}})
            self.assertEqual(read_notification_watchlists(root, only_owner="legacy:analyst"), {})


@skipUnless(os.environ.get("TEST_NOTIFICATION_DATABASE_URL"), "requires an isolated PostgreSQL test database")
class PostgresNotificationTests(TestCase):
    def test_verification_status_capture_retry_deduplication_and_untracking(self):
        import psycopg
        from psycopg import sql
        from psycopg.conninfo import make_conninfo

        dsn = os.environ["TEST_NOTIFICATION_DATABASE_URL"]
        schema = "notify_test_" + uuid4().hex
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            fixture_dsn = make_conninfo(dsn, options=f"-c search_path={schema}")
            mailer = Mock(return_value="message-id")
            service = Notifications(fixture_dsn, "test-key", "alerts@example.com", mailer=mailer)
            service.initialize()
            service.initialize()
            with TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "users.json").write_text('{"ana":{}}', encoding="utf-8")
                user_dir = root / "users" / "ana"
                user_dir.mkdir(parents=True)
                watches = user_dir / "tracked_bills.json"
                watches.write_text('["101"]', encoding="utf-8")
                source = root / "LegiScan_Enhanced_Full_Tracker.csv"

                def update_status(value):
                    source.write_text(
                        "bill_id,bill_number,title,status_stage,url\n"
                        f"101,AB101,Water measure,{value},https://legiscan.com/CA/bill/AB101/2025\n",
                        encoding="utf-8",
                    )

                update_status(1)
                service.request_verification("legacy:ana", "ana@example.com")
                code = mailer.call_args.kwargs["body"].split("\n\n")[1]
                with self.assertRaisesRegex(ValueError, "invalid or expired"):
                    service.verify("legacy:ana", "bad-code")
                with self.assertRaisesRegex(ValueError, "10 minutes"):
                    service.request_verification("legacy:ana", "ana@example.com")
                self.assertFalse(service.get_preferences("legacy:ana")["enabled"])
                service.verify("legacy:ana", code)
                self.assertEqual(service.process(root), (0, 0))
                self.assertEqual(mailer.call_count, 1)
                update_status(2)
                mailer.side_effect = MailError("Temporary failure", retryable=True)
                self.assertEqual(service.process(root), (1, 0))
                original_key = mailer.call_args.kwargs["key"]
                with service._connect() as conn:
                    conn.execute("UPDATE notification_outbox SET available_at=now()")
                mailer.side_effect = None
                self.assertEqual(service.process(root), (0, 1))
                self.assertEqual(mailer.call_args.kwargs["key"], original_key)
                self.assertIn("Introduced to Engrossed", mailer.call_args.kwargs["body"])
                self.assertEqual(service.process(root), (0, 0))
                service.set_activity_types("legacy:ana", ["hearings", "amendments", "committees"])
                workspaces = read_notification_watchlists(root)
                details = {"bill_number": "AB101", "status": 2, "latest_doc_id": 1,
                           "committee": "Rules", "hearings": []}
                self.assertEqual(service.capture_status_changes(workspaces, lambda _: {"101": details}), 0)
                details.update(latest_doc_id=2, committee="Appropriations",
                               hearings=[{"date": "2999-12-31", "description": "Budget hearing"}])
                self.assertEqual(service.capture_status_changes(workspaces, lambda _: {"101": details}), 3)
                service.set_activity_types("legacy:ana", ["hearings", "amendments"])
                self.assertEqual(service.deliver_pending(root), 2)
                self.assertEqual(service.capture_status_changes(workspaces, lambda _: {"101": details}), 0)
                subjects = {call.kwargs["subject"] for call in mailer.call_args_list[-2:]}
                self.assertEqual(subjects, {"AB101: Hearing calendar changed", "AB101: Bill text version changed"})
                update_status(3)
                self.assertEqual(service.capture_status_changes(
                    read_notification_watchlists(root), lambda watched: snapshot_statuses(root, watched),
                ), 1)
                watches.write_text("[]", encoding="utf-8")
                self.assertEqual(service.deliver_pending(root), 0)
                self.assertEqual(mailer.call_count, 5)
                service.set_enabled("legacy:ana", False)
                self.assertFalse(service.get_preferences("legacy:ana")["enabled"])
        finally:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
