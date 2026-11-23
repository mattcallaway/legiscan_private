"""Opt-in tracked-bill notifications backed by PostgreSQL and Resend."""
from contextlib import closing
import csv
from datetime import date
from hashlib import sha256
from hmac import compare_digest
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
from uuid import uuid4

from hearing_calendar import source_link

import requests

SCHEMA = (
    """CREATE TABLE IF NOT EXISTS notification_preferences (
        owner text PRIMARY KEY, email text, enabled boolean NOT NULL DEFAULT false,
        enabled_at timestamptz, pending_email text, code_digest text,
        requested_at timestamptz, expires_at timestamptz
    )""",
    """CREATE TABLE IF NOT EXISTS notification_bill_status (
        owner text NOT NULL REFERENCES notification_preferences(owner),
        bill_id text NOT NULL, status text NOT NULL, PRIMARY KEY (owner, bill_id)
    )""",
    """CREATE TABLE IF NOT EXISTS notification_outbox (
        id text PRIMARY KEY, owner text NOT NULL REFERENCES notification_preferences(owner),
        bill_id text NOT NULL, bill_number text NOT NULL, title text NOT NULL, source_url text NOT NULL,
        old_status text NOT NULL, new_status text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        attempts integer NOT NULL DEFAULT 0, state text NOT NULL DEFAULT 'pending',
        provider_id text, last_error text
    )""",
    "ALTER TABLE notification_preferences ADD COLUMN IF NOT EXISTS activity_types text[] NOT NULL DEFAULT '{}'",
    "ALTER TABLE notification_bill_status ADD COLUMN IF NOT EXISTS activity jsonb NOT NULL DEFAULT '{}'",
    "ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS activity_type text",
    "ALTER TABLE notification_outbox ADD COLUMN IF NOT EXISTS activity_detail text",
    """CREATE INDEX IF NOT EXISTS notification_outbox_pending
       ON notification_outbox (available_at, created_at) WHERE state='pending'""",
)

# LegiScan Status / Progress codes. Entries 7–12 can occur in progress records.
STATUS_LABELS = {
    "0": "Pre-filed", "1": "Introduced", "2": "Engrossed", "3": "Enrolled",
    "4": "Passed", "5": "Vetoed", "6": "Failed", "7": "Override",
    "8": "Chaptered", "9": "Referred", "10": "Reported pass",
    "11": "Reported do not pass", "12": "Draft",
}


ACTIVITY_LABELS = {
    "hearings": "Hearing calendar",
    "amendments": "Bill text version",
    "committees": "Committee activity",
}


def activity_snapshot(record):
    """Return observed activity fields, leaving unavailable source data unobserved."""
    activity = {}
    if "hearings" in record:
        activity["hearings"] = sorted({
            " · ".join(str(event[field]) for field in ("date", "time", "type", "description", "location") if event.get(field))
            for event in record["hearings"]
        } - {""})
    if record.get("latest_doc_id"):
        activity["amendments"] = str(record["latest_doc_id"])
    if "committee" in record or "referrals" in record:
        activity["committees"] = " · ".join(
            str(record[field]) for field in ("committee", "referrals") if record.get(field)
        )
    return activity


def activity_description(kind, previous, current):
    if kind == "hearings":
        today = date.today().isoformat()
        added = sorted(item for item in set(current) - set(previous) if item[:10] >= today)
        removed = sorted(item for item in set(previous) - set(current) if item[:10] >= today)
        lines = [*(f"Added or updated: {item}" for item in added),
                 *(f"No longer listed: {item}" for item in removed)]
        return "\n".join(lines)[:3500]
    if kind == "amendments":
        return "The latest indexed bill text version changed. Open the bill to compare versions."
    return f"Previously: {previous[:1500]}\nNow: {current[:1500]}"


class MailError(RuntimeError):
    def __init__(self, message, *, retryable=False):
        super().__init__(message)
        self.retryable = retryable


def account_owner(username, account):
    identifier = account.get("workspace_id")
    return f"account:{identifier}" if identifier else f"legacy:{username}"


def enabled_config():
    return all(os.environ.get(key) for key in ("NOTIFICATION_DATABASE_URL", "RESEND_API_KEY", "RESEND_FROM_EMAIL"))


def normalize_email(value):
    value = value.strip()
    local, separator, domain = value.rpartition("@")
    atom = r"[A-Za-z0-9!#$%&'*+/=?^_{}`|~\-]+"
    label = r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    if (
        not separator
        or len(value) > 254
        or len(local) > 64
        or not re.fullmatch(rf"{atom}(?:\.{atom})*", local)
        or not re.fullmatch(rf"(?:{label}\.)+[A-Za-z]{{2,63}}", domain)
    ):
        raise ValueError("Enter a valid email address.")
    return f"{local}@{domain.lower()}"


def status_code(value):
    value = "" if value is None else str(value).strip()
    if value.endswith(".0") and value[:-2].isdigit():
        value = value[:-2]
    if value.casefold() in {"", "unknown", "nan", "n/a", "none", "<na>"}:
        return ""
    return next((code for code, label in STATUS_LABELS.items() if label.casefold() == value.casefold()), value)


def preferences_sync(previous, current, watched):
    """Seed newly watched bills and report transitions between known statuses."""
    state = {bill: value for bill, value in previous.items() if bill in watched}
    transitions = []
    for bill, record in current.items():
        if bill not in watched:
            continue
        value = status_code(record.get("status"))
        if not value:
            continue
        if bill in state and state[bill] != value:
            transitions.append((bill, state[bill], value, record))
        state[bill] = value
    return state, transitions


def send_resend(*, recipient, subject, body, key, api_key, sender):
    try:
        response = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {api_key}", "Idempotency-Key": key},
            json={"from": sender, "to": [recipient], "subject": subject, "text": body},
            timeout=(5, 15), allow_redirects=False,
        )
    except requests.RequestException as error:
        raise MailError("Could not reach Resend.", retryable=True) from error
    if not 200 <= response.status_code < 300:
        raise MailError(f"Resend returned HTTP {response.status_code}.",
                        retryable=response.status_code in {408, 409, 425, 429} or response.status_code >= 500)
    try:
        identifier = response.json()["id"]
    except (ValueError, KeyError, TypeError) as error:
        raise MailError("Resend returned no delivery ID.", retryable=True) from error
    if not identifier:
        raise MailError("Resend returned no delivery ID.", retryable=True)
    return identifier


class Notifications:
    def __init__(self, dsn, api_key, sender, *, mailer=send_resend):
        self.dsn = dsn
        self.api_key = api_key
        self.sender = sender
        self.mailer = mailer

    @classmethod
    def from_environment(cls):
        if not enabled_config():
            raise RuntimeError("Email notifications have not been configured.")
        return cls(*(os.environ[key] for key in ("NOTIFICATION_DATABASE_URL", "RESEND_API_KEY", "RESEND_FROM_EMAIL")))

    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row
        return psycopg.connect(self.dsn, row_factory=dict_row, connect_timeout=5)

    def initialize(self):
        with self._connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('notification-schema', 0))")
            for statement in SCHEMA:
                conn.execute(statement)

    def process(self, data_dir):
        """Run a serialized, bounded notification pass for this database."""
        with self._connect() as lease:
            result = lease.execute(
                "SELECT pg_try_advisory_lock(hashtextextended('bill-notification-pass', 0)) AS acquired"
            ).fetchone()
            if not result["acquired"]:
                return None
            owners = {row["owner"] for row in lease.execute(
                "SELECT owner FROM notification_preferences WHERE enabled=true AND email IS NOT NULL"
            )}
            # Session advisory locks are released when this connection closes.
            lease.commit()
            watches = read_notification_watchlists(data_dir, owners=owners) if owners else {}
            snapshot = snapshot_statuses(data_dir, set().union(*watches.values()))
            enqueued = self.capture_status_changes(
                watches, lambda bills: {bill: snapshot[bill] for bill in bills & snapshot.keys()},
            )
            return enqueued, self.deliver_pending(data_dir)

    def get_preferences(self, owner):
        with self._connect() as conn:
            row = conn.execute(
                """SELECT email, enabled, activity_types, CASE WHEN expires_at>now() THEN pending_email END AS pending_email
                   FROM notification_preferences WHERE owner=%s""", (owner,),
            ).fetchone()
            return row or {}

    def request_verification(self, owner, destination):
        destination = normalize_email(destination)
        token = secrets.token_urlsafe(18)
        digest = sha256(token.encode()).hexdigest()
        with self._connect() as conn:
            for lock in sorted((f"verification-owner:{owner}", f"verification-email:{destination.casefold()}")):
                conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock,))
            recent = conn.execute(
                """SELECT 1 FROM notification_preferences WHERE requested_at>now()-interval '10 minutes'
                   AND (owner=%s OR lower(pending_email)=lower(%s) OR lower(email)=lower(%s)) LIMIT 1""",
                (owner, destination, destination),
            ).fetchone()
            if recent:
                raise ValueError("Please wait 10 minutes between verification requests.")
            conn.execute(
                """INSERT INTO notification_preferences
                     (owner, pending_email, code_digest, requested_at, expires_at)
                   VALUES (%s, %s, %s, now(), now()+interval '20 minutes')
                   ON CONFLICT (owner) DO UPDATE SET pending_email=excluded.pending_email,
                   code_digest=excluded.code_digest, requested_at=excluded.requested_at, expires_at=excluded.expires_at""",
                (owner, destination, digest),
            )
        self._send(
            destination, "Verify your bill alert email",
            "Paste this code in Email Notifications in the legislation tracker. It expires in 20 minutes.\n\n"
            + token + "\n\nIf you did not request bill alerts, you can ignore this email.",
            f"bill-verification/{uuid4().hex}",
        )

    def _send(self, recipient, subject, body, key):
        return self.mailer(
            recipient=recipient, subject=subject, body=body, key=key, api_key=self.api_key, sender=self.sender,
        )

    def verify(self, owner, code):
        digest = sha256(code.strip().encode()).hexdigest()
        with self._connect() as conn:
            record = conn.execute(
                """SELECT pending_email, code_digest FROM notification_preferences
                   WHERE owner=%s AND expires_at>now() FOR UPDATE""", (owner,),
            ).fetchone()
            if not record or not record["pending_email"] or not compare_digest(record["code_digest"] or "", digest):
                raise ValueError("Verification code is invalid or expired.")
            conn.execute(
                """UPDATE notification_preferences SET email=pending_email, enabled=true, enabled_at=clock_timestamp(),
                   pending_email=NULL, code_digest=NULL, expires_at=NULL WHERE owner=%s""", (owner,),
            )
            conn.execute("DELETE FROM notification_bill_status WHERE owner=%s", (owner,))

    def set_enabled(self, owner, enabled):
        with self._connect() as conn:
            changed = conn.execute(
                """UPDATE notification_preferences SET enabled=%s, enabled_at=clock_timestamp(),
                   expires_at=NULL, pending_email=NULL, code_digest=NULL
                   WHERE owner=%s AND (email IS NOT NULL OR NOT %s) RETURNING owner""",
                (enabled, owner, enabled),
            ).fetchone()
            if changed:
                conn.execute("DELETE FROM notification_bill_status WHERE owner=%s", (owner,))

    def set_activity_types(self, owner, kinds):
        kinds = sorted(set(kinds))
        if any(kind not in ACTIVITY_LABELS for kind in kinds):
            raise ValueError("Unknown alert type.")
        with self._connect() as conn:
            lease = conn.execute(
                "SELECT pg_try_advisory_xact_lock(hashtextextended('bill-notification-pass', 0)) AS acquired"
            ).fetchone()
            if not lease["acquired"]:
                raise ValueError("An alert pass is running. Try saving again shortly.")
            prior = conn.execute(
                "SELECT activity_types FROM notification_preferences WHERE owner=%s AND email IS NOT NULL FOR UPDATE",
                (owner,),
            ).fetchone()
            if not prior:
                raise ValueError("Verify your email first.")
            changed_kinds = set(kinds) ^ set(prior["activity_types"])
            if changed_kinds:
                conn.execute("UPDATE notification_preferences SET activity_types=%s WHERE owner=%s", (kinds, owner))
                conn.execute(
                    "UPDATE notification_bill_status SET activity=activity-%s::text[] WHERE owner=%s",
                    (sorted(changed_kinds), owner),
                )
                conn.execute(
                    "UPDATE notification_outbox SET state='skipped' WHERE owner=%s AND state='pending' AND activity_type=ANY(%s)",
                    (owner, sorted(changed_kinds)),
                )

    def capture_status_changes(self, workspaces, status_lookup):
        total = 0
        with self._connect() as conn:
            owners = [row["owner"] for row in conn.execute(
                "SELECT owner FROM notification_preferences WHERE enabled=true AND email IS NOT NULL"
            )]
        for owner in owners:
            watched = workspaces.get(owner)
            if watched is None:
                continue
            current = status_lookup(watched)
            with self._connect() as conn:
                preference = conn.execute(
                    "SELECT enabled, activity_types FROM notification_preferences WHERE owner=%s FOR UPDATE", (owner,),
                ).fetchone()
                if not preference or not preference["enabled"]:
                    continue
                baseline = {row["bill_id"]: row for row in conn.execute(
                    "SELECT bill_id, status, activity FROM notification_bill_status WHERE owner=%s", (owner,),
                )}
                before = {bill: row["status"] for bill, row in baseline.items()}
                state, transitions = preferences_sync(before, current, watched)
                removed = list(before.keys() - state.keys())
                if removed:
                    conn.execute("DELETE FROM notification_bill_status WHERE owner=%s AND bill_id=ANY(%s)", (owner, removed))
                for bill, value in state.items():
                    if before.get(bill) != value:
                        conn.execute(
                            """INSERT INTO notification_bill_status (owner,bill_id,status) VALUES (%s,%s,%s)
                               ON CONFLICT(owner,bill_id) DO UPDATE SET status=excluded.status""",
                            (owner, bill, value),
                        )
                activities = set(preference["activity_types"])
                for bill in state.keys() & current.keys():
                    previous = baseline.get(bill, {}).get("activity", {})
                    observed = activity_snapshot(current[bill])
                    updated = {**previous, **{key: observed[key] for key in activities & observed.keys()}}
                    if updated != previous:
                        conn.execute(
                            "UPDATE notification_bill_status SET activity=%s::jsonb WHERE owner=%s AND bill_id=%s",
                            (json.dumps(updated), owner, bill),
                        )
                    for kind in sorted(activities & observed.keys() & previous.keys()):
                        if observed[kind] == previous[kind]:
                            continue
                        detail = activity_description(kind, previous[kind], observed[kind])
                        if not detail:
                            continue
                        record = current[bill]
                        conn.execute(
                            """INSERT INTO notification_outbox
                               (id,owner,bill_id,bill_number,title,source_url,old_status,new_status,activity_type,activity_detail)
                               VALUES (%s,%s,%s,%s,%s,%s,'','',%s,%s)""",
                            (uuid4().hex, owner, bill, record.get("bill_number") or bill,
                             record.get("title") or "", source_link(record.get("url")), kind, detail),
                        )
                        total += 1
                for bill, old, new, record in transitions:
                    conn.execute(
                        """INSERT INTO notification_outbox
                           (id,owner,bill_id,bill_number,title,source_url,old_status,new_status)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (uuid4().hex, owner, bill, record.get("bill_number") or bill,
                         record.get("title") or "", source_link(record.get("url")), old, new),
                    )
                    total += 1
        return total

    def deliver_pending(self, data_dir, *, limit=50):
        sent = 0
        for _ in range(limit):
            with self._connect() as conn:
                job = conn.execute(
                    """SELECT *, created_at<now()-interval '20 hours' AS expired FROM notification_outbox
                       WHERE state='pending' AND available_at<=now()
                       ORDER BY available_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1"""
                ).fetchone()
                if not job:
                    break
                preference = conn.execute(
                    """SELECT email, enabled, activity_types, enabled_at<=%s AS same_subscription
                       FROM notification_preferences WHERE owner=%s FOR UPDATE""",
                    (job["created_at"], job["owner"]),
                ).fetchone()
                allowed = (
                    not job["expired"] and preference and preference["enabled"] and preference["same_subscription"]
                    and (job["activity_type"] is None or job["activity_type"] in preference["activity_types"])
                )
                watched = read_notification_watchlists(data_dir, only_owner=job["owner"]) if allowed else {}
                if job["bill_id"] not in watched.get(job["owner"], set()):
                    conn.execute("UPDATE notification_outbox SET state='skipped' WHERE id=%s", (job["id"],))
                    continue
                number = " ".join(job["bill_number"].split())
                old = STATUS_LABELS.get(job["old_status"], job["old_status"])
                new = STATUS_LABELS.get(job["new_status"], job["new_status"])
                if job["activity_type"]:
                    subject = f"{number}: {ACTIVITY_LABELS[job['activity_type']]} changed"
                    detail = job["activity_detail"]
                else:
                    subject = f"{number}: bill status changed"
                    detail = f"Status changed from {old} to {new}."
                body = (f"{number} — {job['title']}\n\n{detail}\n\n{job['source_url']}\n\n"
                        "You are receiving this because you track this bill. "
                        "Manage alerts in Email Notifications in the tracker.")
                try:
                    provider_id = self._send(preference["email"], subject, body, f"bill-status/{job['id']}")
                except MailError as error:
                    attempt = job["attempts"] + 1
                    retry = error.retryable and attempt < 6
                    conn.execute(
                        """UPDATE notification_outbox SET attempts=%s, state=%s, last_error=%s,
                           available_at=now()+(%s * interval '1 minute') WHERE id=%s""",
                        (attempt, "pending" if retry else "failed", str(error), min(60, 5 * 2 ** attempt), job["id"]),
                    )
                else:
                    conn.execute("UPDATE notification_outbox SET state='sent', provider_id=%s WHERE id=%s",
                                 (provider_id, job["id"]))
                    sent += 1
        return sent


def read_notification_watchlists(data_dir, *, only_owner=None, owners=None):
    """Read current authenticated workspaces. Deleted accounts have no subscriptions."""
    from bill_workspace import read_workspace

    root = Path(data_dir).resolve()
    with (root / "users.json").open(encoding="utf-8") as file:
        accounts = json.load(file)
    result = {}
    for username, account in accounts.items():
        owner = account_owner(username, account)
        if (only_owner is not None and owner != only_owner) or (owners is not None and owner not in owners):
            continue
        identifier = account.get("workspace_id")
        base = root / ("workspaces" if identifier else "users")
        directory = (base / (identifier or username)).resolve()
        if not directory.is_relative_to(base.resolve()):
            raise ValueError("Invalid user workspace path.")
        watched = read_workspace(directory / "tracked_bills.json", [])
        result[owner] = {str(bill) for bill in watched if str(bill).isascii() and str(bill).isdecimal()}
    return result


def snapshot_statuses(data_dir, watched):
    """Prefer the master corpus and use the keyword CSV for bills only indexed there."""
    if not watched:
        return {}
    root = Path(data_dir)
    results = {}
    database = root / "bills.db"
    if database.is_file():
        with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            results = {str(row["bill_id"]): dict(row) for row in conn.execute(
                """SELECT bill_id, bill_number, title, status_stage AS status, url, latest_doc_id, committee, referrals FROM bills
                   WHERE bill_id IN (SELECT value FROM json_each(?))""", (json.dumps(list(watched)),),
            )}
            if conn.execute("SELECT 1 FROM sqlite_master WHERE name='bill_calendar_snapshots'").fetchone():
                for row in conn.execute(
                    """SELECT bill_id, events_json FROM bill_calendar_snapshots
                       WHERE bill_id IN (SELECT value FROM json_each(?))""", (json.dumps(list(results)),),
                ):
                    results[str(row["bill_id"])]["hearings"] = json.loads(row["events_json"])
    snapshot = root / "LegiScan_Enhanced_Full_Tracker.csv"
    if snapshot.is_file() and watched - results.keys():
        with snapshot.open(encoding="utf-8-sig", newline="") as file:
            for row in csv.DictReader(file):
                bill = row.get("bill_id")
                if bill in watched and bill not in results:
                    results[bill] = {**row, "status": row.get("status_stage") or row.get("status")}
    return results
