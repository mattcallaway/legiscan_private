"""Read and update one user's bill workspace JSON files."""
import json
import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile



class NoteConflict(ValueError):
    """Another editor changed this note after it was opened."""


def _read_json(path, default):
    try:
        with open(path, encoding="utf-8-sig") as file:
            value = json.load(file)
    except FileNotFoundError:
        return default
    if not isinstance(value, type(default)):
        raise ValueError(f"Invalid data in {Path(path).name}")
    return value


def _database_path(path):
    return Path(path).parent / ".bill-workspace.sqlite3"


def _connect(path):
    connection = sqlite3.connect(_database_path(path), timeout=10)
    connection.execute(
        """CREATE TABLE IF NOT EXISTS workspace (
            filename TEXT PRIMARY KEY, payload TEXT NOT NULL, imported_payload TEXT NOT NULL
        )"""
    )
    return connection


def _stored_value(connection, path, default):
    row = connection.execute(
        "SELECT payload FROM workspace WHERE filename = ?", (Path(path).name,),
    ).fetchone()
    return json.loads(row[0]) if row else _read_json(path, default)


def read_workspace(path, default):
    if not _database_path(path).exists():
        return _read_json(path, default)
    with closing(_connect(path)) as connection:
        return _stored_value(connection, path, default)


def update_workspace(path, default, update):
    """Import the old document on first edit, then apply edits transactionally."""
    with closing(_connect(path)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        value = _stored_value(connection, path, default)
        imported_payload = json.dumps(value)
        update(value)
        connection.execute(
            """INSERT INTO workspace (filename, payload, imported_payload) VALUES (?, ?, ?)
               ON CONFLICT(filename) DO UPDATE SET payload = excluded.payload""",
            (Path(path).name, json.dumps(value), imported_payload),
        )
    return value


def export_workspace(path):
    """Refresh the legacy JSON mirror used by the Git backup workflow."""
    with closing(_connect(path)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            "SELECT payload FROM workspace WHERE filename = ?", (Path(path).name,),
        ).fetchone()
        if row is None:
            return
        pending = None
        try:
            with NamedTemporaryFile(
                mode="w", dir=Path(path).parent, prefix=".workspace-",
                encoding="utf-8", delete=False,
            ) as file:
                pending = file.name
                json.dump(json.loads(row[0]), file, indent=2)
            os.replace(pending, path)
        finally:
            if pending is not None:
                Path(pending).unlink(missing_ok=True)


def update_tracking(path, *, add=None, remove=()):
    remove = set(remove)

    def apply(items):
        items[:] = [item for item in items if str(item) not in remove]
        if add is not None and all(str(item) != add for item in items):
            items.append(add)

    return update_workspace(path, [], apply)


DECISION_FIELDS = ("position", "priority", "follow_up", "topics")
REVIEW_EVIDENCE_FIELDS = ("Text version", "Status", "Last action", "Action date")


def update_bill_note(path, bill_id, expected_note, changes, *, actor=None, reason=""):
    """Update the note and append any decision record in the same transaction."""
    reason = reason.strip()
    if len(reason) > 500:
        raise ValueError("Keep the decision reason to 500 characters.")
    if "decision_history" in changes:
        raise ValueError("Decision history is append-only.")

    def apply(notes):
        current = notes.get(bill_id, {})
        if current != expected_note:
            raise NoteConflict("This note changed in another session. Refresh the page to review the latest version before saving.")
        updated = {**current, **changes}
        field_changes = {}
        for field in DECISION_FIELDS:
            default = [] if field == "topics" else ""
            old, new = current.get(field, default), updated.get(field, default)
            if old != new:
                field_changes[field] = {"before": old, "after": new}
        revision = updated.get("reviewed_revision") or {}
        reviewed_new_revision = bool(revision) and revision != current.get("reviewed_revision")
        if actor and (field_changes or reason or reviewed_new_revision):
            event = {
                "actor": actor,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "kind": "Decision" if field_changes or reason else "Review",
                "changes": field_changes,
                "reason": reason,
                "evidence": {field: revision[field] for field in REVIEW_EVIDENCE_FIELDS if revision.get(field)},
            }
            updated["decision_history"] = [*current.get("decision_history", []), event]
        notes[bill_id] = updated

    return update_workspace(path, {}, apply)


def reattach_profile_note(path, previous_key, new_key, expected_text):
    """Move an orphaned note after the user identifies the matching profile."""
    def apply(notes):
        if previous_key == new_key or previous_key not in notes or notes[previous_key] != expected_text:
            raise NoteConflict("The source note changed. Reload the profile notes and try again.")
        if new_key in notes:
            raise NoteConflict("The selected profile already has a note. Its existing note was preserved.")
        notes[new_key] = notes.pop(previous_key)

    return update_workspace(path, {}, apply)


def resolve_attachment(upload_directory, reference):
    """Resolve both portable and historical references inside this workspace."""
    if not isinstance(reference, str):
        return None
    name = reference.replace("\\", "/").rsplit("/", 1)[-1]
    if not name or name in {".", ".."}:
        return None
    root = Path(upload_directory).resolve()
    attachment = (root / name).resolve()
    if attachment.is_relative_to(root) and attachment.is_file():
        return attachment
    return None
