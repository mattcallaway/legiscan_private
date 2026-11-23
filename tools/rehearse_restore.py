"""Restore a cold tracker backup privately, run schema upgrades, and verify preservation."""

import argparse
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path, PurePosixPath
import sqlite3
import sys
import tarfile
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
STORES = ("bills.db", "staff.db", "jobs.db")
STORE_FILES = {name + suffix for name in STORES for suffix in ("", "-wal", "-shm", "-journal")}


class RehearsalError(ValueError):
    """A privacy-safe explanation of a failed rehearsal check."""


def extract_backup(archive, destination):
    with tarfile.open(archive, "r:*") as backup:
        members = backup.getmembers()
        seen = set()
        for member in members:
            path = PurePosixPath(member.name)
            if (
                not path.parts or path.parts[0] != "data" or ".." in path.parts
                or "\\" in member.name or ":" in member.name
                or not (member.isfile() or member.isdir())
                or (len(path.parts) == 1 and not member.isdir())
            ):
                raise RehearsalError("Backup must contain regular files and directories under data/.")
            normalized = path.as_posix().casefold()
            if normalized in seen:
                raise RehearsalError("Backup contains duplicate paths.")
            seen.add(normalized)
        backup.extractall(path=destination, members=members, filter="data")


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


def table_signature(connection, name, columns):
    fields = ", ".join(map(identifier, columns))
    order = ", ".join(str(index) for index in range(1, len(columns) + 1))
    digest = sha256()
    count = 0
    for row in connection.execute(f"SELECT {fields} FROM {identifier(name)} ORDER BY {order}"):
        digest.update(repr(row).encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return count, digest.hexdigest()


def inspect_database(database, prior=None):
    with closing(sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)) as connection:
        if connection.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
            raise RehearsalError("A restored database failed SQLite quick_check.")
        tables = {
            name: tuple(column[1] for column in connection.execute(f"PRAGMA table_info({identifier(name)})"))
            for name, in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        signatures = {}
        for name, columns in (tables if prior is None else prior).items():
            if name not in tables or not all(column in tables[name] for column in columns):
                raise RehearsalError("A migration removed a table or column from the backup.")
            signatures[name] = table_signature(connection, name, columns)
        version = connection.execute("PRAGMA user_version").fetchone()[0]
    return version, tables, signatures


def private_file_signatures(root):
    signatures = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if len(relative.parts) == 1 and relative.name in STORE_FILES:
            continue
        if path.is_file():
            with path.open("rb") as source:
                signatures[relative] = sha256_file(source)
    return signatures


def sha256_file(source):
    digest = sha256()
    for block in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(block)
    return digest.hexdigest()


def inspect_workspaces(root):
    from bill_workspace import read_workspace, resolve_attachment

    with (root / "users.json").open(encoding="utf-8") as source:
        accounts = json.load(source)
    if not isinstance(accounts, dict) or not all(isinstance(account, dict) for account in accounts.values()):
        raise RehearsalError("The restored account store is invalid.")
    summary = {"accounts": len(accounts), "tracked_entries": 0, "bill_notes": 0, "missing_attachments": 0}
    owners = set()
    for username, account in accounts.items():
        workspace_id = account.get("workspace_id")
        if workspace_id is not None and not isinstance(workspace_id, str):
            raise RehearsalError("A restored account has an invalid workspace identifier.")
        owner_root = root / ("workspaces" if workspace_id else "users")
        workspace = (owner_root / (workspace_id or username)).resolve()
        if workspace == owner_root or not workspace.is_relative_to(owner_root):
            raise RehearsalError("A restored workspace path escapes its account directory.")
        if workspace in owners:
            raise RehearsalError("Multiple accounts reference the same workspace.")
        owners.add(workspace)
        database = workspace / ".bill-workspace.sqlite3"
        if database.is_file():
            inspect_database(database)
        tracked = read_workspace(workspace / "tracked_bills.json", [])
        notes = read_workspace(workspace / "bill_notes.json", {})
        if not isinstance(tracked, list) or not isinstance(notes, dict):
            raise RehearsalError("A restored workspace has invalid tracking or notes data.")
        summary["tracked_entries"] += len(tracked)
        summary["bill_notes"] += len(notes)
        for note in notes.values():
            if isinstance(note, dict):
                references = note.get("files") or []
                if not isinstance(references, list):
                    raise RehearsalError("A restored bill note has an invalid attachment list.")
                for reference in references:
                    if resolve_attachment(workspace / "uploads", reference) is None:
                        summary["missing_attachments"] += 1
    return summary


def rehearse(archive):
    sys.path.insert(0, str(ROOT))
    from corpus_manager import CorpusManager
    from job_manager import JobManager
    from staff_manager import StaffManager

    with TemporaryDirectory(prefix="tracker-restore-") as directory:
        destination = Path(directory).resolve()
        extract_backup(archive, destination)
        root = destination / "data"
        if not all((root / name).is_file() for name in (*STORES, "users.json")):
            raise RehearsalError("Backup is missing bills.db, staff.db, jobs.db, or users.json.")

        original_files = private_file_signatures(root)
        before = {name: inspect_database(root / name) for name in STORES}
        versions = {}
        constructors = {
            "bills.db": lambda: CorpusManager(str(root / "bills.db"), ""),
            "staff.db": lambda: StaffManager(str(root / "staff.db")),
            "jobs.db": lambda: JobManager(str(root / "jobs.db")),
        }
        managers = {}
        for name, initialize in constructors.items():
            managers[name] = initialize()
            old_version, columns, signatures = before[name]
            new_version, _, after = inspect_database(root / name, prior=columns)
            if after != signatures:
                raise RehearsalError(f"{name} changed existing records during migration.")
            versions[name] = {"before": old_version, "after": new_version}
        if private_file_signatures(root) != original_files:
            raise RehearsalError("A migration changed workspace files or other private backup content.")

        summary = inspect_workspaces(root)
        summary["historical_sessions"] = len(managers["bills.db"].get_historical_coverage())
        summary["legislator_profiles"] = len(managers["staff.db"].get_all_legislators())
        with closing(sqlite3.connect((root / "jobs.db").as_uri() + "?mode=ro", uri=True)) as connection:
            summary["queued_jobs"] = connection.execute(
                "SELECT count(*) FROM system_jobs WHERE status = 'QUEUED'"
            ).fetchone()[0]
        return {
            "schema_versions": versions,
            "preserved_tables": {name: len(before[name][1]) for name in STORES},
            "preserved_private_files": len(original_files),
            **summary,
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path, help="Completed cold tar backup with data/ at its root.")
    args = parser.parse_args()
    try:
        result = rehearse(args.archive)
    except RehearsalError as error:
        parser.exit(1, f"Restore rehearsal failed: {error}\n")
    except (OSError, sqlite3.Error, tarfile.TarError, ValueError, RuntimeError) as error:
        parser.exit(1, f"Restore rehearsal failed: {type(error).__name__}.\n")
    print(json.dumps(result, indent=2))
    return int(result["missing_attachments"] > 0)


if __name__ == "__main__":
    sys.exit(main())
