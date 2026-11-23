# Operations runbook

[README](../README.md) contains commands for installation, APIs, hosted startup, backups and recovery. [Installation manifest](installation.json) captures processes, flags and independent credentials for automation.

## Release procedure

1. Fetch and verify the target commit. Build side by side on Python 3.12 from the hash-locked dependencies, using the same service account permissions as production.
2. Validate dependency resolution and run regression and loopback smoke tests on temporary data. Preserve the existing service configuration, private integration settings, active release, and known good health URL.
3. Identify cron and service writers. Quiesce them for a filesystem backup. Back up the private data directory including hidden files, SQLite journals and user uploads. Back up PostgreSQL separately when configured. Inspect the archive and rehearse restore to an isolated target.
4. Run required migrations with app and writer coordination. For email schema, invoke `python cli.py --task notification-setup` with the app's notification environment. Schedule the notification runner separately and verify with a controlled mailbox before inviting opt-in.
5. Switch the active service, preserve its bind and proxy configuration, check its working directory/commit and health, then exercise authenticated production workflows. Restore scheduled operations only after agreeing which release and persistent paths they target.
6. On failure, revert the startup override and restart the previous release. Restore data only after investigating compatibility and protecting newer data. Coordinate app and PostgreSQL rollback if a future schema change requires it.

## Operational checks

Check service health, active release, log errors, LegiScan request quota, stale jobs, corpus freshness, queue age, and staff preview count changes. Email needs PostgreSQL connectivity, verified Resend sender, completed initialization, active preferences, and a scheduled sender. Keep secrets in protected process configuration and keep private backups independent from Git publication.

The audited VPS had a healthy `221f279` checkout and the legacy staff cron temporarily paused. PostgreSQL/Resend were unconfigured at the last observed checkpoint. The account store, staff and historical records remain on durable filesystem paths. Exact credential values and private backup inventory belong to the operator environment.

## Phase 1 compatibility

Stop old writers before switching releases to activate cross-process serialization. The service account needs write access to the account directory for the lock file and atomic replacement. Account JSON shape, existing hashes, and notification preferences are preserved. New notification address submissions accept ASCII mailbox syntax and ASCII IDN domains in punycode, with carriage returns and line feeds rejected before normalization. Original bill documents remain linked for formatting and metadata omitted by the safe interactive reader.

## Bill corpus schema version

The first app or worker start with this release upgrades a fresh or legacy unversioned bills.db to SQLite user_version 1. Take a private backup of DATA_DIR before upgrading and stop old writers during cutover. The upgrade creates missing baseline tables and indexes, adds the existing document reference columns when absent, and records the version in the same transaction as those changes. The historical sync_meta marker is preserved for old code. Conversion can set WAL mode before the schema transaction. A failed schema change rolls back its DDL and records no completed version. A higher version is refused without changes. Do not edit SQLite's schema version by hand. After upgrade, check Historical Coverage and confirm a known session's vote and text counts.

## Staff and job schema versions

The staff and job databases now adopt an unversioned file as SQLite user_version 1 on first use. The application and worker use the same version check and serialized migration routine as the corpus. Stop the app, worker and cron writers before a cold backup and release switch. Preserve staff.db and jobs.db in that backup, then check a known staff profile and roster snapshot, and inspect queued jobs before starting a worker. The upgrade preserves existing positions, notes stored outside the roster, snapshots, job payloads and status. Existing retry rules still apply when the worker starts. The previously paused legacy staff cron is a separate operational decision.
