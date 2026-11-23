# Technical architecture

The deployed entry is [legiscan_git_sync_update_8_7.py](../legiscan_git_sync_update_8_7.py), a Streamlit process. [tracker_pages.py](../tracker_pages.py) renders the dashboard, hearing calendar, historical coverage and notifications. The main entry still owns core results, profile rendering, filtering, workspace access and admin routing. Older entry points remain source inventory and upgrade references. The [technical map](TECHMAP.md) traces calls and flows, and the [static inventory](techmap.json) preserves source locations, imports and candidate call targets.

| Boundary | Responsibility | Failure behavior to preserve |
| --- | --- | --- |
| Streamlit and auth | Browser session, registered/guest identity, role and account views | Guest storage is temporary. A deleted account or changed credential revision invalidates the affected session. |
| Account and workspace | JSON account store, opaque and legacy paths, SQLite workspace keys, JSON mirrors, uploads | Preserve older accounts and attachments. Isolate write conflicts and recover committed user changes from workspace state. |
| Corpus and scanner | LegiScan dataset and incremental refresh, SQLite research corpus and separate keyword snapshot | Preserve committed partial progress and only advance complete refresh metadata after a successful run. |
| Staff and profiles | Workbook previews, stable identifiers, observed assignment history, vote mapping and sourced service terms | Reject stale previews and keep prior roster snapshots and manual identity decisions. |
| CLI and worker | Job submission, per-job budget, claim, status, recovery and final outcome | Keep queue and execution environment separate from browser state. Retain recoverable job payloads without credentials. |
| Notifications | PostgreSQL preferences, observed baselines, durable outbox and Resend calls | Verification and additional alerts require opt-in. Delivery can retry with an idempotency key and rechecks eligibility. |
| Git and official integrations | Explicit public snapshot allowlist, Congress.gov term lookups and spreadsheet export | Preserve private data boundaries and show upstream incompleteness where available. |

[requirements.txt](../requirements.txt) declares dependencies and [requirements.lock](../requirements.lock) pins the Python 3.12 environment with hashes. [installation.json](installation.json) maps processes, credentials and dependencies. The baseline VPS ran a single systemd Streamlit service with a private persistent filesystem. Multiple web processes require explicit account and database coordination review. Production email was unconfigured at the observed checkpoint. CI run evidence is recorded in release acceptance.

## Corpus connection ownership on improved-exp

The Streamlit resource can cache a CorpusManager across users with the same configured key. Each operation borrows a connection owned by its own thread. Nested operations reuse that connection until the outer scope exits, when it closes. Transactions keep their existing explicit commit boundaries. Uncommitted work is rolled back when a scope closes. Schema additions are performed under a write transaction and WAL is set during initialization. The incremental refresh batch keeps its separate write connection. API request accounting is stored per thread.

Corpus schema setup now checks SQLite user_version before running any DDL. The initial migration takes an immediate write transaction after enabling WAL and rereads the version after acquiring the write lock. It preserves incumbent rows and the legacy sync_meta marker. Startup rejects schema versions outside the versions understood by the release.

The staff, job and corpus databases now share the sqlite_schema.py transaction runner. The version check is repeated after taking the SQLite write lock. If validation or DDL fails, the transaction unwinds and the old user_version remains. Each database keeps its own migration list. Staff initialization now surfaces failures instead of returning a manager against an incomplete roster schema.
