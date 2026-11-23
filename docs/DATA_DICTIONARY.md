# Data dictionary

For table-to-code relationships see [technical map](TECHMAP.md). This dictionary records current compatibility boundaries and core entities.

| Entity | Key and relationship | Field meaning and provenance |
| --- | --- | --- |
| Account | Username in `users.json`, opaque workspace ID for new accounts | Role, bcrypt hash, optional personal LegiScan key and credential revision. Older records can lack workspace ID and revision. |
| Bill workspace | Workspace identifier and per-file record name | Watchlist by LegiScan bill ID, notes, review and decision state, topics, views and labels. JSON mirrors support recovery. Attachment paths resolve within the owning workspace. |
| Session | LegiScan `session_id`, jurisdiction code | Source session title and years can be unavailable. Stored import/refresh times reflect successful application ingestion. Historical import is operator-selected. |
| Bill | LegiScan `bill_id`, optional session relationship | Number, title, sponsor, summaries, action and status come from LegiScan. Bill numbers repeat across sessions. Unknown and missing stages remain distinct from known numeric codes. |
| Document | LegiScan document ID and bill relationship | Version label, media and link depend on the source. A text reference can exist before fetch. HTML comparison is a textual view rather than authoritative formatting. |
| Roll call and vote | LegiScan roll-call ID and provider person ID | Published vote result can exist without individual votes. An absent vote row does not imply a vote against or an abstention. |
| Local roster and profiles | Persistent local legislator/staff IDs, explicit mapping to provider identity | Source workbook observations, assignment history and admin review. Sourced dates carry their published precision. |
| Calendar snapshot | LegiScan bill ID, per-bill event fingerprint | Current published event set and fetch timestamp. Unknown preserves previous known snapshot. Explicit empty can clear it. Reported times can lack source timezone. |
| Job | Random `job_id`, optional retry predecessor | Operation, initiator, scope, attempt, state, heartbeat and outcome. Shared durable store coordinates worker and CLI. |
| Notification | Account owner, tracked bill ID, outbox event ID | PostgreSQL records verified address, activity categories, baselines and delivery state. Resend acceptance is distinct from delivery. |

Migration rules use stable IDs, preserve nullable/unknown records and do not infer absent source evidence. Preserve pre-change backups, validate older fixtures, and document compatibility for every structural change. Phase 1 uses a separate SQLite lock file beside the JSON account store. It carries no account records. Stop all old-version account writers during an upgrade because those processes do not acquire the new lock.

The local login attempt database next to the account store contains temporary counters keyed by a digest of the normalized known account name, plus a shared unknown-account bucket. It contains no passwords and does not determine account permissions.

SQLite user_version 0 represents an unversioned bill corpus, including installations whose sync_meta schema_version text is already 1. Version 1 records completion of the baseline corpus schema transaction. A normal restart reads the version and skips schema setup. This version applies only to bills.db. Staff, job, workspace, and PostgreSQL schemas retain their existing conventions.
