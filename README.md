# UFtW Bill Tracker

Find California and federal bills, organize a private watchlist, follow legislative changes, and research earlier sessions and policymakers.

- [User guide](#user-guide)
- [Local installation](#local-installation)
- [APIs, credentials, and configuration](#apis-credentials-and-configuration)
- [PostgreSQL and Resend setup](#postgresql-and-resend-setup)
- [Hosted installation](#hosted-installation)
- [Administration and scheduling](#administration-and-scheduling)
- [Backup, upgrades, and recovery](#backup-upgrades-and-recovery)
- [Troubleshooting and verification](#troubleshooting-and-verification)
- [Technical map](docs/TECHMAP.md) · [Machine-readable installation guide](docs/installation.json) · [Machine-readable call and symbol map](docs/techmap.json)
- [Development roadmap](docs/DEVELOPMENT_ROADMAP.md) · [Audit findings](docs/AUDIT_FINDINGS.md) · [Release acceptance](docs/RELEASE_ACCEPTANCE.md)

## User guide

### Accounts and navigation

Open the tracker URL and **Register** or **Sign In**. A registered account keeps a private workspace with watchlists, reviews, notes, topics and saved views. Guest mode uses temporary storage. The first registered account in a fresh installation becomes administrator. In the sidebar, use **Account Settings** to change your password or enter a personal LegiScan key for interactive lookups. Only enter a personal key on a server you trust. Scheduled jobs and the worker use the operator's server key. Select a workflow under **View Mode**. Open **Saved Views** to load a preset or save your filters with a name you will recognize.

### Start from the dashboard

- **Find bills** starts a fresh search across California and Congress, or within the jurisdiction you choose. Leave the search box blank to browse. In **All Bills**, narrow results by legislative session and filters.
- **At a glance** shows the size of your watchlist and saved topics, the indexed bill totals by jurisdiction, and when each current session last synced. **Happening soon** shows published events from your tracked bills over the next two weeks, plus your overdue and upcoming follow-ups. Dates use Pacific time. Select an event or follow-up to open its bill.
- Use the top shortcuts to open **Tracked Bills**, **Hearing Calendar**, and **Topic Inbox**. **Explore the workspace** links to historical coverage, people, votes, and other research tools. The **Dashboard** breadcrumb brings you back from any workspace view.
- Administrators can open **Refresh bills** and choose California, Congress, or both. It imports the current session the first time and checks for changes on later runs. If a worker is configured, use **Check refresh status** for the queued jobs. Older sessions are imported manually in **Admin & Database Tools**.

### Find and classify legislation

1. Select **All Bills** and choose a jurisdiction and session. Use **Global Search** for a bill number, title words or sponsor, then refine the available filters. A bill number can recur across sessions. Clear prior filters when results appear incomplete.
2. Open the bill's details. Inspect status, action history, sponsors, committees, document versions and any available roll calls. Refer to official links for deadlines and legal text. A recently introduced bill may appear after an administrator refreshes the corpus.
3. Open **Keyword Matches** to see the shared keyword scan results. This view has its own scan schedule. For private monitoring, create a **Topic Inbox** topic with alternative phrases, any required phrases and excluded terms. Review the match evidence from indexed titles, summaries and subjects. Mark loaded results reviewed after triage.
4. Track useful bills, then open **Tracked Bills → View Details & Edit Notes** to set personal topics, position, priority and follow-up dates. Add research notes or supporting files where available. Keep unrelated subjects in separate topics and reuse a saved view to return to a specific working set. Use quick filters such as **Needs Attention** for triage.
5. Select **Mark reviewed** to save a baseline independently of editing notes. Use **Changes Since Review** to find new statuses, actions, committees or versions. Open **Decision history** for earlier positions, priorities, follow-ups and topic decisions, or download the available JSON record. Older label entries can appear under **Legacy entries to review**.
6. Use **Compare tracked bills** to compare up to four records. Open **Compare amendments** within an individual bill to compare two versions. The amendment diff compares visible HTML text and can omit formatting or unsupported document content. Open the source documents for authoritative wording.
7. Enable **Daily Review** for follow-ups, changed tracked bills, unseen topic matches and upcoming events. Work through the queue or snooze items for one, three, or seven days. Disabling the view preserves review history.

Example. Build a housing portfolio by creating a private topic with phrases like “affordable housing” and “housing element.” Choose the current California session. Review match evidence, track relevant bills, apply your own topic tags and priority, then save a high-priority view and use follow-up dates for deadlines.

### Suggested civic topics

Results show **Civic tags** for shared suggestions and **My tags** for private labels from your rules and notes. On a bill card, **Browse** opens the corresponding shared tag in **Civic Tags**. Expand the details to see the phrase and source field behind each suggestion, or select **Draft a private rule** to review that phrase in Topic Inbox before saving. Suggested labels use a small shared dictionary and phrases learned from the corpus. A mention is evidence for a suggested label. It does not establish that the bill changes policy in that area.

In **Topic Inbox**, choose **Tag matching bills**, **Track matches in Daily Review**, or both. Personal tags apply to matching records in your workspace, including historical bills. Editing the phrase rules reopens existing matches for review. An individual note's **My topics** remains available for labels you apply by hand.

Successful corpus imports and refreshes rebuild a small derived civic index for California and federal bills. The rebuild can promote a phrase if it appears in the title or subjects of at least three bills, has a civic vocabulary anchor, and is not widespread boilerplate or already covered by the dictionary. The automatic vocabulary is capped at 32 learned phrases. Every rebuild revisits previously indexed bills, including bills that already had tags. To run the first backfill or refresh the index after editing the dictionary, run:

    python cli.py --task civic-tags

This runs locally on the existing bill corpus and stores only learned labels, tagged bill IDs, and match evidence in the derived civic index.

The dictionary lives in [civic_topics.json](civic_topics.json). It uses specific phrases for subjects such as drinking water, tenant protection, and voter registration. Common words and legislative boilerplate are filtered when finding possible new phrases. After changing the dictionary, restart the app to load the new rules.

To discover recurring phrases in a known California session, run the following against an already indexed corpus. Substitute the numeric session ID. For federal research use --jurisdiction US. The report counts each phrase once per bill, gives preference to phrases found in titles or subjects, and excludes terms that appear in more than 40 percent of the selected bills by default. Review the candidates before adding them to the dictionary.

    python tools/suggest_civic_terms.py /path/to/bills.db --jurisdiction CA --session-id SESSION_ID

Use --min-bills and --max-share to tune the threshold for the selected corpus. The command reads the existing database and prints JSON. It does not need an API key, an additional database, or an external model. The general method is document-frequency filtering. [Document frequency and n-gram reference](https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.TfidfVectorizer.html)

### Follow hearings and receive email

**Hearing Calendar** displays published events for watched bills with source snapshot times. Use **Refresh tracked-bill calendars** when available to backfill missing snapshots using an admin account or personal LegiScan key. The refresh reports source failures and request limits separately from missing schedule data. Prior snapshots remain available when a check fails. Export calendar markers as iCalendar. The feed does not provide reliable timezones, so exported events are date-only. When the source lists a time it can appear in the description. Confirm attendance details with the Legislature.

Go to **Email Notifications** while signed in. Enter an address, request a verification code, enter it within 20 minutes, and enable alerts. A ten-minute resend throttle can apply. Pause or resume alerts from the same page. The first delivery pass establishes status baselines. Later passes send status changes observed on tracked bills. If several changes occur between passes, an alert can reflect the latest one. Removing a bill from your watchlist makes it ineligible for later delivery when the sender checks it. Use **Additional alerts** to opt into hearing calendar changes, newly indexed bill text versions, or committee changes. The first observed value establishes a baseline for each enabled category. Hearing alerts cover upcoming additions and removals from published snapshots. Treat disappeared calendar entries as a prompt to confirm with the official source. Use bill details and Daily Review for other changes.

### Research historical records and policymakers

1. Open **Historical Coverage** to see imported California sessions and Congresses, import and refresh timestamps, bills with text references, and roll calls with individual votes. A text reference can point to a document that is yet to be cached. Source coverage can vary.
2. Ask an administrator to manually import a missing earlier session. Once imported, search that session in **All Bills**. Bills imported into the corpus use the same LegiScan bill IDs as existing tracked or keyword records.
3. In **Legislator Directory**, open a member or staff profile and inspect staff, issue assignments, bills, committees and vote evidence as available. Committees shown on voted bills and official committee membership have different sources. Check coverage and the source roll call before making claims about a member's positions.
4. In **Federal Voting Profiles**, select an indexed voter and filter by sourced terms where available. Imported Congress.gov terms preserve year precision. Exact service dates require an official citation. **Staff Analytics** provides additional roster and observed-assignment views. Assignment observations reflect when the app saw a change.
5. Save private profile notes in the profile editor. Use the older-note reconnection controls when available to attach legacy notes to the correct staffer or legislator.

## Local installation

### Prerequisites and dependencies

- Authorized access to the private GitHub repository and Git, or an authorized copy of the source.
- Python **3.12**, matching the CI configuration, and a writable Python virtual environment. A browser connects to Streamlit. No Node build is required.
- A durable writable local data directory. Use a filesystem with SQLite locking support.
- LegiScan credentials for fresh bill ingestion. PostgreSQL and Resend are optional for email. Congress.gov is optional for federal term sourcing. A downloadable or uploaded spreadsheet is optional for the staff directory.
- [requirements.txt](requirements.txt) lists Streamlit, pandas, Requests, bcrypt, openpyxl, python-dateutil, and Psycopg with its binary extra. Its minimum package versions are resolver constraints. Use a current compatible set on Python 3.12 and record resolved versions after validation. [Python downloads](https://www.python.org/downloads/) · [Git downloads](https://git-scm.com/downloads) · [Streamlit installation](https://docs.streamlit.io/get-started/installation/command-line) · [Psycopg installation](https://www.psycopg.org/psycopg3/docs/basic/install.html)

### Windows PowerShell

Install Python 3.12 and Git for Windows, then open a new PowerShell window. These commands use the virtual environment directly, so activation is optional. If the Python launcher is absent, substitute your Python 3.12 executable for `py -3.12`.

```powershell
git clone https://github.com/mattcallaway/legiscan_private.git
Set-Location legiscan_private
py -3.12 -m venv .venv
$trackerPython = Join-Path (Get-Location) '.venv\Scripts\python.exe'
& $trackerPython -m pip install --upgrade pip
& $trackerPython -m pip install -r requirements.txt
$env:DATA_DIR = Join-Path $env:LOCALAPPDATA 'UFtWTracker\data'
$env:REPO_DIR = Join-Path $env:LOCALAPPDATA 'UFtWTracker\publication'
& $trackerPython -m streamlit run legiscan_git_sync_update_8_7.py --server.address 127.0.0.1
```

### macOS and Linux

On macOS install Python 3.12 using Python.org or your package manager, and Git using the command line developer tools or your package manager. On Ubuntu/Debian install Git and venv support for your chosen Python interpreter. On Fedora or other Linux distributions install Git and the appropriate Python 3.12 packages. Interpreter and package names vary by release. Confirm the interpreter version and `venv` availability before continuing.

```sh
git clone https://github.com/mattcallaway/legiscan_private.git
cd legiscan_private
python3.12 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
export DATA_DIR="$HOME/uftw-tracker/data"
export REPO_DIR="$HOME/uftw-tracker/publication"
umask 077
mkdir -p "$DATA_DIR" "$REPO_DIR"
.venv/bin/python -m streamlit run legiscan_git_sync_update_8_7.py --server.address 127.0.0.1
```

Open [the local app](http://localhost:8501) unless your terminal prints a different port. Use the environment settings below before launching, or restart after changing credentials. Do not rely on the repository's Windows-style default paths when running on macOS, Linux or a hosted service.


## APIs, credentials, and configuration

[config.py](config.py) reads [config.json](config.json) next to the source, expands its path defaults and prefers environment overrides. Supply credentials as environment variables on each relevant process. On Windows use `$env:NAME = 'value'`. On macOS or Linux use `export NAME='value'`. For unattended tasks, use the host's secrets and environment settings. Child processes inherit environment variables. Keep values out of the checkout, screenshots and logs. [Streamlit secret deployment guidance](https://docs.streamlit.io/deploy/concepts/secrets)

| Setting | Process | Purpose |
| --- | --- | --- |
| `DATA_DIR` | App, worker, CLI, sender | Absolute, private, writable, persistent directory. Use the same path for every process on an instance. |
| `REPO_DIR` | App and Git publishing | A separate plain directory is sufficient for private operation. Use an existing normal checkout only for intentional publication. |
| `TRACKER_REGISTRATION_MODE` | App | `open` (default), `admin` (admin user management only), or `disabled` (operator provisioning only). |
| `API_KEY` | App and ingestion processes | Server LegiScan API key. Required for current bill ingestion and shared keyword scanning. |
| `CONGRESS_API_KEY` | App | Optional Congress.gov key for federal term lookup. |
| `LEGISCAN_BACKGROUND_JOBS` | App | Optional `1`, `true`, or `yes` queues supported jobs for a separately started worker. |
| `LEGISCAN_JOB_REQUEST_LIMIT` | Ingestion execution | Optional positive integer number of LegiScan requests per ingestion job. Monitor provider-period quotas separately. |
| `NOTIFICATION_DATABASE_URL` | App, email schema setup and sender | Optional PostgreSQL DSN for notifications. |
| `RESEND_API_KEY` | App, email schema setup and sender | Optional sending key for notifications. |
| `RESEND_FROM_EMAIL` | App, email schema setup and sender | Optional verified sender such as `Tracker <alerts@example.org>`. |
| `TEST_NOTIFICATION_DATABASE_URL` | Tests only | DSN for a disposable PostgreSQL database. |

### Account enrollment and login

Set `TRACKER_REGISTRATION_MODE=admin` on a hosted instance to close public signup while allowing signed-in administrators to create regular accounts. Use `disabled` to close all browser account creation. With either setting, an authorized local operator can bootstrap a fresh data directory through `auth.register_user` under the service identity. If unset, `open` preserves public signup and first-account administrator behavior. An unrecognized value disables browser account creation. Existing account login and guest browsing work in every mode.

Sign-in allows eight password attempts per known username in a five-minute window. A successful login clears the counter. Unknown usernames share a separate budget. This protects the password verification path; a public deployment still benefits from proxy-level request limits. The account directory must be writable by the service because it stores the SQLite attempt counters next to `users.json`.

### LegiScan

Obtain a key through LegiScan and confirm quota and bulk dataset access. Set `API_KEY` for the app and ingestion processes. California uses jurisdiction code `CA`, and U.S. Congress uses `US`. Both can use the same server credential when the account supplies the datasets. Open **System Status** and perform the relevant jurisdiction's initial bulk import. [LegiScan API access](https://legiscan.com/legiscan) · [LegiScan API manual](https://api.legiscan.com/dl/LegiScan_API_User_Manual.pdf)

### Congress.gov

Request a Congress.gov API key and set `CONGRESS_API_KEY` for Streamlit. As an admin, choose a voter in **Federal Voting Profiles**, expand **Link official Congress.gov terms**, find the official Bioguide ID, verify that it matches the voter, then preview and apply terms. The data source supplies year precision. Record exact dates separately when officially sourced. LegiScan supplies the federal bill and vote records. [Congress.gov API](https://api.congress.gov/) · [Member endpoint](https://github.com/LibraryOfCongress/api.congress.gov/blob/main/Documentation/MemberEndpoint.md)

### Google Sheets and Git

The built-in Capitol Matrix source uses a direct Google Sheets XLSX export. For a restricted workbook, download it through your authorized account and upload the file in **Staff Intelligence**. The app has no Google OAuth setup. See [staff import](#staff-directory-import-and-vote-identities).

For optional publication, configure `REPO_DIR` as a normal existing Git checkout with a configured upstream and authenticated write access under the app user. Git author identity is also required. The publisher allowlists the shared `LegiScan_Enhanced_Full_Tracker.csv` and `keywords.json`, and these resolved paths must be inside that checkout. If your deployment uses this arrangement, private data can also be physically present inside the checkout. Keep it untracked and avoid blanket Git staging. Review the keywords and CSV for sensitive content before intentionally pushing. For fully private operation, keep an independent `DATA_DIR` and point `REPO_DIR` at a plain directory; use protected storage backups. [Publisher implementation](sync_github_repo.py)

## PostgreSQL and Resend setup

1. Install PostgreSQL on Windows, macOS or Linux, or provision a managed instance. The project CI uses PostgreSQL 16. Ensure the app and sender can reach the host. Use loopback for a local instance and the hosting provider's network controls and TLS configuration for remote service. [PostgreSQL platform downloads](https://www.postgresql.org/download/)
2. Create a dedicated database and login. The schema setup role must be able to create tables and indexes. An isolated database owned by this role works for a single instance. If you separate runtime privileges, grant schema access and the required table and sequence privileges to the role used by the app and runner.

   ```sql
   CREATE ROLE tracker_notifications LOGIN;
   \password tracker_notifications
   CREATE DATABASE tracker_notifications OWNER tracker_notifications;
   ```

   Run those commands in an administrator's interactive `psql` session. The password command prompts for the value. Use a provider-supplied DSN or a PostgreSQL URI such as `postgresql://tracker_notifications:PERCENT_ENCODED_PASSWORD@localhost:5432/tracker_notifications`. Encode reserved password characters for URI syntax.

3. In Resend, add a domain or sending subdomain you control. Publish the DNS records Resend specifies and wait for verification. Create a sending key scoped to the verified domain where supported. Choose the sender address on that domain and a test recipient mailbox you control. [Resend domain setup](https://resend.com/docs/dashboard/domains/introduction) · [Resend API keys](https://resend.com/docs/dashboard/api-keys/introduction)
4. Set `NOTIFICATION_DATABASE_URL`, `RESEND_API_KEY`, and `RESEND_FROM_EMAIL` on the app and sender. Give the sender the same persistent `DATA_DIR` as the app. Run schema initialization from the source checkout using the environment's Python. Run it again during upgrades that add notification categories, before restarting the app and sender.

   ```powershell
   $env:NOTIFICATION_DATABASE_URL = 'postgresql://tracker_notifications:PERCENT_ENCODED_PASSWORD@localhost:5432/tracker_notifications'
   $env:RESEND_API_KEY = 'REPLACE_FROM_SECRET_STORE'
   $env:RESEND_FROM_EMAIL = 'Tracker <alerts@example.org>'
   & $trackerPython cli.py --task notification-setup
   ```

   ```sh
   export NOTIFICATION_DATABASE_URL='postgresql://tracker_notifications:PERCENT_ENCODED_PASSWORD@localhost:5432/tracker_notifications'
   export RESEND_API_KEY='REPLACE_FROM_SECRET_STORE'
   export RESEND_FROM_EMAIL='Tracker <alerts@example.org>'
   .venv/bin/python cli.py --task notification-setup
   ```

5. Restart the app. Sign in, track a bill, and enable **Email Notifications** through an address you control. Confirm code delivery and verification. Run `python cli.py --task notifications` once to record baseline status, then run again after a successful bill refresh observes a real change. Use mocked mail and a disposable DB for synthetic status changes.
6. Schedule sender passes regularly. Refresh first when practical, and allow independent passes for delivery retries. The CLI logs acceptance by Resend. Use Resend activity to inspect bounce or delivery status. [Resend Send Email API](https://resend.com/docs/api-reference/emails/send-email) · [Resend idempotency](https://resend.com/docs/dashboard/emails/idempotency-keys)

PostgreSQL holds preferences, baselines and delivery records. Accounts, bill corpus, staff, jobs, notes and uploads still use persistent filesystem storage. The implementation sends via the Resend REST API. It requires neither SMTP settings nor a webhook.

## Hosted installation

### Managed host or container platform

1. Pick a service supporting a private source build, Python 3.12, a persistent filesystem, HTTPS and WebSockets. Run one app instance on that storage. Use a scheduler or a separate worker only if it can access the same persistent mount and environment.
2. Build from the repository root with `python -m pip install -r requirements.txt`. Set the root as the working directory. Mount a private durable volume, for example `/var/lib/uftw-tracker/data`. Set `DATA_DIR` to the mount and configure `REPO_DIR` separately. Ensure the service account can read and write its directories.
3. Load integration secrets as process environment values. Select a start command using the host's required port. This example uses POSIX shell expansion. For a launcher that runs without a shell, specify the concrete port or the host's interpolation syntax.

   ```sh
   python -m streamlit run legiscan_git_sync_update_8_7.py --server.address 0.0.0.0 --server.port "${PORT:-8501}" --server.headless true
   ```

4. Use the host's TLS and WebSocket proxy. Restrict access during initialization so the intended first administrator can register. Sign in through the public HTTPS address, enable the desired workflows, and test persistence across a restart. A probe at `/_stcore/health` can check liveness after you verify the path on the installed Streamlit version. Test real sign-in separately.
5. Deploy the scheduler and any worker with the same persistent path and appropriate environment. Confirm both their permissions and restart behavior. If containerizing, bind-mount the same persistent location into replacement containers. Keep hosted credentials out of image layers. [Streamlit deployment concepts](https://docs.streamlit.io/deploy/concepts) · [Streamlit WebSocket architecture](https://docs.streamlit.io/develop/concepts/architecture/architecture)

### Self-managed Linux

Install under a dedicated non-root account such as `tracker`. Clone to `/opt/uftw-tracker`, create its `.venv` and install dependencies using the Linux steps. Create the data and publication directories outside the checkout, restrict them to the service account, and place the app's environment values in a protected root-owned `/etc/uftw-tracker.env`. Example paths are `DATA_DIR=/var/lib/uftw-tracker/data` and `REPO_DIR=/var/lib/uftw-tracker/publication`.

Save this as `/etc/systemd/system/uftw-tracker.service` and adapt paths and user names before enabling it.

```ini
[Unit]
Description=UFtW Bill Tracker
Wants=network-online.target
After=network-online.target

[Service]
User=tracker
Group=tracker
WorkingDirectory=/opt/uftw-tracker
EnvironmentFile=/etc/uftw-tracker.env
UMask=0077
ExecStart=/opt/uftw-tracker/.venv/bin/python -m streamlit run legiscan_git_sync_update_8_7.py --server.address 127.0.0.1 --server.port 8501 --server.headless true
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Run `sudo systemctl daemon-reload`, `sudo systemctl enable --now uftw-tracker.service`, and inspect `sudo systemctl status uftw-tracker.service` and `sudo journalctl -u uftw-tracker.service -n 100`. Route HTTPS from your reverse proxy to loopback port 8501 and forward WebSocket upgrades. For a continuous worker use a second unit with `ExecStart=/opt/uftw-tracker/.venv/bin/python cli.py --task worker`. For scheduled CLI commands use one-shot units and timers with the same working directory and protected environment. [Streamlit reverse proxy guidance](https://docs.streamlit.io/knowledge-base/deploy/deploy-streamlit-domain-port-80)

The existing Policy Trackr VPS uses `/srv/policy-trackr`, `/srv/policy-trackr/data`, and `streamlit.service`. Use [the VPS release installer](tools/deploy_tracker_vps.sh) as root with a tested `improved-exp` commit. It installs the locked dependencies into a separate checkout, checks a loopback instance with temporary data, and restarts the live service using its existing environment. Keep the printed rollback directory. Running the same installer with `--rollback` and that directory restores the prior release configuration.

### Self-managed Windows or macOS

On Windows, use a dedicated account, durable NTFS directory, and the local Windows setup above. To schedule CLI runs, use Task Scheduler. Set the virtual environment's absolute Python executable as **Program/script**, the CLI arguments as **Add arguments** and the source directory as **Start in**. Configure the assigned identity and use a protected environment loader available to the unattended process. For the web process use a service wrapper or boot task with restart handling.

On macOS, use absolute paths and an explicit account in launchd or a managed process runner. Set the working directory and program arguments, and use an interval-triggered job for recurring CLI activity. Launchd and Windows scheduled jobs need their own environment configuration. Check permissions and schedules under the actual service identity. For serving remote users on either platform, place the loopback-bound app behind your HTTPS and WebSocket proxy.


## Administration and scheduling

### Corpus and historical sessions

1. Sign in as an admin and open **Admin & Database Tools → Master Corpus Tasks**.
2. Select **California Legislature**, **U.S. Congress**, or both and click **Sync selected jurisdictions**. The app discovers each current session and downloads its bulk dataset for the initial import. Following syncs refresh changed bills and retry incomplete vote coverage. Check the jobs and coverage screens after each initial load.
3. Open **Manual session import and refresh** for an older session. Choose the intended session, import it, and verify its coverage. Earlier sessions are intentionally manual. If you want an earlier session refreshed again later, target that session explicitly.
4. The UI and current-session worker route stop if the bulk dataset is unavailable. Direct CLI `bootstrap` can fall back to per-bill calls, which may be expensive. Use a deliberate request limit if using that path.
5. Incomplete refreshes can keep successfully committed bill data while leaving the last-successful timestamp in place. Inspect the job outcome and follow up on vote or source errors.

### Staff directory import and vote identities

1. Open **Staff Intelligence** in the admin tools. Preview the live Capitol Matrix source or upload an authorized `.xlsx`. The expected tabs are `Assembly`, `Senate`, `Asm Issues`, `Sen Issues`, `Asm Cmte Staff`, and `Sen Cmte Staff`.
2. Compare before and after counts, and review warnings, missing sections and unmatched rows before confirming. Applying a changed roster saves a prior private snapshot in `staff.db`. **Restore a roster snapshot** provides a separate preview before confirmation.
3. Verify a representative legislator, staff member, assignment and committee after the import. Open **Review vote identities** to verify or exclude unmatched records. Check official identity, jurisdiction, chamber and district before choosing a mapping. Check the coverage view after reviewing identities.
4. In **Legislator Directory**, admins can record exact service dates with an official citation under **Sourced service terms**. Staff roster imports preserve these sourced records. [California Senate research guide](https://www.senate.ca.gov/citizens-guide/faqs)
5. The legacy `sync_staff_directory.py` applies the live roster and attempts people mapping. It has a fixed Linux log directory and live source URL. Review that behavior before unattended use. The app's preview workflow is the normal reviewed import.

### CLI reference

Use the installed environment's Python from the checkout. Substitute the numeric session ID. A scheduler should use an absolute interpreter path and an explicit working directory.

| Command | Use |
| --- | --- |
| `python cli.py --help` | List accepted flags. |
| `python cli.py --task refresh --jurisdiction CA --session-id SESSION_ID` | Refresh a California session. |
| `python cli.py --task refresh --jurisdiction US --session-id SESSION_ID` | Refresh a federal session. |
| `python cli.py --task bootstrap --jurisdiction CA --session-id SESSION_ID` | Bootstrap with possible per-bill fallback. |
| `python cli.py --task rescan --states CA,US` | Refresh the shared keyword snapshot. The default is CA when omitted. |
| `python cli.py --task worker` | Run the queue worker continuously. |
| `python cli.py --task worker --once` | Attempt at most one queued job and exit. |
| `python cli.py --task civic-tags` | Rebuild shared civic tags from indexed California and federal bills. |
| `python cli.py --task notification-setup` | Initialize PostgreSQL notification tables. |
| `python cli.py --task notifications` | Observe statuses and attempt eligible email delivery once. |

Schedule incremental refreshes with a cadence that fits quota and the upstream update schedule. Run the notification sender after refreshes and regularly enough for retries. Schedule keyword rescans separately. Check job exit status and **Recent Jobs**. With `LEGISCAN_BACKGROUND_JOBS=1` the UI queues supported operations. A worker takes jobs from the same `jobs.db`. Use **Refresh job status** and **Retry sync** when offered. A worker can retry an expired claim once. Review configuration at the beginning of each new legislative session or Congress.

## Backup, upgrades, and recovery

### Back up the complete installation state

- Protect the entire private `DATA_DIR`, including hidden files and nested directories. It holds `bills.db`, `staff.db`, `jobs.db`, the account store, shared keyword data, personal watchlists, notes, private workspace SQLite files, JSON recovery copies and uploads. Use a consistent filesystem snapshot or stop all writers before copying. Preserve SQLite journal sidecars when they belong to the captured state.
- Back up the PostgreSQL database separately when alerts are enabled. Example with a compatible client and secure client credentials already configured: `pg_dump --dbname=tracker_notifications --format=custom --file=notifications.dump`. Inspect with `pg_restore --list notifications.dump` and rehearse into a separate database. Use the provider's host and TLS options. [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) · [pg_restore](https://www.postgresql.org/docs/current/app-pgrestore.html)
- Record the application commit, resolved Python packages, host service configuration, and references to secrets. Restrict access to all backup media. The files can contain password hashes, personal API keys, email addresses, unpublished notes and private attachments.

### Rehearse restore and deploy an upgrade

1. Stop app and worker writers and pause scheduled commands before a cold backup. Record the active release and preserve both storage sets.
2. Restore into an isolated filesystem path and separate PostgreSQL database. Keep worker and scheduler stopped during inspection. Omit notification settings on the rehearsal app unless deliberately testing with an isolated sender and controlled mailbox.
3. Check account login, a watchlist, a bill note, an attachment download, a staff profile and **Historical Coverage**. Inspect restored job and outbox records before enabling delivery.
4. Install upgraded dependencies in a separate virtual environment. Run the regression checks and rehearse startup on copies. Confirm paths, owner, proxy and schedule before returning to normal operation.
5. If rolling back, coordinate the previous code version and compatible backups. A pre-upgrade snapshot can omit user edits and other writes made since the backup.

## Troubleshooting and verification

| Symptom | Check |
| --- | --- |
| App fails to start | Confirm Python version, dependency installation, working directory, entry script, and access to config and data directories. |
| Hosted page repeatedly reconnects | Check WebSocket forwarding, HTTPS origin, reverse proxy timeout and the actual persistent mount. |
| Empty bill search | Clear filters, verify jurisdiction and session, then check Historical Coverage and the latest job. |
| Old keyword results | Check the last keyword rescan independently of the corpus refresh. |
| No federal session | Verify LegiScan bulk access and selection of **U.S. Congress**. The optional Congress.gov key is for federal service terms. |
| Worker leaves jobs queued | Check that it is alive, has `API_KEY`, and points to the same `DATA_DIR` as Streamlit. |
| Incomplete vote counts | Check source roll-call coverage, retry status and reviewed identity mappings separately. |
| Staff roster preview rejected | Confirm the file is a valid XLSX with the expected tabs and appropriate content. Review diagnostics before applying a replacement. |
| Email unavailable | Verify all three settings on Streamlit, PostgreSQL connectivity and successful schema setup. |
| Verification or alert absent | Check spam, verified domain, Resend activity, mailbox preference, sender schedule and corpus freshness. The initial pass establishes baseline. |
| Git publication rejected | Check upstream access, the allowlisted paths and pending commit inventory. Private backups continue through the filesystem and PostgreSQL procedure. |
| State seems gone after deploy | Verify the original mount, data directory and identity. A changed directory can start a new empty instance. |

The app starts at **Dashboard** with shortcuts for discovery, monitoring, and policymaker research. Save private status labels under **Advanced Filters**. Status filters continue to use the underlying provider codes. Admins edit the shared keyword list in **Keyword Rescan**. Save one phrase per line, then rescan to collect new matches. Prior matches remain indexed for historical research.

For repeatable Python 3.12 builds, install the hash-checked cross-platform lock with `python -m pip install --require-hashes -r requirements.lock`. Update it using `uv pip compile requirements.txt --universal --python-version 3.12 --generate-hashes --output-file requirements.lock`. Run the following from the checkout using the installed environment.

```sh
python -m pip check
python -m unittest discover -s tests -v
python cli.py --help
python tools/generate_techmap.py --check
```

CI also runs undefined-name lint checks and an isolated Playwright smoke test. To run them locally, install Ruff 0.15.0 and Playwright 1.58.0, run `python -m ruff check --select F821,F822,F823 .`, install Chromium with `python -m playwright install chromium`, and run `python tools/smoke_browser.py`. The script starts a loopback app with temporary data, checks health and navigates to Historical Coverage using a guest browser session. [Playwright CI setup](https://playwright.dev/python/docs/ci)

The PostgreSQL test is conditional on `TEST_NOTIFICATION_DATABASE_URL`. Use a dedicated scratch database and test role. The [CI workflow](.github/workflows/regression.yml) provisions PostgreSQL 16 and Python 3.12. This guide gives cross-platform procedures. Verify hosting, proxy and mail behavior for your chosen target with controlled data before opening a production instance.
