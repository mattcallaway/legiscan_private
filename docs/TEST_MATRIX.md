# Test matrix

Baseline local Python 3.12 run collected 37 tests, with 36 passing and one skipped for missing isolated PostgreSQL. The earlier VPS release run reported the same result and confirmed the deployed service health. Prior isolated PostgreSQL and browser evidence exists for the same deployed commit; later changes require reruns.

| Capability | Current automated evidence | Next acceptance coverage |
| --- | --- | --- |
| Accounts and workspaces | Account recreation invalidates an old session; tracking conflicts, atomic decision history, recovery and backups have fixtures. | Concurrent account writers, rate-limit behavior, unauthorized role changes, and authenticated browser ownership boundaries. |
| Search and historical coverage | Corpus scope, topic evidence parity and roll-call retry/coverage fixtures. | Historical indexed search browser test, multiple fixture sizes, accessibility and export parity. |
| Jobs and quotas | Conflict, budget, recovery and retry fixture coverage; worker lease fencing test. | Process crash during commit, lock timeouts, duplicate progress and corruption recovery. |
| Staff and profiles | Preview, stale preview rejection, reversible restore, stable identity, term precision and jurisdiction vote coverage. | Representative licensed workbook, official fixture provenance, retries and full admin browser workflow. |
| Hearing and review | Snapshot and cancellation handling, date-only calendar export, due review and snoozing. | Compare with official dated agenda fixtures and test known missing/canceled/tentative distinctions. |
| Email | Validation, provider retry, workspace eligibility and opt-in PostgreSQL integration with mock transport. | Production test mailbox, delivery event visibility and crash/retry after provider acceptance. |
| Deployment and privacy | Browser check for health, guest dashboard, coverage, calendar and custom labels; path allowlist and isolated data restore tests. | Production authenticated workflow, protected backup rehearsal and full rollback drill with a copied data directory. |

[Regression tests](../tests/test_audit_regressions.py) · [Notification tests](../tests/test_notifications.py) · [Browser test](../tools/smoke_browser.py) · [CI workflow](../.github/workflows/regression.yml).

Phase 1 adds a child-process writer barrier test, explicit email address policy cases, and safe bill markup coverage. Earlier deployment evidence remains tied to its original commit. The CI workflow uses separate browser and regression jobs on Ubuntu 24.04 for `improved` pushes and pull requests targeting it.

The account-policy checkpoint adds unit coverage for public, admin-only and disabled enrollment, and verifies that existing account provisioning works independently of browser signup. Login tests check a shared attempt budget, unknown-user comparison, expiry and reset after success. Browser coverage now signs in as a fixture administrator, confirms public signup is hidden, and opens the administrator enrollment controls.
