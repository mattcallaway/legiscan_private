# Release acceptance

| Check | Evidence and disposition |
| --- | --- |
| Reviewed revision | Source baseline `221f279` on `improved`, compared against `8317430` on `main`. Verify remote ref parity at each later release. |
| GitHub branch and history | Remote matched both baseline refs. The recent issue query returned no issue records. CI run 45134301336 passed on the earlier commit. [CI run 45139079392](https://github.com/mattcallaway/legiscan_private/actions/runs/45139079392) failed installing Chromium dependencies; GitHub skipped browser and regression steps. |
| Baseline local tests | 37 collected, 36 passed, 1 PostgreSQL-dependent test skipped on the default run. |
| Previous isolated verification | Prior release tested all 37 with PostgreSQL and mocked Resend, and passed a real-browser guest/dashboard, calendar, coverage and label workflow. |
| VPS release baseline | The operator confirmed the expected commit, active unit, health endpoint, and administrator role. Tests on the VPS collected 37 and skipped the PostgreSQL integration. |
| Performance | Initial fixture measurements recorded in [performance baseline](PERFORMANCE_BASELINE.md). Production performance and comparative superiority remain unestablished. |
| Backup | Operator confirmed archive listing succeeded during the VPS cutover. A full VPS restore has not been demonstrated. |
| Data migrations | Existing notification categories require schema setup on a configured PostgreSQL database. The Phase 1 account serialization work keeps JSON/hashes/workspace IDs and has no destructive migration. |
| Production workflows | Full authenticated watchlist, cross-user privacy, staff automation, source corroboration and live email acceptance remain open. |

This audit checkpoint is ready for continued local development. It does not certify a new VPS deployment, an operating-system migration or any external provider delivery.

## Phase 1 checkpoint

- Local code commit: 4e4b717, on the working branch. Existing account JSON and notification preferences are compatible. Upgrade coordination must stop older account writers first.
- Isolated regression run: 40 passed, including the temporary PostgreSQL integration with a mock mail transport.
- Local browser: Edge passed guest entry, dashboard navigation, historical coverage, calendar and custom labels after waiting for the save confirmation. The first run exposed a browser rerun race in the check.
- Static checks: targeted Ruff and regenerated source map passed. CI is pinned to Ubuntu 24.04 and separates browser and regression jobs. GitHub execution of this workflow remains pending publication.
- Production remains at the previously verified baseline. No real provider email, synthetic live account changes, or full comparative benchmark was performed for this checkpoint.

## Experimental branch checkpoint

The corpus connection change passed 45 local tests, including isolated PostgreSQL with simulated delivery. The browser smoke test also passed. Both GitHub jobs succeeded on [edf613c](https://github.com/mattcallaway/legiscan_private/actions/runs/45149277180). The synthetic benchmark recorded the connection lifecycle cost. There is no new explicit data migration for the change. GitHub Actions and production deployment are separate checks.

## Corpus migration checkpoint

Tests cover a pre-document-column fixture with historical session timestamps and individual votes, a pre-upgrade backup, four concurrent fresh starts, rollback after an interrupted migration, safe restart, and refusal of an unknown newer version. All 49 local regressions passed with isolated PostgreSQL, and the browser smoke test passed. The versioned corpus migration has not been run against live VPS data.

## Staff and job migration checkpoint

All 54 local regressions passed with isolated PostgreSQL, and the browser smoke test passed. The upgrade fixtures preserve staff and identity IDs, restore a pre-versioned roster snapshot, and keep job history and pending payloads claimable after restart. The preceding corpus migration passed both GitHub Actions jobs. Production has not been upgraded.

## Cold-backup rehearsal checkpoint

An automated rehearsal restored a synthetic cold archive, upgraded the three databases, and verified historical coverage, staff profiles, account and workspace data, attachments and a pending job. It rejected archive links and paths outside the data root. The regression suite passed all 55 tests with isolated PostgreSQL, and the final rehearsal-specific checks passed after adding application-level read verification. Both GitHub Actions jobs passed on the preceding staff/job migration. The actual private VPS archive and a PostgreSQL restore remain separate operator checks.
