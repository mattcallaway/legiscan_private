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
