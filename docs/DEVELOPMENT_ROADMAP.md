# Development roadmap

Develop on `improved`. Use `main` solely as a legacy comparison. Prioritize verified correctness and data preservation. Move from local fixtures through integration, authenticated acceptance and controlled rollout. Each implementation commit needs evidence, acceptance criteria and rollback implications.

| Phase | Priority and scope | Dependencies | Complexity | Exit criteria and status |
| --- | --- | --- | --- | --- |
| 0 · Audit and safety | Verify `improved` and `main`, inventory behavior, capture baseline | Read access and reproducible fixture environment | Medium | Initial evidence recorded. Live data lineage, representative load and historical accuracy remain unverified. |
| 1 · Stabilization | Address AUTH-01 and MAIL-01 first. Follow with explicit provisioning, throttling, keyword freshness and worker failure tests | Preserved JSON shape, upgrade fixtures, safe deployment policy | High | Focused first repairs in this checkpoint. Broader authentication and source consistency remain open. |
| 2 · Architecture and persistence | Establish connection ownership, migration contract and page/service separation | Concurrent regressions, backups and rollback fixture | High | Existing data, routes, operation ordering and compatibility paths pass after each extraction. Open. |
| 3 · Performance and reliability | Profile UI reruns, scale corpus fixture, optimize proven bottlenecks and recovery | Correctness parity and a representative workload | High | Publish repeated p50/p95/p99 plus recall and resource measures. Initial synthetic baseline available. |
| 4 · Core UX | Improve dashboard, navigation, unified bill research and accessibility | Real task scenarios and browser tests | High | End-to-end search and workspace tasks pass with state preserved and timed against baseline. Open. |
| 5 · California intelligence | Add official hearings, committee provenance, deadlines, amendments, related bills and vote intelligence | Source-specific fixtures, freshness and identity policies | High | Source citations, uncertainty states and dated historical fixtures demonstrate accuracy. Open. |
| 6 · Research and organizations | Structured briefs, portfolios, organizational positions, assignments, sharing and exports | Stable private storage and explicitly modeled team permissions | High | Workspace ownership, edit conflicts, change history and access boundaries verified. Open. |
| 7 · Optional automation and AI | Evidence-backed review assistance after core provenance is trustworthy | Phase 5/6 gates and a verified evaluation set | High | Human review, privacy, quoted evidence and accuracy evaluated against baseline workflow. Open. |
| 8 · Release | Production acceptance, restore rehearsal, deployment and measured comparative workflow | Authorized access, test credentials, realistic tasks, readiness gates | High | Document exact deployment, backups, performance, production acceptance and rollback. Prior deployment smoke available; new code stays local until authorized for release. |

See [audit findings](AUDIT_FINDINGS.md), [test matrix](TEST_MATRIX.md), [baseline](PERFORMANCE_BASELINE.md), and [release acceptance](RELEASE_ACCEPTANCE.md). No production provider testing is possible from the current workspace without separate environment access and credentials.

## Experimental branch

Development and subsequent follow-up commits from the master brief are now staged on improved-exp. The improved branch remains the pre-experiment release line. The first persistence change isolates CorpusManager connections and API accounting across concurrent threads. Production cutover, a coordinated restore rehearsal, account/workspace migration review, and measured concurrent throughput remain open. Corpus, staff, and job stores now have transactional adoption migrations and reject unsupported newer schemas.
