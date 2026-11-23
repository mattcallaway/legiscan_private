# Performance baseline

Measured on Windows 10 build 19045 with Python 3.12.10. The harness seeds 12,000 synthetic bills across four sessions into a temporary SQLite database, executes one untimed warm-up per operation, then measures 100 repetitions per operation. It covers the Python query layer and its result construction, including DataFrame work where applicable. Database and WAL-related files occupied 3,580,096 bytes at measurement. CPU, peak memory and concurrency envelope were not recorded.

| Operation at `221f279` | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| 50-row session page | 3.627 | 7.161 | 10.717 |
| Literal search plus count, scoped to one session | 20.964 | 29.930 | 41.379 |
| Historical coverage | 12.262 | 14.600 | 17.291 |

Representative SQLite plans used the covering `idx_bills_session_status_page` index and the `idx_bills_status_day` expression index. Literal filtering still evaluates substring predicates. These are synthetic single-process warm results. End-user page rendering, first load, provider latency, production database size, actual VPS RSS, cold filesystem reads, and cooperative concurrent workload are open measurements. No before/after speedup is claimed.

Run `python tools/benchmark_corpus.py --rows 12000 --repetitions 100`. Retain its JSON together with commit, dependency lock, CPU/RAM, OS and fixture size. Repeat for larger data and mixed writer/reader pressure before changing search or storage topology. Keep the same cohort, filter and accuracy checks for regressions.

## Isolated corpus connections on improved-exp

Rerun with the same fixture size and 100 repetitions after the connection change. The harness now releases its seeding connection before measuring so it exercises the normal operation lifetime. This standalone sample used 3,399,680 bytes of database and journal files.

| Operation | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| 50-row session page | 6.236 | 7.383 | 7.654 |
| Literal search plus count, scoped to one session | 28.073 | 29.433 | 29.921 |
| Historical coverage | 17.694 | 18.509 | 18.775 |

The same representative indexes were selected. Opening and closing connections is additional work. Median timings increased in this run. These separate synthetic runs do not establish a production latency change. The regression suite exercises concurrent read isolation, commit visibility and rollback. It is not a concurrent throughput benchmark.
