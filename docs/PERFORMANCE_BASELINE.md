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

## Annotation-filtered search on improved-exp

The expanded synthetic fixture contains 96,000 bills across four sessions. The annotation scenario selects 32 bill IDs across the corpus and measures the All Bills page query and count together. Both runs used the same machine, Python and SQLite environment, fixture, and 30 timed repetitions. The query plan is captured from the actual page query.

| Annotation page plus count | p50 ms | p95 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Before, with bill IDs converted to text in SQL | 74.537 | 77.180 | 84.111 |
| After, with canonical integer IDs supplied to SQL | 9.525 | 11.021 | 12.337 |

SQLite moved from a bills index scan to integer primary-key lookups. The annotation filter continues to match only IDs that had an exact canonical text representation. No schema change or reindex is required. These timings are local Python query timings and do not include the hosted UI. The literal search and historical coverage paths were outside this optimization. In the after run they measured 271.471 ms and 196.085 ms at p50 respectively, making them candidates for separate benchmarks and optimizations.

Reproduce the comparison with the annotation scenario in the harness: python tools/benchmark_corpus.py --rows 96000 --repetitions 30.

## Ordinary literal search across reruns

The same 96,000-bill fixture across four sessions ran 25 repetitions on Windows with Python 3.12.10. A cold count means the harness cleared the count cache for each repetition. A repeated count uses the warmed cache. Both cases read a fresh 50-row result page with the existing literal SQL and ordering.

| Scenario | Cold count p50 / p95 | Repeated count p50 / p95 |
| --- | ---: | ---: |
| Selective phrase, page and count | 254.5 / 296.3 ms | 182.4 / 240.5 ms |
| Common term, page and count | 66.1 / 84.3 ms | 7.3 / 9.1 ms |

The cache holds at most 128 aggregate counts for 60 seconds per corpus manager. It invalidates on a database or committed WAL file change, preserves the existing literal predicate, and bypasses private include/exclude bill sets and explicit SQLite transactions. No schema or index changes are required. The initial page and selective phrase scan remain bottlenecks. These are local fixture timings.

A discarded SQLite trigram experiment cut the selective phrase median from 257.6 ms to 86.1 ms but raised the common term median from 62.9 ms to 273.9 ms. Database footprint grew from 28.3 MB to 71.5 MB and synthetic seeding rose from 3.3 to 30.0 seconds. That index was not shipped. [SQLite FTS5 trigram documentation](https://www.sqlite.org/fts5.html#the_trigram_tokenizer)

Reproduce the count results with python tools/benchmark_corpus.py --rows 96000 --repetitions 25.
