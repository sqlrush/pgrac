# pgrac cluster regression tests

SQL-driven regression tests for the pgrac cluster surface, modeled
after PG's `src/test/regress`.  Each test is a `sql/<name>.sql` input
file paired with an `expected/<name>.out` golden output; `pg_regress`
runs them against a temporary PG instance and fails on any byte-level
diff.

**Status (stage 0.21)**: framework wired up.  Ships one smoke test
(`cluster_smoke`) that locks the column contracts and stable metadata
of the views and GUCs registered through stage 0.13 / 0.16 / 0.17 /
0.18 / 0.19.  Stage 2+ feature specs append additional tests by
following the contribution procedure below — no further build-system
changes needed.

## Running

```
cd src
make -C src/test/cluster_regress check
```

Or as part of the full pgrac test sweep:

```
make -C src/test cluster-check
```

`cluster_regress` is wired into `src/test/Makefile` SUBDIRS only
when configured with `--enable-cluster`; `--disable-cluster` builds
skip the directory entirely (the cluster surface this directory
exercises does not exist in disable-mode binaries by design — see
spec-0.3 symbol contract).

## Adding a new test (Stage 2+ contributors)

1. Write `sql/<name>.sql`.  Inputs and outputs **must be
   deterministic** across host / port / OID / collation / row
   ordering.  Forbidden output sources include hostnames,
   timestamps, OID values, port numbers, `pg_cluster_nodes`
   `hostname` / `interconnect_addr`, and any query whose output
   ordering is not pinned by `ORDER BY`.  See
   spec-0.21-cluster-regress.md §2.2 for the full list.

2. Append `test: <name>` to `parallel_schedule`.  Tests on the same
   line run concurrently; tests on separate lines run serially (in
   the order written).  The default schedule has one test per line
   for simplicity.

3. Run `make -C src/test/cluster_regress check` once.  The first
   run prints "no expected file" and writes the actual output to
   `results/<name>.out`.

4. Manually review `results/<name>.out`.  Confirm every line is
   reproducible (no host-specific values).  If a query produced
   non-deterministic output, fix the SQL — do not commit the
   non-deterministic expected file.

5. `cp results/<name>.out expected/<name>.out` and `git add` it.

6. Re-run the check; it must now pass.

7. Commit the `.sql`, the schedule edit, and the `.out`.

## Distinction vs unit / TAP tests

| | `cluster_regress/` | `cluster_unit/` | `cluster_tap/` |
|---|---|---|---|
| Driver | `pg_regress` (SQL diff) | C `main()` per binary | Perl `prove` |
| Scope | end-to-end SQL behavior | function / link-level | process / startup / multi-step |
| Output | strict byte-level diff | TAP `ok / not ok` | flexible (cmp_ok / regex) |
| Best for | locked column / metadata contracts; DDL / DML behavior | compile-time invariants, sizeof, magic constants | failure paths, log inspection, multi-process timing |
| Worst for | host-dependent output; timing-sensitive cases | runtime SQL surface | static SQL diff |

Pick `cluster_regress` when the answer fits a stable SQL output
table; pick `cluster_tap` when the test needs to inspect logs,
restart the server, or verify behavior across multiple steps.

## When to expect new tests here (Stage 2+ roadmap)

| Subsystem | Likely stage |
|---|---|
| Heartbeat / membership | Stage 2 |
| GRD master selection | Stage 2 |
| PCM block transitions | Stage 2 |
| Cache Fusion 2-way / 3-way | Stage 2 |
| SCN piggyback | Stage 2 |
| Sinval cross-node broadcast | Stage 2 |
| Undo / TT cross-node | Stage 3 |
| Recovery (merged WAL apply) | Stage 4 |
| GES locks (TX / TM / SQ / ...) | Stage 5 |

Each subsystem's spec lands one or more `.sql` files here at the
same time as its first reader code.

## Author

SqlRush \<sqlrush@gmail.com\>
