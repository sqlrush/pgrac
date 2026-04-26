# pgrac cluster TAP integration tests

This directory hosts pgrac's TAP-based integration tests.  Tests
under `t/` are driven by Perl `prove` via PG's standard
`$(prove_check)` rule, identical to `src/test/recovery/`.

**Status (stage 0.5)**: framework + 3 example tests.

## Running

Requires `--enable-tap-tests` at configure time.

```bash
# from src/
./configure --enable-cluster --enable-tap-tests ...
make
make -C src/test/cluster_tap check
```

Or via the pgrac convenience target:

```bash
make -C src/test cluster-tap     # only TAP tests
make -C src/test cluster-check   # unit + tap + regress
```

## Layout

```
cluster_tap/
├── lib/
│   └── PgracClusterNode.pm     # helper extending PostgresNode
└── t/
    ├── 001_single_node_smoke.pl    # single PG starts + cluster symbols
    ├── 002_multi_node_harness.pl   # two PG instances coexist
    └── 003_cluster_init_called.pl  # build-time integration sanity
```

## Distinction from sibling test directories

|                  | cluster_unit       | cluster_tap            | cluster_regress     |
|---               |---                 |---                     |---                  |
| Driver           | C `main()`         | Perl + PostgresNode    | SQL via pg_regress  |
| Spawns PG?       | no                 | yes (one or more)      | yes (single)        |
| Granularity      | function           | process / multi-node   | SQL behavior        |
| Typical runtime  | < 1 s              | 30-60 s                | 1-3 min             |

## When to add tests here

Tests belong in `cluster_tap/` when they need:
- A real running PG server
- Multiple PG instances (for harness scaffolding before real
  Cache Fusion lands)
- Process-level interactions (signals, restart, log inspection)

Tests that only exercise pure C functions belong in `cluster_unit/`.
SQL-level behavior tests (`*.sql` / `expected/*.out`) belong in
`cluster_regress/`.

## Author

SqlRush \<sqlrush@gmail.com\>
