# Contributing to pgrac

pgrac is built in public and still early — testing, feedback, and patches all
help. You don't need to be a Postgres hacker to be useful here.

## Ways to help right now

- **Build it and run the Quick start** (see the README): build with
  `--enable-cluster`, bootstrap a node with `pgrac-init`, and check
  `pg_cluster_nodes` / `pg_cluster_ic_peers`. File anything that breaks.
- **Poke holes in the architecture** (`docs/architecture/overview.md`). If
  you've operated Oracle RAC or any shared-storage cluster, tell us where the
  design is wrong — correctness under Cache Fusion, recovery, and fencing are
  the areas we most want scrutiny on.
- **Pick a `good first issue`.** These are scoped so they don't require deep
  cluster internals.

## What's in scope today

The cluster substrate is live (TCP interconnect, LMON heartbeat,
SCN/ITL/undo block format, multi-node bootstrap). Cache Fusion / GES /
cross-node recovery are scaffolded and return `FEATURE_NOT_SUPPORTED` — patches
there are best coordinated via an issue first, since the protocols are still in
flux.

## Ground rules

- **Don't regress the non-cluster path.** The `--disable-cluster` build must
  stay binary-equivalent to upstream PostgreSQL 16.13 and pass the full
  219-test regression suite.
- **Assertions follow `AGENTS.md`** — a required runtime check must have a real
  production branch, not live only inside `Assert()`.
- Match upstream PostgreSQL C style (`.clang-format` and `.editorconfig` are in
  the tree).

## Reporting issues / security

Open an issue at <https://github.com/sqlrush/pgrac/issues>. For anything that
looks like a data-corruption or memory-safety bug, please flag it in the title.
