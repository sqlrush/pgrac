# PRE2 first-chain TAP tests

Run these outside reserved measurement windows. They create disposable DATA;
they do not provision a physical lab or authorize a fencing target. A real
terminal-I/O provider is required for the recovery scenario. No PID kill is
used as a substitute for that proof.

```sh
python3 src/test/cluster_tap/pre2/run_blackbox_tap.py \
  --build /absolute/path/to/independent-build \
  --entry-file /absolute/path/to/qualified-product-entries.json \
  --output /new/result/directory
```

The build must contain `install/bin/{postgres,pgrac-init,pg_ctl,psql}` and
`src/test/regress/pg_regress`. `identity.json`, if supplied, is retained as the
build's source identity, separately from the test checkout commit and hashes.
Results retain tool output, logs, DATA, binary hashes and configuration hashes.
Do not put credentials in an entry file or SQL output; traces are retained.

Omit `--entry-file` to test the current missing-entry diagnostic. Each TAP
runs installed `pgrac-init --help`, then fails on absent S11/S12 initialization.
No database is started in that path. These are **three failed raw TAP cases**,
classified as **BLOCKED**, never three passes or skips. A SQL/runtime error
after a ready entry mapping is supplied remains **FAIL**. The runner does not
retry SQL, reduce workload or extend an existing acceptance budget.

| TAP | Required product entries | Assertions |
|---|---|---|
| 431 | S11/S12 fresh configuration C, S16 OPEN, S17 shutdown observation, S18 predecessor observation | Four-member fresh OPEN; cross-node INSERT/UPDATE/COMMIT; normal stop with no leftover processes and exact CLOSED writers; same DATA/shared paths/sysid restart with new generation and exact CLOSED predecessor; all rows and a new cross-node commit; second normal stop |
| 432 | Above lifecycle entries plus certified failure, two survivor recoverers and real recovery cuts/journal | Two different surviving nodes execute DATA in one window; the first finalizer is deferred while its peer owns WALR-S; exclusive completion follows peer retirement; durable RECOVERY_COMPLETE; an old-window write is rejected; no DATA write after completion; both executors retire; committed data survives |
| 433 | Above lifecycle entries plus target block holder/ship observations | Actual remote X on the named MAIN block; first read delivers it and leaves both nodes holding S; repeated reads do not ship again; a subsequent X update revokes the old S image and the remote reader sees the new value |

## Product entry mapping

The file is **test command configuration**, not a new product CLI ABI. Populate
it only with documented, supported entries in the build being tested. Until
those entries exist, leave them absent. An empty mapping fails:

```json
{"version": 1, "initialize": [], "operations": {}}
```

`initialize` is an ordered list of objects with `tool: "pgrac-init"` and an
`argv` string array. Exactly one invocation must contain `--cluster-seed`.
All DATA/shared/WAL/voting paths must be newly created by the supported product
initializer. `--force`, `--join-from` and `--join-from-backup` are refused.
Do not clone a backup, strip backup_label, patch root/control/voting bytes,
or create multiple unrelated seed databases. The adapter never runs initdb,
copies database files or formats voting disks itself.

Argument substitutions are individual argv strings, without shell expansion:
`${root}`, `${name}`, `${shared_data_dir}`, `${wal_root}`, `${layout_file}`,
and `${node0_data_dir}`, `${node0_port}`, `${node0_host}`, etc. for all member
fields. Absolute initializer paths must belong to this disposable fixture.
The layout input file also supplies three fresh voting paths and `extra_conf`.
The supported initializer must consume the complete topology and apply its
ports, per-node identities and settings. There is no test-side activation or
fallback configuration patch. No new `pgrac-init` option is presumed here.

`operations` maps each name below to either:

- `{"node": "argument", "sql": "..."}` (or a fixed numeric node). The SQL
  must query/invoke the real product interface and return exactly one JSON
  object. Input is a safely quoted psql variable, `:'pre2_args'::jsonb`.
- `{"tool": "pgrac", "argv": [...]}` (also `pgrac-ctl`). `${args_json}` is
  the full input object as one argv substitution. This is usable only if that
  actual installed CLI supplies the requested operation.

Queries may project existing native fields into the names below, but must not
echo the request as evidence, supply fixed observations, infer completion from
elapsed silence or sort independent host clocks into a causal journal. A
missing native field is a missing dependency, not permission to synthesize it.
JSON replies contain observations only, without `status`, `version` or
`verdict`; the adapter adds the transport envelope and the TAP owns assertions.

### Lifecycle observations

- `root_observation`: current selected `system_identifier` (decimal string),
  `root_digest` (64 lowercase hex), positive `generation`.
- `open_observation`, input `{node}`: the same root identity, `phase: "OPEN"`,
  exact sorted `members`, and `writer: {node, thread, incarnation,
  wal_generation, boot}`. Query the named node. The fixture requires one common
  root cut across all members, distinct WAL threads, actual configuration C,
  and native catalog version 202609290. After restart, `writer.predecessor`
  must identify the exact prior CLOSED `{thread, incarnation, wal_generation,
  boot}`. The root and WAL generations must advance and the boot must change.
- `shutdown_observation`, input `{nodes, writers}`: **offline product CLI**
  readback of `system_identifier`, `final_checkpoint: true`, and `writers`
  with exact identities plus `state: "CLOSED"`. All nodes have stopped, so
  this operation cannot use SQL. Do not echo the supplied expected writers.

`shared_start` uses pg_ctl start `-W` for all nodes before polling SQL.
`shared_stop` sends pg_ctl stop `-m fast -W` to all required nodes, then checks
native stopped status, the pre-stop process tree, normal shutdown records and
absence of abnormal-exit/reinitialization records. No immediate stop is used
as normal-shutdown evidence. PostgreSQL TAP's final error cleanup remains an
error cleanup and cannot turn an already failed case into PASS.
Startup SQL, OPEN polling and normal shutdown each use a 60-second monotonic
budget, including the final successful observation. Every command has a bound.
Scenario SQL has a 30-second client deadline and no retry; identity/entry SQL
has a 10-second bound. A timeout fails the scenario and preserves its output.

### Recovery operations

Input scope includes `relation`, victim node 3, recoverer nodes `[0,1]`, and
`identities: {system_identifier, victim_writer, recoverer_writers}` from the
actual OPEN observations. Writer identities contain node/thread/incarnation/
WAL generation/boot. This is the expected scope, not evidence by itself.

1. `recovery_arm`: arm actual two-recoverer cuts for that relation/window.
2. `recovery_crash_and_fence`: certified failure/terminal-I/O handoff. Return
   current `victim`, `system_identifier`, exact `identities`, `terminal_proof`,
   `duty`, and `token`. The latter three nonempty identifiers scope all later
   operations. Never substitute membership DEAD or PID disappearance.
3. `recovery_wait` for `first_ir_released_peer_s`: node 0 finished its DATA and
   released IR, while node 1 owns WALR-S. Return the real `cut`, `duty`, `token`.
4. `recovery_release` for `first_completion_attempt`, then `recovery_wait` for
   `completion_deferred_peer_data`: node 0 was denied completion by the live
   peer, and node 1 performed DATA after node 0's IR release.
5. `recovery_release` for `peer_finish_then_finalizer`: retire node 1's write
   authority, allow node 0 to publish completion, and exercise rejection of a
   retained old-window write request.
6. `recovery_observation`: complete bounded journal through both executor
   retirements and the stale-write probe. Return scope fields, `identities`,
   `trace_complete: true`, `overflow: false`, and `events`.

Each journal event has consecutive `seq`, exact `duty`/`token`, `node` and that
survivor's `boot`. The adapter collects a causal order at synchronized product
boundaries. Supported event kinds are `walr_s_acquired`, `data_write`,
`ir_released`, `completion_deferred`, `walr_s_released`, `complete_x_acquired`,
`recovery_complete` (with `durable_readback: true`), `complete_x_released`,
`stale_write_rejected` (reason `RECOVERY_COMPLETE`), `worker_retired`, and
`observation_closed`. Every mutation in the selected window must be included;
filtering out late writes or ending the trace while an executor remains live
is invalid. The finalizer's X release restores its original S pin. A separate
`walr_s_released` event must confirm that pin's eventual retirement before
`worker_retired` and `observation_closed`; X→S alone is not retirement.

### Shared-read observations

`cache_target`, input `{relation, fork: "main", block: 0}`, returns the actual
`{path, fork, block, space}` identity. `space` includes the relation incarnation;
`path` must match native `pg_relation_filepath()` on every member.

`cache_observation`, input `{target, holder: 0, reader: 1}`, returns that exact
`target`, `x_holder` (node or null), unique `s_holders`, and monotone `ships` for
this exact block delivered to node 1. The source starts in X, then both nodes
must hold S. The first read must increase `ships`, and subsequent reads must
not increase it. Cluster-wide counters cannot satisfy this target-specific
observation. If the installed build exposes only aggregate counters, the
case remains dependent on the missing observation entry.

## Harness checks

```sh
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p 'test_*.py'
PGRAC_PRE2_BINDIR=/path/to/build/install/bin \
  prove -I src/test/perl src/test/cluster_tap/pre2/test_fixture.pl \
    src/test/cluster_tap/pre2/test_lifecycle.pl
```

These checks use explicit in-memory command/observation fixtures. They test
the judge and process bookkeeping, not real PRE2 startup, recovery or S-cache.
