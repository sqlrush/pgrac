# Fresh shared acceptance tests

These tests create disposable databases. They do not convert an existing
database, edit control/page/WAL formats, or remove a `backup_label` to start a
server. The first-chain TAP tests use the built-in black-box command adapter;
see [BLACKBOX.md](BLACKBOX.md). Its fresh PRE2 product entry mapping is still
required. Without the S11/S12 entries it reports **BLOCKED** with failing TAP
assertions and exit 1. The older Python acceptance runner below continues to
use the external adapter interface.

`PASS` means every real assertion and fixture shutdown succeeded. A missing
dependency is `BLOCKED`; an unexpected runtime error or failed assertion is
`FAIL`. Neither is counted as passing. The Python assertion checks must remain
enabled. Harness unit tests are separate from database acceptance.

## Run

```sh
python3 src/test/cluster_tap/pre2/acceptance.py --list
python3 src/test/cluster_tap/pre2/acceptance.py \
  --bindir /path/to/independent-build/install/bin \
  --adapter /absolute/path/to/qualified-test-adapter \
  --output /new/path/to/results
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p 'test_*.py'
python3 src/test/cluster_tap/pre2/run_legacy.py \
  --build /path/to/independent-build --output /new/path/to/legacy-results
```

`--case NAME` selects cases and may be repeated. The output directory must not
already exist. Results, SQL output, adapter observations, binary/config hashes
and fixture logs are retained. A source-worktree commit and installed binary
SHA are independent identifiers, not an assertion that the binary came from
that commit. The legacy runner runs 310/313/314/316/324 in separate prove
processes so an early bailout cannot suppress the other cases. Its 1800-second
watchdog is a test failure, not an acceptance timeout allowance.
The legacy runner reports skips and unsuccessful runs as `FAIL`; a familiar
startup diagnostic alone is not evidence for `BLOCKED`. Any later dependency
classification must retain the original failure and cite its verified cause.

An authorized experiment schedule and the required frozen-binary microbenchmark
gate remain prerequisites for four-node workload runs. These scripts neither
reserve a lab nor establish that gate. A native fixture smoke test or a mock
adapter does not qualify a physical multi-node deployment.

## TAP fixture entrance

The `PostgreSQL::Test::ClusterPRE2` module defaults to the black-box adapter.
An external adapter can still be selected with `PGRAC_PRE2_TEST_ADAPTER` for
the earlier scenarios. It supplies regular PostgreSQL TAP node handles and
the existing ClusterQuad observation methods. It does not invoke the old
constructor, `init_from_backup`, or the legacy seed/migration path.

At activation, replace the old initialization block with:

```perl
use PostgreSQL::Test::ClusterPRE2;
my $cluster = PostgreSQL::Test::ClusterPRE2->new_for_tap(337, 'sc_ddl');
my $handles = $cluster->legacy_handles;
my ($node0, $node1) = @{$handles->{nodes}};
my $shared_root = $handles->{shared_root};
my $wal_root = $handles->{wal_root};
my $disks_csv = $handles->{disks_csv};
$cluster->start_cluster;
# Existing SQL assertions follow; use $cluster->stop_cluster at teardown.
```

The entrance supports 337/339/346 (shared relmap)/361/366/371 with two nodes
and 362 with four nodes. `legacy_handles` also supplies `disks`, `ic_ports` and
`data_ports`. Pass test-specific `extra_conf` before initialization. Additional
negative formations inside 361 must each use their own fresh entrance; it is
not enough to change only its first formation. Existing TAP files remain
unchanged in this patch; switching their hand-written setup blocks still
requires the ready product adapter. Any old TAP skip must be classified as
BLOCKED by its runner, never as complete acceptance.
The Perl fixture also requires `open_observation` and `shutdown_observation`
as described in BLACKBOX.md; a transport that only supports start/stop is
insufficient to prove OPEN or a normal shutdown.

## Test adapter interface

This is test plumbing, not a database authority or an admission API. The
adapter is an executable invoked without a shell. Each call reads one JSON
object on stdin and writes one JSON object on stdout. Diagnostics go to stderr.

Request: `{version: 1, op: "...", layout: {...}, args: {...}}`.
Reply: `{version: 1, status: "OK", ...observations...}`.
Only `describe` may return `status: "BLOCKED", dependency: "..."`. An error
after declared readiness is FAIL. The adapter must call actual product
interfaces. It cannot fabricate identities, set an activation bit directly,
patch a catalog version, bypass DENY, infer terminal I/O from PID death, or
return a final case verdict. Missing hooks must be omitted from capabilities.

`describe` returns `capabilities`, and for bounded member cases returns
`limits: {recovery_budget_ms: N, source: "existing frozen limit reference"}`.
The runner reads the drain deadline from the actual server GUC and binds the
normal recovery budget before any scenario starts. Limits are not tuned by
the runner.

The layout contains a new root, shared-data and WAL directories, three voting
paths, a node array (`id`, `data_dir`, `host`, `port`, `ic_port`, `data_port`,
`logfile`) and `extra_conf`. Only these disposable paths may be initialized.
The Python runner uses one LMS listener per node. The Perl fixture reserves
two contiguous DATA ports per node for the normal TAP default.

| Capability | Operations / required observations |
|---|---|
| fresh_init | `fresh_init`: supported product initialization into all new paths, including new-format identity and per-thread state; no backup clone fallback |
| shared_start / shared_stop | `shared_start` / `shared_stop`: qualified startup and normal shutdown; no direct serving bypass |
| root_observation | Current selected `system_identifier`, `root_digest`, `generation`; native SQL independently checks sysid, catalog version 202609290 and the shared settings on every node |
| wal_observation | `observe_begin(relation)` and `wal_observation`: complete causally ordered events since the barrier; filter one relation incarnation, retain all writers and WAL generations |
| current_handoff | Exact `tag`, `space`, `block`, `holder`, `previous_holder`, `handoff_id`, `version`, `writer`; record actual cross-node transfers, not cached-X hits |
| checkpoint_barrier | Named holder DATA cut plus `current_handoff_pending` returning exact requester/tag/request ID; must not require a lock-held blocked update to finish before releasing the cut |
| holder_write_eio | Targeted real holder DATA I/O failure; hit observation includes EIO and exact node |
| terminal_fence | `crash_and_fence`: actual failure and qualified old-writer terminal I/O; return exact node, old incarnation, sysid, duty and terminal proof |
| recovery_observation | Catalog/transaction recovery and RESET/open trace, exact duty and survivor boot identities; W06 also requires actual terminal duty |
| wal_recycle | Drive the existing checkpoint/recycle mechanism; no test unlink; report actual release events |
| ddl_barrier / persistent_reader | Precommit and postcommit-before-invalidation cuts, warm long-lived catalog readers; report the actual RESET barrier and SPACE callback result |
| oid_candidate / replay_window / repeat_drop_replay | Qualified test-only candidate control, exact retained DROP window with close held, repeated actual replay; explicit durable close and existing reclaim consumer. Native-only GUC-mutating helpers do not satisfy these capabilities |
| backup_refusal_target | Create a disposable target with a real backup label via supported backup tooling; attempt startup, preserve label/control, observe zero shared mutation |
| membership_owner / leave_barrier | Compose the complete current eight-field request, actual SQL execution, exact RESERVE/drain/COMMIT cuts and terminal traces |
| pi_retirement | Actual contribution/DATA/coverage/retirement observations for the departing node; COMMIT/REMOVE must close them, ABORT may retain live PI with protected WAL |
| rejoin_admission / xid_era_observation | Actual root/DENY/admission/open ordering, exact request/incarnations, durable new XID era and historical allocation upper bound |

Named barriers used by the runner are `checkpoint_before_holder_data`,
`holder_data_eio`, `holder_before_data`, `ddl_before_commit`,
`ddl_committed_before_invalidation`, `drop_before_unlink_checkpoint`,
`leave_drain_after_reserve` and `leave_after_commit`. They are adapter names,
not claims that product injection points already exist. `barrier_wait` must
observe a real hit before the test continues. `barrier_release` must only
release the named test barrier.

### WAL event contract

`wal_observation.events` contains these events in causally observed order;
cross-host wall-clock sorting is not a substitute for a barrier:

- `contribution`: unique `id`, `node`, exact `tag`, `space`, full `writer`
  (thread/incarnation/WAL generation), `version`, absolute retained `wal` path.
- `data`: unique `id`, object identity, resulting `version`, completion of the
  actual DATA fsync, `sha256` and independent `readback_sha256`, plus exact
  `ancestors: [{writer, version}, ...]` observed from the durable coverage input.
- `coverage`: unique `id`, `receipt` DATA ID, equal-length `contributions`,
  `versions`, `writers`. A DATA receipt cannot cover a contribution which
  appeared later or is outside its ancestry.
- `retire`: exact `contribution` and `proof` coverage ID.
- `wal_release`: the actual `wal` identity released by the product.

The test checks that every required WAL file still exists before retirement,
and that release follows exact DATA/coverage/retirement. A scenario must
exercise at least one real release to pass its final release assertion.

### Recovery and member traces

Recovery events are `catalog_recovered(duty)`, `reset_all(duty,node,boot,barrier)`
for every survivor, then `open(duty,barrier)`. RESET must occur after catalog
recovery, and OPEN after all exact current-boot RESETs. The persistent-reader
probe must observe the same barrier, including SPACE identity cache callbacks.

Member trace keys are SHA256 of canonical JSON (sorted keys, no whitespace)
of the original full request and the exact `crash_and_fence` reply. The trace
must also carry those exact input objects. The runner independently computes
the expected keys; a self-consistent trace for another request cannot pass.
Execution must return the real CLI schema with status `accepted` or `retry`;
`blocked` cannot become a positive case by echoing the request.

LEAVE/failure order is `reserve → drain → peer_failed → abort → abort_closed →
fail_stop_reserve → fail_stop_complete`, using one local monotonic elapsed-time
observer and the prebound budgets. COMMIT/peer-death closure must consume voting
disk proof, preserve the request identity, omit the dead peer from required
ACKs, finish within the normal bound and retain committed row values. Removal
and rejoin must return exact original request identities, retire the departed
node's PI obligations and preserve the old data across the new incarnation.
The XID observation's `new_first_xid` and `historical_allocated_upper` are full
epoch-qualified XID bounds, not wrapping 32-bit candidate values.

## Additional owner unit test

```sh
make -C /path/to/build/src/test/cluster_unit -f Makefile \
  -f /path/to/source/src/test/cluster_unit/qvotec_owner_recovery.mk \
  test_cluster_qvotec_owner_recovery
/path/to/build/src/test/cluster_unit/test_cluster_qvotec_owner_recovery
```

It reuses the real voting-file fixture and product objects. It covers accepted
but unsettled requests under owner handoff, competing replacement requests,
and recovery from newly allocated shared memory. The last test is a unit
simulation of shared-memory reinitialization, not a live crash/startup test.
