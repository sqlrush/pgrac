# Fresh shared acceptance tests

These tests create disposable databases. They do not convert an existing
database, edit control/page/WAL formats, or remove a `backup_label` to start a
server. The first-chain TAP tests use the built-in black-box command adapter;
see [BLACKBOX.md](BLACKBOX.md). The local Linux controller supports the native
`initdb --pgrac-initdb-cohort` entry with real quorum and fresh voting devices.
Supported product lifecycle observations are still required. Missing entries
report **BLOCKED** with failing TAP assertions and exit 1. The older Python acceptance runner below continues to
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

### Retained workload manifest intake

Verify a retained four-node capture without contacting a server:

```sh
python3 src/test/cluster_tap/pre2/workload_manifest.py /path/to/capture/manifest.json > capture.json
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_manifest.py
```

The version-1 JSON manifest contains these fields:

| Field | Contents |
| --- | --- |
| `version`, `run_id` | Integer `1` and a nonempty capture identifier |
| `source` | `commit` and `tree` as full lowercase Git SHA1 strings; `file` references the retained source snapshot |
| `harness` | File ID of the retained harness snapshot |
| `files` | Object mapping file IDs to `path`, `sha256`, `size_bytes` descriptors |
| `nodes` | Four objects with distinct integer `node_id` values 0 through 3, nonempty `boot_id`, decimal-string `cluster_epoch`, and file IDs in `postgres`, `config`, `stderr`, `inventory` |

All nodes must declare the same cluster epoch and provide identical postgres
bytes. Each file ID has exactly one role: source, harness, or one of the four
files per node. Paths are relative to the manifest directory and must resolve
inside it to distinct regular files. The reader verifies every file's byte
length and SHA256; missing, changed, aliased or unused files are rejected.
Source snapshots and binaries are hashed in chunks. Stderr and inventory files
are held in memory. Preserve all input files alongside the resulting JSON.

Each inventory is a JSON object with `run_id`, `node_id`, `boot_id`,
`cluster_epoch`, `source_commit`, `source_tree`, `postgres_sha256`,
`config_sha256` and `harness_sha256` matching the manifest declarations and
verified file hashes. Its `processes` array may be empty. Otherwise each entry
has a positive integer `pid`, nonnegative integer `procno`, and nonempty
`backend_start`, `process_incarnation` and `role` strings. Duplicate complete
process identities are rejected. PID reuse with different identities is
retained. Duplicate JSON keys and nonstandard numeric constants are rejected
in both manifest and inventories.

`file_binding=VERIFIED` proves that the files read match the manifest digests.
These are caller-supplied declarations, not origin or capture-time attestation;
source commit and binary hashes do not prove built-from-source or the identity
of a running server. The output therefore retains `build_binding=DECLARED_ONLY`.
The existing LOG reader parses the exact verified stderr bytes. A supported
observation naming a different node is rejected. `process_candidates` lists
inventory indices matching each observation's PID, including all reused-PID
candidates and empty lists for missing processes. No candidate supplies fields
to the raw observation or binds its process incarnation, even with one match.

The output always has `identity_bound=false`, `field_root=OPEN` and
`ready_to_run=false`. It does not produce normalized contract events, request
or receipt joins, barrier/sequence completeness, STOP evidence or workload
verdicts. Exit 0 means file verification succeeded, at least one supported
record parsed, and no known record was malformed; exit 1 means a manifest/file
error or malformed known record; exit 2 means no supported records or an
argument error. Unhandled families and empty node logs retain their original
meaning and cannot establish trace completeness. None of these exit codes
authorizes a workload or satisfies native runtime gates.

### Structured workload LOG intake

Read one retained PostgreSQL stderr log without starting a server:

```sh
python3 src/test/cluster_tap/pre2/workload_log.py /path/to/server.log > observations.json
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_log.py
```

The reader supports the existing READ_IMAGE, R4_UNDO, R4_HEAP_UPDATE, HOT_FULL
and CTRC trace families listed in `workload_log.py`. It accepts `LOG:` records
whose message starts with `PGRAC_FAMILY=`, preserves field values and nested
brace blocks, and records the original line, byte offset and SHA256. CSV/JSON
log formats and multiline messages are not supported. Other families remain
in `unhandled`; malformed known records remain in `rejections` with their
original bytes in hex. Preserve the original log alongside the JSON output.

Exit 0 means at least one supported record parsed and no record was rejected;
exit 1 means rejection; exit 2 means no supported record or a file/argument
error. Parsing checks the envelope and ending disclosure, not every family
field or a complete trace. Values such as unknown identities, skipped samples,
late emission and saturated sequences are retained without granting evidence
credit. Input order is retained, including repeated sequence numbers. The
reader does not join processes, sort clocks or produce normalized workload
events. All results retain `field_root=OPEN`, `identity_bound=false` and
`ready_to_run=false`; a parsing exit code is not an acceptance verdict.

### Workload producer-field projections

Extract typed fields from a verified retained capture:

```sh
python3 src/test/cluster_tap/pre2/workload_evidence.py /path/to/capture/manifest.json > evidence.json
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_evidence.py
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_hot_evidence.py
```

This uses the manifest format above and preserves its file checks and raw trace
output. Each node includes `file_ids` for its four retained file roles and a
`producer_fields` entry for every parsed observation, in the original order.
Each entry's `source` identifies the manifest hash, stderr file ID and hash,
observation index, original line, byte offset, byte length and line hash.

The supported projections are:

| Family | Field groups |
| --- | --- |
| `READ_IMAGE` | Request scalars, full block tag, reply scalars |
| `READ_IMAGE_HOP` | Forward/reply request scalars, known block tag, reply scalars |
| `CTRC_RECEIPT` | Canonical transaction key, local reservation, target |
| `CTRC_RECEIPT_LIFECYCLE` | Canonical key, reservation, target, handle, publication |
| `CTRC_CONSUME` | Canonical key, handle, consume scalars, final target, consumed data target |
| `HOT_FULL` | Snapshot, build/search results, before/FULL pages and their root/queried ItemId, tuple and ITL snapshots |
| `R4_HEAP_UPDATE` | Update scalars, requested/old/new TIDs, undo words, inserted WAL LSN and four page/ItemId snapshots |
| `R4_UNDO` | Builder/route/walk scalars, locator, UBA words/decoded fields, record header/target, page/ITL and target ItemId/tuple |

Each group has `source_fields`, `raw_values`, `guards`, `missing`, `invalid`
and `status`. `values` contains a complete typed group only when its status is
`OBSERVED`. Missing components or a missing/false known flag yield `UNKNOWN`
and empty `values`; malformed known flags or active scalar representations
yield `INVALID`. Raw values remain available in either case. Numeric checks
cover the formatter's decimal representation and range, not protocol validity.
`OBSERVED` means values appeared in one local diagnostic projection; it does
not establish a transaction identity, delivered request or retired receipt.

Native slash fields retain their component names: tags, TIDs and tuple masks
are separate objects; raw UBA words are `word0`/`word1`, without conversion to a
hex UBA or comparison with another projection. ItemId brace fields retain the
original brace text in `raw_values` and use dotted `source_fields` paths such
as `full_root.ctid`. Page, ItemId, tuple and slot known flags gate their own
groups. A redirect or other non-tuple ItemId can expose its flags and storage
without supplying a tuple or ITL slot. Raw command IDs are not resolved combo
command IDs. Before/after pages and record targets stay separate. Inserted WAL
does not establish flush or durability; the original flush flag remains in
the trace. HOT decision text is retained raw. Legacy prune/index-delete
envelopes remain unhandled by the raw reader.

Request and reply IDs remain distinct. Receipt handles do not become receipt
proofs, and final targets remain separate from consumed data targets after a
retarget. The legacy `CTRC_RECEIPT` domain remains absent (`null` in the
projection). Repeated consume sequence numbers remain separate observations.
Other parsed families have `status=UNSUPPORTED`; unhandled families and
malformed records remain in the original trace output.

The aggregate `projection_status` is `PROJECTED`, `PARTIAL`, `INVALID` or
`NOT_OBSERVED`. Exit 0 means supported projections were read without a file,
parser or scalar error, including partial projections. Exit 1 means one of
those errors; exit 2 means no supported projection or an argument error.
Results always retain `identity_bound=false`, `field_root=OPEN` and
`ready_to_run=false`. Inventory candidates never fill missing producer fields.
The adapter does not join lines, infer causal edges, produce contract events,
seal barriers or supply STOP, authority, durability or workload credit.

### Read/image and HOT SQL operations

`WorkloadSql.pm` uses four existing persistent `query_safe` sessions for node
IDs 0 through 3 in one fresh shared database. Supply dedicated idle sessions
with autocommit enabled, superuser access and finite query timeouts. The
module does not form a cluster, open connections or enable trace settings.

```perl
use WorkloadSql; # add this pre2 directory to @INC
my $driver = WorkloadSql->new(
    sessions => \@sessions,
    schema => 'read_hot_unique_run_name', # must not already exist
    emit => sub { print {$journal} JSON::PP->new->canonical->encode($_[0]), "\n"
                  or die "write SQL journal: $!"; },
    between_lanes => sub { run_caller_owned_itt_barrier(); },
    receipt_barrier => sub {
        my ($phase, $sql_facts) = @_;
        capture_caller_owned_receipt_evidence($phase, $sql_facts);
    });
my $result = $driver->run();
```

Flush and retain the journal on every attempt, including exceptions. Configure
each `PostgreSQL::Test::BackgroundPsql` session's finite timeout and call
`set_query_timer_restart()` before use. The caller closes the sessions and
performs its cluster shutdown and evidence collection even when `run()` dies.
The module tries one `ROLLBACK` for each reader it opened and any Q transaction
without a COMMIT acknowledgement. It preserves original SQL, cleanup and
journal errors together, and leaves all three relations for inspection.
A failed connection can prevent cleanup acknowledgement; this is a failure,
and the caller must retain it. A failed COMMIT has an unknown outcome; a later
ROLLBACK attempt does not prove it was undone. COMMIT is never retried, and an
acknowledged commit is recorded before journal emission can fail. The same
driver object cannot be run twice.

Node 0 mutates `r_img`, read by node 1; node 2 mutates the separate `h_hot`,
read by node 3. Both readers hold read-only repeatable-read transactions.
The module retains each exported snapshot's actual file contents, validates
its cluster SCN/epoch and backend/database identity, and uses that SCN for
`cluster_cr_test_image()`. It compares all visible `(id,v)` rows, retaining
duplicates, across the baseline, ordinary read and FULL reconstruction over
all relation blocks. H operations include nonindexed-column updates, indexed
key changes, deletes and VACUUM. Their actual HOT/prune/index-delete decisions
and remote request/serve paths require independent producer LOG evidence.

The separate `q_receipt` relation has one key, seeded before R mutations.
After H, node 0 executes one ordinary `BEGIN`, reads its actual PostgreSQL full
transaction ID, updates that key and sends `COMMIT` once. The journal retains
the baseline locator/rows, full xid as an exact decimal string, mutation
result and SQL commit acknowledgement. No `PREPARE TRANSACTION` is used.
`receipt_barrier` is required and runs once with `before_commit` after the
UPDATE, then once with `after_commit` after COMMIT acknowledgement and result
journaling. It receives an owned snapshot of Q's SQL facts; its edits and
return value cannot grant evidence credit. Keep capture work outside these
four dedicated sessions and bound its duration. A hook exception fails the
attempt and does not retry the hook or COMMIT.

The journal retains SQL bytes, raw successful output, hashes, SQL identities
and client-local sequences. Client sequences are not producer trace sequences;
SQL identities lack procno/incarnation/request bindings. Counter snapshots are
node-wide observations, not attribution to this workload. The between-lanes
hook schedules caller-owned I/TT work; it does not validate that work. A
PostgreSQL full xid does not supply an ITL identity or receipt key. Actual
pending undo, receipt preparation/ACK/retirement, run/build/config identity,
producer LOG joins and stop seals remain caller responsibilities. Results
retain `field_root=OPEN`, `trigger_status=UNBOUND`, `receipt_stop_seal=NOT_CHECKED`
and `ready_to_run=false`, including when SQL rows match and COMMIT is acknowledged.

The SQL-boundary unit test uses no database:

```sh
perl src/test/cluster_tap/pre2/test_workload_sql.pl
```

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

The Linux native CF contention test also needs a module built against the
exact installed server headers. Use the same source tree and installation:

```sh
PG_CONFIG=/absolute/path/to/install/bin/pg_config
SOURCE=/absolute/path/to/source
CF_MODULE=/absolute/path/to/test-output/cf_wait.so
cc -shared -fPIC -I "$("$PG_CONFIG" --includedir-server)" \
  -I "$SOURCE/src/include" "$SOURCE/src/test/cluster_tap/pre2/cf_wait.c" \
  -o "$CF_MODULE"
export PGRAC_PRE2_CF_WAIT_MODULE="$CF_MODULE"
```

This module only belongs in the disposable TAP installation. Native Startup
and shutdown contention tests additionally require GDB and a debug-symbol
build so that the fixture can observe and pause the original CF holder.

### MultiXact cold-copy tests

Run TAP447 and TAP448 together in an independent Linux development VM, with
the same source tree and installed server. The VM needs the LocalQuorum
dependencies described in BLACKBOX.md and a non-root database account:

```sh
sudo python3 src/test/cluster_tap/pre2/run_multixact_lifecycle.py \
  --source /absolute/path/to/source --install /absolute/path/to/install \
  --vendor / --uid 1000 --gid 1000 --output /new/path/to/mx-results
```

The controller first completes the positive start/stop/restart test. It then
copies that test's saved DATA and voting images, clears only the copied
activation sector, and binds the copies inside its own mount namespaces.
TAP448 verifies the negative startup, cleared sectors, unchanged saved voting
images and authority files, and stopped processes. The controller separately
checks that the original voting devices and host mount view did not change.
All DATA, before/after images, fault receipts and logs remain in the output
directory. The expected startup refusal is not reported as a normal stop.

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
