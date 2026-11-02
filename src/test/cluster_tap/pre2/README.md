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

### Undo and maintenance regression

Inspect the serial regression commands without creating files or starting a
database (this also works on macOS):

```sh
python3 -B src/test/cluster_tap/pre2/m1_regression.py --plan \
  --source /path/to/source --build /path/to/build \
  --install /path/to/install --fixture-install /path/to/fixture-install \
  --vendor /path/to/quorum-vendor --output /new/path/to/m1-results \
  --uid 1000 --gid 1000 --expected-postgres-sha256 FULL_POSTGRES_SHA256
```

After the components and runtime prerequisites are qualified and a run is
authorized, omit `--plan` in an isolated Linux root environment. The database
UID/GID must name an existing nonroot account. `--build` supplies the matching
`src/test/regress/pg_regress`; the install also needs its original pg_regress,
and fixture-install supplies the candidate headers and formatter link inputs.
An output directory must be new. External adapter/entry overrides and optimized
Python are refused. All original DATA, votes, logs and failed assertions remain.

The entry runs these original controllers in order and stops after a failed
phase; it does not retry or change SQL, assertions or deadlines:

| Controller | Exact TAP selection |
| --- | --- |
| Legacy `run_legacy.py --suite m1` | 222 undo cleaner, 270 undo record-segment reclaim, 402 stall retry |
| Four-member `run_multixact_lifecycle.py --suite m1` | 431, 445, 447, 448, 453, 482 |
| One-member `run_single_shared_vacuum.py` | 461 |

The unrelated writeback TAP with prefix 270 is not selected. The four-member
controller retains fresh per-case native fixtures and the 447-before-448 cold
copy dependency. Its default MultiXact selection remains 447/448; the legacy
controller's default clean-leave selection is unchanged.

`result.json` reports only this selected regression. A phase requires successful
original TAP, no skip/TODO, complete expected cases, the expected installed
postgres bytes and no controller cleanup error. Without `--build-matrix`,
source-file hashes and the binary hash remain independent observations and
`build_binding` stays `UNKNOWN`.
All regular files under `src/test/cluster_tap` and `src/test/perl` are captured
before execution, including the original Perl modules and indirect helpers.
Their inventory and hashes are checked before and after every phase. Added,
changed or missing inputs stop the regression with the original child log and
return code retained; hashes are never rebased. Missing/empty trees, symlinks
and nonregular inputs are refused. Keep the output directory outside source.
`source_input_trees` records this scope. These boundary checks bind those test
source bytes, not an atomic filesystem view, external dependency closure,
product build provenance or a Git commit.
`stage_acceptance` stays `NOT_READY`: this entry does not cover complete shared
positive physical reclamation, the retention/recovery matrix, the 16/48/64
one-hour soak or the full build/stage acceptance matrix. Controller selftests
(`test_m1_regression.py`) start no database and provide no native runtime credit.

An optional `--build-matrix /path/to/completed-matrix` links an existing
four-profile `m1_build_matrix.py` output before any native phase starts. Keep
the original source and all four build trees in place. `--source` must match
the matrix's complete file inventory and `--build` must select exactly one
enabled profile with the expected installed postgres hash. The reader checks
the original profile configuration, command coverage, return codes, raw log
hashes and original unit/SQL judges for every profile. Missing, failed, changed
or symlink receipt inputs are refused. The same receipt identity is rechecked
at the original phase boundaries; it is never rebased or rerun automatically.
`--plan` only records the requested receipt path and need not open it.

A match reports `build_binding=SOURCE_AND_BINARY_RECEIPT_MATCH` and retains
`source_commit=NOT_ATTESTED`. This is agreement between retained observations,
not attested compilation, complete tool/library provenance, a Git commit or an
atomic filesystem view. R01 and stage qualification remain incomplete. The
option does not run a build matrix, install files, change native deadlines or
grant runtime authorization. Receipt-reader selftests execute no server.

### Extended serial regression preparation

Add `--suite extended` to the regression command above to select all three
original phases followed by the prepared native subchecks:

| Phase | Original native controller suite |
| --- | --- |
| `mx-pages` | `m1-mx-pages`, TAP487 |
| `mx-continuation` | `m1-mx-continuation`, TAP486 |
| `idle-freeze` | `m1-idle-freeze`, TAP485 |
| `retention` | `m1-retention`, TAP484 |
| `soak` | `m1-soak-workload`, TAP483 at 16, 48 and 64 clients |

The default remains `--suite core`, with the exact original ten TAP cases.
Extended phases use separate output directories, the same expected postgres
hash and the original controllers, which own formation, deadlines and cleanup.
All phases run serially. The first failed or incomplete phase stops the entry;
no later phase is counted as passed and no automatic retry is added.

Each new phase needs its exact scope, complete case/client list, expected
postgres bytes, successful original prove output and no cleanup debt. The soak
reader requires all three distinct client directories and validates each TAP;
the repeated TAP filename does not combine the three points into one result.
X04/P01/P02/P07 and stage markers must remain `NOT_READY` in these subcheck results.
The MultiXact phase requires its original TAP486 and
`multixact-continuation-and-live-members` scope; the older restart case cannot
substitute for it. Its captured TAP input is included in the same source checks
before and after every phase. Original native cases retain their 1200-second
envelopes, and the soak remains last with its original per-point budget.

Exit zero covers only the selected regression subchecks. Full-width F/H and
catalog cutoff qualification, reader-release recycle progress, complete soak
resource/generation evidence, full MultiXact gap/occupied-page/concurrent
issuance coverage, shared physical reclaim, general recovery and the full
build/stage matrix remain separate. `--plan --suite extended` starts
no processes and reserves no runtime. Executing the extended plan still needs
component readiness, candidate qualification and explicit runtime authorization.
The parser/planner tests in `test_m1_extended_regression.py` use file inputs and
small actual prove results; they run no database and certify no native result.

### Offline long-run commit ledger

`m1_soak.sql` is a fixed simple-protocol workload for 16, 48 or 64 clients.
Before each authorized run, create the same 64 independent tables
`m1_soak_0` through `m1_soak_63`, each with columns `client_id integer PRIMARY KEY`
and `value bigint NOT NULL`, and one row `(table suffix, 0)`. Each client updates
only its own heap. Retain initial and final `client_id|value` rows from all four
members, including the inactive heaps, using a UNION ALL over the 64 tables and
ORDER BY client_id. Preserve the original four-member start/stop judges.

The workload invocation uses `pgbench -n -M simple -j 2 -c CLIENTS -T 3600
--max-tries=1 -l --log-prefix=CAPTURE/pgbench_log -f m1_soak.sql postgres`, with
the authorized node's normal connection environment. Retain both raw per-thread
log files, stdout, stderr, exit status and the exact SQL input. Do not sample,
aggregate, throttle or retry transactions. These instructions grant no runtime
authorization or microbenchmark qualification.

Run `python3 m1_soak_ledger.py CAPTURE/manifest.json` to compare retained files.
The JSON manifest has exactly these fields:

| Field | Value |
| --- | --- |
| `version`, `clients`, `duration_seconds`, `pgbench_rc` | `1`, one of `16/48/64`, `3600`, `0` |
| `workload`, `summary`, `stderr` | File descriptors for the SQL input and pgbench stdout/stderr |
| `logs` | Two raw pgbench file descriptors, one per thread |
| `before`, `after` | Four row-cut descriptors each, in declared member order 0–3 |

A descriptor contains `path` (relative to the manifest), `size_bytes` and lowercase
`sha256`. Inputs must be distinct regular files within the capture. The reader
checks contiguous per-client commit sequences, successful raw records, summary
totals and every final row against the independent ledger. Missing or changed
files, errors, inactive-heap writes and discrepancies fail the subcheck.

Exit zero certifies only this file comparison. `p07` and `stage_acceptance` remain
`NOT_READY`: actual elapsed duration, candidate/cohort identity, generation
progress, complete resource observations and original clean stop need separate
evidence. The existing regression coordinator continues to list the soak as
uncovered. `test_m1_soak_ledger.py` uses literal parser inputs and runs no database.

### Native soak workload and normal-stop preparation

`m1_soak_workload.py` prints the fixed plan without creating files or starting
processes. The opt-in native controller suite `m1-soak-workload` runs
`t/483_pre2_m1_soak_workload.pl` three times, with 16/48/64 clients, sequentially
in separate original four-member formations. Each point uses64 fresh independent
heaps on node 0, the same configuration/SQL and a full 3600-second pgbench run.
All four members supply initial/final row cuts. It retains the actual command,
monotonic elapsed time, summary/stderr, unaggregated logs, hash-bound ledger
manifest/result, original OPEN observations and original normal-stop receipt.

After the original normal stop, `m1_soak_member_logs.py` scans the four closed
member stderr files in node order. `member-logs.json` records each file's size,
SHA256 and exact ERROR/FATAL/PANIC header counts. Each severity retains its first
line number, byte offset, full length/hash and up to 4096 original bytes as hex;
longer excerpts carry `raw_truncated=true`. The first severity token uses the
existing standard-stderr grammar, so error words inside LOG/STATEMENT messages
are not new headers. This does not cover arbitrary log formats or attest origin
identity. Missing, aliased, nonregular, symlinked, changed, non-UTF8 or unterminated
inputs fail the check. A member error or scan failure makes the final
`execution.json` result fail while preserving business and cleanup errors. The
original corruption/PANIC checks still run. No resource SQL is added.

For an offline check, pass exactly four retained closed logs to
`python3 -B m1_soak_member_logs.py NODE0.log NODE1.log NODE2.log NODE3.log`.
Exit 0 means only that this file scan found no matching error headers; identity
remains `DECLARED_ONLY`, and P07/stage acceptance remain `NOT_READY`. The extended
regression captures the reader and its severity-grammar dependency in the same
source hash checks as its other captured inputs.

After component readiness, binary/source binding, applicable microbenchmark
gates and runtime authorization, use the original Linux/root controller:

```bash
python3 -B src/test/cluster_tap/pre2/run_multixact_lifecycle.py \
  --suite m1-soak-workload --source /absolute/source --install /absolute/install \
  --fixture-install /absolute/fixture-install --vendor /absolute/vendor \
  --output /absolute/new-soak-output --uid DATABASE_UID --gid DATABASE_GID \
  --expected-postgres-sha256 INSTALLED_POSTGRES_SHA256
```

No duration, client-count or retry override is provided. The 5400-second outer
envelope per point includes formation, bounded data cuts and offline log
verification; it does not extend the original normal-stop deadline. Existing
native cases retain their 1200-second envelopes. The first failed point stops
the matrix; original cleanup still runs and all DATA/raw evidence are retained.

Exit 0 certifies only this workload/ledger/normal-stop subcheck. Controller and
TAP evidence always state `p07=NOT_READY` and `stage_acceptance=NOT_READY`:
real recycle-generation progress and complete four-resource non-exhaustion
evidence are still missing. No resource-polling SQL is introduced. This suite
does not replace full P07, R01, network-isolation testing or stage acceptance;
the regression coordinator continues to list the complete soak as uncovered.
Planning selftests and Perl syntax checks provide no native runtime credit.

### Original build and SQL regression matrix

`m1_build_matrix.py` drives four externally configured, separate VPATH trees:
cluster enabled/disabled, each with cassert on/off. Every tree must refer to the
same source directory. It checks actual generated configuration, runs original
`make all`, enabled-only full `cluster_unit check` and `cluster_regress check`,
and top-level `make check` in all four profiles. Disabled no-op cluster targets
are marked inapplicable; they supply no passing cluster tests.

```bash
python3 -B src/test/cluster_tap/pre2/m1_build_matrix.py --plan \
  --source /absolute/source --output /absolute/new-matrix-output \
  --enabled-assert /absolute/build-enabled-assert \
  --enabled-release /absolute/build-enabled-release \
  --disabled-assert /absolute/build-disabled-assert \
  --disabled-release /absolute/build-disabled-release
```

`--plan` reads the original schedules and starts no processes or builds. Remove
it only in an authorized nonroot regression environment. Source, four build
trees and new output must be distinct and nonoverlapping. Source snapshots
must contain regular files/directories, without symlinks; `.git` metadata is
excluded. No configure, install, cleanup, retry, deadline override or test
subset is added. Make/test override environment variables must be unset.

The current original schedules contain 219 PG and 13 cluster SQL tests; changed
coverage requires requalification. SQL success needs the complete original
names, sequence, plan, summary and return code. Full unit success needs the
original all-binary loop and census binary. Raw stage logs, commands, codes,
source/config hashes and actual profile postgres hashes are retained. The
first failed stage stops the matrix, with later stages not counted as passed.

Exit 0 certifies only this build/regression subcheck. Generated configuration
and command evidence do not attest a source commit, installed artifact or
native cohort: results keep `source_commit=NOT_ATTESTED`, `r01=NOT_READY` and
`stage_acceptance=NOT_READY`. Native lifecycle, full retention/recovery and
complete soak gates remain separate. Local planner/config/parser selftests
provide no product build, SQL or native runtime credit.

### Native retained readers and snapshot import

The opt-in `m1-retention` native suite runs `t/484_pre2_retained_snapshot.pl`
through the original four-member controller. It leaves the default M1,
MultiXact and soak selections unchanged. Use the same authorized Linux/root
controller arguments as above, replacing `--suite m1-soak-workload` with
`--suite m1-retention`; its outer case budget is the original 1200 seconds.

Four real repeatable-read readers pin the initial rows. Node 0 exports its
snapshot to two importers on that same instance. One importer requests parallel
execution, and actual `EXPLAIN ANALYZE` workers plus query results are required.
Four origin-striped writers each commit one row update; a fifth update rolls
back. Ordinary `VACUUM (FREEZE, VERBOSE)` runs while readers remain and after
their staged release. Exporter/direct readers exit first, ordinary importer
next, parallel importer last. Held values must stay unchanged; fresh reads on
all four members must match the independent literal committed values.

The test retains original exported snapshot bytes, actor PID/birth observations,
writer FullXids/origins, actual row cuts, parallel plans, VACUUM output and the
original normal-stop receipt. These are separate observations, not an atomic
wait graph or full PGPROC-generation proof. First failure and cleanup errors
remain failures; no forced xid, floor, clock, horizon, report or allocator state
is used. Original normal-stop and corruption/PANIC checks remain required.

Exit 0 certifies only retained values, exercised parallel reads and normal stop.
Controller and TAP reports keep `p02=NOT_READY` and
`stage_acceptance=NOT_READY`: recycle/frozen-horizon advancement after the final
reader exits still needs separate proof. Successful VACUUM or reader release
does not supply it. Native execution requires readiness and runtime approval;
local selection tests and Perl syntax checks supply no native runtime credit.

### Native idle freeze and clean restart

The opt-in `m1-idle-freeze` suite runs `t/485_pre2_idle_freeze_restart.pl`
through the original four-member controller, with the original 1200-second
outer case budget. Use the native command above with `--suite m1-idle-freeze`
after component readiness, candidate qualification and runtime authorization.
The original `pageinspect` library and extension files must already be in
that candidate's installation. The case registers the extension in its fresh
database. Missing files fail the case; no build/install step or skip is added.

Four native origin writers insert sixteen literal rows. The test records their
FullXids and checks the unfrozen tuple xmin, CTID and payload through original
`get_raw_page`, `heap_page_items` and `heap_tuple_infomask_flags`. After ten
seconds without business SQL, one original `VACUUM (FREEZE, VERBOSE)` must
actually set every target tuple's `HEAP_XMIN_FROZEN` flag. Elapsed time and a
successful VACUUM command do not supply this proof. All four members must read
the frozen tuples and expected row values. Original full normal stop and
same-DATA restart then precede another four-member readback and final normal
stop. Original startup/stop identity checks and deadlines remain unchanged.

Raw page images, tuple observations, row cuts, actual writer identities,
monotonic idle interval, VACUUM output, target catalog observations and both
stop receipts are retained. No xid/floor/report/clock assignment, resource
polling, workload retry or test adapter is introduced. The first error remains
a failure, including cleanup errors and unexpected member ERROR/FATAL/PANIC.

Exit zero certifies only target tuple freezing and data/catalog preservation
across clean restart. Reports always retain `p01=NOT_READY` and
`stage_acceptance=NOT_READY`: the full-width F/H/catalog cutoff proof and full
M1 acceptance remain separate. Raw xid ordering, stored cutoff equality and
clean restart do not certify them or general crash recovery. Local selection
tests and Perl syntax checks provide no native/product-path runtime credit.

### Native MultiXact continuation after clean restart

The opt-in `m1-mx-continuation` suite runs
`t/486_pre2_multixact_continuation.pl` through the original four-member
controller, retaining its 1200-second outer case budget. Use the native command
above with `--suite m1-mx-continuation` after component readiness, candidate
qualification and runtime authorization. Existing suites remain unchanged.

Before and after original normal stop and same-DATA restart, two real local
`FOR KEY SHARE` holders per origin create four MultiXacts on separate literal
rows. The case records each holder's full/raw transaction ID and PID/birth,
and requires both exact members in `keysh` mode. With all holders still live,
one original `VACUUM (FREEZE, VERBOSE)` runs per origin. After each VACUUM,
each origin must read its unchanged local MultiXact and both original members.
All four members also read back every literal row. Holders must retain their
original transaction identities, commit and exit before each full normal stop.

Original OPEN/stop observations, holder identities, SQL output/status and
first/cleanup errors are retained. No allocator, floor, horizon or SLRU state
is assigned. Errors, refusals, corruption and cleanup debt fail the case.
Exit zero covers only this continuation/live-member subcheck. Both controller
and TAP retain `x04=NOT_READY` and `stage_acceptance=NOT_READY`: forced G>N,
offset-page gap crossing, occupied-page preservation and exact allocator/VACUUM
issuance overlap still require separate evidence. Selection selftests and Perl
syntax checks run no database and provide no native runtime credit.

### Native MultiXact offset-page member preservation

The opt-in `m1-mx-pages` suite runs `t/487_pre2_multixact_offset_pages.pl`
through the original four-member controller and its 1200-second case budget.
Use the native command above with `--suite m1-mx-pages` only after component
readiness, candidate qualification and runtime authorization. It also runs
before the continuation subcheck in `--suite extended`; core stays unchanged.

For each origin, one ordinary transaction retains key-share locks on a bounded
set of independent rows. A second client keeps a live transaction on the first
row. A third client starts distinct transactions on subsequent rows, commits
the intermediate ones and keeps its last transaction active. Distinct member
XIDs require distinct native MultiXacts. The case creates
`block_size / 4 / 16 + 1` IDs per origin (129 with 8KiB blocks), checks actual
local MXIDs and requires an observed offset-page crossing.

Fresh normal psql connections reread the first and last rows' original MXIDs
and exact two native members while both original members of each endpoint
remain active. Intermediate committed members may be removed or recomposed;
their recorded MXIDs are issuance facts, with no unchanged-member assertion.
All three holders' original PID/birth and assigned FullXids are checked before
and after endpoint reads; each observer also records its PID/birth. All clients
must exit before the four-member row ledger and original full normal stop.
First errors, cleanup failures and member-log errors fail the case.

No cursor/floor/SLRU assignment, allocator injection or new product probe is
used. Native execution is not implied by selection, parser or syntax checks.
`x04=NOT_READY` and `stage_acceptance=NOT_READY` remain: natural G>N qualification,
raw page-image/redo proof, exact allocator/VACUUM overlap, general recovery and
full M1 acceptance are separate. This subcheck observes member continuity across
offset pages; it does not certify a raw physical page image or all X04 branches.

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

An inventory may additionally declare `trace_identity_assignments`, an array
of objects with exactly `source` and `identity` members. Use these only for
explicit per-observation declarations; the reader never generates them from
PID candidates or timestamps.

| Member | Required fields |
| --- | --- |
| `source` | `stderr_file_id`, `stderr_sha256`, `observation_index`, `line`, `byte_offset`, `byte_length`, `sha256` |
| `identity` | Integer `node_id`, positive integer `pid`, nonnegative integer `procno`; nonempty string `boot_id`, `backend_start`, `process_incarnation`, `role`, `sequence_domain` |

`source` identifies one parsed observation: the zero-based index in that
node's `trace.observations`, its one-based raw line number, byte offset,
length and SHA256 including the newline, and the complete stderr file ID and
SHA256. Every field must match the verified capture. One observation may have
only one assignment. The complete process tuple and role must match one
inventory process, the node/boot must match its inventory, and PID and sequence
domain must match the raw envelope. A missing raw domain cannot be supplied by
the declaration, including for `CTRC_RECEIPT` and the legacy maintenance formats.
An existing raw `procno` must agree. Raw `backend_start`, `process_incarnation`
and `boot_id` must agree when present and their respective known flag is not
zero. The numeric legacy producer `role` is not translated to an inventory role.

Each node's output has `trace_identity_assignments` in observation order.
Unassigned observations have `status=UNKNOWN`. Accepted declarations have
`status=DECLARED_ONLY`, retain their source and identity separately from the raw
fields, and include the inventory file ID and SHA256. They do not attest process
birth or fill producer fields marked unknown. All entries keep
`identity_bound=false`; repeated sequence numbers remain separate raw lines
and assignments do not establish sequence completeness or causal order.

Each node also has a `trace_sequence_audit` for accepted declarations. Streams
are separated by the entire declared node, boot, PID, procno, backend start,
process incarnation, role and sequence domain. Each stream retains its
`identity`, `observation_indices` and numeric `event_sequences` in original
observation order; the assignment array's order does not reorder observations.
No unassigned observation or other process/domain supplies a missing sequence.

Stream `issues` identify `MISSING_START` (first sequence is not 1), `FORWARD_GAP`
(an adjacent observed sequence jumps forward), `DUPLICATE`, `LOCAL_REVERSAL`
(an adjacent observed sequence decreases), and `EXHAUSTED` (the producer's
exhausted/saturated ending flag is 1). Each issue records the observation index
and sequence; gaps record the expected sequence, duplicates the first retained
index, and reversals the previous retained index. No ranges are expanded, and
later observations do not erase earlier issues. Raw lines are never sorted,
deduplicated or changed. These are observations of retained LOG order, including
deferred emission; they do not establish execution order or causal edges.

A stream has `prefix_status=OBSERVED_PREFIX` only when it starts at 1 with no
observed issue; otherwise it is `ROOT_OPEN`. The node audit is `UNKNOWN` without
declared streams, `PARTIAL` if a stream has issues or any parsed observation is
unassigned or a record was rejected/unhandled, and otherwise `DECLARED_ONLY`.
It retains `unassigned_observation_indices`, `rejected_record_count` and
`unhandled_record_count`. All audits keep `sequence_complete=false` and
`identity_bound=false`: a contiguous retained prefix does not seal an export,
detect missing trailing records, or supply barrier/STOP evidence. The producer
projection reader retains this audit without granting additional credit.

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

The reader supports the existing READ_IMAGE, R4_UNDO, R4_HEAP_UPDATE, HOT_FULL,
R4_HEAP_PRUNE, R4_BTREE_DELETE and CTRC trace families listed in
`workload_log.py`. It accepts `LOG:` records
whose emitter message starts with `PGRAC_FAMILY=`, preserves field values and nested
brace blocks, and records the original line, byte offset and SHA256. CSV/JSON
log formats and multiline messages are not supported. Other families remain
in `unhandled`; malformed known records remain in `rejections` with their
original bytes in hex. Preserve the original log alongside the JSON output.
The optional verbose stderr prefix `SSSSS: ` (five ASCII uppercase letters or
digits) remains in the original message and is exposed as `log_sqlstate` outside
the emitter fields. It is log decoration and does not grant error, identity or
authority credit; plain records do not acquire this metadata. Only one exact
prefix immediately before `PGRAC_FAMILY=` is accepted.
The English PostgreSQL stderr suffix ` at character N` is retained in the
original message and exposed separately as `log_cursor_position`. Its positive
signed-int position is log decoration, outside the emitter fields; the required
emitter ending must still be last before this suffix. Other trailing text is
rejected.

Exit 0 means at least one supported record parsed and no record was rejected;
exit 1 means rejection; exit 2 means no supported record or a file/argument
error. Parsing checks the envelope and ending disclosure, not every family
field or a complete trace. Values such as unknown identities, skipped samples,
late emission and saturated sequences are retained without granting evidence
credit. Input order is retained, including repeated sequence numbers. The
reader does not join processes, sort clocks or produce normalized workload
events. All results retain `field_root=OPEN`, `identity_bound=false` and
`ready_to_run=false`; a parsing exit code is not an acceptance verdict.
The legacy prune/index-delete envelopes retain `node` without adding `node_id`,
`sequence_scope` or `sequence_domain`. The manifest checks that original `node`
against the file role; a PID inventory candidate still does not bind identity.
The existing B-tree summary format concatenates `after_scn` and
`conflict_horizon_known` without whitespace. For `delete.applied` and
`vacuum.applied`, the reader recognizes that exact boundary while retaining
the original message and raw bytes. Other missing separators are rejected.

### Workload producer-field projections

Extract typed fields from a verified retained capture:

```sh
python3 src/test/cluster_tap/pre2/workload_evidence.py /path/to/capture/manifest.json > evidence.json
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_evidence.py
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_hot_evidence.py
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_workload_maintenance_evidence.py
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
| `R4_HEAP_PRUNE` | `prune.result` local page/floor/count/context fields; `prune.item` independent before/after ItemId, redirect, tuple and ITL fields |
| `R4_BTREE_DELETE` | `delete.applied`/`vacuum.applied` local page/horizon/count/inserted-WAL fields; `index.item` original offset, index tuple and captured posting operands |

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
the trace. HOT decision text is retained raw.

Prune/index-delete schemas are selected by event. Summary flags and process
fields never fill item records. Prune `itl_wrap` remains an ITL slot projection,
without supplying a TT wrap or FullXid. Encoded `raw_index_tid` stays separate
from a heap TID, which requires its own `heap_tid_known` flag. Captured posting
positions retain order, duplicates and explicit unknown TIDs; an `OBSERVED`
position list can contain `heap_tid_known=false` entries. Its original brace
text remains available. False posting-shape flags keep the list `UNKNOWN`.
Counts, omitted items, an original index offset and `operation_seq` do not join
summary/item lines, establish a complete deletion history, or map a new offset.
Unknown legacy events remain raw observations with `status=UNSUPPORTED`.

Request and reply IDs remain distinct. Receipt handles do not become receipt
proofs, and final targets remain separate from consumed data targets after a
retarget. The legacy `CTRC_RECEIPT` domain remains absent (`null` in the
projection). Repeated consume sequence numbers remain separate observations.
Prune/index-delete sequence domains also remain absent.
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

### Normalized workload contract

Use `contract.check_workload_contract(records)` for caller-supplied normalized
records. Run its offline tests with:

```sh
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_contract.py
```

Every event and barrier must carry the same nonempty `run_id`, `source_sha`,
`source_tree`, `build_sha`, `binary_sha256`, `config_sha256`, `harness_sha256`
and `cluster_epoch` strings. Source commit/tree use full lowercase Git SHA1;
binary/config/harness digests use lowercase SHA256; epoch is a decimal string.
`build_sha` is a caller-supplied build label, independent of the binary digest.

The `IDENTITY_SEAL` lists complete identities with `node`, `boot_id`, positive
integer `pid`, nonnegative integer `procno`, `backend_start`,
`process_incarnation`, `sequence_domain` and `role`. All other identity fields
are nonempty strings. Events and every barrier snapshot repeat the complete
identity and role. A capture has one boot per node and one role per process.
Different backend starts or incarnations keep independent sequence prefixes
even when PID/procno are reused. Each stream begins at sequence 1; a gap remains
`ROOT_OPEN` and cannot borrow another incarnation's prefix. The fixture in
`test_contract.py` demonstrates the ordered barriers, disjoint lane objects,
supplied causal edges and final STOP state.

The result preserves capture and sequence declarations with
`capture_binding=DECLARED_ONLY`, `identity_binding=DECLARED_ONLY`,
`field_root=OPEN` and `ready_to_run=false`. A focused diagnostic PASS means the
supplied records satisfy the checks; it does not attest their origin or prove
real SQL, transport, authority, durability or shutdown. Raw LOG/projection and
SQL-journal outputs still require a separately qualified adapter; inventory
PID candidates cannot fill absent boot, start, incarnation or causal fields.

### Read/image and HOT SQL operations

`WorkloadSql.pm` uses four existing persistent `query_safe` sessions for node
IDs 0 through 3, plus a separate `hot_reader` session on node 2, in one fresh
shared database. Supply five distinct dedicated idle sessions
with autocommit enabled, superuser access and finite query timeouts. The
module does not form a cluster, open connections or enable trace settings.

```perl
use WorkloadSql; # add this pre2 directory to @INC
my $driver = WorkloadSql->new(
    sessions => \@sessions,
    hot_reader => $hot_reader, # another backend on node 2
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
The module tries one `ROLLBACK` for each reader it opened and any H writer or
Q transaction without a COMMIT acknowledgement. It preserves original SQL, cleanup and
journal errors together, and leaves all three relations for inspection.
A failed connection can prevent cleanup acknowledgement; this is a failure,
and the caller must retain it. A failed COMMIT has an unknown outcome; a later
ROLLBACK attempt does not prove it was undone. COMMIT is never retried, and an
acknowledged commit is recorded before journal emission can fail. The same
driver object cannot be run twice.

Node 0 mutates `r_img`, read by node 1; node 2 mutates the separate `h_hot`,
read by the additional node 2 backend. The module validates all five SQL
identities before creating tables, including the H reader's node, matching
postmaster birth and distinct PID, and checks for session replacement at the
end. Both readers hold read-only repeatable-read transactions.
The module retains each exported snapshot's actual file contents, validates
its cluster SCN/epoch and backend/database identity, and uses that SCN for
`cluster_cr_test_target_image()`. This superuser-only diagnostic accepts only
the target API's FULL/NONE result; refusal, deferral and exceptions fail the
comparison without a local current-page read, own-instance fallback or retry.
Its catalog entry requires a fresh homogeneous initdb; it is unavailable in
older installed catalogs. The existing `cluster_cr_test_image()` continues to
exercise own-instance construction for its original tests.

The module compares all visible `(id,v)` rows, retaining
duplicates, across the baseline, ordinary read and FULL reconstruction over
all relation blocks. The diagnostic returns raw NORMAL tuples, including their
physical offsets; it is not a complete MVCC visibility scan or proof of remote
transport, authority or protocol completion. H runs three nonindexed-column
updates, an indexed key change and a delete in one writer transaction, then
VACUUM after its COMMIT acknowledgement. Its reader takes the baseline with
`SET LOCAL cluster.cr_tuple_level_fastpath = off`, then compares reads with
the setting on and off in the same held snapshot, followed by the target
FULL diagnostic. A setting error or disagreement fails the attempt. These
inputs do not guarantee fast path eligibility or execution. Actual HOT/prune/index-delete decisions
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
five dedicated sessions and bound its duration. A hook exception fails the
attempt and does not retry the hook or COMMIT.

The journal retains SQL bytes, raw successful output, hashes, SQL identities
and client-local sequences. `node` remains the server node ID; `client_session`
identifies slots 0 through 3 and the extra H reader in slot 4. Each slot has its
own client sequence, including the two node 2 sessions. Client sequences are not producer trace sequences;
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
| root_observation | Current selected `system_identifier`, `root_digest`, `generation`; native SQL independently checks sysid, the exact `CATALOG_VERSION_NO` from this source tree's `src/include/catalog/catversion.h`, and the shared settings on every node |
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
