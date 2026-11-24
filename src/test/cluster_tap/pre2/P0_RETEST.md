# Targeted regression commands

Each command prepares or runs one SQL schedule on the normal four-member PRE2
fixture. It does not create, start or stop a VM. Obtain a qualified live entry
file using the existing [cohort controller](BLACKBOX.md#local-linux-quorum-controller)
before running a database case. The entry file must remain live for the whole
test. Use a fresh result directory and an independent candidate build.

Prepare the four commands on macOS or Linux without starting a database:

```sh
python3 src/test/cluster_tap/pre2/p0_5_retest.py prepare --output /new/path/history
python3 src/test/cluster_tap/pre2/p0_8_retest.py prepare --output /new/path/capacity
python3 src/test/cluster_tap/pre2/p0_10_retest.py prepare --output /new/path/image
python3 src/test/cluster_tap/pre2/p0_12_retest.py prepare --output /new/path/hot
```

Each output contains `preparation.json` and an executable `run.sh`. To execute
one prepared schedule:

```sh
P0_BUILD=/path/to/build \
P0_ENTRY_FILE=/path/to/live/entry.json \
P0_OUTPUT=/new/path/native-result /new/path/image/run.sh
```

The build contains `install/bin/{postgres,psql}` and
`src/test/regress/pg_regress`. The capacity case also needs the existing
`test_pgrac_shared_config` test module and a cassert build. The cohort requires
the remaining tools documented in BLACKBOX.md. Scheduling permission and
applicable runtime gates must be satisfied separately.

| Command | SQL schedule |
| --- | --- |
| `p0_5_retest.py` | Hold an old snapshot on node 0, retain the seed creator FullXid, execute 4096 ordinary autocommit updates on node 3, compare the old rows and the target FULL image. |
| `p0_8_retest.py` | Run the existing capacity cell: eight real writers plus a ninth UPDATE on one heap block, wait on exact observed owners, release those owners, reconcile all four members. Other capacity-suite cells are not selected. |
| `p0_10_retest.py` | Node 1 holds a snapshot while node 0 updates all 64 rows; compare ordinary SELECT and the target FULL image at the original read SCN. |
| `p0_12_retest.py` | A distinct reader on node 2 holds a snapshot over three HOT-candidate passes, indexed-key UPDATE, DELETE and VACUUM; compare tuple, ordinary FULL and target FULL results. |

All schedules preserve the first SQL error, client cleanup results, complete
member logs and normal-stop evidence. They do not retry failed transactions.
The capacity schedule retains its existing statement deadline and diagnostics.
The other schedules use a 90-second statement timeout. Output includes SQL
operations and identities in `log/p0-sql.jsonl`, or existing `m2-b07-*` evidence
for the capacity cell. Raw TAP is `tap.log`; the judgement is `judgement.json`.

The judgement also retains existing ordinary-TT failure observations in
`request_evidence`, grouped only when all original carrier fields match.
Missing or malformed fields remain ungrouped with their raw line. Grouping
does not establish a process incarnation or prove delivery or consumption.
For the capacity case, an existing `log/m2-b07-candidate-latest.json` export is
projected into `itl_evidence`: all eight page slots retain their actual indices,
and TT publications stay separate. A complete slot copy means only that its
exported fields are present and fit their byte widths. Producer identity stays
`UNKNOWN`; no TT wrap is inferred from an ITL wrap. Missing, malformed or
overflowed exports never supply original-path credit or change the SQL verdict.

Rejudge retained output without contacting a server:

```sh
python3 src/test/cluster_tap/pre2/p0_10_retest.py judge --output /path/to/native-result
```

`FAIL` retains SQL/server errors, incomplete TAP, missing member logs, failed
client exits and unproven shutdown. Missing case results are `NOT_REACHED`.
`WORKLOAD_PASS` means only that this bounded SQL schedule and its controls
completed. It does not prove the old transaction slot was reused, that a query
shipped a remote block, or that a specific HOT member or historical UNKNOWN
slot was exercised. `original_path=UNKNOWN` and `p0_closed=false` remain explicit
until that separate evidence is established. These commands do not replace a
long-duration acceptance run or a required block handoff measurement.

Offline checks:

```sh
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_p0_retest.py
python3 -m unittest discover -s src/test/cluster_tap/pre2 -p test_p0_retest_evidence.py
prove -v src/test/cluster_tap/pre2/test_p0_retest_sql.pl \
  src/test/cluster_tap/pre2/test_p0_retest_cleanup.pl
```
