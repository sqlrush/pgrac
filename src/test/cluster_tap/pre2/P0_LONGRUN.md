# Four-member long-run driver

`p0_longrun.py` runs one command per result directory: create and start the
normal four-member PRE2 fixture on one Linux development VM, apply a shared
UPDATE workload for a chosen time, collect every server and client error,
stop all members normally, and judge the retained evidence. It repeats this
for each requested cycle. It does not create, start or stop a VM, does not
reserve a machine, and does not satisfy any separate scheduling requirement.

## What a cycle does

1. Fresh cohort through the existing [local quorum controller](BLACKBOX.md#local-linux-quorum-controller)
   (`initdb --pgrac-initdb-cohort`, three freshly formatted voting devices),
   start, and wait for every member to report OPEN.
2. Node 0 creates `lab.stop_load(id int PRIMARY KEY, v bigint NOT NULL)` with
   131072 rows; every member reads it back.
3. Each member gets N persistent connections. Every connection repeats
   `BEGIN; UPDATE lab.stop_load SET v=v+1 WHERE id=<random>; COMMIT;` with no
   retry and no pause. A failed transaction is recorded with its SQLSTATE and
   server fields and the connection continues; a lost connection is recorded
   and reopened.
4. Every member reads the whole table; each row is compared with the commit
   ledger (acknowledged commits must be present, nothing else may be).
5. First cycle only: create a small table, read it on every member, `DROP` it.
6. Normal (fast) stop of all members, then the native CLOSED observation.

Server settings default to `autovacuum = off`, `shared_buffers = '16MB'` and
`log_error_verbosity = verbose`; `--conf 'name = value'` adds or replaces one.

## Run

Build the candidate on the VM first; `--install` is its installation
(`bin/`, `lib/`, `include/server`). Run as root; the database runs as `--uid`
(default 1000), which needs passwordless `sudo`.

```sh
D=src/test/cluster_tap/pre2
# 1. Read-only prerequisite check; creates nothing.
sudo python3 $D/p0_longrun.py run --install /path/to/install \
    --output /new/result/dir --clients 4 --minutes 10 --preflight-only
# 2. One 10-minute cycle, 4 connections per member.
sudo python3 $D/p0_longrun.py run --install /path/to/install \
    --output /new/result/dir --clients 4 --minutes 10
# 3. Three cycles of 60 minutes with 2, 4 and 8 connections per member.
sudo python3 $D/p0_longrun.py run --install /path/to/install \
    --output /new/result/dir2 --clients 2,4,8 --minutes 60
```

| Option | Meaning |
| --- | --- |
| `--clients L` | Connections per member (1..16): one value, or one value per cycle |
| `--cycles N` | Number of cycles when `--clients` has one value |
| `--minutes M` | Workload time per cycle (default 60) |
| `--vendor DIR` | Root of the unpacked Corosync packages (default `/`) |
| `--vote-formatter F` | Existing `native_vote`; built into the result directory if omitted |
| `--conf 'name = value'` | Extra server setting, repeatable |
| `--no-drop-step` | Omit step 5 |
| `--restart-verify` | After the normal stop: start the same DATA, repeat step 4, stop again |
| `--crash-cycle N` | In cycle N, SIGKILL every server process under load instead of draining the clients, then start the same DATA, repeat step 4 and stop. Default 0: not run |
| `--stall-abort-seconds S` | End the workload early if no transaction completes for S seconds (default 300, 0 disables); such a cycle fails |
| `--grace-seconds S` | Time connections get to return after the workload time (default 60); a connection still blocked is reported as hung |
| `--allow FILE` | Explicit allow-list, see below. Default: none |
| `--expect-postgres-sha256 H` | Refuse to start unless the installed `postgres` has this digest |

Ten minutes means workload time per cycle; initialization, seeding,
reconciliation and shutdown add time. `--minutes 10 --cycles 3 --clients 4`
requests three ten-minute workloads and three normal stops. One ten-minute
cycle does not establish three-round acceptance. A missing or failed third
stop cannot be replaced by two earlier successes.

The exit status is 0 for PASS, 1 for FAIL and 2 for BLOCKED (a prerequisite
is missing; nothing about the database was exercised).

## Result

| File | Contents |
| --- | --- |
| `SUMMARY.md` | One page, in Chinese: verdict, per-cycle commits per node, error counts per label, unlabelled errors (first 20), stop results |
| `result.json` | The same, complete and machine readable |
| `identity.json`, `plan.json`, `preflight.json` | Binary digests, arguments, cycle plan, prerequisite checks |
| `cycleN/evidence/steps.json` | Step results and each member's log offset at every step boundary |
| `cycleN/evidence/client-errors.jsonl` | Every client error: member, connection, step, statement, SQLSTATE, message, detail, source location, backend PID |
| `cycleN/evidence/load.json`, `ledger-nodeK.txt`, `progress.jsonl` | Per-member counts, hung connections, the commit ledger, 10-second progress samples |
| `cycleN/evidence/verify.json` | Per-member reconciliation with the ledger |
| `cycleN/cluster/` | DATA, shared files, WAL and the four server logs, retained |

## Verdict

The rule is fixed. A run passes only if every planned step of every cycle
passed, every member committed transactions, no connection hung, every
reconciliation matched, every normal stop produced the CLOSED observation
without a forced cleanup, and there is **no** server `ERROR`/`FATAL`/`PANIC`,
no abnormal process exit and no client error anywhere in the run. Errors that
the driver's own crash step causes on the client connections are counted
separately and are the only exception.

The retained files must support that verdict: a passed workload needs its
load report and every member's ledger; requested reconciliations, client
error and progress files must be present. Member identities must cover the
whole cohort exactly once, logs must reach their recorded end, and measured
workload time and connection counts must agree with the plan. Missing,
damaged or contradictory evidence fails the run even when the step journal
says PASS. A malformed client record fails and does not hide later records.

Each error is given a label from `p0_longrun_rules.py` when one of its rules
matches: first by quoted message text, then by declared SQLSTATE, then by the
step in which a severe record appeared. Labels only sort the report. An error
that matches no rule is listed under "未分类错误" with its original text.

Unclassified server and client errors always fail, including records matched
by an explicit allow-list. An allow-list makes only named, classified errors
non-failing. Without one, nothing is allowed. `PANIC`, abnormal process exits and wrong client answers can never
be allowed, and a pattern that matches arbitrary text is refused.

```json
{"version": 1, "entries": [
  {"id": "short-name", "reason": "why this is expected", "message": "regex on the message",
   "sqlstate": "57014", "phases": ["load"], "sources": ["server", "client"],
   "severities": ["ERROR"]}]}
```

`id`, `reason` and `message` are required; `severities` defaults to `ERROR`.

## Not covered

- Continued service after one member is isolated needs several hosts and real
  fencing; the driver reports it as not implemented.
- The capacity cases that need many idle open transactions or many writers on
  one page are separate directed tests; the driver reports them as not run.
- The crash step is off unless `--crash-cycle` is given; it is then reported
  as passed or failed like any other step.

## Without the VM

```sh
# Rejudge a retained result directory, e.g. after the rule table changed.
python3 $D/p0_longrun.py judge --run /retained/result/dir
# Unit tests of the driver, including the retained log sample.
python3 -B -m unittest discover -s $D -p '*p0_longrun*.py' -v
# Whole driver path against one ordinary local instance (not an acceptance
# result; verdict SELFTEST_OK or SELFTEST_FAILED).
python3 $D/p0_longrun.py selftest --install /path/to/install --output /new/dir
```

`selftest` accepts `--crash`, `--restart-verify` and `--inject-error` (every
fifth row fails, to see the judge refuse the run).

## Interrupted runs

On interruption or watchdog expiry the controller first terminates and reaps
its cycle process, then invokes the existing identity-checked member cleanup.
This is recorded as failure, never as a normal CLOSED stop. If process exit
or member cleanup cannot be confirmed, the quorum fixture is retained and
`controller.json` records why; its namespaces, service processes and devices
need owner cleanup before another run. Do not remove those resources under a
surviving database.
