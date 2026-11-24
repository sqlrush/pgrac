# Verdict resolver measurement module

This opt-in module calls the server's exported durable TT scan and verdict
core from a backend. It is not built by `make check` or installed by default.
Use a disposable, local, single-node data directory. All SQL functions require
a superuser. Provisioning creates undo segment files; do not run it on an
application database.

Build against an existing cluster-enabled build, then install:

```sh
make -C /path/to/build/src/test/modules/test_pgrac_verdict_bench install
```

On macOS, optionally compile the independent libc observer against that build:

```sh
PG_CONFIG=/path/to/install/bin/pg_config
cc -O2 -Wall -Wextra -Werror -dynamiclib \
  -I"$("$PG_CONFIG" --includedir-server)" -I"$("$PG_CONFIG" --includedir)" \
  -o /tmp/vb_interpose.dylib vb_interpose.c
```

Run latency measurements **without** the observer. Use a new `VB_TAG` for every
run; the driver refuses an existing data directory, uses a private socket
directory, and checks the new postmaster PID and server data directory before
writing. The server is stopped on
normal completion or script failure; data and logs are preserved.

```sh
VB_INSTALL=/path/to/install VB_WORK=/path/to/disposable/results \
  VB_TAG=timing VB_PORT=54739 VB_ROUNDS=3 VB_ITERS=2000 \
  VB_COLD=0 VB_PARALLEL=0 bash run_bench.sh
python3 vb_summarize.py /path/to/disposable/results/results-timing.jsonl
```

For call counts, use a separate run with `VB_INTERPOSE=/tmp/vb_interpose.dylib`
and fewer iterations. Then run `vb_check_counts.py` on its JSONL. The driver
starts postgres directly so macOS does not strip the preload variable through
a protected system launcher. A null observer is unavailable, not zero I/O.

`libc[0:6]` contains undo pread calls, returned bytes, successful opens, failed
opens, closes, and access probes. Slots 6–7 count all-descriptor preads and
all-path opens. Slots 8–12 are wrapper durations for undo pread/open success/
open failure/close/access. Slots 13–15 count undo preads whose actual backend
LWLock list was inspected, those with a held lock, and unavailable inspections.
No observer lock is acquired. The `control/scan_lock_control` rows deliberately
hold `XactTruncationLock` shared around the real scan; they must show held locks
and are excluded from hot latency summaries. This does not observe spinlocks,
heavyweight locks, lock waits before I/O, or other backend processes.

Product smgr counters can include other processes; libc observations are
process-local. The serial coverage check requires their pread counts to agree;
do not apply it to overlapping loops. Observer timings are instrumented and
must not be substituted for uninstrumented latency.

Provisioning with `publish` uses product header creation/publication but does
not validate allocator admission. Xids come from real commit/rollback SQL;
the process-local stripe latch makes congruent xids resolve as own. A clean
restart exercises zero-match startup-bound results over empty TT headers.
This measures the verdict core, not the LMS queue, network, block handoff,
transaction-slot allocation, or a complete recovery/cluster-creation path.
The `clog_leg` mode is a separate decomposition aid and does not reproduce the
verdict core's ordering. Measurements must be bound to the current server build;
results from an earlier core implementation do not carry over.

Cold eviction is best effort. Check every eviction row's `failed` and
`resident_after` fields and the process disk counters before describing it as
a physical cold read. The summary excludes failed, missing, or incomplete
evictions; cache residency alone does not prove physical I/O.
Parallel/side-workload options measure client backends,
not LMS service throughput.
