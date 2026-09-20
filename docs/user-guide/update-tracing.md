# Short-window UPDATE tracing

Author: SqlRush <sqlrush@gmail.com>

`cluster.update_trace` records per-UPDATE execution phases and related
consistent-read service events. It defaults to `off`. Use it for short,
controlled diagnostics; leave it off for throughput qualification. It is an
observation facility, not a correctness checker or a statement timeout.

## Capture

Use a cluster administrator account. On each instance being diagnosed:

```sql
ALTER SYSTEM SET cluster.update_trace = 'on';
SELECT pg_reload_conf();
```

Open a fresh connection and check `SHOW cluster.update_trace` before starting
the workload. A session-only `SET` does not enable tracing in the background
processes that serve remote requests. Allow configuration reload to reach them.

Run the short diagnostic workload, then wait for its clients to finish
naturally. Stop capture on every instance:

```sql
ALTER SYSTEM SET cluster.update_trace = 'off';
SELECT pg_reload_conf();
```

Export each instance separately. Use a new output directory and keep these raw
files alongside the binary hash, source commit, configuration, client results,
and server logs:

```sh
psql -XqAt -v ON_ERROR_STOP=1 -h NODE_HOST -p NODE_PORT -d postgres \
  -c "SELECT coalesce(json_agg(s), '[]'::json) FROM pg_cluster_state s" \
  > node0-trace.json
```

Repeat for nodes 1–3 with their actual endpoints and distinct filenames.
Do not combine snapshots from different server starts into one capture.

## Analyze

From the source repository, using Python 3.9 or newer:

```sh
python3 scripts/perf/collect_update_trace.py \
  node0-trace.json node1-trace.json node2-trace.json node3-trace.json \
  -o update-records.jsonl
python3 scripts/perf/analyze_update_trace.py update-records.jsonl \
  -o update-report.json
```

The report ranks inclusive and exclusive execution phases. Inclusive times
overlap; do not add them together. Exclusive phase time plus unattributed time
must equal the recorded UPDATE execution time. COMMIT, client network time,
and client-side scheduling are outside that execution span.

`join_update_trace.py` additionally joins instrumented client attempts by
`node_id`, backend `pid`, and `backend_sequence`. Ordinary benchmark summary
CSVs do not contain this identity and cannot establish the client slowest-20%
cohort. Keep client errors and incomplete attempts; do not silently discard
unmatched slow operations.

## Capacity and interpretation

- Each instance retains at most 32,768 UPDATE records and 524,288 service
  events per server lifetime. New records are dropped when full; existing
  records are not overwritten. Toggling the setting does not reset the buffers.
- Check `record_capacity`, `reserved_count`, and `dropped_count` in the
  `update_trace` and `update_trace_event` categories. Dropped events, unfinished
  operations, or accounting errors limit attribution; they must not be hidden.
- A fresh capture after exhaustion requires a coordinated normal restart.
  Preserve data and logs; this is not an instruction to force-stop or recover
  an unclean cluster.
- Source-side slot residence overlaps requester waiting. Do not add source
  slot-seconds to client-seconds, subtract clocks from different instances,
  or translate a wait percentage directly into an expected TPS increase.
- Diagnostic capture failure does not make the database workload pass or
  fail. Client/server errors, full-row checks, health checks, and clean shutdown
  must still be assessed independently.
