# Cooperative Clean Leave — User Manual

> Status: introduced in linkdb stage 5.13.

## Overview

A **clean leave** lets a live, healthy cluster node leave the cluster *on
purpose* — for planned maintenance, scale-in, or a rolling restart — without
looking like a crash.

Before the node exits, it cooperatively hands everything it holds over to the
surviving nodes: its global lock grants, the lock-directory shards it masters,
its page locks, and its dirty pages (force-logged and flushed to shared
storage).  Only once the survivors have taken over and the leave is committed
does the node stop participating.  A surviving node then reads the just-flushed
current data from shared storage — never a stale copy.

This is the cooperative counterpart to a node *crash*: a crash leaves the
survivors to recover the dead node's state; a clean leave is an orderly handover
that completes before departure.  If a cooperative leave cannot finish (the
leaving node dies partway, or the deadline expires), the cluster automatically
falls back to ordinary crash recovery — a clean leave never weakens crash
safety.

Clean leave is **opt-in** and **off by default**.

## Configuration

| GUC | Default | Range | Context | Description |
|---|---|---|---|---|
| `cluster.clean_leave_enabled` | `off` | boolean | `POSTMASTER` | Enable cooperative clean leave on this node.  When `off`, `pg_cluster_clean_leave_request()` returns `rejected:feature_disabled` and the node never participates in a clean-leave handover. |
| `cluster.clean_leave_drain_timeout_ms` | `30000` | `[1000, 600000]` | `POSTMASTER` | Fail-closed deadline for the drain.  If the handover does not complete within this window, the leave is abandoned and the node falls back to crash recovery. |

To use clean leave, set `cluster.clean_leave_enabled = on` in `pgrac.conf` (or
`postgresql.conf`) on **every** node and restart, then issue the request below
on the node you want to remove.

## Requesting a leave

```sql
SELECT pg_cluster_clean_leave_request();
```

Superuser only.  Run it on the node that should leave.  The function returns a
short text status:

| Result | Meaning |
|---|---|
| `accepted` | The leave was accepted and is draining.  Watch `pg_cluster_clean_leave_state` for progress. |
| `rejected:feature_disabled` | `cluster.clean_leave_enabled` is `off` on this node. |
| `rejected:not_in_quorum` | This node is not currently in quorum and cannot leave cleanly. |
| `rejected:leave_in_progress` | A clean leave is already in progress on this node. |
| `noop:no_peer` | There is no surviving peer to hand off to (single-node, or all peers are down). |
| `rejected:marker_not_durable` | The intent could not be recorded durably on a voting-disk majority; the leave was not started. |

A mixed-mode cluster — where a surviving node has `cluster.clean_leave_enabled =
off` — cannot complete a cooperative handover.  The leave is accepted, then
cleanly aborted (no commit, no data movement) once a disabled survivor declines;
enable the feature everywhere first.

After the request returns `accepted`, poll the progress view until `phase`
reaches `committed`, then stop the node:

```sql
SELECT phase FROM pg_cluster_clean_leave_state;   -- wait for 'committed'
```

```
pg_ctl -D <datadir> stop
```

## Progress: `pg_cluster_clean_leave_state`

An always-one-row view of this node's leave progress (read-only; granted to
PUBLIC).  When the node is idle, `phase` is `idle` and `leaving_node_id` is `-1`.

| Column | Type | Description |
|---|---|---|
| `phase` | text | `idle`, `requested`, `quiescing`, `ges_draining`, `gcs_flushing`, `barrier_wait`, `committed`, `aborted`, `aborted_escalate`. |
| `leaving_node_id` | int4 | The node id that is leaving, or `-1` when idle. |
| `leave_epoch` | int8 | The membership epoch this leave is bound to. |
| `ges_drained_count` | int8 | Lock grants / shards drained. |
| `gcs_flushed_count` | int8 | Dirty pages flushed to shared storage. |
| `shards_remastered` | int8 | Lock-directory shards moved off the leaving node. |
| `survivor_ack_count` | int4 | Survivors that have acknowledged the handover. |
| `barrier_deadline` | timestamptz | Fail-closed deadline for the current leave, or `NULL` when idle. |
| `escalate_count` | int8 | Lifetime count of leaves that fell back to crash recovery. |

The membership-event kind also appears in `pg_cluster_reconfig_state`: a
committed clean leave surfaces there with `reconfig_kind = 'clean_leave'`.

## Error code

| Code | Name | Meaning | Retry |
|---|---|---|---|
| `53R62` | `cluster_clean_leave_in_progress` | A writable transaction on the leaving node was rolled back while the node drains for departure. | Reconnect to a surviving node and retry — retry is safe. |

In-flight **read-only** transactions on the leaving node are not affected; only
writable transactions are rolled back before the node departs.
