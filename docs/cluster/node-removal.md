# Permanent Node Removal — User Manual

> Status: introduced in linkdb stage 5.18.

## Overview

**Permanent node removal** decommissions a declared cluster node for good. It is
the finishing step after a node has already left — either by a cooperative
[clean leave](clean-leave.md) or by a crash (fail-stop) — and you have decided it
will not come back.

Removing a node does three things:

1. **Shrinks the membership.** The node's effective membership state becomes
   `removed`. Surviving nodes stop counting it as a member, stop expecting it in
   membership barriers, and never again treat its heartbeats as a returning
   member.
2. **Fences it.** A removed node is write-fenced on the shared storage: even if a
   stale instance of it comes back, it cannot write shared data, cannot fool a
   surviving node, and cannot passively rejoin.
3. **Cleans up after it cluster-wide.** Every lock-directory shard the removed
   node mastered is permanently reassigned to a surviving member, its global lock
   and page-lock leftovers are cleared, its voting-disk record is tombstoned, and
   the cluster verifies that zero references to it remain anywhere.

Removal is **opt-in** and **off by default**. It only works on a node that has
already left — you cannot remove a live, actively-serving node (you must clean-
leave or stop it first, so its in-memory data is not lost).

## Configuration

| GUC | Default | Range | Context | Description |
|---|---|---|---|---|
| `cluster.online_node_removal` | `off` | boolean | `POSTMASTER` | Enable permanent node removal on this node. When `off`, `pg_cluster_remove_node()` returns `rejected:feature_disabled` and no removal, fence, or cleanup runs. |
| `cluster.node_removal_cleanup_timeout_ms` | `30000` | `[5000, 120000]` | `SIGHUP` | Deadline for the post-shrink cluster-wide cleanup. If cleanup does not finish (all surviving members acknowledge and zero leftovers are proven) within this window, the removal enters a resumable `cleanup_blocked` state — it is never reported complete until cleanup actually succeeds. |

To use node removal, set `cluster.online_node_removal = on` in `pgrac.conf` (or
`postgresql.conf`) on the surviving nodes and restart, then issue the request
below on a surviving node.

## Removing a node

```sql
SELECT pg_cluster_remove_node(<node_id>);
```

Superuser only. Run it on a **surviving** node (not the node being removed). The
function returns a short text status:

| Result | Meaning |
|---|---|
| `accepted` | The removal was accepted and is running. Watch `pg_cluster_node_removal_state` for progress until `phase` reaches `committed`. |
| `noop:already_removed` | The node was already permanently removed. |
| `resume:cleanup_pending` | The node is already shrunk out and fenced, but its cleanup had not finished; the request resumes and completes the cleanup. |
| `rejected:feature_disabled` | `cluster.online_node_removal` is `off` on this node. |
| `rejected:cannot_remove_self` | You cannot remove the node you are connected to. |
| `rejected:not_declared` | `node_id` is not a declared cluster node. |
| `rejected:node_not_drained` | The node is still active and has not left. Clean-leave or stop it first — removal never force-evicts a live node. |
| `rejected:not_in_quorum` | This node is not currently in quorum. |
| `rejected:removal_in_progress` | A different removal is already in progress. |

After the request returns `accepted`, poll the progress view until `phase`
reaches `committed`:

```sql
SELECT phase FROM pg_cluster_node_removal_state;   -- wait for 'committed'
```

## Progress: `pg_cluster_node_removal_state`

An always-one-row view of the removal in progress (read-only; granted to PUBLIC).
When idle, `phase` is `idle` and `target_node_id` is `-1`.

| Column | Type | Description |
|---|---|---|
| `phase` | text | `idle`, `requested`, `precheck`, `fence_arming`, `shrink_committing`, `cleanup`, `cleanup_blocked`, `committed`, `aborted`, `aborted_escalate`. |
| `target_node_id` | int4 | The node being removed, or `-1` when idle. |
| `coordinator_node_id` | int4 | The surviving node driving the removal. |
| `remove_epoch` | int8 | The membership epoch the removal is bound to. |
| `fence_armed` | bool | The removed node's write fence is majority-durable. |
| `membership_shrunk` | bool | The membership has been shrunk (the node is a non-member). |
| `grd_cleaned` | bool | Lock-directory remaster + cleanup is done. |
| `pcm_cleaned` | bool | Page-lock cleanup is done. |
| `ack_count` | int4 | Surviving members that acknowledged the cleanup. |
| `deadline_us` | int8 | Cleanup deadline, or `NULL` before cleanup starts. |
| `removal_committed_count` | int8 | Lifetime count of completed removals. |
| `cleanup_blocked_count` | int8 | Lifetime count of cleanups that hit the deadline and resumed. |
| `leftover_detected_count` | int8 | Lifetime count of leftover-reference detections (fail-closed). |
| `zombie_write_rejected_count` | int8 | Lifetime count of write attempts rejected from removed nodes. |

A committed removal also surfaces in `pg_cluster_reconfig_state` with
`reconfig_kind = 'node_removed'`, and in `pg_cluster_membership` the node's row
shows `state = 'removed'`, `removed = true`, and a non-zero `removed_epoch`.

## Error codes

| Code | Name | Meaning | Retry |
|---|---|---|---|
| `53R63` | `cluster_node_removal_in_progress` | A writable transaction was rolled back while a removal epoch was publishing. | Retry — retry is safe on the new epoch. |
| `53R64` | `cluster_node_removed_fenced` | A removed node tried to serve or rejoin the cluster. | **No.** The node must be re-admitted by an operator before it can return (see below). |
| `53R51` | `cluster_write_fenced` | A removed (fenced) node tried to write shared storage. | The write is refused; the node is no longer a member. |

## A removed node does not come back automatically

This is the most important operational point. **In this version, a removed node
cannot rejoin on its own.** Restarting it, or reconnecting it to the cluster,
does not bring it back — it stays fenced and is refused (`53R64`). Bringing a
removed node back is a deliberate operator action (un-fencing it) that is **not
provided as a command in this version**; it is a future operational procedure.

A single `join` is therefore only half of what a return would require — the node
must first be explicitly un-fenced. This is intentional: a removed node is treated
as gone until an operator decides, out of band, that it is safe to return.

## Production note: external fencing

The write fence applied by node removal is a **cooperative** fence: it relies on
the removed node's own storage and startup paths to honor it. For a node that is
healthy and correctly configured, that is sufficient — it will not write shared
data after removal.

For a hard guarantee against a malfunctioning or malicious instance (one that
does not honor the cooperative fence), a production deployment should pair node
removal with an **external fencer** at the node level (for example STONITH, IPMI
power control, or a cloud power/network API). External fencing is outside the
database and is configured by your cluster operations tooling.
