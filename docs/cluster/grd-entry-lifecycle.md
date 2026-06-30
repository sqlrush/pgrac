# GRD Entry Lifecycle And Cold Reclaim

Status: spec-6.3a implementation note.

## Lock Model

GRD entries live in the partitioned GRD HTAB and are protected in this order:

1. shard LWLock
2. entry spinlock

There is no table-wide GRD entry lock in the hot create or reclaim path.
Create, lookup, and `HASH_REMOVE` take only the target shard LWLock.

Scan paths do not use `hash_seq_search` over the live HTAB. Each entry is
linked into a shard-local intrusive list while the shard LWLock is held. A
scanner takes one shard LWLock in shared mode, copies `ClusterResId` keys,
releases the shard, and later re-lookups each key through the normal
pin/release API. This keeps scan safety local to the shard and avoids
serializing unrelated entry churn.

## Pin Discipline

`cluster_grd_entry_lookup_or_create()` pins an entry before releasing the
shard LWLock. Callers must pair every successful lookup with
`cluster_grd_entry_release()`.

`cluster_grd_entry_release()` copies the `ClusterResId` before decrementing
the pin. After the decrement publishes `pin == 0`, release does not
dereference the old entry pointer. Last-unpin reclaim re-enters by copied
resource id, takes the shard LWLock in exclusive mode, revalidates
cold state under the entry spinlock, sets `RECLAIMING`, unlinks the entry
from the shard list, and then calls `HASH_REMOVE`.

Cold means `pin == 0` and no holders, waiters, converts, or reservations.
Entries with live state are never reclaimed.

## ERROR Cleanup Audit

Pinned windows are intentionally short. spec-6.3a classifies lookup sites as:

| Class | Sites | Cleanup rule |
|---|---|---|
| F | Snapshot walkers, cleanup sweeps, normal grant/release/convert mutators | The pinned window contains only fixed-size copies, spinlock-protected array mutation, atomics, and no allocation or visitor callback. External WFG refresh and SQL row visitors run after release. |
| T | Starvation fairness grant-barrier LMD submit/cancel while a pin is held | Wrapped by `grd_lmd_submit_wait_edge_pinned()` / `grd_lmd_cancel_wait_edge_pinned()`. `PG_CATCH` releases the entry pin and rethrows. |
| R | none in spec-6.3a | No long-lived GRD entry pin is registered in `ResourceOwner`. Future paths that keep a pin across arbitrary backend code must add ResourceOwner tracking or a local `PG_TRY` guard. |

The unit case `test_grd_pin_cleanup_on_lmd_submit_error` injects an ERROR
through the pinned LMD submit path and verifies the pin is not leaked.

## Tests

The cluster unit lifecycle suite covers paired pin/release, last-unpin cold
reclaim, periodic sweep reclaim, live-state exclusion, large sweep batches,
over-release fail-safe behavior, and the pinned LMD ERROR cleanup path.
