# spec-6.13 RDMA Tier3 And Block-Reply Zero-Copy

## Status

spec-6.13 extends the existing spec-6.1 RDMA transport from a generic
verbs mux into a tier3-optimized block-shipping path.  The scope is now
explicitly expanded to include receiver-side GCS block-reply direct-land;
that work remains in this spec and is not split into a later spec.

| Area | Status | Notes |
|---|---:|---|
| tier3 provider selection and mlx5dv probing | shipped | `tier3` and `provider=mlx5` prefer mlx5dv and obey the configured fallback policy. |
| inline send and unsignaled batching | shipped | Small frames may use `IBV_SEND_INLINE`; signaled completion cadence is bounded by `cluster_ic_rdma_signal_batch_k()`. |
| bounded busypoll completions | shipped | `cluster.interconnect_rdma_completion=busypoll` drains CQ within `cluster.interconnect_rdma_busypoll_us`. |
| sender-side live shared_buffers SGE | shipped | Non-destructive GCS block replies may raw-pin a stable page and send it as a registered local SGE. |
| scratch SGE and TCP fallback | shipped | Scratch MR remains the RDMA fallback, and TCP contiguous envelope fallback remains fail-safe. |
| tier3/GCS observability | shipped | `pg_stat_cluster_ic` and `dump_cluster_state` expose the new counters. |
| direct-land wire flag and constants | partial | Request/forward flags and sidecar size constants are reserved and unit-tested. |
| receiver-side direct-land data path | planned in 6.13 | Requires a dedicated block-reply lane, two-SGE receive, slot correlation, and abort cleanup. |
| soft-RoCE direct-land TAP | planned in 6.13 | Required before this spec is fully shipped. |

## Goals

- Keep TCP and generic RDMA envelope semantics intact for all non-block-reply
  messages.
- Reduce sender-side block shipping copies by using live shared_buffers SGE
  where safe.
- Add receiver-side direct-land only on a dedicated block-reply lane so an
  unrelated generic message can never DMA into a target buffer page.
- Preserve the existing GCS verification discipline: checksum, epoch,
  transition id, requester identity, and slot generation must be validated
  before a direct-landed page is made visible.
- Keep all new RDMA optimizations fail-closed with explicit fallback to scratch
  SGE or TCP where correctness permits.

## Non-goals

- No RDMA WRITE/RDMA READ data path in spec-6.13.  Block images use SEND/RECV.
- No CRC offload.  Application CRC32C remains mandatory.
- No direct-land on the generic interconnect QP.  The generic QP remains a
  single logical envelope lane.
- No direct-land for Smart Fusion reply-v2 or destructive X-transfer until
  their ownership/drop-before-send rules have a separate proof.

## Existing Sender-Side Design

GCS block replies now choose payload source in this order:

1. Live shared_buffers SGE when the reply is non-destructive, the peer is RDMA,
   shared_buffers is registered, and the target page can be raw-pinned after WAL
   flush plus HC89 revalidation.
2. Per-peer scratch MR when live SGE is disallowed or cannot be borrowed.
3. Contiguous copy/TCP fallback when RDMA SGE cannot be used.

The live SGE path never holds a buffer content lock across asynchronous RDMA
completion.  The raw pin prevents buffer reuse; block-image CRC and envelope
verification remain the fail-closed boundary for post-revalidation page drift.

## D6 Direct-Land Expansion

### Why The Generic QP Cannot Be Reused

The current generic RDMA QP has one receive lane and posts one receive SGE.
RC SEND consumes the next posted receive WR in FIFO order without looking at the
future envelope payload.  If a target buffer page were posted on that generic
lane, a heartbeat, GES message, GCS control message, or stale block reply could
consume it before GCS verifies `request_id`.  That would DMA unverified bytes
into shared_buffers.  Therefore direct-land must use a dedicated block-reply
lane whose posted receives are consumed only by block-reply SENDs.

### New Functional Items

| ID | Function | Required behavior |
|---|---|---|
| D6.1 | Block-reply lane/QP | Create a dedicated per-peer block-reply RDMA lane separate from the generic envelope QP.  It may be a second RC QP or an equivalent lane with independent posted receives and CQE classification. |
| D6.2 | Two-SGE receive | Post one WR with SGE0 = direct-land sidecar/header quarantine and SGE1 = target page or staging page.  The QP capability must advertise `max_recv_sge >= 2` for this lane. |
| D6.3 | Slot correlation | Encode enough identity in `wr_id` and sidecar to bind a completion to `(backend_id, request_id, slot_generation, peer_node)`.  Completion must reject stale or mismatched slots. |
| D6.4 | LMON arm-before-send handoff | Backend block request enqueue must ask LMON to arm the direct-land receive before the request is sent to the master/holder.  If arming fails, the request must clear the direct-land flag and use the existing copy path. |
| D6.5 | Sender capability gate | Sender may use the direct-land reply format only when the request/forward flag says the requester armed a direct-land receive and the peer negotiated block-reply lane support. |
| D6.6 | Completion verifier | On direct-land CQE success, validate sidecar, envelope fields, checksum over the landed page, epoch, transition id, requester identity, and slot generation before setting `reply_received`. |
| D6.7 | Abort cleanup | CQE error, peer down, timeout, checksum failure, stale reply, or backend cancel must unpin/release the target, clear the armed slot, increment abort counters, and leave the buffer invisible. |
| D6.8 | Fallback semantics | Any arming or capability failure falls back to scratch/copy/TCP.  Any post-DMA verification failure is fail-closed, not fallback-installed. |
| D6.9 | Observability | Increment direct-land success/abort counters and expose lane state/error reason in RDMA debug output or `pg_stat_cluster_ic`. |
| D6.10 | Tests | Add unit tests for sidecar/slot identity and state transitions; add soft-RoCE TAP for direct-land success, stale slot rejection, checksum failure, peer disconnect, and fallback. |

### Direct-Land Sidecar

The direct-land receive sidecar is fixed at
`CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES` and precedes the 8 KB block image.
It is quarantine metadata, not trusted state.  It must carry or authenticate:

- envelope magic/version/message type/source/destination/epoch/payload length;
- `GcsBlockReplyHeader`;
- requester backend id and request id;
- slot generation;
- sending peer node id;
- checksum for the landed page.

The exact field packing must preserve the existing 84-byte constant unless the
constant, unit test, and compatibility note are updated together.

### Request/Forward Flags

`GcsBlockRequestPayload.reserved_0[1]` means the original requester armed a
direct-land receive.  Forwarded holder requests carry the same intent in the
forward payload flag already reserved for spec-6.13.  A sender must treat the
flag as advisory and also check RDMA peer capability before choosing the
direct-land reply format.

### Receiver State Machine

| State | Meaning | Exit |
|---|---|---|
| `UNARMED` | Slot uses existing staging-copy reply path. | Request may proceed normally. |
| `ARMING` | Backend has reserved a slot and asked LMON to post the direct-land receive. | `ARMED` on post success, `UNARMED` on arming failure. |
| `ARMED` | Receive WR is posted on the block-reply lane; request/forward may advertise direct-land. | `LANDED`, `ABORTED`, or timeout. |
| `LANDED` | CQE succeeded and bytes are in the target/staging memory, but not trusted. | `INSTALLED` after verifier succeeds, `ABORTED` otherwise. |
| `INSTALLED` | Page passed verification and was made visible to the waiting backend. | Slot release. |
| `ABORTED` | Any error path released resources and marked the attempt unusable. | Retry without direct-land or propagate existing GCS error. |

### Resource Ownership

- Backend owns the GCS outstanding slot and wait condition variable.
- LMON owns RDMA QPs, CQs, and posted receives.
- The direct-land arm record bridges the two with an explicit slot generation.
- The buffer raw pin or staging page reference is held from successful arming
  until verifier success or abort cleanup.
- Backend exit must cancel any armed receive or mark it orphaned so LMON drops a
  later CQE without installing bytes.

### Verification Rules

Direct-land completion may set `reply_received` only after all checks pass:

- CQE status is success and byte length matches sidecar + block size.
- `wr_id` maps to a live armed slot with matching generation.
- Sidecar peer/source/destination matches the RDMA peer and local node.
- `request_id`, `requester_backend_id`, `transition_id`, and epoch match the
  outstanding slot.
- GCS block checksum over landed bytes matches the reply header.
- Existing lost-write and page-LSN/SCN checks remain in force.

Failure of any check increments `direct_install_abort_count`, releases the arm
record, and never installs the landed page.

## Observability

Already shipped counters:

| Counter | Meaning |
|---|---|
| `tier3_send_count` | RDMA sends under tier3 accounting. |
| `inline_send_count` | Sends posted with inline payload. |
| `unsignaled_batch_count` | Unsignaled send decisions. |
| `busypoll_us_burned` | Time spent in bounded busypoll. |
| `busypoll_fallback_count` | Busypoll loops that exhausted the configured budget. |
| `scratch_copy_count` | GCS block payload copied into scratch/copy storage. |
| `live_sge_send_count` | GCS live-page SGE payload accepted by the send helper. |
| `live_sge_fallback_count` | Live SGE was requested but fell back. |
| `install_copy_count` | Receiver installed from the normal staging copy path. |

Direct-land counters to make live in D6:

| Counter | Meaning |
|---|---|
| `direct_install_count` | Verified direct-land page installed. |
| `direct_install_abort_count` | Direct-land arm or completion failed after resources were reserved. |
| `block_reply_lane_fallback_count` | Direct-land requested but lane/capability/arming was unavailable. |
| `block_reply_lane_error_count` | Block-reply lane CQE or provider error. |

## Acceptance

The spec is fully shipped only when all of the following pass:

- `make -C src/test/cluster_unit check`;
- unit coverage for D6 sidecar packing, slot generation rejection, and state
  transitions;
- soft-RoCE TAP proving direct-land success and fallback;
- soft-RoCE TAP proving stale slot, checksum failure, and peer disconnect are
  fail-closed;
- manual or CI artifact showing `direct_install_count` increments on a valid
  RDMA block reply and `install_copy_count` does not increment for that reply.

## Implementation Order

| Step | Work |
|---|---|
| 1 | Add block-reply lane structs, capability negotiation, shmem stats, and lifecycle cleanup. |
| 2 | Add two-SGE receive post API and CQE demux for the block-reply lane. |
| 3 | Extend GCS outstanding slot with direct-land arm state, generation, and release hooks. |
| 4 | Add LMON arm-before-send handoff and fallback to normal request when arm fails. |
| 5 | Add sender-side direct-land reply choice gated by request flag and lane capability. |
| 6 | Add completion verifier and install/abort state transitions. |
| 7 | Add unit tests, soft-RoCE TAP, docs/reference updates, and final full test run. |
