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
| direct-land wire flag and constants | shipped | Request/forward flags, sidecar size, WR id packing, and status whitelist are unit-tested. |
| receiver-side direct-land data path | shipped | Dedicated block-reply lane, two-SGE receive, slot correlation, LMON arm-before-send, verifier, and abort cleanup are implemented. |
| soft-RoCE direct-land TAP | shipped opt-in | `t/334_ic_rdma_soft_roce.pl` covers tier3 block-reply lane connection and direct-land success when run with `PGRAC_RUN_RDMA_SOFT_ROCE=1` on Linux rxe. |

Ship evidence for the code-bearing head is the GitHub nightly full CI
workflow-dispatch run
`https://github.com/sqlrush/pgrac/actions/runs/28702365954`, completed
successfully on 2026-07-04 for commit
`3755f1f814a09755238af1c6b00dc8a38b7a2466`.  Local evidence also included
object compilation plus `make -C src/test/cluster_unit check`.  The soft-RoCE
TAP is an opt-in hardware environment test and skips outside a Linux rxe
runner.

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

### D6.1 Block-Reply Lane/QP

The block-reply lane is a per-peer RDMA lane used only for
`PGRAC_IC_MSG_GCS_BLOCK_REPLY` direct-land attempts.  It is intentionally not
part of `ClusterICOps.recv_bytes()` and is never used by the generic envelope
dispatcher.

Each `ClusterICRdmaPeer` grows a second logical lane:

| Field | Meaning |
|---|---|
| `block_reply_qp` | Dedicated RC QP, or equivalent provider lane, for direct-land block replies. |
| `block_reply_connected` | True only after the peer negotiated and established the block-reply lane. |
| `block_reply_max_recv_sge` | Provider-reported receive SGE capacity; must be at least 2. |
| `block_reply_send_busy` | SEND backpressure for direct-land replies. |
| `block_reply_arm_head/tail` | LMON-owned arm table or freelist for posted receives. |
| `block_reply_last_error` | Human-readable reason for the last lane fallback/error. |

The generic QP remains unchanged:

- generic receive WRs keep one SGE and feed `rdma_process_recv_completion()`;
- generic CQEs are still demuxed by SEND/RECV plus peer;
- generic traffic may not consume block-reply posted receives;
- block-reply CQEs may not enter `rdma_dispatch_pending_frames()`.

Lane setup follows the same RDMA CM peer lifecycle as the generic QP.  If the
provider cannot create a second QP, or the created QP reports
`max_recv_sge < 2`, the peer remains RDMA-capable for generic traffic but
`block_reply_connected=false`; D6 direct-land requests for that peer fall back
before the request is sent with the direct-land flag.

### D6.2 Two-SGE Receive

A direct-land receive WR has exactly two SGEs:

| SGE | Address | Length | Trust level |
|---|---|---:|---|
| SGE0 | LMON-owned sidecar buffer | `CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES` | Quarantine metadata, untrusted until verified. |
| SGE1 | Reserved target page | `BLCKSZ` | Untrusted bytes until verifier succeeds. |

SGE1 must never point at a currently visible valid shared buffer page.  The
allowed targets are:

| Target kind | Use | Install rule |
|---|---|---|
| Reserved invalid shared buffer | Preferred zero-copy target.  The requester reserves/pins the buffer for `BufferTag`, keeps it not valid and not visible, and lets RDMA DMA into its page memory. | Verifier marks the page valid/installed only after all checks pass. |
| Registered staging page | Safety fallback when a final buffer cannot be reserved before the request. | Verifier copies into the final buffer through the existing install path; this increments copy counters and is not counted as direct install. |

The first D6 implementation should target the reserved invalid shared-buffer
case for normal read misses.  It must keep the existing staging-copy path for
cases that cannot prove exclusive ownership of the target page.

Implementation constraint: while RDMA DMA is possible, a reserved
shared-buffer target must stay pinned, invalid, and absent from any visible
lookup/install path.  The verifier may set it valid only while holding the
proper buffer/content locks used by the existing install path.  Abort cleanup
must not release or recycle the target until the posted WR is known completed
or the block-reply lane has been flushed/reset.

### D6.3 Slot Correlation

The receive CQE must map to a single live outstanding GCS slot without scanning
all backends.  The correlation has three layers:

| Layer | Carries | Purpose |
|---|---|---|
| `wr_id` | WR type `BLOCK_REPLY_RECV`, arm table index, generation bits | Fast LMON CQE demux to an arm record. |
| Arm record | Full `backend_id`, slot index, request id, slot generation, expected peer, target pointer | Authoritative local ownership and cleanup state. |
| Sidecar/reply header | Envelope source/destination, reply request id, requester backend id, transition id, epoch, checksum | Wire identity echoed by the sender and verified against the slot. |

The `wr_id` does not need to pack the full backend/request identity.  It should
pack a bounded arm-table index plus enough generation bits to reject stale CQEs
before following the arm pointer.  The arm record then stores the full identity.
The D6 wire layout assigns 16 bits to `arm_id` and 16 bits to generation.  The
slot generation must therefore be the same 16-bit wire generation used in
`wr_id`: value 0 is reserved, and the sequence wraps `65535 -> 1`.  Startup
must fail closed or disable RDMA direct-land if the arm-table capacity exceeds
the 16-bit arm-id value space.

`ClusterGcsBlockOutstandingSlot` grows direct-land fields:

| Field | Meaning |
|---|---|
| `direct_generation` | Incremented every time the slot is reserved; prevents stale CQE reuse. |
| `direct_state` | `UNARMED`, `ARMING`, `ARMED`, `LANDED`, `INSTALLED`, `ABORTING`, `ABORTED`. |
| `direct_expected_peer` | Peer whose block-reply lane has the posted receive. |
| `direct_arm_id` | LMON arm-table index or invalid. |
| `direct_target_kind` | Reserved invalid buffer or staging page. |
| `direct_target_buf` | Buffer descriptor when SGE1 targets shared_buffers. |
| `direct_sidecar` | Pointer to sidecar quarantine bytes. |
| `direct_abort_reason` | Last local abort reason for debug/observability. |

The arm record stores the same `direct_generation`.  A CQE is usable only if
the slot is still in `ARMED`, the arm record generation matches, and the sidecar
echoes the current request identity.

### D6.4 LMON Arm-Before-Send Handoff

LMON owns QPs and posted receives, so the backend must not post the direct-land
WR itself.  The request path is:

1. Backend reserves a GCS outstanding slot and increments `direct_generation`.
2. Backend marks the slot `ARMING` and enqueues a normal GCS outbound request
   with `direct_land_desired=true`; it does not set the wire flag yet.
3. LMON drains the outbound request.  Before sending the request to the master,
   LMON checks peer capability and tries to post a two-SGE receive on that
   peer's block-reply lane.
4. If arming succeeds, LMON marks the slot `ARMED`, sets
   `GcsBlockRequestPayload.reserved_0[1]`, and sends the request.
5. If arming fails, LMON marks the slot `UNARMED`, clears the direct-land flag,
   increments lane fallback stats, and sends the request on the existing
   staging-copy path.

This keeps arming and request SEND ordered inside LMON: the wire request cannot
advertise direct-land until the receive WR is already posted.

Forwarded holder replies require an exact expected sender.  The safe initial
D6 rule is:

- direct-land is not enabled when the requester already has local proof that a
  holder different from the expected peer is current;
- if the master receives a direct-armed request but must forward to a holder
  that is not the armed peer, it must first consume the posted direct receive
  with an authoritative direct-land denial and the requester retries with
  direct-land suppressed;
- forwarded `GcsBlockForwardPayload` direct-land is set only when a future
  redirect-arm handshake proves the exact holder has been armed; the initial
  D6 implementation always clears it;
- a later in-spec enhancement may add a redirect-arm handshake, but holder
  direct-land must not rely on a wildcard receive posted to the wrong peer.

### D6.5 Sender Capability Gate

A sender may use the direct-land reply format only if all conditions hold:

- the inbound request or forward payload has the direct-land armed flag set;
- the sender has a connected block-reply lane to the requester;
- the reply is a v1 GCS block reply with exactly `GcsBlockReplyHeader + BLCKSZ`;
- the reply is non-Smart-Fusion-v2 and non-destructive X-transfer;
- the sender can produce a valid block image from live SGE, scratch SGE, or a
  local copy buffer.

When the flag is set and the lane is connected, the sender should consume the
requester's posted receive on the block-reply lane.  It may still source the
page from a copied buffer; direct-land is about receiver placement, not only
sender live SGE.

If the sender cannot build a successful block reply after the requester armed a
receive, it should send a direct-land denial reply with a zeroed page payload on
the block-reply lane when possible.  Sending a generic reply after the request
advertised direct-land is unsafe because the posted block-reply receive would
remain outstanding and could later DMA into a released target.  If the
block-reply lane itself fails, the requester times out, resets or drains the
lane, and retries without direct-land.

### D6.6 Completion Verifier

LMON handles block-reply CQEs before waking the backend.  Completion processing:

1. Decode `wr_id` and find the arm record.
2. Reject if the arm index/generation is stale or the slot is not `ARMED`.
3. On CQE error, move to abort cleanup.
4. On CQE success, mark the arm `LANDED` but keep the target invisible.
5. Verify sidecar envelope and reply header.
6. Verify the reply status.  Only success statuses may install bytes:
   `GCS_BLOCK_REPLY_GRANTED`, `GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER`,
   and `GCS_BLOCK_REPLY_S_GRANTED_XHOLDER_DOWNGRADE`.
   `GCS_BLOCK_REPLY_X_GRANTED_FROM_HOLDER` is excluded from the D6 whitelist
   until destructive X-transfer has separate ownership/drop-before-send proof
   and tests.  Any `DENIED_*`, `GCS_BLOCK_REPLY_GRANTED_STORAGE_FALLBACK`, or
   CR-result status must not install a landed page even when the sidecar and
   page checksum are valid.
7. Verify `request_id`, `requester_backend_id`, `transition_id`, epoch, and
   expected peer against the outstanding slot.
8. Verify envelope CRC over the sidecar payload header plus landed page bytes.
9. Verify `GcsBlockReplyHeader.checksum` over the landed page bytes.
10. Run the existing lost-write/page-LSN/SCN checks.
11. Install or mark valid only after every check passes.

Direct-land denial replies use an existing non-success status, not a new wire
status.  The preferred denial for "sender cannot produce a valid image" is
`GCS_BLOCK_REPLY_DENIED_MASTER_NOT_HOLDER`; epoch and validator failures retain
their existing `DENIED_EPOCH_STALE` and `DENIED_VALIDATOR_REJECT` statuses.
A non-success direct-land reply is authoritative for the outstanding slot only
after the `wr_id`/sidecar identity, epoch, transition id, request id, requester
backend, expected peer, envelope CRC, and page checksum all match.  If any of
those checks fail, the receiver must treat the reply as stale or corrupt,
abort/clean up the arm, and retry the normal path; it must not wake the waiter
as if this were a valid denial for the slot.

The verifier must not call the generic envelope dispatcher.  It can reuse the
same checksum helpers, but the payload bytes are discontiguous across sidecar
and SGE1.

### D6.7 Abort Cleanup

Abort cleanup must account for posted receives that cannot be unposted from an
RC QP.  The cleanup policy is:

| Failure | Cleanup |
|---|---|
| Arming failed before `ibv_post_recv` | Clear flag, release target, send normal request. |
| Timeout while WR is posted | Mark arm `ABORTING`; reset or drain the block-reply lane until the WR completes with error before releasing target memory. |
| CQE error | Release target, clear arm, increment `direct_install_abort_count` and lane error stats. |
| Sidecar/checksum/identity failure | Leave target invisible, release target after verifier marks slot failed, increment abort counters. |
| Backend exit/cancel | Mark slot orphaned and ask LMON to reset/drain the lane before releasing or reusing the target. |
| Peer disconnect | Fail all armed slots for that peer, reset lane, wake waiters for retry/failure. |

No abort path may release a target page while a posted receive can still DMA
into it.  If the provider cannot prove the WR is completed or flushed, reset the
block-reply QP and rebuild the lane before accepting new direct-land arms.

### D6.8 Fallback Semantics

Fallback is allowed before the direct-land request is advertised on the wire:

- no RDMA build/runtime support;
- peer not using RDMA;
- no block-reply lane;
- two-SGE receive unsupported;
- no reserved invalid buffer target;
- arm-table full;
- `ibv_post_recv` failed.

After the request has been sent with direct-land armed, the attempt must finish
through the block-reply lane or abort.  It must not silently install a generic
fallback reply into the same slot while the posted receive is still live.

### D6.9 Observability

The existing counters remain valid, but D6 makes the direct-land counters live
and adds lane-level evidence:

| Counter/state | Source | Meaning |
|---|---|---|
| `direct_install_count` | GCS | Verified direct-land page installed with no staging copy. |
| `direct_install_abort_count` | GCS | Direct-land arm/completion/verifier failed after resources were reserved. |
| `block_reply_lane_fallback_count` | RDMA peer stats | Direct-land desired but not armed before request send. |
| `block_reply_lane_error_count` | RDMA peer stats | CQE/provider/lane reset error on the block-reply lane. |
| `block_reply_lane_state` | `pg_stat_cluster_ic` or debug dump | `disabled`, `connecting`, `connected`, `resetting`, `error`. |
| `last_block_reply_error` | `pg_stat_cluster_ic` or debug dump | Last direct-land lane error/fallback reason. |

### D6.10 Tests

Unit tests:

- sidecar size/layout is exactly `ClusterICEnvelope + GcsBlockReplyHeader`;
- `wr_id` arm-index/generation decoding rejects stale CQEs;
- `wr_id` generation wrap stays in the 16-bit wire domain (`65535 -> 1`) and
  arm-table capacity cannot exceed the arm-id field value space;
- slot state transitions reject illegal edges;
- arming failure clears request/forward direct-land flags;
- success-status whitelist excludes destructive X-transfer and non-success
  denial statuses;
- no-forward direct-land identity accepts sendable non-success denial statuses
  after sidecar/checksum validation so they become authoritative denials rather
  than stale `BAD_IDENTITY` aborts;
- direct-land arming is skipped when local holder state proves the expected
  peer is not the holder, and master-side forward decisions consume a direct
  receive with a denial before any generic holder reply can race it;
- forward direct-land flag uses `GcsBlockForwardPayload.reserved_0[5]` and
  master forwarding clears it unless an exact holder arm exists;
- verifier rejects wrong peer, wrong backend id, wrong request id, wrong
  transition id, stale epoch, bad envelope CRC, bad block checksum;
- timeout/backend-exit cleanup does not release a live posted target.

soft-RoCE TAP:

- tier3 generic RDMA lane and dedicated block-reply lane both reach
  `connected` over Linux soft-RoCE;
- `pg_stat_cluster_ic` exposes block-reply lane state, fallback, error, and
  last-error columns;
- a cross-node read direct-lands at least one GCS block reply into a reserved
  invalid buffer and increments `direct_install_count`;
- the direct-land success path does not increment the block-reply lane fallback
  counter.

### Direct-Land Sidecar

The direct-land receive sidecar is fixed at
`CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES` and precedes the 8 KB block image.
It is quarantine metadata, not trusted state.  The frozen byte layout is:

| Bytes | Field | Notes |
|---:|---|---|
| `0..35` | `ClusterICEnvelope` | `msg_type=PGRAC_IC_MSG_GCS_BLOCK_REPLY`; `payload_length=sizeof(GcsBlockReplyHeader)+BLCKSZ`. |
| `36..83` | `GcsBlockReplyHeader` | Existing v1 reply header.  Smart Fusion v2 replies are not direct-landed in D6. |

The 84-byte size is exactly `sizeof(ClusterICEnvelope) +
sizeof(GcsBlockReplyHeader)`.  Slot generation is local receiver state and is
carried by `wr_id` plus the LMON arm record, not by the sender-visible sidecar.
Changing this layout requires changing
`CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES`, unit tests, and compatibility
notes in one patch.

### Request/Forward Flags

`GcsBlockRequestPayload.reserved_0[1]` means the original requester armed a
direct-land receive.  Forwarded holder requests carry the same intent in
`GcsBlockForwardPayload.reserved_0[5]`, which is the spec-6.13 forward
direct-land flag.  `GcsBlockForwardPayload.reserved_0[3]` remains reserved for
the spec-6.12a downgrade request flag, and `reserved_0[4]` remains reserved for
the spec-6.12b CR request flag.  A sender must treat the flag as advisory and
also check RDMA peer capability before choosing the direct-land reply format.

The master must clear the forward direct-land flag unless it can prove the
requester armed the holder as the exact expected reply peer.  Without that
proof, the holder uses the existing generic reply path.

### Receiver State Machine

| State | Meaning | Exit |
|---|---|---|
| `UNARMED` | Slot uses existing staging-copy reply path. | Request may proceed normally. |
| `ARMING` | Backend has reserved a slot and asked LMON to post the direct-land receive. | `ARMED` on post success, `UNARMED` on arming failure. |
| `ARMED` | Receive WR is posted on the block-reply lane; request/forward may advertise direct-land. | `LANDED`, `ABORTED`, or timeout. |
| `LANDED` | CQE succeeded and bytes are in the target/staging memory, but not trusted. | `INSTALLED` after verifier succeeds, `ABORTED` otherwise. |
| `INSTALLED` | Page passed verification and was made visible to the waiting backend. | Slot release. |
| `ABORTED` | Any error path released resources and marked the attempt unusable. | Retry without direct-land or propagate existing GCS error. |

Legal transitions:

| From | To | Trigger |
|---|---|---|
| `UNARMED` | `ARMING` | Backend reserves a slot and direct-land is desired. |
| `ARMING` | `ARMED` | LMON posts the two-SGE receive successfully. |
| `ARMING` | `UNARMED` | LMON cannot arm; request proceeds without direct-land flag. |
| `ARMED` | `LANDED` | Block-reply lane CQE success. |
| `ARMED` | `ABORTED` | CQE error, peer loss, timeout with lane reset, backend cancel. |
| `LANDED` | `INSTALLED` | Verifier accepts sidecar and page. |
| `LANDED` | `ABORTED` | Verifier rejects sidecar/page. |
| `INSTALLED` | `UNARMED` | Slot release/reuse. |
| `ABORTED` | `UNARMED` | Slot release/retry. |

### Resource Ownership

- Backend owns the GCS outstanding slot and wait condition variable.
- LMON owns RDMA QPs, CQs, and posted receives.
- The direct-land arm record bridges the two with an explicit slot generation.
- The reserved invalid buffer pin or staging page reference is held from
  successful arming until verifier success or abort cleanup.
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

For a reserved invalid shared-buffer target, "never installs" means the page is
left invalid and no waiter can observe the DMA bytes.  For a staging target,
the staging memory is released without copying into shared_buffers.

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

Direct-land counters shipped in D6:

| Counter | Meaning |
|---|---|
| `direct_install_count` | Verified direct-land page installed. |
| `direct_install_abort_count` | Direct-land arm or completion failed after resources were reserved. |
| `block_reply_lane_fallback_count` | Direct-land requested but lane/capability/arming was unavailable. |
| `block_reply_lane_error_count` | Block-reply lane CQE or provider error. |

## Acceptance

The spec is shipped when all mandatory local checks pass and the opt-in RDMA
TAP is available for Linux rxe CI:

- `make -C src/test/cluster_unit check`;
- unit coverage for D6 sidecar packing, slot generation rejection, and state
  transitions;
- object compilation for the touched RDMA, GCS, LMON, outbound, and bufmgr
  modules;
- `prove -v src/test/cluster_tap/t/334_ic_rdma_soft_roce.pl` has a valid skip
  path outside Linux rxe and, when enabled with `PGRAC_RUN_RDMA_SOFT_ROCE=1`,
  proves tier3 block-reply lane connection and `direct_install_count`
  increment on a valid RDMA block reply;
- verifier fail-closed cases are covered by unit invariants for WR id type,
  state transitions, success-status whitelist, direct forward flag gating, and
  generic-forward direct flag clearing.

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
