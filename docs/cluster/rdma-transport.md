# RDMA interconnect transport

Spec: `spec-6.1-rdma-transport-stack.md`

The RDMA transport is optional.  Default builds do not link libibverbs
or librdmacm.  Build with `--with-rdma` to enable RDMA provider and mux
support; if the headers or libraries are missing, `configure` fails
before build.

## Tier selection

`cluster.interconnect_tier=tier2` and `tier3` bind the interconnect mux.
The mux keeps the existing `ClusterICOps` API stable while allowing
per-peer transport selection.

| Tier | Meaning |
|---|---|
| `tier1` | TCP transport |
| `tier2` | RDMA-capable mux baseline |
| `tier3` | Reserved for mlx5 direct-verbs optimization; spec-6.1 fails closed with `FEATURE_NOT_SUPPORTED` until the `mlx5dv` path lands |

If the binary was not built with `--with-rdma`, selecting `tier2` or
`tier3` fails closed with SQLSTATE `53R22`
(`cluster_ic_rdma_unavailable`).  If RDMA was built but no usable data
path is active, `cluster.interconnect_rdma_fallback=auto` uses TCP
fallback; `fallback=off` fails closed.  A `--with-rdma` build opens a
verbs device and creates PD/CQ/completion-channel resources only in LMON.
Peer traffic stays on TCP until the per-peer RDMA CM/QP handshake reaches
connected state.

## pgrac.conf RDMA fields

Each `[node.N]` may declare RDMA-specific metadata:

```ini
[node.0]
interconnect_addr = 10.0.0.1:6432
rdma_addr = 10.10.0.1:18515
rdma_gid = fe80::1
rdma_port = 1
rdma_pkey = 0xffff
rdma_qkey = 0
```

`rdma_addr` is the RDMA CM listener address.  It must be set for an RDMA
link; `interconnect_addr` remains reserved for the always-on TCP
fallback listener.  `rdma_gid` is recorded for operator validation and
diagnostics.  `rdma_port` defaults to `1` and must be in `[1,255]`.
`rdma_pkey` defaults to `0xffff` and accepts decimal or `0x` hexadecimal
values in `[0,65535]`.  `rdma_qkey` defaults to `0` and accepts decimal
or `0x` hexadecimal `uint32` values.  RDMA CM private data verifies the
peer's declared port, P_Key, and Q_Key before enabling RDMA traffic.

## Correctness constraints

RDMA carries the same `ClusterICEnvelope` as TCP.  Block shipping must
not bypass envelope verification.  Spec-6.1 uses a registered per-peer
block scratch MR: the sender copies a stable page image into scratch while
holding the buffer content lock, releases the lock, and then sends the
scratch region by RDMA SEND-with-SGE.  This removes the unsafe live-page
DMA window and keeps the receiver path unchanged: data lands in
quarantine, passes the common envelope/CRC32C verification path, and is
installed only after verification succeeds.

Destructive X-transfer replies that must drop the sender's local X copy
before the wire reply is visible materialize the block into a local 8 KB
buffer before the drop.  That keeps the existing single-owner
drop-before-send rule intact; ordinary GRANTED and READ_IMAGE replies may
use the registered scratch SGE when the peer is RDMA-connected.

Application-level CRC32C remains mandatory for block shipping.
`cluster.interconnect_rdma_crc_offload=on` fails closed in spec-6.1 and
does not disable block-image CRC.
`cluster.interconnect_rdma_completion=busypoll` and
`cluster.interconnect_rdma_provider=mlx5` are reserved settings in
spec-6.1; selecting either fails with `FEATURE_NOT_SUPPORTED` rather than
silently behaving like generic event-driven verbs.

## Hardening

Spec-6.1 uses SQLSTATE `58R16` for RDMA fabric/provider failures.
`58R14` and `58R15` remain owned by the spec-6.0a shared-storage backend
and are not reused by the RDMA transport.

The block data path is copy-into-scratch, not zero-copy DMA from live
shared buffers.  The sender copies the page image into registered
per-peer scratch storage before posting RDMA SEND-with-SGE, so no
asynchronous work request can retain a live buffer-content lock or a
pointer into shared_buffers.

Inbound frames are verified before dispatch or block install.  CQE
triage treats local-protection and provider programming failures as
fail-closed fabric errors, while peer-loss conditions may use TCP
fallback only when the configured policy allows it.

## SQLSTATEs

| SQLSTATE | Condition | Meaning |
|---|---|---|
| `53R22` | `cluster_ic_rdma_unavailable` | RDMA tier requested but not available and fallback is disabled or impossible. |
| `58R16` | `cluster_ic_rdma_fabric_error` | RDMA fabric/provider error.  `58R14` and `58R15` are owned by the spec-6.0a shared-storage backend. |

## Observability

The wait-event registry includes RDMA send, receive, poll, connect, and
TCP fallback events under `Cluster: Interconnect`.  Use
`pg_stat_cluster_wait_events` to inspect the registered event names.

`pg_stat_cluster_ic` exposes the mux state: per-peer transport,
RDMA state, provider, RDMA address metadata, MR registration status, CQ
depth, fallback count, byte counters, SEND-with-SGE block counters, and
last error summary.
