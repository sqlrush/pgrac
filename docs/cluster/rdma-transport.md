# RDMA interconnect transport

Spec: `spec-6.1-rdma-transport-stack.md` and the spec-6.13 RDMA tier3
extensions.  The detailed spec-6.13 design is
`docs/cluster/spec-6.13-rdma-tier3-zero-copy.md`; the shipped evidence is
summarised in `docs/cluster/spec-6.13-ship-report.md`.

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
| `tier2` | Generic verbs RDMA mux baseline |
| `tier3` | RDMA tier that prefers mlx5 direct-verbs capability and falls back according to `cluster.interconnect_rdma_fallback` |

If the binary was not built with `--with-rdma`, selecting `tier2` or
`tier3` fails closed with SQLSTATE `53R22`
(`cluster_ic_rdma_unavailable`).  If RDMA was built but no usable data
path is active, `cluster.interconnect_rdma_fallback=auto` uses TCP
fallback; `fallback=off` fails closed.  A `tier3` or explicit
`cluster.interconnect_rdma_provider=mlx5` request probes mlx5dv support.
With `fallback=auto`, a missing build/runtime mlx5 path logs the reason
and uses generic verbs; with `fallback=off`, it fails closed.  A
`--with-rdma` build opens a verbs device and creates
PD/CQ/completion-channel resources only in LMON.  Peer traffic stays on
TCP until the per-peer RDMA CM/QP handshake reaches connected state.

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

RDMA carries the same `ClusterICEnvelope` as TCP.  Generic messages still land
in the normal inbound frame buffer, pass the common envelope/CRC32C
verification path, and dispatch only after verification succeeds.  GCS
block-reply direct-land is the only receiver-side exception: it uses a
dedicated block-reply lane, posts a two-SGE receive, quarantines sidecar bytes
separately from the target page, and installs the page only after the D6
verifier accepts identity, epoch, status, checksum, and slot generation.

There are two sender-side block payload sources:

| Source | Use |
|---|---|
| Live shared_buffers SGE | Preferred for non-destructive GCS block replies when the peer is RDMA-connected and shared_buffers has an RDMA MR/lkey.  The sender raw-pins the buffer, performs the WAL flush plus HC89 single-retry revalidation, posts the page as a local SEND SGE, and releases the raw pin when the SEND path completes or falls back. |
| Per-peer scratch MR | Fallback when live SGE is not allowed or cannot be borrowed/validated.  The sender copies the page image into registered scratch storage and sends that scratch region by RDMA SEND-with-SGE. |

Destructive X-transfer replies that must drop the sender's local X copy
before the wire reply is visible materialize the block into a local 8 KB
buffer before the drop.  That keeps the existing single-owner
drop-before-send rule intact.  Smart Fusion reply-v2 also stays on its
copy-based payload path.  Ordinary GRANTED and READ_IMAGE replies may use
the live SGE path first and fall back to scratch/copy/TCP.

Application-level CRC32C remains mandatory for block shipping.
`cluster.interconnect_rdma_crc_offload=on` still fails closed and does
not disable block-image CRC.  `cluster.interconnect_rdma_completion`
supports event-driven completions and bounded busypoll completions.
`cluster.interconnect_rdma_provider=mlx5` selects the tier3 provider
when the binary and device expose mlx5dv capability; otherwise it follows
the configured fallback policy.

Receiver direct-land must not use the generic RC QP.  The generic QP has a
single receive lane and one receive SGE, so a block reply cannot safely
pre-post a sidecar + target-page receive on that lane without risking FIFO
consumption by an unrelated message.  The shipped D6 path uses the dedicated
block-reply QP/lane, two-SGE receive posting, slot generation correlation, and
LMON arm-before-send handoff described in the spec-6.13 design.

## Hardening

Spec-6.1 uses SQLSTATE `58R16` for RDMA fabric/provider failures.
`58R14` and `58R15` remain owned by the spec-6.0a shared-storage backend
and are not reused by the RDMA transport.

The live SGE path never holds a buffer content lock across asynchronous
RDMA completion.  It relies on a raw pin to prevent buffer reuse and on
the existing block checksum/envelope verification to fail closed if page
contents drift after revalidation.  Scratch SGE and TCP fallback remain
available whenever the live page cannot be borrowed or registered.

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
depth, fallback count, byte counters, SEND-with-SGE block counters,
tier3 send count, inline send count, unsignaled batch count, busypoll
burn/fallback counters, block-reply lane state/fallback/error counters, and
last error summaries.

`dump_cluster_state` exposes GCS-side copy-path counters:
`scratch_copy_count`, `live_sge_send_count`, `live_sge_fallback_count`,
`direct_install_count`, `direct_install_abort_count`, and
`install_copy_count`.  The direct-install counters are live for the shipped
spec-6.13 D6 direct-land lane.
