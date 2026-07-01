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
| `tier3` | RDMA-capable mux optimized/provider-selected tier |

If the binary was not built with `--with-rdma`, selecting `tier2` or
`tier3` fails closed with SQLSTATE `53R22`
(`cluster_ic_rdma_unavailable`).  If RDMA was built but no usable data
path is active, `cluster.interconnect_rdma_fallback=auto` uses TCP
fallback; `fallback=off` fails closed.  A `--with-rdma` build opens a
verbs device, creates PD/CQ/completion-channel resources, and registers
the whole shared buffer pool as one MR during tier init.  Peer traffic
stays on TCP until the per-peer RDMA CM/QP handshake reaches connected
state.

## pgrac.conf RDMA fields

Each `[node.N]` may declare RDMA-specific metadata:

```ini
[node.0]
interconnect_addr = 10.0.0.1:6432
rdma_addr = 10.10.0.1:18515
rdma_gid = fe80::1
rdma_port = 1
```

`rdma_addr` is the RDMA CM listener address.  It must be set for an RDMA
link; `interconnect_addr` remains reserved for the always-on TCP
fallback listener.  `rdma_gid` is recorded for operator validation and
diagnostics.  `rdma_port` defaults to `1` and must be in `[1,255]`.

## Correctness constraints

RDMA carries the same `ClusterICEnvelope` as TCP.  Block shipping must
not bypass envelope verification.  SEND-with-SGE uses a registered
`shared_buffers` page SGE for normal block-image replies, so the HCA DMA
reads the sender's page directly.  The sender keeps a raw buffer pin and
content-lock shared hold until the RDMA SEND completion releases the SGE.
The receiver still lands data in quarantine, verifies the envelope and
CRC32C, and installs the block only after verification succeeds.

Destructive X-transfer replies that must drop the sender's local X copy
before the wire reply is visible materialize the block into a local 8 KB
buffer before the drop.  That keeps the existing single-owner
drop-before-send rule intact; non-destructive GRANTED and READ_IMAGE
replies use the registered `shared_buffers` SGE.

Application-level CRC32C remains mandatory for block shipping.
`cluster.interconnect_rdma_crc_offload` is limited to future
control-plane experiments and does not disable block-image CRC.

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
