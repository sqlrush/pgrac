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
fallback; `fallback=off` fails closed.  The provider surface does not
advertise RDMA as active until QP/MR activation is wired.

## Correctness constraints

RDMA carries the same `ClusterICEnvelope` as TCP.  Block shipping must
not bypass envelope verification.  SEND-with-SGE can avoid the sender
side block-image copy, but the receiver still lands data in quarantine,
verifies the envelope and CRC32C, and installs the block only after
verification succeeds.

Application-level CRC32C remains mandatory for block shipping.
`cluster.interconnect_rdma_crc_offload` is limited to future
control-plane experiments and does not disable block-image CRC.

## SQLSTATEs

| SQLSTATE | Condition | Meaning |
|---|---|---|
| `53R22` | `cluster_ic_rdma_unavailable` | RDMA tier requested but not available and fallback is disabled or impossible. |
| `58R16` | `cluster_ic_rdma_fabric_error` | RDMA fabric/provider error.  `58R14` and `58R15` are already used by the production shared-storage backend. |

## Observability

The wait-event registry includes RDMA send, receive, poll, connect, and
TCP fallback events under `Cluster: Interconnect`.  Use
`pg_stat_cluster_wait_events` to inspect the registered event names.
