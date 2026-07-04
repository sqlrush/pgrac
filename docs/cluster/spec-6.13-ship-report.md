# spec-6.13 Ship Report

## Status

spec-6.13 RDMA tier3 and GCS block-reply zero-copy is shipped on
2026-07-04.

The shipped scope includes:

- tier3 provider selection with mlx5dv preference and fallback policy handling;
- inline send, bounded unsignaled batching, and bounded busypoll completions;
- sender-side live shared_buffers SGE for non-destructive GCS block replies;
- dedicated block-reply lane/QP for receiver-side direct-land;
- two-SGE receive posting with quarantined sidecar and target page;
- outstanding-slot correlation by arm id, 16-bit wire generation, request id,
  backend id, epoch, peer, and checksum;
- LMON arm-before-send handoff and fail-closed fallback when arming or lane
  capability is unavailable;
- direct-land verifier success whitelist, authoritative denial handling, and
  abort cleanup for CQE/provider/checksum/identity failures;
- `pg_stat_cluster_ic`, `pg_cluster_state`, and wait-event observability for
  spec-6.13 counters and lane state.

## Evidence

Code-bearing head:

```text
3755f1f814a09755238af1c6b00dc8a38b7a2466
```

GitHub Actions nightly full CI:

```text
https://github.com/sqlrush/pgrac/actions/runs/28702365954
status: completed
conclusion: success
```

The successful run covered Linux regression, macOS smoke, and all
`cluster_tap` shards including foundation-core, stage2-gcs, stage3-core,
stage5-ges-locking, and stage6-storage.

## Operator Notes

`cluster.interconnect_tier=tier3` now selects the shipped RDMA tier3 path in
`--with-rdma` builds.  Without RDMA support, or when the requested provider is
unavailable and fallback is disabled, startup still fails closed with `53R22`.
With fallback enabled, tier3 may use generic verbs or TCP fallback while keeping
the same GCS verification and checksum rules.

The D6 direct-land path is intentionally limited to GCS block replies on the
dedicated block-reply lane.  Generic interconnect messages, Smart Fusion
reply-v2, RDMA READ/WRITE, CRC offload, and destructive X-transfer direct-land
remain outside the shipped 6.13 scope.
