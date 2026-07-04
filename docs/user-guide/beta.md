# Beta release: scope, disclaimer, and known limitations

pgrac `v0.5.0-beta` is the first public release. It delivers the RAC core —
shared-disk multi-node clustering — for **evaluation and early pilots**. This
page states what the beta promises, what it does not, and the current behavior
at each edge.

Read this before running pgrac on data you care about.

## Beta disclaimer

- **This is a beta, not a general-availability release.** The RAC core works end
  to end across nodes, but the release is intended for evaluation and early
  pilots, not unattended production.
- **No service-level, uptime, or data-integrity guarantee.** Keep independent
  backups of anything irreplaceable. Do not treat the cluster as a system of
  record for critical data during the beta.
- **Support model.** Issues are tracked publicly and addressed with patches.
  There is no commercial support SLA.
- **Not GA.** General availability is a later `v1.0.0` release, after production
  hardening (production storage backends, hardware fencing, RDMA transport,
  standby / DR, and full operational monitoring).
- **No upgrade path.** This is the first release. In-place upgrade from the beta
  is not offered; plan to reinitialize when moving to a later release.

## What works in the beta

The seven RAC core capabilities operate across nodes:

- Shared-disk multi-node cluster on one shared database.
- Cache Fusion block transfer over the interconnect.
- Cross-node MVCC with a global SCN clock.
- Cluster crash recovery with no lost commits.
- Cooperative, fail-closed fencing on quorum loss.
- Global Enqueue Service with cross-node deadlock detection.
- Online cluster reconfiguration (join / clean leave / removal).

See the [release notes](../release-notes/v0.5.0-beta.md) for a description of
each, and the [multi-node quickstart](bootstrap.md#multi-node-cluster-tier1-tcp-interconnect)
to bring up a cluster.

## Known limitations register

Each entry states the current behavior. Where a capability is not available, the
cluster fails closed with a clear error rather than degrading silently.

### Node count

Up to 16 nodes. This is an architectural ceiling of the current cluster address
space, not a tuning parameter.

### Fencing is cooperative, not hardware-enforced

Membership fencing is enforced at the database layer: a node fails closed and
stops accepting writes when it loses its voting-disk majority. Hardware-level
isolation of a partitioned node — STONITH power fencing or SCSI-3 persistent
reservations — is **not enforced by this beta**. For deployments that need a
guarantee that an isolated node cannot touch shared storage, pair the cluster
with an external node-level watchdog (for example a systemd / IPMI / cloud-API
fencer) alongside the built-in cooperative fencing.

### Subsystems not part of the validated beta surface

The following subsystems exist in the source tree as experimental work but are
**not covered by this beta's validation**. Do not rely on them for evaluation:

- **RDMA interconnect transport.** The default interconnect is the TCP tier;
  RDMA transport tiers are experimental. Selecting an unimplemented tier fails
  closed with `FEATURE_NOT_SUPPORTED`.
- **Active Data Guard standby.**
- **Backup / point-in-time recovery.**

### Storage backend

The validated configuration uses the shared-filesystem storage backend. A
block-device backend exists in the tree, but its production hardening (direct
I/O tuning and hardware-reservation fencing) is not part of this beta.

### Cross-node consistent-read cache

The cross-node consistent-read (CR) cache fast path ships **disabled by
default**. It is workload-conditional: enable it only for read-heavy workloads
and follow the tuning guidance, since the benefit depends on read/write mix.

### Cross-node forward Cache Fusion

Some forward-transfer paths in the Cache Fusion protocol are partially limited
in this release.

### Scale evaluation on a single machine

A single machine can run a small cluster for functional evaluation. To evaluate
multi-node behavior under sustained write load — several nodes with continuous
transactions and injected failures — use separate hosts; a single machine
running four full nodes plus voting disks and continuous load is
resource-constrained.

## Reporting problems

Please report defects — especially anything that looks like a lost commit, a
visibility anomaly across nodes, or a hang — through the public issue tracker
with the reproduction steps and the relevant cluster view output. That feedback
is exactly what the beta period is for.
