/*-------------------------------------------------------------------------
 *
 * cluster_hw_snapshot.h
 *	  HW (relation extend) cluster authority -- durable snapshot envelope
 *	  (spec-5.7 D3, §3.1b R1/R10/R12/R13).
 *
 *	  The HW authority HWM table lives in shmem at the enqueue master and is
 *	  the running source of truth (spec-5.7 §3.1a).  To survive a crash, a
 *	  WAL prune, or an online remaster it is also captured into a durable
 *	  snapshot: at every checkpoint each master writes its hw_htab to a
 *	  per-master file on shared storage, and an online remaster writes an
 *	  ADOPTION snapshot of the inherited HWMs before it serves the shard.
 *	  Recovery rebuilds the HWM from snapshot + the HW_RESERVE WAL tail
 *	  (rebuild = max(snapshot, tail >= snapshot_lsn), §3.1b R3).
 *
 *	  R12 (unified identity envelope): the checkpoint snapshot (R1) and the
 *	  adoption snapshot (R9) MUST share one envelope and one parse/validate/
 *	  read path -- a split "plain header for R1, rich header for R9" risks a
 *	  missing field or a mis-bound stale image.  The two triggers differ only
 *	  in snapshot_kind, snapshot_lsn semantics (checkpoint = redo LSN /
 *	  adoption = rebuild-complete LSN) and generation.
 *
 *	  This header declares the PURE layer (serialize / deserialize+validate /
 *	  identity / generation-supersede), unit-tested standalone with the real
 *	  CRC32C from libpgport.  The durable torn-safe file I/O (temp +
 *	  durable_rename, mirror spec-5.6 cluster_cf_authority) and the checkpoint
 *	  / remaster hooks land in cluster_hw_snapshot.c with D3 S3/S5.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hw_snapshot.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D3, §3.1b R1/R10/R12/R13)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HW_SNAPSHOT_H
#define CLUSTER_HW_SNAPSHOT_H

#include "cluster/cluster_grd.h" /* ClusterResId */
#include "storage/block.h"		 /* BlockNumber */

/*
 * On-disk envelope identity.  magic doubles as the byte-order / structural
 * gate (a foreign byte order or a torn header fails the magic check before any
 * CRC work), version guards the layout.
 */
#define CLUSTER_HW_SNAPSHOT_MAGIC 0x48575350 /* 'HWSP' */
#define CLUSTER_HW_SNAPSHOT_VERSION 1

/*
 * ClusterHwSnapshotKind -- which trigger wrote this snapshot.  R12: kind is
 * the ONLY structural difference between a checkpoint and an adoption
 * snapshot; both use this same envelope and the same read path.
 */
typedef enum ClusterHwSnapshotKind {
	CLUSTER_HW_SNAPSHOT_CHECKPOINT = 0, /* CreateCheckPoint hook; snapshot_lsn = redo LSN */
	CLUSTER_HW_SNAPSHOT_ADOPTION = 1,	/* online remaster; snapshot_lsn = rebuild-complete LSN */
} ClusterHwSnapshotKind;

/*
 * ClusterHwSnapshotValidity -- outcome of deserialize+validate.  Any non-VALID
 * outcome makes the recovery caller fail closed (53RA6): it must never
 * auto-create the authority at 0 or read FileSize from a snapshot it cannot
 * trust (spec-5.7 §3.1b R6).
 */
typedef enum ClusterHwSnapshotValidity {
	CLUSTER_HW_SNAPSHOT_VALID = 0,
	CLUSTER_HW_SNAPSHOT_INVALID_SHORT,	  /* fewer bytes than the declared structure */
	CLUSTER_HW_SNAPSHOT_INVALID_MAGIC,	  /* magic / version mismatch (corrupt or foreign) */
	CLUSTER_HW_SNAPSHOT_INVALID_CRC,	  /* stored CRC != recomputed (torn / corrupt) */
	CLUSTER_HW_SNAPSHOT_INVALID_IDENTITY, /* CRC ok but system_id/owner/shard mismatch */
} ClusterHwSnapshotValidity;

/*
 * ClusterHwSnapshotHeader -- the fixed 48-byte envelope header.  Field offsets
 * are chosen so the struct has NO internal padding (the two uint64 fields sit
 * at 8-aligned offsets 8 and 32), so a memcpy of a fully-initialised header is
 * a deterministic byte image -- a torn or padding-dependent CRC is impossible.
 *
 *	magic / version    structural + layout gate (R12 single envelope).
 *	system_id          ControlFileData.system_identifier of the owning cluster.
 *	owner_node_id      the shard master that wrote this snapshot.
 *	shard_partition    the shard / resid-partition identity this snapshot covers.
 *	snapshot_kind      CLUSTER_HW_SNAPSHOT_CHECKPOINT | _ADOPTION.
 *	generation         shard_master_generation: monotone across an owner's
 *	                   snapshots; a reader picks the identity-matching maximum
 *	                   generation as authoritative (R10).
 *	snapshot_lsn       checkpoint redo LSN, or adoption rebuild-complete LSN;
 *	                   the rebuild replays HW_RESERVE with lsn >= snapshot_lsn.
 *	n_entries          number of (resid, next_hwm) pairs that follow.
 *	reserved           must be 0 (CRC-covered; keeps the header 8-aligned).
 */
typedef struct ClusterHwSnapshotHeader {
	uint32 magic;			/* offset 0 */
	uint32 version;			/* offset 4 */
	uint64 system_id;		/* offset 8 */
	uint32 owner_node_id;	/* offset 16 */
	uint32 shard_partition; /* offset 20 */
	uint32 snapshot_kind;	/* offset 24; ClusterHwSnapshotKind */
	uint32 generation;		/* offset 28 */
	uint64 snapshot_lsn;	/* offset 32 */
	uint32 n_entries;		/* offset 40 */
	uint32 reserved;		/* offset 44; must be 0 */
} ClusterHwSnapshotHeader;

StaticAssertDecl(sizeof(ClusterHwSnapshotHeader) == 48,
				 "HW snapshot header on-disk ABI 48-byte lock (no padding)");

/*
 * ClusterHwSnapshotEntry -- one (rel,fork) authority HWM.  16-byte resid +
 * 4-byte next free block, no padding (sizeof 20), so the entry array
 * serialises as a tight 20-byte stride.
 */
typedef struct ClusterHwSnapshotEntry {
	ClusterResId resid;	  /* 16 bytes */
	BlockNumber next_hwm; /* authoritative next free block */
} ClusterHwSnapshotEntry;

StaticAssertDecl(sizeof(ClusterHwSnapshotEntry) == 20,
				 "HW snapshot entry on-disk ABI 20-byte lock");

#define CLUSTER_HW_SNAPSHOT_HEADER_SIZE 48
#define CLUSTER_HW_SNAPSHOT_ENTRY_SIZE 20
#define CLUSTER_HW_SNAPSHOT_CRC_SIZE 4

/*
 * cluster_hw_snapshot_serialized_size -- on-disk byte count for n_entries:
 *	header (48) + n_entries * 20 + trailing crc32c (4).
 */
extern size_t cluster_hw_snapshot_serialized_size(uint32 n_entries);

/*
 * cluster_hw_snapshot_serialize -- write *hdr (its n_entries pairs taken from
 * `entries`) followed by a trailing CRC32C into `buf`.  The caller must set
 * hdr->magic = CLUSTER_HW_SNAPSHOT_MAGIC, hdr->version, hdr->reserved = 0 and
 * the identity fields.  Returns the number of bytes written, or 0 if `buflen`
 * is smaller than cluster_hw_snapshot_serialized_size(hdr->n_entries).
 */
extern size_t cluster_hw_snapshot_serialize(const ClusterHwSnapshotHeader *hdr,
											const ClusterHwSnapshotEntry *entries, char *buf,
											size_t buflen);

/*
 * cluster_hw_snapshot_deserialize -- validate `buf` (`len` bytes) and, on
 * CLUSTER_HW_SNAPSHOT_VALID, fill *hdr_out and up to max_entries pairs into
 * entries_out (hdr_out->n_entries is the true count).  Validation order:
 * structural length -> magic/version -> entry-length + capacity -> CRC.  A
 * declared n_entries greater than max_entries fails closed as INVALID_SHORT
 * (the caller did not provision enough room to rebuild the authority safely).
 */
extern ClusterHwSnapshotValidity
cluster_hw_snapshot_deserialize(const char *buf, size_t len, ClusterHwSnapshotHeader *hdr_out,
								ClusterHwSnapshotEntry *entries_out, uint32 max_entries);

/*
 * cluster_hw_snapshot_identity_ok -- does this header belong to the cluster /
 * owner / shard we expect?  R12 reader gate: a snapshot is only considered for
 * a rebuild when its (system_id, owner_node_id, shard_partition) all match the
 * expected values.  A zero expected_sysid skips the cluster check (early
 * bootstrap before the identity is known).
 */
extern bool cluster_hw_snapshot_identity_ok(const ClusterHwSnapshotHeader *hdr,
											uint64 expected_sysid, uint32 expected_owner,
											uint32 expected_shard);

/*
 * cluster_hw_snapshot_supersedes -- R10 generation pick.  True iff `cand` has
 * the SAME identity as `cur` (system_id + owner + shard) AND a strictly greater
 * generation; i.e. cand is the newer authoritative snapshot for that shard.
 * A candidate with a different identity never supersedes (it is a foreign /
 * mis-bound image), and an equal or lower generation never supersedes (stale).
 */
extern bool cluster_hw_snapshot_supersedes(const ClusterHwSnapshotHeader *cand,
										   const ClusterHwSnapshotHeader *cur);

/*
 * cluster_hw_snapshot_rebuild_value -- the R3/R11 rebuild merge kernel.
 *
 *	The recovery rebuild folds the snapshot HWM and every replayed HW_RESERVE
 *	new_hwm (lsn >= snapshot_lsn) into one value by repeated application of this
 *	function: returns max(current, candidate).  Monotone (R3): the HWM is only
 *	ever raised.  Conservative-hole (R11): a candidate LOWER than current never
 *	lowers it -- the over-estimate is an unused block-number hole (wasteful but
 *	safe), never a regression that could re-hand an already-reserved range.
 *	Single-sourced so cluster_hw_apply_hwm (HW_RESERVE redo) and the snapshot
 *	load use one tested definition of "never lower than any replied end".
 */
extern BlockNumber cluster_hw_snapshot_rebuild_value(BlockNumber current, BlockNumber candidate);

#ifndef FRONTEND

#include "access/xlogdefs.h" /* XLogRecPtr */

/*
 * cluster_hw_apply_snapshot -- recovery rebuild, snapshot half (R3): load a
 * deserialized snapshot's entries into the master authority HWM table, raising
 * each (rel,fork) to its snapshot value via the monotone cluster_hw_apply_hwm.
 * The HW_RESERVE WAL tail (lsn >= snapshot_lsn) is replayed afterwards by the
 * redo handler, completing rebuild = max(snapshot, tail) (§3.1b R3/R11).
 * Backend-only (touches the authority shmem); defined in cluster_hw_shmem.c.
 */
extern void cluster_hw_apply_snapshot(const ClusterHwSnapshotEntry *entries, uint32 n_entries);

/*
 * cluster_hw_snapshot_path -- per-master snapshot path under the shared data
 * dir (cluster.shared_data_dir/global/pg_hw_snapshot.<owner_node_id>).  Each
 * master writes its own file so a survivor can read a dead master's snapshot;
 * owner_node_id is in the name.  Returns false (empty dst) when the shared root
 * is unset, so a caller fails closed rather than touching a bogus path.
 */
extern bool cluster_hw_snapshot_path(uint32 owner_node_id, char *dst, size_t dstlen);

/*
 * cluster_hw_snapshot_write -- durably write the local master's authority
 * snapshot (R1/R2).  Serialises the unified envelope (header + entries + CRC)
 * and installs it torn-safe: write a temp file, fsync, durable_rename over the
 * per-master path (mirror spec-5.6 cluster_cf_authority_write).  Caller has
 * already copied `entries` out of the authority HTAB under the region lock and
 * stamped snapshot_lsn (R10/R13 single-writer).  PANICs on I/O failure
 * (mirroring update_controlfile).
 */
extern void cluster_hw_snapshot_write(uint32 owner_node_id, uint32 shard_partition,
									  uint32 generation, XLogRecPtr snapshot_lsn, uint64 system_id,
									  ClusterHwSnapshotKind kind,
									  const ClusterHwSnapshotEntry *entries, uint32 n_entries);

/*
 * cluster_hw_snapshot_read -- read + validate a per-master snapshot (R6 fail-
 * closed).  Reads owner_node_id's file, deserialises, and checks the identity
 * envelope (system_id / owner / shard).  Returns CLUSTER_HW_SNAPSHOT_VALID and
 * fills *out_hdr + up to max_entries entries on success; a missing file is
 * INVALID_SHORT, a torn image INVALID_CRC, a foreign/mis-bound image
 * INVALID_IDENTITY.  Never ereports (safe on the recovery early path); the
 * caller fails closed (53RA6) on any non-VALID outcome and never rebuilds from
 * zero or FileSize.
 */
extern ClusterHwSnapshotValidity
cluster_hw_snapshot_read(uint32 owner_node_id, uint64 expected_sysid, uint32 expected_owner,
						 uint32 expected_shard, ClusterHwSnapshotHeader *out_hdr,
						 ClusterHwSnapshotEntry *entries_out, uint32 max_entries);

/*
 * cluster_hw_authority_active -- gate for the checkpoint write + recovery load:
 * true only in a multi-node cluster with shared storage (where the HW authority
 * is engaged and a survivor must read a dead master's snapshot).  Single-node
 * is a no-op.  Defined in cluster_hw_shmem.c.
 */
extern bool cluster_hw_authority_active(void);

/*
 * cluster_hw_snapshot_checkpoint_write -- persist this node's authority HWM
 * table bound to a checkpoint (snapshot_lsn = redo_lsn).  Hooked into
 * CreateCheckPoint before the checkpoint record, gated on
 * cluster_hw_authority_active().  Defined in cluster_hw_shmem.c.
 */
extern void cluster_hw_snapshot_checkpoint_write(XLogRecPtr redo_lsn);

/*
 * cluster_hw_snapshot_adoption_write -- §3.1b R9: durably persist this survivor's
 * full authority table as an ADOPTION snapshot during an online remaster, after
 * the dead master's snapshot+tail has been applied and before the shard is marked
 * rebuilt / unfrozen.  Shares the R10 single-writer lock with the checkpoint
 * write.  Defined in cluster_hw_shmem.c; called from cluster_hw_remaster.c.
 */
extern void cluster_hw_snapshot_adoption_write(void);

/*
 * cluster_hw_snapshot_recovery_load -- load this node's authority snapshot into
 * the hw_htab at recovery start, before the HW_RESERVE redo replays the tail on
 * top.  A missing snapshot rebuilds from the WAL tail; a corrupt / foreign one
 * FATALs (R6).  Hooked into PerformWalRecovery.  Defined in cluster_hw_shmem.c.
 */
extern void cluster_hw_snapshot_recovery_load(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_HW_SNAPSHOT_H */
