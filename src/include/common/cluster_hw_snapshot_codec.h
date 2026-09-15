/*-------------------------------------------------------------------------
 *
 * cluster_hw_snapshot_codec.h
 *    Frontend/backend shared byte codec for the existing HW snapshot v1.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HW_SNAPSHOT_CODEC_H
#define CLUSTER_HW_SNAPSHOT_CODEC_H

#include "c.h"

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

#define CLUSTER_HW_SNAPSHOT_HEADER_SIZE 48
#define CLUSTER_HW_SNAPSHOT_ENTRY_SIZE 20
#define CLUSTER_HW_SNAPSHOT_CRC_SIZE 4

/*
 * Entries are opaque arrays of exactly n_entries * 20 bytes.  Their canonical
 * resid/HWM layout remains backend-owned; a frontend creates an empty seed
 * with n_entries=0 and entries=NULL, without including GRD runtime headers.
 * This codec retains version1 byte order and compatibility validation.
 */
extern size_t cluster_hw_snapshot_codec_size(uint32 n_entries);
extern size_t cluster_hw_snapshot_codec_serialize(const ClusterHwSnapshotHeader *hdr,
												  const void *entries, char *buf, size_t buflen);
extern ClusterHwSnapshotValidity
cluster_hw_snapshot_codec_deserialize(const char *buf, size_t len, ClusterHwSnapshotHeader *hdr_out,
									  void *entries_out, uint32 max_entries);

#endif
