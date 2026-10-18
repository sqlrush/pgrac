/*-------------------------------------------------------------------------
 *
 * cluster_relmap_v2.h
 *    Relmap v2 encoding and committed/pending copy classification.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/common/cluster_relmap_v2.h
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_V2_H
#define CLUSTER_RELMAP_V2_H

#include "c.h"
#include "common/relmap.h"

#define CLUSTER_RELMAP_V2_MAGIC ((uint32)0x0240DEA9)
#define CLUSTER_RELMAP_V2_VERSION 2
#define CLUSTER_RELMAP_V2_HEADER_SIZE 128
#define CLUSTER_RELMAP_V2_SLOT_SIZE 1024
#define CLUSTER_RELMAP_V2_FILE_SIZE 2176
#define CLUSTER_RELMAP_V2_COMMITTED_OFFSET 128
#define CLUSTER_RELMAP_V2_PENDING_OFFSET 1152

/* In-memory values only: never write or CRC these structs as a header. */
typedef struct ClusterRelmapV2Namespace {
	uint64 system_identifier;
	uint64 database_incarnation;
	Oid dbid;
	bool shared;
} ClusterRelmapV2Namespace;

typedef struct ClusterRelmapV2Owner {
	uint16 node;
	uint64 top_full_xid;
	uint64 membership_epoch;
	uint64 admitted_incarnation;
	uint64 wal_end;
	uint64 boot_incarnation;
} ClusterRelmapV2Owner;

typedef struct ClusterRelmapV2Image {
	ClusterRelmapV2Namespace identity;
	uint64 committed_generation;
	uint64 pending_generation;
	ClusterRelmapV2Owner pending_owner;
	RelMapFile committed;
	RelMapFile pending;
} ClusterRelmapV2Image;

typedef enum ClusterRelmapV2Validity {
	CLUSTER_RELMAP_V2_VALID,
	CLUSTER_RELMAP_V2_INVALID_ARGUMENT,
	CLUSTER_RELMAP_V2_INVALID_LENGTH,
	CLUSTER_RELMAP_V2_INVALID_FORMAT,
	CLUSTER_RELMAP_V2_INVALID_CRC,
	CLUSTER_RELMAP_V2_INVALID_IDENTITY,
	CLUSTER_RELMAP_V2_INVALID_GENERATION,
	CLUSTER_RELMAP_V2_INVALID_OWNER,
	CLUSTER_RELMAP_V2_INVALID_IMAGE
} ClusterRelmapV2Validity;

typedef enum ClusterRelmapV2PairResult {
	CLUSTER_RELMAP_V2_PAIR_VALID,
	CLUSTER_RELMAP_V2_PAIR_DEGRADED,
	CLUSTER_RELMAP_V2_PAIR_INVALID,
	CLUSTER_RELMAP_V2_PAIR_CONFLICT
} ClusterRelmapV2PairResult;

typedef struct ClusterRelmapV2View {
	RelMapFile committed;
	uint64 committed_generation;
	bool used_backup;
	/* False on fallback: zero known pending entries is then NOT an empty set. */
	bool pending_complete;
	bool has_pending;
	uint64 pending_generation;
	ClusterRelmapV2Owner pending_owner;
	RelMapFile pending;
} ClusterRelmapV2View;

/* Decode checks shape and namespace; it does not authenticate an owner/WAL. */
extern ClusterRelmapV2Validity cluster_relmap_v2_decode(const void *bytes, size_t length,
														const ClusterRelmapV2Namespace *expected,
														ClusterRelmapV2Image *output);
extern ClusterRelmapV2Validity cluster_relmap_v2_encode(const ClusterRelmapV2Image *image,
														void *bytes, size_t length);

/* Pending output is a retention obligation, never a terminal/release proof. */
extern ClusterRelmapV2PairResult
cluster_relmap_v2_read_pair(const void *primary, size_t primary_length, const void *backup,
							size_t backup_length, const ClusterRelmapV2Namespace *expected,
							ClusterRelmapV2View *output);

#endif /* CLUSTER_RELMAP_V2_H */
