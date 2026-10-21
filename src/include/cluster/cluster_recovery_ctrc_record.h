/*-------------------------------------------------------------------------
 * cluster_recovery_ctrc_record.h
 *    Canonical recovery CTRC release payload encoding.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_CTRC_RECORD_H
#define CLUSTER_RECOVERY_CTRC_RECORD_H

#include "cluster/cluster_tt_slot.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_RECOVERY_CTRC_VERSION UINT16_C(2)
#define CLUSTER_RECOVERY_CTRC_BYTES 320
#define CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN UINT32_C(1)

/* Logical fields, never an on-disk struct. Reserved bytes and the payload/
 * identity hashes are derived by the codec, not supplied by the caller. */
typedef struct ClusterRecoveryCTRCReleaseV2 {
	uint16 version;
	uint16 length;
	uint32 flags;
	uint64 root_id;
	uint64 root_generation;
	uint64 formation_epoch;
	uint64 recovery_serial;
	uint64 database_incarnation;
	uint64 writer_boot;
	uint64 covered_history;
	uint64 highest_fpi_end;
	uint64 cohort_bitmap;
	uint16 origin_node_id;
	uint16 writer_thread;
	uint16 target_owner;
	uint16 slot_offset;
	uint16 wrap;
	uint8 terminal_outcome;
	uint32 segment_id;
	uint32 segment_generation;
	TransactionId xid;
	TTSlot before;
	TTSlot after;
	uint8 graph_sha256[32];
	uint8 input_sha256[32];
} ClusterRecoveryCTRCReleaseV2;

/* Only payload shape and integrity. Neither function proves graph/input
 * completeness, native base/floor/issued, authority, durable release or redo
 * applicability. Original reader and owner proofs remain mandatory. There is
 * no WAL registration or writer here. system_identifier is the original
 * ROOT-selected identity, external to the fixed payload. Exact length only;
 * on refusal encode clears up to 320 output bytes, decode clears *out. */
extern bool cluster_recovery_ctrc_encode_v2(const ClusterRecoveryCTRCReleaseV2 *record,
											uint64 system_identifier, uint8 *bytes, size_t length);
extern bool cluster_recovery_ctrc_decode_v2(const uint8 *bytes, size_t length,
											uint64 system_identifier,
											ClusterRecoveryCTRCReleaseV2 *out);
#endif
#endif
