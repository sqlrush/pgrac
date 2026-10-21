/*-------------------------------------------------------------------------
 * cluster_recovery_ctrc_record.h
 *    Canonical recovery CTRC release payload encoding.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_CTRC_RECORD_H
#define CLUSTER_RECOVERY_CTRC_RECORD_H

#include "access/xlogdefs.h"
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

typedef enum ClusterRecoveryCTRCTargetMatchV2 {
	CLUSTER_RECOVERY_CTRC_TARGET_REFUSED = 0,
	CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_BEFORE,
	CLUSTER_RECOVERY_CTRC_TARGET_EXPECTED_AFTER
} ClusterRecoveryCTRCTargetMatchV2;

/* Read-only image classification, never APPLY, idempotency or obsolete proof.
 * Requires a canonical payload and a complete immutable native header image
 * (exact BLCKSZ bytes). Checks original native identity/integrity, exact
 * generation and the complete selected TT slot. Newer generation/wrap refuses
 * without an original REUSE proof. Input alignment is unrestricted.
 *
 * EXPECTED_AFTER still needs the original typed full idempotency proof. Neither
 * result supplies ROOT/source/cut/owner authority, checkpoint coverage, graph
 * completeness, release or completion. Those original gates remain mandatory;
 * no I/O, mutation or live redo dispatch occurs here. */
extern ClusterRecoveryCTRCTargetMatchV2
cluster_recovery_ctrc_target_match_v2(const uint8 *payload, size_t payload_length,
									  uint64 system_identifier, const void *header_page,
									  size_t header_length);

/* Hash the complete final native record (including CRC), outside its payload.
 * start/end must be the original reader/insert owner's actual positions; end
 * may include intervening WAL page headers. Checks only length/CRC and domain
 * shape, not D0 opcode/payload, reader/source/cut authority, retention, graph
 * completeness or completion. Original reader, codec and owner proofs remain
 * mandatory. No insertion or redo. Output is 32 bytes, cleared on refusal;
 * it may alias an input, which is consumed before output publication. */
extern bool cluster_recovery_ctrc_record_hash_v2(const uint8 source_identity_digest[32],
												 XLogRecPtr start, XLogRecPtr end,
												 const uint8 *raw_record, size_t raw_length,
												 uint8 out[32]);
#endif
#endif
