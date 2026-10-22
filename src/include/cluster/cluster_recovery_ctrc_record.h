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
#include "access/xlogreader.h"

#define CLUSTER_RECOVERY_CTRC_VERSION UINT16_C(2)
#define CLUSTER_RECOVERY_CTRC_BYTES 320
#define CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN UINT32_C(1)
/* Frozen encoding only; no resource-manager registration or live dispatch. */
#define CLUSTER_RECOVERY_CTRC_WAL_INFO UINT8_C(0xD0)

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

/* Read only the current record after a successful original native WAL read.
 * Require its actual positions, standalone recovery frame and original
 * system/expected-thread selection. Native reader CRC/framing, source/cut,
 * retention and owner authority remain prerequisites, not proofs produced by
 * this adapter. No raw-record SHA/reference, graph, target applicability or
 * completion is derived here. Never treat the decoded header as complete raw
 * bytes. On refusal clear *out; out must not overlap reader-owned storage. */
extern bool cluster_recovery_ctrc_read_v2(const XLogReaderState *reader,
										  ClusterRecoveryCTRCReleaseV2 *out);

/* Logical R fields, never an on-disk struct or an owner completion proof. */
typedef struct ClusterRecoveryCTRCReferenceV2 {
	uint32 source_index;
	XLogRecPtr start;
	XLogRecPtr end;
	uint32 raw_length;
	uint8 raw_record_sha256[32];
} ClusterRecoveryCTRCReferenceV2;

/* Bind a successfully read standalone D0 to the original owner's complete,
 * immutable final raw snapshot. Require exact native header, long-data frame
 * and all payload bytes; hash those supplied bytes, never reconstructed data
 * or a private reader buffer. Positions come only from the current reader.
 *
 * The caller retains the original ROOT-selected source index/identity, cut,
 * pins and owner authority. This function neither qualifies those inputs nor
 * proves graph completeness, target durability, release or completion. No
 * live writer/dispatch. On refusal clear *out. Output may alias raw/source
 * input consumed before publication, but must not overlap reader storage. */
extern bool cluster_recovery_ctrc_reference_v2(const XLogReaderState *reader, uint32 source_index,
											   const uint8 source_identity_digest[32],
											   const uint8 *raw_record, size_t raw_length,
											   ClusterRecoveryCTRCReferenceV2 *out);

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

/* Prepare a private complete native after image from exact EXPECTED_BEFORE.
 * Reuse the matcher's native checks; change only the selected slot's release
 * bit and reseal the existing page checksum. EXPECTED_AFTER refuses: this
 * function supplies no idempotency, obsolete or application authorization.
 * Original graph/source/cut/owner proofs, window, WAL flush, held target I/O
 * and direct sync/readback remain independently required by the executor.
 * No shared target mutation, I/O or live dispatch occurs here.
 *
 * All lengths must be exact (320, BLCKSZ, BLCKSZ). Alignment is unrestricted.
 * Inputs are consumed before output publication, so out may overlap either
 * input. On refusal clear min(out_length, BLCKSZ) bytes when out is non-NULL. */
extern bool cluster_recovery_ctrc_target_image_v2(const uint8 *payload, size_t payload_length,
												  uint64 system_identifier, const void *header_page,
												  size_t header_length, void *out,
												  size_t out_length);

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

/* Immutable canonical bytes supplied by the original graph producers. */
typedef struct ClusterRecoveryCTRCGraphBytesV2 {
	const uint8 *data;
	size_t length;
} ClusterRecoveryCTRCGraphBytesV2;

typedef struct ClusterRecoveryCTRCGraphNodeV2 {
	uint16 kind; /* 1 reference/history, 2 original-owner proof */
	ClusterRecoveryCTRCGraphBytesV2 key;
	ClusterRecoveryCTRCGraphBytesV2 evidence;
} ClusterRecoveryCTRCGraphNodeV2;

typedef struct ClusterRecoveryCTRCGraphEdgeV2 {
	uint16 kind; /* 1 requires, 2 references, 3 retained_history, 4 discharged_by */
	size_t from; /* indices into the supplied nodes, never hashed as indices */
	size_t to;
} ClusterRecoveryCTRCGraphEdgeV2;

typedef struct ClusterRecoveryCTRCGraphV2 {
	const ClusterRecoveryCTRCGraphNodeV2 *nodes;
	size_t node_count;
	const ClusterRecoveryCTRCGraphEdgeV2 *edges;
	size_t edge_count;
	const size_t *roots; /* indices into nodes; canonical root ids must be unique */
	size_t root_count;
} ClusterRecoveryCTRCGraphV2;

/* Canonical transcript hash only, outside critical sections/spinlocks. The
 * original producers must already have canonical key/evidence atoms, lawful
 * roots/edges, complete reference/effect coverage, applicable owner rules and
 * witnesses, current/future-record exclusion and final source/ROOT checks.
 * This primitive does not parse/qualify those bytes or establish those proofs.
 * Hash success supplies no D0 admission, native base/floor/issued/terminal,
 * target durability, release or live execution authority.
 *
 * Sort full encoded ids/edges as unsigned bytes, collapse identical nodes/
 * edges, reject conflicting evidence, duplicate roots, missing endpoints and
 * unreachable owner nodes. At least one reachable owner is required; legal
 * cycles are permitted. Scratch is bounded by MaxAllocSize; scratch allocation
 * failure refuses. The original codec/crypto ERROR behavior is unchanged.
 * Bind the record's canonical fields with input/source-target hashes;
 * graph_sha256 is ignored to avoid self-reference. Inputs remain immutable for
 * the call. out may alias any input consumed before publication; on refusal
 * clear exactly 32 bytes. No graph builder, proof verifier, I/O or live caller. */
extern bool cluster_recovery_ctrc_graph_hash_v2(const ClusterRecoveryCTRCReleaseV2 *record,
												uint64 system_identifier,
												const ClusterRecoveryCTRCGraphV2 *graph,
												uint8 out[32]);
#endif
#endif
