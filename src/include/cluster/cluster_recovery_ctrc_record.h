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
#include "cluster/cluster_recovery_namespace.h"
#include "cluster/cluster_recovery_reference_graph.h"
#include "cluster/cluster_recovery_tt_record.h"

#define CLUSTER_RECOVERY_CTRC_VERSION UINT16_C(2)
#define CLUSTER_RECOVERY_CTRC_BYTES 320
#define CLUSTER_RECOVERY_CTRC_RELEASE_PROVEN UINT32_C(1)
/* Frozen D0 opcode; live redo dispatch remains disabled. */
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

/* Canonical atom1: U16(1) || B(R) || B(complete native raw record).
 * R is 56 field-coded bytes; this adapter accepts only standalone D0. */
#define CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES                                                       \
	(2 + 8 + 56 + 8 + SizeOfXLogRecord + SizeOfXLogRecordDataHeaderLong                            \
	 + CLUSTER_RECOVERY_CTRC_BYTES)

/* Reuse reference_v2's exact current-reader/raw binding, then encode its R
 * and the unmodified raw bytes. Original source/cut/order/component ownership,
 * pins, prior D0 completion and current/future D0 exclusion remain the graph
 * producer's prerequisites. No owner completion, full graph, release, target
 * durability or live dispatch is established by encoding an atom.
 *
 * Output length must equal WAL_ATOM_BYTES. Refusal clears min(out_length,
 * WAL_ATOM_BYTES) bytes for non-NULL out. Unaligned buffers and raw/source
 * aliases are supported; output must not overlap reader-owned storage. */
extern bool cluster_recovery_ctrc_wal_atom_v2(const XLogReaderState *reader, uint32 source_index,
											  const uint8 source_identity_digest[32],
											  const uint8 *raw_record, size_t raw_length,
											  uint8 *out, size_t out_length);

#define CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT 3
#define CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES 116
#define CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_EVIDENCE_BYTES                                         \
	(16 + CLUSTER_RECOVERY_CTRC_WAL_ATOM_BYTES)

/* Byte encodings, not native structs on disk. keys[0..2] identify the target
 * xid, before-slot xid and after-slot xid, respectively. All share evidence. */
typedef struct ClusterRecoveryCTRCWalReferencesV2 {
	uint8 keys[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT]
			  [CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_EVIDENCE_BYTES];
} ClusterRecoveryCTRCWalReferencesV2;

/* Derive these three kind1 keys from the codec-validated original native D0,
 * with actual raw field offsets and singleton ordinal zero. Reuse atom_v2's
 * exact reader/raw binding; evidence contains that one complete WAL atom.
 * Original source/cut/component authority, pins, prior completion, exclusion
 * of current/future D0 and final ROOT checks remain caller prerequisites.
 * This is only the D0 transaction-field inventory, not a complete graph,
 * original-owner proof, target durability, release or live admission.
 *
 * Publish all keys/evidence together after consuming inputs. On refusal clear
 * *out. Unaligned buffers and raw/source aliases are allowed; output must not
 * overlap reader-owned storage. */
extern bool cluster_recovery_ctrc_wal_references_v2(const XLogReaderState *reader,
													uint32 source_index,
													const uint8 source_identity_digest[32],
													const uint8 *raw_record, size_t raw_length,
													ClusterRecoveryCTRCWalReferencesV2 *out);

/* Original fixed native BIND payload xid only, not all WAL transaction fields.
 * The accepted short-data record is 32-byte header + 2-byte frame + 24 bytes. */
#define CLUSTER_RECOVERY_CTRC_BIND_RAW_BYTES 58
#define CLUSTER_RECOVERY_CTRC_BIND_ATOM_BYTES (74 + CLUSTER_RECOVERY_CTRC_BIND_RAW_BYTES)
#define CLUSTER_RECOVERY_CTRC_BIND_EVIDENCE_BYTES (16 + CLUSTER_RECOVERY_CTRC_BIND_ATOM_BYTES)
typedef struct ClusterRecoveryCTRCBindReferenceV2 {
	uint8 key[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_BIND_EVIDENCE_BYTES];
} ClusterRecoveryCTRCBindReferenceV2;

/* Read-only BIND field extraction through the original native decoder and
 * historical field inspection, completed selected-thread reader, exact raw
 * header/frame/payload and full CRC/source hash. database_incarnation is the
 * original ROOT-selected incarnation; BIND itself does not carry it. Only the
 * original fixed short-data shape is accepted, with no extra frames. The
 * header xid is retained in raw evidence but is NOT inventoried by this API.
 *
 * This supplies one kind1 key and singleton atom1 evidence, not qualified
 * base/floor/last-issued/terminal, a complete graph or release authorization.
 * Source/cut/component/owner authority, retention pins and final ROOT checks
 * remain original caller prerequisites. No I/O, mutation or live dispatch.
 *
 * All inputs are consumed before publishing *out; refusal clears *out.
 * Unaligned raw/output and raw/source output aliases are allowed. Output must
 * not overlap reader-owned storage. The original decoder's non-const reader
 * parameter is retained; this call does not change the reader. */
extern bool cluster_recovery_ctrc_bind_reference_v2(XLogReaderState *reader,
												 uint64 database_incarnation, uint32 source_index,
												 const uint8 source_identity_digest[32],
												 const uint8 *raw_record, size_t raw_length,
												 ClusterRecoveryCTRCBindReferenceV2 *out);

/* One original payload xid reference from BIND24, exact ABORT24 or the
 * generation-bearing CTRC_RELEASE96. Evidence has its actual encoded length;
 * the remaining bytes are zero. evidence_length is caller metadata, not part
 * of the canonical evidence or graph hash. */
#define CLUSTER_RECOVERY_CTRC_TT_RAW_MAX_BYTES 130
#define CLUSTER_RECOVERY_CTRC_TT_ATOM_MAX_BYTES (74 + CLUSTER_RECOVERY_CTRC_TT_RAW_MAX_BYTES)
#define CLUSTER_RECOVERY_CTRC_TT_EVIDENCE_MAX_BYTES (16 + CLUSTER_RECOVERY_CTRC_TT_ATOM_MAX_BYTES)
typedef struct ClusterRecoveryCTRCTTReferenceV2 {
	uint8 key[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_TT_EVIDENCE_MAX_BYTES];
	size_t evidence_length;
} ClusterRecoveryCTRCTTReferenceV2;

/* Same original decoder/inspection, completed-reader, full raw/source and
 * caller authority prerequisites as BIND above. Only exact short main-data
 * shapes are accepted. Legacy ABORT, COMMIT and SET_HEAD lack a generation
 * and refuse; a zero generation carried by an accepted payload is valid.
 * Header xid is evidence only, not another inventoried field. This component
 * does not supply graph completeness, semantic terminal/owner qualification,
 * execution, release or a live caller. The existing BIND API remains BIND-only.
 *
 * Consume inputs before publishing the whole result; refusal clears *out.
 * Unaligned raw/output and raw/source output aliases are supported, except
 * overlap with reader-owned storage. No reader or shared-state mutation. */
extern bool cluster_recovery_ctrc_tt_reference_v2(XLogReaderState *reader,
											 uint64 database_incarnation, uint32 source_index,
											 const uint8 source_identity_digest[32],
											 const uint8 *raw_record, size_t raw_length,
											 ClusterRecoveryCTRCTTReferenceV2 *out);

/* Inventory the complete xid fields of one of the above exact TT carriers.
 * Only an absent header xid is accepted, leaving the sole original payload
 * field. Every nonzero header xid refuses: even an equal raw xid cannot prove
 * that header and payload have the same full transaction identity. The
 * payload-only API above keeps its original header-as-evidence contract.
 *
 * Same caller prerequisites, alias/clearing rules and absence of semantic
 * graph/owner qualification or live authority as the payload adapter. This
 * does not inventory other native WAL shapes or resolve unknown identities. */
extern bool cluster_recovery_ctrc_tt_wal_inventory_v2(
	XLogReaderState *reader, uint64 database_incarnation, uint32 source_index,
	const uint8 source_identity_digest[32], const uint8 *raw_record, size_t raw_length,
	ClusterRecoveryCTRCTTReferenceV2 *out);

/* Four independent native TT-ABORT fields share one exact raw-record atom.
 * The C0 payload has no xid-bearing WAL header; this adapter inventories only
 * the complete target/top/exact/before fields and keeps all source, cut,
 * owner, graph and durability qualification with the caller. */
#define CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT 4
#define CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES \
	(SizeOfXLogRecord + SizeOfXLogRecordDataHeaderShort + CLUSTER_RECOVERY_TT_ABORT_BYTES)
#define CLUSTER_RECOVERY_CTRC_TT_ABORT_ATOM_BYTES \
	(74 + CLUSTER_RECOVERY_CTRC_TT_ABORT_RAW_BYTES)
#define CLUSTER_RECOVERY_CTRC_TT_ABORT_EVIDENCE_BYTES \
	(16 + CLUSTER_RECOVERY_CTRC_TT_ABORT_ATOM_BYTES)
typedef struct ClusterRecoveryCTRCTTAbortReferencesV2 {
	uint8 keys[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT]
		[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_KEY_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_TT_ABORT_EVIDENCE_BYTES];
	size_t evidence_length;
} ClusterRecoveryCTRCTTAbortReferencesV2;

/* Inventory native TT-ABORT's four complete transaction fields as one
 * all-or-nothing group. The exact short-data frame, decoded C0 payload and
 * full raw CRC/hash are required; no semantic terminal, owner, graph,
 * release, floor/issued or live-admission authority is supplied. */
extern bool cluster_recovery_ctrc_tt_abort_wal_inventory_v2(
	XLogReaderState *reader, uint64 database_incarnation, uint32 source_index,
	const uint8 source_identity_digest[32], const uint8 *raw_record, size_t raw_length,
	ClusterRecoveryCTRCTTAbortReferencesV2 *out);

/* One selected native block-0 xid field, with the complete original page as
 * singleton atom2 evidence. Lengths are metadata, not encoded graph bytes;
 * unused array tails are zero. The path itself is not NUL-terminated on wire. */
#define CLUSTER_RECOVERY_CTRC_TT_DATA_KEY_MAX_BYTES (106 + MAXPGPATH - 1)
#define CLUSTER_RECOVERY_CTRC_TT_DATA_EVIDENCE_MAX_BYTES (96 + BLCKSZ + MAXPGPATH - 1)
typedef struct ClusterRecoveryCTRCTTDataReferenceV2 {
	uint8 key[CLUSTER_RECOVERY_CTRC_TT_DATA_KEY_MAX_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_TT_DATA_EVIDENCE_MAX_BYTES];
	size_t key_length;
	size_t evidence_length;
} ClusterRecoveryCTRCTTDataReferenceV2;

/* Encode an original namespace-selected, owner-reread immutable TT block 0.
 * Reuse the original path classifier, native header identity and VALID page
 * checksum. The selected physical slot must carry a normal xid, known status,
 * wrap and generation. No terminal polarity or release is inferred.
 *
 * The caller supplies the original ROOT system/incarnation, current namespace
 * entry and complete owner reread; source/cut/physical identity, pins, final
 * namespace/ROOT checks and owner qualification remain caller prerequisites.
 * This byte adapter supplies no qualified base/floor/last-issued, semantic
 * graph completeness, I/O, mutation, completion or live admission. The full
 * native page retains UBA128 bytes; no U64 UNDO carrier is derived here.
 *
 * Exact BLCKSZ input only. Consume all inputs before publishing *out; refusal
 * clears *out. Entry/page/output may be unaligned and may alias each other. */
extern bool cluster_recovery_ctrc_tt_data_reference_v2(
	uint64 system_identifier, uint64 database_incarnation,
	const ClusterRecoveryNamespaceEntry *entry, const void *header_page, size_t header_length,
	uint16 slot_offset, ClusterRecoveryCTRCTTDataReferenceV2 *out);

/* All physical TT xid fields of one original block 0, in physical order.
 * Keys share key_length and the same complete singleton atom2 evidence; tails
 * are zero. reference_count may be zero, but full object evidence remains.
 * An empty result is not a graph-completeness or terminal/release proof. */
typedef struct ClusterRecoveryCTRCTTDataInventoryV2 {
	uint8 keys[TT_SLOTS_PER_SEGMENT][CLUSTER_RECOVERY_CTRC_TT_DATA_KEY_MAX_BYTES];
	uint8 evidence[CLUSTER_RECOVERY_CTRC_TT_DATA_EVIDENCE_MAX_BYTES];
	size_t reference_count;
	size_t key_length;
	size_t evidence_length;
} ClusterRecoveryCTRCTTDataInventoryV2;

/* Same original namespace/owner reread, ROOT/source/cut/pin and final-check
 * prerequisites as the selected-slot adapter. Reuse the native header scanner
 * for all 48 slots: counters, bitmap, lifecycle and RELEASED never shorten it.
 * Only a byte-exact zero UNUSED slot is omitted. Any residual UNUSED or invalid
 * nonempty slot refuses the whole inventory. No terminal polarity, complete
 * semantic graph, qualified base/floor/last-issued or release is derived.
 * Native UBA128 remains full-page DATA evidence, not a U64 UNDO carrier.
 *
 * Exact BLCKSZ input; unaligned inputs/output and input/output aliases allowed.
 * Consume all inputs before whole-output publication; refusal clears *out.
 * No I/O, shared mutation, live caller or new authority. */
extern bool cluster_recovery_ctrc_tt_data_inventory_v2(
	uint64 system_identifier, uint64 database_incarnation,
	const ClusterRecoveryNamespaceEntry *entry, const void *header_page, size_t header_length,
	ClusterRecoveryCTRCTTDataInventoryV2 *out);

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

/* Logical original-decoder fields, never persistent native struct bytes. */
typedef struct ClusterRecoveryCTRCOwnerEffectV2 {
	ClusterRecoveryCTRCReferenceV2 record;
	uint16 component_kind;
	uint32 component_ordinal;
} ClusterRecoveryCTRCOwnerEffectV2;

typedef struct ClusterRecoveryCTRCOwnerScopeV2 {
	ClusterRecoveryTxnKey transaction;
	uint16 owner_family; /* frozen matrix row 1..14, not a source enum */
	const uint32 *source_indexes; /* original complete input/ROOT ordering */
	size_t source_count;
	const ClusterRecoveryCTRCOwnerEffectV2 *effects;
	size_t effect_count;
} ClusterRecoveryCTRCOwnerScopeV2;

/* Encode only kind2.key = U16(owner_family)||B(scope), where scope is
 * T34||U64(source_count)||sorted(U32 sources)||U64(effect_count)||sorted(
 * R56||U16(component_kind)||U32(component_ordinal)). Exact out_length is
 * 60 + 4*source_count + 62*effect_count, bounded by MaxAllocSize. Sort complete
 * unsigned encoded bytes; reject duplicate sources/effect IDs and effects
 * outside the supplied sources. R fields receive only structural checks.
 * Component kinds/ordinals, complete effect/input coverage and lawful overlap
 * remain the original decoder/owner's responsibility. Empty effects do not
 * prove a qualified base or no effect. No evidence/completion verification,
 * source/ROOT/owner qualification, graph admission, I/O, release or live caller.
 *
 * Outside critical sections/spinlocks. One bounded NO_OOM scratch allocation;
 * allocation failure refuses. Inputs are immutable during the call, may be
 * unaligned, and may overlap out: consume before publication. On refusal clear
 * min(out_length,60) bytes if out is non-NULL; no partial encoded tail is ever
 * published. The caller supplies valid storage for every declared input. */
extern bool cluster_recovery_ctrc_owner_key_v2(const ClusterRecoveryCTRCOwnerScopeV2 *scope,
	uint8 *out, size_t out_length);

/* Immutable canonical bytes supplied by the original graph producers. */
typedef struct ClusterRecoveryCTRCGraphBytesV2 {
	const uint8 *data;
	size_t length;
} ClusterRecoveryCTRCGraphBytesV2;

/* Canonical raw witness evidence: U64(count) || concat(B(atom)), where each
 * input atom is U16(kind) || B(subject) || B(bytes). Only raw WAL kind1 and
 * native DATA/UNDO kind2 envelopes are supported; subject/bytes are nonempty.
 * Sort complete unsigned encoded bytes, collapse exact duplicates, and refuse
 * conflicting bytes for the same kind/subject. This structural codec does not
 * qualify subjects or verify raw CRC/source, owner rules or completion. Inputs
 * must come from the original native encoders with the caller's source/cut/
 * ROOT/pin/owner reread and final revalidation prerequisites intact. An empty
 * encoding is not permission to discharge an obligation. No live caller,
 * completion atom3/4, graph admission, I/O, durability or release authority.
 *
 * Outside critical sections/spinlocks; one MaxAllocSize-bounded NO_OOM scratch.
 * Inputs must remain immutable and have valid storage for their declared
 * sizes. Views/bytes may be unaligned and overlap out: all are consumed before
 * publication. out_length is disjoint caller metadata (may be unaligned), not
 * encoded. Capacity is at most MaxAllocSize. Success writes the exact canonical
 * length, leaving any tail unchanged. Refusal sets out_length to zero if given
 * and clears min(out_capacity,8) bytes if out is non-NULL; no partial new tail
 * is published. Allocation failure refuses. */
extern bool cluster_recovery_ctrc_raw_evidence_v2(const ClusterRecoveryCTRCGraphBytesV2 *atoms,
	size_t atom_count, uint8 *out, size_t out_capacity, size_t *out_length);

typedef struct ClusterRecoveryCTRCGraphNodeV2 {
	uint16 kind; /* 1 reference/history, 2 original-owner proof */
	ClusterRecoveryCTRCGraphBytesV2 key;
	ClusterRecoveryCTRCGraphBytesV2 evidence;
} ClusterRecoveryCTRCGraphNodeV2;

/* Expose one complete TT-ABORT inventory as four canonical reference nodes.
 * The nodes borrow the inventory's immutable key/evidence storage and are
 * published only after the shared raw atom and all four field offsets pass
 * exact structural checks. This adapter supplies no graph completeness,
 * owner/terminal/base qualification, durability, release or live authority.
 * Refusal clears the fixed output array; no allocation or input mutation. */
extern bool cluster_recovery_ctrc_tt_abort_graph_nodes_v2(
	const ClusterRecoveryCTRCTTAbortReferencesV2 *references,
	ClusterRecoveryCTRCGraphNodeV2 out[CLUSTER_RECOVERY_CTRC_TT_ABORT_REFERENCE_COUNT]);

/* Expose one complete native TT reference inventory as a borrowed kind-1
 * graph node. The singleton raw atom and key metadata are rechecked before
 * publication; this remains structural correspondence without graph,
 * owner/terminal/base/floor/issued, durability, release or live authority. */
extern bool cluster_recovery_ctrc_tt_reference_graph_node_v2(
	const ClusterRecoveryCTRCTTReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out);

/* Expose one dormant native BIND reference as a borrowed kind-1 graph node.
 * The fixed BIND atom/key shape is rechecked before publication; no graph,
 * owner, terminal/base/floor/issued, durability, release or live authority. */
extern bool cluster_recovery_ctrc_bind_graph_node_v2(
	const ClusterRecoveryCTRCBindReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out);

/* Expose the three-field native WAL reference inventory as borrowed kind-1
 * graph nodes. The shared raw atom, subject bytes and physical offsets are
 * rechecked before whole-array publication; no graph or admission authority. */
extern bool cluster_recovery_ctrc_wal_reference_graph_nodes_v2(
	const ClusterRecoveryCTRCWalReferencesV2 *references,
	ClusterRecoveryCTRCGraphNodeV2 out[CLUSTER_RECOVERY_CTRC_WAL_REFERENCE_COUNT]);

/* Expose one selected native TT-DATA reference as a borrowed kind-1 graph
 * node. The complete page atom, DATA identity and physical slot coordinates
 * are rechecked; no graph, owner, terminal/base/floor/issued, durability,
 * release or live authority is supplied. */
extern bool cluster_recovery_ctrc_tt_data_reference_graph_node_v2(
	const ClusterRecoveryCTRCTTDataReferenceV2 *reference,
	ClusterRecoveryCTRCGraphNodeV2 *out);

/* Expose a complete DATA inventory as borrowed kind-1 graph reference nodes.
 * The shared atom envelope, key lengths/identity bytes and physical slot
 * ordinals are rechecked before publishing the whole fixed descriptor array.
 * This is structural correspondence only: no graph completeness, owner,
 * terminal/base/floor/issued, durability, release or live authority. */
extern bool cluster_recovery_ctrc_tt_data_graph_nodes_v2(
	const ClusterRecoveryCTRCTTDataInventoryV2 *inventory,
	ClusterRecoveryCTRCGraphNodeV2 out[TT_SLOTS_PER_SEGMENT], size_t *out_count);

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

/* Read the original completed native D0 record and require its embedded graph
 * SHA to equal the original canonical graph hash before publishing the result.
 * This structural correspondence does not qualify graph completeness, original
 * source/cut/ROOT/pins, owner/base/floor/issued/terminal or durability/release.
 * All original read_v2 and graph_hash_v2 prerequisites and ERROR behavior apply;
 * no live caller or execution authority. Outside critical sections/spinlocks.
 * Inputs remain immutable throughout. Graph byte views and out may be unaligned;
 * graph descriptors follow graph_hash_v2. Graph inputs may overlap out, consumed
 * before publication. out must not overlap reader-owned storage. Refusal clears
 * exactly sizeof(*out) bytes when non-NULL; success copies the whole result. */
extern bool cluster_recovery_ctrc_read_graph_v2(const XLogReaderState *reader,
	const ClusterRecoveryCTRCGraphV2 *graph, ClusterRecoveryCTRCReleaseV2 *out);
#endif
#endif
