/*-------------------------------------------------------------------------
 * cluster_r4_operation.h
 *   Independent compare context and peer operation receipt for R4 undo.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_OPERATION_H
#define CLUSTER_R4_OPERATION_H

#include "cluster/cluster_savepoint_reader.h"
#include "port/pg_crc32c.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_R4_OPERATION_WIRE_PRESENT 1
#define CLUSTER_R4_OPERATION_CONTEXT_VERSION 1
#define CLUSTER_R4_OPERATION_RETAINED UINT32_C(1)
#define CLUSTER_R4_OPERATION_ORDERED UINT32_C(1)
#define CLUSTER_R4_OPERATION_CHILD UINT32_C(2)
#define CLUSTER_R4_OPERATION_V1 UINT32_C(4)
#define CLUSTER_R4_OPERATION_PROOF_MAGIC UINT32_C(0x504f3452)
#define CLUSTER_R4_OPERATION_REPLY_SIZE 8352
#define CLUSTER_R4_OPERATION_CHECKSUM_SIZE 8304

typedef struct ClusterR4OperationContextWire {
	uint8 bytes[100];
} ClusterR4OperationContextWire;
typedef struct ClusterR4OperationForwardPayload {
	uint8 bytes[196];
} ClusterR4OperationForwardPayload;
typedef struct ClusterR4PeerOperationWire {
	uint8 bytes[96];
} ClusterR4PeerOperationWire;

typedef struct ClusterR4OperationRequest {
	BufferTag tag;
	ClusterItlSlotData head;
	bool retained;
	ClusterSavepointReaderIdentity identity;
} ClusterR4OperationRequest;

/* A peer receipt never becomes a ClusterTxResolution local_* observation.
 * The enclosing exact request/epoch/admission/route and DATA CRC are required
 * in addition to this value. It has no cache or retention authority. */
typedef struct ClusterR4PeerOperationProof {
	ClusterSavepointReaderIdentity identity;
	uint32 flags;
	uint8 effect, record_format;
	ClusterTxOutcome outcome;
	ClusterTxProofKind proof_kind;
	SCN commit_scn;
	uint64 admission_generation;
	UBA uba;
} ClusterR4PeerOperationProof;

static inline uint64
ClusterR4OperationRead(const uint8 *bytes, unsigned width)
{
	uint64 value = 0;
	for (unsigned i = 0; i < width; i++)
		value |= (uint64)bytes[i] << (8 * i);
	return value;
}

static inline void
ClusterR4OperationWrite(uint8 *bytes, unsigned width, uint64 value)
{
	for (unsigned i = 0; i < width; i++)
		bytes[i] = (uint8)(value >> (8 * i));
}

static inline bool
ClusterR4OperationIdentityValid(const ClusterSavepointReaderIdentity *id)
{
	return id != NULL && FullTransactionIdIsNormal(id->top_fxid)
		   && U64FromFullTransactionId(id->top_fxid) != UINT64_MAX
		   && id->canonical.xid == XidFromFullTransactionId(id->top_fxid)
		   && id->canonical.segment_id > 0 && id->canonical.segment_id <= UINT16_MAX
		   && id->canonical.reserved32 == 0
		   && (id->canonical.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
				  < UNDO_OWNER_INSTANCE_MAX
		   && id->canonical.slot_offset < TT_SLOTS_PER_SEGMENT && id->canonical.wrap <= TT_WRAP_MAX
		   && id->canonical_generation != UINT32_MAX;
}

static inline bool
ClusterR4OperationRequestValid(const ClusterR4OperationRequest *request)
{
	uint32 segment, block;
	uint16 slot, row;
	const ClusterItlSlotData *head;
	const ClusterSavepointReaderIdentity *id;
	ClusterSavepointReaderIdentity zero = { 0 };
	if (request == NULL)
		return false;
	head = &request->head;
	id = &request->identity;
	if (request->tag.forkNum != MAIN_FORKNUM || request->tag.blockNum == InvalidBlockNumber
		|| !TransactionIdIsNormal(head->xid) || !SCN_VALID(head->write_scn)
		|| !uba_decode_record(head->undo_segment_head, &segment, &block, &slot, &row)
		|| (head->flags != ITL_FLAG_ACTIVE && head->flags != ITL_FLAG_COMMITTED
			&& head->flags != ITL_FLAG_ABORTED && head->flags != ITL_FLAG_NEEDS_CLEANOUT
			&& !ITL_FLAG_IS_LOCK_ONLY(head->flags)))
		return false;
	if (!request->retained)
		return memcmp(id, &zero, sizeof(zero)) == 0;
	return ClusterR4OperationIdentityValid(id) && id->canonical.xid == head->xid
		   && FullTransactionIdEquals(id->top_fxid, id->operation_fxid)
		   && id->operation_sequence == 0 && id->command_id == InvalidCommandId
		   && id->subid == TopSubTransactionId;
}

static inline bool
ClusterR4OperationContextEncode(ClusterR4OperationContextWire *wire,
								const ClusterR4OperationRequest *request)
{
	uint8 *p;
	if (wire == NULL)
		return false;
	memset(wire, 0, sizeof(*wire));
	if (!ClusterR4OperationRequestValid(request))
		return false;
	p = wire->bytes;
	ClusterR4OperationWrite(p, 2, CLUSTER_R4_OPERATION_CONTEXT_VERSION);
	ClusterR4OperationWrite(p + 2, 2, sizeof(*wire));
	ClusterR4OperationWrite(p + 4, 4, request->retained ? CLUSTER_R4_OPERATION_RETAINED : 0);
	ClusterR4OperationWrite(p + 8, 4, request->tag.spcOid);
	ClusterR4OperationWrite(p + 12, 4, request->tag.dbOid);
	ClusterR4OperationWrite(p + 16, 4, request->tag.relNumber);
	ClusterR4OperationWrite(p + 20, 4, request->tag.forkNum);
	ClusterR4OperationWrite(p + 24, 4, request->tag.blockNum);
	ClusterR4OperationWrite(p + 28, 4, request->head.xid);
	ClusterR4OperationWrite(p + 32, 2, request->head.wrap);
	p[34] = request->head.flags;
	p[35] = request->head.lock_count;
	ClusterR4OperationWrite(p + 36, 8, request->head.undo_segment_head.raw[0]);
	ClusterR4OperationWrite(p + 44, 8, request->head.undo_segment_head.raw[1]);
	ClusterR4OperationWrite(p + 52, 8, request->head.commit_scn);
	ClusterR4OperationWrite(p + 60, 8, request->head.write_scn);
	ClusterR4OperationWrite(p + 68, 8, request->head.first_change_lsn);
	if (request->retained) {
		ClusterR4OperationWrite(p + 76, 8, U64FromFullTransactionId(request->identity.top_fxid));
		ClusterR4OperationWrite(p + 84, 4, request->identity.canonical.segment_id);
		ClusterR4OperationWrite(p + 88, 4, request->identity.canonical_generation);
		ClusterR4OperationWrite(p + 92, 4, request->identity.canonical.xid);
		ClusterR4OperationWrite(p + 96, 2, request->identity.canonical.slot_offset);
		ClusterR4OperationWrite(p + 98, 2, request->identity.canonical.wrap);
	}
	return true;
}

static inline bool
ClusterR4OperationContextDecode(const ClusterR4OperationContextWire *wire,
								ClusterR4OperationRequest *out)
{
	ClusterR4OperationRequest request = { 0 };
	const uint8 *p;
	uint32 flags;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (wire == NULL)
		return false;
	p = wire->bytes;
	flags = (uint32)ClusterR4OperationRead(p + 4, 4);
	if (ClusterR4OperationRead(p, 2) != CLUSTER_R4_OPERATION_CONTEXT_VERSION
		|| ClusterR4OperationRead(p + 2, 2) != sizeof(*wire)
		|| (flags & ~CLUSTER_R4_OPERATION_RETAINED) != 0)
		return false;
	request.tag.spcOid = (Oid)ClusterR4OperationRead(p + 8, 4);
	request.tag.dbOid = (Oid)ClusterR4OperationRead(p + 12, 4);
	request.tag.relNumber = (RelFileNumber)ClusterR4OperationRead(p + 16, 4);
	request.tag.forkNum = (ForkNumber)ClusterR4OperationRead(p + 20, 4);
	request.tag.blockNum = (BlockNumber)ClusterR4OperationRead(p + 24, 4);
	request.head.xid = (TransactionId)ClusterR4OperationRead(p + 28, 4);
	request.head.wrap = (uint16)ClusterR4OperationRead(p + 32, 2);
	request.head.flags = p[34];
	request.head.lock_count = p[35];
	request.head.undo_segment_head.raw[0] = ClusterR4OperationRead(p + 36, 8);
	request.head.undo_segment_head.raw[1] = ClusterR4OperationRead(p + 44, 8);
	request.head.commit_scn = (SCN)ClusterR4OperationRead(p + 52, 8);
	request.head.write_scn = (SCN)ClusterR4OperationRead(p + 60, 8);
	request.head.first_change_lsn = ClusterR4OperationRead(p + 68, 8);
	request.retained = flags == CLUSTER_R4_OPERATION_RETAINED;
	if (request.retained) {
		request.identity.top_fxid = request.identity.operation_fxid
			= FullTransactionIdFromU64(ClusterR4OperationRead(p + 76, 8));
		request.identity.canonical.segment_id = (uint32)ClusterR4OperationRead(p + 84, 4);
		request.identity.canonical_generation = (uint32)ClusterR4OperationRead(p + 88, 4);
		request.identity.canonical.xid = (TransactionId)ClusterR4OperationRead(p + 92, 4);
		request.identity.canonical.slot_offset = (uint16)ClusterR4OperationRead(p + 96, 2);
		request.identity.canonical.wrap = (uint16)ClusterR4OperationRead(p + 98, 2);
		request.identity.command_id = InvalidCommandId;
		request.identity.subid = TopSubTransactionId;
	} else {
		for (unsigned i = 76; i < 100; i++)
			if (p[i] != 0)
				return false;
	}
	if (!ClusterR4OperationRequestValid(&request))
		return false;
	*out = request;
	return true;
}

static inline bool
ClusterR4PeerOperationValid(const ClusterR4PeerOperationProof *proof)
{
	const ClusterSavepointReaderIdentity *id;
	uint32 segment, block;
	uint16 slot, row;
	if (proof == NULL)
		return false;
	id = &proof->identity;
	if (!ClusterR4OperationIdentityValid(id) || proof->admission_generation == 0
		|| !uba_decode_record(proof->uba, &segment, &block, &slot, &row)
		|| slot != id->canonical.slot_offset
		|| (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
			   != (id->canonical.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE
		|| proof->proof_kind != CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG
		|| (proof->outcome != CLUSTER_TX_COMMITTED && proof->outcome != CLUSTER_TX_ABORTED
			&& proof->outcome != CLUSTER_TX_IN_PROGRESS)
		|| (proof->outcome == CLUSTER_TX_COMMITTED ? !SCN_VALID(proof->commit_scn)
												   : proof->commit_scn != InvalidScn))
		return false;
	if (proof->flags == (CLUSTER_R4_OPERATION_ORDERED | CLUSTER_R4_OPERATION_CHILD))
		return proof->record_format == 2
			   && (proof->effect == CLUSTER_SAVEPOINT_EFFECT_LIVE
				   || proof->effect == CLUSTER_SAVEPOINT_EFFECT_DISCARDED)
			   && FullTransactionIdIsNormal(id->operation_fxid)
			   && U64FromFullTransactionId(id->operation_fxid) != UINT64_MAX
			   && U64FromFullTransactionId(id->operation_fxid)
					  > U64FromFullTransactionId(id->top_fxid)
			   && id->operation_sequence != 0 && id->command_id != InvalidCommandId
			   && id->subid > TopSubTransactionId;
	return proof->flags == (CLUSTER_R4_OPERATION_ORDERED | CLUSTER_R4_OPERATION_V1)
		   && proof->record_format == 1 && proof->effect == 0
		   && FullTransactionIdEquals(id->operation_fxid, id->top_fxid)
		   && id->operation_sequence == 0 && id->command_id == InvalidCommandId
		   && id->subid == TopSubTransactionId;
}

static inline bool
ClusterR4PeerOperationEncode(ClusterR4PeerOperationWire *wire,
							 const ClusterR4PeerOperationProof *proof)
{
	uint8 *p;
	const ClusterSavepointReaderIdentity *id;
	if (wire == NULL)
		return false;
	memset(wire, 0, sizeof(*wire));
	if (!ClusterR4PeerOperationValid(proof))
		return false;
	p = wire->bytes;
	id = &proof->identity;
	ClusterR4OperationWrite(p, 4, CLUSTER_R4_OPERATION_PROOF_MAGIC);
	ClusterR4OperationWrite(p + 4, 2, CLUSTER_R4_OPERATION_CONTEXT_VERSION);
	ClusterR4OperationWrite(p + 6, 2, sizeof(*wire));
	ClusterR4OperationWrite(p + 8, 4, proof->flags);
	ClusterR4OperationWrite(p + 12, 4, id->canonical.segment_id);
	ClusterR4OperationWrite(p + 16, 4, id->canonical_generation);
	ClusterR4OperationWrite(p + 20, 4, id->canonical.xid);
	ClusterR4OperationWrite(p + 24, 2, id->canonical.slot_offset);
	ClusterR4OperationWrite(p + 26, 2, id->canonical.wrap);
	p[28] = proof->effect;
	p[29] = proof->record_format;
	p[30] = (uint8)proof->outcome;
	p[31] = (uint8)proof->proof_kind;
	ClusterR4OperationWrite(p + 32, 8, U64FromFullTransactionId(id->top_fxid));
	ClusterR4OperationWrite(p + 40, 8, U64FromFullTransactionId(id->operation_fxid));
	ClusterR4OperationWrite(p + 48, 8, id->operation_sequence);
	ClusterR4OperationWrite(p + 56, 4, id->command_id);
	ClusterR4OperationWrite(p + 60, 4, id->subid);
	ClusterR4OperationWrite(p + 64, 8, proof->commit_scn);
	ClusterR4OperationWrite(p + 72, 8, proof->admission_generation);
	ClusterR4OperationWrite(p + 80, 8, proof->uba.raw[0]);
	ClusterR4OperationWrite(p + 88, 8, proof->uba.raw[1]);
	return true;
}

static inline bool
ClusterR4PeerOperationDecode(const ClusterR4PeerOperationWire *wire,
							 ClusterR4PeerOperationProof *out)
{
	ClusterR4PeerOperationProof proof = { 0 };
	ClusterSavepointReaderIdentity *id = &proof.identity;
	const uint8 *p;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (wire == NULL)
		return false;
	p = wire->bytes;
	if (ClusterR4OperationRead(p, 4) != CLUSTER_R4_OPERATION_PROOF_MAGIC
		|| ClusterR4OperationRead(p + 4, 2) != CLUSTER_R4_OPERATION_CONTEXT_VERSION
		|| ClusterR4OperationRead(p + 6, 2) != sizeof(*wire))
		return false;
	proof.flags = (uint32)ClusterR4OperationRead(p + 8, 4);
	id->canonical.segment_id = (uint32)ClusterR4OperationRead(p + 12, 4);
	id->canonical_generation = (uint32)ClusterR4OperationRead(p + 16, 4);
	id->canonical.xid = (TransactionId)ClusterR4OperationRead(p + 20, 4);
	id->canonical.slot_offset = (uint16)ClusterR4OperationRead(p + 24, 2);
	id->canonical.wrap = (uint16)ClusterR4OperationRead(p + 26, 2);
	proof.effect = p[28];
	proof.record_format = p[29];
	proof.outcome = (ClusterTxOutcome)p[30];
	proof.proof_kind = (ClusterTxProofKind)p[31];
	id->top_fxid = FullTransactionIdFromU64(ClusterR4OperationRead(p + 32, 8));
	id->operation_fxid = FullTransactionIdFromU64(ClusterR4OperationRead(p + 40, 8));
	id->operation_sequence = ClusterR4OperationRead(p + 48, 8);
	id->command_id = (CommandId)ClusterR4OperationRead(p + 56, 4);
	id->subid = (SubTransactionId)ClusterR4OperationRead(p + 60, 4);
	proof.commit_scn = (SCN)ClusterR4OperationRead(p + 64, 8);
	proof.admission_generation = ClusterR4OperationRead(p + 72, 8);
	proof.uba.raw[0] = ClusterR4OperationRead(p + 80, 8);
	proof.uba.raw[1] = ClusterR4OperationRead(p + 88, 8);
	if (!ClusterR4PeerOperationValid(&proof))
		return false;
	*out = proof;
	return true;
}

extern bool cluster_r4_peer_operation_extract(const ClusterR4OperationContextWire *request_wire,
											  const ClusterR4PeerOperationWire *proof_wire,
											  const ClusterTxLocator *locator,
											  uint64 admission_generation, const char page[BLCKSZ],
											  char record_out[BLCKSZ], size_t *length_out,
											  ClusterTxLocator *canonical_out);

static inline uint32
ClusterR4OperationChecksum(const char page[BLCKSZ], const void *auth,
						   const ClusterR4PeerOperationWire *proof)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page, BLCKSZ);
	COMP_CRC32C(crc, auth, 16);
	COMP_CRC32C(crc, proof, sizeof(*proof));
	FIN_CRC32C(crc);
	return (uint32)crc;
}
#endif
#endif
