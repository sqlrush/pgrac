/*-------------------------------------------------------------------------
 * cluster_r4_projection.h
 *   Bounded receipts for one private R4 FULL projection.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_PROJECTION_H
#define CLUSTER_R4_PROJECTION_H

#include "access/htup_details.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_r4_operation.h"
#include "cluster/cluster_resource_x_identity.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_R4_PROJECTION_PRESENT 1
#define CLUSTER_R4_PROJECTION_MAGIC UINT32_C(0x50523452)
#define CLUSTER_R4_PROJECTION_VERSION 1
#define CLUSTER_R4_PROJECTION_HEADER_BYTES 192
#define CLUSTER_R4_PROJECTION_ENTRY_BYTES 112
#define CLUSTER_R4_PROJECTION_CAPACITY 32
#define CLUSTER_R4_PROJECTION_BYTES                                                                \
	(CLUSTER_R4_PROJECTION_HEADER_BYTES                                                            \
	 + CLUSTER_R4_PROJECTION_ENTRY_BYTES * CLUSTER_R4_PROJECTION_CAPACITY)
#define CLUSTER_R4_PROJECTION_XMIN 1
#define CLUSTER_R4_PROJECTION_XMAX 2

typedef struct ClusterR4ProjectionWire {
	uint8 bytes[CLUSTER_R4_PROJECTION_BYTES];
} ClusterR4ProjectionWire;

/* Transport bytes and receipts only: never a buffer cache, TT observation,
 * parent mapping, publication receipt or retention/retirement permission. */
typedef struct ClusterR4ProjectionScratch {
	ClusterR4ProjectionWire wire;
	uint8 reply_header[48];
	bool valid;
} ClusterR4ProjectionScratch;

typedef struct ClusterR4ProjectionBinding {
	BufferTag tag;
	SCN read_scn;
	uint64 request_id, formation_epoch, activation_generation;
	uint64 master_generation, master_transition;
	SCN expected_page_scn;
	int32 master, holder, requester, backend;
	uint64 slot_generation, builder_incarnation, admission_generation;
	uint32 capability_generation;
	uint8 owner[40];
	XLogRecPtr page_lsn;
} ClusterR4ProjectionBinding;

typedef struct ClusterR4ProjectionEntry {
	ClusterR4PeerOperationProof operation;
	OffsetNumber offset;
	uint16 length, record_type;
	uint8 role, itl_index;
	TransactionId raw_xid;
	uint32 tuple_crc;
} ClusterR4ProjectionEntry;

StaticAssertDecl(CLUSTER_R4_PROJECTION_BYTES == 3776 && CLUSTER_R4_PROJECTION_BYTES <= 4096,
				 "R4 FULL projection has a fixed bounded segment");

static inline uint32
ClusterR4ProjectionCrc(const void *bytes, Size length)
{
	pg_crc32c crc;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static inline uint32
ClusterR4ProjectionFrameCrc(const ClusterR4ProjectionWire *wire, const void *header,
							const char page[BLCKSZ], const void *owner)
{
	pg_crc32c crc;
	const uint32 zero = 0;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, header, 48);
	COMP_CRC32C(crc, page, BLCKSZ);
	COMP_CRC32C(crc, owner, 40);
	COMP_CRC32C(crc, wire->bytes, 12);
	COMP_CRC32C(crc, &zero, sizeof(zero));
	COMP_CRC32C(crc, wire->bytes + 16, sizeof(*wire) - 16);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static inline bool
ClusterR4ProjectionEntryShape(const ClusterR4ProjectionEntry *entry)
{
	return entry != NULL && entry->offset >= FirstOffsetNumber
		   && entry->offset <= MaxHeapTuplesPerPage && entry->length >= SizeofHeapTupleHeader
		   && entry->length <= BLCKSZ && entry->itl_index < CLUSTER_ITL_INITRANS_DEFAULT
		   && entry->raw_xid == XidFromFullTransactionId(entry->operation.identity.operation_fxid)
		   && (entry->role == CLUSTER_R4_PROJECTION_XMIN
			   || entry->role == CLUSTER_R4_PROJECTION_XMAX)
		   && (entry->role == CLUSTER_R4_PROJECTION_XMIN
				   ? entry->record_type == UNDO_RECORD_INSERT
						 || entry->record_type == UNDO_RECORD_UPDATE
				   : entry->record_type == UNDO_RECORD_UPDATE
						 || entry->record_type == UNDO_RECORD_DELETE);
}

static inline bool
ClusterR4ProjectionEntryPut(ClusterR4ProjectionWire *wire, unsigned index,
							const ClusterR4ProjectionEntry *entry)
{
	ClusterR4PeerOperationWire peer;
	uint8 *p;
	if (wire == NULL || index >= CLUSTER_R4_PROJECTION_CAPACITY
		|| !ClusterR4ProjectionEntryShape(entry)
		|| !ClusterR4PeerOperationEncode(&peer, &entry->operation))
		return false;
	p = wire->bytes + CLUSTER_R4_PROJECTION_HEADER_BYTES
		+ index * CLUSTER_R4_PROJECTION_ENTRY_BYTES;
	memcpy(p, &peer, sizeof(peer));
	ClusterR4OperationWrite(p + 96, 2, entry->offset);
	ClusterR4OperationWrite(p + 98, 2, entry->length);
	p[100] = entry->role;
	p[101] = entry->itl_index;
	ClusterR4OperationWrite(p + 102, 2, entry->record_type);
	ClusterR4OperationWrite(p + 104, 4, entry->raw_xid);
	ClusterR4OperationWrite(p + 108, 4, entry->tuple_crc);
	return true;
}

static inline bool
ClusterR4ProjectionEntryGet(const ClusterR4ProjectionWire *wire, unsigned index,
							ClusterR4ProjectionEntry *entry)
{
	const uint8 *p;
	ClusterR4PeerOperationWire peer;
	if (wire == NULL || entry == NULL || index >= CLUSTER_R4_PROJECTION_CAPACITY)
		return false;
	p = wire->bytes + CLUSTER_R4_PROJECTION_HEADER_BYTES
		+ index * CLUSTER_R4_PROJECTION_ENTRY_BYTES;
	memset(entry, 0, sizeof(*entry));
	memcpy(&peer, p, sizeof(peer));
	if (!ClusterR4PeerOperationDecode(&peer, &entry->operation))
		return false;
	entry->offset = (OffsetNumber)ClusterR4OperationRead(p + 96, 2);
	entry->length = (uint16)ClusterR4OperationRead(p + 98, 2);
	entry->role = p[100];
	entry->itl_index = p[101];
	entry->record_type = (uint16)ClusterR4OperationRead(p + 102, 2);
	entry->raw_xid = (TransactionId)ClusterR4OperationRead(p + 104, 4);
	entry->tuple_crc = (uint32)ClusterR4OperationRead(p + 108, 4);
	return ClusterR4ProjectionEntryShape(entry);
}

static inline bool
ClusterR4ProjectionBindingValid(const ClusterR4ProjectionBinding *b)
{
	return b != NULL && b->tag.forkNum == MAIN_FORKNUM && b->tag.blockNum != InvalidBlockNumber
		   && SCN_VALID(b->read_scn) && SCN_VALID(b->expected_page_scn) && b->request_id != 0
		   && b->activation_generation != 0 && b->admission_generation == b->activation_generation
		   && b->master_generation != 0
		   && (uint32)(b->master_generation >> 32) == (uint32)b->formation_epoch
		   && b->master_transition != 0 && b->master_transition != UINT64_MAX && b->master >= 0
		   && b->master < RESOURCE_X_PROTOCOL_NODE_LIMIT && b->holder >= 0
		   && b->holder < RESOURCE_X_PROTOCOL_NODE_LIMIT && b->requester >= 0
		   && b->requester < RESOURCE_X_PROTOCOL_NODE_LIMIT && b->backend > 0
		   && b->slot_generation != 0 && b->slot_generation != UINT64_MAX
		   && b->builder_incarnation != 0 && b->capability_generation != 0;
}

/* Header offsets are explicit LE fields; no native padding crosses the wire. */
static inline bool
ClusterR4ProjectionSeal(ClusterR4ProjectionWire *wire, unsigned count,
						const ClusterR4ProjectionBinding *b, const void *header,
						const char page[BLCKSZ])
{
	uint8 *p;
	if (wire == NULL || count > CLUSTER_R4_PROJECTION_CAPACITY
		|| !ClusterR4ProjectionBindingValid(b) || header == NULL || page == NULL)
		return false;
	p = wire->bytes;
	memset(p, 0, CLUSTER_R4_PROJECTION_HEADER_BYTES);
	memset(p + CLUSTER_R4_PROJECTION_HEADER_BYTES + count * CLUSTER_R4_PROJECTION_ENTRY_BYTES, 0,
		   (CLUSTER_R4_PROJECTION_CAPACITY - count) * CLUSTER_R4_PROJECTION_ENTRY_BYTES);
	ClusterR4OperationWrite(p, 4, CLUSTER_R4_PROJECTION_MAGIC);
	ClusterR4OperationWrite(p + 4, 2, CLUSTER_R4_PROJECTION_VERSION);
	ClusterR4OperationWrite(p + 6, 2, sizeof(*wire));
	ClusterR4OperationWrite(p + 8, 2, count);
	ClusterR4OperationWrite(p + 10, 2, CLUSTER_R4_PROJECTION_ENTRY_BYTES);
	ClusterR4OperationWrite(p + 16, 4, b->tag.spcOid);
	ClusterR4OperationWrite(p + 20, 4, b->tag.dbOid);
	ClusterR4OperationWrite(p + 24, 4, b->tag.relNumber);
	ClusterR4OperationWrite(p + 28, 4, b->tag.forkNum);
	ClusterR4OperationWrite(p + 32, 4, b->tag.blockNum);
	ClusterR4OperationWrite(p + 36, 8, b->read_scn);
	ClusterR4OperationWrite(p + 44, 8, b->request_id);
	ClusterR4OperationWrite(p + 52, 8, b->formation_epoch);
	ClusterR4OperationWrite(p + 60, 8, b->activation_generation);
	ClusterR4OperationWrite(p + 68, 8, b->master_generation);
	ClusterR4OperationWrite(p + 76, 8, b->master_transition);
	ClusterR4OperationWrite(p + 84, 8, b->expected_page_scn);
	ClusterR4OperationWrite(p + 92, 4, b->master);
	ClusterR4OperationWrite(p + 96, 4, b->holder);
	ClusterR4OperationWrite(p + 100, 4, b->requester);
	ClusterR4OperationWrite(p + 104, 4, b->backend);
	ClusterR4OperationWrite(p + 108, 8, b->slot_generation);
	ClusterR4OperationWrite(p + 116, 8, b->builder_incarnation);
	ClusterR4OperationWrite(p + 124, 8, b->admission_generation);
	ClusterR4OperationWrite(p + 132, 8, b->capability_generation);
	memcpy(p + 140, b->owner, 40);
	ClusterR4OperationWrite(p + 180, 4, ClusterR4ProjectionCrc(page, BLCKSZ));
	ClusterR4OperationWrite(p + 184, 8, b->page_lsn);
	ClusterR4OperationWrite(p + 12, 4, ClusterR4ProjectionFrameCrc(wire, header, page, b->owner));
	return true;
}

static inline bool
ClusterR4ProjectionDecode(const ClusterR4ProjectionWire *wire, const void *header,
						  const char page[BLCKSZ], const void *owner, ClusterR4ProjectionBinding *b)
{
	const uint8 *p;
	unsigned count;
	if (wire == NULL || header == NULL || page == NULL || owner == NULL || b == NULL)
		return false;
	p = wire->bytes;
	count = (unsigned)ClusterR4OperationRead(p + 8, 2);
	if (ClusterR4OperationRead(p, 4) != CLUSTER_R4_PROJECTION_MAGIC
		|| ClusterR4OperationRead(p + 4, 2) != CLUSTER_R4_PROJECTION_VERSION
		|| ClusterR4OperationRead(p + 6, 2) != sizeof(*wire)
		|| ClusterR4OperationRead(p + 10, 2) != CLUSTER_R4_PROJECTION_ENTRY_BYTES
		|| count > CLUSTER_R4_PROJECTION_CAPACITY
		|| ClusterR4OperationRead(p + 12, 4)
			   != ClusterR4ProjectionFrameCrc(wire, header, page, owner)
		|| ClusterR4OperationRead(p + 180, 4) != ClusterR4ProjectionCrc(page, BLCKSZ)
		|| memcmp(p + 140, owner, 40) != 0 || ClusterR4OperationRead(p + 132, 8) > UINT32_MAX)
		return false;
	for (unsigned i
		 = CLUSTER_R4_PROJECTION_HEADER_BYTES + count * CLUSTER_R4_PROJECTION_ENTRY_BYTES;
		 i < sizeof(*wire); i++)
		if (p[i] != 0)
			return false;
	for (unsigned i = 0; i < count; i++) {
		ClusterR4ProjectionEntry entry;
		if (!ClusterR4ProjectionEntryGet(wire, i, &entry)
			|| entry.operation.admission_generation != ClusterR4OperationRead(p + 124, 8))
			return false;
		for (unsigned j = 0; j < i; j++) {
			const uint8 *prior
				= p + CLUSTER_R4_PROJECTION_HEADER_BYTES + j * CLUSTER_R4_PROJECTION_ENTRY_BYTES;
			if (ClusterR4OperationRead(prior + 96, 2) == entry.offset && prior[100] == entry.role)
				return false;
		}
	}
	memset(b, 0, sizeof(*b));
	b->tag.spcOid = (Oid)ClusterR4OperationRead(p + 16, 4);
	b->tag.dbOid = (Oid)ClusterR4OperationRead(p + 20, 4);
	b->tag.relNumber = (RelFileNumber)ClusterR4OperationRead(p + 24, 4);
	b->tag.forkNum = (ForkNumber)ClusterR4OperationRead(p + 28, 4);
	b->tag.blockNum = (BlockNumber)ClusterR4OperationRead(p + 32, 4);
	b->read_scn = ClusterR4OperationRead(p + 36, 8);
	b->request_id = ClusterR4OperationRead(p + 44, 8);
	b->formation_epoch = ClusterR4OperationRead(p + 52, 8);
	b->activation_generation = ClusterR4OperationRead(p + 60, 8);
	b->master_generation = ClusterR4OperationRead(p + 68, 8);
	b->master_transition = ClusterR4OperationRead(p + 76, 8);
	b->expected_page_scn = ClusterR4OperationRead(p + 84, 8);
	b->master = (int32)ClusterR4OperationRead(p + 92, 4);
	b->holder = (int32)ClusterR4OperationRead(p + 96, 4);
	b->requester = (int32)ClusterR4OperationRead(p + 100, 4);
	b->backend = (int32)ClusterR4OperationRead(p + 104, 4);
	b->slot_generation = ClusterR4OperationRead(p + 108, 8);
	b->builder_incarnation = ClusterR4OperationRead(p + 116, 8);
	b->admission_generation = ClusterR4OperationRead(p + 124, 8);
	b->capability_generation = (uint32)ClusterR4OperationRead(p + 132, 8);
	memcpy(b->owner, p + 140, 40);
	b->page_lsn = ClusterR4OperationRead(p + 184, 8);
	return ClusterR4ProjectionBindingValid(b);
}

static inline bool
ClusterR4ProjectionFind(const ClusterR4ProjectionWire *wire, OffsetNumber offset, uint8 role,
						ClusterR4ProjectionEntry *entry)
{
	unsigned count;
	if (wire == NULL)
		return false;
	count = (unsigned)ClusterR4OperationRead(wire->bytes + 8, 2);
	if (count > CLUSTER_R4_PROJECTION_CAPACITY)
		return false;
	for (unsigned i = 0; i < count; i++) {
		const uint8 *p = wire->bytes + CLUSTER_R4_PROJECTION_HEADER_BYTES
						 + i * CLUSTER_R4_PROJECTION_ENTRY_BYTES;
		if (ClusterR4OperationRead(p + 96, 2) == offset && p[100] == role)
			return ClusterR4ProjectionEntryGet(wire, i, entry);
	}
	return false;
}

static inline void
ClusterR4ProjectionForget(ClusterR4ProjectionScratch *projection)
{
	if (projection != NULL)
		memset(projection, 0, sizeof(*projection));
}

/* A DATA carrier selects a role/origin only; receipt identity remains
 * independent. Invalid/missing/recycled authority never becomes a verdict. */
static inline bool
ClusterR4ProjectionSlotOrigin(Page page, uint8 index, int32 *origin)
{
	const ClusterItlSlotData *slot;
	uint32 segment, block;
	uint16 tt, row;
	if (index >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	slot = &ClusterPageGetItlSlots(page)[index];
	if (slot->flags < ITL_FLAG_ACTIVE || slot->flags > ITL_FLAG_NEEDS_CLEANOUT
		|| !TransactionIdIsNormal(slot->xid)
		|| !uba_decode_record(slot->undo_segment_head, &segment, &block, &tt, &row))
		return false;
	*origin = (int32)((segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE);
	return *origin >= 0 && *origin < RESOURCE_X_PROTOCOL_NODE_LIMIT;
}

static inline bool
ClusterR4ProjectionEntryMatches(const ClusterR4ProjectionEntry *entry, Page page, SCN read_scn,
								int32 requester)
{
	ItemId item;
	HeapTupleHeader tuple;
	int32 carrier_origin;
	uint32 origin;
	TransactionId raw;
	if (entry->offset > PageGetMaxOffsetNumber(page))
		return false;
	item = PageGetItemId(page, entry->offset);
	if (!ItemIdIsNormal(item) || ItemIdGetLength(item) != entry->length)
		return false;
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	raw = entry->role == CLUSTER_R4_PROJECTION_XMIN ? HeapTupleHeaderGetRawXmin(tuple)
													: HeapTupleHeaderGetRawXmax(tuple);
	origin = (entry->operation.identity.canonical.segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE;
	if ((int32)origin == requester || entry->raw_xid != raw
		|| entry->operation.outcome != CLUSTER_TX_COMMITTED
		|| scn_time_cmp(entry->operation.commit_scn, read_scn) > 0
		|| (entry->operation.record_format == 2
			&& entry->operation.effect != CLUSTER_SAVEPOINT_EFFECT_LIVE)
		|| entry->tuple_crc != ClusterR4ProjectionCrc(tuple, entry->length)
		|| !ClusterR4ProjectionSlotOrigin(page, entry->itl_index, &carrier_origin)
		|| carrier_origin != (int32)origin
		|| (entry->role == CLUSTER_R4_PROJECTION_XMIN
				? HeapTupleHeaderXminFrozen(tuple) || raw == BootstrapTransactionId
				: tuple->t_itl_slot_idx != entry->itl_index
					  || (tuple->t_infomask & (HEAP_XMAX_INVALID | HEAP_XMAX_IS_MULTI)) != 0
					  || HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)))
		return false;
	return true;
}

/* Validate every external role before returning even the first tuple. A
 * carrier that cannot classify a role is not a proof of local ownership. */
static inline bool
ClusterR4ProjectionPageComplete(const ClusterR4ProjectionWire *wire, Page page, SCN read_scn,
								int32 requester)
{
	PageHeader header = (PageHeader)page;
	unsigned count, used = 0;
	if (wire == NULL || page == NULL || header->pd_lower < SizeOfPageHeaderData
		|| header->pd_lower > header->pd_upper || header->pd_upper > header->pd_special
		|| header->pd_special > BLCKSZ || !PageHasItl(page)
		|| PageGetSpecialSize(page) < CLUSTER_ITL_ARRAY_SIZE
		|| PageGetMaxOffsetNumber(page) > MaxHeapTuplesPerPage)
		return false;
	count = (unsigned)ClusterR4OperationRead(wire->bytes + 8, 2);
	if (count > CLUSTER_R4_PROJECTION_CAPACITY)
		return false;
	for (OffsetNumber off = FirstOffsetNumber; off <= PageGetMaxOffsetNumber(page); off++) {
		ItemId item = PageGetItemId(page, off);
		HeapTupleHeader tuple;
		if (!ItemIdIsNormal(item))
			continue;
		if (ItemIdGetLength(item) < SizeofHeapTupleHeader
			|| ItemIdGetOffset(item) < header->pd_upper
			|| (Size)ItemIdGetOffset(item) + ItemIdGetLength(item) > header->pd_special)
			return false;
		tuple = (HeapTupleHeader)PageGetItem(page, item);
		if (tuple->t_hoff < SizeofHeapTupleHeader || tuple->t_hoff > ItemIdGetLength(item))
			return false;
		for (uint8 role = CLUSTER_R4_PROJECTION_XMIN; role <= CLUSTER_R4_PROJECTION_XMAX; role++) {
			ClusterR4ProjectionEntry entry;
			int32 carrier_origin;
			TransactionId raw;
			uint8 slot = tuple->t_itl_slot_idx;
			if (role == CLUSTER_R4_PROJECTION_XMIN) {
				raw = HeapTupleHeaderGetRawXmin(tuple);
				if (HeapTupleHeaderXminFrozen(tuple) || raw == BootstrapTransactionId)
					continue;
			} else {
				if ((tuple->t_infomask & HEAP_XMAX_INVALID) != 0
					|| HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask))
					continue;
				if ((tuple->t_infomask & HEAP_XMAX_IS_MULTI) != 0)
					return false;
				raw = HeapTupleHeaderGetRawXmax(tuple);
			}
			if (!TransactionIdIsNormal(raw))
				return false;
			if (ClusterR4ProjectionFind(wire, off, role, &entry)) {
				if (!ClusterR4ProjectionEntryMatches(&entry, page, read_scn, requester))
					return false;
				used++;
				continue;
			}
			if (role == CLUSTER_R4_PROJECTION_XMIN) {
				uint8 selected = CLUSTER_ITL_SLOT_UNALLOCATED;
				for (uint8 i = 0; i < CLUSTER_ITL_INITRANS_DEFAULT; i++) {
					const ClusterItlSlotData *candidate = &ClusterPageGetItlSlots(page)[i];
					if (candidate->flags >= ITL_FLAG_ACTIVE
						&& candidate->flags <= ITL_FLAG_NEEDS_CLEANOUT && candidate->xid == raw) {
						if (selected != CLUSTER_ITL_SLOT_UNALLOCATED)
							return false;
						selected = i;
					}
				}
				if (selected != CLUSTER_ITL_SLOT_UNALLOCATED)
					slot = selected;
			}
			if (slot >= CLUSTER_ITL_INITRANS_DEFAULT
				|| !ClusterR4ProjectionSlotOrigin(page, slot, &carrier_origin)
				|| carrier_origin != requester
				|| (ClusterPageGetItlSlots(page)[slot].xid != raw
					&& (ClusterR4OperationRead(wire->bytes + 140 + 34, 2) != 1
						|| wire->bytes[140 + 32] != requester + 1
						|| ClusterR4OperationRead(wire->bytes + 140 + 24, 4)
							   != ClusterPageGetItlSlots(page)[slot].xid)))
				return false;
		}
	}
	return used == count;
}

static inline bool
ClusterR4ProjectionScratchValid(const ClusterR4ProjectionScratch *projection, Page page,
								const BufferTag *tag, SCN read_scn, uint64 epoch, int32 requester)
{
	ClusterR4ProjectionBinding binding;
	return projection != NULL && projection->valid && tag != NULL
		   && ClusterR4ProjectionDecode(&projection->wire, projection->reply_header, page,
										projection->wire.bytes + 140, &binding)
		   && BufferTagsEqual(&binding.tag, tag) && binding.read_scn == read_scn
		   && binding.formation_epoch == epoch && binding.requester == requester
		   && ClusterR4ProjectionPageComplete(&projection->wire, page, read_scn, requester);
}
#endif
#endif
