/*-------------------------------------------------------------------------
 *
 * cluster_uba.h
 *	  pgrac UBA (Undo Block Address) encoding helpers (spec-3.4b D1).
 *
 *	  UBA is a 16-byte opaque address embedded in ItlSlotData.undo_segment_head
 *	  (and elsewhere later).  The on-page typedef and InvalidUba sentinel
 *	  are owned by cluster_itl_slot.h (spec-1.5); this header layers on
 *	  pure inline encode / decode helpers per the block-format §5 field
 *	  layout, plus a two-tier owner_instance lookup used by the visibility
 *	  reader (spec-3.4b D7).
 *
 *	  This header MUST NOT redefine the `UBA` typedef; it includes
 *	  cluster_tt_slot.h (which transitively pulls in cluster_itl_slot.h
 *	  for `UBA` + `InvalidUba_init` + `UBA_is_invalid`) so callers can
 *	  reach `TT_SLOTS_PER_SEGMENT` (spec-3.4b D3 dependency) and the UBA
 *	  typedef chain via a single include.
 *
 *	  Layout (block-format §5, 16 bytes):
 *	    raw[0]:
 *	      bits [0..31]  segment_id   (spec-3.4b: [1, UINT16_MAX]; 0 == invalid)
 *	      bits [32..63] block_no     (spec-3.4b TT-only MVP: 0 reserved)
 *	    raw[1]:
 *	      bits [0..15]  tt_slot_offset (spec-3.4b: [0, TT_SLOTS_PER_SEGMENT))
 *	      bits [16..31] row_offset     (spec-3.4b TT-only MVP: 0 reserved)
 *	      bits [32..63] reserved       (MUST be zero; decode rejects non-zero)
 *
 *	  Validation contract (uba_decode returns false on any of):
 *	    1. UBA_is_invalid(u)           — all-zero sentinel
 *	    2. segment_id == 0             — segment 0 is bootstrap-only (F2)
 *	    3. segment_id > UINT16_MAX     — F4 ABI alias guard
 *	    4. tt_slot_offset >= TT_SLOTS_PER_SEGMENT — out of allocator range
 *	    5. reserved bits non-zero      — layout corruption
 *
 *	  Encoder contract (uba_encode Assert on):
 *	    1. segment_id != 0
 *	    2. segment_id <= UINT16_MAX
 *	    3. tt_slot_offset < TT_SLOTS_PER_SEGMENT
 *	  Real allocator callers MUST NOT pass forbidden values.  Bootstrap
 *	  paths (segment 0) MUST NOT pass through uba_encode at all; leave
 *	  the UBA at InvalidUba.
 *
 *	  Two-tier owner lookup (uba_origin_node_id):
 *	    Decoded segment_id maps to owner node via the per-instance
 *	    segment-id range encoding documented in cluster_undo_alloc.h
 *	    (spec-3.4b D2): owner_node = (segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE.
 *	    Returns -1 (InvalidNodeId sentinel) when the UBA is invalid or
 *	    derives an owner_node outside [0, SCN_MAX_VALID_NODE_ID].
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_uba.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Layer-1 helpers (uba_encode / uba_decode) are pure bit manipulation
 *	  -- no cluster API calls -- and safe under critical section.
 *	  Layer-2 helper (uba_origin_node_id) is also pure derivation (no
 *	  segment-header read) and safe under critical section.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UBA_H
#define CLUSTER_UBA_H

#include "c.h"
#include "cluster/cluster_scn.h"				/* NodeId, SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_tt_slot.h"			/* TT_SLOTS_PER_SEGMENT + UBA typedef chain */
#include "cluster/cluster_undo_segment.h"		/* UNDO_BLOCKS_PER_SEGMENT */
#include "cluster/storage/cluster_undo_alloc.h" /* logical segment-range constants */


/*
 * InvalidNodeId — sentinel for "no valid owner node could be derived".
 *
 *	cluster_scn.h documents the NodeId valid range as [0, 127] with -1
 *	meaning "unset / single-node fallback".  We reuse -1 as the lookup
 *	failure sentinel here.
 */
#define InvalidNodeId ((NodeId) - 1)


/* Sanity: spec-1.5 freezes UBA at exactly 16 bytes. */
StaticAssertDecl(sizeof(UBA) == 16, "UBA must remain 16 bytes (spec-1.5)");


/*
 * uba_encode
 *	  Pack a (segment_id, block_no, tt_slot_offset, row_offset) tuple into
 *	  a 16-byte UBA per block-format §5.  Validation is Assert-only; real
 *	  allocator callers must satisfy the contract before calling.
 */
static inline UBA
uba_encode(uint32 segment_id, uint32 block_no, uint16 tt_slot_offset, uint16 row_offset)
{
	UBA u;

	Assert(segment_id != 0);		  /* F2 — segment 0 bootstrap-only */
	Assert(segment_id <= UINT16_MAX); /* F4 — exact-key uint16 alias guard */
	Assert(tt_slot_offset < TT_SLOTS_PER_SEGMENT);

	u.raw[0] = ((uint64)segment_id) | (((uint64)block_no) << 32);
	u.raw[1] = ((uint64)tt_slot_offset) | (((uint64)row_offset) << 16);
	return u;
}


/*
 * uba_decode
 *	  Unpack a UBA into its component fields.  Returns false (without
 *	  touching out-params beyond first store attempt) when the UBA is
 *	  invalid or malformed; returns true on a valid real UBA.
 *
 *	  Callers MUST check the return value before consuming the fields.
 *	  spec-3.4b D7 reader uses this for explicit ERRCODE_DATA_CORRUPTED
 *	  reporting when malformed UBA bytes are found in a non-FREE,
 *	  non-InvalidUba ITL slot.
 */
static inline bool
uba_decode(UBA u, uint32 *segment_id, uint32 *block_no, uint16 *tt_slot_offset, uint16 *row_offset)
{
	uint32 reserved;
	uint32 seg;
	uint16 off;

	if (UBA_is_invalid(u))
		return false;

	seg = (uint32)(u.raw[0] & 0xFFFFFFFFULL);
	off = (uint16)(u.raw[1] & 0xFFFFULL);
	reserved = (uint32)(u.raw[1] >> 32);

	if (seg == 0 || seg > UINT16_MAX)
		return false;
	if (off >= TT_SLOTS_PER_SEGMENT)
		return false;
	if (reserved != 0)
		return false;

	*segment_id = seg;
	*block_no = (uint32)(u.raw[0] >> 32);
	*tt_slot_offset = off;
	*row_offset = (uint16)((u.raw[1] >> 16) & 0xFFFFULL);
	return true;
}


/*
 * uba_decode_record -- Decode only an undo DATA-record address.
 *
 *	The general decoder must continue to accept the historical TT-only
 *	block-zero UBA.  Record readers have the narrower R4A contract: block zero,
 *	the one-past-end block and segment identities outside the exact 128 x 256
 *	owner map are rejected before path resolution or I/O.  Outputs are assigned
 *	only after every record-specific predicate succeeds.
 */
static inline bool
uba_decode_record(UBA u, uint32 *segment_id, uint32 *block_no, uint16 *tt_slot_offset,
				  uint16 *row_offset)
{
	uint32 decoded_segment;
	uint32 decoded_block;
	uint16 decoded_slot;
	uint16 decoded_row;
	const uint32 max_segment = (uint32)UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_SEGS_PER_INSTANCE;

	if (segment_id == NULL || block_no == NULL || tt_slot_offset == NULL || row_offset == NULL)
		return false;
	if (!uba_decode(u, &decoded_segment, &decoded_block, &decoded_slot, &decoded_row))
		return false;
	if (decoded_segment > max_segment || decoded_block == 0
		|| decoded_block >= UNDO_BLOCKS_PER_SEGMENT)
		return false;

	*segment_id = decoded_segment;
	*block_no = decoded_block;
	*tt_slot_offset = decoded_slot;
	*row_offset = decoded_row;
	return true;
}


/*
 * uba_get_segment_id / uba_get_tt_slot_offset
 *	  Fast accessors for callers that only need one field without paying
 *	  the full validation cost.  Result is unspecified when called on
 *	  InvalidUba or a malformed UBA; callers are responsible for first
 *	  checking UBA_is_invalid() / calling uba_decode().
 */
static inline uint32
uba_get_segment_id(UBA u)
{
	return (uint32)(u.raw[0] & 0xFFFFFFFFULL);
}


static inline uint16
uba_get_tt_slot_offset(UBA u)
{
	return (uint16)(u.raw[1] & 0xFFFFULL);
}


/*
 * uba_origin_node_id
 *	  Two-tier helper: decode the UBA, then map the embedded segment_id
 *	  to its owning node_id via the per-instance segment-id range encoding
 *	  (see cluster_undo_alloc.h CLUSTER_UNDO_SEGS_PER_INSTANCE).
 *
 *	  Returns InvalidNodeId when:
 *	    - uba_decode() fails (InvalidUba or malformed)
 *	    - derived owner_node falls outside [0, SCN_MAX_VALID_NODE_ID]
 *
 *	  This is a pure derivation -- O(1), no I/O, safe under critical
 *	  section.  Implementation lives in cluster_uba.c so the segment-id
 *	  encoding constant is consolidated with the allocator.
 */
extern NodeId uba_origin_node_id(UBA u);


#endif /* CLUSTER_UBA_H */
