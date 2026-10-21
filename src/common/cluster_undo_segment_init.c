/*-------------------------------------------------------------------------
 *
 * cluster_undo_segment_init.c
 *	  Frontend-safe pure helper that fills an 8 KB buffer with the
 *	  byte-exact layout of a freshly-allocated UndoSegmentHeaderData
 *	  page (block 0 of every undo segment file).
 *
 *	  Lives in src/common/ so both backend (bufpage.c
 *	  PageInitUndoSegmentHeader) and frontend (initdb.c seed segment
 *	  writer) can call it.  This is the single source of truth for
 *	  segment-header byte generation; backend and frontend produce
 *	  byte-identical pages.
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §D14c (v0.2 P1-B).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/common/cluster_undo_segment_init.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Frontend-safe: no PG buffer manager / shmem / latch / WAL / elog
 *	  dependency.  The body is gated by USE_PGRAC_CLUSTER so the file
 *	  compiles to a no-op in --disable-cluster builds (callers in that
 *	  mode never reach this function -- they are themselves gated).
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <string.h>

#include "cluster/cluster_undo_segment_init.h"

#ifdef USE_PGRAC_CLUSTER
#include "access/transam.h"
#include "cluster/cluster_itl_slot.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_undo_segment.h"
#include "storage/bufpage.h"
/* Keep the canonical PG algorithm private to this frontend-safe module;
 * the backend also exports pg_checksum_page from its page subsystem. */
static uint16 undo_header_checksum(char *page, BlockNumber blockno);
#define pg_checksum_page undo_header_checksum
#include "storage/checksum_impl.h"
#undef pg_checksum_page
#endif


/*
 * cluster_undo_segment_make_header_bytes
 *
 *   Implementation: see header file for contract.  The body is a
 *   sequence of explicit field writes after a single MemSet to zero;
 *   placeholder fields (xid, wrap, status, flags, commit_scn,
 *   first_undo_block, retention info, statistics, free_block_bitmap,
 *   _reserved) are technically zero from the MemSet but re-written
 *   explicitly using sentinel constants for readability + defensive
 *   programming (matching spec-1.5 §8 Q6=B explicit-writes pattern).
 *
 *   On-disk byte offsets are guaranteed by the StaticAssertDecl block
 *   in cluster_undo_segment.h (spec-1.21 ## Hardening v1.0.1):
 *     0    PageHeader prefix (32 bytes; spec-1.4 cluster mode)
 *     32   segment_id
 *     40   segment_state, owner_instance, tt_slots_count
 *     64   oldest_active_scn, ...
 *     112  tt_slots[48]
 *     1648 tt_next_free_slot, tt_active_count
 *     1656 free_block_bitmap[1024]
 *     2680 _reserved[5512]
 */
void
cluster_undo_segment_make_header_bytes(uint32 segment_id, uint8 owner_instance, char *page)
{
#ifdef USE_PGRAC_CLUSTER
	UndoSegmentHeaderData *hdr;
	PageHeader ph;
	int i;

	Assert(page != NULL);
	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/*
	 * Single MemSet zeros the entire 8 KB block.  All sentinel-zero
	 * placeholders (InvalidScn, InvalidUba, InvalidTransactionId,
	 * TT_SLOT_UNUSED, SEGMENT_ALLOCATED, TT_WRAP_INITIAL,
	 * TT_FLAGS_RESERVED, UNDO_OWNER_INSTANCE_INVALID 0, free_block_bitmap)
	 * are now correct without further writes.
	 */
	memset(page, 0, BLCKSZ);

	ph = (PageHeader)page;
	hdr = (UndoSegmentHeaderData *)page;

	/*
	 * PG standard PageHeader prefix (32 bytes; spec-1.4 cluster mode).
	 * pd_lsn / pd_checksum / pd_prune_xid / pd_block_scn left as 0
	 * from MemSet (pd_block_scn = InvalidScn = 0).
	 */
	ph->pd_flags = PD_UNDO_SEG_HEADER;
	ph->pd_lower = SizeOfPageHeaderData;
	ph->pd_upper = BLCKSZ;
	ph->pd_special = BLCKSZ;
	PageSetPageSizeAndVersion((Page)page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);

	/*
	 * Segment metadata (offsets 32-63 per spec-1.21 Hardening v1.0.1).
	 */
	hdr->segment_id = segment_id;
	hdr->segment_size_bytes = UNDO_SEGMENT_SIZE_BYTES;
	hdr->segment_state = SEGMENT_ALLOCATED;
	hdr->owner_instance = owner_instance;
	hdr->tt_slots_count = TT_SLOTS_PER_SEGMENT;
	/* head_block / tail_block / last_recyclable_block / segment_flags
	 * left at 0 from MemSet (block 0 = invalid block; segment empty). */

	/* Retention info / Statistics zero from MemSet. */

	/*
	 * TT slots (48 x 32 = 1536 bytes; offsets 112-1647).  Explicit
	 * writes for clarity even though MemSet already produced the
	 * correct sentinel-zero state (spec-1.20 TT_SLOT_UNUSED = 0
	 * permanent placeholder).
	 */
	for (i = 0; i < TT_SLOTS_PER_SEGMENT; i++) {
		hdr->tt_slots[i].xid = InvalidTransactionId;
		hdr->tt_slots[i].wrap = TT_WRAP_INITIAL;
		hdr->tt_slots[i].status = TT_SLOT_UNUSED;
		hdr->tt_slots[i].flags = TT_FLAGS_RESERVED;
		hdr->tt_slots[i].commit_scn = InvalidScn;
		/* first_undo_block (UBA, 16 bytes) = InvalidUba; zero from MemSet. */
	}

	/* tt_next_free_slot / tt_active_count zero from MemSet. */
	/* free_block_bitmap[1024] zero from MemSet (no blocks allocated yet). */
	/* _reserved[5512] zero from MemSet. */
#else
	/*
	 * --disable-cluster builds: function symbol must resolve at link
	 * time but no caller actually reaches here (callers are themselves
	 * gated by USE_PGRAC_CLUSTER).  Zero-fill the buffer as a safety
	 * net so accidental misuse produces a recognizable empty page
	 * rather than uninitialized data.
	 */
	(void)segment_id;
	(void)owner_instance;
	if (page != NULL)
		memset(page, 0, 8192);
#endif
}

/* Check a private aligned copy: the PG algorithm temporarily clears the
 * checksum field. Never modify an immutable recovery capture while checking. */
ClusterUndoHeaderIntegrity
cluster_undo_segment_header_integrity(const void *page)
{
#ifdef USE_PGRAC_CLUSTER
	PGAlignedBlock copy;
	PageHeader header;
	uint16 stored;

	if (page == NULL)
		return CLUSTER_UNDO_HEADER_CORRUPT;
	memcpy(copy.data, page, BLCKSZ);
	header = (PageHeader)copy.data;
	stored = header->pd_checksum;
	if (stored == 0)
		return CLUSTER_UNDO_HEADER_UNVERIFIED;
	if (PageIsNew(copy.data))
		return CLUSTER_UNDO_HEADER_CORRUPT;
	return stored == undo_header_checksum(copy.data, 0) ? CLUSTER_UNDO_HEADER_VALID
		: CLUSTER_UNDO_HEADER_CORRUPT;
#else
	return CLUSTER_UNDO_HEADER_CORRUPT;
#endif
}

bool
cluster_undo_segment_header_seal(void *page)
{
#ifdef USE_PGRAC_CLUSTER
	PGAlignedBlock copy;
	uint16 checksum;

	if (page == NULL)
		return false;
	memcpy(copy.data, page, BLCKSZ);
	if (PageIsNew(copy.data))
		return false;
	checksum = undo_header_checksum(copy.data, 0);
	memcpy((char *)page + offsetof(PageHeaderData, pd_checksum), &checksum, sizeof(checksum));
	return true;
#else
	return false;
#endif
}
