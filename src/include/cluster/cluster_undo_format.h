/*-------------------------------------------------------------------------
 *
 * cluster_undo_format.h
 *	  pgrac undo block + slot directory format (spec-3.7 D2).
 *
 *	  Stage 3 第 11 sub-spec — Undo physical layer 真激活的 block-level
 *	  + record-directory layout.  builds on spec-1.21/1.22 已 ship 的
 *	  `cluster_undo_segment.h` (segment header block 0 8KB) — 本 header
 *	  定义 segment data block(block 1..N)内部的 block header + slot
 *	  directory layout.
 *
 *	  Block layout(8KB,grows from both ends):
 *
 *	    offset  size  description
 *	    ------  ----  -------------------------------------------
 *	    0       48    UndoBlockHeader (HC211 v1.0.1: 40B record+SCN
 *	                  portion + spec-3.18 D2 block_lsn(8) = 48B)
 *	    48      ...   record 1 var bytes (UndoRecordHeader 64B +
 *	                  payload variable)
 *	    ...     ...   record 2, 3, ...
 *	    ...     ...   FREE SPACE
 *	    [end - 8 × slot_count]
 *	            8     slot_N (last record's UndoSlotDirEntry)
 *	    [end - 8]
 *	            8     slot_1 (first record's UndoSlotDirEntry)
 *	    8192          end of block
 *
 *	  Records grow upward from offset 48 (= sizeof(UndoBlockHeader));
 *	  slot directory grows downward from offset 8192.  free_offset (in
 *	  UndoBlockHeader) is the lower-bound of available space.  Block full
 *	  when (free_offset + 8 × (slot_count + 1)) > 8192.
 *
 *	  Slot directory invariant:  slot index 0 is the most-recently
 *	  written record (at offset 8184);  slot index slot_count-1 is
 *	  the oldest record in this block.  UBA `row_offset` (per
 *	  spec-3.4b cluster_uba.h) addresses the slot dir index;  reader
 *	  resolves slot → record_offset → record bytes.
 *
 *	  HC211 / HC212 ABI lock(static asserts in this header):
 *	    HC211 sizeof(UndoBlockHeader) == 48 (40B HC211 v1.0.1 +
 *	          spec-3.18 D2 block_lsn 8B)
 *	    HC212 sizeof(UndoSlotDirEntry) == 8
 *
 *	  Frontend-safe: this header has no backend-only includes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.7-undo-record-format-allocator.md (FROZEN v0.4 +
 *       Hardening v1.0.1 — H-1 HC211 40B arithmetic fix)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_format.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Pure ABI typedef + static asserts;  no functions.  Frontend-safe.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_FORMAT_H
#define CLUSTER_UNDO_FORMAT_H

#include "c.h"					 /* uint8/16/32/64, BLCKSZ */
#include "access/xlogdefs.h"	 /* XLogRecPtr */
#include "cluster/cluster_scn.h" /* SCN */


/*
 * Magic constants — first 4 bytes of each undo block / record-type discriminator.
 */
#define PGRAC_UNDO_BLOCK_MAGIC 0x55444F31U /* "UDO1" little-endian */

/* Block version — spec-3.7 ships block_version=1; future amend bumps. */
#define UNDO_BLOCK_VERSION_1 1

/*
 * UndoBlockHeader -- per undo block header (48 bytes, offset 0).
 *
 *	Hardening v1.0.1 HC211:  the record+SCN portion is 40B not 32B (spec-3.7
 *	body §2.1 arithmetic fix — the 32B claim missed the SCN 8B-alignment pad).
 *
 *	spec-3.18 D2:  block_lsn appended (40B -> 48B).  It carries the page-LSN of
 *	the newest change to this block.  The undo WAL emitter
 *	(XLOG_UNDO_BLOCK_WRITE) compares block_lsn against the checkpoint redo
 *	pointer to choose a full-page image vs a delta, the same way PG heap pages
 *	use pd_lsn;  redo restores block_lsn along with the rest of the block.
 *
 *	Layout:  magic(4) + block_version(2) + slot_count(2) + free_offset(4)
 *	+ _pad12(4) + first_change_scn(8, SCN 8B-aligned) + first_change_lsn(8)
 *	+ crc64(8) + block_lsn(8) = 48 bytes.
 */
typedef struct UndoBlockHeader {
	uint32 magic;				 /* offset  0,  PGRAC_UNDO_BLOCK_MAGIC */
	uint16 block_version;		 /* offset  4,  UNDO_BLOCK_VERSION_1 */
	uint16 slot_count;			 /* offset  6,  # of records in this block */
	uint32 free_offset;			 /* offset  8,  byte offset to next free byte */
	uint32 _pad12;				 /* offset 12,  alignment to 8B for SCN below */
	SCN first_change_scn;		 /* offset 16,  SCN of first record in block */
	XLogRecPtr first_change_lsn; /* offset 24,  PG LSN at first record (cross-correlation) */
	uint64 crc64;				 /* offset 32,  block self CRC (computed with crc64 field zeroed) */
	XLogRecPtr block_lsn;		 /* offset 40,  page-LSN of newest change (FPI-vs-delta + redo) */
} UndoBlockHeader;

StaticAssertDecl(sizeof(UndoBlockHeader) == 48,
				 "UndoBlockHeader must be 48B — HC211(40) + spec-3.18 D2 block_lsn(8)");

/* Bytes available for records + slot dir within an 8KB block. */
#define UNDO_BLOCK_PAYLOAD_BYTES (BLCKSZ - sizeof(UndoBlockHeader))

/* Initial free_offset value when block is fresh. */
#define UNDO_BLOCK_INIT_FREE_OFFSET ((uint32)sizeof(UndoBlockHeader))


/*
 * UndoSlotDirEntry -- 8 bytes per record, grows downward from end of block.
 *
 *	Each slot points back to a record's start offset + length + type + flags.
 *	The UBA row_offset (spec-3.4b) addresses the slot index;  reader follows
 *	slot → record bytes.
 */
typedef struct UndoSlotDirEntry {
	uint32 record_offset; /* byte offset within block to record start */
	uint16 record_length; /* total record byte length (header + payload) */
	uint8 record_type;	  /* UNDO_INSERT / UNDO_UPDATE / UNDO_DELETE / UNDO_ITL */
	uint8 flags;		  /* FIRST_IN_TX / CONTINUED / TOAST / etc. */
} UndoSlotDirEntry;

StaticAssertDecl(sizeof(UndoSlotDirEntry) == 8, "UndoSlotDirEntry must be 8B — HC212");


/*
 * Slot directory addressing macros.
 *
 *	Slots grow downward from end of block.  Slot 0 is at offset BLCKSZ-8;
 *	slot N is at offset BLCKSZ - 8*(N+1).
 */
#define UNDO_SLOT_DIR_OFFSET(slot_idx)                                                             \
	((uint32)(BLCKSZ - (((uint32)(slot_idx) + 1) * sizeof(UndoSlotDirEntry))))

#define UNDO_SLOT_DIR_PTR(block_buf, slot_idx)                                                     \
	((UndoSlotDirEntry *)((char *)(block_buf) + UNDO_SLOT_DIR_OFFSET(slot_idx)))


/*
 * Block layout invariant:  no record may overlap the slot directory.
 *	Block full when:  free_offset + 8 * (slot_count + 1) > BLCKSZ.
 */
static inline bool
cluster_undo_block_has_space(uint32 free_offset, uint16 slot_count, uint16 record_length)
{
	uint32 slot_dir_low = (uint32)BLCKSZ - ((uint32)(slot_count + 1) * sizeof(UndoSlotDirEntry));

	return (free_offset + record_length) <= slot_dir_low;
}


/*-------------------------------------------------------------------------
 * spec-3.27 Q12-A / §2.5 — payload-relative addressing for the
 * buffer-backed undo model.
 *
 *	Under Q12-A an undo block gains a standard PG PageHeader at offset 0
 *	(pd_lsn@0) so the standard bufmgr FlushBuffer / BufferSync / checkpoint
 *	LSN-gating applies directly.  The undo payload (UndoBlockHeader + records
 *	+ slot directory) therefore starts at offset UNDO_PAGE_HEADER_BYTES
 *	(= SizeOfPageHeaderData) and spans UNDO_PAYLOAD_BYTES bytes.
 *
 *	The legacy full-BLCKSZ macros above (UNDO_SLOT_DIR_OFFSET /
 *	cluster_undo_block_has_space) address a raw block with UndoBlockHeader@0;
 *	applying them to a payload pointer would run
 *	`payload + (BLCKSZ - 8)` past the physical page tail.  The macros below
 *	are the payload-relative replacements — every reader/writer/redo of a
 *	Q12-A undo block MUST use these (never the legacy ones) with
 *	`payload = (char *) page + UNDO_PAGE_HEADER_BYTES` as the base.
 *
 *	SizeOfPageHeaderData is a backend concept (storage/bufpage.h), so this
 *	block is guarded by #ifndef FRONTEND to keep the header frontend-safe
 *	(pg_waldump undo/ITL descriptors include this file — L8 boundary).  The
 *	frontend decodes WAL records, not on-disk page payload, so it needs only
 *	the ABI typedefs above.
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND

#include "storage/bufpage.h" /* SizeOfPageHeaderData, Page */

/* Bytes consumed by the standard PG PageHeader prepended under Q12-A. */
#define UNDO_PAGE_HEADER_BYTES ((uint32)SizeOfPageHeaderData)

/* Bytes available for the undo payload (UndoBlockHeader + records + slots). */
#define UNDO_PAYLOAD_BYTES ((uint32)(BLCKSZ - SizeOfPageHeaderData))

/* Bytes available for records + slot dir inside the payload. */
#define UNDO_PAYLOAD_BLOCK_BYTES (UNDO_PAYLOAD_BYTES - (uint32)sizeof(UndoBlockHeader))

/*
 * Payload-relative slot directory addressing.  Slot 0 is the newest record,
 * at payload offset UNDO_PAYLOAD_BYTES - 8;  slot N at UNDO_PAYLOAD_BYTES -
 * 8*(N+1).  `payload` is the UndoBlockHeader base (= page + header bytes),
 * NOT the raw page.
 */
#define UNDO_PAYLOAD_SLOT_DIR_OFFSET(slot_idx)                                                     \
	((uint32)(UNDO_PAYLOAD_BYTES - (((uint32)(slot_idx) + 1) * sizeof(UndoSlotDirEntry))))

#define UNDO_PAYLOAD_SLOT_DIR_PTR(payload, slot_idx)                                               \
	((UndoSlotDirEntry *)((char *)(payload) + UNDO_PAYLOAD_SLOT_DIR_OFFSET(slot_idx)))

/*
 * P2 payload abstraction (spec-3.27 §2.4):  single entry point that maps a
 * Q12-A undo page to its UndoBlockHeader base.  All undo reader/writer/redo
 * go through this;  raw `BufferGetBlock` casts are forbidden (would skip the
 * PageHeader offset).
 */
static inline UndoBlockHeader *
cluster_undo_page_get_payload(Page page)
{
	return (UndoBlockHeader *)((char *)page + UNDO_PAGE_HEADER_BYTES);
}

/* Payload-relative block-full test (mirrors cluster_undo_block_has_space). */
static inline bool
cluster_undo_payload_has_space(uint32 free_offset, uint16 slot_count, uint16 record_length)
{
	uint32 slot_dir_low
		= UNDO_PAYLOAD_BYTES - ((uint32)(slot_count + 1) * sizeof(UndoSlotDirEntry));

	return (free_offset + record_length) <= slot_dir_low;
}

/*
 * cluster_undo_page_init (spec-3.27 D3a / Q12-A) -- initialize a fresh undo DATA
 * block (block_no >= 1) as a standard-but-empty PG page with the UndoBlockHeader
 * seeded in its payload.
 *
 *	PageInit lays down a valid PageHeaderData at offset 0 (pd_lsn = 0, pd_lower =
 *	SizeOfPageHeaderData, pd_upper = pd_special = BLCKSZ, a valid
 *	pd_pagesize_version) so the block passes PageIsVerified when bufmgr's
 *	ReadBufferExtended(RBM_NORMAL) reads it back (D3b).  The undo payload
 *	(UndoBlockHeader + records + slot directory) lives entirely below the
 *	PageHeader, addressed with the UNDO_PAYLOAD_* macros;  PG treats it as free
 *	space and never touches it (we never call PageAddItem).
 *
 *	free_offset is payload-relative:  sizeof(UndoBlockHeader) = the first byte
 *	after the header, exactly as in the legacy full-BLCKSZ layout (the header now
 *	sits at payload offset 0 instead of raw page offset 0).  crc64 is left 0
 *	(computed lazily by the writer, matching the legacy fresh-block path).
 */
static inline void
cluster_undo_page_init(Page page, SCN first_change_scn, XLogRecPtr first_change_lsn)
{
	UndoBlockHeader *blkhdr;

	PageInit(page, BLCKSZ, 0);

	blkhdr = cluster_undo_page_get_payload(page);
	blkhdr->magic = PGRAC_UNDO_BLOCK_MAGIC;
	blkhdr->block_version = UNDO_BLOCK_VERSION_1;
	blkhdr->slot_count = 0;
	blkhdr->free_offset = (uint32)sizeof(UndoBlockHeader);
	blkhdr->_pad12 = 0;
	blkhdr->first_change_scn = first_change_scn;
	blkhdr->first_change_lsn = first_change_lsn;
	blkhdr->crc64 = 0;
	blkhdr->block_lsn = InvalidXLogRecPtr; /* == PageGetLSN(page) (0) invariant */
}

/*
 * §2.5 non-overflow invariants (P1-1).  The UndoBlockHeader plus at least
 * one record byte must fit below the slot directory, and slot 0 must land
 * strictly inside the payload (never past the physical page tail).
 */
StaticAssertDecl(SizeOfPageHeaderData + sizeof(UndoBlockHeader) + sizeof(UndoSlotDirEntry) < BLCKSZ,
				 "Q12-A undo payload too small — PageHeader + UndoBlockHeader + one slot must fit");

#endif /* !FRONTEND */


#endif /* CLUSTER_UNDO_FORMAT_H */
