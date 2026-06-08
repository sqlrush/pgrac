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


#endif /* CLUSTER_UNDO_FORMAT_H */
