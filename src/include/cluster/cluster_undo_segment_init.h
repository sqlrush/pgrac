/*-------------------------------------------------------------------------
 *
 * cluster_undo_segment_init.h
 *	  Frontend-safe pure helper that fills an 8 KB buffer with the
 *	  byte-exact layout of a freshly-allocated UndoSegmentHeaderData
 *	  page (block 0 of every seg_<id>.dat file).
 *
 *	  Used by both backend (bufpage.c PageInitUndoSegmentHeader) and
 *	  frontend (initdb.c seed segment writer) to avoid divergence: a
 *	  single byte-generation implementation, two callers.
 *
 *	  No PG buffer manager / shmem / latch / WAL dependency.  Caller
 *	  owns the destination buffer (must be 8-byte aligned to satisfy
 *	  SCN / TimestampTz field alignment).
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
 *	  src/include/cluster/cluster_undo_segment_init.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Frontend-safe: no backend-only includes.  initdb.c can include
 *	  this header to write the seed segment without linking the
 *	  backend buffer manager / WAL infrastructure.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_SEGMENT_INIT_H
#define CLUSTER_UNDO_SEGMENT_INIT_H

#include "c.h"
#include "cluster/cluster_undo_segment.h"


/*
 * cluster_undo_segment_make_header_bytes
 *
 *   Fill `page` (a BLCKSZ-byte, 8-byte-aligned buffer) with the
 *   byte-exact layout of a freshly-allocated UndoSegmentHeaderData:
 *     - Standard PG PageHeader prefix (32 bytes; spec-1.4 cluster mode)
 *       with PD_UNDO_SEG_HEADER bit set.
 *     - Segment metadata (segment_id, owner_instance, segment_state =
 *       SEGMENT_ALLOCATED, tt_slots_count = TT_SLOTS_PER_SEGMENT).
 *     - 48 TT slots in placeholder state (TT_SLOT_UNUSED status, all
 *       sentinel-zero fields).
 *     - Free block bitmap, retention info, statistics, and reserved
 *       padding zero-initialized.
 *
 *   Args:
 *     segment_id     : globally unique segment ID (uint32).
 *     owner_instance : (cluster_node_id + 1) ∈ [1, 128]; 0 reserved as
 *                      "unallocated" sentinel (spec-1.21 Q5 REVISED).
 *     page           : caller-allocated BLCKSZ buffer (must be
 *                      8-byte aligned; backend = BufferGetPage,
 *                      frontend = static aligned char[BLCKSZ]).
 *
 *   Spec: spec-1.22-undo-tablespace-bootstrap.md §2.2 + §D14c.
 */
extern void cluster_undo_segment_make_header_bytes(uint32 segment_id, uint8 owner_instance,
												   char *page);

/* Fresh V2 template only, not an upgrade of the previous destination bytes.
 * Same aligned BLCKSZ destination contract. Refusal leaves it unchanged;
 * disabled-cluster builds always refuse. Live V1 callers are not switched. */
extern bool cluster_undo_segment_make_header_v2_bytes(uint32 segment_id, uint8 owner_instance,
													char *page);


#endif /* CLUSTER_UNDO_SEGMENT_INIT_H */
