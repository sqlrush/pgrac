/*-------------------------------------------------------------------------
 *
 * cluster_block_recovery.h
 *	  pgrac online single-block recovery orchestrator (spec-4.10 D2+).
 *
 *	  Online block recovery reconstructs one corrupt / lost-write block from
 *	  WAL -- on a detached page, in a backend, outside PG's startup redo loop
 *	  -- instead of a full-database PANIC.  This header declares the
 *	  reconstruction primitive; the detection trigger (D1), the recovering-set
 *	  access gate + online install (D4), and cross-node coordination (D5) build
 *	  on top of it.
 *
 *	  cluster_block_recovery_reconstruct() is the D2 BOUNDED reconstructor: the
 *	  caller supplies an explicit WAL window [scan_lower, scan_upper]; the
 *	  function scans this node's OWN WAL stream in that window, rebuilds the
 *	  block to its last own-thread version <= scan_upper, and writes it to the
 *	  caller's page only on success.  It fails closed (UNRECOVERABLE) on any
 *	  uncertainty (no full-page-image base found, an unsupported delta, or a
 *	  header-insane result) -- it never returns a possibly-wrong page (8.A).
 *
 *	  DELIBERATELY OUT OF SCOPE here (caller / later deliverables):
 *	    - WHERE [scan_lower, scan_upper] come from (oldest-WAL / checkpoint /
 *	      PI watermark): the caller derives them (D1/D2 trigger).
 *	    - The own-thread *gate against a foreign last-writer*: this reads only
 *	      the local stream, so it is correct for single-node / single-writer
 *	      blocks; a block whose latest version (<= scan_upper) lives in a peer
 *	      thread would reconstruct to a STALE own-thread version.  The
 *	      authoritative cross-thread target check that fails such cases closed
 *	      is D5 (merged WAL / PI watermark).  Callers MUST guarantee the
 *	      own-thread precondition until then.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_block_recovery.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_BLOCK_RECOVERY_H
#define CLUSTER_BLOCK_RECOVERY_H

#include "access/xlogdefs.h"
#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

/*
 * Result of a single-block reconstruction attempt.  Anything other than
 * RECOVERED is a fail-closed signal: the caller must NOT install the block
 * and must surface a retryable / terminal error (8.A).  IN_PROGRESS (another
 * backend/node already recovering -> 53R9L retry) is produced once the
 * recovering-set lands (D4); D2 returns only RECOVERED / UNRECOVERABLE.
 */
typedef enum ClusterBlkRecResult {
	CLUSTER_BLKREC_RECOVERED,	  /* reconstructed + validated; out_page written */
	CLUSTER_BLKREC_UNRECOVERABLE, /* no FPI base / unsupported delta / validate
								   * fail -> fail-closed (terminal) */
} ClusterBlkRecResult;

/*
 * cluster.block_recovery_on_unrecoverable: action when a corrupt block cannot
 * be rebuilt (no FPI base / WAL recycled / unsupported delta / cross-node).
 */
typedef enum ClusterBlkRecAction {
	CLUSTER_BLKREC_ACTION_ERROR = 0, /* fail the read (ERRCODE_DATA_CORRUPTED) */
	CLUSTER_BLKREC_ACTION_PANIC = 1, /* escalate to PANIC (operator opt-in) */
} ClusterBlkRecAction;

/* GUCs (registered in cluster_guc.c). */
extern bool cluster_online_block_recovery;
extern int cluster_block_recovery_on_unrecoverable;

/* D6 observability: shmem counters (registered via the cluster shmem registry). */
extern void cluster_block_recovery_shmem_register(void);
extern uint64 cluster_block_recovery_get_blocks_recovered(void);
extern uint64 cluster_block_recovery_get_failclosed(void);

struct SMgrRelationData;

/*
 * cluster_block_recovery_on_read -- D1/D4 read-path entry.  On a corrupt block
 *		read, try to rebuild the block from WAL into `buffer` and durably
 *		install it.
 *
 *	Gated: cluster.online_block_recovery on, NOT during startup recovery, and
 *	SINGLE-NODE only (cluster_conf_has_peers() -> false; a multi-node block may
 *	have a foreign last-writer that the own-thread reconstruct would rebuild
 *	stale -> forward Stage 5, D5).  Derives the scan window (oldest available
 *	WAL .. flush LSN) and calls cluster_block_recovery_reconstruct.
 *
 *	On success the rebuilt page is left in `buffer` AND written back to disk
 *	(D4 durable install): the version is own-thread (pd_lsn <= flush), so the
 *	WAL it depends on is already durable -- XLogFlush is a no-op, no new WAL is
 *	produced, and a crash/torn write simply leaves the block corrupt for the
 *	next read to re-recover (idempotent).  Returns true iff the block was
 *	rebuilt (caller treats the read as verified); false otherwise (caller
 *	falls back to its zero/error policy).  Never installs a possibly-wrong
 *	block (8.A): any uncertainty -> false.
 *
 *	The recovering-set / cross-path access gate the spec sketches for D4 is
 *	subsumed here by the buffer manager's in-progress I/O (concurrent readers
 *	of the same block wait, never see the corrupt/half-rebuilt page); the
 *	explicit recovering-set + 53R9L retry is built when its real consumer
 *	lands -- cross-node coordination (D5).
 */
extern bool cluster_block_recovery_on_read(struct SMgrRelationData *reln, ForkNumber forknum,
										   BlockNumber blocknum, char *buffer);

/*
 * cluster_block_recovery_checksum_mismatch -- true iff the page has a real data
 *		checksum mismatch (Q3: detect corruption that ignore_checksum_failure
 *		would otherwise mask, so online recovery still triggers).  Returns false
 *		when data checksums are disabled or the page is new.
 */
extern bool cluster_block_recovery_checksum_mismatch(char *page, BlockNumber blocknum);

/*
 * cluster_block_recovery_reconstruct -- rebuild one block from this node's own
 *		WAL stream within an explicit window, onto a detached page.
 *
 *	Scans [scan_lower, scan_upper] of the local WAL: skips the block's records
 *	until the first apply-able full-page image (the base), then applies the
 *	post-FPI deltas in order via cluster_block_apply_one() (a later FPI resets
 *	the base; the latest FPI <= the last touch therefore wins).  The version
 *	installed is the block's LAST touching record with EndRecPtr <= scan_upper
 *	(WAL-derived, never the corrupt page's pd_lsn).
 *
 *	out_page (a BLCKSZ buffer) is written ONLY on CLUSTER_BLKREC_RECOVERED.
 */
extern ClusterBlkRecResult
cluster_block_recovery_reconstruct(RelFileLocator rlocator, ForkNumber forknum,
								   BlockNumber blocknum, XLogRecPtr scan_lower,
								   XLogRecPtr scan_upper, char *out_page);

#endif /* CLUSTER_BLOCK_RECOVERY_H */
