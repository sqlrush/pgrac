/*-------------------------------------------------------------------------
 *
 * cluster_undo_vtable.c
 *	  pgrac buffer-backed undo smgr adapter (spec-3.27 D2, §2.2 / Q3-A).
 *
 *	  Implements the sixteen f_smgr callbacks that let the PG shared buffer
 *	  manager read / write / flush / checkpoint per-instance undo segment blocks
 *	  through smgrsw[CLUSTER_UNDO_SMGR_SMGRSW_INDEX].  Each callback decodes the
 *	  SMgrRelation's reserved undo RelFileLocator (cluster_undo_buftag.h) back to
 *	  (owner_instance, segment_id) and delegates the actual block I/O to the
 *	  existing cluster_undo_smgr.c layer (BasicOpenFile + pg_pread/pwrite +
 *	  pg_fsync).
 *
 *	  fail-closed (rule 8.A):  a failed read / write ereport(ERROR)s rather than
 *	  returning a torn or zero block, so no half-written undo image can become
 *	  visible.
 *
 *	  fsync / durability:  smgr_write honours skipFsync by fsyncing the segment
 *	  file immediately when skipFsync == false (a correct-but-eager durability
 *	  barrier).  spec-3.27 D6 replaces the eager fsync with checkpoint sync-request
 *	  registration (register_dirty_segment style) so undo dirty buffers are
 *	  fsynced at checkpoint time along the standard WAL-before-data path.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.27-undo-buffer-backed-model.md (FROZEN v1.0, D2 / §2.2 / Q3-A)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_vtable.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>

#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/smgr.h"
#include "utils/elog.h"

#include "cluster/cluster_undo_buftag.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "cluster/storage/cluster_undo_vtable.h"


/*
 * undo_decode -- pull (owner_instance, segment_id) out of an undo SMgrRelation.
 *
 *	Only MAIN_FORKNUM is valid for undo (no init / fsm / vm forks).  smgropen()
 *	routed us here only for reserved-undo-spcOid locators, so the is_undo check
 *	is a defensive Assert.
 */
static inline void
undo_decode(SMgrRelation reln, ForkNumber forknum, uint8 *owner_instance, uint32 *segment_id)
{
	RelFileLocator rl = reln->smgr_rlocator.locator;

	Assert(cluster_undo_locator_is_undo(rl));

	if (forknum != MAIN_FORKNUM)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("buffer-backed undo supports only the main fork (got fork %d)",
							   (int)forknum)));

	*owner_instance = cluster_undo_locator_owner_instance(rl);
	*segment_id = (uint32)cluster_undo_locator_segment_id(rl);
}


void
cluster_undo_vtable_open(SMgrRelation reln)
{
	/*
	 * Nothing to set up:  block I/O is stateless with a lazy per-backend fd
	 * cache inside cluster_undo_smgr.c.  smgr_open must not be NULL (smgr.c
	 * calls it unconditionally at smgropen time), so this is a live no-op.
	 */
	(void)reln;
}

void
cluster_undo_vtable_close(SMgrRelation reln, ForkNumber forknum)
{
	(void)reln;
	(void)forknum;

	/*
	 * Drop the cached undo segment fd defensively so a subsequently recycled
	 * segment (spec-3.13, D5) cannot be served through a stale descriptor.
	 * The cache self-heals on (segment, owner) mismatch anyway;  this just
	 * bounds the window.
	 */
	cluster_undo_smgr_fd_cache_reset();
}

void
cluster_undo_vtable_create(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
	uint8 owner_instance;
	uint32 segment_id;

	(void)isRedo;

	/*
	 * Undo segments are normally created by the undo allocator
	 * (cluster_undo_segment_allocate), not by bufmgr.  Delegate to the
	 * idempotent create helper so an unexpected create request is still
	 * correct rather than silently dropped.
	 */
	undo_decode(reln, forknum, &owner_instance, &segment_id);
	(void)cluster_undo_smgr_create_segment_file(segment_id, owner_instance);
}

bool
cluster_undo_vtable_exists(SMgrRelation reln, ForkNumber forknum)
{
	RelFileLocator rl = reln->smgr_rlocator.locator;
	uint8 owner_instance;
	uint32 segment_id;
	char path[MAXPGPATH];

	Assert(cluster_undo_locator_is_undo(rl));

	if (forknum != MAIN_FORKNUM)
		return false; /* undo has only a main fork */

	owner_instance = cluster_undo_locator_owner_instance(rl);
	segment_id = (uint32)cluster_undo_locator_segment_id(rl);

	if (cluster_undo_path_resolve(owner_instance, segment_id, path, sizeof(path)) != 0)
		return false;

	return (access(path, F_OK) == 0);
}

void
cluster_undo_vtable_unlink(RelFileLocatorBackend rlocator, ForkNumber forknum, bool isRedo)
{
	(void)rlocator;
	(void)forknum;
	(void)isRedo;

	/*
	 * No-op:  undo segments never enter PG's transactional pending-delete /
	 * smgrdounlink path (spec-3.27 §4 pt 6).  Segment recycle is driven by the
	 * undo cleaner (spec-3.13, D5), not by bufmgr / commit-time unlink.  A call
	 * here would be a caller bug;  do nothing rather than unlink an undo file.
	 */
}

void
cluster_undo_vtable_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						   const void *buffer, bool skipFsync)
{
	uint8 owner_instance;
	uint32 segment_id;

	undo_decode(reln, forknum, &owner_instance, &segment_id);

	if (blocknum >= UNDO_BLOCKS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("undo segment extend past fixed size: block %u >= %u", blocknum,
							   (uint32)UNDO_BLOCKS_PER_SEGMENT)));

	if (!cluster_undo_smgr_write_block(segment_id, owner_instance, blocknum, (const char *)buffer,
									   !skipFsync))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not extend undo segment %u (instance %u) block %u",
							   segment_id, owner_instance, blocknum)));

	reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
}

void
cluster_undo_vtable_zeroextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							   int nblocks, bool skipFsync)
{
	uint8 owner_instance;
	uint32 segment_id;
	PGAlignedBlock zerobuf;
	int i;

	undo_decode(reln, forknum, &owner_instance, &segment_id);

	if (nblocks <= 0)
		return;
	if ((uint64)blocknum + (uint64)nblocks > (uint64)UNDO_BLOCKS_PER_SEGMENT)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("undo segment zero-extend past fixed size: [%u, %u) > %u", blocknum,
							   blocknum + (uint32)nblocks, (uint32)UNDO_BLOCKS_PER_SEGMENT)));

	memset(zerobuf.data, 0, BLCKSZ);
	for (i = 0; i < nblocks; i++) {
		bool do_fsync = (!skipFsync && i == nblocks - 1);

		if (!cluster_undo_smgr_write_block(segment_id, owner_instance, blocknum + i, zerobuf.data,
										   do_fsync))
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not zero-extend undo segment %u (instance %u) block %u",
								   segment_id, owner_instance, blocknum + i)));
	}

	reln->smgr_cached_nblocks[forknum] = InvalidBlockNumber;
}

bool
cluster_undo_vtable_prefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum)
{
	(void)reln;
	(void)forknum;
	(void)blocknum;
	return false; /* prefetch hint not implemented for undo */
}

void
cluster_undo_vtable_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, void *buffer)
{
	uint8 owner_instance;
	uint32 segment_id;

	undo_decode(reln, forknum, &owner_instance, &segment_id);

	if (!cluster_undo_smgr_read_block(segment_id, owner_instance, blocknum, (char *)buffer))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not read undo segment %u (instance %u) block %u", segment_id,
							   owner_instance, blocknum)));
}

void
cluster_undo_vtable_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
						  const void *buffer, bool skipFsync)
{
	uint8 owner_instance;
	uint32 segment_id;

	undo_decode(reln, forknum, &owner_instance, &segment_id);

	/*
	 * do_fsync = !skipFsync:  bufmgr passes skipFsync=false for FlushBuffer /
	 * checkpoint writes that must be made durable.  D2 satisfies that with an
	 * eager fsync;  D6 swaps in checkpoint sync-request registration.
	 */
	if (!cluster_undo_smgr_write_block(segment_id, owner_instance, blocknum, (const char *)buffer,
									   !skipFsync))
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write undo segment %u (instance %u) block %u", segment_id,
							   owner_instance, blocknum)));
}

void
cluster_undo_vtable_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
							  BlockNumber nblocks)
{
	(void)reln;
	(void)forknum;
	(void)blocknum;
	(void)nblocks;
	/* writeback is an advisory hint (sync_file_range);  a no-op is safe. */
}

BlockNumber
cluster_undo_vtable_nblocks(SMgrRelation reln, ForkNumber forknum)
{
	Assert(cluster_undo_locator_is_undo(reln->smgr_rlocator.locator));

	if (forknum != MAIN_FORKNUM)
		return 0;

	/*
	 * Undo segments are pre-extended to full size (64 MB / UNDO_BLOCKS_PER_
	 * SEGMENT) by the allocator's unconditional ftruncate (cluster_undo_alloc.c),
	 * so a live segment always spans exactly UNDO_BLOCKS_PER_SEGMENT blocks.
	 * Returning the constant keeps bufmgr off the extend path;  a read of an
	 * unallocated segment fails closed in cluster_undo_vtable_read.
	 */
	return (BlockNumber)UNDO_BLOCKS_PER_SEGMENT;
}

void
cluster_undo_vtable_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber old_blocks,
							 BlockNumber nblocks)
{
	(void)reln;
	(void)forknum;
	(void)old_blocks;
	(void)nblocks;

	/*
	 * No-op:  undo segments are fixed-size and never truncated through bufmgr.
	 * Segment recycle (spec-3.13, D5) invalidates buffers and reuses the whole
	 * segment;  it does not drive smgr_truncate.
	 */
}

void
cluster_undo_vtable_immedsync(SMgrRelation reln, ForkNumber forknum)
{
	uint8 owner_instance;
	uint32 segment_id;

	undo_decode(reln, forknum, &owner_instance, &segment_id);

	if (!cluster_undo_smgr_fsync_segment_file(segment_id, owner_instance))
		ereport(ERROR,
				(errcode_for_file_access(), errmsg("could not fsync undo segment %u (instance %u)",
												   segment_id, owner_instance)));
}
