/*-------------------------------------------------------------------------
 *
 * cluster_undo_xlog.c
 *	  pgrac undo segment WAL resource manager (RM_CLUSTER_UNDO).
 *
 *	  Stage 1.22 ships ONE subtype: XLOG_UNDO_SEGMENT_INIT, emitted by
 *	  cluster_undo_segment_allocate when block 0 of a fresh seg_<id>.dat
 *	  file is initialized.  redo handler reads the payload and pwrites
 *	  the page image directly to the segment file via cluster_undo_path
 *	  (no PG buffer manager / smgr involvement -- pg_undo files live
 *	  outside PG's RelFileLocator namespace).
 *
 *	  The payload identification scheme (instance + segment_id) is the
 *	  v0.2 P1-A 修订: XLOG_FPI assumes RelFileLocator/ForkNumber/BlockNumber
 *	  block tags routed via XLogReadBufferForRedo() -> smgr -> relpath(),
 *	  which is incompatible with $PGDATA/pg_undo/instance_N/seg_M.dat.
 *	  Custom RM record bypasses that routing layer entirely.
 *
 *	  Spec: spec-1.22-undo-tablespace-bootstrap.md §D14a.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/storage/cluster_undo_xlog.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogreader.h"
#include "access/xlogutils.h"
#include "cluster/cluster_guc.h"				/* cluster_undo_gcs_coherence (D2-2) */
#include "cluster/cluster_hw.h"					/* spec-5.7 D1 HW authority apply/encode */
#include "cluster/cluster_mode.h"				/* cluster_peer_mode_enabled (D2-2) */
#include "cluster/cluster_tt_status.h"			/* spec-3.16 D5 recovery counters */
#include "cluster/cluster_tt_durable.h"			/* spec-3.11: redo decision predicate */
#include "cluster/cluster_undo_gcs.h"			/* shared-root decision (D2-2 裁决 A) */
#include "cluster/cluster_undo_segment.h"		/* UNDO_SEGMENT_SIZE_BYTES */
#include "cluster/storage/cluster_shared_fs.h"	/* undo shared-root dir resolve (D2-2) */
#include "cluster/storage/cluster_undo_alloc.h" /* path resolve + intent (3.13 reuse redo) */
#include "cluster/storage/cluster_undo_buf.h"	/* spec-3.18 D2b write-back gate */
#include "cluster/storage/cluster_undo_xlog.h"
#include "common/file_perm.h" /* pg_mkdir_p / pg_dir_create_mode (shared subdir, D2-2) */
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/proc.h" /* spec-3.18 D2b MyProc->delayChkptFlags assert */


/*
 * Build the undo segment path for the REDO write surface.
 *
 *   spec-5.22b D2-2 (Hardening v1.0.1 裁决 A): the redo write surface must
 *   resolve to the SAME physical root as the runtime write surface, or an
 *   own-instance segment would be read from the shared root (runtime) while
 *   redo re-stamps the local copy -- own-instance undo split-brain (block-0
 *   TT is WAL-redo-only, so a lost redo write on the shared copy is a
 *   false-invisible 8.A hazard).  So this delegates to the ONE decision
 *   source, cluster_undo_path_resolve: own redo (rec->instance == self) under
 *   coherence resolves to the shared cluster_fs root; a foreign rec->instance
 *   (dead-origin materialization rebuilt locally) resolves to the local
 *   DataDir.  Callers pass cluster_undo_intent_for_owner(rec->instance).
 *
 *   Returns 0 on success, -1 on path-too-long / unset shared root (fail-
 *   closed).  Caller supplies buf with capacity >= MAXPGPATH.
 */
static int
build_undo_segment_path(ClusterUndoPathIntent intent, uint8 owner_instance, uint32 segment_id,
						char *buf, size_t buf_size)
{
	return cluster_undo_path_resolve(intent, owner_instance, segment_id, buf, buf_size);
}


/*
 * Ensure $PGDATA/pg_undo/instance_<N>/ exists (creates if missing).
 *
 *   Idempotent: EEXIST is not an error.  PANIC on any other failure
 *   (recovery contract -- a half-replayed undo segment is corruption).
 *
 *   Stage 1.22 redo path is the primary user: standbys + cross-instance
 *   crash recovery may see XLOG_UNDO_SEGMENT_INIT records for instance
 *   subdirs that initdb never created locally.  pg_undo/ itself is
 *   established by initdb (D4) and assumed to exist.
 */
static void
ensure_undo_instance_subdir(uint8 owner_instance)
{
	char path[MAXPGPATH];
	int ret;

	Assert(owner_instance >= 1 && owner_instance <= UNDO_OWNER_INSTANCE_MAX);

	/*
	 * spec-5.22b D2-2: own-instance redo under coherence materializes the
	 * segment on the shared cluster_fs root, whose pg_undo/instance_<N> tree
	 * is not seeded by initdb -- create the whole path (idempotent).  A
	 * foreign owner (dead-origin materialization) stays on the local DataDir
	 * (裁决 A), so only own redo under coherence takes the shared branch.
	 */
	if (cluster_undo_path_uses_shared_root(cluster_undo_intent_for_owner(owner_instance),
										   cluster_peer_mode_enabled(),
										   cluster_undo_gcs_coherence)) {
		if (cluster_shared_fs_undo_instance_dir_resolve(owner_instance, path, sizeof(path)) != 0)
			ereport(PANIC, (errmsg("undo shared instance subdir unresolved: owner_instance=%u "
								   "(cluster.shared_data_dir unset)",
								   (unsigned)owner_instance)));
		if (pg_mkdir_p(path, pg_dir_create_mode) != 0 && errno != EEXIST)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not create undo shared instance subdir \"%s\": %m", path)));
		return;
	}

	/* local DataDir: pg_undo seeded by initdb; single-level mkdir (P1-1 naming). */
	ret = snprintf(path, sizeof(path), "%s/pg_undo/instance_%u", DataDir,
				   (unsigned)(owner_instance - 1));
	if (ret < 0 || (size_t)ret >= sizeof(path))
		ereport(PANIC, (errmsg("undo instance subdir path too long: owner_instance=%u",
							   (unsigned)owner_instance)));

	if (mkdir(path, S_IRWXU) != 0 && errno != EEXIST)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not create undo instance subdir \"%s\": %m", path)));
}


/*
 * cluster_undo_redo_open_segment -- open an undo segment for redo,
 *	MATERIALIZING it (parent dir + O_CREAT + ftruncate to the full segment
 *	size) when it is missing and the record can rebuild the touched block on
 *	its own.
 *
 *	spec-4.5a: a crashed peer's undo segment is materialized on the SURVIVING
 *	node from the merged-replay window, which begins at the peer's last
 *	checkpoint.  The segment's XLOG_UNDO_SEGMENT_INIT can predate that window,
 *	so the FIRST post-checkpoint touch -- a full-page-image block write, or the
 *	folded TT commit stamp that read-modify-writes block 0 -- has to create the
 *	file.  These records are self-contained (the FPI carries the whole block;
 *	the TT stamp overwrites only its slot, and the durable-TT reader keys on
 *	the slot, not the header magic).  A DELTA (non-FPI) block write must NOT
 *	create the file: it needs the prior block content, so a genuine
 *	INIT-before-WRITE ordering violation still fails closed.  Returns the fd, or
 *	-1 with errno set (the caller PANICs with its own message).
 */
static int
cluster_undo_redo_open_segment(uint8 instance, uint32 segment_id, const char *path,
							   bool create_if_missing)
{
	int fd = BasicOpenFile(path, O_RDWR | PG_BINARY);

	if (fd >= 0 || errno != ENOENT || !create_if_missing)
		return fd;

	(void)segment_id; /* path already encodes it; arg kept for symmetry */
	ensure_undo_instance_subdir(instance);
	fd = BasicOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);
	if (fd < 0)
		return fd;
	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		int save = errno;

		close(fd);
		errno = save;
		return -1;
	}
	return fd;
}


/*
 * cluster_undo_emit_segment_init
 *
 *   Backend caller emits XLOG_UNDO_SEGMENT_INIT for the just-written
 *   segment header block.  page_image is the 8 KB page bytes (typically
 *   just produced by PageInitUndoSegmentHeader on a locked buffer; the
 *   caller copies them here so the WAL record is self-contained -- the
 *   redo handler doesn't need a buffer manager lookup).
 */
XLogRecPtr
cluster_undo_emit_segment_init(uint8 instance, uint32 segment_id, const char *page_image)
{
	xl_cluster_undo_segment_init hdr;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(page_image != NULL);

	memset(&hdr, 0, sizeof(hdr));
	hdr.instance = instance;
	hdr.segment_id = segment_id;

	XLogBeginInsert();
	XLogRegisterData((char *)&hdr, sizeof(hdr));
	XLogRegisterData((char *)page_image, BLCKSZ);

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_INIT);

	return lsn;
}


/*
 * cluster_hw_emit_reserve (spec-5.7a D1, §3.1a M1c)
 *
 *   Emit a durable HWM advance.  The master XLogFlush(returned lsn) BEFORE it
 *   replies the granted range, so no block is ever used without a durable
 *   reservation.  24-byte delta, no page image.
 */
XLogRecPtr
cluster_hw_emit_reserve(RelFileLocator rloc, ForkNumber fork, BlockNumber new_hwm, uint32 granted)
{
	xl_hw_reserve rec;
	XLogRecPtr lsn;

	memset(&rec, 0, sizeof(rec));
	rec.dbOid = (uint32)rloc.dbOid;
	rec.relNumber = (uint32)rloc.relNumber;
	rec.spcOid = (uint32)rloc.spcOid;
	rec.fork = (uint32)fork;
	rec.new_hwm = (uint32)new_hwm;
	rec.granted = granted;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_HW_RESERVE);

	return lsn;
}

/*
 * cluster_hw_redo_reserve (spec-5.7a D1, §3.1a M1e; amend #3)
 *
 *   Rebuild the in-memory HW authority HWM from WAL.  Raises the (rel,fork)
 *   authority entry to at least the recorded high-water mark (monotone,
 *   create-if-absent via cluster_hw_apply_hwm), so a crash / remaster
 *   reconstructs the authority from HW_RESERVE alone (new relations are owned
 *   from block 0; there is no HW_SEED).  No file/page I/O -- the authority is a
 *   runtime shmem table, not on-disk state.
 */
static void
cluster_hw_redo_reserve(XLogReaderState *record)
{
	const xl_hw_reserve *rec;
	RelFileLocator rloc;
	ClusterResId resid;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC,
				(errmsg("invalid XLOG_HW_RESERVE record length: %u", XLogRecGetDataLen(record))));
	rec = (const xl_hw_reserve *)XLogRecGetData(record);

	rloc.dbOid = rec->dbOid;
	rloc.relNumber = rec->relNumber;
	rloc.spcOid = rec->spcOid;
	cluster_hw_resid_encode(rloc, (ForkNumber)rec->fork, &resid);
	cluster_hw_apply_hwm(&resid, (BlockNumber)rec->new_hwm);
}


/*
 * cluster_undo_emit_tt_slot_commit (spec-3.11 D3)
 *
 *   Emit a 24-byte delta record durably stamping commit_scn on one TT slot.
 *   No page image (unlike segment_init): redo does a block-0 RMW.  Caller
 *   inserts this BEFORE the commit XLOG record (spec-3.11 C1); group commit /
 *   the commit record's XLogFlush make it durable (no independent fsync --
 *   spec-3.11 C10).
 */
XLogRecPtr
cluster_undo_emit_tt_slot_commit(uint8 instance, uint32 segment_id, uint16 slot_offset, uint16 wrap,
								 TransactionId xid, SCN commit_scn)
{
	xl_undo_tt_slot_commit rec;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.slot_offset = slot_offset;
	rec.wrap = wrap;
	rec.xid = xid;
	rec.instance = instance;
	rec.commit_scn = commit_scn;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_COMMIT);

	return lsn;
}


/*
 * cluster_undo_emit_tt_slot_abort -- spec-3.15 D5 (ROLLBACK PREPARED).
 *
 *   Mirrors emit_tt_slot_commit; durability follows the same C10
 *   contract (the prepared-abort WAL flush carries this record).
 */
XLogRecPtr
cluster_undo_emit_tt_slot_abort(uint8 instance, uint32 segment_id, uint16 slot_offset, uint16 wrap,
								TransactionId xid)
{
	xl_undo_tt_slot_abort rec;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.slot_offset = slot_offset;
	rec.wrap = wrap;
	rec.xid = xid;
	rec.instance = instance;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_ABORT);

	return lsn;
}


/*
 * cluster_undo_emit_tt_slot_set_head -- spec-4.8 D7-A.
 *
 *   Mirrors emit_tt_slot_abort; emitted at ROLLBACK PREPARED prefinish AFTER
 *   the 0x60 abort (so the slot already owns this xid/wrap at redo time).  The
 *   prepared-abort WAL flush carries it (same C10 durability contract).
 */
XLogRecPtr
cluster_undo_emit_tt_slot_set_head(uint8 instance, uint32 segment_id, uint16 slot_offset,
								   uint16 wrap, TransactionId xid, UBA first_undo_block)
{
	xl_undo_tt_slot_set_head rec;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(slot_offset < TT_SLOTS_PER_SEGMENT);

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.slot_offset = slot_offset;
	rec.wrap = wrap;
	rec.xid = xid;
	rec.instance = instance;
	rec.first_undo_block = first_undo_block;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_TT_SLOT_SET_HEAD);

	return lsn;
}


/*
 * cluster_undo_emit_segment_recycle -- spec-3.13 D3.
 *
 *   WAL-before-data with EXPLICIT durability (spec-3.13 v0.3 (1)):
 *   the caller sequence is XLogFlush(returned lsn) -> pwrite block 0
 *   -> fsync segment file.  Pure WAL+redo is NOT sufficient for these
 *   direct pg_undo writes: once a checkpoint advances past this
 *   record, a page-cache-lost header rewrite would never be replayed
 *   (pg_undo files are not in the checkpointer sync queue).
 */
XLogRecPtr
cluster_undo_emit_segment_recycle(uint8 instance, uint32 segment_id, uint32 expected_generation,
								  uint8 old_state, uint8 new_state)
{
	xl_undo_segment_recycle rec;
	XLogRecPtr lsn;

	Assert(instance >= 1);

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.expected_generation = expected_generation;
	rec.instance = instance;
	rec.old_state = old_state;
	rec.new_state = new_state;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_RECYCLE);

	return lsn;
}


/*
 * cluster_undo_emit_segment_reuse -- spec-3.13 D4.
 *
 *   Registers the 16B record header plus the full BLCKSZ fresh block-0
 *   image.  Same v0.3 (1) caller contract as recycle: XLogFlush(lsn)
 *   -> pwrite block 0 -> fsync segment file.
 */
XLogRecPtr
cluster_undo_emit_segment_reuse(uint8 instance, uint32 segment_id, uint32 old_generation,
								uint32 new_generation, const char *fresh_header_image)
{
	xl_undo_segment_reuse rec;
	XLogRecPtr lsn;

	Assert(instance >= 1);
	Assert(new_generation == old_generation + 1);
	Assert(fresh_header_image != NULL);

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.old_generation = old_generation;
	rec.new_generation = new_generation;
	rec.instance = instance;

	XLogBeginInsert();
	XLogRegisterData((char *)&rec, sizeof(rec));
	XLogRegisterData(unconstify(char *, fresh_header_image), BLCKSZ);

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_SEGMENT_REUSE);

	return lsn;
}


/*
 * cluster_undo_emit_block_write (spec-3.18 D2a)
 *	  Emit XLOG_UNDO_BLOCK_WRITE carrying the full BLCKSZ image of one undo
 *	  data block (block_no >= 1).  D2a is always-FPI: redo restores the image
 *	  wholesale, which is torn-write safe without any checkpoint-relative FPI
 *	  decision (the 3-range delta + DELAY_CHKPT_START race-close is D2b).  The
 *	  caller stamps the returned LSN into the block's block_lsn header field
 *	  before its write-through (§2.6: block_lsn is the record's own LSN, never
 *	  carried in the WAL body).
 */
XLogRecPtr
cluster_undo_emit_block_write(uint8 instance, uint32 segment_id, uint32 block_no,
							  const char *block_image, XLogRecPtr old_block_lsn, uint16 rec_off,
							  uint16 rec_len, uint16 slot_off)
{
	xl_undo_block_write rec;
	XLogRecPtr lsn;
	bool use_delta = false;

	Assert(instance >= 1);
	Assert(block_no >= 1); /* block 0 is the segment header (SEGMENT_INIT/REUSE) */
	Assert(block_image != NULL);

	/*
	 * FPI-vs-delta decision (§2.6 v0.8).  Always-FPI while write-back is gated
	 * off (D2a): the full image self-repairs torn writes with no checkpoint-
	 * relative decision.  With write-back on (D2b): emit a 3-range delta unless
	 * a full image is required:
	 *   - the block is fresh (old_block_lsn invalid) -- a delta has no on-disk
	 *     base to apply onto, so the first write must carry the whole block; OR
	 *   - full_page_writes is ON and this is the block's first touch since the
	 *     last checkpoint (old_block_lsn <= RedoRecPtr) -- the checkpoint flush
	 *     of this block may have torn, so the first post-checkpoint write FPIs
	 *     to give redo a clean base.
	 * When full_page_writes is OFF the storage guarantees atomic block writes
	 * (no torn writes), so the checkpoint-flushed block is an intact delta base
	 * -- a delta is safe (matching PG, which also omits full-page images then).
	 * The caller holds DELAY_CHKPT_START across this + the block write, which
	 * (with undo blocks in the checkpoint-flush set) closes the FPW race.
	 */
	if (cluster_undo_buf_writeback_allowed()) {
		XLogRecPtr redo;
		XLogRecPtr stale_redo;
		bool do_page_writes;

		Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);

		/*
		 * The FPI-vs-delta decision MUST use the authoritative redo pointer,
		 * not the backend-local cache.  GetFullPageWriteInfo() returns the
		 * stale cached RedoRecPtr (xlog.c), which is only refreshed when this
		 * backend itself inserts WAL / calls GetRedoRecPtr().  A checkpoint
		 * advances XLogCtl->RedoRecPtr (under info_lck) BEFORE it waits on
		 * DELAY_CHKPT_START, so the cache can lag the real redo point:  using
		 * it could pick a delta for a block whose first post-checkpoint touch
		 * needs an FPI, and a torn write would then be unrecoverable from the
		 * post-checkpoint redo stream.  GetRedoRecPtr() reads
		 * XLogCtl->RedoRecPtr under info_lck;  with DELAY_CHKPT_START held the
		 * checkpoint that advanced redo is blocked on us, so the value is
		 * current AND stable until we release.  This mirrors PG's own
		 * XLogSaveBufferForHint, which also uses GetRedoRecPtr() under
		 * DELAY_CHKPT_START.  GetFullPageWriteInfo is used only for
		 * do_page_writes (full_page_writes / forced-FPW state).
		 */
		GetFullPageWriteInfo(&stale_redo, &do_page_writes);
		(void)stale_redo; /* discarded; GetRedoRecPtr() is the authoritative redo */
		redo = GetRedoRecPtr();
		/* delta unless fresh (no base) or FPW-on first-post-checkpoint touch. */
		use_delta
			= !XLogRecPtrIsInvalid(old_block_lsn) && (!do_page_writes || old_block_lsn > redo);
	}

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.block_no = block_no;
	rec.instance = instance;
	rec.has_fpi = use_delta ? 0 : 1;

	XLogBeginInsert();
	if (use_delta) {
		rec.rec_off = rec_off;
		rec.rec_len = rec_len;
		rec.slot_off = slot_off;
		XLogRegisterData((char *)&rec, sizeof(rec));
		/* body = hdr_prefix [0,40) ++ record ++ slot (matches the redo apply). */
		XLogRegisterData(unconstify(char *, block_image), UNDO_BLOCK_HDR_PREFIX_LEN);
		XLogRegisterData(unconstify(char *, block_image) + rec_off, rec_len);
		XLogRegisterData(unconstify(char *, block_image) + slot_off, sizeof(UndoSlotDirEntry));
	} else {
		XLogRegisterData((char *)&rec, sizeof(rec));
		XLogRegisterData(unconstify(char *, block_image), BLCKSZ);
	}

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE);

	return lsn;
}

/*
 * cluster_undo_emit_block_write_multi (spec-3.25 D1b)
 *
 *	  Multi-record sibling of cluster_undo_emit_block_write: one record carries
 *	  ALL records this transaction appended to one undo data block (a single
 *	  contiguous span -- the allocator appends sequentially) plus their slot-dir
 *	  entries as a single span.  The FPI-vs-delta decision is identical to the
 *	  single-record emitter (same §2.6 v0.8 rules; old_block_lsn is the block
 *	  LSN captured when the deferred image was loaded, i.e. pre-batch).
 */
XLogRecPtr
cluster_undo_emit_block_write_multi(uint8 instance, uint32 segment_id, uint32 block_no,
									const char *block_image, XLogRecPtr old_block_lsn,
									uint16 rec_off, uint16 rec_len, uint16 slot_off,
									uint16 slot_len)
{
	xl_undo_block_write_multi rec;
	XLogRecPtr lsn;
	bool use_delta = false;

	Assert(instance >= 1);
	Assert(block_no >= 1);
	Assert(block_image != NULL);
	Assert(rec_len > 0 && slot_len > 0);

	/* Same decision as cluster_undo_emit_block_write (see its comment). */
	if (cluster_undo_buf_writeback_allowed()) {
		XLogRecPtr redo;
		XLogRecPtr stale_redo;
		bool do_page_writes;

		Assert((MyProc->delayChkptFlags & DELAY_CHKPT_START) != 0);

		GetFullPageWriteInfo(&stale_redo, &do_page_writes);
		(void)stale_redo; /* discarded; GetRedoRecPtr() is the authoritative redo */
		redo = GetRedoRecPtr();
		use_delta
			= !XLogRecPtrIsInvalid(old_block_lsn) && (!do_page_writes || old_block_lsn > redo);
	}

	memset(&rec, 0, sizeof(rec));
	rec.segment_id = segment_id;
	rec.block_no = block_no;
	rec.instance = instance;
	rec.has_fpi = use_delta ? 0 : 1;

	XLogBeginInsert();
	if (use_delta) {
		rec.rec_off = rec_off;
		rec.rec_len = rec_len;
		rec.slot_off = slot_off;
		rec.slot_len = slot_len;
		XLogRegisterData((char *)&rec, sizeof(rec));
		/* body = hdr_prefix [0,40) ++ records span ++ slot-dir span. */
		XLogRegisterData(unconstify(char *, block_image), UNDO_BLOCK_HDR_PREFIX_LEN);
		XLogRegisterData(unconstify(char *, block_image) + rec_off, rec_len);
		XLogRegisterData(unconstify(char *, block_image) + slot_off, slot_len);
	} else {
		XLogRegisterData((char *)&rec, sizeof(rec));
		XLogRegisterData(unconstify(char *, block_image), BLCKSZ);
	}

	lsn = XLogInsert(RM_CLUSTER_UNDO_ID, XLOG_UNDO_BLOCK_WRITE_MULTI);

	return lsn;
}


/*
 * Replay XLOG_UNDO_SEGMENT_INIT.
 *
 *   Spec-1.22 Hardening v1.0.3: full idempotent create + size + restore
 *   semantics for crash / standby replay.  Because pg_undo/ files live
 *   outside PG's RelFileLocator namespace, neither SMGR nor XLOG_FPI
 *   manage file lifecycle for us; the redo handler must own:
 *     1. mkdir parent directory (instance_<N>/) if missing
 *     2. open(O_CREAT | O_RDWR) (creates file if missing)
 *     3. ftruncate to UNDO_SEGMENT_SIZE_BYTES (extends sparse if missing)
 *     4. pwrite block 0 with the WAL-shipped page image
 *     5. fsync file
 *     6. fsync parent directory (durable dirent for create case)
 *
 *   Idempotent across replay scenarios:
 *     - Segment + dir + size all present (allocator path normal): every
 *       step is a no-op except pwrite (overwrites block 0 with same bytes)
 *     - Standby never saw the allocator: full create + extend + write
 *     - Operator deleted seg_<id>.dat between checkpoints: rebuild
 *     - Crash mid-allocator (between create and extend): ftruncate
 *       extends to full size; pwrite restores block 0
 *
 *   Errors promote to PANIC per the standard recovery contract.
 */
static void
cluster_undo_redo_segment_init(XLogReaderState *record)
{
	xl_cluster_undo_segment_init *hdr;
	char *payload;
	const char *page_image;
	char path[MAXPGPATH];
	int fd;
	ssize_t written;

	payload = XLogRecGetData(record);
	if (XLogRecGetDataLen(record) != sizeof(*hdr) + BLCKSZ)
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_SEGMENT_INIT record length: %u",
							   XLogRecGetDataLen(record))));

	hdr = (xl_cluster_undo_segment_init *)payload;
	page_image = payload + sizeof(*hdr);

	if (build_undo_segment_path(cluster_undo_intent_for_owner(hdr->instance), hdr->instance,
								hdr->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", hdr->instance,
							   hdr->segment_id)));

	/* Step 1: ensure parent instance subdir exists (idempotent on EEXIST). */
	ensure_undo_instance_subdir(hdr->instance);

	/* Step 2: open the segment file, creating if missing. */
	fd = BasicOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not open or create undo segment file \"%s\": %m", path)));

	/*
	 * Step 3: ensure file is exactly UNDO_SEGMENT_SIZE_BYTES (64 MB).
	 * ftruncate is idempotent: shrink-to-same-size and extend-to-target
	 * are both no-ops when the file already has the target size.  Tail
	 * bytes are sparse zeros until the allocator path writes real undo
	 * records (deferred to feature-117).
	 */
	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not extend undo segment file \"%s\" to %d bytes: %m", path,
							   UNDO_SEGMENT_SIZE_BYTES)));
	}

	/* Step 4: pwrite block 0 with the WAL-shipped page image. */
	written = pg_pwrite(fd, page_image, BLCKSZ, 0);
	if (written != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write undo segment header for \"%s\": "
							   "wrote %zd of %d bytes",
							   path, written, BLCKSZ)));
	}

	/* Step 5: fsync the file (durable block 0 + tail allocation). */
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync undo segment file \"%s\": %m", path)));
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));

	/*
	 * Step 6: fsync the parent directory (instance_<N>/).  Required for
	 * the create case (Step 2 with O_CREAT) so the dirent is durable;
	 * harmless for the already-exists case.  fsync_parent_path is
	 * idempotent and tolerates missing intermediate directories.
	 */
	{
		char dir[MAXPGPATH];
		int dret;

		/* directory uses cluster_node_id (= owner_instance - 1) per Hardening v1.0.4 P1-1 */
		dret = snprintf(dir, sizeof(dir), "%s/pg_undo/instance_%u", DataDir,
						(unsigned)(hdr->instance - 1));
		if (dret >= 0 && (size_t)dret < sizeof(dir))
			fsync_fname(dir, true);
	}

	cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
}


/* spec-4.8 D7-A (P1#1): canonical invalid chain head for commit/abort redo. */
static const UBA InvalidUbaVal = InvalidUba_init;

/*
 * cluster_tt_durable_redo_stamp_slot -- shared block-0 RMW that stamps one
 * TTSlot COMMITTED + commit_scn during recovery (spec-3.11 D3 / spec-3.18 D4.1).
 *
 *   Called from two redo sites with identical semantics:
 *     - cluster_undo_redo_tt_slot_commit() below, replaying the standalone 0x30
 *       (2PC COMMIT PREPARED path).
 *     - xact_redo_commit() (xact.c), replaying the D4.1 xl_xact_tt_commit delta
 *       folded into a normal commit record.
 *   Sharing one primitive guarantees the fold redo can never diverge from the
 *   0x30 redo (规则 8.A: visibility/recovery correctness must not fork).
 *
 *   Block-0 read-modify-write gated by the shared last-writer-wins wrap
 *   predicate (cluster_tt_durable_redo_decide; spec-3.11 v0.3 F1).  The segment
 *   + header block are created by the preceding XLOG_UNDO_SEGMENT_INIT in WAL
 *   order, so the file must already exist (open without O_CREAT; missing = WAL
 *   ordering violation = PANIC).  For the fold path the same ordering holds: the
 *   segment's 0x10 SEGMENT_INIT precedes any undo write, which precedes this
 *   xact's commit record.
 *
 *   Redo decision:
 *     rec.wrap >= slot.wrap -> APPLY (fresh UNUSED slot, FREE-path same-wrap
 *                              reuse, recycle, or idempotent replay)
 *     rec.wrap <  slot.wrap -> SKIP  (newer generation already durable)
 *     invalid slot.status   -> PANIC (garbage byte, not a legal TTSlot state)
 *
 *   L47 idempotence: same-record re-replay reaches the same on-disk state.
 *   fsync makes the replayed slot durable (recovery contract).  No buffer
 *   manager -- pg_undo files are outside RelFileLocator namespace (no FPI /
 *   page-LSN skip), so the wrap table is the stale-safety mechanism.
 */
void
cluster_tt_durable_redo_stamp_slot(uint8 instance, uint32 segment_id, uint16 slot_offset,
								   uint16 wrap, TransactionId xid, SCN commit_scn)
{
	char path[MAXPGPATH];
	int fd;
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;
	TTSlot *slot;
	ssize_t nread;

	if (slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(PANIC, (errmsg("TT slot commit redo: slot_offset %u out of range (max %d)",
							   slot_offset, TT_SLOTS_PER_SEGMENT - 1)));

	if (build_undo_segment_path(cluster_undo_intent_for_owner(instance), instance, segment_id, path,
								sizeof(path))
		!= 0)
		ereport(PANIC,
				(errmsg("undo segment path too long: instance=%u seg=%u", instance, segment_id)));

	/* spec-4.5a: materialize block 0 if missing -- a peer's TT commit can be
	 * the first post-checkpoint touch of a segment whose INIT predates the
	 * merge window.  The RMW below overwrites only the slot. */
	fd = cluster_undo_redo_open_segment(instance, segment_id, path, true);
	if (fd < 0)
		ereport(
			PANIC,
			(errcode_for_file_access(),
			 errmsg("could not open undo segment file \"%s\" for TT slot commit redo: %m", path),
			 errhint("XLOG_UNDO_SEGMENT_INIT must precede the TT slot commit in WAL.")));

	nread = pg_pread(fd, blockbuf.data, BLCKSZ, 0);
	if (nread != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read undo segment header \"%s\": read %zd of %d bytes",
							   path, nread, BLCKSZ)));
	}

	hdr = (UndoSegmentHeaderData *)blockbuf.data;
	slot = &hdr->tt_slots[slot_offset];

	/* Decide via the shared pure predicate (cluster_unit-tested; spec-3.11 §2.3). */
	switch (cluster_tt_durable_redo_decide(slot->status, slot->xid, slot->wrap, xid, wrap)) {
	case CLUSTER_TT_REDO_BADSTATUS:
		close(fd);
		ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("undo segment \"%s\" TT slot %u has invalid status %u during redo",
							   path, slot_offset, slot->status)));
		break;
	case CLUSTER_TT_REDO_SKIP:
		/* stale record; a newer commit is already durable -> no write. */
		cluster_vis_bump_recovery_undo_redo_skips(); /* spec-3.16 D5 */
		break;
	case CLUSTER_TT_REDO_APPLY: {
		ssize_t written;

		/* overwrite (recycle-then-commit) or idempotent same-owner. */
		slot->xid = xid;
		slot->wrap = wrap;
		slot->status = TT_SLOT_COMMITTED;
		slot->commit_scn = commit_scn;
		slot->first_undo_block = InvalidUbaVal; /* spec-4.8 D7-A (P1#1): no stale head */

		written = pg_pwrite(fd, blockbuf.data, BLCKSZ, 0);
		if (written != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not write undo segment \"%s\" TT slot commit: "
								   "wrote %zd of %d bytes",
								   path, written, BLCKSZ)));
		}
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync undo segment \"%s\" after TT slot commit: %m", path)));
		}
		cluster_tt_durable_count_redo_apply();		   /* spec-3.11 D8 observability */
		cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
		break;
	}
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));
}

/*
 * Replay XLOG_UNDO_TT_SLOT_COMMIT (spec-3.11 D3) -- the standalone 0x30 record
 * (now emitted only by the 2PC COMMIT PREPARED path; spec-3.18 D4.1).  Thin
 * wrapper: validate the record, then delegate to the shared stamp primitive.
 */
static void
cluster_undo_redo_tt_slot_commit(XLogReaderState *record)
{
	const xl_undo_tt_slot_commit *rec;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_TT_SLOT_COMMIT record length: %u",
							   XLogRecGetDataLen(record))));
	rec = (const xl_undo_tt_slot_commit *)XLogRecGetData(record);

	cluster_tt_durable_redo_stamp_slot(rec->instance, rec->segment_id, rec->slot_offset, rec->wrap,
									   rec->xid, rec->commit_scn);
}


/*
 * Replay XLOG_UNDO_TT_SLOT_ABORT (spec-3.15 D5).
 *
 *   Same block-0 RMW + last-writer-wins decision as the 0x30 redo
 *   (cluster_tt_durable_redo_decide); APPLY writes TT_SLOT_ABORTED
 *   with xid/wrap preserved and commit_scn cleared (V-2).
 */
static void
cluster_undo_redo_tt_slot_abort(XLogReaderState *record)
{
	xl_undo_tt_slot_abort *rec;
	char path[MAXPGPATH];
	int fd;
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;
	TTSlot *slot;
	ssize_t nread;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_TT_SLOT_ABORT record length: %u",
							   XLogRecGetDataLen(record))));
	rec = (xl_undo_tt_slot_abort *)XLogRecGetData(record);

	if (rec->slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(PANIC, (errmsg("XLOG_UNDO_TT_SLOT_ABORT slot_offset %u out of range (max %d)",
							   rec->slot_offset, TT_SLOTS_PER_SEGMENT - 1)));

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open undo segment file \"%s\" for TT slot abort redo: %m", path),
				 errhint("XLOG_UNDO_SEGMENT_INIT must precede XLOG_UNDO_TT_SLOT_ABORT.")));

	nread = pg_pread(fd, blockbuf.data, BLCKSZ, 0);
	if (nread != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read undo segment header \"%s\": read %zd of %d bytes",
							   path, nread, BLCKSZ)));
	}

	hdr = (UndoSegmentHeaderData *)blockbuf.data;
	slot = &hdr->tt_slots[rec->slot_offset];

	switch (
		cluster_tt_durable_redo_decide(slot->status, slot->xid, slot->wrap, rec->xid, rec->wrap)) {
	case CLUSTER_TT_REDO_BADSTATUS:
		close(fd);
		ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("undo segment \"%s\" TT slot %u has invalid status %u during redo",
							   path, rec->slot_offset, slot->status)));
		break;
	case CLUSTER_TT_REDO_SKIP:
		/* A newer owner is already durable. */
		cluster_vis_bump_recovery_undo_redo_skips(); /* spec-3.16 D5 */
		break;
	case CLUSTER_TT_REDO_APPLY: {
		ssize_t written;

		slot->xid = rec->xid;
		slot->wrap = rec->wrap;
		slot->status = TT_SLOT_ABORTED;
		slot->commit_scn = InvalidScn;
		slot->first_undo_block
			= InvalidUbaVal; /* spec-4.8 D7-A (P1#1): cleared; 0x90 re-attaches */

		written = pg_pwrite(fd, blockbuf.data, BLCKSZ, 0);
		if (written != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not write undo segment \"%s\" TT slot abort: "
								   "wrote %zd of %d bytes",
								   path, written, BLCKSZ)));
		}
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync undo segment \"%s\" after TT slot abort: %m", path)));
		}
		cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
		break;
	}
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));
}


/*
 * Replay XLOG_UNDO_TT_SLOT_SET_HEAD (spec-4.8 D7-A).
 *
 *   Block-0 RMW stamping TTSlot.first_undo_block (the undo-chain head) WITHOUT
 *   changing slot.status -- the paired 0x60 abort (emitted just before this
 *   record) already set ABORTED + xid + wrap, so here the on-disk slot owns
 *   exactly (rec.xid, rec.wrap).  Gate on that identity: if the slot was
 *   recycled to a different owner since (xid or wrap differ), SKIP -- never
 *   stamp a head onto another transaction's slot (规则 8.A).  Idempotent: a
 *   re-applied record writes the same head.
 */
static void
cluster_undo_redo_tt_slot_set_head(XLogReaderState *record)
{
	xl_undo_tt_slot_set_head *rec;
	char path[MAXPGPATH];
	int fd;
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;
	TTSlot *slot;
	ssize_t nread;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_TT_SLOT_SET_HEAD record length: %u",
							   XLogRecGetDataLen(record))));
	rec = (xl_undo_tt_slot_set_head *)XLogRecGetData(record);

	if (rec->slot_offset >= TT_SLOTS_PER_SEGMENT)
		ereport(PANIC, (errmsg("XLOG_UNDO_TT_SLOT_SET_HEAD slot_offset %u out of range (max %d)",
							   rec->slot_offset, TT_SLOTS_PER_SEGMENT - 1)));

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(
			PANIC,
			(errcode_for_file_access(),
			 errmsg("could not open undo segment file \"%s\" for TT slot set-head redo: %m", path),
			 errhint("XLOG_UNDO_SEGMENT_INIT must precede XLOG_UNDO_TT_SLOT_SET_HEAD.")));

	nread = pg_pread(fd, blockbuf.data, BLCKSZ, 0);
	if (nread != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read undo segment header \"%s\": read %zd of %d bytes",
							   path, nread, BLCKSZ)));
	}

	hdr = (UndoSegmentHeaderData *)blockbuf.data;
	slot = &hdr->tt_slots[rec->slot_offset];

	/*
	 * Identity gate: only stamp the head if the slot still owns this xact's
	 * (xid, wrap).  A mismatch means the slot was recycled to a later owner
	 * after the abort -- skip (do not clobber a newer transaction's head).
	 */
	if (slot->xid == rec->xid && slot->wrap == rec->wrap) {
		ssize_t written;

		slot->first_undo_block = rec->first_undo_block;

		written = pg_pwrite(fd, blockbuf.data, BLCKSZ, 0);
		if (written != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not write undo segment \"%s\" TT slot set-head: "
								   "wrote %zd of %d bytes",
								   path, written, BLCKSZ)));
		}
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not fsync undo segment \"%s\" after TT slot set-head: %m",
								   path)));
		}
		cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
	} else {
		cluster_vis_bump_recovery_undo_redo_skips(); /* recycled owner -> skip */
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));
}


/*
 * Replay XLOG_UNDO_SEGMENT_RECYCLE (spec-3.13 D3).
 *
 *   Generation-ordered block-0 state RMW, mirroring the 0x30 redo I/O
 *   shape.  Decision via the header-inline pure table
 *   (cluster_undo_segment_recycle_redo_decide):
 *     disk gen >  rec gen -> stale skip (a later reuse is durable);
 *     disk gen == rec gen -> apply for any legal not-newer lifecycle state
 *                            (direct-file writes may expose stale header
 *                            state at crash recovery);
 *     disk gen <  rec gen -> PANIC (the preceding REUSE redo must have
 *                            aligned the generation; v0.3 (2));
 *     same gen, illegal state -> PANIC.
 */
static void
cluster_undo_redo_segment_recycle(XLogReaderState *record)
{
	xl_undo_segment_recycle *rec;
	char path[MAXPGPATH];
	int fd;
	PGAlignedBlock blockbuf;
	UndoSegmentHeaderData *hdr;
	ssize_t nread;

	if (XLogRecGetDataLen(record) != sizeof(*rec))
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_SEGMENT_RECYCLE record length: %u",
							   XLogRecGetDataLen(record))));
	rec = (xl_undo_segment_recycle *)XLogRecGetData(record);

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open undo segment file \"%s\" for recycle redo: %m", path),
				 errhint("XLOG_UNDO_SEGMENT_INIT must precede XLOG_UNDO_SEGMENT_RECYCLE.")));

	nread = pg_pread(fd, blockbuf.data, BLCKSZ, 0);
	if (nread != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not read undo segment header \"%s\": read %zd of %d bytes",
							   path, nread, BLCKSZ)));
	}

	hdr = (UndoSegmentHeaderData *)blockbuf.data;

	switch (cluster_undo_segment_recycle_redo_decide(hdr->wrap_count, hdr->segment_state, rec)) {
	case CLUSTER_SEGRECYCLE_REDO_SKIP_STALE:
		cluster_vis_bump_recovery_undo_redo_skips(); /* spec-3.16 D5 */
		break; /* a later whole-segment reuse is already durable */
	case CLUSTER_SEGRECYCLE_REDO_BAD_GENERATION:
		close(fd);
		ereport(PANIC,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("undo segment \"%s\" generation %u behind recycle record %u during redo",
						path, hdr->wrap_count, rec->expected_generation),
				 errdetail("The preceding XLOG_UNDO_SEGMENT_REUSE replay must have aligned the "
						   "on-disk generation; a lower value indicates lost writes.")));
		break;
	case CLUSTER_SEGRECYCLE_REDO_BAD_STATE:
		close(fd);
		ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("undo segment \"%s\" state %u incompatible with recycle redo "
							   "(%u -> %u) at generation %u",
							   path, hdr->segment_state, rec->old_state, rec->new_state,
							   rec->expected_generation)));
		break;
	case CLUSTER_SEGRECYCLE_REDO_APPLY: {
		ssize_t written;

		hdr->segment_state = rec->new_state;

		written = pg_pwrite(fd, blockbuf.data, BLCKSZ, 0);
		if (written != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write undo segment \"%s\" recycle state: wrote %zd of %d "
							"bytes",
							path, written, BLCKSZ)));
		}
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync undo segment \"%s\" after recycle redo: %m", path)));
		}
		cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
		break;
	}
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));
}


/*
 * Replay XLOG_UNDO_SEGMENT_REUSE (spec-3.13 D4).
 *
 *   Idempotent whole-block-0 rebirth.  Mirrors the SEGMENT_INIT redo
 *   file-lifecycle ownership (mkdir parent / O_CREAT / ftruncate) so a
 *   standby or post-checkpoint crash replay works even when the file
 *   vanished.  Generation decision via the header-inline pure table.
 */
static void
cluster_undo_redo_segment_reuse(XLogReaderState *record)
{
	xl_undo_segment_reuse *rec;
	const char *image;
	char path[MAXPGPATH];
	int fd;
	PGAlignedBlock blockbuf;
	bool header_valid = false;
	uint32 disk_generation = 0;
	ssize_t nread;

	if (XLogRecGetDataLen(record) != sizeof(*rec) + BLCKSZ)
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_SEGMENT_REUSE record length: %u",
							   XLogRecGetDataLen(record))));
	rec = (xl_undo_segment_reuse *)XLogRecGetData(record);
	image = XLogRecGetData(record) + sizeof(*rec);

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	ensure_undo_instance_subdir(rec->instance);

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | PG_BINARY);
	if (fd < 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open undo segment file \"%s\" for reuse redo: %m", path)));

	if (ftruncate(fd, (off_t)UNDO_SEGMENT_SIZE_BYTES) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not extend undo segment file \"%s\": %m", path)));
	}

	nread = pg_pread(fd, blockbuf.data, BLCKSZ, 0);
	if (nread == BLCKSZ
		&& cluster_undo_segment_header_identity_ok(blockbuf.data, rec->segment_id, rec->instance)) {
		header_valid = true;
		disk_generation = ((UndoSegmentHeaderData *)blockbuf.data)->wrap_count;
	}

	switch (cluster_undo_segment_reuse_redo_decide(header_valid, disk_generation, rec)) {
	case CLUSTER_SEGREUSE_REDO_SKIP_STALE:
		cluster_vis_bump_recovery_undo_redo_skips(); /* spec-3.16 D5 */
		break;
	case CLUSTER_SEGREUSE_REDO_BAD_GENERATION:
		close(fd);
		ereport(PANIC, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("undo segment \"%s\" generation %u behind reuse record (%u -> %u) "
							   "during redo",
							   path, disk_generation, rec->old_generation, rec->new_generation)));
		break;
	case CLUSTER_SEGREUSE_REDO_APPLY: {
		ssize_t written;

		written = pg_pwrite(fd, image, BLCKSZ, 0);
		if (written != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not write undo segment \"%s\" reuse image: wrote %zd of %d "
							"bytes",
							path, written, BLCKSZ)));
		}
		if (pg_fsync(fd) != 0) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync undo segment \"%s\" after reuse redo: %m", path)));
		}
		cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
		break;
	}
	}

	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));
}


/*
 * cluster_undo_redo_check_delta_base -- spec-4.8ab D4 redo-determinism guard.
 *
 *	A delta XLOG_UNDO_BLOCK_WRITE patches an EXISTING block image that the redo
 *	caller just pre-read from the segment file.  Its correctness rests on the
 *	own-thread LSN chain being intact:  a block's first WAL touch is always a
 *	full image (a fresh-block FPI, or -- with full_page_writes on -- the first
 *	post-checkpoint FPI), and the checkpoint-coverage boundary (spec-4.8ab D1)
 *	guarantees a pre-redo dirty block was flushed.  So when a delta replays, its
 *	base block ALWAYS carries a valid block_lsn (the redo stream is LSN-monotone
 *	per block, base older-or-equal vs this record).  A base whose block_lsn is
 *	INVALID (zero) means the required full-image base never reached disk -- the
 *	FPI-before-delta / INIT-before-WRITE ordering was broken and patching it
 *	would corrupt the block.  Fail-closed PANIC (8.A;  this is recovery, so a
 *	hard stop, and a real runtime guard -- NOT Assert -- L214).
 *
 *	Determinism / idempotent re-apply (NOT gated here, documented):  block_lsn
 *	is stamped = this record's own EndRecPtr and never travels in the WAL body,
 *	so replaying the same stream yields a byte-identical block.  Re-applying a
 *	delta onto a base already at a NEWER LSN (a partial-recovery restart re-
 *	touched the block on disk) CONVERGES to the same final state -- every delta
 *	rewrites fixed [hdr-prefix + record + slot] ranges in LSN order, so the last
 *	writer per range wins.  old_block_lsn >= record_lsn is therefore a benign
 *	idempotent re-apply, NOT a violation, and is deliberately NOT rejected.
 */
static inline void
cluster_undo_redo_check_delta_base(const char *block_image, uint32 segment_id, uint32 block_no)
{
	XLogRecPtr base_lsn = ((const UndoBlockHeader *)block_image)->block_lsn;

	if (XLogRecPtrIsInvalid(base_lsn))
		ereport(
			PANIC,
			(errmsg("XLOG_UNDO_BLOCK_WRITE delta redo: base block %u of segment %u carries no "
					"durable image (block_lsn invalid)",
					block_no, segment_id),
			 errhint("A delta requires a prior full-image base;  "
					 "XLOG_UNDO_SEGMENT_INIT/REUSE and the post-checkpoint FPI must precede the "
					 "delta in WAL.")));
}


/*
 * Replay XLOG_UNDO_BLOCK_WRITE (spec-3.18 D2a).
 *
 *   D2a ships always-FPI: the record carries a full BLCKSZ image.  Redo
 *   restores it wholesale into a data block (block_no >= 1) of the undo
 *   segment file and stamps block_lsn with this record's own end LSN (§2.6).
 *   Idempotent: re-replaying writes byte-identical bytes (a full overwrite is
 *   the determinism anchor;  spec-4.8ab D4).  The 3-range delta form (has_fpi=0)
 *   lands in D2b -- fail closed here until then.
 *
 *   pg_undo/ files are outside PG's RelFileLocator namespace, so we open +
 *   pwrite + fsync directly.  SEGMENT_INIT/REUSE (which create the file)
 *   always precede a block write in WAL LSN order, so the file must already
 *   exist (O_RDWR, no O_CREAT).
 */
static void
cluster_undo_redo_block_write(XLogReaderState *record)
{
	xl_undo_block_write *rec;
	const char *body;
	uint32 total_len;
	uint32 body_len;
	char path[MAXPGPATH];
	PGAlignedBlock blockbuf;
	int fd;
	ssize_t written;

	total_len = XLogRecGetDataLen(record);
	if (total_len < sizeof(xl_undo_block_write))
		ereport(PANIC, (errmsg("invalid XLOG_UNDO_BLOCK_WRITE record length: %u", total_len)));
	rec = (xl_undo_block_write *)XLogRecGetData(record);
	body = (const char *)XLogRecGetData(record) + sizeof(*rec);
	body_len = total_len - (uint32)sizeof(*rec);

	if (rec->block_no == 0)
		ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE redo: block_no 0 is the segment header")));

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	/* spec-4.5a: an FPI block write is self-contained, so materialize a missing
	 * segment (peer's INIT predates the merge window).  A delta needs the prior
	 * block, so it must NOT create -- a real ordering violation still PANICs. */
	fd = cluster_undo_redo_open_segment(rec->instance, rec->segment_id, path, rec->has_fpi == 1);
	if (fd < 0)
		ereport(
			PANIC,
			(errcode_for_file_access(),
			 errmsg("could not open undo segment file \"%s\" for block write redo: %m", path),
			 errhint("XLOG_UNDO_SEGMENT_INIT/REUSE must precede XLOG_UNDO_BLOCK_WRITE in WAL.")));

	if (rec->has_fpi == 1) {
		/* Full-page image: restore wholesale (no existing-block read needed). */
		if (body_len != BLCKSZ) {
			close(fd);
			ereport(PANIC,
					(errmsg("XLOG_UNDO_BLOCK_WRITE FPI body %u != BLCKSZ %d", body_len, BLCKSZ)));
		}
		cluster_undo_apply_block_write_fpi(body, record->EndRecPtr, blockbuf.data);
	} else {
		/*
		 * 3-range delta (D2b): patch the existing block, which an earlier
		 * post-checkpoint FPI already restored (recovery replays in LSN order,
		 * first touch per block is its FPI).  Bounds are validated fail-closed
		 * (8.A) before touching the block.
		 */
		ssize_t nread;
		uint32 expect_body
			= UNDO_BLOCK_HDR_PREFIX_LEN + rec->rec_len + (uint32)sizeof(UndoSlotDirEntry);

		if (rec->rec_off < sizeof(UndoBlockHeader) || rec->rec_len == 0
			|| (uint32)rec->rec_off + rec->rec_len > BLCKSZ
			|| rec->slot_off < sizeof(UndoBlockHeader)
			|| (uint32)rec->slot_off + sizeof(UndoSlotDirEntry) > BLCKSZ) {
			close(fd);
			ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE delta out of bounds: "
								   "rec_off=%u rec_len=%u slot_off=%u",
								   rec->rec_off, rec->rec_len, rec->slot_off)));
		}
		if (body_len != expect_body) {
			close(fd);
			ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE delta body %u != expected %u", body_len,
								   expect_body)));
		}
		nread = pg_pread(fd, blockbuf.data, BLCKSZ, (off_t)rec->block_no * BLCKSZ);
		if (nread != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not read undo block %u of \"%s\" for delta redo: "
								   "read %zd of %d bytes",
								   rec->block_no, path, nread, BLCKSZ)));
		}
		cluster_undo_redo_check_delta_base(blockbuf.data, rec->segment_id, rec->block_no);
		cluster_undo_apply_block_write_delta(blockbuf.data, rec, body, record->EndRecPtr);
	}

	written = pg_pwrite(fd, blockbuf.data, BLCKSZ, (off_t)rec->block_no * BLCKSZ);
	if (written != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write undo block %u of \"%s\": wrote %zd of %d bytes",
							   rec->block_no, path, written, BLCKSZ)));
	}
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync undo segment \"%s\" after block write: %m", path)));
	}
	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));

	cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
}


/*
 * cluster_undo_redo_block_write_multi -- spec-3.25 D1b redo for the
 *	  multi-record block write.  Mirrors cluster_undo_redo_block_write with the
 *	  slot-dir SPAN instead of a single entry; every bound is validated
 *	  fail-closed (8.A) before the block is touched.
 */
static void
cluster_undo_redo_block_write_multi(XLogReaderState *record)
{
	xl_undo_block_write_multi *rec;
	const char *body;
	uint32 total_len;
	uint32 body_len;
	char path[MAXPGPATH];
	PGAlignedBlock blockbuf;
	int fd;
	ssize_t written;

	total_len = XLogRecGetDataLen(record);
	if (total_len < sizeof(xl_undo_block_write_multi))
		ereport(PANIC,
				(errmsg("invalid XLOG_UNDO_BLOCK_WRITE_MULTI record length: %u", total_len)));
	rec = (xl_undo_block_write_multi *)XLogRecGetData(record);
	body = (const char *)XLogRecGetData(record) + sizeof(*rec);
	body_len = total_len - (uint32)sizeof(*rec);

	if (rec->block_no == 0)
		ereport(PANIC,
				(errmsg("XLOG_UNDO_BLOCK_WRITE_MULTI redo: block_no 0 is the segment header")));

	if (build_undo_segment_path(cluster_undo_intent_for_owner(rec->instance), rec->instance,
								rec->segment_id, path, sizeof(path))
		!= 0)
		ereport(PANIC, (errmsg("undo segment path too long: instance=%u seg=%u", rec->instance,
							   rec->segment_id)));

	/* spec-4.5a: see cluster_undo_redo_block_write -- FPI materializes a missing
	 * peer segment; a delta still requires INIT-before-WRITE. */
	fd = cluster_undo_redo_open_segment(rec->instance, rec->segment_id, path, rec->has_fpi == 1);
	if (fd < 0)
		ereport(
			PANIC,
			(errcode_for_file_access(),
			 errmsg("could not open undo segment file \"%s\" for block write redo: %m", path),
			 errhint("XLOG_UNDO_SEGMENT_INIT/REUSE must precede XLOG_UNDO_BLOCK_WRITE in WAL.")));

	if (rec->has_fpi == 1) {
		if (body_len != BLCKSZ) {
			close(fd);
			ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE_MULTI FPI body %u != BLCKSZ %d", body_len,
								   BLCKSZ)));
		}
		cluster_undo_apply_block_write_fpi(body, record->EndRecPtr, blockbuf.data);
	} else {
		ssize_t nread;
		uint32 expect_body = UNDO_BLOCK_HDR_PREFIX_LEN + rec->rec_len + rec->slot_len;

		if (rec->rec_off < sizeof(UndoBlockHeader) || rec->rec_len == 0
			|| (uint32)rec->rec_off + rec->rec_len > BLCKSZ
			|| rec->slot_off < sizeof(UndoBlockHeader) || rec->slot_len == 0
			|| rec->slot_len % sizeof(UndoSlotDirEntry) != 0
			|| (uint32)rec->slot_off + rec->slot_len > BLCKSZ) {
			close(fd);
			ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE_MULTI delta out of bounds: "
								   "rec_off=%u rec_len=%u slot_off=%u slot_len=%u",
								   rec->rec_off, rec->rec_len, rec->slot_off, rec->slot_len)));
		}
		if (body_len != expect_body) {
			close(fd);
			ereport(PANIC, (errmsg("XLOG_UNDO_BLOCK_WRITE_MULTI delta body %u != expected %u",
								   body_len, expect_body)));
		}
		nread = pg_pread(fd, blockbuf.data, BLCKSZ, (off_t)rec->block_no * BLCKSZ);
		if (nread != BLCKSZ) {
			int save_errno = errno;

			close(fd);
			errno = save_errno;
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not read undo block %u of \"%s\" for delta redo: "
								   "read %zd of %d bytes",
								   rec->block_no, path, nread, BLCKSZ)));
		}
		cluster_undo_redo_check_delta_base(blockbuf.data, rec->segment_id, rec->block_no);
		cluster_undo_apply_block_write_multi_delta(blockbuf.data, rec, body, record->EndRecPtr);
	}

	written = pg_pwrite(fd, blockbuf.data, BLCKSZ, (off_t)rec->block_no * BLCKSZ);
	if (written != BLCKSZ) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not write undo block %u of \"%s\": wrote %zd of %d bytes",
							   rec->block_no, path, written, BLCKSZ)));
	}
	if (pg_fsync(fd) != 0) {
		int save_errno = errno;

		close(fd);
		errno = save_errno;
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not fsync undo segment \"%s\" after block write: %m", path)));
	}
	if (close(fd) != 0)
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not close undo segment file \"%s\": %m", path)));

	cluster_vis_bump_recovery_undo_redo_applies(); /* spec-3.16 D5 */
}


/*
 * cluster_undo_redo -- RM_CLUSTER_UNDO redo handler entry point.
 *
 *   Dispatches by xl_info & XLR_INFO_MASK after stripping the framework
 *   bits (XLR_SPECIAL_REL_UPDATE etc.; we don't use those in stage 1.22).
 */
void
cluster_undo_redo(XLogReaderState *record)
{
	/*
	 * spec-3.16 D3 (ITL ref recovery reachability): this handler restores the
	 * durable TT slot (0x30/0x60) and segment lifecycle (0x10/0x40/0x50) into
	 * the pg_undo files.  The heap page ITL ref that points at a TT slot is
	 * restored by the HEAP rmgr (FPI / heap redo) at its own LSN.  Both records
	 * replay in StartupXLOG LSN order, and a reader (heapam_visibility) only
	 * runs AFTER recovery reaches consistency + finishes -- so by the time any
	 * ITL ref is dereferenced, both the page ref and its durable TT slot are
	 * already replayed.  No cross-rmgr ordering dependency (t/225 L4 e2e).
	 */

	uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;

	/* spec-3.16 D7 (C-R4): redo runs only during recovery.  Debug-only
	 * invariant -- NOT a production guard (L218): the rmgr framework
	 * already restricts redo to StartupXLOG. */
	Assert(InRecovery || RecoveryInProgress());

	switch (info) {
	case XLOG_UNDO_SEGMENT_INIT:
		cluster_undo_redo_segment_init(record);
		break;
	case XLOG_UNDO_TT_SLOT_COMMIT:
		cluster_undo_redo_tt_slot_commit(record);
		break;
	case XLOG_UNDO_TT_SLOT_ABORT:
		cluster_undo_redo_tt_slot_abort(record);
		break;
	case XLOG_UNDO_SEGMENT_RECYCLE:
		cluster_undo_redo_segment_recycle(record);
		break;
	case XLOG_UNDO_SEGMENT_REUSE:
		cluster_undo_redo_segment_reuse(record);
		break;
	case XLOG_UNDO_BLOCK_WRITE:
		cluster_undo_redo_block_write(record);
		break;
	case XLOG_UNDO_BLOCK_WRITE_MULTI:
		cluster_undo_redo_block_write_multi(record);
		break;
	case XLOG_UNDO_TT_SLOT_SET_HEAD:
		cluster_undo_redo_tt_slot_set_head(record);
		break;
	case XLOG_HW_RESERVE:
		cluster_hw_redo_reserve(record);
		break;
	default:
		ereport(PANIC, (errmsg("cluster_undo_redo: unknown op code %u", info)));
	}
}


/*
 * NOTE: cluster_undo_desc + cluster_undo_identify live in
 * src/backend/access/rmgrdesc/clusterundodesc.c so they are picked up
 * by both the backend xlog.o linker and the frontend pg_waldump build
 * (which globs src/backend/access/rmgrdesc/ "*desc.c" into its OBJS).
 */
