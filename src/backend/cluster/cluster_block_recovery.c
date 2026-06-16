/*-------------------------------------------------------------------------
 *
 * cluster_block_recovery.c
 *	  pgrac online single-block recovery orchestrator (spec-4.10 D2).
 *
 *	  cluster_block_recovery_reconstruct() rebuilds one corrupt / lost-write
 *	  block from this node's own WAL stream within an explicit window, onto a
 *	  detached page, driving the verified single-block apply core
 *	  (cluster_block_apply_one).  It fails closed on any uncertainty (8.A): the
 *	  caller never installs a possibly-wrong block.
 *
 *	  Scan contract (D2):
 *	    - read the local WAL stream over [scan_lower, scan_upper];
 *	    - for the target block, skip records until the first apply-able
 *	      full-page image establishes the base (a delta before any base is
 *	      irrelevant -- it is overwritten by a later FPI);
 *	    - then apply each touching record in order (a later FPI resets the
 *	      base via RestoreBlockImage, so the latest FPI <= the last touch
 *	      wins; deltas accumulate);
 *	    - the installed version is the block's LAST touching record with
 *	      EndRecPtr <= scan_upper (WAL-derived target, never the corrupt
 *	      page's pd_lsn);
 *	    - validate header-sanity + pd_lsn == target before returning the page.
 *
 *	  Bounds derivation (where [scan_lower, scan_upper] come from) and the
 *	  cross-thread own-thread gate are deliberately the caller's / later
 *	  deliverables' concern; see the header.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_block_recovery.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.10-online-block-recovery.md (FROZEN v0.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "port/atomics.h"
#include "portability/instr_time.h" /* PGRAC: spec-4.13 D4 LOG-only latency */
#include "storage/bufpage.h"
#include "storage/checksum.h"
#include "storage/fd.h"
#include "storage/relfilelocator.h"
#include "storage/shmem.h"
#include "storage/smgr.h"

#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_block_recovery.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_shmem.h"

/*
 * D6 observability counters (shmem).  blocks_recovered / recovery_failclosed
 * are the two live-site outcomes of the synchronous read-path recovery
 * (cluster_block_recovery_on_read).  Finer reasons (fpi-not-found vs
 * replay-incomplete), the in-progress gauge, and the BlockRecovery wait event
 * have no live site in the synchronous single-node model -- they belong to the
 * recovering-set / cross-node model and land with D5.
 */
typedef struct ClusterBlockRecoveryShmem {
	pg_atomic_uint64 blocks_recovered;	  /* rebuilt + installed */
	pg_atomic_uint64 recovery_failclosed; /* attempted, could not rebuild */
} ClusterBlockRecoveryShmem;

static ClusterBlockRecoveryShmem *cluster_block_recovery_shmem = NULL;

static Size
cluster_block_recovery_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterBlockRecoveryShmem));
}

static void
cluster_block_recovery_shmem_init(void)
{
	bool found;

	cluster_block_recovery_shmem = (ClusterBlockRecoveryShmem *)ShmemInitStruct(
		"pgrac block recovery", cluster_block_recovery_shmem_size(), &found);
	if (!found) {
		pg_atomic_init_u64(&cluster_block_recovery_shmem->blocks_recovered, 0);
		pg_atomic_init_u64(&cluster_block_recovery_shmem->recovery_failclosed, 0);
	}
}

static const ClusterShmemRegion cluster_block_recovery_region = {
	.name = "pgrac block recovery",
	.size_fn = cluster_block_recovery_shmem_size,
	.init_fn = cluster_block_recovery_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-4.10 block recovery",
	.reserved_flags = 0,
};

void
cluster_block_recovery_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_block_recovery_region);
}

uint64
cluster_block_recovery_get_blocks_recovered(void)
{
	return cluster_block_recovery_shmem
			   ? pg_atomic_read_u64(&cluster_block_recovery_shmem->blocks_recovered)
			   : 0;
}

uint64
cluster_block_recovery_get_failclosed(void)
{
	return cluster_block_recovery_shmem
			   ? pg_atomic_read_u64(&cluster_block_recovery_shmem->recovery_failclosed)
			   : 0;
}

/*
 * page_header_sane -- header-only validity of a reconstructed page (8.A).
 *
 *	Mirrors PageIsVerifiedExtended's header check WITHOUT the checksum (the
 *	checksum is a write-time stamp applied at install, D4; reconstruct produces
 *	the pre-stamp page).  A reconstructed block must not be "new" (all-zero):
 *	rebuilding a real block always yields a populated header.
 */
static bool
page_header_sane(Page page)
{
	PageHeader p = (PageHeader)page;

	if (PageIsNew(page))
		return false;
	if (p->pd_lower < SizeOfPageHeaderData || p->pd_lower > p->pd_upper
		|| p->pd_upper > p->pd_special || p->pd_special > BLCKSZ)
		return false;
	return true;
}

/*
 * derive_window -- compute the scan window [*lo, *hi] for an online rebuild
 *		(D1 decision: oldest available WAL .. flush LSN).
 *
 *	*hi = GetFlushRecPtr() (the global durable bound).  *lo = the start LSN of
 *	the OLDEST WAL segment still present in pg_wal.  Scanning from the oldest
 *	segment locates the latest FPI <= the block's last touch regardless of
 *	checkpoint boundaries (GetRedoRecPtr / control-file checkpoint redo can
 *	advance past the target after a post-restart checkpoint).  If the FPI was
 *	recycled/archived it simply will not be found -> reconstruct fails closed.
 *
 *	NOTE: single-timeline scope (the local stream reader follows the current
 *	timeline); the caller gates on single-node (no failover) -- see
 *	cluster_block_recovery_on_read.
 */
static bool
derive_window(XLogRecPtr *lo, XLogRecPtr *hi)
{
	DIR *dir;
	const struct dirent *de;
	XLogSegNo min_seg = 0;
	bool found = false;
	TimeLineID tli;

	*hi = GetFlushRecPtr(&tli);

	dir = AllocateDir(XLOGDIR);
	while ((de = ReadDir(dir, XLOGDIR)) != NULL) {
		TimeLineID seg_tli;
		XLogSegNo seg;

		if (!IsXLogFileName(de->d_name))
			continue;
		XLogFromFileName(de->d_name, &seg_tli, &seg, wal_segment_size);
		if (!found || seg < min_seg) {
			min_seg = seg;
			found = true;
		}
	}
	FreeDir(dir);

	if (!found)
		return false;

	XLogSegNoOffsetToRecPtr(min_seg, 0, wal_segment_size, *lo);
	return *lo <= *hi;
}

ClusterBlkRecResult
cluster_block_recovery_reconstruct(RelFileLocator rlocator, ForkNumber forknum,
								   BlockNumber blocknum, XLogRecPtr scan_lower,
								   XLogRecPtr scan_upper, char *out_page)
{
	XLogReaderState *xlogreader;
	ReadLocalXLogPageNoWaitPrivate *private_data;
	XLogRecPtr first_valid;
	PGAlignedBlock pagebuf;
	char *page = pagebuf.data;
	bool have_base = false;
	bool aborted = false;
	XLogRecPtr target_lsn = InvalidXLogRecPtr;
	/* PGRAC: spec-4.13 D4 LOG-only scan cost (no semantic effect). */
	int64 records_scanned = 0;
	int64 wal_bytes_scanned = 0;

	if (XLogRecPtrIsInvalid(scan_lower) || XLogRecPtrIsInvalid(scan_upper)
		|| scan_lower > scan_upper)
		return CLUSTER_BLKREC_UNRECOVERABLE;

	memset(page, 0, BLCKSZ);

	/* WAL reader over the local stream (pg_walinspect idiom). */
	private_data
		= (ReadLocalXLogPageNoWaitPrivate *)palloc0(sizeof(ReadLocalXLogPageNoWaitPrivate));
	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page_no_wait,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									private_data);
	if (xlogreader == NULL) {
		pfree(private_data);
		return CLUSTER_BLKREC_UNRECOVERABLE;
	}

	first_valid = XLogFindNextRecord(xlogreader, scan_lower);
	if (XLogRecPtrIsInvalid(first_valid)) {
		XLogReaderFree(xlogreader);
		pfree(private_data);
		return CLUSTER_BLKREC_UNRECOVERABLE;
	}

	for (;;) {
		char *errormsg;
		const XLogRecord *record;
		int max_id;
		int block_id;

		record = XLogReadRecord(xlogreader, &errormsg);
		if (record == NULL) {
			/* clean end of stream is fine; a read error in-window fails closed */
			if (!private_data->end_of_wal)
				aborted = true;
			break;
		}

		/* PGRAC: spec-4.13 D4 scan cost (counted, never alters control flow). */
		records_scanned++;
		wal_bytes_scanned += record->xl_tot_len;

		/*
		 * Stop before applying any record that ENDS past the window: the
		 * installed version must be the block's last touching record with
		 * EndRecPtr <= scan_upper (a record straddling scan_upper, e.g. one
		 * flushed after scan_upper was captured, would otherwise overshoot the
		 * bound and set target_lsn past it).
		 */
		if (xlogreader->EndRecPtr > scan_upper)
			break;

		max_id = XLogRecMaxBlockId(xlogreader);
		for (block_id = 0; block_id <= max_id; block_id++) {
			RelFileLocator rl;
			ForkNumber f;
			BlockNumber b;
			bool is_fpi;
			ClusterBlkApplyResult res;

			if (!XLogRecGetBlockTagExtended(xlogreader, (uint8)block_id, &rl, &f, &b, NULL))
				continue;
			if (!RelFileLocatorEquals(rl, rlocator) || f != forknum || b != blocknum)
				continue;

			is_fpi = XLogRecHasBlockImage(xlogreader, (uint8)block_id)
					 && XLogRecBlockImageApply(xlogreader, (uint8)block_id);

			/*
			 * Skip the block's records until the first apply-able FPI: an
			 * earlier delta has no base to apply onto and is overwritten by
			 * the FPI anyway.  (Unlike the test SRF, which requires the first
			 * in-range record to be the FPI, reconstruct scans a wider window
			 * to locate the latest FPI <= target.)
			 */
			if (!have_base && !is_fpi)
				continue;

			res = cluster_block_apply_one(xlogreader, (uint8)block_id, page);
			if (res == CLUSTER_BLKAPPLY_OK) {
				have_base = true;
				target_lsn = xlogreader->EndRecPtr;
			} else {
				/* UNSUPPORTED / FAILED (NOOP impossible: tag matched) -> stop */
				aborted = true;
				break;
			}
		}

		if (aborted)
			break;
	}

	XLogReaderFree(xlogreader);
	pfree(private_data);

	/* PGRAC: spec-4.13 D4 scan-cost observability (DEBUG1, no semantic effect). */
	ereport(DEBUG1,
			(errmsg_internal("online block recovery scan: rel=%u/%u/%u fork=%d block=%u "
							 "records_scanned=" INT64_FORMAT " wal_bytes_scanned=" INT64_FORMAT,
							 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum, blocknum,
							 records_scanned, wal_bytes_scanned)));

	/* Fail closed on any uncertainty: never return a possibly-wrong page. */
	if (aborted || !have_base)
		return CLUSTER_BLKREC_UNRECOVERABLE;
	if (!page_header_sane(page) || PageGetLSN(page) != target_lsn)
		return CLUSTER_BLKREC_UNRECOVERABLE;

	memcpy(out_page, page, BLCKSZ);
	return CLUSTER_BLKREC_RECOVERED;
}

bool
cluster_block_recovery_checksum_mismatch(char *page, BlockNumber blocknum)
{
	if (!DataChecksumsEnabled())
		return false;
	if (PageIsNew((Page)page))
		return false;
	return pg_checksum_page(page, blocknum) != ((PageHeader)page)->pd_checksum;
}

bool
cluster_block_recovery_on_read(struct SMgrRelationData *reln, ForkNumber forknum,
							   BlockNumber blocknum, char *buffer)
{
	RelFileLocator rlocator = reln->smgr_rlocator.locator;
	XLogRecPtr lo;
	XLogRecPtr hi;
	PGAlignedBlock scratch;
	instr_time rec_start; /* PGRAC: spec-4.13 D4 LOG-only latency */
	instr_time rec_end;

	if (!cluster_online_block_recovery)
		return false;

	/*
	 * Not during startup/crash recovery: that path redoes WAL itself (FPI +
	 * deltas) and a reentrant WAL scan here would be unsafe.  Online recovery
	 * is a normal-operation feature.
	 */
	if (RecoveryInProgress())
		return false;

	/*
	 * Own-thread gate (8.A): only auto-recover when this node has no peers.  A
	 * multi-node block may have a foreign last-writer that the local-stream
	 * reconstruct would rebuild to a STALE own-thread version; that path is
	 * forward Stage 5 (D5, authoritative cross-thread target).  Single-node is
	 * always own-thread, so reconstruct's last own-thread touch IS the latest.
	 */
	if (cluster_conf_has_peers())
		return false;

	if (!derive_window(&lo, &hi))
		return false;

	/* PGRAC: spec-4.13 D4 -- time the reconstruct+install (trigger->return). */
	INSTR_TIME_SET_CURRENT(rec_start);

	if (cluster_block_recovery_reconstruct(rlocator, forknum, blocknum, lo, hi, scratch.data)
		!= CLUSTER_BLKREC_RECOVERED) {
		/* attempted (passed all gates) but could not rebuild -> fail-closed */
		if (cluster_block_recovery_shmem)
			pg_atomic_fetch_add_u64(&cluster_block_recovery_shmem->recovery_failclosed, 1);
		INSTR_TIME_SET_CURRENT(rec_end);
		INSTR_TIME_SUBTRACT(rec_end, rec_start);
		ereport(LOG, (errmsg("online block recovery: rel=%u/%u/%u fork=%d block=%u "
							 "outcome=failclosed latency_us=" INT64_FORMAT,
							 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum, blocknum,
							 INSTR_TIME_GET_MICROSEC(rec_end))));
		return false;
	}

	/* Reconstruct validated header-sanity + pd_lsn == target. */
	memcpy(buffer, scratch.data, BLCKSZ);

	/*
	 * D4 durable install: persist the rebuilt block so future reads do not
	 * re-recover.  The version is own-thread (pd_lsn <= scan_upper = flush LSN),
	 * so the WAL it depends on is already durable -- XLogFlush is a no-op and no
	 * new WAL is produced (own-thread no-new-WAL crash-safety).  PageSetChecksum
	 * stamps the write-time checksum.  A crash/torn write before this completes
	 * leaves the block corrupt for the next read to re-recover (idempotent) --
	 * or to fail closed if the FPI's WAL segment has since been recycled;
	 * post-write redo is idempotent too (pd_lsn >= any replayed record).
	 */
	XLogFlush(PageGetLSN((Page)buffer));
	PageSetChecksumInplace((Page)buffer, blocknum);
	smgrwrite(reln, forknum, blocknum, buffer, false);

	if (cluster_block_recovery_shmem)
		pg_atomic_fetch_add_u64(&cluster_block_recovery_shmem->blocks_recovered, 1);

	/* PGRAC: spec-4.13 D4 -- emitted after the durable install completes. */
	INSTR_TIME_SET_CURRENT(rec_end);
	INSTR_TIME_SUBTRACT(rec_end, rec_start);
	ereport(LOG, (errmsg("online block recovery: rel=%u/%u/%u fork=%d block=%u outcome=recovered "
						 "latency_us=" INT64_FORMAT,
						 rlocator.spcOid, rlocator.dbOid, rlocator.relNumber, forknum, blocknum,
						 INSTR_TIME_GET_MICROSEC(rec_end))));
	return true;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
