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
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlogutils.h"
#include "storage/bufpage.h"
#include "storage/relfilelocator.h"

#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_block_recovery.h"

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
		XLogRecord *record;
		int max_id;
		int block_id;

		record = XLogReadRecord(xlogreader, &errormsg);
		if (record == NULL) {
			/* clean end of stream is fine; a read error in-window fails closed */
			if (!private_data->end_of_wal)
				aborted = true;
			break;
		}

		/* stop once we pass the requested window */
		if (xlogreader->ReadRecPtr > scan_upper)
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

	/* Fail closed on any uncertainty: never return a possibly-wrong page. */
	if (aborted || !have_base)
		return CLUSTER_BLKREC_UNRECOVERABLE;
	if (!page_header_sane(page) || PageGetLSN(page) != target_lsn)
		return CLUSTER_BLKREC_UNRECOVERABLE;

	memcpy(out_page, page, BLCKSZ);
	return CLUSTER_BLKREC_RECOVERED;
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
