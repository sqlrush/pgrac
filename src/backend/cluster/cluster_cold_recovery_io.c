/*-------------------------------------------------------------------------
 *
 * cluster_cold_recovery_io.c
 *	  Read-only I/O for typed cold-crash replay: pass-1 root scan, DATA
 *	  observation and the pass-2 source reader.
 *
 *	  Pass 1 visits every retained record of a RECOVERY_REQUIRED root through
 *	  the sealed recovery visitor and feeds the plan; nothing visited is
 *	  trusted until the visitor and the observed cut both match the ROOT.
 *	  DATA observation reads storage directly, before replay touches shared
 *	  buffers.  Pass 2 re-reads the same generation through the selected
 *	  restart-input segment opener; the caller compares each record with its
 *	  pass-1 identity.  No function here writes, locks pages or grants
 *	  replay authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cold_recovery_io.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-05-instance-and-cluster-recovery.md
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "cluster/cluster_cold_recovery.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_wal_restart_read.h"
#include "cluster/cluster_wal_tail.h"
#include "pgstat.h"
#include "storage/bufpage.h"
#include "storage/smgr.h"

struct ClusterColdReaderV1 {
	ClusterWalSourceRef source;
	XLogReaderState *reader;
	int segment_fd;
	XLogSegNo segment_no;
};

static int
cold_reader_page_read(XLogReaderState *state, XLogRecPtr target_page, int required,
					  XLogRecPtr target_record, char *read_buffer)
{
	ClusterColdReaderV1 *cold = (ClusterColdReaderV1 *)state->private_data;
	XLogSegNo segment_no;
	int read_bytes;

	(void)target_record;
	XLByteToSeg(target_page, segment_no, state->segcxt.ws_segsize);
	if (cold->segment_fd < 0 || segment_no != cold->segment_no) {
		ClusterControlRootResult result;

		if (cold->segment_fd >= 0) {
			close(cold->segment_fd);
			cold->segment_fd = -1;
		}
		result = cluster_wal_restart_segment_open(cluster_wal_threads_dir, &cold->source,
												  cold->source.timeline, segment_no,
												  state->segcxt.ws_segsize, &cold->segment_fd);
		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			return -1; /* end of the selected stream */
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not open selected WAL segment of thread %u for cold replay",
							cold->source.claim.identity.origin_thread_id),
					 errdetail("Segment " UINT64_FORMAT ", root result %d.", (uint64)segment_no,
							   (int)result),
					 errhint("Preserve every original WAL generation; no fallback source is "
							 "permitted.")));
		cold->segment_no = segment_no;
	}
	pgstat_report_wait_start(WAIT_EVENT_WAL_READ);
	read_bytes = pg_pread(cold->segment_fd, read_buffer, XLOG_BLCKSZ,
						  (off_t)XLogSegmentOffset(target_page, state->segcxt.ws_segsize));
	pgstat_report_wait_end();
	if (read_bytes < required)
		return -1;
	return read_bytes;
}

ClusterColdReaderV1 *
cluster_cold_reader_open_v1(const ClusterWalSourceRef *source, uint64 system_identifier,
							XLogRecPtr start)
{
	ClusterColdReaderV1 *cold;

	if (source == NULL || source->timeline == 0 || system_identifier == 0
		|| start == InvalidXLogRecPtr || cluster_wal_threads_dir == NULL
		|| cluster_wal_threads_dir[0] == '\0')
		return NULL;
	cold = (ClusterColdReaderV1 *)palloc0(sizeof(*cold));
	cold->source = *source;
	cold->segment_fd = -1;
	cold->reader = XLogReaderAllocate(wal_segment_size, NULL,
									  XL_ROUTINE(.page_read = cold_reader_page_read), cold);
	if (cold->reader == NULL) {
		pfree(cold);
		return NULL;
	}
	cold->reader->system_identifier = system_identifier;
	cold->reader->seg.ws_tli = source->timeline;
	cold->reader->cluster_expected_thread_id = source->claim.identity.origin_thread_id;
	XLogBeginRead(cold->reader, start);
	return cold;
}

XLogReaderState *
cluster_cold_reader_next_v1(ClusterColdReaderV1 *cold, char **errormsg)
{
	if (errormsg != NULL)
		*errormsg = NULL;
	if (cold == NULL || cold->reader == NULL)
		return NULL;
	return XLogReadRecord(cold->reader, errormsg) != NULL ? cold->reader : NULL;
}

void
cluster_cold_reader_close_v1(ClusterColdReaderV1 **cold_address)
{
	ClusterColdReaderV1 *cold;

	if (cold_address == NULL || *cold_address == NULL)
		return;
	cold = *cold_address;
	if (cold->segment_fd >= 0)
		close(cold->segment_fd);
	if (cold->reader != NULL)
		XLogReaderFree(cold->reader);
	pfree(cold);
	*cold_address = NULL;
}

static bool
cold_observe_incarnation(ClusterColdObserverV1 *observer, RelFileLocator locator,
						 uint8 incarnation[16])
{
	ClusterSpaceIdentity identity;

	if (observer->cached_valid && RelFileLocatorEquals(observer->cached_locator, locator)) {
		memcpy(incarnation, observer->cached_incarnation, 16);
		return true;
	}
	if (!cluster_space_relation_read_redo_identity(locator, &identity)
		|| identity.state != CLUSTER_SPACE_IDENTITY_LIVE)
		return false;
	observer->cached_locator = locator;
	memcpy(observer->cached_incarnation, identity.incarnation, 16);
	observer->cached_valid = true;
	memcpy(incarnation, identity.incarnation, 16);
	return true;
}

static bool
cold_page_all_zero(const char *page)
{
	Size i;

	for (i = 0; i < BLCKSZ; i++)
		if (page[i] != 0)
			return false;
	return true;
}

bool
cluster_cold_observe_data_v1(void *arg, const RfPageIdentityV1 *page, ClusterColdDataV1 *out)
{
	ClusterColdObserverV1 *observer = (ClusterColdObserverV1 *)arg;
	PGAlignedBlock block;
	SMgrRelation relation;
	ForkNumber fork;

	if (observer == NULL || page == NULL || out == NULL || page->forknum > MAX_FORKNUM)
		return false;
	memset(out, 0, sizeof(*out));
	fork = (ForkNumber)page->forknum;
	observer->pages_observed++;
	relation = smgropen(page->locator, InvalidBackendId);
	if (!smgrexists(relation, fork) || page->blockno >= smgrnblocks(relation, fork)) {
		out->kind = CLUSTER_COLD_DATA_ABSENT;
		return true;
	}
	smgrread(relation, fork, page->blockno, block.data);
	if (PageIsNew((Page)block.data)) {
		if (!cold_page_all_zero(block.data)) {
			observer->pages_invalid++;
			out->kind = CLUSTER_COLD_DATA_INVALID;
			return true;
		}
		out->kind = CLUSTER_COLD_DATA_UNFORMATTED;
		return cold_observe_incarnation(observer, page->locator, out->version.segment_incarnation);
	}
	if (!PageIsVerifiedExtended((Page)block.data, page->blockno, 0)) {
		observer->pages_invalid++;
		out->kind = CLUSTER_COLD_DATA_INVALID;
		return true;
	}
	/* A formatted page without a version token cannot be placed on a chain;
	 * refuse rather than treat it as replaceable. */
	if (((PageHeader)block.data)->pd_block_scn == 0)
		return false;
	out->kind = CLUSTER_COLD_DATA_PRESENT;
	out->version.mutation_token = (uint64)((PageHeader)block.data)->pd_block_scn;
	return cold_observe_incarnation(observer, page->locator, out->version.segment_incarnation);
}

typedef struct ColdScanWork {
	ClusterColdPlanV1 *plan;
	uint32 participant;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	bool space_active;
	bool foreign;
	ClusterColdDetailV1 detail;
	ClusterColdScanResultV1 *result;
	ClusterColdDecodedV1 decoded;
} ColdScanWork;

static bool
cold_scan_visit(XLogReaderState *reader, void *arg)
{
	ColdScanWork *work = (ColdScanWork *)arg;

	CHECK_FOR_INTERRUPTS();
	work->detail
		= cluster_cold_recovery_decode_v1(reader, work->system_identifier, work->storage_uuid,
										  work->space_active, work->foreign, &work->decoded);
	if (work->detail == CLUSTER_COLD_OK)
		work->detail
			= cluster_cold_plan_feed_v1(work->plan, work->participant, &work->decoded.record);
	if (work->detail != CLUSTER_COLD_OK) {
		work->result->failed_read_rec_ptr = reader->ReadRecPtr;
		work->result->route_detail = work->decoded.route_detail;
		return false;
	}
	work->result->records++;
	return true;
}

ClusterColdDetailV1
cluster_cold_scan_root_v1(ClusterColdPlanV1 *plan, uint32 participant,
						  const ClusterControlRootSnapshot *root,
						  const ClusterControlRootReadToken *token, bool space_active, bool foreign,
						  ClusterColdScanResultV1 *result)
{
	ColdScanWork *work;
	ClusterWalTailObservation observed;
	ClusterControlRootResult visit;
	ClusterColdDetailV1 detail;

	if (result == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	memset(result, 0, sizeof(*result));
	if (plan == NULL || root == NULL || token == NULL)
		return CLUSTER_COLD_INVALID_ARGUMENT;
	work = (ColdScanWork *)palloc0(sizeof(*work));
	work->plan = plan;
	work->participant = participant;
	work->system_identifier = root->identity.system_identifier;
	memcpy(work->storage_uuid, root->identity.storage_uuid, 16);
	work->space_active = space_active;
	work->foreign = foreign;
	work->detail = CLUSTER_COLD_OK;
	work->result = result;
	visit = cluster_control_root_recovery_visit(root, token, cold_scan_visit, work, &observed);
	result->root_result = (int)visit;
	detail = work->detail;
	if (detail == CLUSTER_COLD_OK
		&& (visit != CLUSTER_CONTROL_ROOT_OK_PRIMARY || observed.records != result->records
			|| observed.complete_end != root->validated_tail_lsn_exclusive))
		detail = CLUSTER_COLD_SOURCE_GAP;
	pfree(work);
	return detail;
}

#endif /* USE_PGRAC_CLUSTER */
