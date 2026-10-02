/* PGRAC: apply exact cold block decisions through native redo buffers.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_page_cold_redo.h"
#include "cluster/cluster_page_wal.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"

typedef struct ColdRedoTarget {
	ClusterColdRedoBlockV1 decision;
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blockno;
	Buffer buffer;
	bool present;
	bool read;
	bool dirtied;
	bool image;
} ColdRedoTarget;

static struct {
	XLogReaderState *record;
	XLogRecPtr read_lsn, end_lsn;
	pg_crc32c crc;
	uint8 rmid, info;
	bool violation;
	int violation_block_id;
	ColdRedoTarget targets[XLR_MAX_BLOCK_ID + 1];
} cold_redo;
bool cluster_page_cold_redo_active_v1;

static void cold_redo_refuse(XLogReaderState *record, int block_id) pg_attribute_noreturn();

static void
cold_redo_refuse(XLogReaderState *record, int block_id)
{
	XLogRecPtr lsn = cluster_page_cold_redo_active_v1 ? cold_redo.read_lsn
					 : record != NULL				  ? record->ReadRecPtr
													  : InvalidXLogRecPtr;
	ereport(FATAL, (errcode(ERRCODE_DATA_CORRUPTED),
					errmsg("typed cold redo block proof does not match its native consumer"),
					errdetail("WAL record %X/%X, block ID %d.", LSN_FORMAT_ARGS(lsn), block_id)));
}

void
cluster_page_cold_redo_abort_v1(void)
{
	cluster_page_cold_redo_active_v1 = false;
	memset(&cold_redo, 0, sizeof(cold_redo));
}

static bool
cold_redo_record_matches(XLogReaderState *record)
{
	return cluster_page_cold_redo_active_v1 && record != NULL && record == cold_redo.record
		   && record->record != NULL && record->ReadRecPtr == cold_redo.read_lsn
		   && record->EndRecPtr == cold_redo.end_lsn
		   && record->record->header.xl_crc == cold_redo.crc
		   && record->record->header.xl_rmid == cold_redo.rmid
		   && record->record->header.xl_info == cold_redo.info;
}

void
cluster_page_cold_redo_begin_v1(XLogReaderState *record)
{
	if (cluster_page_cold_redo_active_v1 || !InRecovery || !cluster_shared_config
		|| MyBackendType != B_STARTUP || record == NULL || record->record == NULL
		|| record->record->max_block_id < 0 || record->record->max_block_id > XLR_MAX_BLOCK_ID
		|| !cluster_page_wal_cold_redo_write_allowed_v1())
		cold_redo_refuse(record, -1);
	memset(&cold_redo, 0, sizeof(cold_redo));
	for (int i = 0; i <= record->record->max_block_id; i++) {
		ColdRedoTarget *target = &cold_redo.targets[i];
		ClusterSpaceIdentity space;
		if (!XLogRecHasBlockRef(record, i))
			continue;
		if (!cluster_cold_redo_block_decision_v1(record, i, &target->decision)
			|| !XLogRecGetBlockTagExtended(record, i, &target->locator, &target->forknum,
										   &target->blockno, NULL))
			cold_redo_refuse(record, i);
		target->present = true;
		if (target->decision.action == CLUSTER_COLD_REDO_SKIP)
			continue;
		if (target->decision.action == CLUSTER_COLD_REDO_NATIVE) {
			if (target->forknum != FSM_FORKNUM)
				cold_redo_refuse(record, i);
			continue;
		}
		if (target->decision.action != CLUSTER_COLD_REDO_APPLY
			|| (target->forknum != MAIN_FORKNUM && target->forknum != VISIBILITYMAP_FORKNUM)
			|| target->decision.result.mutation_token == 0
			|| target->decision.expected_kind > CLUSTER_COLD_DATA_ABSENT
			|| !cluster_space_relation_read_redo_identity(target->locator, &space)
			|| memcmp(space.incarnation, target->decision.result.segment_incarnation, 16) != 0)
			cold_redo_refuse(record, i);
		if (target->decision.expected_kind == CLUSTER_COLD_DATA_PRESENT
			|| target->decision.expected_kind == CLUSTER_COLD_DATA_UNFORMATTED) {
			if (memcmp(space.incarnation, target->decision.expected_before.segment_incarnation, 16)
				|| (target->decision.expected_kind == CLUSTER_COLD_DATA_PRESENT
						? target->decision.expected_before.mutation_token == 0
						: target->decision.expected_before.mutation_token != 0))
				cold_redo_refuse(record, i);
		}
	}
	cold_redo.record = record;
	cold_redo.read_lsn = record->ReadRecPtr;
	cold_redo.end_lsn = record->EndRecPtr;
	cold_redo.crc = record->record->header.xl_crc;
	cold_redo.rmid = record->record->header.xl_rmid;
	cold_redo.info = record->record->header.xl_info;
	cluster_page_cold_redo_active_v1 = true;
}

static bool
cold_redo_target_matches(XLogReaderState *record, uint8 block_id, ClusterColdRedoBlockV1 *now)
{
	return cold_redo_record_matches(record) && block_id <= XLR_MAX_BLOCK_ID
		   && cold_redo.targets[block_id].present
		   && cluster_cold_redo_block_decision_v1(record, block_id, now)
		   && memcmp(now, &cold_redo.targets[block_id].decision, sizeof(*now)) == 0;
}

static ColdRedoTarget *
cold_redo_target(XLogReaderState *record, uint8 block_id, ClusterColdRedoBlockV1 *now)
{
	if (!cold_redo_target_matches(record, block_id, now))
		cold_redo_refuse(record, block_id);
	return &cold_redo.targets[block_id];
}

/* Two heap targets in one record can share its single full VM image. */
bool
cluster_page_cold_redo_vm_image_applied_v1(XLogReaderState *record, uint8 block_id)
{
	ClusterColdRedoBlockV1 decision;
	ColdRedoTarget *target = cold_redo_target(record, block_id, &decision);

	if (decision.action != CLUSTER_COLD_REDO_APPLY || target->forknum != VISIBILITYMAP_FORKNUM
		|| (target->read && (!target->dirtied || !target->image)))
		cold_redo_refuse(record, block_id);
	return target->dirtied;
}

static bool
cold_redo_page_zero(Page page)
{
	for (Size i = 0; i < BLCKSZ; i++)
		if (page[i] != 0)
			return false;
	return true;
}

bool
cluster_page_cold_redo_read_v1(XLogReaderState *record, uint8 block_id,
							   ReadBufferMode mode pg_attribute_unused(), bool cleanup,
							   Buffer *buffer, XLogRedoAction *action)
{
	ClusterColdRedoBlockV1 decision;
	ColdRedoTarget *target;
	PGAlignedBlock image;
	Buffer result = InvalidBuffer;
	bool restore, will_init;
	Page page;

	if (!cluster_page_cold_redo_active_v1) {
		if (!cluster_cold_redo_block_decision_v1(record, block_id, &decision)
			|| decision.action != CLUSTER_COLD_REDO_NATIVE)
			cold_redo_refuse(record, block_id);
		return false;
	}
	target = cold_redo_target(record, block_id, &decision);
	if (decision.action == CLUSTER_COLD_REDO_NATIVE)
		return false;
	*buffer = InvalidBuffer;
	if (decision.action == CLUSTER_COLD_REDO_SKIP) {
		*action = BLK_NOTFOUND;
		return true;
	}
	if (target->read)
		cold_redo_refuse(record, block_id);
	restore = XLogRecBlockImageApply(record, block_id);
	will_init = (XLogRecGetBlock(record, block_id)->flags & BKPBLOCK_WILL_INIT) != 0;
	if (restore) {
		if (!RestoreBlockImage(record, block_id, image.data) || PageIsNew((Page)image.data)
			|| ((PageHeader)image.data)->pd_block_scn != decision.result.mutation_token)
			cold_redo_refuse(record, block_id);
	}
	if ((decision.expected_kind == CLUSTER_COLD_DATA_INVALID
		 || decision.expected_kind == CLUSTER_COLD_DATA_ABSENT
		 || decision.expected_kind == CLUSTER_COLD_DATA_UNFORMATTED)
		&& !restore && !will_init)
		cold_redo_refuse(record, block_id);
	if (decision.expected_kind != CLUSTER_COLD_DATA_INVALID) {
		result = XLogReadBufferExtended(target->locator, target->forknum, target->blockno,
										RBM_NORMAL_NO_LOG, InvalidBuffer);
		if (!BufferIsValid(result)) {
			if (decision.expected_kind != CLUSTER_COLD_DATA_ABSENT)
				cold_redo_refuse(record, block_id);
		} else {
			if (cleanup)
				LockBufferForCleanup(result);
			else
				LockBuffer(result, BUFFER_LOCK_EXCLUSIVE);
			page = BufferGetPage(result);
			if (decision.expected_kind == CLUSTER_COLD_DATA_ABSENT
				|| (decision.expected_kind == CLUSTER_COLD_DATA_UNFORMATTED
						? !cold_redo_page_zero(page)
						: PageIsNew(page)
							  || ((PageHeader)page)->pd_block_scn
									 != decision.expected_before.mutation_token))
				cold_redo_refuse(record, block_id);
		}
	}
	if (!BufferIsValid(result))
		result = XLogReadBufferExtended(target->locator, target->forknum, target->blockno,
										cleanup ? RBM_ZERO_AND_CLEANUP_LOCK : RBM_ZERO_AND_LOCK,
										InvalidBuffer);
	if (!BufferIsValid(result))
		cold_redo_refuse(record, block_id);
	target->buffer = result;
	target->read = true;
	target->image = restore;
	*buffer = result;
	if (restore) {
		page = BufferGetPage(result);
		memcpy(page, image.data, BLCKSZ);
		PageSetLSN(page, record->EndRecPtr);
		MarkBufferDirty(result);
		*action = BLK_RESTORED;
	} else
		*action = BLK_NEEDS_REDO;
	return true;
}

/* MarkBufferDirty may run inside a critical section. Latch the first
 * violation without raising or blessing the page; the driver checks it after
 * native redo has left all critical sections. */
static void
cold_redo_note_violation(int block_id)
{
	/* The native rmgr can release its locks before end reports failure. Stop
	 * every recovery buffer writer first, including already-dirty pages. */
	cluster_page_wal_cold_redo_fail_v1();
	if (!cold_redo.violation) {
		cold_redo.violation = true;
		cold_redo.violation_block_id = block_id;
	}
}

void
cluster_page_cold_redo_dirty_v1(Buffer buffer)
{
	RelFileLocator locator;
	ForkNumber forknum;
	BlockNumber blockno;
	if (!cluster_page_cold_redo_active_v1 || cold_redo.violation)
		return;
	if (!BufferIsValid(buffer)) {
		cold_redo_note_violation(-1);
		return;
	}
	BufferGetTag(buffer, &locator, &forknum, &blockno);
	if (forknum == FSM_FORKNUM)
		return;
	for (uint8 i = 0; i <= XLR_MAX_BLOCK_ID; i++) {
		ColdRedoTarget *target = &cold_redo.targets[i];
		ClusterColdRedoBlockV1 decision;
		Page page;
		if (!target->present || target->forknum != forknum || target->blockno != blockno
			|| !RelFileLocatorEquals(target->locator, locator))
			continue;
		if (!cold_redo_target_matches(cold_redo.record, i, &decision)
			|| decision.action != CLUSTER_COLD_REDO_APPLY || !target->read
			|| target->buffer != buffer) {
			cold_redo_note_violation(i);
			return;
		}
		page = BufferGetPage(buffer);
		if (PageIsNew(page)
			|| (target->image
				&& ((PageHeader)page)->pd_block_scn != decision.result.mutation_token)) {
			cold_redo_note_violation(i);
			return;
		}
		((PageHeader)page)->pd_block_scn = decision.result.mutation_token;
		target->dirtied = true;
		return;
	}
	cold_redo_note_violation(-1);
}

void
cluster_page_cold_redo_end_v1(XLogReaderState *record)
{
	if (cold_redo.violation)
		cold_redo_refuse(record, cold_redo.violation_block_id);
	if (!cold_redo_record_matches(record))
		cold_redo_refuse(record, -1);
	for (uint8 i = 0; i <= XLR_MAX_BLOCK_ID; i++) {
		ColdRedoTarget *target = &cold_redo.targets[i];
		ClusterColdRedoBlockV1 decision;
		if (!target->present)
			continue;
		(void)cold_redo_target(record, i, &decision);
		if (decision.action == CLUSTER_COLD_REDO_APPLY && (!target->read || !target->dirtied))
			cold_redo_refuse(record, i);
	}
	cluster_page_cold_redo_abort_v1();
}
#endif
