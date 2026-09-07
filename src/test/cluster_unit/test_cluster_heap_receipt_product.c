/*-------------------------------------------------------------------------
 *
 * test_cluster_heap_receipt_product.c
 *    Runtime-boundary adapter for real heap receipt consumers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_heap_receipt_product.c
 *
 * NOTES
 *    This is a pgrac-original test file. No USE_CLUSTER_UNIT: target capture,
 *    final planning and final recheck are the actual heap implementation.
 *    A single buffer, stable PCM and epoch are runtime fixture boundaries.
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1
#include "../../backend/access/heap/heapam.c"

bool heap_receipt_test_capture(Page page, bool first, uint8 operation, ClusterCtrcTargetV1 *target);
uint32 heap_receipt_test_authority_mismatch(void);
bool heap_receipt_test_final(ClusterUndoRecordPrepareReceipt *receipt, ClusterCtrcTargetV1 *target);
bool heap_receipt_test_plan_capture(ClusterUndoRecordPrepareReceipt *receipt);
bool heap_receipt_test_plan_recheck(ClusterUndoRecordPrepareReceipt *receipt);
int heap_receipt_test_error_detail(const ClusterUndoRecordPrepareReceipt *receipt,
								   const ClusterCtrcTargetV1 *observed);

int NBuffers = 1;
int NLocBuffer = 0;
int wal_level = WAL_LEVEL_REPLICA;
char *BufferBlocks = NULL;
Block *LocalBufferBlockPointers = NULL;
static BufferDescPadded probe_descriptors[1];
BufferDescPadded *BufferDescriptors = probe_descriptors;
bool cluster_recmerge_window_active = false;
bool cluster_recmerge_apply_foreign = false;
uint64 cluster_recmerge_window_scn = 0;
uint64 cluster_recmerge_window_own_lsn = 0;
static ClusterPcmOwnSnapshot probe_pcm;
static ClusterHeapDmlAuthorityGuard probe_guard;

ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	if (buf != &probe_descriptors[0].bufferdesc || out == NULL)
		return CLUSTER_PCM_OWN_CORRUPT;
	*out = probe_pcm;
	return CLUSTER_PCM_OWN_OK;
}

uint64
cluster_epoch_get_current(void)
{
	return 19;
}

bool
heap_receipt_test_capture(Page page, bool first, uint8 operation, ClusterCtrcTargetV1 *target)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };
	ClusterHeapDmlAuthorityGuard current;
	HeapTupleData tuple = { 0 };
	ItemId item;

	BufferBlocks = page;
	if (first) {
		memset(probe_descriptors, 0, sizeof(probe_descriptors));
		memset(&probe_pcm, 0, sizeof(probe_pcm));
		probe_pcm.tag.spcOid = 1663;
		probe_pcm.tag.dbOid = 5;
		probe_pcm.tag.relNumber = 9001;
		probe_pcm.tag.forkNum = MAIN_FORKNUM;
		probe_pcm.tag.blockNum = 44;
		probe_pcm.generation = 17;
		probe_pcm.resource_x_activation_generation = 1;
		probe_pcm.pcm_state = PCM_STATE_X;
		probe_descriptors[0].bufferdesc.tag = probe_pcm.tag;
	}
	item = PageGetItemId(page, FirstOffsetNumber);
	tuple.t_data = (HeapTupleHeader)PageGetItem(page, item);
	tuple.t_len = ItemIdGetLength(item);
	ItemPointerSet(&tuple.t_self, 44, FirstOffsetNumber);
	if (!cluster_heap_dml_authority_guard_capture(1, &tuple, &current))
		return false;
	if (first)
		probe_guard = current;
	form.relpersistence = RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &form;
	return cluster_heap_ctrc_pending_itl_target(&relation, 1, &current, operation, target);
}

uint32
heap_receipt_test_authority_mismatch(void)
{
	return cluster_heap_dml_authority_guard_mismatch(1, &probe_guard);
}

static ClusterHeapPreparedUndoTargetPlan probe_plan;

bool
heap_receipt_test_final(ClusterUndoRecordPrepareReceipt *receipt, ClusterCtrcTargetV1 *target)
{
	UBA uba = InvalidUba_init;

	return cluster_heap_ctrc_final_itl_target(1, 700, false, receipt, 0, 64, 0, 1000, target, &uba);
}

bool
heap_receipt_test_plan_capture(ClusterUndoRecordPrepareReceipt *receipt)
{
	MemSet(&probe_plan, 0, sizeof(probe_plan));
	if (!cluster_heap_dml_authority_guard_capture(1, NULL, &probe_plan.guard)
		|| !cluster_heap_dml_authority_guard_bind_itl_slot(1, 0, &probe_plan.guard)
		|| !cluster_heap_ctrc_final_itl_target(1, 700, false, receipt, 0, 64, 0, 1000,
											   &probe_plan.final_target, &probe_plan.planned_uba))
		return false;
	probe_plan.valid = true;
	probe_plan.buffer = 1;
	probe_plan.xid = 700;
	probe_plan.payload_len = 64;
	probe_plan.write_scn = 1000;
	return true;
}

bool
heap_receipt_test_plan_recheck(ClusterUndoRecordPrepareReceipt *receipt)
{
	RelationData relation = { 0 };
	FormData_pg_class form = { 0 };

	form.relpersistence = RELPERSISTENCE_PERMANENT;
	relation.rd_rel = &form;
	return cluster_heap_itl_prepared_undo_target_recheck(&relation, receipt, &probe_plan);
}

int
heap_receipt_test_error_detail(const ClusterUndoRecordPrepareReceipt *receipt,
							   const ClusterCtrcTargetV1 *observed)
{
	ClusterCtrcTargetV1 targets[CLUSTER_UNDO_RECORD_CTRC_TARGETS] = { { 0 } };

	targets[0] = *observed;
	return cluster_heap_undo_retry_errdetail(receipt, targets, 1, 0, 1);
}
