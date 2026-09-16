/*-------------------------------------------------------------------------
 *
 * heapam_r4_private.h
 *	  Backend-private R4 heap HOT search result and test seams.
 *
 * This contract is shared only by heapam.c, heapam_visibility.c and
 * heapam_handler.c.  It does not change the public heap or TableAM API.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_r4_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAM_R4_PRIVATE_H
#define HEAPAM_R4_PRIVATE_H

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "cluster/cluster_scn.h"
#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_mode.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_tx_resolve.h"
#include "cluster/cluster_tx_enqueue.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_visibility_resolve.h"
#endif
#include "executor/tuptable.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
#include "utils/snapshot.h"
#include "utils/rel.h"

#ifdef USE_PGRAC_CLUSTER
/* Exact remote Dirty wait channel for the built-in successor consumer only. */
extern bool cluster_heap_fetch_waitable(Relation relation, Snapshot snapshot, HeapTuple tuple,
										Buffer *userbuf, bool keep_buf, bool *remote_xmax_wait,
										ClusterTxLocator *remote_wait_locator);
extern bool cluster_heap_fetch_owned(Relation relation, Snapshot snapshot, HeapTuple tuple,
									 Buffer *userbuf, bool keep_buf, char storage[BLCKSZ],
									 bool *remote_xmax_wait, ClusterTxLocator *remote_wait_locator);
extern bool cluster_heap_wait_successor(const ClusterTxLocator *locator, LockWaitPolicy wait_policy,
										uint64 *deadline_us);
extern TM_Result cluster_heap_lock_tuple_owned(Relation relation, HeapTuple tuple,
	CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, bool follow_updates,
	Buffer *buffer, TM_FailureData *tmfd, const ClusterHeapSuccessorProof *expected_successor,
	ClusterHeapSuccessorProof *next_successor, char storage[BLCKSZ]);
extern void cluster_heap_rebind_locked_tuple(Relation relation, Buffer buffer,
	HeapTuple tuple, TransactionId *creation_xmin);

/* A pin protects the mapping, not tuple byte offsets across GCS installs. */
static inline bool
cluster_heap_read_needs_copy(Relation relation)
{
	return cluster_storage_mode_enabled() && !RelationUsesLocalBuffers(relation);
}

static inline void
cluster_heap_copy_read_tuple(char storage[BLCKSZ], HeapTuple tuple)
{
	/* A copied result must be bounded in release builds too. */
	if (tuple->t_data == NULL || tuple->t_len < SizeofHeapTupleHeader || tuple->t_len > BLCKSZ)
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
			errmsg("cluster heap read result exceeds its owned storage")));
	memcpy(storage, tuple->t_data, tuple->t_len);
}

static inline void
cluster_heap_scan_capture_page(HeapScanDesc scan)
{
	if (cluster_heap_read_needs_copy(scan->rs_base.rs_rd))
	{
		memcpy(scan->rs_owned_page, BufferGetPage(scan->rs_cbuf), BLCKSZ);
		scan->rs_owned_kind = 1;
	}
}

static inline void
cluster_heap_scan_capture_tuple(HeapScanDesc scan)
{
	if (cluster_heap_read_needs_copy(scan->rs_base.rs_rd))
	{
		cluster_heap_copy_read_tuple(scan->rs_owned_page, &scan->rs_ctup);
		scan->rs_ctup.t_data = (HeapTupleHeader) scan->rs_owned_page;
		scan->rs_owned_kind = 2;
	}
}

static inline Page
cluster_heap_scan_page(HeapScanDesc scan)
{
	if (cluster_heap_read_needs_copy(scan->rs_base.rs_rd))
	{
		Assert(scan->rs_owned_kind == 1);
		return (Page) scan->rs_owned_page;
	}
	return BufferGetPage(scan->rs_cbuf);
}

static inline void
cluster_heap_scan_store(HeapScanDesc scan, TupleTableSlot *slot)
{
	if (cluster_heap_read_needs_copy(scan->rs_base.rs_rd))
	{
		HeapTupleData selected = scan->rs_ctup;

		Assert(scan->rs_owned_kind != 0);
		ExecForceStoreHeapTuple(&selected, slot, false);
		slot->tts_tid = selected.t_self;
		slot->tts_tableOid = RelationGetRelid(scan->rs_base.rs_rd);
	}
	else
		ExecStoreBufferHeapTuple(&scan->rs_ctup, slot, scan->rs_cbuf);
}
#endif

typedef enum HeapHotSearchResultKind
{
	HEAP_HOT_SEARCH_NOT_FOUND = 0,
	HEAP_HOT_SEARCH_BUFFER_BACKED,
	HEAP_HOT_SEARCH_OWNED_SCRATCH,
	HEAP_HOT_SEARCH_OWNED_CURRENT
} HeapHotSearchResultKind;

#ifdef USE_PGRAC_CLUSTER
/* Backend-private observations only; never inputs to visibility policy. */
#define CLUSTER_R4_SCRATCH_TRACE_CAPACITY 8

typedef struct ClusterR4ScratchVerdict {
	bool sampled;
	ClusterVisEvidence evidence;
	ClusterTTStatus status;
	SCN commit_scn;
	bool is_bound;
	ClusterUndoTTSlotRef ref;
} ClusterR4ScratchVerdict;

typedef struct ClusterR4ScratchObservation {
	OffsetNumber offset;
	TransactionId xmin;
	TransactionId xmax;
	uint16 infomask;
	uint16 infomask2;
	uint8 creator_slot;
	uint8 writer_slot;
	bool complete;
	bool visible;
	ClusterR4ScratchVerdict creator;
	ClusterR4ScratchVerdict deleter;
} ClusterR4ScratchObservation;

typedef struct ClusterR4ScratchTrace {
	uint32 total;
	ClusterR4ScratchObservation items[CLUSTER_R4_SCRATCH_TRACE_CAPACITY];
} ClusterR4ScratchTrace;

extern bool cluster_heap_r4_trace_format(const ClusterR4ScratchTrace *trace, char *out, Size size);
#endif

typedef struct HeapHotSearchResult
{
	HeapHotSearchResultKind kind;
	HeapTupleData tuple;
#ifdef USE_PGRAC_CLUSTER
	bool		remote_xmax_wait;
	ClusterTxLocator remote_wait_locator;
	ClusterR4ScratchTrace visibility_trace;
#endif
	char scratch_page[BLCKSZ] pg_attribute_aligned(MAXIMUM_ALIGNOF);
} HeapHotSearchResult;

typedef struct ClusterR4HotScratchTestContext
{
	Page scratch_page;
	BufferTag tag;
	ItemPointerData logical_root;
	SCN read_scn;
	bool already_full;
	bool allow_hint;
	bool allow_cleanout;
#ifdef USE_PGRAC_CLUSTER
	ClusterR4ScratchTrace *visibility_trace;
#endif
} ClusterR4HotScratchTestContext;

typedef void (*ClusterR4HotLockTestHook)(void *arg, bool acquire);
typedef bool (*ClusterR4HotFetchFullTestHook)(void *arg,
												const BufferTag *tag,
												SCN read_scn,
												char dst_page[BLCKSZ]);
typedef bool (*ClusterR4HotScratchSearchTestHook)(
	void *arg, const ClusterR4HotScratchTestContext *context,
	HeapTuple scratch_tuple);
typedef void (*ClusterHeapDmlAuthorityGuardTestHook)(
	Buffer buffer, HeapTuple tuple, void *arg);

typedef enum ClusterHeapNoRetryTestCaller
{
	CLUSTER_HEAP_NO_RETRY_TEST_INSERT = 0,
	CLUSTER_HEAP_NO_RETRY_TEST_DELETE,
	CLUSTER_HEAP_NO_RETRY_TEST_UPDATE_PAIR,
	CLUSTER_HEAP_NO_RETRY_TEST_TEMP_LOCK,
	CLUSTER_HEAP_NO_RETRY_TEST_HEAP_LOCK,
	CLUSTER_HEAP_NO_RETRY_TEST_UPDATE_CHAIN
} ClusterHeapNoRetryTestCaller;

typedef enum ClusterHeapNoRetryTestOutcome
{
	CLUSTER_HEAP_NO_RETRY_TEST_ZERO_APPLY_RETRY = 0,
	CLUSTER_HEAP_NO_RETRY_TEST_APPLIED,
	CLUSTER_HEAP_NO_RETRY_TEST_REFUSED
} ClusterHeapNoRetryTestOutcome;

typedef struct ClusterHeapNoRetryTestReport
{
	ClusterHeapNoRetryTestOutcome outcome;
	uint8 preflight_calls;
	uint8 apply_calls;
	uint8 consume_calls;
	uint8 last_preflight_event;
	uint8 first_apply_event;
	uint8 last_apply_event;
	uint8 consume_event;
	uint8 retained_undo_handle_count;
	bool retry_edge;
} ClusterHeapNoRetryTestReport;

typedef struct ClusterHeapPrepareRetryTestReport
{
	uint8 prepare_calls;
	bool deadline_stable;
	uint64 observed_deadline_us;
	ClusterUndoRecordPrepareResult terminal_result;
} ClusterHeapPrepareRetryTestReport;

typedef enum ClusterHeapMultiInsertRoute
{
	CLUSTER_HEAP_MULTI_INSERT_NATIVE_BATCH = 0,
	CLUSTER_HEAP_MULTI_INSERT_RECEIPT_SAFE_PER_TUPLE
} ClusterHeapMultiInsertRoute;

/* A27: capacity is not boolean.  An exact Resource-X successor lifecycle is
 * caller-owned requalification; only unknown/invalid evidence is refusal. */
typedef enum ClusterHeapItlCapacityResult
{
	CLUSTER_HEAP_ITL_CAPACITY_EXHAUSTED_OR_REFUSED = 0,
	CLUSTER_HEAP_ITL_CAPACITY_READY,
	CLUSTER_HEAP_ITL_CAPACITY_RETRY_REQUALIFY
} ClusterHeapItlCapacityResult;

/* PK IndexScan companion; public heap_hot_search_buffer() remains unchanged. */
extern HeapHotSearchResultKind heap_hot_search_buffer_result(
	ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
	HeapHotSearchResult *result, bool *all_dead, bool first_call);

extern bool HeapTupleSatisfiesMVCCScratch(
	HeapTuple tuple, Snapshot snapshot,
	const ClusterR4HotScratchTestContext *context);

#ifdef USE_PGRAC_CLUSTER
extern bool cluster_heap_tuple_satisfies_visibility_waitable(
	HeapTuple tuple, Snapshot snapshot, Buffer buffer,
	bool *remote_xmax_wait, ClusterTxLocator *remote_wait_locator);
#endif

#ifdef USE_CLUSTER_UNIT
extern bool cluster_heap_test_satisfies_mvcc(HeapTuple tuple, Snapshot snapshot, Buffer buffer);
extern bool cluster_heap_test_writer_wait(Relation relation, Buffer buffer, HeapTuple tuple,
										  TransactionId xid, uint16 infomask, TM_Result *result);
extern bool cluster_heap_test_r4_target_reachable(void);
extern HeapHotSearchResultKind cluster_heap_test_r4_hot_full_cycle(
	BufferTag tag, ItemPointerData logical_root, SCN read_scn,
	HeapHotSearchResult *result, ClusterR4HotLockTestHook lock_hook,
	ClusterR4HotFetchFullTestHook fetch_full_hook,
	ClusterR4HotScratchSearchTestHook scratch_search_hook, void *hook_arg,
	bool *call_again, bool *all_dead);
extern TableIndexFetchTupleResult cluster_heap_test_r4_store_hot_result(
	HeapHotSearchResult *result, TupleTableSlot *slot, Buffer buffer,
	bool *call_again, bool *all_dead);
#ifdef USE_PGRAC_CLUSTER
extern TableIndexFetchTupleResult cluster_heap_test_r4_index_hot_result(
	ItemPointer tid, Relation relation, Buffer buffer, Snapshot snapshot,
	HeapHotSearchResult *result, TupleTableSlot *slot,
	bool *call_again, bool *all_dead);
extern bool cluster_heap_test_itl_alloc_with_terminal_census(
	Buffer buffer, TransactionId xid, bool lock_only, uint8 *slot_index_out);
extern ClusterHeapItlCapacityResult cluster_heap_test_itl_capacity_outcome(
	Buffer buffer, TransactionId xid, bool lock_only);
extern ClusterVisEvidence
cluster_heap_test_resolve_recycled_writer_ref(Buffer buffer, TransactionId xid,
											  const ClusterUndoTTSlotRef *ref, bool lock_only,
											  ClusterVisResolve *out);
extern int cluster_heap_test_itl_remaining_wait_ms(uint64 *deadline_us, uint64 now_us,
												   int budget_ms);
extern ClusterTxwResult cluster_heap_test_itl_wait_capacity(Buffer old_buffer, Buffer new_buffer,
															Buffer full_buffer, TransactionId xid,
															uint64 *deadline_us,
															const char **diagnostic_reason);
extern bool cluster_heap_test_itl_resolve_pair_terminal_census(
	Buffer old_buffer, Buffer new_buffer, Buffer full_buffer);
extern bool cluster_heap_test_itl_update_same_page_failure_cleanup(void);
extern void cluster_heap_test_itl_last_census_stats(
	uint8 *locator_mask, uint8 *attempted_mask,
	uint8 *terminal_mask, uint8 *terminal_count);
extern bool cluster_heap_test_dml_authority_guard_recheck_with_hook(
	Buffer buffer, HeapTuple tuple,
	ClusterHeapDmlAuthorityGuardTestHook hook, void *hook_arg);
extern bool cluster_heap_test_dml_authority_guard_slot_recheck_with_hook(
	Buffer buffer, HeapTuple tuple, uint8 slot_index,
	ClusterHeapDmlAuthorityGuardTestHook hook, void *hook_arg);
extern bool cluster_heap_test_no_retry_boundary(
	ClusterHeapNoRetryTestCaller caller, bool final_recheck_drift,
	bool partial_apply_failure, ClusterHeapNoRetryTestReport *report);
extern bool cluster_heap_test_prepare_retry_sequence(
	const ClusterUndoRecordPrepareResult *results, uint8 result_count,
	uint64 absolute_deadline_us, ClusterHeapPrepareRetryTestReport *report);
extern bool cluster_heap_test_itl_receipt_identity_admitted(
	TransactionId canonical_xid, uint32 segment_id);
extern ClusterHeapMultiInsertRoute cluster_heap_test_multi_insert_route(
	bool current_itl_path);
extern bool cluster_heap_test_update_needs_successor_prediction(
	bool current_itl_path, bool current_mx_recomposed);
extern bool cluster_heap_test_itl_relation_route(
	bool storage_mode, bool uses_local_buffers, bool shared_catalog,
	RelFileNumber rel_number);
extern bool cluster_heap_test_current_mx_authorize_keyshare(
	Relation relation, Buffer buffer, HeapTuple tuple,
	TransactionId requester_xid, TM_Result *result,
	MultiXactMember *normalized, uint16 normalized_cap,
	uint16 *normalized_count);
extern bool cluster_heap_test_current_mx_epoch_supported(uint64 epoch);
extern bool cluster_heap_test_current_mx_read_requalification(
	const ClusterPcmOwnSnapshot *before,
	const ClusterPcmOwnSnapshot *after);
#endif
#endif

#endif							/* HEAPAM_R4_PRIVATE_H */
