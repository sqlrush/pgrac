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
 * IDENTIFICATION
 *	  src/backend/access/heap/heapam_r4_private.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HEAPAM_R4_PRIVATE_H
#define HEAPAM_R4_PRIVATE_H

#include "access/htup.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "cluster/cluster_scn.h"
#ifdef USE_PGRAC_CLUSTER
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

typedef enum HeapHotSearchResultKind
{
	HEAP_HOT_SEARCH_NOT_FOUND = 0,
	HEAP_HOT_SEARCH_BUFFER_BACKED,
	HEAP_HOT_SEARCH_OWNED_SCRATCH
} HeapHotSearchResultKind;

typedef struct HeapHotSearchResult
{
	HeapHotSearchResultKind kind;
	HeapTupleData tuple;
#ifdef USE_PGRAC_CLUSTER
	bool		remote_xmax_wait;
	ClusterTxLocator remote_wait_locator;
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
															Buffer full_buffer, uint64 *deadline_us,
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
