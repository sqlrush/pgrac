/*-------------------------------------------------------------------------
 *
 * test_cluster_ko_cancel.c
 *    Actual KO producer/callback/sender; the included fixture supplies IO/GES.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ko_cancel.c
 *
 * NOTES
 *    PGRAC-original standalone tests. Product APIs retain their cluster_
 *    namespace; dependency fixtures do not establish SQL or fabric results.
 *
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static ClusterKoSharedContext before_cancel;

static void
capture_cancel(void)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used && storage.contexts[i].request.batch_id == active_batch)
			before_cancel = storage.contexts[i];
}

UT_TEST(test_native_cancel_preserves_operation)
{
	RelationData rel;
	Oid old_database = MyDatabaseId;
	volatile bool caught = false;
	unsigned retained = 0;
	v3_native_structure_setup(&rel);
	MyDatabaseId = rel.rd_locator.dbOid;
	barrier_clock_wait = true;
	barrier_complete = false;
	barrier_reply_after = 100;
	barrier_interrupt_at = 3;
	barrier_interrupt_sqlstate = ERRCODE_QUERY_CANCELED;
	barrier_before_interrupt = capture_cancel;
	holder_probe_clock = 1;
	memset(&before_cancel, 0, sizeof(before_cancel));
	PG_TRY();
	{
		cluster_ko_flush_and_wait_ack_relation(&rel, RELPERSISTENCE_PERMANENT,
											   CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	barrier_before_interrupt = NULL;
	MyDatabaseId = old_database;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reported_sqlstate, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(barrier_requests, 1);
	UT_ASSERT(before_cancel.used && before_cancel.v3 && !before_cancel.complete);
	UT_ASSERT_EQ(before_cancel.request_sent_peers, 2);
	UT_ASSERT_EQ(before_cancel.request_uncertain_peers, 0);
	UT_ASSERT_EQ(record_count + failure_records + drop_count, 0);
	UT_ASSERT_EQ(barrier_removes, 1);
	UT_ASSERT(!barrier_active);
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used && storage.contexts[i].serial == before_cancel.serial)
			retained++;
	UT_ASSERT_EQ(retained, 1);
}

static ClusterKoSharedContext *
origin_context(const ClusterKoSharedMessageV3 *request)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY; i++)
		if (storage.contexts[i].used
			&& storage.contexts[i].request.batch_id == request->base.batch_id)
			return &storage.contexts[i];
	return NULL;
}

static void
deliver_origin_result(ClusterKoSharedMessageV3 request, uint16 status)
{
	ClusterICEnvelope env = { 0 };
	uint8 wire[CLUSTER_KO_SHARED_V3_BYTES];
	request.base.verb = CLUSTER_KO_SHARED_ACK;
	request.base.status = status;
	UT_ASSERT(cluster_ko_shared_encode_v3(&request, wire, sizeof(wire)));
	env.source_node_id = request.base.peer_node;
	env.payload_length = sizeof(wire);
	cluster_ko_flush_ack_handler(&env, wire);
}

static void
origin_ticks(void)
{
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY / 8; i++)
		shared_send_tick();
}

UT_TEST(test_cancel_wins_before_send_claim)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup();
	ClusterKoSharedContext *entry = origin_context(&request);
	ClusterKoCompletionV2 completion = { .pid = MyProcPid, .slot = 0, .serial = entry->serial };
	UT_ASSERT(ko_shared_enqueue_v3(&request, request.ax_master_generation));
	ko_completion_cancel(&completion);
	origin_ticks();
	UT_ASSERT(entry->used && entry->origin_detached);
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT_EQ(storage.send_v3_count, 0);
	UT_ASSERT_EQ(release_poll_calls, 0); /* SQL cancel is not native release. */
}

UT_TEST(test_claim_wins_cancel_and_late_exact_result)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup(), stale;
	ClusterKoSharedContext *entry = origin_context(&request);
	UT_ASSERT(ko_shared_v3_request_claim(&request));
	entry->origin_detached = entry->ax_release_requested = true;
	deliver_origin_result(request, CLUSTER_KO_SHARED_DONE);
	origin_ticks();
	UT_ASSERT_EQ(release_poll_calls, 0); /* transport call still owns a reference */
	UT_ASSERT_EQ(entry->request_done_peers, 2);
	ko_shared_v3_record_ownership(&request, KO_SEND_UNKNOWN);
	UT_ASSERT_EQ(entry->request_uncertain_peers, 0);
	stale = request;
	stale.base.batch_id++;
	deliver_origin_result(stale, CLUSTER_KO_SHARED_FAILED);
	UT_ASSERT_EQ(entry->request_failed_peers, 0);
	release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
	origin_ticks();
	UT_ASSERT(!entry->used);
	UT_ASSERT_EQ(release_poll_calls, 1);
}

UT_TEST(test_failed_and_unknown_never_release)
{
	for (int failed = 0; failed < 2; failed++) {
		ClusterKoSharedMessageV3 request = origin_retained_send_setup();
		ClusterKoSharedContext *entry = origin_context(&request);
		const char *reason;
		UT_ASSERT(ko_shared_v3_request_claim(&request));
		ko_shared_v3_record_ownership(&request, KO_SEND_UNKNOWN);
		entry->origin_detached = entry->ax_release_requested = true;
		if (failed)
			deliver_origin_result(request, CLUSTER_KO_SHARED_FAILED);
		release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
		origin_ticks();
		UT_ASSERT_EQ(release_poll_calls, 0);
		UT_ASSERT(entry->used);
		UT_ASSERT_EQ(cluster_ko_shared_normal_stop_poll_v2(&reason),
					 failed ? CLUSTER_NORMAL_STOP_INVALID : CLUSTER_NORMAL_STOP_PENDING);
	}
}

UT_TEST(test_native_release_conjoins_all_operations_same_ax)
{
	ClusterKoSharedMessageV3 first = origin_retained_send_setup(), second = first;
	uint64 boots[CLUSTER_KO_SHARED_NODE_LIMIT] = { 11, 22 };
	LOCKTAG tag;
	ClusterKoSharedContext *a = origin_context(&first), *b;
	memset(&tag, 0, sizeof(tag));
	SET_LOCKTAG_RELATION(tag, first.ax_resid.field1, first.ax_resid.field2);
	second.base.batch_id++;
	UT_ASSERT(ko_shared_reserve_v3(&second, boots, NULL) >= 0);
	b = origin_context(&second);
	UT_ASSERT(ko_shared_v3_request_claim(&first));
	UT_ASSERT(ko_shared_v3_request_claim(&second));
	ko_shared_v3_record_ownership(&first, KO_SEND_TRANSPORT_OWNED);
	ko_shared_v3_record_ownership(&second, KO_SEND_TRANSPORT_OWNED);
	deliver_origin_result(first, CLUSTER_KO_SHARED_DONE);
	UT_ASSERT(cluster_ko_ax_release_handoff_v3(&tag, AccessExclusiveLock, &first.ax_holder));
	UT_ASSERT(a->ax_release_requested && b->ax_release_requested);
	origin_ticks();
	UT_ASSERT_EQ(release_poll_calls, 0);
	deliver_origin_result(second, CLUSTER_KO_SHARED_DONE);
	release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
	origin_ticks();
	UT_ASSERT_EQ(release_poll_calls, 1);
	UT_ASSERT(!a->used && !b->used);
}

UT_TEST(test_release_error_and_cut_change_keep_original_owner)
{
	for (int failed = 0; failed < 2; failed++) {
		ClusterKoSharedMessageV3 request = origin_retained_send_setup();
		ClusterKoSharedContext *entry = origin_context(&request);
		volatile bool caught = false;
		entry->origin_detached = entry->ax_release_requested = true;
		release_poll_error = !failed;
		release_poll_result
			= failed ? CLUSTER_GES_REDECLARE_CUT_CHANGED : CLUSTER_GES_REDECLARE_PENDING;
		PG_TRY();
		{
			origin_ticks();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, !failed);
		UT_ASSERT(entry->used && entry->origin_detached && entry->ax_release_requested);
		UT_ASSERT(entry->ax_release.exchange.initialized);
		UT_ASSERT_EQ(entry->ax_release.exchange.key.request_id, request.ax_holder.request_id);
		UT_ASSERT_EQ(entry->ax_failed, failed);
	}
}

UT_TEST(test_cleanup_snapshot_claim_blocks_new_publication_and_retains_exact_holder)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup();
	ClusterKoAxCleanupV3 sample;
	ClusterGrdHolderId stale = request.ax_holder;
	ClusterKoSharedContext *entry = origin_context(&request);
	cluster_ko_ax_cleanup_sample_v3(&request.ax_resid, request.ax_holder.procno, -1, &sample);
	UT_ASSERT(entry->origin_detached && entry->ax_release_requested);
	UT_ASSERT_EQ(sample.count, 1);
	UT_ASSERT(cluster_ko_ax_cleanup_claim_v3(&sample));
	UT_ASSERT(!cluster_ko_ax_cleanup_claim_v3(&sample));
	UT_ASSERT(pg_atomic_read_u64(&storage.ax_publication_revision) & 1);
	UT_ASSERT(cluster_ko_ax_cleanup_retains_v3(&sample, &request.ax_holder));
	stale.request_id++;
	UT_ASSERT(!cluster_ko_ax_cleanup_retains_v3(&sample, &stale));
	cluster_ko_ax_cleanup_finish_v3(&sample);
	UT_ASSERT(!cluster_ko_ax_cleanup_claim_v3(&sample));
	UT_ASSERT_EQ(pg_atomic_read_u64(&storage.ax_publication_revision) & 1, 0);
}

UT_TEST(test_reentry_same_proc_group_and_reused_pid_wait)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup();
	ClusterKoSharedContext *entry = origin_context(&request);
	ClusterResId different = request.ax_resid;
	entry->origin_detached = true;
	entry->ax_lock_group = 71;
	UT_ASSERT(cluster_ko_ax_acquire_pending_v3(&request.ax_resid, request.ax_holder.procno, 0));
	MyProcPid++;
	UT_ASSERT(cluster_ko_ax_acquire_pending_v3(&request.ax_resid, request.ax_holder.procno, 0));
	MyProcPid--;
	UT_ASSERT(
		cluster_ko_ax_acquire_pending_v3(&request.ax_resid, request.ax_holder.procno + 1, 71));
	UT_ASSERT(
		!cluster_ko_ax_acquire_pending_v3(&request.ax_resid, request.ax_holder.procno + 1, 72));
	entry->ax_lock_group = 0; /* AX predates the holder becoming group leader. */
	UT_ASSERT(cluster_ko_ax_acquire_pending_v3(&request.ax_resid, request.ax_holder.procno + 1,
											   request.ax_holder.procno + 1));
	different.field2++;
	UT_ASSERT(!cluster_ko_ax_acquire_pending_v3(&different, request.ax_holder.procno, 71));
}

UT_TEST(test_completed_native_barrier_abort_and_exit_keep_same_slot)
{
	RelationData rel;
	Oid old_database = MyDatabaseId;
	ClusterKoCompletionV2 *completion;
	ClusterKoSharedContext *entry;
	uint64 serial;
	v3_native_structure_setup(&rel);
	MyDatabaseId = rel.rd_locator.dbOid;
	cluster_ko_flush_and_wait_ack_relation(&rel, RELPERSISTENCE_PERMANENT,
										   CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	completion = ko_completions;
	UT_ASSERT(completion && completion->barrier_private && completion->barrier_v3);
	UT_ASSERT(completion->slot < CLUSTER_KO_SHARED_CAPACITY);
	entry = &storage.contexts[completion->slot];
	serial = entry->serial;
	UT_ASSERT(entry->used && entry->complete);
	ko_completion_cancel(completion);
	UT_ASSERT(entry->used && entry->origin_detached && !entry->ax_release_requested);
	ko_shared_backend_exit(0, 0);
	UT_ASSERT(entry->used && entry->ax_release_requested && entry->serial == serial);
	UT_ASSERT_EQ(storage.native_incoming, InvalidDsaPointer);
	MyDatabaseId = old_database;
}


static int native_s6_calls, native_backout_calls;
void
cluster_ges_send_convert_rollback(const ClusterResId *resid, uint32 upgraded_mode, uint32 old_mode,
								  const ClusterGrdHolderId *holder, uint64 old_request_id,
								  uint64 convert_request_id)
{
	native_backout_calls++;
}
static LOCALLOCK *native_weaker;
static void *native_hash;
static void *
native_hash_find(void *table, const void *key, int action, bool *found)
{
	const LOCALLOCKTAG *tag = key;
	return native_weaker != NULL && tag->mode == native_weaker->tag.mode ? native_weaker : NULL;
}
void
cluster_lock_release(const ClusterLockReleaseRequest *request)
{
	native_s6_calls++;
}
static bool
cluster_advisory_locallock_is_session_scoped(const LOCALLOCK *lock)
{
	return false;
}
#define hash_search native_hash_find
#define LockMethodLocalHash native_hash
#include "test_cluster_ko_native_upgrade.inc"
static void
native_release_all_hook(LOCALLOCK *locallock, bool allLocks)
{
#include "test_cluster_ko_native_release_all.inc"
}
static void
native_release_hook(LOCALLOCK *locallock, bool sessionLock)
{
	const LOCKTAG *locktag = &locallock->tag.lock;
	LOCKMODE lockmode = locallock->tag.mode;
#include "test_cluster_ko_native_release.inc"
}
#undef LockMethodLocalHash
#undef hash_search

UT_TEST(test_native_all_preserves_surviving_session_upgrade)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup();
	LOCALLOCK high = { 0 }, low = { 0 };
	LOCALLOCKOWNER session = { .owner = NULL, .nLocks = 1 };
	PGPROC proc = { 0 };
	ClusterKoSharedContext *entry = origin_context(&request);
	SET_LOCKTAG_RELATION(high.tag.lock, request.ax_resid.field1, request.ax_resid.field2);
	high.tag.mode = AccessExclusiveLock;
	high.cluster_registered = true;
	high.cluster_request_id = request.ax_holder.request_id;
	high.cluster_convert_old_request_id = request.ax_holder.request_id - 1;
	high.cluster_convert_old_mode = AccessShareLock;
	memcpy(high.cluster_holder_raw, &request.ax_holder, sizeof(request.ax_holder));
	low.tag.lock = high.tag.lock;
	low.tag.mode = AccessShareLock;
	low.nLocks = 1;
	low.numLockOwners = 1;
	low.lockOwners = &session;
	native_weaker = &low;
	MyProc = &proc;
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 1);
	native_s6_calls = 0;
	native_release_all_hook(&high, false);
	UT_ASSERT(low.cluster_registered && low.cluster_ko_retained_ax);
	UT_ASSERT(!entry->ax_release_requested);
	UT_ASSERT_EQ(native_s6_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	native_weaker = NULL;
	native_release_all_hook(&low, true);
	UT_ASSERT(entry->ax_release_requested);
	UT_ASSERT_EQ(native_s6_calls, 0);
	MyProc = NULL;
}

UT_TEST(test_native_single_release_retains_parent_then_transfers_exact_ax)
{
	ClusterKoSharedMessageV3 request = origin_retained_send_setup();
	ClusterKoSharedContext *entry = origin_context(&request);
	LOCALLOCK high = { 0 }, low = { 0 };
	PGPROC proc = { 0 };
	SET_LOCKTAG_RELATION(high.tag.lock, request.ax_resid.field1, request.ax_resid.field2);
	high.tag.mode = AccessExclusiveLock;
	high.cluster_registered = true;
	high.cluster_request_id = request.ax_holder.request_id;
	high.cluster_convert_old_request_id = request.ax_holder.request_id - 1;
	high.cluster_convert_old_mode = AccessShareLock;
	memcpy(high.cluster_holder_raw, &request.ax_holder, sizeof(request.ax_holder));
	low.tag.lock = high.tag.lock;
	low.tag.mode = AccessShareLock;
	low.nLocks = 1;
	native_weaker = &low;
	MyProc = &proc;
	pg_atomic_init_u32(&proc.cluster_grd_registered_count, 1);
	native_s6_calls = native_backout_calls = 0;
	native_release_hook(&high, false);
	UT_ASSERT(low.cluster_registered && low.cluster_ko_retained_ax);
	UT_ASSERT_EQ(memcmp(low.cluster_holder_raw, &request.ax_holder, sizeof(request.ax_holder)), 0);
	UT_ASSERT(entry->origin_detached && !entry->ax_release_requested);
	UT_ASSERT_EQ(native_s6_calls + native_backout_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&proc.cluster_grd_registered_count), 1);
	native_weaker = NULL;
	native_release_hook(&low, false);
	UT_ASSERT(entry->ax_release_requested);
	UT_ASSERT_EQ(native_s6_calls + native_backout_calls, 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&proc.cluster_grd_registered_count), 0);
	MyProc = NULL;
}

int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_native_cancel_preserves_operation);
	UT_RUN(test_cancel_wins_before_send_claim);
	UT_RUN(test_claim_wins_cancel_and_late_exact_result);
	UT_RUN(test_failed_and_unknown_never_release);
	UT_RUN(test_native_release_conjoins_all_operations_same_ax);
	UT_RUN(test_release_error_and_cut_change_keep_original_owner);
	UT_RUN(test_cleanup_snapshot_claim_blocks_new_publication_and_retains_exact_holder);
	UT_RUN(test_reentry_same_proc_group_and_reused_pid_wait);
	UT_RUN(test_completed_native_barrier_abort_and_exit_keep_same_slot);
	UT_RUN(test_native_all_preserves_surviving_session_upgrade);
	UT_RUN(test_native_single_release_retains_parent_then_transfers_exact_ax);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
