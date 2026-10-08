/*-------------------------------------------------------------------------
 * test_cluster_grd_capacity.c -- exact owners on the real GRD/GES path.
 *
 * Allocation, formation and transport reuse the handoff fixture. Capacity,
 * compatible grants, LMON decisions and exact reclamation execute product C.
 * The four node IDs below are offline identities, not a running cluster.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_HW_HANDOFF_EMBEDDED
#include "test_cluster_hw_handoff.c"
#include "cluster/cluster_control_retire.h"
#include "cluster/cluster_ic_router.h"

/* Retain fail-stop control transport boundaries. Master-side retirement uses
 * A's dedup storage observer; it does not send or certify a cleanup exchange. */
Latch *MyLatch;

void
ResetLatch(Latch *latch pg_attribute_unused())
{}

int
WaitLatch(Latch *latch pg_attribute_unused(), int events pg_attribute_unused(),
		  long timeout pg_attribute_unused(), uint32 event pg_attribute_unused())
{
	abort();
}

bool
cluster_recovery_transport_components_current(void)
{
	return false;
}

bool
cluster_ges_dedup_retire_control_request(uint32 node, uint32 procno, uint64 epoch, uint64 request)
{
	if (hw_control_retire_observe != NULL)
		return hw_control_retire_observe(node, procno, epoch, request);
	abort();
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 type pg_attribute_unused(), int32 dest pg_attribute_unused(),
						 const void *payload pg_attribute_unused(),
						 uint32 len pg_attribute_unused())
{
	abort();
}

typedef struct CapacityCounts {
	int entries;
	int holders;
	int waiters;
	int converts;
} CapacityCounts;

static void
capacity_count_row(void *context, const int32 fields[11])
{
	CapacityCounts *counts = context;

	counts->entries++;
	counts->holders += fields[7];
	counts->waiters += fields[8];
	counts->converts += fields[9];
}

static void
capacity_expect_queues(int entries, int holders, int waiters, int converts)
{
	CapacityCounts counts = { 0 };

	cluster_grd_entries_walk(capacity_count_row, &counts);
	UT_ASSERT_EQ(counts.entries, entries);
	UT_ASSERT_EQ(counts.holders, holders);
	UT_ASSERT_EQ(counts.waiters, waiters);
	UT_ASSERT_EQ(counts.converts, converts);
	UT_ASSERT_EQ(cluster_grd_entry_count(), entries);
}

static void
capacity_expect_counts(int entries, int holders)
{
	capacity_expect_queues(entries, holders, 0, 0);
}

static void
capacity_expect_stop(ClusterNormalStopPollResult expected)
{
	bool saved = IsUnderPostmaster;
	bool saved_enabled = cluster_enabled;

	/* The census requires this explicit formation precondition. Its scan and
	 * verdict remain real; no running postmaster is involved. */
	IsUnderPostmaster = true;
	cluster_enabled = true;
	UT_ASSERT_EQ(cluster_grd_normal_stop_poll(NULL, NULL, NULL), expected);
	cluster_enabled = saved_enabled;
	IsUnderPostmaster = saved;
}

static void
capacity_setup(ClusterLockAcquireRequest *request, ClusterGrdHolderId *lmon_holder)
{
	ClusterGrdShared *shared;
	bool found;

	queued_cut_case = true;
	hw_bast_observe = NULL;
	hw_reply_observe = NULL;
	hw_dedup_remove_observe = NULL;
	hw_dedup_record_observe = NULL;
	hw_control_retire_observe = NULL;
	queued_cut_prepare(GES_REQ_OPCODE_REQUEST, false, 0, request, lmon_holder);
	ut_wfg_reset();
	request->resid = (ClusterResId){ .field1 = 5,
									 .field2 = 16385,
									 .type = LOCKTAG_RELATION,
									 .lockmethodid = DEFAULT_LOCKMETHOD };
	cluster_node_id = 1;
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && shared != NULL);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&request->resid)], 1);
	memcpy(master_request.resid, &request->resid, sizeof(request->resid));
	master_request.lockmode = RowExclusiveLock;
	master_work_pending = false;
	capacity_expect_counts(0, 0);
}

static ClusterGrdHolderId
capacity_holder(int index)
{
	ClusterGrdHolderId holder = grd_lifecycle_holder(index % 4, 100 + index, 1000 + index);

	holder.cluster_epoch = ut_mock_epoch;
	return holder;
}

static ClusterGrdGrantAction
capacity_acquire(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	ClusterGrdGrantAction action;
	int conflicts = -1;

	action = cluster_grd_entry_enqueue_or_grant(resid, holder, holder->node_id, holder->request_id,
												9, GES_REQ_OPCODE_REQUEST, RowExclusiveLock, NULL,
												&conflicts);
	UT_ASSERT_EQ(conflicts, 0);
	return action;
}

static void
capacity_release_present(const ClusterResId *resid, const ClusterGrdHolderId *holder)
{
	if (cluster_grd_holder_mode_by_id(resid, holder, NULL)) {
		UT_ASSERT_EQ(cluster_grd_release_holder_by_id(resid, holder), CLUSTER_GRD_ENTRY_OK);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(resid, holder, NULL));
	}
}

static int capacity_attach_cleanup_calls;

static void
capacity_attach_scope_cleanup(int code, Datum arg)
{
	UT_ASSERT_EQ(code, 0);
	UT_ASSERT_EQ(DatumGetInt32(arg), 501);
	capacity_attach_cleanup_calls++;
}

static void
capacity_first_attach_scope(bool inject_error)
{
	ClusterResId resid;
	sigjmp_buf *saved_exception_stack = PG_exception_stack;
	ErrorContextCallback *saved_context_stack = error_context_stack;
	volatile int lookups = 0;
	volatile bool caught = false;
	volatile bool returned = false;

	/* Initialize shared storage without a lookup. This process must still be
	 * cold when the temporary cleanup is pushed, as in current_acquire_begin. */
	grd_lifecycle_reset(4);
	grd_lifecycle_resid(501, &resid);
	UT_ASSERT_EQ(ut_grd_before_count, 0);
	UT_ASSERT_EQ(ut_grd_on_count, 0);
	UT_ASSERT_EQ(ut_grd_exit_lifo_errors, 0);
	capacity_attach_cleanup_calls = 0;
	PG_TRY();
	{
		PG_ENSURE_ERROR_CLEANUP(capacity_attach_scope_cleanup, Int32GetDatum(501));
		{
			for (int i = 0; i < 2; i++) {
				ClusterGrdEntry *entry = NULL;

				UT_ASSERT_EQ(cluster_grd_entry_lookup_or_create(&resid, false, &entry),
							 CLUSTER_GRD_ENTRY_NOT_FOUND);
				UT_ASSERT(entry == NULL);
				lookups++;
				UT_ASSERT_EQ(ut_grd_before_count, 1);
				UT_ASSERT_EQ(ut_grd_on_count, 1);
				if (ut_grd_before_count > 0
					&& ut_grd_before_count <= lengthof(ut_grd_before_callbacks)) {
					UT_ASSERT(ut_grd_before_callbacks[ut_grd_before_count - 1]
							  == capacity_attach_scope_cleanup);
					UT_ASSERT_EQ(ut_grd_before_arguments[ut_grd_before_count - 1],
								 Int32GetDatum(501));
				}
			}
			if (inject_error)
				ereport(ERROR, (errmsg("injected error after first GRD attachment")));
		}
		PG_END_ENSURE_ERROR_CLEANUP(capacity_attach_scope_cleanup, Int32GetDatum(501));
		returned = true;
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	printf("# cold_attach error=%d lookups=%d before=%d on=%d lifo=%d cleanup=%d caught=%d "
		   "returned=%d\n",
		   inject_error, lookups, ut_grd_before_count, ut_grd_on_count, ut_grd_exit_lifo_errors,
		   capacity_attach_cleanup_calls, caught, returned);
	UT_ASSERT_EQ(lookups, 2);
	UT_ASSERT_EQ(caught, inject_error);
	UT_ASSERT_EQ(returned, !inject_error);
	UT_ASSERT_EQ(capacity_attach_cleanup_calls, inject_error ? 1 : 0);
	UT_ASSERT_EQ(ut_grd_before_count, 0);
	UT_ASSERT_EQ(ut_grd_on_count, 1);
	UT_ASSERT_EQ(ut_grd_exit_lifo_errors, 0);
	UT_ASSERT(PG_exception_stack == saved_exception_stack);
	UT_ASSERT(error_context_stack == saved_context_stack);
}

UT_TEST(first_grd_attach_preserves_temporary_error_cleanup_scope)
{
	/* Run first: neither child may inherit an already attached GRD area. Two
	 * fresh processes cover normal END cancellation and the ERROR unwind;
	 * a failing callback stack cannot contaminate the other capacity cases. */
	for (int inject_error = 0; inject_error < 2; inject_error++) {
		pid_t child = fork();
		pid_t waited;
		int status = 0;

		UT_ASSERT(child >= 0);
		if (child < 0)
			continue;
		if (child == 0) {
			alarm(30);
			ut_current_failed = 0;
			capacity_first_attach_scope(inject_error != 0);
			_exit(ut_current_failed ? 1 : 0);
		}
		do {
			waited = waitpid(child, &status, 0);
		} while (waited < 0 && errno == EINTR);
		UT_ASSERT_EQ(waited, child);
		UT_ASSERT(WIFEXITED(status));
		if (waited == child && WIFEXITED(status))
			UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
}

/* A compatible owner must not disappear at the old per-resource boundary.
 * Keep scanning and clean actual owners even on RED, so the same run proves
 * both the missing grants and whether exact release leaves residual state. */
static void
capacity_compatible_owners(int count)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId lmon_holder;
	ClusterGrdHolderId holders[256];
	bool present[256] = { false };
	int granted = 0;
	int first_refused = 0;

	HW_CHECK(count > 0 && count <= lengthof(holders));
	capacity_setup(&request, &lmon_holder);
	for (int i = 0; i < count; i++) {
		ClusterGrdGrantAction action;
		LOCKMODE mode = NoLock;

		holders[i] = capacity_holder(i);
		action = capacity_acquire(&request.resid, &holders[i]);
		present[i] = cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode);
		UT_ASSERT_EQ(present[i], action == CLUSTER_GRD_GRANT_NOW);
		if (present[i])
			UT_ASSERT_EQ(mode, RowExclusiveLock);
		if (action == CLUSTER_GRD_GRANT_NOW)
			granted++;
		else if (first_refused == 0)
			first_refused = i + 1;
	}
	printf("# compatible requested=%d granted=%d first_refused=%d\n", count, granted,
		   first_refused);
	UT_ASSERT_EQ(granted, count);
	capacity_expect_counts(1, count);
	for (int i = count - 1; i >= 0; i--) {
		capacity_release_present(&request.resid, &holders[i]);
		if (i > 0 && present[i - 1])
			UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i - 1], NULL));
	}
	capacity_expect_counts(0, 0);
	printf("# compatible requested=%d cleanup_entries=%d\n", count, cluster_grd_entry_count());
	queued_cut_case = false;
	MyProc = NULL;
}

UT_TEST(compatible_16_control_releases_exact_owners)
{
	capacity_compatible_owners(16);
}

UT_TEST(compatible_17_owners_are_all_granted)
{
	capacity_compatible_owners(17);
}

UT_TEST(compatible_32_owners_are_all_granted)
{
	capacity_compatible_owners(32);
}

UT_TEST(compatible_256_owners_are_all_granted)
{
	capacity_compatible_owners(256);
}

/* Assert the required grant, not the baseline's generic refusal. The reply
 * is diagnostic evidence for this offline path, not a claim about the sole
 * source of any live rejection. Freeing one slot must also allow exact reuse. */
UT_TEST(real_lmon_grants_the_17th_compatible_owner)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId lmon_holder;
	ClusterGrdHolderId holders[16];
	LOCKMODE mode = NoLock;
	bool installed;

	capacity_setup(&request, &lmon_holder);
	for (int i = 0; i < lengthof(holders); i++) {
		holders[i] = capacity_holder(i);
		UT_ASSERT_EQ(capacity_acquire(&request.resid, &holders[i]), CLUSTER_GRD_GRANT_NOW);
	}
	capacity_expect_counts(1, 16);
	stage_master_work();
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(master_reply_count, 1);
	installed = cluster_grd_holder_mode_by_id(&request.resid, &lmon_holder, &mode);
	printf("# lmon owner=17 opcode=%u reason=%u installed=%d\n", master_reply.opcode,
		   master_reply.reject_reason, installed);
	UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(master_reply.reject_reason, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(master_reply.reply_for_opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(master_reply.holder_node_id, lmon_holder.node_id);
	UT_ASSERT_EQ(master_reply.holder_procno, lmon_holder.procno);
	UT_ASSERT_EQ(master_reply.holder_cluster_epoch_lo, (uint32)lmon_holder.cluster_epoch);
	UT_ASSERT_EQ(master_reply.holder_cluster_epoch_hi, (uint32)(lmon_holder.cluster_epoch >> 32));
	UT_ASSERT_EQ(master_reply.holder_request_id_lo, (uint32)lmon_holder.request_id);
	UT_ASSERT_EQ(master_reply.holder_request_id_hi, (uint32)(lmon_holder.request_id >> 32));
	UT_ASSERT(memcmp(master_reply.resid, &request.resid, sizeof(request.resid)) == 0);
	UT_ASSERT(installed);
	if (installed)
		UT_ASSERT_EQ(mode, RowExclusiveLock);
	capacity_expect_counts(1, 17);

	/* Reissue the same identity after release, with no deadline change. */
	capacity_release_present(&request.resid, &lmon_holder);
	capacity_release_present(&request.resid, &holders[0]);
	stage_master_work();
	master_reply_count = 0;
	memset(&master_reply, 0, sizeof(master_reply));
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(master_reply_count, 1);
	UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(master_reply.reject_reason, GES_REJECT_REASON_NONE);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &lmon_holder, &mode));
	UT_ASSERT_EQ(mode, RowExclusiveLock);
	capacity_expect_counts(1, 16);
	capacity_release_present(&request.resid, &lmon_holder);
	for (int i = 1; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	printf("# lmon exact_reuse_granted=%d cleanup_entries=%d\n",
		   master_reply.opcode == GES_REPLY_OPCODE_GRANT, cluster_grd_entry_count());
	queued_cut_case = false;
	MyProc = NULL;
}

static void
capacity_seed(const ClusterResId *resid, ClusterGrdHolderId *holders, int count)
{
	int granted = 0;

	for (int i = 0; i < count; i++) {
		holders[i] = capacity_holder(i);
		if (capacity_acquire(resid, &holders[i]) == CLUSTER_GRD_GRANT_NOW)
			granted++;
	}
	printf("# seed requested=%d granted=%d\n", count, granted);
	UT_ASSERT_EQ(granted, count);
}

static bool
capacity_same_holder(const ClusterGrdHolderId *a, const ClusterGrdHolderId *b)
{
	return a->node_id == b->node_id && a->procno == b->procno
		   && a->cluster_epoch == b->cluster_epoch && a->request_id == b->request_id;
}

/* The production snapshot feeds targeted BAST. The inherited WFG sink records
 * the edges emitted by real GRD mutations; it does not decide grant outcomes. */
UT_TEST(conflict_snapshot_and_wfg_include_all_32_owners)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId waiter, holders[32];
	ClusterGrdConflictHolder *conflicts = NULL;
	int count = -1;

	capacity_setup(&request, &waiter);
	capacity_seed(&request.resid, holders, lengthof(holders));
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&request.resid, &waiter, waiter.node_id,
													waiter.request_id, 9, GES_REQ_OPCODE_REQUEST,
													AccessExclusiveLock, &conflicts, &count),
				 CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT_EQ(count, 32);
	UT_ASSERT(conflicts != NULL);
	UT_ASSERT_EQ(
		ut_wfg_count_waiter(waiter.node_id, waiter.procno, waiter.cluster_epoch, waiter.request_id),
		32);
	for (int i = 0; i < lengthof(holders); i++) {
		int matches = 0;

		for (int j = 0; conflicts != NULL && j < count && j < 32; j++) {
			if (capacity_same_holder(&conflicts[j].holder, &holders[i])) {
				matches++;
				UT_ASSERT_EQ(conflicts[j].source_node_id, holders[i].node_id);
				UT_ASSERT_EQ(conflicts[j].held_mode, RowExclusiveLock);
			}
		}
		UT_ASSERT_EQ(matches, 1);
		UT_ASSERT(ut_wfg_has_edge(waiter.node_id, waiter.procno, waiter.cluster_epoch,
								  waiter.request_id, holders[i].node_id, holders[i].procno,
								  holders[i].cluster_epoch, holders[i].request_id));
	}
	if (conflicts != NULL)
		pfree(conflicts);
	capacity_expect_queues(1, 32, 1, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &waiter), CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiter, NULL));
	for (int i = 0; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	MyProc = NULL;
}

UT_TEST(nowait_conflict_never_adds_waiter_or_wfg_edge)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId waiter, holders[32];

	capacity_setup(&request, &waiter);
	capacity_seed(&request.resid, holders, lengthof(holders));
	UT_ASSERT_EQ(cluster_grd_entry_grant_conditional(
					 &request.resid, &waiter, waiter.node_id, waiter.request_id, 9,
					 GES_REQ_OPCODE_REQUEST_NOWAIT, AccessExclusiveLock, NULL, NULL),
				 CLUSTER_GRD_CONFLICT_NOWAIT);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiter, NULL));
	UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &waiter),
				 CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	capacity_expect_counts(1, 32);
	for (int i = 0; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	UT_ASSERT_EQ(cluster_grd_entry_grant_conditional(
					 &request.resid, &waiter, waiter.node_id, waiter.request_id, 9,
					 GES_REQ_OPCODE_REQUEST_NOWAIT, AccessExclusiveLock, NULL, NULL),
				 CLUSTER_GRD_GRANT_NOW);
	capacity_release_present(&request.resid, &waiter);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	MyProc = NULL;
}

UT_TEST(release_and_reuse_preserve_each_exact_identity)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId waiter, holders[32], changed;

	capacity_setup(&request, &waiter);
	capacity_seed(&request.resid, holders, lengthof(holders));
	for (int dimension = 0; dimension < 4; dimension++) {
		changed = holders[0];
		if (dimension == 0)
			changed.node_id = 1;
		else if (dimension == 1)
			changed.procno += 4096;
		else if (dimension == 2)
			changed.cluster_epoch += UINT64CONST(0x100000000);
		else
			changed.request_id += UINT64CONST(0x100000000);
		UT_ASSERT_EQ(cluster_grd_release_holder_by_id(&request.resid, &changed),
					 CLUSTER_GRD_ENTRY_NOT_FOUND);
		UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[0], NULL));
	}
	capacity_release_present(&request.resid, &holders[0]);
	changed = holders[0];
	changed.request_id += UINT64CONST(0x100000000);
	UT_ASSERT_EQ(capacity_acquire(&request.resid, &changed), CLUSTER_GRD_GRANT_NOW);
	UT_ASSERT_EQ(cluster_grd_release_holder_by_id(&request.resid, &holders[0]),
				 CLUSTER_GRD_ENTRY_NOT_FOUND);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &changed, NULL));
	capacity_expect_counts(1, 32);
	capacity_release_present(&request.resid, &changed);
	for (int i = 1; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	MyProc = NULL;
}

UT_TEST(waiters_32_keep_fifo_after_exact_cancellation)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId blocker, waiters[32];
	ClusterGrdGrantIdentity granted[33];
	int queued = 0;
	int n;

	capacity_setup(&request, &blocker);
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&request.resid, &blocker, blocker.node_id,
													blocker.request_id, 9, GES_REQ_OPCODE_REQUEST,
													AccessExclusiveLock, NULL, NULL),
				 CLUSTER_GRD_GRANT_NOW);
	for (int i = 0; i < lengthof(waiters); i++) {
		ClusterGrdWaiterMeta meta = { 0 };

		waiters[i] = capacity_holder(i);
		meta.wait_seq = 500 + i;
		if (cluster_grd_entry_enqueue_or_grant_meta(
				&request.resid, &waiters[i], waiters[i].node_id, waiters[i].request_id, meta, 9,
				GES_REQ_OPCODE_REQUEST, AccessExclusiveLock, NULL, NULL)
			== CLUSTER_GRD_ENQUEUED_WAITER)
			queued++;
	}
	printf("# waiters requested=32 queued=%d\n", queued);
	UT_ASSERT_EQ(queued, 32);
	capacity_expect_queues(1, 1, 32, 0);
	UT_ASSERT_EQ(ut_wfg_n, 32);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_PENDING);
	UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id_seq(&request.resid, &waiters[0], 501),
				 CLUSTER_GRD_ENTRY_NOT_FOUND);
	/* Keep the oldest and newest; cancel every intervening exact sequence. */
	for (int i = 1; i < 31; i++)
		UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id_seq(&request.resid, &waiters[i], 500 + i),
					 CLUSTER_GRD_ENTRY_OK);
	capacity_expect_queues(1, 1, 2, 0);
	n = cluster_grd_release_and_drain(&request.resid, &blocker, granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	if (n == 1)
		UT_ASSERT(capacity_same_holder(&granted[0].holder, &waiters[0]));
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiters[31], NULL));
	n = cluster_grd_release_and_drain(&request.resid, &waiters[0], granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	if (n == 1)
		UT_ASSERT(capacity_same_holder(&granted[0].holder, &waiters[31]));
	for (int i = 0; i < lengthof(waiters); i++) {
		(void)cluster_grd_cancel_waiter_by_id(&request.resid, &waiters[i]);
		capacity_release_present(&request.resid, &waiters[i]);
	}
	capacity_release_present(&request.resid, &blocker);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	MyProc = NULL;
}

UT_TEST(converts_12_keep_drain_priority_after_exact_cancel)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId waiter, holders[13], converts[12];
	ClusterGrdGrantIdentity granted[33];
	int queued = 0;
	int n;

	capacity_setup(&request, &waiter);
	capacity_seed(&request.resid, holders, lengthof(holders));
	for (int i = 0; i < lengthof(converts); i++) {
		ClusterGrdWaiterMeta meta = { 0 };

		converts[i] = holders[i];
		converts[i].request_id = 5000 + i;
		meta.wait_seq = 700 + i;
		if (cluster_grd_convert_or_enqueue_meta(
				&request.resid, converts[i].node_id, converts[i].procno, converts[i].cluster_epoch,
				RowExclusiveLock, AccessExclusiveLock, converts[i].request_id, converts[i].node_id,
				9, meta, NULL, NULL)
			== CLUSTER_GRD_CONVERT_ENQUEUED)
			queued++;
	}
	printf("# converts requested=12 queued=%d\n", queued);
	UT_ASSERT_EQ(queued, 12);
	capacity_expect_queues(1, 13, 0, 12);
	/* A conflicting request queues behind the existing convert. Preserve the
	 * original drain priority; do not invent a new compatible-arrival barrier. */
	UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(&request.resid, &waiter, waiter.node_id,
													waiter.request_id, 9, GES_REQ_OPCODE_REQUEST,
													AccessExclusiveLock, NULL, NULL),
				 CLUSTER_GRD_ENQUEUED_WAITER);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiter, NULL));
	UT_ASSERT_EQ(cluster_grd_cancel_convert_by_id(&request.resid, &converts[0], 701),
				 CLUSTER_GRD_ENTRY_NOT_FOUND);
	for (int i = 1; i < lengthof(converts); i++) {
		LOCKMODE mode = NoLock;

		UT_ASSERT_EQ(cluster_grd_cancel_convert_by_id(&request.resid, &converts[i], 700 + i),
					 CLUSTER_GRD_ENTRY_OK);
		UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode));
		UT_ASSERT_EQ(mode, RowExclusiveLock);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
	}
	capacity_expect_queues(1, 13, 1, 1);
	for (int i = 1; i < lengthof(holders); i++) {
		n = cluster_grd_release_and_drain(&request.resid, &holders[i], granted, lengthof(granted));
		UT_ASSERT_EQ(n, i == 12 ? 1 : 0);
		if (n == 1) {
			UT_ASSERT(capacity_same_holder(&granted[0].holder, &converts[0]));
			UT_ASSERT_EQ(granted[0].request_opcode, GES_REQ_OPCODE_CONVERT);
			UT_ASSERT_EQ(granted[0].mode, AccessExclusiveLock);
		}
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiter, NULL));
	}
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &holders[0], NULL));
	UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &converts[0], NULL));
	n = cluster_grd_release_and_drain(&request.resid, &converts[0], granted, lengthof(granted));
	UT_ASSERT_EQ(n, 1);
	if (n == 1)
		UT_ASSERT(capacity_same_holder(&granted[0].holder, &waiter));
	capacity_release_present(&request.resid, &waiter);
	for (int i = 0; i < lengthof(converts); i++) {
		(void)cluster_grd_cancel_convert_by_id(&request.resid, &converts[i], 700 + i);
		capacity_release_present(&request.resid, &converts[i]);
	}
	for (int i = 0; i < lengthof(holders); i++)
		capacity_release_present(&request.resid, &holders[i]);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	MyProc = NULL;
}

UT_TEST(reservations_32_survive_s3_s5_and_exact_s7_cancel)
{
	ClusterLockAcquireRequest base, requests[32];
	ClusterGrdHolderId lmon_holder;
	ClusterLockAcquireResult s3[32];
	int reserved = 0;

	capacity_setup(&base, &lmon_holder);
	base.locktag.locktag_type = LOCKTAG_RELATION;
	base.lockmode = RowExclusiveLock;
	for (int i = 0; i < lengthof(requests); i++) {
		requests[i] = base;
		requests[i].request_id = 2000 + i;
		MyProc->pgprocno = i;
		s3[i] = cluster_lock_acquire_s3_partition_reservation(&requests[i]);
		if (s3[i] == CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			reserved++;
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&base.resid, &requests[i].holder, NULL));
	}
	printf("# reservations requested=32 reserved=%d\n", reserved);
	UT_ASSERT_EQ(reserved, 32);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_PENDING);
	/* All S3 obligations overlap. Cancel odd requests before promoting even
	 * requests; a late promotion cannot resurrect a cancelled reservation. */
	for (int i = 1; i < lengthof(requests); i += 2) {
		MyProc->pgprocno = i;
		UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup(&requests[i]),
					 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT_EQ(cluster_grd_promote_remote_grant_mode_exact(&base.resid, &requests[i].holder,
																 RowExclusiveLock),
					 CLUSTER_GRD_ENTRY_NOT_FOUND);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&base.resid, &requests[i].holder, NULL));
	}
	for (int i = 0; i < lengthof(requests); i += 2) {
		LOCKMODE mode = NoLock;

		MyProc->pgprocno = i;
		/* Failed S3 is already a RED and must never be treated as a grant. */
		if (s3[i] != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			continue;
		UT_ASSERT_EQ(cluster_lock_acquire_s4_remote_request_wait(&requests[i]),
					 CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK);
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&requests[i]),
					 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT(cluster_grd_holder_mode_by_id(&base.resid, &requests[i].holder, &mode));
		UT_ASSERT_EQ(mode, RowExclusiveLock);
		UT_ASSERT_EQ(cluster_lock_acquire_s7_cleanup(&requests[i]),
					 CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
		UT_ASSERT(cluster_grd_holder_mode_by_id(&base.resid, &requests[i].holder, NULL));
		capacity_release_present(&base.resid, &requests[i].holder);
	}
	for (int i = 0; i < lengthof(requests); i++) {
		(void)cluster_grd_cancel_reservation_by_id(&base.resid, &requests[i].holder);
		capacity_release_present(&base.resid, &requests[i].holder);
	}
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	MyProc = NULL;
}

typedef struct CapacityBast {
	uint32 destination;
	GesRequestPayload payload;
} CapacityBast;

static CapacityBast capacity_basts[256];
static int capacity_bast_count;

static void
capacity_observe_bast(uint32 destination, const GesRequestPayload *payload)
{
	if (capacity_bast_count < lengthof(capacity_basts)) {
		capacity_basts[capacity_bast_count].destination = destination;
		capacity_basts[capacity_bast_count].payload = *payload;
	}
	capacity_bast_count++;
}

static void
capacity_lmon_basts(int count, bool nowait, bool compatible_tail)
{
	const int32 remote_nodes[] = { 0, 2, 3 };
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId waiter, holders[32];
	int granted = 0;
	int expected = nowait ? 0 : count - (compatible_tail ? 1 : 0);

	HW_CHECK(count > 0 && count <= lengthof(holders));
	capacity_setup(&request, &waiter);
	for (int i = 0; i < count; i++) {
		LOCKMODE mode = compatible_tail && i == count - 1 ? AccessShareLock : RowExclusiveLock;

		holders[i] = capacity_holder(i);
		holders[i].node_id = remote_nodes[i % lengthof(remote_nodes)];
		holders[i].request_id += UINT64CONST(0x100000000);
		if (cluster_grd_entry_enqueue_or_grant(&request.resid, &holders[i], holders[i].node_id,
											   holders[i].request_id, 9, GES_REQ_OPCODE_REQUEST,
											   mode, NULL, NULL)
			== CLUSTER_GRD_GRANT_NOW)
			granted++;
	}
	UT_ASSERT_EQ(granted, count);
	memset(capacity_basts, 0, sizeof(capacity_basts));
	capacity_bast_count = 0;
	hw_bast_observe = capacity_observe_bast;
	/* Share conflicts with RowExclusive but not AccessShare. All seeded
	 * holders are remote; local ProcSignal delivery is a separate boundary. */
	master_request.lockmode = ShareLock;
	master_request.opcode = nowait ? GES_REQ_OPCODE_REQUEST_NOWAIT : GES_REQ_OPCODE_REQUEST;
	stage_master_work();
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	printf("# lmon_bast holders=%d nowait=%d expected=%d observed=%d\n", count, nowait, expected,
		   capacity_bast_count);
	UT_ASSERT_EQ(capacity_bast_count, expected);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiter, NULL));
	for (int i = 0; i < count; i++) {
		int matches = 0;

		for (int j = 0; j < capacity_bast_count && j < lengthof(capacity_basts); j++) {
			const CapacityBast *bast = &capacity_basts[j];
			const GesRequestPayload *p = &bast->payload;
			ClusterGrdHolderId observed;

			observed.node_id = p->holder_node_id;
			observed.procno = p->holder_procno;
			observed.cluster_epoch
				= ((uint64)p->holder_cluster_epoch_hi << 32) | p->holder_cluster_epoch_lo;
			observed.request_id = ((uint64)p->holder_request_id_hi << 32) | p->holder_request_id_lo;
			if (capacity_same_holder(&observed, &holders[i])) {
				matches++;
				UT_ASSERT_EQ(bast->destination, holders[i].node_id);
				UT_ASSERT_EQ(p->opcode, GES_REQ_OPCODE_BAST);
				UT_ASSERT_EQ(p->lockmode, ShareLock);
				UT_ASSERT(memcmp(p->resid, &request.resid, sizeof(request.resid)) == 0);
			}
		}
		UT_ASSERT_EQ(matches, nowait || (compatible_tail && i == count - 1) ? 0 : 1);
	}
	if (nowait) {
		UT_ASSERT_EQ(master_reply_count, 1);
		UT_ASSERT_EQ(master_reply.opcode, GES_REPLY_OPCODE_REJECT);
		UT_ASSERT_EQ(master_reply.reject_reason, GES_REJECT_REASON_LOCK_CONFLICT);
		UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &waiter),
					 CLUSTER_GRD_ENTRY_NOT_FOUND);
		UT_ASSERT_EQ(ut_wfg_n, 0);
		capacity_expect_counts(1, count);
	} else {
		UT_ASSERT_EQ(master_reply_count, 0);
		capacity_expect_queues(1, count, 1, 0);
		UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &waiter),
					 CLUSTER_GRD_ENTRY_OK);
	}
	hw_bast_observe = NULL;
	for (int i = 0; i < count; i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_grd_pool_used, 0);
	MyProc = NULL;
}

UT_TEST(real_lmon_bast_16_control)
{
	capacity_lmon_basts(16, false, false);
}

UT_TEST(real_lmon_bast_reaches_all_32_exact_remote_owners)
{
	capacity_lmon_basts(32, false, false);
}

UT_TEST(real_lmon_nowait_sends_no_bast)
{
	capacity_lmon_basts(32, true, false);
}

UT_TEST(real_lmon_bast_excludes_compatible_owner)
{
	capacity_lmon_basts(16, false, true);
}

typedef enum CapacityPoolKind {
	CAPACITY_POOL_HOLDER,
	CAPACITY_POOL_WAITER,
	CAPACITY_POOL_CONVERT,
	CAPACITY_POOL_RESERVATION
} CapacityPoolKind;

/* Return success only for the requested responsibility. No allocator call or
 * result is substituted here: the product reaches A's bounded DSA boundary. */
static bool
capacity_pool_attempt(const ClusterResId *resid, const ClusterGrdHolderId *holder,
					  CapacityPoolKind kind, uint64 sequence, int *result)
{
	ClusterGrdWaiterMeta meta = { 0 };
	uint64 generation;

	meta.wait_seq = sequence;
	if (kind == CAPACITY_POOL_CONVERT) {
		*result = cluster_grd_convert_or_enqueue_meta(
			resid, holder->node_id, holder->procno, holder->cluster_epoch, RowExclusiveLock,
			AccessExclusiveLock, holder->request_id, holder->node_id, 9, meta, NULL, NULL);
		return *result == CLUSTER_GRD_CONVERT_ENQUEUED;
	}
	if (kind == CAPACITY_POOL_RESERVATION) {
		*result = cluster_grd_try_reserve(resid, holder, RowExclusiveLock, cluster_node_id, NULL,
										  &generation);
		return *result == CLUSTER_GRD_ENTRY_OK;
	}
	*result = cluster_grd_entry_enqueue_or_grant_meta(
		resid, holder, holder->node_id, holder->request_id, meta, 9, GES_REQ_OPCODE_REQUEST,
		kind == CAPACITY_POOL_WAITER ? AccessExclusiveLock : RowExclusiveLock, NULL, NULL);
	return *result
		   == (kind == CAPACITY_POOL_WAITER ? CLUSTER_GRD_ENQUEUED_WAITER : CLUSTER_GRD_GRANT_NOW);
}

static void
capacity_pool_exhaustion(CapacityPoolKind kind)
{
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId blocker, occupied[128], holders[13], probes[64];
	ClusterResId subject;
	ClusterGrdShared *shared;
	CapacityCounts before = { 0 }, after = { 0 };
	int saved_max_backends = MaxBackends;
	int attempted = 0;
	int rejected = -1;
	int result = -1;
	int count = kind == CAPACITY_POOL_CONVERT ? 12 : lengthof(probes);
	Size budget = 0;
	bool found;

	/* All proc numbers below fit this explicit startup configuration. */
	MaxBackends = 256;
	capacity_setup(&request, &blocker);
	capacity_seed(&request.resid, occupied, lengthof(occupied));
	UT_ASSERT(ut_grd_pool_used > 0);
	subject = request.resid;
	subject.field2++;
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && shared != NULL);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&subject)], 1);
	if (kind == CAPACITY_POOL_WAITER)
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(
						 &subject, &blocker, blocker.node_id, blocker.request_id, 9,
						 GES_REQ_OPCODE_REQUEST, AccessExclusiveLock, NULL, NULL),
					 CLUSTER_GRD_GRANT_NOW);
	if (kind == CAPACITY_POOL_CONVERT) {
		for (int i = 0; i < lengthof(holders); i++) {
			holders[i] = capacity_holder(i);
			holders[i].request_id += 20000;
			UT_ASSERT_EQ(capacity_acquire(&subject, &holders[i]), CLUSTER_GRD_GRANT_NOW);
		}
	}
	/* Freeze an actually occupied byte budget. The first resource's release
	 * must make a later allocation possible without raising this limit. */
	budget = ut_grd_pool_used;
	UT_ASSERT(budget > 0);
	if (budget == 0)
		goto cleanup;
	ut_grd_pool_limit = budget;
	for (int i = 0; i < count; i++) {
		memset(&before, 0, sizeof(before));
		cluster_grd_entries_walk(capacity_count_row, &before);
		probes[i] = kind == CAPACITY_POOL_CONVERT ? holders[i] : capacity_holder(i);
		probes[i].request_id += 40000;
		attempted++;
		if (!capacity_pool_attempt(&subject, &probes[i], kind, 9000 + i, &result)) {
			rejected = i;
			break;
		}
	}
	printf("# pool kind=%d budget=%zu used=%zu admitted=%d result=%d\n", kind, (size_t)budget,
		   (size_t)ut_grd_pool_used, rejected >= 0 ? rejected : attempted, result);
	UT_ASSERT(rejected >= 0);
	UT_ASSERT_EQ(ut_grd_pool_used, budget);
	if (rejected >= 0) {
		UT_ASSERT_EQ(result, kind == CAPACITY_POOL_CONVERT		 ? CLUSTER_GRD_CONVERT_QUEUE_FULL
							 : kind == CAPACITY_POOL_RESERVATION ? CLUSTER_GRD_ENTRY_FULL
																 : CLUSTER_GRD_WAIT_QUEUE_FULL);
		cluster_grd_entries_walk(capacity_count_row, &after);
		UT_ASSERT_EQ(after.entries, before.entries);
		UT_ASSERT_EQ(after.holders, before.holders);
		UT_ASSERT_EQ(after.waiters, before.waiters);
		UT_ASSERT_EQ(after.converts, before.converts);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&subject, &probes[rejected], NULL));
		if (kind == CAPACITY_POOL_WAITER)
			UT_ASSERT_EQ(
				cluster_grd_cancel_waiter_by_id_seq(&subject, &probes[rejected], 9000 + rejected),
				CLUSTER_GRD_ENTRY_NOT_FOUND);
		if (kind == CAPACITY_POOL_CONVERT) {
			LOCKMODE mode = NoLock;

			UT_ASSERT_EQ(
				cluster_grd_cancel_convert_by_id(&subject, &probes[rejected], 9000 + rejected),
				CLUSTER_GRD_ENTRY_NOT_FOUND);
			UT_ASSERT(cluster_grd_holder_mode_by_id(&subject, &holders[rejected], &mode));
			UT_ASSERT_EQ(mode, RowExclusiveLock);
		}
		if (kind == CAPACITY_POOL_RESERVATION)
			UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&subject, &probes[rejected]),
						 CLUSTER_GRD_ENTRY_NOT_FOUND);
		capacity_expect_stop(CLUSTER_NORMAL_STOP_PENDING);
		for (int i = 0; i < lengthof(occupied); i++)
			capacity_release_present(&request.resid, &occupied[i]);
		UT_ASSERT(ut_grd_pool_used < budget);
		UT_ASSERT_EQ(ut_grd_pool_limit, budget);
		UT_ASSERT(
			capacity_pool_attempt(&subject, &probes[rejected], kind, 9000 + rejected, &result));
		UT_ASSERT(ut_grd_pool_used <= budget);
		printf("# pool kind=%d exact_retry_result=%d used=%zu unchanged_budget=%zu\n", kind, result,
			   (size_t)ut_grd_pool_used, (size_t)ut_grd_pool_limit);
	}

cleanup:
	for (int i = 0; i < attempted; i++) {
		if (kind == CAPACITY_POOL_WAITER)
			(void)cluster_grd_cancel_waiter_by_id_seq(&subject, &probes[i], 9000 + i);
		if (kind == CAPACITY_POOL_CONVERT)
			(void)cluster_grd_cancel_convert_by_id(&subject, &probes[i], 9000 + i);
		if (kind == CAPACITY_POOL_RESERVATION)
			(void)cluster_grd_cancel_reservation_by_id(&subject, &probes[i]);
		capacity_release_present(&subject, &probes[i]);
	}
	if (kind == CAPACITY_POOL_CONVERT)
		for (int i = 0; i < lengthof(holders); i++)
			capacity_release_present(&subject, &holders[i]);
	capacity_release_present(&subject, &blocker);
	for (int i = 0; i < lengthof(occupied); i++)
		capacity_release_present(&request.resid, &occupied[i]);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_grd_pool_used, 0);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	ut_grd_pool_limit = 0;
	MaxBackends = saved_max_backends;
	MyProc = NULL;
}

UT_TEST(pool_full_never_partially_grants_holder_and_release_reuses_bytes)
{
	capacity_pool_exhaustion(CAPACITY_POOL_HOLDER);
}

UT_TEST(pool_full_never_partially_enqueues_waiter_and_release_reuses_bytes)
{
	capacity_pool_exhaustion(CAPACITY_POOL_WAITER);
}

UT_TEST(pool_full_keeps_convert_original_holder_and_release_reuses_bytes)
{
	capacity_pool_exhaustion(CAPACITY_POOL_CONVERT);
}

UT_TEST(pool_full_never_orphans_reservation_and_release_reuses_bytes)
{
	capacity_pool_exhaustion(CAPACITY_POOL_RESERVATION);
}

typedef struct CapacityReply {
	uint32 destination;
	GesReplyPayload payload;
} CapacityReply;

static CapacityReply capacity_replies[64];
static int capacity_reply_count;
static ClusterGesDedupKey capacity_dedup_removals[8];
static ClusterGesDedupKey capacity_dedup_records[64];
static int capacity_dedup_remove_count;
static int capacity_dedup_record_count;

static void
capacity_observe_dedup_remove(const ClusterGesDedupKey *key)
{
	if (capacity_dedup_remove_count < lengthof(capacity_dedup_removals))
		capacity_dedup_removals[capacity_dedup_remove_count] = *key;
	capacity_dedup_remove_count++;
}

static void
capacity_observe_dedup_record(const ClusterGesDedupKey *key, const GesReplyPayload *reply)
{
	if (capacity_dedup_record_count < lengthof(capacity_dedup_records))
		capacity_dedup_records[capacity_dedup_record_count] = *key;
	capacity_dedup_record_count++;
	UT_ASSERT_EQ(reply->opcode, GES_REPLY_OPCODE_GRANT);
	UT_ASSERT_EQ(reply->reject_reason, GES_REJECT_REASON_NONE);
}

static void
capacity_expect_dedup_key(const ClusterGesDedupKey *keys, int count, int capacity,
						  const ClusterGrdHolderId *holder, uint32 opcode, uint64 generation)
{
	int matches = 0;

	for (int i = 0; i < count && i < capacity; i++) {
		const ClusterGesDedupKey *key = &keys[i];

		if (key->origin_node_id == (uint32)holder->node_id && key->holder_procno == holder->procno
			&& key->cluster_epoch == holder->cluster_epoch && key->request_id == holder->request_id
			&& key->opcode == opcode) {
			matches++;
			UT_ASSERT_EQ(key->shard_master_generation, generation);
			UT_ASSERT_EQ(key->_pad0, 0);
		}
	}
	UT_ASSERT_EQ(matches, 1);
}

static void
capacity_observe_reply(uint32 destination, const GesReplyPayload *payload)
{
	if (capacity_reply_count < lengthof(capacity_replies)) {
		capacity_replies[capacity_reply_count].destination = destination;
		capacity_replies[capacity_reply_count].payload = *payload;
	}
	capacity_reply_count++;
}

static void
capacity_expect_grant_reply(const ClusterResId *resid, const ClusterGrdHolderId *holder,
							uint32 request_opcode)
{
	int matches = 0;

	for (int i = 0; i < capacity_reply_count && i < lengthof(capacity_replies); i++) {
		const CapacityReply *reply = &capacity_replies[i];
		const GesReplyPayload *p = &reply->payload;
		ClusterGrdHolderId observed;

		observed.node_id = p->holder_node_id;
		observed.procno = p->holder_procno;
		observed.cluster_epoch
			= ((uint64)p->holder_cluster_epoch_hi << 32) | p->holder_cluster_epoch_lo;
		observed.request_id = ((uint64)p->holder_request_id_hi << 32) | p->holder_request_id_lo;
		if (capacity_same_holder(&observed, holder)) {
			matches++;
			UT_ASSERT_EQ(reply->destination, holder->node_id);
			UT_ASSERT_EQ(p->opcode, GES_REPLY_OPCODE_GRANT);
			UT_ASSERT_EQ(p->reply_for_opcode, request_opcode);
			UT_ASSERT_EQ(p->reject_reason, GES_REJECT_REASON_NONE);
			UT_ASSERT(memcmp(p->resid, resid, sizeof(*resid)) == 0);
		}
	}
	UT_ASSERT_EQ(matches, 1);
}

static void
capacity_lmon_release_converts(int count)
{
	const int32 remote_nodes[] = { 0, 2, 3 };
	const uint32 acquire_opcodes[] = { GES_REQ_OPCODE_REQUEST, GES_REQ_OPCODE_REQUEST_NOWAIT,
									   GES_REQ_OPCODE_CONVERT, GES_REQ_OPCODE_REDECLARE };
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId blocker, holders[32], converts[32];
	int queued = 0;
	int promoted = 0;

	HW_CHECK(count > 0 && count <= lengthof(holders));
	capacity_setup(&request, &blocker);
	blocker.node_id = 2;
	blocker.request_id += UINT64CONST(0x300000000);
	UT_ASSERT_EQ(capacity_acquire(&request.resid, &blocker), CLUSTER_GRD_GRANT_NOW);
	/* All original AccessShare owners coexist with RowExclusive. Their Share
	 * upgrades wait only for that blocker and can all be granted on its release. */
	for (int i = 0; i < count; i++) {
		holders[i] = capacity_holder(i);
		holders[i].node_id = remote_nodes[i % lengthof(remote_nodes)];
		holders[i].request_id += UINT64CONST(0x100000000);
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(
						 &request.resid, &holders[i], holders[i].node_id, holders[i].request_id, 9,
						 GES_REQ_OPCODE_REQUEST, AccessShareLock, NULL, NULL),
					 CLUSTER_GRD_GRANT_NOW);
	}
	for (int i = 0; i < count; i++) {
		ClusterGrdWaiterMeta meta = { 0 };

		converts[i] = holders[i];
		converts[i].request_id += UINT64CONST(0x100000000);
		meta.wait_seq = 15000 + i;
		if (cluster_grd_convert_or_enqueue_meta(&request.resid, converts[i].node_id,
												converts[i].procno, converts[i].cluster_epoch,
												AccessShareLock, ShareLock, converts[i].request_id,
												converts[i].node_id, 9, meta, NULL, NULL)
			== CLUSTER_GRD_CONVERT_ENQUEUED)
			queued++;
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
	}
	UT_ASSERT_EQ(queued, count);
	capacity_expect_queues(1, count + 1, 0, count);
	master_request.opcode = GES_REQ_OPCODE_RELEASE;
	master_request.lockmode = RowExclusiveLock;
	master_request.holder_node_id = blocker.node_id;
	master_request.holder_procno = blocker.procno;
	master_request.holder_cluster_epoch_lo = (uint32)blocker.cluster_epoch;
	master_request.holder_cluster_epoch_hi = (uint32)(blocker.cluster_epoch >> 32);
	master_request.holder_request_id_lo = (uint32)blocker.request_id;
	master_request.holder_request_id_hi = (uint32)(blocker.request_id >> 32);
	memset(capacity_replies, 0, sizeof(capacity_replies));
	memset(capacity_dedup_removals, 0, sizeof(capacity_dedup_removals));
	memset(capacity_dedup_records, 0, sizeof(capacity_dedup_records));
	capacity_reply_count = 0;
	capacity_dedup_remove_count = 0;
	capacity_dedup_record_count = 0;
	master_reply_count = 0;
	hw_reply_observe = capacity_observe_reply;
	hw_dedup_remove_observe = capacity_observe_dedup_remove;
	hw_dedup_record_observe = capacity_observe_dedup_record;
	stage_master_work();
	UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
	UT_ASSERT_EQ(capacity_reply_count, count + 1);
	UT_ASSERT_EQ(master_reply_count, count + 1);
	UT_ASSERT_EQ(capacity_dedup_record_count, count);
	UT_ASSERT_EQ(capacity_dedup_remove_count, lengthof(acquire_opcodes));
	for (int i = 0; i < lengthof(acquire_opcodes); i++)
		capacity_expect_dedup_key(capacity_dedup_removals, capacity_dedup_remove_count,
								  lengthof(capacity_dedup_removals), &blocker, acquire_opcodes[i],
								  47);
	capacity_expect_grant_reply(&request.resid, &blocker, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &blocker, NULL));
	for (int i = 0; i < count; i++) {
		LOCKMODE mode = NoLock;

		capacity_expect_grant_reply(&request.resid, &converts[i], GES_REQ_OPCODE_CONVERT);
		capacity_expect_dedup_key(capacity_dedup_records, capacity_dedup_record_count,
								  lengthof(capacity_dedup_records), &converts[i],
								  GES_REQ_OPCODE_CONVERT, 9);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &holders[i], NULL));
		if (cluster_grd_holder_mode_by_id(&request.resid, &converts[i], &mode))
			promoted++;
		UT_ASSERT_EQ(mode, ShareLock);
	}
	printf("# lmon_release converts=%d queued=%d promoted=%d replies=%d expected=%d\n", count,
		   queued, promoted, capacity_reply_count, count + 1);
	UT_ASSERT_EQ(promoted, count);
	capacity_expect_queues(1, count, 0, 0);
	hw_reply_observe = NULL;
	hw_dedup_remove_observe = NULL;
	hw_dedup_record_observe = NULL;
	/* Clean both actual outcomes after RED; an unpromoted original owner or
	 * pending convert must never be mistaken for a delivered GRANT. */
	for (int i = 0; i < count; i++) {
		(void)cluster_grd_cancel_convert_by_id(&request.resid, &converts[i], 15000 + i);
		capacity_release_present(&request.resid, &converts[i]);
		capacity_release_present(&request.resid, &holders[i]);
	}
	capacity_release_present(&request.resid, &blocker);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_grd_pool_used, 0);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	MyProc = NULL;
}

UT_TEST(real_lmon_release_delivers_all_16_compatible_convert_grants)
{
	capacity_lmon_release_converts(16);
}

UT_TEST(real_lmon_release_delivers_all_32_compatible_convert_grants)
{
	capacity_lmon_release_converts(32);
}

typedef enum CapacityControlDrain {
	CAPACITY_CONTROL_LMON_RELEASE,
	CAPACITY_CONTROL_LOCAL_RELEASE,
	CAPACITY_CONTROL_RETIRE
} CapacityControlDrain;

static ClusterGrdHolderId capacity_last_control_retired;
static int capacity_control_retired_count;

static bool
capacity_observe_control_retire(uint32 node, uint32 procno, uint64 epoch, uint64 request)
{
	capacity_last_control_retired = (ClusterGrdHolderId){ .node_id = node,
														  .procno = procno,
														  .cluster_epoch = epoch,
														  .request_id = request };
	capacity_control_retired_count++;
	return true; /* Dedup storage only; the real master decides retirement. */
}

static void
capacity_retire_control(const ClusterResId *resid, const ClusterGrdHolderId *holder,
						const ClusterGrdHolderId *previous)
{
	ClusterControlRetireMessage message = { 0 };
	ClusterControlRequestCut cut = { 0 };
	int before = capacity_control_retired_count;
	bool have_cut = cluster_control_retire_cut(resid, &cut);

	UT_ASSERT(have_cut);
	if (!have_cut)
		return;
	UT_ASSERT_EQ(MyBackendType, B_LMON);
	message.key.resid = *resid;
	message.key.holder = *holder;
	message.cleanup_epoch = cut.epoch;
	message.exchange_id = 20000 + before;
	message.verb = CLUSTER_CONTROL_RETIRE;
	if (previous != NULL) {
		message.previous_request = previous->request_id;
		message.previous_mode = AccessShareLock;
	}
	UT_ASSERT_EQ(cluster_ges_control_retire_at_master(&message, &cut), CLUSTER_CONTROL_RETIRED);
	UT_ASSERT_EQ(capacity_control_retired_count, before + 1);
	UT_ASSERT(capacity_same_holder(&capacity_last_control_retired, holder));
}

/* The ordered control callback accepts only canonical control namespaces.
 * Keep the existing RELATION cases separate. This WALR key exercises the
 * generic PG lock-mode batch and cancellation engine, not a WALR owner or
 * transport certification. All convert targets are mutually compatible. */
static void
capacity_control_convert_batch(int count, CapacityControlDrain path, bool cancel_pending)
{
	const int32 remote_nodes[] = { 0, 2, 3 };
	const uint32 acquire_opcodes[] = { GES_REQ_OPCODE_REQUEST, GES_REQ_OPCODE_REQUEST_NOWAIT,
									   GES_REQ_OPCODE_CONVERT, GES_REQ_OPCODE_REDECLARE };
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId blocker, holders[32], converts[32];
	ClusterGrdShared *shared;
	BackendType saved_backend = MyBackendType;
	bool found;
	int queued = 0;
	int promoted = 0;
	int expected_grants = cancel_pending ? 0 : count;
	int expected_replies = expected_grants + (path == CAPACITY_CONTROL_LMON_RELEASE ? 1 : 0);
	int fanout_replies;

	HW_CHECK(count > 0 && count <= lengthof(holders));
	capacity_setup(&request, &blocker);
	request.resid = (ClusterResId){ .field1 = 1,
									.type = CLUSTER_WAL_RETENTION_RESID_TYPE,
									.lockmethodid = DEFAULT_LOCKMETHOD };
	UT_ASSERT(cluster_control_request_resid_valid(&request.resid));
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && shared != NULL);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&request.resid)], 1);
	blocker.node_id = 2;
	blocker.request_id += UINT64CONST(0x300000000);
	UT_ASSERT_EQ(capacity_acquire(&request.resid, &blocker), CLUSTER_GRD_GRANT_NOW);
	for (int i = 0; i < count; i++) {
		holders[i] = capacity_holder(i);
		holders[i].node_id = remote_nodes[i % lengthof(remote_nodes)];
		holders[i].request_id += UINT64CONST(0x100000000);
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(
						 &request.resid, &holders[i], holders[i].node_id, holders[i].request_id, 9,
						 GES_REQ_OPCODE_REQUEST, AccessShareLock, NULL, NULL),
					 CLUSTER_GRD_GRANT_NOW);
	}
	for (int i = 0; i < count; i++) {
		ClusterGrdWaiterMeta meta = { 0 };

		converts[i] = holders[i];
		converts[i].request_id += UINT64CONST(0x100000000);
		meta.wait_seq = 25000 + i;
		if (cluster_grd_convert_or_enqueue_meta(&request.resid, converts[i].node_id,
												converts[i].procno, converts[i].cluster_epoch,
												AccessShareLock, ShareLock, converts[i].request_id,
												converts[i].node_id, 9, meta, NULL, NULL)
			== CLUSTER_GRD_CONVERT_ENQUEUED)
			queued++;
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
	}
	UT_ASSERT_EQ(queued, count);
	capacity_expect_queues(1, count + 1, 0, count);
	memset(capacity_replies, 0, sizeof(capacity_replies));
	memset(capacity_dedup_removals, 0, sizeof(capacity_dedup_removals));
	memset(capacity_dedup_records, 0, sizeof(capacity_dedup_records));
	memset(&capacity_last_control_retired, 0, sizeof(capacity_last_control_retired));
	capacity_reply_count = capacity_dedup_remove_count = capacity_dedup_record_count = 0;
	capacity_control_retired_count = master_reply_count = 0;
	hw_reply_observe = capacity_observe_reply;
	hw_dedup_remove_observe = capacity_observe_dedup_remove;
	hw_dedup_record_observe = capacity_observe_dedup_record;
	hw_control_retire_observe = capacity_observe_control_retire;
	MyBackendType = B_LMON;
	if (cancel_pending) {
		for (int i = 0; i < count; i++) {
			LOCKMODE mode = NoLock;

			capacity_retire_control(&request.resid, &converts[i], &holders[i]);
			capacity_retire_control(&request.resid, &converts[i], &holders[i]);
			UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
			UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode));
			UT_ASSERT_EQ(mode, AccessShareLock);
			capacity_expect_queues(1, count + 1, 0, count - i - 1);
			UT_ASSERT_EQ(capacity_reply_count, 0);
			UT_ASSERT_EQ(capacity_dedup_record_count, 0);
		}
	}
	if (path == CAPACITY_CONTROL_LMON_RELEASE) {
		memcpy(master_request.resid, &request.resid, sizeof(request.resid));
		master_request.opcode = GES_REQ_OPCODE_RELEASE;
		master_request.lockmode = RowExclusiveLock;
		master_request.holder_node_id = blocker.node_id;
		master_request.holder_procno = blocker.procno;
		master_request.holder_cluster_epoch_lo = (uint32)blocker.cluster_epoch;
		master_request.holder_cluster_epoch_hi = (uint32)(blocker.cluster_epoch >> 32);
		master_request.holder_request_id_lo = (uint32)blocker.request_id;
		master_request.holder_request_id_hi = (uint32)(blocker.request_id >> 32);
		stage_master_work();
		UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		capacity_expect_grant_reply(&request.resid, &blocker, GES_REQ_OPCODE_RELEASE);
		UT_ASSERT_EQ(capacity_dedup_remove_count, lengthof(acquire_opcodes));
		for (int i = 0; i < lengthof(acquire_opcodes); i++)
			capacity_expect_dedup_key(capacity_dedup_removals, capacity_dedup_remove_count,
									  lengthof(capacity_dedup_removals), &blocker,
									  acquire_opcodes[i], 47);
	} else if (path == CAPACITY_CONTROL_LOCAL_RELEASE) {
		UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&request.resid, &blocker),
					 GES_REJECT_REASON_NONE);
		UT_ASSERT_EQ(capacity_dedup_remove_count, 0);
	} else {
		capacity_retire_control(&request.resid, &blocker, NULL);
		UT_ASSERT_EQ(capacity_dedup_remove_count, 0);
	}
	fanout_replies = capacity_reply_count;
	UT_ASSERT_EQ(capacity_reply_count, expected_replies);
	UT_ASSERT_EQ(master_reply_count, expected_replies);
	UT_ASSERT_EQ(capacity_dedup_record_count, expected_grants);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &blocker, NULL));
	for (int i = 0; i < count; i++) {
		LOCKMODE mode = NoLock;

		if (!cancel_pending) {
			capacity_expect_grant_reply(&request.resid, &converts[i], GES_REQ_OPCODE_CONVERT);
			capacity_expect_dedup_key(capacity_dedup_records, capacity_dedup_record_count,
									  lengthof(capacity_dedup_records), &converts[i],
									  GES_REQ_OPCODE_CONVERT, 9);
			UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &holders[i], NULL));
			if (cluster_grd_holder_mode_by_id(&request.resid, &converts[i], &mode))
				promoted++;
			UT_ASSERT_EQ(mode, ShareLock);
		} else {
			UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
			UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode));
			UT_ASSERT_EQ(mode, AccessShareLock);
		}
	}
	printf("# control_batch path=%d converts=%d canceled=%d promoted=%d replies=%d expected=%d\n",
		   path, count, cancel_pending ? count : 0, promoted, fanout_replies, expected_replies);
	UT_ASSERT_EQ(promoted, expected_grants);
	capacity_expect_queues(1, count, 0, 0);
	for (int i = 0; i < count; i++) {
		LOCKMODE mode = NoLock;

		if (!cancel_pending) {
			/* Canceling a delivered upgrade restores its exact original owner.
			 * Repeated cleanup must neither resurrect the upgrade nor grant anew. */
			capacity_retire_control(&request.resid, &converts[i], &holders[i]);
			capacity_retire_control(&request.resid, &converts[i], &holders[i]);
		}
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
		UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &holders[i], &mode));
		UT_ASSERT_EQ(mode, AccessShareLock);
		capacity_retire_control(&request.resid, &holders[i], NULL);
		capacity_retire_control(&request.resid, &holders[i], NULL);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &holders[i], NULL));
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &converts[i], NULL));
		UT_ASSERT_EQ(capacity_reply_count, expected_replies);
		UT_ASSERT_EQ(capacity_dedup_record_count, expected_grants);
	}
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_grd_pool_used, 0);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	printf("# control_cleanup path=%d converts=%d callbacks=%d entries=%d pool=%zu wfg=%d\n", path,
		   count, capacity_control_retired_count, cluster_grd_entry_count(),
		   (size_t)ut_grd_pool_used, ut_wfg_n);
	hw_reply_observe = NULL;
	hw_dedup_remove_observe = NULL;
	hw_dedup_record_observe = NULL;
	hw_control_retire_observe = NULL;
	MyBackendType = saved_backend;
	MyProc = NULL;
}

UT_TEST(lmon_release_16_control_grants_can_all_be_canceled_and_retired)
{
	capacity_control_convert_batch(16, CAPACITY_CONTROL_LMON_RELEASE, false);
}

UT_TEST(lmon_release_32_control_grants_can_all_be_canceled_and_retired)
{
	capacity_control_convert_batch(32, CAPACITY_CONTROL_LMON_RELEASE, false);
}

UT_TEST(local_release_delivers_and_reclaims_all_16_convert_grants)
{
	capacity_control_convert_batch(16, CAPACITY_CONTROL_LOCAL_RELEASE, false);
}

UT_TEST(local_release_delivers_and_reclaims_all_32_convert_grants)
{
	capacity_control_convert_batch(32, CAPACITY_CONTROL_LOCAL_RELEASE, false);
}

UT_TEST(control_retire_delivers_and_reclaims_all_16_convert_grants)
{
	capacity_control_convert_batch(16, CAPACITY_CONTROL_RETIRE, false);
}

UT_TEST(control_retire_delivers_and_reclaims_all_32_convert_grants)
{
	capacity_control_convert_batch(32, CAPACITY_CONTROL_RETIRE, false);
}

UT_TEST(canceled_32_control_converts_never_grant_after_real_lmon_release)
{
	capacity_control_convert_batch(32, CAPACITY_CONTROL_LMON_RELEASE, true);
}

static Size capacity_oom_min_bytes;
static Size capacity_oom_failed_bytes;
static int capacity_oom_fail_at;
static int capacity_oom_allocations;
static int capacity_oom_matches;
static int capacity_oom_failures;
static int capacity_oom_failed_flags;

static bool
capacity_fail_projection_allocation(Size bytes, int flags)
{
	capacity_oom_allocations++;
	if (bytes < capacity_oom_min_bytes || ++capacity_oom_matches != capacity_oom_fail_at)
		return false;
	capacity_oom_failures++;
	capacity_oom_failed_bytes = bytes;
	capacity_oom_failed_flags = flags;
	return true; /* A's allocator decides ERROR versus NO_OOM NULL. */
}

static void
capacity_expect_no_pin_leak(const ClusterResId *resid)
{
	ClusterGrdEntry *entry = NULL;

	UT_ASSERT_EQ(cluster_grd_entry_lookup_or_create(resid, false, &entry), CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT(entry != NULL);
	if (entry != NULL) {
		/* Only this inspection pin may remain. Never dereference after release. */
		UT_ASSERT_EQ(cluster_grd_entry_pin_count(entry), 1);
		cluster_grd_entry_release(entry);
	}
}

/* Fail only the allocator boundary, after all real queues and old WFG edges
 * exist. Both grants and their outbound replies must survive projection OOM.
 * The large shape grants a convert plus one FIFO waiter and leaves 17 holders
 * and 32 waiters, so each snapshot vector must exceed its inline capacity. */
static void
capacity_projection_oom(CapacityControlDrain path, int snapshot_allocation)
{
	const int32 remote_nodes[] = { 0, 2, 3 };
	const uint32 acquire_opcodes[] = { GES_REQ_OPCODE_REQUEST, GES_REQ_OPCODE_REQUEST_NOWAIT,
									   GES_REQ_OPCODE_CONVERT, GES_REQ_OPCODE_REDECLARE };
	bool large = snapshot_allocation != 0;
	int nholders = large ? 16 : 0;
	int nwaiters = large ? 33 : 1;
	int ngrants = large ? 2 : 1;
	int nreplies = ngrants + (path == CAPACITY_CONTROL_LMON_RELEASE ? 1 : 0);
	ClusterLockAcquireRequest request;
	ClusterGrdHolderId blocker, holders[16], waiters[33], convert = { 0 };
	ClusterGrdShared *shared;
	BackendType saved_backend = MyBackendType;
	sigjmp_buf *saved_exception_stack = PG_exception_stack;
	ErrorContextCallback *saved_context_stack = error_context_stack;
	volatile bool caught = false;
	volatile bool returned = false;
	bool found;
	LOCKMODE mode = NoLock;

	HW_CHECK(path == CAPACITY_CONTROL_LMON_RELEASE || path == CAPACITY_CONTROL_RETIRE);
	HW_CHECK(snapshot_allocation >= 0 && snapshot_allocation <= 2);
	capacity_setup(&request, &blocker);
	request.resid = (ClusterResId){ .field1 = 1,
									.type = CLUSTER_WAL_RETENTION_RESID_TYPE,
									.lockmethodid = DEFAULT_LOCKMETHOD };
	UT_ASSERT(cluster_control_request_resid_valid(&request.resid));
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	HW_CHECK(found && shared != NULL);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&request.resid)], 1);
	blocker.node_id = 2;
	blocker.request_id += UINT64CONST(0x300000000);
	UT_ASSERT_EQ(capacity_acquire(&request.resid, &blocker), CLUSTER_GRD_GRANT_NOW);
	for (int i = 0; i < nholders; i++) {
		holders[i] = capacity_holder(i);
		holders[i].node_id = remote_nodes[i % lengthof(remote_nodes)];
		holders[i].request_id += UINT64CONST(0x100000000);
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant(
						 &request.resid, &holders[i], holders[i].node_id, holders[i].request_id, 9,
						 GES_REQ_OPCODE_REQUEST, AccessShareLock, NULL, NULL),
					 CLUSTER_GRD_GRANT_NOW);
	}
	if (large) {
		ClusterGrdWaiterMeta meta = { 0 };

		convert = holders[0];
		convert.request_id += UINT64CONST(0x100000000);
		meta.wait_seq = 35000;
		UT_ASSERT_EQ(cluster_grd_convert_or_enqueue_meta(
						 &request.resid, convert.node_id, convert.procno, convert.cluster_epoch,
						 AccessShareLock, ShareLock, convert.request_id, convert.node_id, 9, meta,
						 NULL, NULL),
					 CLUSTER_GRD_CONVERT_ENQUEUED);
	}
	for (int i = 0; i < nwaiters; i++) {
		ClusterGrdWaiterMeta meta = { 0 };

		waiters[i] = capacity_holder(100 + i);
		waiters[i].node_id = remote_nodes[i % lengthof(remote_nodes)];
		waiters[i].request_id += UINT64CONST(0x400000000);
		meta.wait_seq = 36000 + i;
		UT_ASSERT_EQ(cluster_grd_entry_enqueue_or_grant_meta(
						 &request.resid, &waiters[i], waiters[i].node_id, waiters[i].request_id,
						 meta, 9, GES_REQ_OPCODE_REQUEST, ShareLock, NULL, NULL),
					 CLUSTER_GRD_ENQUEUED_WAITER);
		UT_ASSERT(ut_wfg_has_edge(waiters[i].node_id, waiters[i].procno, waiters[i].cluster_epoch,
								  waiters[i].request_id, blocker.node_id, blocker.procno,
								  blocker.cluster_epoch, blocker.request_id));
	}
	UT_ASSERT_EQ(ut_wfg_n, nwaiters + (large ? 1 : 0));
	capacity_expect_queues(1, nholders + 1, nwaiters, large ? 1 : 0);
	capacity_expect_no_pin_leak(&request.resid);
	memset(capacity_replies, 0, sizeof(capacity_replies));
	memset(capacity_dedup_removals, 0, sizeof(capacity_dedup_removals));
	memset(capacity_dedup_records, 0, sizeof(capacity_dedup_records));
	capacity_reply_count = capacity_dedup_remove_count = capacity_dedup_record_count = 0;
	capacity_control_retired_count = master_reply_count = 0;
	hw_reply_observe = capacity_observe_reply;
	hw_dedup_remove_observe = capacity_observe_dedup_remove;
	hw_dedup_record_observe = capacity_observe_dedup_record;
	hw_control_retire_observe = capacity_observe_control_retire;
	MyBackendType = B_LMON;
	memcpy(master_request.resid, &request.resid, sizeof(request.resid));
	master_request.opcode = GES_REQ_OPCODE_RELEASE;
	master_request.lockmode = RowExclusiveLock;
	master_request.holder_node_id = blocker.node_id;
	master_request.holder_procno = blocker.procno;
	master_request.holder_cluster_epoch_lo = (uint32)blocker.cluster_epoch;
	master_request.holder_cluster_epoch_hi = (uint32)(blocker.cluster_epoch >> 32);
	master_request.holder_request_id_lo = (uint32)blocker.request_id;
	master_request.holder_request_id_hi = (uint32)(blocker.request_id >> 32);
	/* The small case fails any allocation, covering the former departed list.
	 * For each large snapshot failure, allow that bounded identity list so the
	 * old product reaches both later projection allocations independently. */
	capacity_oom_min_bytes = large ? (Size)(ngrants + 1) * sizeof(ClusterGrdHolderId) + 1 : 0;
	capacity_oom_fail_at = large ? snapshot_allocation : 1;
	capacity_oom_allocations = capacity_oom_matches = capacity_oom_failures = 0;
	capacity_oom_failed_bytes = 0;
	capacity_oom_failed_flags = 0;
	ut_grd_alloc_fail = capacity_fail_projection_allocation;
	PG_TRY();
	{
		if (path == CAPACITY_CONTROL_LMON_RELEASE) {
			stage_master_work();
			UT_ASSERT_EQ(cluster_ges_lmon_drain_work_queue(), 1);
		} else
			capacity_retire_control(&request.resid, &blocker, NULL);
		returned = true;
	}
	PG_CATCH();
	{
		caught = true;
		FlushErrorState();
	}
	PG_END_TRY();
	ut_grd_alloc_fail = NULL;
	printf("# projection_oom path=%d snapshot=%d allocs=%d failures=%d bytes=%zu flags=%d "
		   "caught=%d returned=%d replies=%d expected=%d wfg=%d\n",
		   path, snapshot_allocation, capacity_oom_allocations, capacity_oom_failures,
		   (size_t)capacity_oom_failed_bytes, capacity_oom_failed_flags, caught, returned,
		   capacity_reply_count, nreplies, ut_wfg_n);
	UT_ASSERT(!caught);
	UT_ASSERT(returned);
	UT_ASSERT(PG_exception_stack == saved_exception_stack);
	UT_ASSERT(error_context_stack == saved_context_stack);
	if (large) {
		UT_ASSERT_EQ(capacity_oom_failures, 1);
		UT_ASSERT_EQ(capacity_oom_matches, snapshot_allocation);
		UT_ASSERT((capacity_oom_failed_flags & MCXT_ALLOC_NO_OOM) != 0);
	}
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &blocker, NULL));
	UT_ASSERT_EQ(capacity_reply_count, nreplies);
	UT_ASSERT_EQ(master_reply_count, nreplies);
	UT_ASSERT_EQ(capacity_dedup_record_count, ngrants);
	capacity_expect_grant_reply(&request.resid, &waiters[0], GES_REQ_OPCODE_REQUEST);
	capacity_expect_dedup_key(capacity_dedup_records, capacity_dedup_record_count,
							  lengthof(capacity_dedup_records), &waiters[0], GES_REQ_OPCODE_REQUEST,
							  9);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &waiters[0], &mode));
	UT_ASSERT_EQ(mode, ShareLock);
	if (large) {
		capacity_expect_grant_reply(&request.resid, &convert, GES_REQ_OPCODE_CONVERT);
		capacity_expect_dedup_key(capacity_dedup_records, capacity_dedup_record_count,
								  lengthof(capacity_dedup_records), &convert,
								  GES_REQ_OPCODE_CONVERT, 9);
		UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &holders[0], NULL));
		mode = NoLock;
		UT_ASSERT(cluster_grd_holder_mode_by_id(&request.resid, &convert, &mode));
		UT_ASSERT_EQ(mode, ShareLock);
	}
	if (path == CAPACITY_CONTROL_LMON_RELEASE) {
		capacity_expect_grant_reply(&request.resid, &blocker, GES_REQ_OPCODE_RELEASE);
		UT_ASSERT_EQ(capacity_dedup_remove_count, lengthof(acquire_opcodes));
		for (int i = 0; i < lengthof(acquire_opcodes); i++)
			capacity_expect_dedup_key(capacity_dedup_removals, capacity_dedup_remove_count,
									  lengthof(capacity_dedup_removals), &blocker,
									  acquire_opcodes[i], 47);
	} else {
		UT_ASSERT_EQ(capacity_control_retired_count, 1);
		UT_ASSERT_EQ(capacity_dedup_remove_count, 0);
	}
	capacity_expect_queues(1, nholders + 1, nwaiters - 1, 0);
	/* All old edges must be gone, including identities beyond the first chunk.
	 * No GRD cleanup or explicit graph reset has occurred since fault injection. */
	for (int i = 0; i < nwaiters; i++) {
		UT_ASSERT_EQ(ut_wfg_count_waiter(waiters[i].node_id, waiters[i].procno,
										 waiters[i].cluster_epoch, waiters[i].request_id),
					 0);
		if (i > 0)
			UT_ASSERT(!cluster_grd_holder_mode_by_id(&request.resid, &waiters[i], NULL));
	}
	UT_ASSERT_EQ(ut_wfg_n, 0);
	capacity_expect_no_pin_leak(&request.resid);
	/* Cancel remaining queues before freeing holders, so RED cleanup cannot
	 * grant additional requests and hide the missing original notification. */
	for (int i = 1; i < nwaiters; i++)
		UT_ASSERT_EQ(cluster_grd_cancel_waiter_by_id(&request.resid, &waiters[i]),
					 CLUSTER_GRD_ENTRY_OK);
	if (large) {
		(void)cluster_grd_cancel_convert_by_id(&request.resid, &convert, 35000);
		capacity_release_present(&request.resid, &convert);
	}
	for (int i = 0; i < nholders; i++)
		capacity_release_present(&request.resid, &holders[i]);
	capacity_release_present(&request.resid, &waiters[0]);
	capacity_release_present(&request.resid, &blocker);
	capacity_expect_counts(0, 0);
	capacity_expect_stop(CLUSTER_NORMAL_STOP_READY);
	UT_ASSERT_EQ(ut_grd_pool_used, 0);
	UT_ASSERT_EQ(ut_wfg_n, 0);
	hw_reply_observe = NULL;
	hw_dedup_remove_observe = NULL;
	hw_dedup_record_observe = NULL;
	hw_control_retire_observe = NULL;
	MyBackendType = saved_backend;
	MyProc = NULL;
}

UT_TEST(real_lmon_release_oom_never_loses_a_granted_waiter_reply)
{
	capacity_projection_oom(CAPACITY_CONTROL_LMON_RELEASE, 0);
}

UT_TEST(control_retire_oom_never_loses_a_granted_waiter_reply)
{
	capacity_projection_oom(CAPACITY_CONTROL_RETIRE, 0);
}

UT_TEST(real_lmon_release_holder_snapshot_oom_delivers_grants_and_retracts_all_old_edges)
{
	capacity_projection_oom(CAPACITY_CONTROL_LMON_RELEASE, 1);
}

UT_TEST(real_lmon_release_waiter_snapshot_oom_delivers_grants_and_retracts_all_old_edges)
{
	capacity_projection_oom(CAPACITY_CONTROL_LMON_RELEASE, 2);
}

UT_TEST(control_retire_holder_snapshot_oom_delivers_grants_and_retracts_all_old_edges)
{
	capacity_projection_oom(CAPACITY_CONTROL_RETIRE, 1);
}

UT_TEST(control_retire_waiter_snapshot_oom_delivers_grants_and_retracts_all_old_edges)
{
	capacity_projection_oom(CAPACITY_CONTROL_RETIRE, 2);
}

int
main(void)
{
	MyBackendType = B_BACKEND;
	setvbuf(stdout, NULL, _IONBF, 0);
	alarm(30); /* Same standalone watchdog; no product deadline is changed. */
	UT_PLAN(35);
	UT_RUN(first_grd_attach_preserves_temporary_error_cleanup_scope);
	UT_RUN(compatible_16_control_releases_exact_owners);
	UT_RUN(compatible_17_owners_are_all_granted);
	UT_RUN(compatible_32_owners_are_all_granted);
	UT_RUN(compatible_256_owners_are_all_granted);
	UT_RUN(real_lmon_grants_the_17th_compatible_owner);
	UT_RUN(conflict_snapshot_and_wfg_include_all_32_owners);
	UT_RUN(nowait_conflict_never_adds_waiter_or_wfg_edge);
	UT_RUN(release_and_reuse_preserve_each_exact_identity);
	UT_RUN(waiters_32_keep_fifo_after_exact_cancellation);
	UT_RUN(converts_12_keep_drain_priority_after_exact_cancel);
	UT_RUN(reservations_32_survive_s3_s5_and_exact_s7_cancel);
	UT_RUN(real_lmon_bast_16_control);
	UT_RUN(real_lmon_bast_reaches_all_32_exact_remote_owners);
	UT_RUN(real_lmon_nowait_sends_no_bast);
	UT_RUN(real_lmon_bast_excludes_compatible_owner);
	UT_RUN(pool_full_never_partially_grants_holder_and_release_reuses_bytes);
	UT_RUN(pool_full_never_partially_enqueues_waiter_and_release_reuses_bytes);
	UT_RUN(pool_full_keeps_convert_original_holder_and_release_reuses_bytes);
	UT_RUN(pool_full_never_orphans_reservation_and_release_reuses_bytes);
	UT_RUN(real_lmon_release_delivers_all_16_compatible_convert_grants);
	UT_RUN(real_lmon_release_delivers_all_32_compatible_convert_grants);
	UT_RUN(lmon_release_16_control_grants_can_all_be_canceled_and_retired);
	UT_RUN(lmon_release_32_control_grants_can_all_be_canceled_and_retired);
	UT_RUN(local_release_delivers_and_reclaims_all_16_convert_grants);
	UT_RUN(local_release_delivers_and_reclaims_all_32_convert_grants);
	UT_RUN(control_retire_delivers_and_reclaims_all_16_convert_grants);
	UT_RUN(control_retire_delivers_and_reclaims_all_32_convert_grants);
	UT_RUN(canceled_32_control_converts_never_grant_after_real_lmon_release);
	UT_RUN(real_lmon_release_oom_never_loses_a_granted_waiter_reply);
	UT_RUN(control_retire_oom_never_loses_a_granted_waiter_reply);
	UT_RUN(real_lmon_release_holder_snapshot_oom_delivers_grants_and_retracts_all_old_edges);
	UT_RUN(real_lmon_release_waiter_snapshot_oom_delivers_grants_and_retracts_all_old_edges);
	UT_RUN(control_retire_holder_snapshot_oom_delivers_grants_and_retracts_all_old_edges);
	UT_RUN(control_retire_waiter_snapshot_oom_delivers_grants_and_retracts_all_old_edges);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
