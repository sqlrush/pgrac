/*-------------------------------------------------------------------------
 * test_cluster_ko_origin_marker.c
 *    Native origin COMMIT publication and retained DROP identity.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Real KO producer/result, native commit-tail and call/AX owners. GES,
 * publication transport, SPACE durability and buffer cleanup are fixtures.
 *-------------------------------------------------------------------------
 */
int ko_stop_fixture_main(void);
#define main ko_stop_fixture_main
#include "test_cluster_ko_stop.c"
#undef main

static bool publication_error;
static unsigned publication_fault;
static bool open_at_publication, closed_before_deletes;
static ClusterKoSharedMessageV3 origin_request;

static KoDropIdentity *
origin_identity(void)
{
	for (unsigned i = 0; i < KO_DROP_IDENTITY_CAPACITY; i++) {
		KoDropIdentity *entry = &storage.drop_identities[i];
		if (entry->state != KO_DROP_IDENTITY_FREE
			&& memcmp(&entry->request, &origin_request, sizeof(origin_request)) == 0)
			return entry;
	}
	return NULL;
}

static bool
origin_io_allowed(void)
{
	ClusterKoDirectoryIoV3 io;
	bool allowed = cluster_ko_directory_io_begin_v3(origin_request.base.key.locator, &io);
	if (allowed)
		UT_ASSERT(cluster_ko_directory_io_end_v3(&io));
	return allowed;
}

static void
origin_publication(bool commit)
{
	AtEOXact_Inval(commit);
	open_at_publication = origin_io_allowed();
	if (publication_error)
		pg_re_throw();
	if (publication_fault != 0) {
		ClusterKoNativeContinuation *work
			= dsa_get_address(ko_native_area, postcommit_owner->native_unready);
		switch (publication_fault) {
		case 1: work->context.request_done_peers = 0; break;
		case 2: work->context.request_v3.base.batch_id++; break;
		case 3: work->context.terminal.record_end++; break;
		case 4: work->context.structure[0] ^= 1; break;
		case 5: work->context.peer_boots[1]++; break;
		}
	}
}

static void
origin_pending_deletes(bool commit)
{
	closed_before_deletes = !origin_io_allowed();
	smgrDoPendingDeletes(commit);
}

static void
origin_native_tail(void)
{
	bool is_parallel_worker = false;
#define AtEOXact_Inval origin_publication
#define smgrDoPendingDeletes origin_pending_deletes
#include "test_cluster_ko_postcommit.inc"
#undef smgrDoPendingDeletes
#undef AtEOXact_Inval
}

static ClusterKoCompletionV2 *
prepare_origin(uint16 operation)
{
	ClusterKoCompletionV2 *owner = v3_native_structure_owner(operation), *claimed = NULL;
	bool drop = operation == CLUSTER_KO_SHARED_V3_OPERATION_DROP;

	origin_request = owner->barrier_request_v3;
	UT_ASSERT(cluster_ko_shared_claim_structure(&space_identity,
		drop ? CLUSTER_SPACE_WAL_TOMBSTONE : CLUSTER_SPACE_WAL_TRUNCATE, &claimed));
	UT_ASSERT(claimed == owner);
	v3_structure_record(drop, space_identity.operation, &postcommit_binding, postcommit_wal);
	UT_ASSERT(cluster_ko_shared_observe_space_v2(owner, &postcommit_binding,
		postcommit_wal, sizeof(postcommit_wal)));
	if (!drop)
		UT_ASSERT(cluster_ko_shared_observe_truncate_v2(owner));
	postcommit_owner = owner;
	postcommit_drop = drop;
	postcommit_error = postcommit_scope_changed = postcommit_callback_error = false;
	postcommit_deletes = postcommit_phases = 0;
	postcommit_native_count = 1;
	postcommit_storage_consumer = NULL;
	postcommit_buffer_error = false;
	native_commit_active = true;
	publication_error = false;
	publication_fault = 0;
	open_at_publication = closed_before_deletes = false;
	return owner;
}

static void
origin_release_ax(const ClusterKoSharedMessageV3 *request)
{
	LOCKTAG tag;
	SET_LOCKTAG_RELATION(tag, request->ax_resid.field1, request->ax_resid.field2);
	UT_ASSERT(cluster_ko_ax_release_handoff_v3(&tag, AccessExclusiveLock, &request->ax_holder));
	release_poll_result = CLUSTER_GES_REDECLARE_CONFIRMED;
	for (unsigned i = 0; i < CLUSTER_KO_SHARED_CAPACITY / 8; i++)
		shared_send_tick();
}

UT_TEST(test_origin_reserves_before_request_and_abort_waits_for_s6)
{
	ClusterKoCompletionV2 *owner = prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	KoDropIdentity *identity = origin_identity();

	UT_ASSERT(identity != NULL && identity->state == KO_DROP_IDENTITY_RESERVED);
	UT_ASSERT(origin_io_allowed());
	cluster_ko_shared_release_v2(&owner);
	UT_ASSERT(origin_identity() != NULL);
	UT_ASSERT(origin_io_allowed());
	origin_release_ax(&origin_request);
	UT_ASSERT(origin_identity() == NULL);
	UT_ASSERT_EQ(release_poll_calls, 1);
}

UT_TEST(test_origin_identity_capacity_refuses_before_marker_and_peer_send)
{
	RelationData rel;
	Oid saved = MyDatabaseId;
	volatile bool caught = false;
	v3_native_structure_setup(&rel);
	MyDatabaseId = rel.rd_locator.dbOid;
	storage.drop_free = 0;
	expecting_error = true;
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
	expecting_error = false;
	MyDatabaseId = saved;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(marker_registrations, 0);
	UT_ASSERT_EQ(barrier_requests, 0);
	UT_ASSERT_EQ(native_allocated, 0);
	native_fixture_shared_memory_end();
}

UT_TEST(test_origin_publication_closes_before_pending_delete_and_survives_ax_release)
{
	(void)prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	origin_native_tail();
	UT_ASSERT(open_at_publication && closed_before_deletes);
	UT_ASSERT(cluster_ko_directory_drop_committed_v3(origin_request.base.key.locator));
	UT_ASSERT(!origin_io_allowed());
	origin_release_ax(&origin_request);
	UT_ASSERT(!origin_io_allowed());
	UT_ASSERT(origin_identity() != NULL);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	UT_ASSERT(!origin_io_allowed());
	native_fixture_shared_memory_end();
}

UT_TEST(test_origin_inflight_call_blocks_promotion_without_reopening)
{
	ClusterKoDirectoryIoV3 io;
	(void)prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	UT_ASSERT(cluster_ko_directory_io_begin_v3(origin_request.base.key.locator, &io));
	origin_native_tail();
	UT_ASSERT(closed_before_deletes);
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PENDING);
	UT_ASSERT(!origin_io_allowed());
	UT_ASSERT_EQ(storage.native_waiting, 1);
	UT_ASSERT(cluster_ko_directory_io_end_v3(&io));
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	UT_ASSERT(!origin_io_allowed());
	native_fixture_shared_memory_end();
}

UT_TEST(test_failed_publication_cannot_manufacture_local_marker)
{
	volatile bool caught = false;
	(void)prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
	publication_error = true;
	PG_TRY(); { origin_native_tail(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && open_at_publication);
	UT_ASSERT(!cluster_ko_directory_drop_committed_v3(origin_request.base.key.locator));
	UT_ASSERT(origin_io_allowed());
	UT_ASSERT_EQ(postcommit_deletes, 0);
	UT_ASSERT_EQ(storage.native_waiting, 1);
	native_fixture_shared_memory_end();
}

UT_TEST(test_publication_exact_continuation_drift_retains_failed_owner)
{
	for (unsigned fault = 1; fault <= 5; fault++) {
		(void)prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		publication_fault = fault;
		origin_native_tail();
		UT_ASSERT(!cluster_ko_directory_drop_committed_v3(origin_request.base.key.locator));
		UT_ASSERT(origin_io_allowed());
		UT_ASSERT(storage.native_recovery_required);
		MyBackendType = B_CHECKPOINTER;
		UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_INVALID);
		native_fixture_shared_memory_end();
	}
}

UT_TEST(test_inplace_truncate_does_not_get_drop_identity_or_gate)
{
	(void)prepare_origin(CLUSTER_KO_SHARED_V3_OPERATION_TRUNCATE);
	UT_ASSERT(origin_identity() == NULL);
	origin_native_tail();
	UT_ASSERT(open_at_publication && !closed_before_deletes);
	UT_ASSERT(!cluster_ko_directory_drop_committed_v3(origin_request.base.key.locator));
	UT_ASSERT(origin_io_allowed());
	MyBackendType = B_CHECKPOINTER;
	UT_ASSERT_EQ(cluster_ko_shared_native_promote_v2(), CLUSTER_KO_STRUCTURE_PROGRESS);
	native_fixture_shared_memory_end();
}

UT_TEST(test_seventy_two_native_aborts_reuse_reservations_after_exact_s6)
{
	RelationData rel;
	Oid saved = MyDatabaseId;
	v3_native_structure_setup(&rel);
	multiple_barriers = true;
	MyDatabaseId = rel.rd_locator.dbOid;
	for (unsigned i = 0; i < 72; i++) {
		ClusterKoCompletionV2 *owner;

		rel.rd_locator.relNumber++;
		space_identity.key.locator = rel.rd_locator;
		/* The registration fixture counts sends within one operation. Do
		 * not reset any shared owner, identity table or release state. */
		barrier_requests = 0;
		cluster_ko_flush_and_wait_ack_relation(&rel, RELPERSISTENCE_PERMANENT,
			CLUSTER_KO_SHARED_V3_OPERATION_DROP);
		UT_ASSERT_EQ(barrier_requests, 1);
		UT_ASSERT_EQ(marker_registrations, i + 1);
		owner = ko_completions;
		UT_ASSERT(owner != NULL);
		origin_request = owner->barrier_request_v3;
		UT_ASSERT(origin_identity() != NULL);
		cluster_ko_shared_release_v2(&owner);
		UT_ASSERT(origin_identity() != NULL);
		origin_release_ax(&origin_request);
		UT_ASSERT(origin_identity() == NULL);
		UT_ASSERT(origin_io_allowed());
		UT_ASSERT_EQ(completion_allocations + native_allocated, 0);
	}
	UT_ASSERT_EQ(release_poll_calls, 72);
	MyDatabaseId = saved;
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_origin_reserves_before_request_and_abort_waits_for_s6);
	UT_RUN(test_origin_identity_capacity_refuses_before_marker_and_peer_send);
	UT_RUN(test_origin_publication_closes_before_pending_delete_and_survives_ax_release);
	UT_RUN(test_origin_inflight_call_blocks_promotion_without_reopening);
	UT_RUN(test_failed_publication_cannot_manufacture_local_marker);
	UT_RUN(test_publication_exact_continuation_drift_retains_failed_owner);
	UT_RUN(test_inplace_truncate_does_not_get_drop_identity_or_gate);
	UT_RUN(test_seventy_two_native_aborts_reuse_reservations_after_exact_s6);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
