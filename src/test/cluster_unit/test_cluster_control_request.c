/*-------------------------------------------------------------------------
 * test_cluster_control_request.c -- real shared request ownership and codec.
 * Only allocation/LWLock primitives are replaced by a single-process fixture.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_control_request.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_wal_retention.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static void *memory;
static LWLock *locked;
static ClusterControlRequestOwner owner;
static ClusterControlRequestKey key;
static ClusterControlRequestCut cut;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *found)
{
	*found = memory != NULL;
	if (memory == NULL)
		memory = calloc(1, size);
	Assert(memory != NULL);
	return memory;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	UT_ASSERT_EQ(region->size_fn(), cluster_control_request_shmem_size());
}

int
LWLockNewTrancheId(void)
{
	return 200;
}
void
LWLockRegisterTranche(int id pg_attribute_unused(), const char *name pg_attribute_unused())
{}
void
LWLockInitialize(LWLock *lock, int id)
{
	lock->tranche = id;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode pg_attribute_unused())
{
	Assert(locked == NULL);
	locked = lock;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	Assert(locked == lock);
	locked = NULL;
}

static void
reset(void)
{
	free(memory);
	memory = NULL;
	cluster_control_request_shmem_init();
	memset(&key, 0, sizeof(key));
	key.resid.type = CLUSTER_CF_RESID_TYPE;
	key.resid.lockmethodid = DEFAULT_LOCKMETHOD;
	key.holder.node_id = 1;
	key.holder.procno = 12;
	key.holder.cluster_epoch = 8;
	key.holder.request_id = 101;
	UT_ASSERT(cluster_control_request_owner_init(12, 1000, &owner));
	memset(&cut, 0, sizeof(cut));
	cut.epoch = 9;
	cut.generation = 9001;
	cut.master = 2;
}

static ClusterControlRequestHandle
registration(uint64 previous)
{
	ClusterControlRequestHandle handle = { 0 };

	UT_ASSERT(cluster_control_request_register(&key, ExclusiveLock, &owner, previous,
											   previous ? ShareLock : NoLock, &handle));
	return handle;
}

UT_TEST(only_canonical_control_resources_are_registered)
{
	ClusterResId resource;

	reset();
	resource = key.resid;
	UT_ASSERT(cluster_control_request_resid_valid(&resource));
	resource.field1 = 1;
	UT_ASSERT(!cluster_control_request_resid_valid(&resource));
	resource.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	UT_ASSERT(cluster_control_request_resid_valid(&resource));
	resource.field1 = CLUSTER_WAL_RETENTION_MAX_THREADS + 1;
	UT_ASSERT(!cluster_control_request_resid_valid(&resource));
	resource.type = CLUSTER_IR_RESID_TYPE;
	resource.field1 = 1;
	UT_ASSERT(!cluster_control_request_resid_valid(&resource));
	resource.field2 = 17;
	UT_ASSERT(cluster_control_request_resid_valid(&resource));
	resource.field4 = 1;
	UT_ASSERT(!cluster_control_request_resid_valid(&resource));
}

UT_TEST(producer_fence_is_one_way_and_unknown_is_not_permission)
{
	ClusterControlRequestHandle handle;
	ClusterControlRetireMessage message;
	uint64 driver;

	reset();
	UT_ASSERT(!cluster_control_request_send_allowed(&key));
	handle = registration(0);
	UT_ASSERT(cluster_control_request_send_allowed(&key));
	UT_ASSERT(cluster_control_request_mark_held(&handle, &owner));
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	UT_ASSERT(!cluster_control_request_mark_held(&handle, &owner));
	UT_ASSERT(!cluster_control_request_send_allowed(&key));
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	driver = cluster_control_request_driver_start();
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &message));
	UT_ASSERT_EQ(message.key.holder.cluster_epoch, 8);
	UT_ASSERT_EQ(message.cleanup_epoch, 9);
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&message, 2, driver, &cut));
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
	UT_ASSERT(!cluster_control_request_send_allowed(&key));
	UT_ASSERT(!cluster_control_request_mark_held(&handle, &owner));
}

UT_TEST(retry_and_wrong_ack_do_not_erase_cleanup_debt)
{
	ClusterControlRequestHandle handle;
	ClusterControlRetireMessage message, wrong, repeat;
	ClusterControlRequestView view;
	uint64 driver;
	int i;

	reset();
	handle = registration(0);
	driver = cluster_control_request_driver_start();
	UT_ASSERT(!cluster_control_request_claim(&handle, driver, &cut, &message));
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &message));
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &repeat));
	UT_ASSERT_EQ(repeat.exchange_id, message.exchange_id);
	for (i = 0; i < 8; i++) {
		wrong = message;
		wrong.verb = CLUSTER_CONTROL_RETIRED;
		if (i == 0)
			wrong.key.holder.node_id++;
		if (i == 1)
			wrong.key.holder.procno++;
		if (i == 2)
			wrong.key.holder.cluster_epoch--;
		if (i == 3)
			wrong.key.holder.request_id++;
		if (i == 4)
			wrong.key.resid.field1++;
		if (i == 5)
			wrong.cleanup_epoch++;
		if (i == 6)
			wrong.exchange_id++;
		if (i == 7)
			wrong.previous_request++;
		UT_ASSERT(!cluster_control_request_ack(&wrong, 2, driver, &cut));
	}
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(!cluster_control_request_ack(&message, 3, driver, &cut));
	message.verb = CLUSTER_CONTROL_RETIRE_RETRY;
	UT_ASSERT(cluster_control_request_ack(&message, 2, driver, &cut));
	UT_ASSERT(!cluster_control_request_forget(&handle, &owner, &cut));
	UT_ASSERT(cluster_control_request_snapshot(&handle, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
}

UT_TEST(cut_and_driver_changes_invalidate_old_observations)
{
	ClusterControlRequestHandle handle;
	ClusterControlRetireMessage old, next;
	uint64 driver, restarted;

	reset();
	handle = registration(0);
	UT_ASSERT(cluster_control_request_abandon(&handle, &owner));
	driver = cluster_control_request_driver_start();
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &old));
	old.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&old, 2, driver, &cut));
	cut.generation++;
	UT_ASSERT(!cluster_control_request_forget(&handle, &owner, &cut));
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &next));
	UT_ASSERT_NE(next.exchange_id, old.exchange_id);
	UT_ASSERT(!cluster_control_request_ack(&old, 2, driver, &cut));
	restarted = cluster_control_request_driver_start();
	UT_ASSERT_NE(restarted, driver);
	UT_ASSERT(!cluster_control_request_claim(&handle, driver, &cut, &old));
	UT_ASSERT(cluster_control_request_claim(&handle, restarted, &cut, &old));
	UT_ASSERT_NE(next.exchange_id, old.exchange_id);
	next.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(!cluster_control_request_ack(&next, 2, driver, &cut));
}

UT_TEST(exit_handoff_does_not_infer_abandonment_from_active_state)
{
	ClusterControlRequestHandle handle;
	ClusterControlRequestOwner reused;
	ClusterControlRequestView view;
	ClusterControlRetireMessage message;
	uint64 driver;

	reset();
	handle = registration(100);
	driver = cluster_control_request_driver_start();
	UT_ASSERT(!cluster_control_request_claim(&handle, driver, &cut, &message));
	UT_ASSERT(cluster_control_request_owner_init(owner.procno, owner.pid, &reused));
	cluster_control_request_owner_exit(&reused);
	UT_ASSERT(cluster_control_request_send_allowed(&key));
	cluster_control_request_owner_exit(&owner);
	UT_ASSERT(!cluster_control_request_send_allowed(&key));
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &message));
	UT_ASSERT_EQ(message.previous_request, 0);
	UT_ASSERT_EQ(message.previous_mode, NoLock);
	UT_ASSERT(cluster_control_request_snapshot(&handle, &view));
	UT_ASSERT(view.owner_exited);
}

UT_TEST(capacity_refusal_never_overwrites_an_owned_record)
{
	ClusterControlRequestHandle first = { 0 }, handle = { 0 };
	ClusterControlRequestView view;
	int i;

	reset();
	for (i = 0; i < CLUSTER_CONTROL_REQUEST_CAPACITY; i++) {
		key.holder.request_id = i + 1;
		handle = registration(0);
		if (i == 0)
			first = handle;
	}
	key.holder.request_id++;
	UT_ASSERT(!cluster_control_request_register(&key, ExclusiveLock, &owner, 0, NoLock, &handle));
	UT_ASSERT(cluster_control_request_snapshot(&first, &view));
	UT_ASSERT_EQ(view.message.key.holder.request_id, 1);
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ACTIVE);
}

UT_TEST(wire_is_exact_sized_little_endian_and_rejects_malformed_identity)
{
	ClusterControlRetireMessage message = { 0 }, decoded;
	uint8 wire[CLUSTER_CONTROL_RETIRE_BYTES];
	int i;

	reset();
	message.key = key;
	message.cleanup_epoch = cut.epoch;
	message.exchange_id = UINT64_C(0x102030405060708);
	message.verb = CLUSTER_CONTROL_RETIRE;
	UT_ASSERT(cluster_control_retire_encode(&message, wire));
	UT_ASSERT_EQ(wire[0], 19);
	UT_ASSERT_EQ(wire[56], 8);
	UT_ASSERT_EQ(wire[63], 1);
	UT_ASSERT(cluster_control_retire_decode(wire, sizeof(wire), &decoded));
	UT_ASSERT_EQ(decoded.exchange_id, message.exchange_id);
	UT_ASSERT_EQ(decoded.key.holder.request_id, key.holder.request_id);
	UT_ASSERT(!cluster_control_retire_decode(wire, sizeof(wire) - 1, &decoded));
	for (i = 0; i < 5; i++) {
		UT_ASSERT(cluster_control_retire_encode(&message, wire));
		if (i == 0)
			wire[0]++;
		if (i == 1)
			wire[76]++;
		if (i == 2)
			wire[4] = 0;
		if (i == 3)
			memset(wire + 24, 0, 8);
		if (i == 4)
			memset(wire + 48, 0, 8);
		UT_ASSERT(!cluster_control_retire_decode(wire, sizeof(wire), &decoded));
	}
}

static void
upgrade_pair(ClusterControlRequestHandle *original, ClusterControlRequestHandle *replacement)
{
	key.resid.type = CLUSTER_WAL_RETENTION_RESID_TYPE;
	key.resid.field1 = 2;
	cut.epoch = key.holder.cluster_epoch;
	UT_ASSERT(cluster_control_request_register(&key, ShareLock, &owner, 0, NoLock, original));
	UT_ASSERT(cluster_control_request_mark_held(original, &owner));
	UT_ASSERT(cluster_control_request_pause_upgrade(original, &owner));
	UT_ASSERT(!cluster_control_request_send_allowed(&key));
	key.holder.request_id++;
	UT_ASSERT(cluster_control_request_register(&key, ExclusiveLock, &owner,
											   key.holder.request_id - 1, ShareLock, replacement));
}

UT_TEST(failed_upgrade_restores_only_after_exact_retirement)
{
	ClusterControlRequestHandle original, replacement;
	ClusterControlRetireMessage message;
	ClusterControlRequestView old_view;
	uint64 driver;

	reset();
	upgrade_pair(&original, &replacement);
	UT_ASSERT(!cluster_control_request_mark_held(&original, &owner));
	UT_ASSERT(!cluster_control_request_restore_upgrade(&original, &replacement, &owner, &cut));
	UT_ASSERT(cluster_control_request_abandon(&replacement, &owner));
	driver = cluster_control_request_driver_start();
	UT_ASSERT(cluster_control_request_claim(&replacement, driver, &cut, &message));
	UT_ASSERT_EQ(message.previous_request, 101);
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&message, cut.master, driver, &cut));
	cut.generation++;
	UT_ASSERT(!cluster_control_request_restore_upgrade(&original, &replacement, &owner, &cut));
	cut.generation--;
	UT_ASSERT(cluster_control_request_restore_upgrade(&original, &replacement, &owner, &cut));
	UT_ASSERT(cluster_control_request_snapshot(&original, &old_view));
	UT_ASSERT_EQ(old_view.state, CLUSTER_CONTROL_REQUEST_HELD);
	UT_ASSERT(cluster_control_request_send_mode_allowed(&old_view.message.key, ShareLock));
	UT_ASSERT(!cluster_control_request_snapshot(&replacement, &old_view));
}

UT_TEST(confirmed_upgrade_and_downgrade_do_not_replay_old_mode)
{
	ClusterControlRequestHandle original, replacement;
	ClusterControlRequestView view;

	reset();
	upgrade_pair(&original, &replacement);
	UT_ASSERT(!cluster_control_request_confirm_upgrade(&original, &replacement, &owner, &cut));
	UT_ASSERT(cluster_control_request_mark_held(&replacement, &owner));
	UT_ASSERT(cluster_control_request_confirm_upgrade(&original, &replacement, &owner, &cut));
	UT_ASSERT(cluster_control_request_snapshot(&original, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT(cluster_control_request_snapshot(&replacement, &view));
	UT_ASSERT_EQ(view.message.previous_request, 0);
	UT_ASSERT(!cluster_control_request_restore_upgrade(&original, &replacement, &owner, &cut));
	UT_ASSERT(cluster_control_request_begin_downgrade(&replacement, &owner));
	UT_ASSERT(!cluster_control_request_send_mode_allowed(&key, ExclusiveLock));
	UT_ASSERT(cluster_control_request_send_mode_allowed(&key, ShareLock));
	UT_ASSERT(!cluster_control_request_begin_downgrade(&replacement, &owner));
	UT_ASSERT(cluster_control_request_mark_held(&replacement, &owner));
	UT_ASSERT(cluster_control_request_snapshot(&replacement, &view));
	UT_ASSERT_EQ(view.mode, ShareLock);
	UT_ASSERT(cluster_control_request_abandon(&replacement, &owner));
	UT_ASSERT(!cluster_control_request_mark_held(&replacement, &owner));
}

UT_TEST(surrendered_upgrade_cannot_restore_an_already_retired_share)
{
	ClusterControlRequestHandle original, replacement, wrong;
	ClusterControlRetireMessage before, after;
	ClusterControlRequestView view;
	uint64 driver;

	reset();
	upgrade_pair(&original, &replacement);
	UT_ASSERT(cluster_control_request_abandon(&replacement, &owner));
	driver = cluster_control_request_driver_start();
	UT_ASSERT(cluster_control_request_claim(&replacement, driver, &cut, &before));
	UT_ASSERT_EQ(before.previous_request, 101);
	wrong = original;
	wrong.generation++;
	UT_ASSERT(!cluster_control_request_surrender_upgrade(&wrong, &replacement, &owner));
	UT_ASSERT(cluster_control_request_surrender_upgrade(&original, &replacement, &owner));
	before.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(!cluster_control_request_ack(&before, cut.master, driver, &cut));
	UT_ASSERT(cluster_control_request_claim(&replacement, driver, &cut, &after));
	UT_ASSERT_EQ(after.previous_request, 0);
	UT_ASSERT_EQ(after.previous_mode, NoLock);
	UT_ASSERT(after.exchange_id != before.exchange_id);
	UT_ASSERT(cluster_control_request_snapshot(&original, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT(!cluster_control_request_restore_upgrade(&original, &replacement, &owner, &cut));
}

UT_TEST(shared_census_cannot_lose_exited_owner_or_aba)
{
	ClusterControlRequestHandle handle;
	ClusterControlRetireMessage message;
	uint64 version, next_version, driver;

	reset();
	UT_ASSERT(cluster_control_request_census(cut.epoch, &version));
	UT_ASSERT(cluster_control_request_empty());
	key.holder.cluster_epoch = cut.epoch;
	handle = registration(0);
	UT_ASSERT(!cluster_control_request_census(cut.epoch, &next_version));
	UT_ASSERT(!cluster_control_request_empty());
	UT_ASSERT(cluster_control_request_mark_held(&handle, &owner));
	UT_ASSERT(!cluster_control_request_census_unchanged(cut.epoch, version));
	UT_ASSERT(!cluster_control_request_census(cut.epoch + 1, &version));
	UT_ASSERT(cluster_control_request_census(cut.epoch, &version));
	UT_ASSERT(cluster_control_request_census_unchanged(cut.epoch, version));
	/* The creator is now absent from the backend census, not from this one. */
	cluster_control_request_owner_exit(&owner);
	UT_ASSERT(!cluster_control_request_census_unchanged(cut.epoch, version));
	UT_ASSERT(!cluster_control_request_census(cut.epoch, &next_version));
	driver = cluster_control_request_driver_start();
	UT_ASSERT(cluster_control_request_claim(&handle, driver, &cut, &message));
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&message, cut.master, driver, &cut));
	UT_ASSERT(!cluster_control_request_census(cut.epoch, &next_version));
	UT_ASSERT(cluster_control_request_forget(&handle, &owner, &cut));
	UT_ASSERT(cluster_control_request_census(cut.epoch, &next_version));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT(next_version != version);
	UT_ASSERT(!cluster_control_request_census_unchanged(cut.epoch, version));
}

UT_TEST(rebuild_restore_cannot_consume_an_unrelated_terminal_attempt)
{
	ClusterControlRequestHandle original, failed, decoy;
	ClusterControlRetireMessage message;
	ClusterControlRequestView view;
	uint64 driver;

	reset();
	upgrade_pair(&original, &failed);
	UT_ASSERT(cluster_control_request_abandon(&failed, &owner));
	UT_ASSERT(cluster_control_request_rebuild_upgrade_begin(&original, &failed, &owner));
	/* Same creator/resource/epoch and X mode is still not the failed X. */
	key.holder.request_id++;
	UT_ASSERT(cluster_control_request_register(&key, ExclusiveLock, &owner, 0, NoLock, &decoy));
	UT_ASSERT(cluster_control_request_abandon(&decoy, &owner));
	driver = cluster_control_request_driver_start();
	cut.epoch++;
	UT_ASSERT(cluster_control_request_claim(&decoy, driver, &cut, &message));
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&message, cut.master, driver, &cut));
	UT_ASSERT(!cluster_control_request_rebuild_upgrade_finish(&original, &decoy, &owner, &cut));
	UT_ASSERT(cluster_control_request_snapshot(&original, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_QUIESCED);
	UT_ASSERT(cluster_control_request_claim(&failed, driver, &cut, &message));
	UT_ASSERT_EQ(message.previous_request, 0);
	message.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&message, cut.master, driver, &cut));
	UT_ASSERT(cluster_control_request_rebuild_upgrade_finish(&original, &failed, &owner, &cut));
	UT_ASSERT(cluster_control_request_snapshot(&original, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_HELD);
	UT_ASSERT(view.message.key.holder.cluster_epoch < cut.epoch);
}

UT_TEST(rebuild_can_reissue_rejected_restore_but_invalid_is_not_terminal)
{
	ClusterControlRequestHandle original, failed;
	ClusterControlRetireMessage rejected = { 0 }, next = { 0 };
	ClusterControlRequestView view;
	uint64 driver;

	reset();
	upgrade_pair(&original, &failed);
	UT_ASSERT(cluster_control_request_abandon(&failed, &owner));
	driver = cluster_control_request_driver_start();
	cut.epoch++;
	UT_ASSERT(cluster_control_request_claim(&failed, driver, &cut, &rejected));
	rejected.verb = CLUSTER_CONTROL_RETIRE_INVALID;
	UT_ASSERT(cluster_control_request_ack(&rejected, cut.master, driver, &cut));
	UT_ASSERT(!cluster_control_request_rebuild_upgrade_finish(&original, &failed, &owner, &cut));
	/* The caller separately proves the common reconstruction freeze. */
	UT_ASSERT(cluster_control_request_rebuild_upgrade_begin(&original, &failed, &owner));
	UT_ASSERT(cluster_control_request_snapshot(&original, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_QUIESCED);
	UT_ASSERT(cluster_control_request_snapshot(&failed, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT(!cluster_control_request_rebuild_upgrade_finish(&original, &failed, &owner, &cut));
	UT_ASSERT(cluster_control_request_claim(&failed, driver, &cut, &next));
	UT_ASSERT_EQ(next.previous_request, 0);
	UT_ASSERT(next.exchange_id != rejected.exchange_id);
	UT_ASSERT(!cluster_control_request_ack(&rejected, cut.master, driver, &cut));
	next.verb = CLUSTER_CONTROL_RETIRED;
	UT_ASSERT(cluster_control_request_ack(&next, cut.master, driver, &cut));
	UT_ASSERT(cluster_control_request_rebuild_upgrade_finish(&original, &failed, &owner, &cut));
}

int
main(void)
{
	UT_PLAN(13);
	UT_RUN(only_canonical_control_resources_are_registered);
	UT_RUN(producer_fence_is_one_way_and_unknown_is_not_permission);
	UT_RUN(retry_and_wrong_ack_do_not_erase_cleanup_debt);
	UT_RUN(cut_and_driver_changes_invalidate_old_observations);
	UT_RUN(exit_handoff_does_not_infer_abandonment_from_active_state);
	UT_RUN(capacity_refusal_never_overwrites_an_owned_record);
	UT_RUN(wire_is_exact_sized_little_endian_and_rejects_malformed_identity);
	UT_RUN(failed_upgrade_restores_only_after_exact_retirement);
	UT_RUN(confirmed_upgrade_and_downgrade_do_not_replay_old_mode);
	UT_RUN(surrendered_upgrade_cannot_restore_an_already_retired_share);
	UT_RUN(shared_census_cannot_lose_exited_owner_or_aba);
	UT_RUN(rebuild_restore_cannot_consume_an_unrelated_terminal_attempt);
	UT_RUN(rebuild_can_reissue_rejected_restore_but_invalid_is_not_terminal);
	UT_DONE();
	free(memory);
	return ut_failed_count ? 1 : 0;
}
