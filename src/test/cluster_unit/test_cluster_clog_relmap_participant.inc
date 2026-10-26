/*-------------------------------------------------------------------------
 *
 * test_cluster_clog_relmap_participant.inc
 *     Original Startup participant custody of native CLOG page observations.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_clog_relmap_participant.inc
 *
 * NOTES
 *     Real native file reads; original episode/IR/ROOT and fault delivery are
 *     controlled leaves. No native lifecycle or COMPLETE certification.
 *
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_wal_inputs.h"

bool enableFsync = true;
static bool relmap_base_cf;

bool
cluster_cf_held(LOCKMODE mode pg_attribute_unused())
{
	return relmap_base_cf;
}

static ClusterRecoveryRelmapSourceV1 relmap_base_source;
static bool relmap_base_declared, relmap_base_all_held;
static bool relmap_base_root_ok;
static unsigned relmap_base_failures, relmap_base_rechecks;

bool
cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	return source != NULL && relmap_base_declared && relmap_base_all_held && authority
		&& memcmp(source, &relmap_base_source, sizeof(*source)) == 0;
}

void
cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	if (source != NULL && source->scope == relmap_base_source.scope
		&& memcmp(&source->input_owner, &relmap_base_source.input_owner,
			sizeof(source->input_owner)) == 0) {
		relmap_base_failures++;
		authority = false;
	}
}

bool
cluster_wal_inputs_relmap_source_revalidate_v1(const ClusterRecoveryRelmapSourceV1 *source)
{
	bool ok;
	/* Whole ROOT I/O must not be attempted under any native origin lock. */
	for (unsigned i = 0; i < fixture_lock_count; i++)
		UT_ASSERT(!fixture_locks[i].held);
	relmap_base_rechecks++;
	ok = cluster_wal_inputs_relmap_source_current_v1(source) && relmap_base_root_ok;
	if (!ok)
		cluster_wal_inputs_relmap_source_fail_v1(source);
	return ok;
}

#ifdef USE_RELMAP_CLOG_REFUSAL_SHIM
bool
cluster_clog_relmap_base_read_v1(const ClusterRecoveryRelmapSourceV1 *source pg_attribute_unused(),
	uint64 full pg_attribute_unused(), ClusterRelmapClogBaseV1 *out,
	ClusterRelmapClogBaseHandleV1 **held pg_attribute_unused())
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return false;
}
void
cluster_clog_relmap_base_release_v1(ClusterRelmapClogBaseHandleV1 **held)
{
	if (held != NULL)
		*held = NULL;
}
#endif

static void
relmap_base_reset(int origin)
{
	memset(&relmap_base_source, 0, sizeof(relmap_base_source));
	relmap_base_source.scope = (ClusterWalInputsV1 *)&relmap_base_source;
	relmap_base_source.input_owner.generation = 19;
	relmap_base_source.work_generation = 23;
	relmap_base_source.ir_request_id = 47;
	relmap_base_source.input_count = 4;
	relmap_base_source.replay_count = 2;
	relmap_base_source.selected.kind = CLUSTER_WAL_INPUT_CHECKPOINT;
	relmap_base_source.selected.source.claim.identity.origin_thread_id = origin + 1;
	relmap_base_source.selected.source.claim.identity.system_identifier = 179;
	relmap_base_source.selected.source.claim.database_incarnation = 181;
	relmap_base_source.replay = relmap_base_source.selected;
	relmap_base_declared = relmap_base_all_held = relmap_base_root_ok = authority = true;
	relmap_base_failures = relmap_base_rechecks = 0;
	cluster_shared_config = enableFsync = true; relmap_base_cf = false;
	relmap_base_interrupts = true; InterruptHoldoffCount = 0;
	MyAuxProcType = StartupProcess;
	c0_authority = revoke_on_lock = revoke_after_read = revoke_after_sync = false;
	fail_open = fail_read = fail_sync = fail_dirsync = fail_sync_close = false;
	directory_sync_error = directory_open_error = change_after_read = 0;
	fixture_origin = origin;
	InRecovery = false; /* Original Start is post native END_OF_RECOVERY. */
	CritSectionCount = 0;
}

static uint64
relmap_base_query(void)
{
	return (UINT64CONST(7) << 32) | 100;
}

UT_TEST(relmap_episode_base_all_statuses_and_original_origin_routes)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	for (int origin = 0; origin < 2; origin++) {
		for (int status = 0; status < 4; status++) {
			unsigned before = writes;
			relmap_base_reset(origin); fixture_zero(0); fixture_bit(100, status);
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			if (held == NULL) return;
			UT_ASSERT_EQ(base.origin, origin); UT_ASSERT_EQ(base.page_number, 0);
			UT_ASSERT_EQ(base.queried_full_xid, relmap_base_query());
			UT_ASSERT_EQ(base.status, status); UT_ASSERT(base.inode != 0);
			UT_ASSERT(base.size >= BLCKSZ); UT_ASSERT_EQ(transient_fds, 1);
			UT_ASSERT_EQ((base.image[TransactionIdToByte(100)]
				>> (TransactionIdToBIndex(100) * CLOG_BITS_PER_XACT)) & CLOG_XACT_BITMASK, status);
			UT_ASSERT_EQ(writes, before); UT_ASSERT_EQ(relmap_base_failures, 0);
			UT_ASSERT(relmap_base_rechecks >= 2);
			UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
				relmap_base_query(), &base, &held));
			UT_ASSERT_EQ(transient_fds, 1);
			cluster_clog_relmap_base_release_v1(&held); fixture_idle();
		}
	}
}

UT_TEST(relmap_episode_preconditions_keep_caller_locks_and_original_debt)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	LWLock *locks[] = {XactSLRULock, XactTruncationLock, XidGenLock,
		&ClogRecoveryLocks[3].lock, ClogRecoveryCtl[1].shared->ControlLock};
	unsigned io;
	relmap_base_reset(1); fixture_zero(0); io = reads + syncs + dirsyncs;
	for (unsigned i = 0; i < lengthof(locks); i++) {
		LWLockAcquire(locks[i], LW_EXCLUSIVE);
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT(LWLockHeldByMe(locks[i])); LWLockRelease(locks[i]);
	}
	relmap_base_cf = true;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); relmap_base_cf = false;
	CritSectionCount = 1;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); CritSectionCount = 0;
	relmap_base_declared = false;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); relmap_base_declared = true;
	relmap_base_all_held = false;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); relmap_base_all_held = true;
	UT_ASSERT_EQ(io, reads + syncs + dirsyncs); UT_ASSERT_EQ(relmap_base_failures, 0);
	UT_ASSERT(held == NULL); UT_ASSERT(authority); fixture_idle();
}

UT_TEST(relmap_episode_same_fd_replacement_and_full_image_change_are_sticky)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	for (int mutation = 0; mutation < 2; mutation++) {
		char path[MAXPGPATH], backup[MAXPGPATH];
		relmap_base_reset(1); fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held)); if (held == NULL) return;
		fixture_path(0, path); snprintf(backup, sizeof(backup), "%s.saved", path);
		if (mutation == 0) {
			UT_ASSERT_EQ(rename(path, backup), 0); fixture_zero(0);
			fixture_bit(100, TRANSACTION_STATUS_COMMITTED);
		} else fixture_bit(101, TRANSACTION_STATUS_ABORTED);
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT_EQ(base.queried_full_xid, 0); UT_ASSERT(!authority);
		UT_ASSERT_EQ(transient_fds, 1);
		if (mutation == 0) {
			UT_ASSERT_EQ(unlink(path), 0); UT_ASSERT_EQ(rename(backup, path), 0);
		} else fixture_bit(101, TRANSACTION_STATUS_IN_PROGRESS);
		authority = true;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		cluster_clog_relmap_base_release_v1(&held); fixture_idle();
	}
}

UT_TEST(relmap_episode_native_failures_keep_unknown_and_original_protection)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	for (int failure = 0; failure < 7; failure++) {
		char path[MAXPGPATH], backup[MAXPGPATH];
		relmap_base_reset(1); fixture_zero(0); fixture_path(0, path);
		snprintf(backup, sizeof(backup), "%s.saved", path);
		if (failure < 3) {
			UT_ASSERT_EQ(rename(path, backup), 0);
			if (failure == 1) UT_ASSERT_EQ(symlink(backup, path), 0);
			if (failure == 2) UT_ASSERT_EQ(link(backup, path), 0);
		} else if (failure == 3) UT_ASSERT_EQ(truncate(path, BLCKSZ - 1), 0);
		else if (failure == 4) fail_sync = true;
		else if (failure == 5) directory_sync_error = EIO;
		else fail_open = true;
		UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held));
		UT_ASSERT(held == NULL); UT_ASSERT_EQ(base.queried_full_xid, 0);
		UT_ASSERT(!authority); UT_ASSERT(relmap_base_failures > 0);
		if (failure < 3) {
			if (failure != 0) UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(rename(backup, path), 0);
		}
		fixture_idle();
	}
}

UT_TEST(relmap_episode_late_holder_loss_revokes_after_io)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	relmap_base_reset(1); fixture_zero(0); revoke_after_sync = true;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(held == NULL); UT_ASSERT(!authority); UT_ASSERT(relmap_base_failures > 0);
	revoke_after_sync = false; fixture_idle();
}

UT_TEST(relmap_episode_error_keeps_held_fd_until_local_cleanup)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	volatile bool caught = false;
	relmap_base_reset(1); fixture_zero(0);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); if (held == NULL) return;
	fail_dirsync = expect_error = true;
	PG_TRY();
	{
		(void)cluster_clog_relmap_base_read_v1(&relmap_base_source,
			relmap_base_query(), &base, &held);
	}
	PG_CATCH(); { caught = true; } PG_END_TRY();
	fail_dirsync = expect_error = false;
	UT_ASSERT(caught); UT_ASSERT(!authority); UT_ASSERT_EQ(transient_fds, 1);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	authority = true;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	cluster_clog_relmap_base_release_v1(&held); fixture_idle();
}

UT_TEST(relmap_episode_own_route_and_cache_are_not_substituted)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	char own[MAXPGPATH]; int slot;
	relmap_base_reset(0); fixture_zero(0); fixture_bit(100, TRANSACTION_STATUS_ABORTED);
	LWLockAcquire(XactSLRULock, LW_EXCLUSIVE);
	slot = SimpleLruReadPage(XactCtl, 0, true, 100);
	XactCtl->shared->page_buffer[slot][TransactionIdToByte(100)] = 0xff;
	XactCtl->shared->page_dirty[slot] = true; LWLockRelease(XactSLRULock);
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	if (held == NULL) return;
	UT_ASSERT_EQ(base.status, TRANSACTION_STATUS_ABORTED);
	UT_ASSERT(XactCtl->shared->page_dirty[slot]);
	UT_ASSERT_EQ((uint8)XactCtl->shared->page_buffer[slot][TransactionIdToByte(100)], 0xff);
	cluster_clog_relmap_base_release_v1(&held);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	snprintf(own, sizeof(own), "%s/native_side/origin_1/pg_xact", fixture_root);
	UT_ASSERT_EQ(symlink(own, "pg_xact"), 0);
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held)); UT_ASSERT(!authority);
	UT_ASSERT_EQ(unlink("pg_xact"), 0);
	snprintf(own, sizeof(own), "%s/native_side/origin_0/pg_xact", fixture_root);
	UT_ASSERT_EQ(symlink(own, "pg_xact"), 0);
	XactCtl->shared->page_dirty[slot] = false; fixture_idle();
}

UT_TEST(relmap_episode_exact_source_history_and_root_recheck)
{
	ClusterRelmapClogBaseV1 base;
	ClusterRelmapClogBaseHandleV1 *held = NULL;
	ClusterRecoveryRelmapSourceV1 original;
	relmap_base_reset(1); fixture_zero(0); original = relmap_base_source;
	UT_ASSERT(cluster_clog_relmap_base_read_v1(&original, relmap_base_query(), &base, &held));
	if (held == NULL) return;
	relmap_base_source.selected.source.claim.identity.origin_owner_incarnation++;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&original, relmap_base_query(), &base, &held));
	UT_ASSERT(!authority); cluster_clog_relmap_base_release_v1(&held); fixture_idle();
	relmap_base_reset(1); relmap_base_root_ok = false;
	UT_ASSERT(!cluster_clog_relmap_base_read_v1(&relmap_base_source,
		relmap_base_query(), &base, &held));
	UT_ASSERT(!authority); UT_ASSERT(held == NULL); fixture_idle();
	InRecovery = true; fixture_origin = 1; authority = true;
}
