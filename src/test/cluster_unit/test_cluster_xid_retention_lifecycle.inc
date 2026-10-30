/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_lifecycle.inc
 *     Retained transaction through original relmap terminal callbacks.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_lifecycle.inc
 * NOTES
 *     The singleton module, CLOG/SLRU and pair files execute. Lower GES,
 *     notification transport, directory authority and commit WAL are fixtures.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_sinval.h"
#include "storage/sinval.h"
#include "utils/memutils.h"
#include "test_cluster_relmap_native_lock.h"

enum RetainedLifecycleFault
{
	LIFECYCLE_OK, LIFECYCLE_SEND_ERROR, LIFECYCLE_ACK_ERROR,
	LIFECYCLE_ACK_EXHAUSTED, LIFECYCLE_SEND_DRIFT, LIFECYCLE_ACK_DRIFT
};
static enum RetainedLifecycleFault lifecycle_fault;
static bool lifecycle_active, lifecycle_commit;
static unsigned lifecycle_sends[2], lifecycle_acks[2], lifecycle_ack_failures[2];
static unsigned lifecycle_warnings, lifecycle_reports;
static int lifecycle_error_level;
static ClusterConf lifecycle_peers;
ClusterConf *ClusterConfShmem;
MemoryContext ErrorContext;
MemoryContext CurrentMemoryContext;

int errdetail_internal(const char *format pg_attribute_unused(), ...) { return 0; }
int errhidecontext(bool hide pg_attribute_unused()) { return 0; }
int errhidestmt(bool hide pg_attribute_unused()) { return 0; }

static bool
lifecycle_errstart(int level)
{
	UT_ASSERT(lifecycle_active);
	if (level == WARNING) {
		lifecycle_warnings++;
		return false;
	}
	if (!retention_error_expected || level < ERROR) abort();
	lifecycle_error_level = level;
	return true;
}

void
EmitErrorReport(void)
{
	UT_ASSERT(lifecycle_active && error_context_stack == NULL);
	UT_ASSERT(CurrentMemoryContext == ErrorContext);
	lifecycle_reports++;
}

bool
cluster_relmap_authority_x_lock(void)
{
	UT_ASSERT(lifecycle_active && !held && !retention_held && !retention_mapping_held);
	caller_global_x = test_relmap_native_lock_acquire();
	return caller_global_x;
}

void
cluster_relmap_authority_x_unlock(void)
{
	UT_ASSERT(lifecycle_active && caller_global_x);
	test_relmap_native_lock_release();
	caller_global_x = test_relmap_native_lock_held();
}

/* Checked extraction of the complete original CCI and v2 terminal bodies. */
#include "test_cluster_xid_retention_lifecycle_product.inc"

static void
lifecycle_clean_custody(void)
{
	UT_ASSERT(lifecycle_active && test_relmap_native_lock_held() && caller_global_x);
	UT_ASSERT(!held && !retention_held && !retention_mapping_held && !retention_truncation_held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
	UT_ASSERT(cluster_relmap_v2_finish_entered);
	UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
	UT_ASSERT_EQ(active_shared_updates.num_mappings + active_local_updates.num_mappings, 0);
	for (unsigned slot = 0; slot < 2; slot++)
		retained_pair_expect(slot, false, lifecycle_commit);
	UT_ASSERT_EQ(retained_native_successful_syncs, lifecycle_commit ? 2 : 0);
}

void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
	unsigned slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	lifecycle_clean_custody();
	UT_ASSERT(lifecycle_commit);
	UT_ASSERT_EQ(n, 1); UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, stage_pair_prepared[slot].identity.dbid);
	UT_ASSERT_EQ(lifecycle_sends[slot], 0);
	if (slot == 1) UT_ASSERT(lifecycle_acks[0] > lifecycle_ack_failures[0] || !cluster_peer_mode_enabled());
	lifecycle_sends[slot]++;
	if (slot == 0 && lifecycle_fault == LIFECYCLE_SEND_ERROR)
		ereport(ERROR, (errmsg("controlled retained local invalidation failure")));
	if (slot == 0 && lifecycle_fault == LIFECYCLE_SEND_DRIFT) caller_boot++;
}

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	unsigned slot = msgs[0].rm.dbId == InvalidOid ? 0 : 1;

	lifecycle_clean_custody();
	UT_ASSERT(lifecycle_commit && cluster_peer_mode_enabled());
	UT_ASSERT_EQ(n, 1); UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, stage_pair_prepared[slot].identity.dbid);
	UT_ASSERT_EQ(lifecycle_sends[slot], 1);
	lifecycle_acks[slot]++;
	if (slot == 0) {
		if (lifecycle_fault == LIFECYCLE_ACK_ERROR)
			ereport(ERROR, (errmsg("controlled retained ACK failure")));
		if (lifecycle_fault == LIFECYCLE_ACK_DRIFT) caller_boot++;
		if (lifecycle_fault == LIFECYCLE_ACK_EXHAUSTED)
			return CLUSTER_SINVAL_ACK_TIMEOUT;
	}
	return lifecycle_acks[slot] <= lifecycle_ack_failures[slot]
		? CLUSTER_SINVAL_ACK_ENQUEUE_FAILED : CLUSTER_SINVAL_ACK_DONE;
}

static void
lifecycle_before_release(void)
{
	lifecycle_clean_custody();
	for (unsigned slot = 0; slot < 2; slot++) {
		UT_ASSERT_EQ(lifecycle_sends[slot], lifecycle_commit ? 1 : 0);
		UT_ASSERT_EQ(lifecycle_acks[slot], lifecycle_commit && cluster_peer_mode_enabled()
			? lifecycle_ack_failures[slot] + 1 : 0);
	}
}

static TestRelmapNativeLockConfig
lifecycle_lock_config(void)
{
	TestRelmapNativeLockConfig config = {
		.acquire_result = CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK,
		.promote_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED,
		.release_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED,
		.before_release = lifecycle_before_release
	};

	return config;
}

static void
lifecycle_start(bool rollover, bool commit, bool peers, const TestRelmapNativeLockConfig *config)
{
	retained_native_pair_start(rollover);
	memset(&active_shared_updates, 0, sizeof(active_shared_updates));
	memset(&active_local_updates, 0, sizeof(active_local_updates));
	memset(&pending_shared_updates, 0, sizeof(pending_shared_updates));
	memset(&pending_local_updates, 0, sizeof(pending_local_updates));
	memset(lifecycle_sends, 0, sizeof(lifecycle_sends));
	memset(lifecycle_acks, 0, sizeof(lifecycle_acks));
	memset(lifecycle_ack_failures, 0, sizeof(lifecycle_ack_failures));
	lifecycle_warnings = lifecycle_reports = 0;
	lifecycle_error_level = 0; lifecycle_fault = LIFECYCLE_OK;
	lifecycle_active = true; lifecycle_commit = commit;
	lifecycle_peers.node_count = 2;
	ClusterConfShmem = peers ? &lifecycle_peers : NULL;
	ErrorContext = (MemoryContext)(uintptr_t)10;
	CurrentMemoryContext = (MemoryContext)(uintptr_t)11;
	error_context_stack = NULL;
	retention_error_start = lifecycle_errstart;
	test_relmap_native_lock_reset(config);
	caller_global_x = false;
	caller_authority_reader = test_relmap_native_lock_held;
	if (!commit) stage_pair_terminal_write_observer = NULL;
}

static void
lifecycle_prepare(void)
{
	TestRelmapNativeLockStats stats;

	pending_shared_updates = pending_local_updates = stage_updates;
	UT_ASSERT(!test_relmap_native_lock_held());
	AtCCI_RelationMap();
	UT_ASSERT_EQ(active_shared_updates.num_mappings, 1);
	UT_ASSERT_EQ(active_local_updates.num_mappings, 1);
	UT_ASSERT_EQ(pending_shared_updates.num_mappings + pending_local_updates.num_mappings, 0);
	cluster_relmap_producer_precommit_v2(false);
	test_relmap_native_lock_stats(&stats);
	UT_ASSERT_EQ(stats.acquires, 1); UT_ASSERT_EQ(stats.promotions, 1); UT_ASSERT_EQ(stats.releases, 0);
	UT_ASSERT(stats.exact_request && stats.request_id != 0 && test_relmap_native_lock_held());
	UT_ASSERT_EQ(native_publish_count, 1);
	UT_ASSERT_EQ(active_shared_updates.num_mappings + active_local_updates.num_mappings, 0);
	UT_ASSERT(cluster_relmap_v2_arms[0].armed && cluster_relmap_v2_arms[1].armed);
	UT_ASSERT_EQ(cluster_relmap_v2_arms[0].sample.owner.top_full_xid,
		U64FromFullTransactionId(TopTransactionStateData.fullTransactionId));
	stage_pair_finishing = true;
	retained_pair_expect(0, true, false); retained_pair_expect(1, true, false);
}

static void
lifecycle_decide(void)
{
	TransactionId top = (TransactionId)cluster_relmap_v2_arms[0].sample.owner.top_full_xid;

	test_xid_native_clog_set_status(top, 0, NULL,
		lifecycle_commit ? TRANSACTION_STATUS_COMMITTED : TRANSACTION_STATUS_ABORTED);
	TopTransactionStateData.state = lifecycle_commit ? TRANS_COMMIT : TRANS_ABORT;
}

static bool
lifecycle_terminal(bool commit)
{
	volatile bool caught = false;

	lifecycle_error_level = 0;
	retention_error_expected = true;
	PG_TRY(); {
		if (commit) cluster_relmap_producer_postcommit_v2();
		else cluster_relmap_producer_abort_v2();
	}
	PG_CATCH(); {
		caught = true;
		UT_ASSERT_EQ(lifecycle_error_level, PANIC);
		UT_ASSERT(error_context_stack == NULL && CurrentMemoryContext == ErrorContext);
	}
	PG_END_TRY();
	retention_error_expected = false;
	return caught;
}

static void
lifecycle_debt(const ClusterRelmapProducerArmV2 *arms, const ClusterRelmapRetentionDirectory *directory,
	unsigned releases)
{
	TestRelmapNativeLockStats stats;
	TestRelmapPhysicalStats physical_before = test_relmap_physical_stats(), physical_after;
	XidNativeClogTerminalState native_before, native_after;
	TransactionId top = (TransactionId)arms[0].sample.owner.top_full_xid;
	unsigned sends = lifecycle_sends[0] + lifecycle_sends[1];
	unsigned acks = lifecycle_acks[0] + lifecycle_acks[1];

	UT_ASSERT(test_relmap_native_lock_held() && caller_global_x && cluster_relmap_v2_finish_entered);
	UT_ASSERT_EQ(memcmp(arms, cluster_relmap_v2_arms, sizeof(cluster_relmap_v2_arms)), 0);
	UT_ASSERT_EQ(memcmp(directory, &relmap_runtime_shmem->directories[4], sizeof(*directory)), 0);
	test_xid_native_clog_terminal_state(top, &native_before);
	UT_ASSERT(lifecycle_terminal(true)); UT_ASSERT(lifecycle_terminal(false));
	UT_ASSERT(!cluster_relmap_producer_finish_v2(true));
	UT_ASSERT(!cluster_relmap_producer_finish_v2(false));
	UT_ASSERT(!cluster_relmap_producer_arm_v2(true, &stage_updates));
	physical_after = test_relmap_physical_stats();
	UT_ASSERT_EQ(memcmp(&physical_before, &physical_after, sizeof(physical_before)), 0);
	test_xid_native_clog_terminal_state(top, &native_after);
	UT_ASSERT_EQ(memcmp(&native_before, &native_after, sizeof(native_before)), 0);
	test_relmap_native_lock_stats(&stats);
	UT_ASSERT_EQ(stats.releases, releases); UT_ASSERT(stats.exact_request);
	UT_ASSERT_EQ(lifecycle_sends[0] + lifecycle_sends[1], sends);
	UT_ASSERT_EQ(lifecycle_acks[0] + lifecycle_acks[1], acks);
	UT_ASSERT_EQ(memcmp(arms, cluster_relmap_v2_arms, sizeof(cluster_relmap_v2_arms)), 0);
	UT_ASSERT_EQ(memcmp(directory, &relmap_runtime_shmem->directories[4], sizeof(*directory)), 0);
}

static void
lifecycle_stop(void)
{
	TestRelmapNativeLockConfig config = lifecycle_lock_config();

	/* Isolated fixture teardown, including after simulated PANIC. No product
	 * acquire/release or successful retirement is inferred from these resets. */
	caller_authority_reader = NULL;
	test_relmap_native_lock_reset(&config);
	caller_global_x = true;
	ClusterConfShmem = NULL; retention_error_start = NULL;
	lifecycle_active = false;
	retained_native_pair_stop();
}

UT_TEST(retained_lifecycle_commit_retires_custody_after_ack_and_original_release)
{
	for (unsigned which = 0; which < 4; which++) {
		TestRelmapNativeLockConfig config = lifecycle_lock_config();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory directory;
		TestRelmapPhysicalStats before, after;

		lifecycle_start((which & 1) != 0, true, (which & 2) != 0, &config);
		lifecycle_prepare(); lifecycle_decide();
		lifecycle_ack_failures[0] = 2; lifecycle_ack_failures[1] = 1;
		directory = relmap_runtime_shmem->directories[4];
		UT_ASSERT(!lifecycle_terminal(true));
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.acquires, 1); UT_ASSERT_EQ(stats.releases, 1);
		UT_ASSERT(stats.exact_request && stats.held_at_release && !test_relmap_native_lock_held());
		UT_ASSERT(!caller_global_x && !cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_v2_finish_entered);
		UT_ASSERT_EQ(lifecycle_warnings, (which & 2) ? 3 : 0);
		UT_ASSERT_EQ(memcmp(&directory, &relmap_runtime_shmem->directories[4], sizeof(directory)), 0);
		before = test_relmap_physical_stats();
		UT_ASSERT(!lifecycle_terminal(true)); UT_ASSERT(!lifecycle_terminal(false));
		after = test_relmap_physical_stats();
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		test_relmap_native_lock_stats(&stats); UT_ASSERT_EQ(stats.releases, 1);
		lifecycle_stop();
	}
}

UT_TEST(retained_lifecycle_abort_discards_before_original_release)
{
	for (unsigned rollover = 0; rollover < 2; rollover++) {
		TestRelmapNativeLockConfig config = lifecycle_lock_config();
		TestRelmapNativeLockStats stats;
		ClusterRelmapRetentionDirectory directory;
		XidNativeClogTerminalState native;
		TransactionId top;

		lifecycle_start(rollover != 0, false, true, &config);
		lifecycle_prepare(); lifecycle_decide();
		top = (TransactionId)cluster_relmap_v2_arms[0].sample.owner.top_full_xid;
		directory = relmap_runtime_shmem->directories[4];
		UT_ASSERT(!lifecycle_terminal(false));
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT_EQ(stats.releases, 1); UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT(!test_relmap_native_lock_held() && !caller_global_x);
		UT_ASSERT(!cluster_relmap_v2_arms[0].armed && !cluster_relmap_v2_arms[1].armed);
		UT_ASSERT(!cluster_relmap_v2_finish_entered);
		UT_ASSERT_EQ(memcmp(&directory, &relmap_runtime_shmem->directories[4], sizeof(directory)), 0);
		test_xid_native_clog_terminal_state(top, &native); UT_ASSERT_EQ(native.calls, 0);
		UT_ASSERT(!lifecycle_terminal(false)); UT_ASSERT(!lifecycle_terminal(true));
		test_relmap_native_lock_stats(&stats); UT_ASSERT_EQ(stats.releases, 1);
		lifecycle_stop();
	}
}

UT_TEST(retained_lifecycle_notification_failure_keeps_clean_pair_debt)
{
	for (unsigned fault = LIFECYCLE_SEND_ERROR; fault <= LIFECYCLE_ACK_DRIFT; fault++) {
		TestRelmapNativeLockConfig config = lifecycle_lock_config();
		ClusterRelmapProducerArmV2 arms[2];
		ClusterRelmapRetentionDirectory directory;

		lifecycle_start(false, true, true, &config);
		lifecycle_prepare(); lifecycle_decide();
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		directory = relmap_runtime_shmem->directories[4]; lifecycle_fault = fault;
		UT_ASSERT(lifecycle_terminal(true));
		retained_pair_expect(0, false, true); retained_pair_expect(1, false, true);
		UT_ASSERT_EQ(lifecycle_reports, 1);
		UT_ASSERT_EQ(lifecycle_sends[0], 1); UT_ASSERT_EQ(lifecycle_sends[1], 0);
		UT_ASSERT_EQ(lifecycle_warnings, fault == LIFECYCLE_ACK_EXHAUSTED ? CLUSTER_RELMAP_PUBLISH_RETRIES : 0);
		lifecycle_debt(arms, &directory, 0);
		lifecycle_stop();
	}
}

UT_TEST(retained_lifecycle_partial_native_or_pair_failure_stops_before_notifications)
{
	for (unsigned fault = 0; fault < 3; fault++) {
		TestRelmapNativeLockConfig config = lifecycle_lock_config();
		ClusterRelmapProducerArmV2 arms[2];
		ClusterRelmapRetentionDirectory directory;
		TestRelmapPhysicalStats before;

		lifecycle_start(false, true, true, &config);
		lifecycle_prepare(); lifecycle_decide();
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		directory = relmap_runtime_shmem->directories[4];
		before = test_relmap_physical_stats();
		if (fault == 0) retained_native_fault = XID_CLOG_TERMINAL_OPEN_ERROR;
		if (fault == 1) test_relmap_physical_fail_sync(false, before.file_syncs + 3);
		if (fault == 2) retained_native_sync_drift = true;
		UT_ASSERT(lifecycle_terminal(true));
		UT_ASSERT_EQ(lifecycle_sends[0] + lifecycle_sends[1] + lifecycle_acks[0] + lifecycle_acks[1], 0);
		if (fault == 1) {
			TestRelmapPhysicalStats after = test_relmap_physical_stats();

			UT_ASSERT_EQ(after.file_syncs, before.file_syncs + 3);
			retained_pair_expect(0, false, true); retained_pair_expect(1, true, false);
		} else {
			retained_pair_expect(0, true, false); retained_pair_expect(1, true, false);
		}
		lifecycle_debt(arms, &directory, 0);
		lifecycle_stop();
	}
}

UT_TEST(retained_lifecycle_release_failure_never_reenters_storage_or_s6)
{
	for (unsigned which = 0; which < 4; which++) {
		TestRelmapNativeLockConfig config = lifecycle_lock_config();
		ClusterRelmapProducerArmV2 arms[2];
		ClusterRelmapRetentionDirectory directory;
		TestRelmapNativeLockStats stats;

		config.release_result = (which & 1) ? CLUSTER_LOCK_ACQUIRE_OK_GRANTED : CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
		config.release_error = (which & 1) != 0;
		lifecycle_start(false, (which & 2) != 0, true, &config);
		lifecycle_prepare(); lifecycle_decide();
		memcpy(arms, cluster_relmap_v2_arms, sizeof(arms));
		directory = relmap_runtime_shmem->directories[4];
		UT_ASSERT(lifecycle_terminal(lifecycle_commit));
		test_relmap_native_lock_stats(&stats);
		UT_ASSERT(stats.exact_request && stats.held_at_release);
		UT_ASSERT_EQ(lifecycle_reports, 1);
		retained_pair_expect(0, false, lifecycle_commit); retained_pair_expect(1, false, lifecycle_commit);
		lifecycle_debt(arms, &directory, 1);
		lifecycle_stop();
	}
}
