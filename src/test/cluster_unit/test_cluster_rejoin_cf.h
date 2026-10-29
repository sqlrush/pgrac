/* Original retained join-stage and owner-rejoin publisher with actual CF.
 * Canonical ROOT/JCMK reads, marker completion and final admission are leaves;
 * the test does not create a real admission or a new retry mechanism.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_marker_async.h"
#include "cluster/cluster_recovery_duty.h"
#include "test_cluster_rejoin_cf_types.inc"

int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
static ClusterReconfigJoinCommitStage rejoin_stage;
static ClusterReconfigState rejoin_state;
static ClusterControlRootSnapshot rejoin_snapshot;
static ClusterControlRootResult rejoin_lookup_result;
static ClusterRecoveryOwnerImportResult rejoin_import_result;
static ClusterMarkerPollResult rejoin_marker_poll;
static uint32 rejoin_marker_result;
static uint64 rejoin_proven_incarnation;
static unsigned rejoin_reads, rejoin_imports, rejoin_publications;
static unsigned rejoin_prepares, rejoin_publishes, rejoin_fences;
static ClusterControlRootReadToken rejoin_seen_token;
static ClusterControlRootPatch rejoin_seen_patch;

static ClusterControlRootResult
rejoin_lookup(int32 node, ClusterControlRootIdentity *identity,
			  ClusterControlRootSnapshot *snapshot, ClusterControlRootReadToken *token)
{
	UT_ASSERT_EQ(node, 0);
	++rejoin_reads;
	if (rejoin_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| rejoin_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*identity = rejoin_snapshot.identity;
		*snapshot = rejoin_snapshot;
		*token = generic_token;
	}
	return rejoin_lookup_result;
}

static ClusterRecoveryOwnerImportResult
rejoin_import(int32 node, const ClusterWalThreadClaim *claim, uint64 lo, uint64 hi, uint64 *out)
{
	ClusterWalThreadClaim expected;

	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(lo | hi, 0);
	UT_ASSERT_EQ(cluster_write_fence_enforcement, CLUSTER_WRITE_FENCE_ENFORCE_ON);
	cluster_wal_thread_claim_fill(&expected, 1, 0, INT64_C(12345));
	UT_ASSERT_EQ(memcmp(claim, &expected, sizeof(expected)), 0);
	++rejoin_imports;
	*out = rejoin_proven_incarnation;
	return rejoin_import_result;
}

static ClusterControlRootResult
rejoin_compare(const ClusterControlRootReadToken *token, const ClusterControlRootPatch *patch,
			   ClusterControlRootPublishReason reason, ClusterControlRootSnapshot *out,
			   ClusterControlRootReadToken *out_token)
{
	++rejoin_publications;
	rejoin_seen_token = *token;
	rejoin_seen_patch = *patch;
	return cluster_control_root_compare_and_publish(token, patch, reason, out, out_token);
}

static bool
rejoin_submit(ClusterMarkerAsync *async, int32 node, const ClusterJoinCommitMarker *marker,
			  ClusterMarkerAsyncKind kind, TimestampTz now)
{
	(void)async;
	(void)node;
	(void)marker;
	(void)kind;
	(void)now;
	UT_ASSERT(false); /* Fixture starts at an already submitted stage. */
	return false;
}

static ClusterMarkerPollResult
rejoin_poll(ClusterMarkerAsync *async, TimestampTz now, uint32 *result, uint64 *elapsed)
{
	(void)now;
	UT_ASSERT(async == &rejoin_stage.async);
	*result = rejoin_marker_result;
	*elapsed = 10;
	return rejoin_marker_poll;
}

static void
rejoin_marker_note(ClusterMarkerAsyncKind kind, int32 node, uint64 elapsed)
{
	UT_ASSERT_EQ(kind, CLUSTER_MARKER_KIND_JOIN_COMMITTED);
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(elapsed, 10);
}

static bool
rejoin_slot(int32 node, uint64 *incarnation, uint64 *generation)
{
	UT_ASSERT_EQ(node, 0);
	*incarnation = 77;
	*generation = 1;
	return true;
}

static ClusterJoinVerdict
rejoin_vet(int32 node, uint64 incarnation, uint64 generation)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(incarnation, 77);
	UT_ASSERT_EQ(generation, 1);
	return CLUSTER_JOIN_ACCEPT;
}

static bool
rejoin_prepare(int32 node, uint64 incarnation, uint64 epoch)
{
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(incarnation, 77);
	UT_ASSERT_EQ(epoch, owner_epoch);
	++rejoin_prepares;
	return true;
}

static bool
rejoin_publish(void)
{
	++rejoin_publishes;
	UT_ASSERT(false); /* Enforced fencing must precede final membership. */
	return false;
}

static bool
rejoin_fence(TimestampTz now)
{
	(void)now;
	++rejoin_fences;
	return true; /* Final membership/fence service is not exercised. */
}

static void
rejoin_prebump(int active)
{
	(void)active;
	UT_ASSERT(false);
}

static void
rejoin_external_release(int32 node)
{
	(void)node;
	UT_ASSERT(false);
}

#define write_u16_le prepared_write_u16
#define write_u32_le prepared_write_u32
#define write_u64_le prepared_write_u64
#define cluster_recovery_duty_key_encode_for_claim rejoin_key_encode
#define cluster_recovery_duty_key_compare_for_claim rejoin_key_compare_for_claim
#define cluster_recovery_duty_key_compare rejoin_key_compare
#define cluster_recovery_owner_rejoin_v1 rejoin_owner
#define cluster_control_root_lookup_owner_by_node_runtime rejoin_lookup
#define cluster_recovery_owner_import_read_v1 rejoin_import
#define cluster_control_root_compare_and_publish rejoin_compare
#define join_commit_stage rejoin_stage
#define ReconfigShmem (&rejoin_state)
#define cluster_reconfig_submit_join_marker_async rejoin_submit
#define cluster_reconfig_poll_join_marker_async rejoin_poll
#define cluster_reconfig_note_marker_timeout rejoin_marker_note
#define cluster_reconfig_note_marker_slow_ack rejoin_marker_note
#define cluster_reconfig_get_observed_slot rejoin_slot
#define cluster_membership_vet_joiner rejoin_vet
#define cluster_reconfig_prepare_join_commit rejoin_prepare
#define cluster_reconfig_publish_prepared_join_commit rejoin_publish
#define cluster_reconfig_poll_join_fence_stage rejoin_fence
#define cluster_reconfig_set_prebump_sync_active rejoin_prebump
#define cluster_reconfig_external_rejoin_release_slot rejoin_external_release
#include "test_cluster_rejoin_cf.inc"
#undef cluster_reconfig_external_rejoin_release_slot
#undef cluster_reconfig_set_prebump_sync_active
#undef cluster_reconfig_poll_join_fence_stage
#undef cluster_reconfig_publish_prepared_join_commit
#undef cluster_reconfig_prepare_join_commit
#undef cluster_membership_vet_joiner
#undef cluster_reconfig_get_observed_slot
#undef cluster_reconfig_note_marker_slow_ack
#undef cluster_reconfig_note_marker_timeout
#undef cluster_reconfig_poll_join_marker_async
#undef cluster_reconfig_submit_join_marker_async
#undef ReconfigShmem
#undef join_commit_stage
#undef cluster_control_root_compare_and_publish
#undef cluster_recovery_owner_import_read_v1
#undef cluster_control_root_lookup_owner_by_node_runtime
#undef cluster_recovery_owner_rejoin_v1
#undef cluster_recovery_duty_key_compare
#undef cluster_recovery_duty_key_compare_for_claim
#undef cluster_recovery_duty_key_encode_for_claim
#undef write_u64_le
#undef write_u32_le
#undef write_u16_le

static void
rejoin_refresh_token(void)
{
	prepared_disk.records[0] = rejoin_snapshot;
	make_read_token(&prepared_disk, 1, CONTROL_ROOT_SOURCE_PRIMARY, &generic_token);
}

static void
rejoin_stage_reset(void)
{
	memset(&rejoin_stage, 0, sizeof(rejoin_stage));
	rejoin_stage.async.state = CLUSTER_MARKER_ASYNC_SUBMITTED;
	rejoin_stage.async.has_staged_event = true;
	rejoin_stage.async.inflight_seq = 11;
	rejoin_stage.async.staged_expect_epoch = owner_epoch;
	rejoin_stage.submitted = true;
	rejoin_stage.node_id = 0;
	rejoin_stage.admitted_incarnation = 77;
}

static void
rejoin_fixture(uint32 lifecycle)
{
	ClusterWalThreadClaim claim;
	ClusterControlRootIdentity *identity;

	generic_fixture(lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
						? CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
						: CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN);
	memset(&rejoin_state, 0, sizeof(rejoin_state));
	pg_atomic_init_u64(&rejoin_state.join_reject_count, 0);
	memset(&rejoin_snapshot, 0, sizeof(rejoin_snapshot));
	identity = &rejoin_snapshot.identity;
	identity->system_identifier = 1234;
	identity->storage_uuid[0] = 31;
	identity->authority_uuid[0] = 41;
	identity->authority_uuid[6] = 0x40;
	identity->authority_uuid[8] = 0x80;
	identity->origin_thread_id = 1;
	identity->origin_node_id = 0;
	identity->thread_claim_created_at = 12345;
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(12345));
	identity->thread_claim_crc32c = claim.crc;
	identity->origin_owner_incarnation = 70;
	identity->root_lineage_seq = 5;
	UT_ASSERT(cluster_recovery_duty_key_valid_v1(identity));
	rejoin_snapshot.root_publish_seq = 7;
	rejoin_snapshot.lifecycle = lifecycle;
	rejoin_refresh_token();
	rejoin_lookup_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	rejoin_import_result = CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
	rejoin_proven_incarnation = 77;
	rejoin_reads = rejoin_imports = rejoin_publications = 0;
	rejoin_prepares = rejoin_publishes = rejoin_fences = 0;
	rejoin_marker_poll = CLUSTER_MARKER_POLL_ACKED;
	rejoin_marker_result = CLUSTER_JOIN_MARKER_SUBMIT_ACK;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	rejoin_stage_reset();
}

static uint64
rejoin_timeout_debt(uint64 before)
{
	ClusterCfAttemptObservation observed;
	ClusterControlRequestView view;
	uint64 cookie = cluster_cf_owner_cookie(ExclusiveLock);

	UT_ASSERT(cluster_cf_last_attempt_since(before, &observed));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&observed));
	UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cf_hold_x.owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	UT_ASSERT(cookie != 0);
	UT_ASSERT(!root_publish_authority.active);
	UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes | entry_waits, 0);
	UT_ASSERT(cluster_control_request_snapshot(&cf_hold_x.owner.attempt, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	printf(
		"# rejoin-cf lifecycle=%u cookie=" UINT64_FORMAT
		" key=%u/%u/%u/%u/%u/%u mode=%d holder=%u/%u/" UINT64_FORMAT "/" UINT64_FORMAT
		" registry=%u/" UINT64_FORMAT " creator=%d/%u/" UINT64_FORMAT
		" stage=%d rejects=" UINT64_FORMAT "\n",
		rejoin_snapshot.lifecycle, cookie, view.message.key.resid.type,
		view.message.key.resid.lockmethodid, view.message.key.resid.field1,
		view.message.key.resid.field2, view.message.key.resid.field3, view.message.key.resid.field4,
		(int)view.mode, view.message.key.holder.node_id, view.message.key.holder.procno,
		view.message.key.holder.cluster_epoch, view.message.key.holder.request_id, view.handle.slot,
		view.handle.generation, view.owner.pid, view.owner.procno, view.owner.incarnation,
		rejoin_stage.async.has_staged_event, pg_atomic_read_u64(&rejoin_state.join_reject_count));
	return cookie;
}

static void
rejoin_retire(uint64 cookie)
{
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, cookie));
	UT_ASSERT(cluster_control_request_empty());
}

static void
rejoin_finish(void)
{
	prepared_finish();
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
}

/* Desired continuity assertion: a CF wait is not a rejected admission. Keep
 * opt-in RED separate from the safety characterization until policy freezes. */
UT_TEST(rejoin_cf_wait_keeps_original_staged_publication)
{
	const uint32 lifecycles[] = { CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED,
								  CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE };

	for (unsigned i = 0; i < lengthof(lifecycles); i++) {
		uint64 before;

		rejoin_fixture(lifecycles[i]);
		before = cluster_cf_attempt_sequence();
		UT_ASSERT(cluster_reconfig_poll_join_commit_stage());
		(void)rejoin_timeout_debt(before);
		UT_ASSERT(rejoin_stage.async.has_staged_event);
		UT_ASSERT_EQ(pg_atomic_read_u64(&rejoin_state.join_reject_count), 0);
		rejoin_finish();
	}
}

/* Break caught: CF timeout leaks MEMBER/fence work or discards cleanup debt. */
UT_TEST(rejoin_cf_slice_releases_stage_but_keeps_original_cleanup)
{
	const uint32 lifecycles[] = { CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED,
								  CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE };

	for (unsigned i = 0; i < lengthof(lifecycles); i++) {
		uint64 before, cookie;

		rejoin_fixture(lifecycles[i]);
		before = cluster_cf_attempt_sequence();
		UT_ASSERT(cluster_reconfig_poll_join_commit_stage());
		cookie = rejoin_timeout_debt(before);
		UT_ASSERT(!rejoin_stage.async.has_staged_event);
		UT_ASSERT_EQ(pg_atomic_read_u64(&rejoin_state.join_reject_count), 1);
		UT_ASSERT_EQ(rejoin_reads, 1);
		UT_ASSERT_EQ(rejoin_imports, 1);
		UT_ASSERT_EQ(rejoin_publications, 1);
		UT_ASSERT_EQ(rejoin_prepares | rejoin_publishes | rejoin_fences, 0);
		rejoin_retire(cookie);
		UT_ASSERT(!cluster_reconfig_poll_join_commit_stage());
		UT_ASSERT_EQ(rejoin_reads, 1);
		UT_ASSERT_EQ(rejoin_imports, 1);
		rejoin_finish();
	}
}

/* Break caught: a fresh upstream attempt reuses the old token/lineage/JCMK. */
UT_TEST(rejoin_fresh_attempt_rechecks_original_root_and_jcmk)
{
	uint64 before, first, second;

	rejoin_fixture(CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	before = cluster_cf_attempt_sequence();
	UT_ASSERT(!rejoin_owner(0, 77));
	first = rejoin_timeout_debt(before);
	UT_ASSERT_EQ(rejoin_seen_token.root_publish_seq, 7);
	UT_ASSERT_EQ(rejoin_seen_patch.desired.identity.root_lineage_seq, 6);
	rejoin_retire(first);
	rejoin_snapshot.root_publish_seq = 8;
	rejoin_snapshot.identity.root_lineage_seq = 6;
	rejoin_snapshot.identity.origin_owner_incarnation = 71;
	rejoin_refresh_token();
	before = cluster_cf_attempt_sequence();
	UT_ASSERT(!rejoin_owner(0, 77));
	second = rejoin_timeout_debt(before);
	UT_ASSERT(first != second);
	UT_ASSERT_EQ(rejoin_reads, 2);
	UT_ASSERT_EQ(rejoin_imports, 2);
	UT_ASSERT_EQ(rejoin_publications, 2);
	UT_ASSERT_EQ(rejoin_seen_token.root_publish_seq, 8);
	UT_ASSERT_EQ(rejoin_seen_patch.desired.identity.root_lineage_seq, 7);
	rejoin_finish();
}

/* Break caught: stale/missing fresh ROOT or JCMK proof reaches another CF. */
UT_TEST(rejoin_fresh_proof_drift_never_reuses_consumed_publication)
{
	for (unsigned field = 0; field < 6; field++) {
		uint64 before, cookie;

		rejoin_fixture(CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
		before = cluster_cf_attempt_sequence();
		UT_ASSERT(!rejoin_owner(0, 77));
		cookie = rejoin_timeout_debt(before);
		rejoin_retire(cookie);
		if (field == 0)
			rejoin_lookup_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else if (field == 1)
			rejoin_snapshot.identity.thread_claim_crc32c ^= 1;
		else if (field == 2)
			rejoin_snapshot.identity.root_lineage_seq = UINT64_MAX;
		else if (field == 3)
			rejoin_snapshot.identity.origin_owner_incarnation = 77;
		else if (field == 4)
			rejoin_import_result = CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE;
		else
			rejoin_proven_incarnation = 78;
		before = cluster_cf_attempt_sequence();
		UT_ASSERT(!rejoin_owner(0, 77));
		UT_ASSERT_EQ(rejoin_reads, 2);
		UT_ASSERT_EQ(rejoin_imports, field >= 4 ? 2 : 1);
		UT_ASSERT_EQ(rejoin_publications, 1);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), before);
		UT_ASSERT(!root_publish_authority.active);
		rejoin_finish();
	}
}

/* Break caught: marker wait/failure accidentally consumes ROOT authority. */
UT_TEST(rejoin_staged_marker_wait_is_not_a_root_wait)
{
	const ClusterMarkerPollResult polls[]
		= { CLUSTER_MARKER_POLL_IDLE, CLUSTER_MARKER_POLL_PENDING, CLUSTER_MARKER_POLL_TIMEOUT,
			CLUSTER_MARKER_POLL_ACKED };

	for (unsigned i = 0; i < lengthof(polls); i++) {
		uint64 before;

		rejoin_fixture(CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
		rejoin_marker_poll = polls[i];
		rejoin_marker_result = CLUSTER_JOIN_MARKER_SUBMIT_FAILED;
		before = cluster_cf_attempt_sequence();
		UT_ASSERT(cluster_reconfig_poll_join_commit_stage());
		UT_ASSERT_EQ(rejoin_stage.async.has_staged_event, i < 2);
		UT_ASSERT_EQ(rejoin_reads | rejoin_imports | rejoin_publications, 0);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), before);
		rejoin_finish();
	}
}

/* Break caught: already-OPEN skips current JCMK proof or republishes lineage. */
UT_TEST(rejoin_open_exact_owner_still_requires_fresh_import)
{
	for (unsigned changed = 0; changed < 3; changed++) {
		uint64 before;

		rejoin_fixture(CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		rejoin_snapshot.identity.origin_owner_incarnation = 77;
		if (changed == 1)
			rejoin_import_result = CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE;
		else if (changed == 2)
			rejoin_proven_incarnation = 78;
		before = cluster_cf_attempt_sequence();
		UT_ASSERT_EQ(rejoin_owner(0, 77), changed == 0);
		UT_ASSERT_EQ(rejoin_reads, 1);
		UT_ASSERT_EQ(rejoin_imports, 1);
		UT_ASSERT_EQ(rejoin_publications, 0);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), before);
		UT_ASSERT_EQ(rejoin_snapshot.identity.root_lineage_seq, 5);
		rejoin_finish();
	}
}
