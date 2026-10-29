/* Generic publication's one-use authority joined to actual CF retirement.
 * WALR/physical storage are explicit leaves. Only the refusal path is
 * exercised; these tests do not authorize a publication or upstream rebind.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootReadToken generic_token;
static ClusterControlRootPatch generic_patch;
static ClusterControlRootPublishReason generic_reason;
static unsigned generic_pin_begins, generic_pin_ends;
static bool generic_pin_active, generic_pin_confirmed;
static bool generic_rejoin_io;
static void (*generic_after_cf_read)(void);

static ClusterWalPinResult
generic_pin_begin(const ClusterControlRootReadToken *token, bool sealed,
				  ClusterWalRootPublishGuard **out)
{
	UT_ASSERT(!root_publish_authority.active);
	UT_ASSERT_EQ(memcmp(token, &generic_token, sizeof(*token)), 0);
	UT_ASSERT(!sealed && !generic_pin_active);
	++generic_pin_begins;
	generic_pin_active = true;
	*out = (ClusterWalRootPublishGuard *)1;
	return CLUSTER_WAL_PIN_OK;
}

static ClusterWalrReleaseResult
generic_pin_end(ClusterWalRootPublishGuard **guard)
{
	UT_ASSERT(generic_pin_active && *guard == (ClusterWalRootPublishGuard *)1);
	++generic_pin_ends;
	if (!generic_pin_confirmed)
		return CLUSTER_WALR_RELEASE_UNCONFIRMED;
	generic_pin_active = false;
	*guard = NULL;
	return CLUSTER_WALR_RELEASE_CONFIRMED;
}

static ClusterControlRootResult
generic_snapshot(const ClusterControlRootSnapshot *snapshot pg_attribute_unused(),
				 uint16 thread pg_attribute_unused(), uint64 sysid pg_attribute_unused(),
				 const uint8 storage[16] pg_attribute_unused(),
				 const uint8 authority[16] pg_attribute_unused(), bool v2 pg_attribute_unused())
{
	if (generic_rejoin_io) {
		UT_ASSERT_EQ(snapshot->identity.origin_owner_incarnation, 77);
		UT_ASSERT_EQ(snapshot->identity.root_lineage_seq, 6);
		UT_ASSERT_EQ(snapshot->lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	UT_ASSERT(false); /* No storage path is expected after a failed CF. */
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}

/* Physical publication remains a leaf; the generic CAS/one-use authority,
 * exact CF owner and readback/token paths above and below it are real. */
static void
generic_encode_record(uint8 *dst, const ClusterControlRootSnapshot *record, uint64 incarnation,
					  uint32 node, ClusterControlRootPublishReason reason)
{
	if (!generic_rejoin_io) {
		prepared_encode_record(dst, record, incarnation, node, reason);
		return;
	}
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(incarnation, 77);
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT(reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
			  || reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN);
	memcpy(dst, record, Min(sizeof(*record), CLUSTER_CONTROL_ROOT_RECORD_BYTES));
}

static bool
generic_updated(const ControlRootImage *old, const ControlRootImage *next)
{
	if (!generic_rejoin_io)
		return prepared_updated(old, next);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(next->header.file_txn_seq, old->header.file_txn_seq + 1);
	UT_ASSERT_EQ(next->records[0].identity.origin_owner_incarnation, 77);
	UT_ASSERT_EQ(next->records[0].identity.root_lineage_seq, 6);
	++prepared_writes;
	prepared_disk = *next;
	return true;
}

static ClusterControlRootResult
generic_read_pair(ControlRootImage *primary, ControlRootImage *bak)
{
	ClusterControlRootResult result = prepared_read_pair(primary, bak);

	if (generic_after_cf_read != NULL)
		generic_after_cf_read();
	return result;
}

#define patch_shape_valid generic_shape
#define reason_mask generic_reason_mask
#define snapshot_reserved_zero generic_reserved
#define acquire_clusterwide_cf prepared_acquire
#define release_cf prepared_release
#define storage_contract_check prepared_storage
#define read_canonical_pair generic_read_pair
#define encode_record generic_encode_record
#define encode_header prepared_encode_header
#define publish_updated_image generic_updated
#define build_control_path prepared_path
#define read_one_image prepared_read_one
#define snapshot_validate generic_snapshot
#define cluster_wal_retention_root_publish_begin_exact generic_pin_begin
#define cluster_wal_retention_root_publish_end generic_pin_end
#include "test_cluster_generic_cf.inc"
#undef cluster_wal_retention_root_publish_end
#undef cluster_wal_retention_root_publish_begin_exact
#undef snapshot_validate
#undef read_one_image
#undef build_control_path
#undef publish_updated_image
#undef encode_header
#undef encode_record
#undef read_canonical_pair
#undef storage_contract_check
#undef release_cf
#undef acquire_clusterwide_cf
#undef snapshot_reserved_zero
#undef reason_mask
#undef patch_shape_valid

static ClusterControlRootPatch
generic_valid_patch(ClusterControlRootPublishReason reason)
{
	ClusterControlRootPatch patch = { 0 };

	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	switch (reason) {
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN:
		patch.mask = UINT64_C(0x3b);
		patch.expected_lifecycle = reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
									   ? CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
									   : CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
		patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
		patch.desired.identity.origin_owner_incarnation = 100;
		patch.desired.identity.root_lineage_seq = 2;
		break;
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE:
		patch.mask = UINT64_C(0x39);
		patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		break;
	case CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE:
		patch.mask = UINT64_C(0x38);
		break;
	case CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY:
		patch.mask = UINT64_C(0x40);
		patch.desired.root_flags = CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
		break;
	default:
		UT_ASSERT(false);
	}
	UT_ASSERT(generic_shape(&patch, reason));
	return patch;
}

static void
generic_fixture(ClusterControlRootPublishReason reason)
{
	prepared_fixture(false);
	generic_rejoin_io = false;
	generic_after_cf_read = NULL;
	cluster_control_root_publish_authority_clear_v1();
	generic_reason = reason;
	generic_pin_begins = generic_pin_ends = 0;
	generic_pin_active = false;
	generic_pin_confirmed = true;
	prepared_disk.records[0].root_publish_seq = 7;
	make_read_token(&prepared_disk, 1, CONTROL_ROOT_SOURCE_PRIMARY, &generic_token);
	generic_patch = generic_valid_patch(reason);
}

static ClusterControlRootResult
generic_call(const ClusterControlRootReadToken *token, const ClusterControlRootPatch *patch,
			 ClusterControlRootPublishReason reason)
{
	ClusterControlRootSnapshot out, zero = { 0 };
	ClusterControlRootReadToken out_token, zero_token = { 0 };
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	memset(&out_token, 0xa5, sizeof(out_token));
	result = cluster_control_root_compare_and_publish(token, patch, reason, &out, &out_token);
	UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(zero)), 0);
	UT_ASSERT_EQ(memcmp(&out_token, &zero_token, sizeof(zero_token)), 0);
	UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes | entry_waits, 0);
	return result;
}

static void
generic_timeout(void)
{
	ClusterCfAttemptObservation observed;
	uint64 before = cluster_cf_attempt_sequence();

	UT_ASSERT(cluster_control_root_publish_authority_bind_v1(&generic_token, &generic_patch,
															 generic_reason));
	UT_ASSERT_EQ(generic_call(&generic_token, &generic_patch, generic_reason),
				 generic_pin_confirmed ? CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE
									   : CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	UT_ASSERT(cluster_cf_last_attempt_since(before, &observed));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&observed));
	UT_ASSERT(!root_publish_authority.active);
	prepared_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT(prepared_cookie != 0 && cluster_cf_held(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT(cluster_control_request_snapshot(&cf_hold_x.owner.attempt, &prepared_request));
	UT_ASSERT_EQ(prepared_request.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	printf("# generic-cf reason=%d cookie=" UINT64_FORMAT
		   " key=%u/%u/%u/%u/%u/%u mode=%d holder=%u/%u/" UINT64_FORMAT "/" UINT64_FORMAT
		   " registry=%u/" UINT64_FORMAT " creator=%d/%u/" UINT64_FORMAT " state=%d authority=%d\n",
		   (int)generic_reason, prepared_cookie, prepared_request.message.key.resid.type,
		   prepared_request.message.key.resid.lockmethodid,
		   prepared_request.message.key.resid.field1, prepared_request.message.key.resid.field2,
		   prepared_request.message.key.resid.field3, prepared_request.message.key.resid.field4,
		   (int)prepared_request.mode, prepared_request.message.key.holder.node_id,
		   prepared_request.message.key.holder.procno,
		   prepared_request.message.key.holder.cluster_epoch,
		   prepared_request.message.key.holder.request_id, prepared_request.handle.slot,
		   prepared_request.handle.generation, prepared_request.owner.pid,
		   prepared_request.owner.procno, prepared_request.owner.incarnation,
		   (int)prepared_request.state, root_publish_authority.active);
}

/* Break caught: retiring the old request revives the already-consumed
 * publication proof and issues another CF request without its publisher. */
UT_TEST(generic_cf_refusal_consumes_authority_before_any_retry)
{
	const ClusterControlRootPublishReason reasons[]
		= { CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
			CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE,
			CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE,
			CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY };

	for (unsigned i = 0; i < lengthof(reasons); i++) {
		int seven;
		uint64 sequence;

		generic_fixture(reasons[i]);
		generic_timeout();
		seven = g_seven_count;
		UT_ASSERT(acknowledge_retirements());
		cluster_cf_retirement_poll();
		UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
		UT_ASSERT(cluster_control_request_empty());
		printf("# generic-cf completion reason=%d cookie=" UINT64_FORMAT
			   " receipt=1 registry_empty=1\n",
			   (int)generic_reason, prepared_cookie);
		sequence = cluster_cf_attempt_sequence();
		UT_ASSERT_EQ(generic_call(&generic_token, &generic_patch, generic_reason),
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT_EQ(g_seven_count, seven);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
		UT_ASSERT_EQ(generic_pin_begins, i == 4 ? 0 : 1);
		UT_ASSERT_EQ(generic_pin_ends, generic_pin_begins);
		UT_ASSERT(!generic_pin_active);
		prepared_finish();
	}
}

/* Break caught: a mismatched nested caller consumes another publisher's
 * bound proof, or touches CF/storage before exact token/patch/reason match. */
UT_TEST(generic_cf_drift_never_consumes_a_different_publication)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterControlRootReadToken token;
		ClusterControlRootPatch patch;
		ClusterControlRootPublishReason reason;
		uint64 sequence;

		generic_fixture(CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN);
		token = generic_token;
		patch = generic_patch;
		reason = generic_reason;
		if (field == 0)
			token.root_publish_seq++;
		else if (field == 1)
			patch.desired.root_flags ^= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
		else {
			reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN;
			patch = generic_valid_patch(reason);
		}
		UT_ASSERT(generic_shape(&patch, reason));
		UT_ASSERT(cluster_control_root_publish_authority_bind_v1(&generic_token, &generic_patch,
																 generic_reason));
		sequence = cluster_cf_attempt_sequence();
		UT_ASSERT_EQ(generic_call(&token, &patch, reason), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(root_publish_authority.active);
		UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
		UT_ASSERT_EQ(generic_pin_begins | generic_pin_ends | prepared_acquires, 0);
		/* The unchanged original proof is still consumable exactly once. */
		UT_ASSERT_EQ(generic_call(&generic_token, &generic_patch, generic_reason),
					 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
		UT_ASSERT(!root_publish_authority.active);
		UT_ASSERT_EQ(prepared_acquires, 1);
		prepared_finish();
	}
}

/* Break caught: a WALR cleanup refusal restores publication permission.
 * Pin ownership here is a leaf, not a native retained-WALR proof. */
UT_TEST(generic_cf_unconfirmed_pin_release_never_restores_authority)
{
	uint64 sequence;

	generic_fixture(CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN);
	generic_pin_confirmed = false;
	generic_timeout();
	UT_ASSERT(generic_pin_active && !root_publish_authority.active);
	sequence = cluster_cf_attempt_sequence();
	UT_ASSERT_EQ(generic_call(&generic_token, &generic_patch, generic_reason),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_cf_attempt_sequence(), sequence);
	UT_ASSERT_EQ(generic_pin_begins, 1);
	UT_ASSERT_EQ(generic_pin_ends, 1);
	/* Explicit fixture teardown of its simulated retained pin, not a new
	 * completion receipt or a production cleanup policy. */
	generic_pin_active = false;
	prepared_finish();
}
