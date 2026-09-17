/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_duty.c
 *	  RF-ROOT P2 tests for the no-generation recovery-duty identity.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>

#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_semantic_activation.h" /* ACK stage enum (G3) */
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "common/sha2.h"

#include "../../backend/cluster/cluster_control_root_private.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterControlRootResult ut_root_lookup_result;
static ClusterControlRootResult ut_root_publish_result;
static ClusterRecoveryOwnerImportResult ut_owner_read_result;
static ClusterControlRootIdentity ut_root_identity;
static ClusterControlRootSnapshot ut_root_snapshot;
static ClusterControlRootReadToken ut_root_token;
static uint64 ut_owner_read_incarnation;
static int ut_owner_read_calls;
static int ut_root_publish_calls;
static ClusterControlRootPatch ut_root_published_patch;
static ClusterControlRootPublishReason ut_root_published_reason;
static bool ut_root_publish_context_authorized;
static bool ut_root_publish_mutate_token;
/* RF-ROOT P6 contract: the commit-time re-vet reads the durable
 * clean-departed evidence to distinguish the missed-clean-close repair
 * (OPEN root under the old owner, clean-departed) from a crash-rejoin
 * commit racing the FSM (stays fail-closed). */
static bool ut_clean_departed = false;
/* recovery path: fail the next compare_and_publish once (transient refusal). */
static bool ut_publish_fail_once = false;

/* RF-ROOT P7 recovery path: the checkpointer's bounded THREAD_CLEAN_CLOSE retry
 * stubs — an advancing fake clock, an immediate latch, and a controllable
 * leaver serving rebind. */
#include "storage/latch.h"
static Latch ut_retry_latch;
Latch *MyLatch = &ut_retry_latch;
static TimestampTz ut_now_us = 1700000000000000LL;
static int ut_waitlatch_calls = 0;
static bool ut_serving_rebind_ok = false;

TimestampTz
GetCurrentTimestamp(void)
{
	return ut_now_us;
}

TimestampTz
TimestampTzPlusMilliseconds(TimestampTz t, int64 ms)
{
	return t + (TimestampTz)ms * 1000;
}

int
WaitLatch(Latch *latch, int wakeEvents, long timeout, uint32 wait_event_info)
{
	(void)latch;
	(void)wakeEvents;
	(void)timeout;
	(void)wait_event_info;
	ut_waitlatch_calls++;
	/* Advance the fake clock 100ms per backoff so the bounded-retry
	 * deadline tests complete quickly. */
	ut_now_us += 100000;
	return WL_TIMEOUT;
}

void
ResetLatch(Latch *latch)
{
	(void)latch;
}

volatile sig_atomic_t InterruptPending = 0;

void
ProcessInterrupts(void)
{}

bool
cluster_authority_serving_rebind_leaver(void)
{
	return ut_serving_rebind_ok;
}

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 old_node_id,
												  ClusterControlRootIdentity *out_identity,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	(void)old_node_id;
	if (ut_root_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| ut_root_lookup_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*out_identity = ut_root_identity;
		*out_snapshot = ut_root_snapshot;
		*out_token = ut_root_token;
	}
	return ut_root_lookup_result;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
									  uint64 frozen_admitted_bitmap_low,
									  uint64 frozen_admitted_bitmap_high, uint64 *out_incarnation)
{
	(void)node_id;
	(void)immutable_claim;
	(void)frozen_admitted_bitmap_low;
	(void)frozen_admitted_bitmap_high;
	ut_owner_read_calls++;
	*out_incarnation = ut_owner_read_incarnation;
	return ut_owner_read_result;
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(const ClusterControlRootReadToken *expected_token,
										 const ClusterControlRootPatch *patch,
										 ClusterControlRootPublishReason reason,
										 ClusterControlRootSnapshot *out_snapshot,
										 ClusterControlRootReadToken *out_token)
{
	ClusterControlRootReadToken observed_token = *expected_token;

	ut_root_publish_calls++;
	/* recovery path retry test: fail the NEXT publish attempt once (transient S1
	 * serving-stale refusal), then behave normally. */
	if (ut_publish_fail_once) {
		ut_publish_fail_once = false;
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	}
	ut_root_published_patch = *patch;
	ut_root_published_reason = reason;
	if (ut_root_publish_mutate_token)
		observed_token.root_publish_seq++;
	ut_root_publish_context_authorized
		= cluster_control_root_publish_authority_current_v1(&observed_token, patch, reason);
	if (!ut_root_publish_context_authorized)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* Mirror the real CAS monotonicity (control_root.c compare_and_publish,
	 * OWNER_REJOIN / THREAD_OPEN): desired owner must strictly exceed the
	 * current owner and lineage must advance by exactly one.  The head gate
	 * no longer pre-rejects the CLOSED branch (verification split), so
	 * a stale / same-incarnation reopen must fail HERE like the real CAS. */
	if (ut_root_publish_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
			|| reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN)
		&& (ut_root_snapshot.identity.root_lineage_seq == UINT64_MAX
			|| patch->desired.identity.root_lineage_seq
				   != ut_root_snapshot.identity.root_lineage_seq + 1
			|| patch->desired.identity.origin_owner_incarnation
				   <= ut_root_snapshot.identity.origin_owner_incarnation))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (ut_root_publish_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out_snapshot = ut_root_snapshot;
		/* Mirror the real apply_patch: only MASKED fields change. */
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE) != 0)
			out_snapshot->lifecycle = patch->desired.lifecycle;
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE) != 0) {
			out_snapshot->identity.origin_owner_incarnation
				= patch->desired.identity.origin_owner_incarnation;
			out_snapshot->identity.root_lineage_seq = patch->desired.identity.root_lineage_seq;
		}
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT) != 0) {
			out_snapshot->checkpoint_tli = patch->desired.checkpoint_tli;
			out_snapshot->checkpoint_source_kind = patch->desired.checkpoint_source_kind;
			out_snapshot->checkpoint_lower_lsn = patch->desired.checkpoint_lower_lsn;
			out_snapshot->checkpoint_record_crc32c = patch->desired.checkpoint_record_crc32c;
		}
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_TAIL) != 0) {
			out_snapshot->tail_tli = patch->desired.tail_tli;
			out_snapshot->tail_validation_kind = patch->desired.tail_validation_kind;
			out_snapshot->validated_tail_lsn_exclusive
				= patch->desired.validated_tail_lsn_exclusive;
			out_snapshot->tail_last_record_lsn = patch->desired.tail_last_record_lsn;
			out_snapshot->tail_last_record_crc32c = patch->desired.tail_last_record_crc32c;
		}
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS) != 0) {
			out_snapshot->root_flags = patch->desired.root_flags;
			out_snapshot->recovered_tli = patch->desired.recovered_tli;
			out_snapshot->recovered_through_lsn_exclusive
				= patch->desired.recovered_through_lsn_exclusive;
			out_snapshot->recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
			out_snapshot->recovered_last_record_crc32c
				= patch->desired.recovered_last_record_crc32c;
		}
		if ((patch->mask & CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY) != 0)
			out_snapshot->root_flags = patch->desired.root_flags;
		memset(out_token, 0, sizeof(*out_token));
	}
	return ut_root_publish_result;
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

bool
cluster_reconfig_capture_formation_snapshot_v1(uint16 origin_thread,
											   ClusterFormationSnapshotV1 *out)
{
	(void)origin_thread;
	(void)out;
	return false;
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	(void)out;
	return CLUSTER_FENCE_AUTHORITY_NO_MAJORITY;
}

uint64
cluster_write_fence_authority_cache_sequence(void)
{
	return 0;
}

bool
cluster_write_fence_authority_cache_publish_if_unchanged(const ClusterFenceMarker *marker,
														 uint64 published_at_us,
														 uint64 expected_sequence)
{
	(void)marker;
	(void)published_at_us;
	(void)expected_sequence;
	return false;
}

ClusterFenceAuthorityCacheResult
cluster_write_fence_revalidate_cached_nowait(const ClusterFenceMarker *expected, uint64 now_us)
{
	(void)expected;
	(void)now_us;
	return CLUSTER_FENCE_CACHE_INVALID;
}

/* RF-ROOT P6 (L4 admission / phase-3 gate diag refs): cluster_recovery_duty.o
 * samples the live-component predicates; the pure unit pins them inert so the
 * binary stays standalone (the formation-witness paths under test use the
 * dedicated fixture mocks above). */
int cluster_node_id = 0;

bool
cluster_cssd_get_status(void)
{
	return false;
}

int
cluster_qvotec_get_status(void)
{
	return 0;
}

bool
cluster_qvotec_in_quorum(void)
{
	return false;
}

bool
cluster_membership_is_member(int32 node_id pg_attribute_unused())
{
	return false;
}

bool
cluster_reconfig_self_join_admitted(void)
{
	return false;
}

bool
cluster_lms_is_recovery_ready(void)
{
	return false;
}

int
cluster_current_phase(void)
{
	return 0;
}

bool
cluster_recovery_transport_components_current(void)
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

static void
put_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
put_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

static void
sha256_bytes(const uint8 *bytes, size_t len, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, bytes, len) < 0
		|| pg_cryptohash_final(ctx, out, PG_SHA256_DIGEST_LENGTH) < 0)
		abort();
	pg_cryptohash_free(ctx);
}

static void
build_valid_key(ClusterRecoveryDutyKey *key)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(key, 0, sizeof(*key));
	key->system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 16; i++) {
		key->storage_uuid[i] = (uint8)(i + 1);
		key->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	key->authority_uuid[6] = 0x46;
	key->authority_uuid[8] = 0x8a;
	key->origin_thread_id = 4;
	key->origin_node_id = 3;
	key->thread_claim_created_at = INT64_C(1700000000000123);
	cluster_wal_thread_claim_fill(&claim, key->origin_thread_id, key->origin_node_id,
								  key->thread_claim_created_at);
	key->thread_claim_crc32c = claim.crc;
	key->origin_owner_incarnation = UINT64_C(0x1122334455667788);
	key->root_lineage_seq = UINT64_C(9);
}

static void
setup_owner_rejoin(uint64 old_incarnation, uint64 new_incarnation)
{
	memset(&ut_root_identity, 0, sizeof(ut_root_identity));
	build_valid_key(&ut_root_identity);
	ut_root_snapshot = (ClusterControlRootSnapshot){ 0 };
	ut_root_snapshot.identity = ut_root_identity;
	ut_root_snapshot.identity.origin_owner_incarnation = old_incarnation;
	ut_root_identity = ut_root_snapshot.identity;
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	ut_root_snapshot.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	ut_root_snapshot.checkpoint_tli = 1;
	ut_root_snapshot.tail_tli = 1;
	ut_root_snapshot.recovered_tli = 1;
	ut_root_snapshot.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	ut_root_snapshot.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	ut_root_snapshot.checkpoint_lower_lsn = UINT64_C(0x1000000);
	ut_root_snapshot.validated_tail_lsn_exclusive = UINT64_C(0x1100000);
	ut_root_snapshot.recovered_through_lsn_exclusive = UINT64_C(0x1100000);
	ut_root_snapshot.checkpoint_record_crc32c = UINT32_C(0x33445566);
	memset(&ut_root_token, 0, sizeof(ut_root_token));
	ut_root_token.origin_thread_id = ut_root_identity.origin_thread_id;
	ut_root_lookup_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	ut_owner_read_result = CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
	ut_owner_read_incarnation = new_incarnation;
	ut_owner_read_calls = 0;
	ut_root_publish_calls = 0;
	ut_root_publish_context_authorized = false;
	ut_root_publish_mutate_token = false;
	ut_clean_departed = false;
	ut_publish_fail_once = false;
	memset(&ut_root_published_patch, 0, sizeof(ut_root_published_patch));
}

/* Link-only stub for the durable clean-departed evidence read (increment
 * 21 repair routing); the fixture sets ut_clean_departed. */
bool
cluster_reconfig_is_clean_departed(int32 node_id pg_attribute_unused())
{
	return ut_clean_departed;
}

/* RF-ROOT P7 G3: the R4 cutover coordinator proof's ACK-complete read.
 * The fixture controls the verdict + records the round identity the proof
 * presented and the minimum stage demanded. */
static bool ut_ack_complete_ok = false;
static int ut_ack_complete_calls = 0;
static uint32 ut_ack_min_stage = 0;

bool
cluster_semantic_activation_ack_complete_matches(
	uint64 transition_epoch, uint64 record_generation, uint64 expected_members_lo,
	uint64 expected_members_hi, uint64 source_feature_bitmap, uint64 target_feature_bitmap,
	uint64 capability_sample_digest, ClusterSemanticActivationAckStage minimum_stage)
{
	ut_ack_complete_calls++;
	ut_ack_min_stage = (uint32)minimum_stage;
	(void)transition_epoch;
	(void)record_generation;
	(void)expected_members_lo;
	(void)expected_members_hi;
	(void)source_feature_bitmap;
	(void)target_feature_bitmap;
	(void)capability_sample_digest;
	return ut_ack_complete_ok;
}

/* RF-ROOT P7 G4: the runtime census stub.  contract follow-up: the census
 * gate moved from the activate proof to the bit22 latch apply (cutover
 * round binding), so this stub now only serves the "census must not gate
 * activate" assertion in test_activate_authority_requires_complete_ack_round.
 */
static bool ut_census_ok = false;

bool
cluster_wal_state_correctness_census_ok(void)
{
	return ut_census_ok;
}

/* Stateless pure predicate; replicate the production whitelist so the
 * coordinator proof's known-bit gate is exercised with real semantics. */
bool
cluster_control_root_feature_bitmap_is_known(uint64 active_feature_bitmap)
{
	return (active_feature_bitmap & ~PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1) == 0;
}

UT_TEST(test_checkpoint_advance_publishes_canonical_bound)
{
	/* RF-ROOT P7 G1a: the checkpointer's canonical checkpoint advertisement
	 * (CHECKPOINT_ADVANCE, frozen 0x38 shape) advances the root's
	 * checkpoint_lower_lsn + record CRC + the validated tail (the just
	 * written checkpoint record is the WAL-extent validation point);
	 * owner lineage untouched. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(cluster_control_root_checkpoint_advance_publish(
		UINT64_C(0x2000000), 1, UINT64_C(0x1fffff0), UINT64_C(0x2000020), UINT32_C(0x44556677)));
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(ut_root_publish_context_authorized);
	UT_ASSERT_EQ((int)ut_root_published_reason,
				 (int)CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE);
	UT_ASSERT_EQ(ut_root_published_patch.mask, UINT64_C(0x38));
	UT_ASSERT_EQ(ut_root_published_patch.expected_lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	/* The 0x38 mask has no LIFECYCLE bit: desired.lifecycle stays 0. */
	UT_ASSERT_EQ(ut_root_published_patch.desired.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED);
	UT_ASSERT_EQ(ut_root_published_patch.desired.checkpoint_lower_lsn, UINT64_C(0x2000000));
	UT_ASSERT_EQ(ut_root_published_patch.desired.checkpoint_record_crc32c, UINT32_C(0x44556677));
	UT_ASSERT_EQ(ut_root_published_patch.desired.validated_tail_lsn_exclusive, UINT64_C(0x2000020));
	UT_ASSERT_EQ(ut_root_published_patch.desired.tail_last_record_lsn, UINT64_C(0x1fffff0));
	UT_ASSERT_EQ(ut_root_published_patch.desired.tail_last_record_crc32c, UINT32_C(0x44556677));
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.origin_owner_incarnation, 0);
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.root_lineage_seq, 0);

	/* A non-advancing redo (<= current bound) is a no-op: zero publishes. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(!cluster_control_root_checkpoint_advance_publish(
		UINT64_C(0x1000000), 1, UINT64_C(0xfffff0), UINT64_C(0x1000020), UINT32_C(0x44556677)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	/* Not OPEN (e.g. CLOSED / RECOVERY_COMPLETE) is fail-closed. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT(!cluster_control_root_checkpoint_advance_publish(
		UINT64_C(0x2000000), 1, UINT64_C(0x1fffff0), UINT64_C(0x2000020), UINT32_C(0x44556677)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
}

UT_TEST(test_fpw_sticky_publishes_canonical_flag)
{
	/* RF-ROOT P7 G1a-2: the checkpointer's canonical FPW-off sticky
	 * (FPW_STICKY, frozen 0x40 shape) sets the root's FLAG_FPW_WAS_OFF. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(cluster_control_root_fpw_sticky_publish());
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(ut_root_publish_context_authorized);
	UT_ASSERT_EQ((int)ut_root_published_reason, (int)CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY);
	UT_ASSERT_EQ(ut_root_published_patch.mask, CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY);
	UT_ASSERT_EQ(ut_root_published_patch.expected_lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT((ut_root_published_patch.desired.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF)
			  != 0);

	/* Already sticky is a no-op (the apply_patch guard never clears it). */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_root_snapshot.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
	UT_ASSERT(cluster_control_root_fpw_sticky_publish());
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	/* Not OPEN is fail-closed. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT(!cluster_control_root_fpw_sticky_publish());
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
}

UT_TEST(test_create_authority_requires_complete_ack_round)
{
	/* RF-ROOT P7 G3 (verification: ACK-consumer boundary tests):
	 * the R4 cutover create authority is the coordinator's one-shot proof —
	 * refused for a non-coordinator, an incomplete ACK table, or a round
	 * without bit22; granted only when all gates hold. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;

	memset(&image, 0, sizeof(image));
	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 7;
	round.transition_epoch = 3;
	round.source_feature_bitmap = UINT64_C(1);
	round.target_feature_bitmap
		= UINT64_C(1) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = UINT64_C(0x03);
	round.admitted_bitmap_high = 0;
	round.capability_sample_digest = UINT64_C(0xabcd);
	round.coordinator_node_id = 0;
	round.coordinator_incarnation = 99;
	cluster_node_id = 0;

	/* RF-ROOT P9 verification (contract): the bit22 cutover round's create is
	 * EXEMPT from the SAMPLE-stage ACK precondition — granted even while
	 * the ACK table is not COMPLETE, and no ACK read happens at create
	 * (the W6 clause-3 CLOSED binding lives in the activate proof). */
	ut_ack_complete_ok = false;
	ut_ack_complete_calls = 0;
	UT_ASSERT(cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);

	/* Non-coordinator -> refused BEFORE any ACK read (fail-fast). */
	cluster_node_id = 1;
	ut_ack_complete_ok = true;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);

	/* Coordinator + bit22 target -> granted, still no ACK read. */
	cluster_node_id = 0;
	ut_ack_complete_calls = 0;
	UT_ASSERT(cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);

	/* Target WITHOUT bit22 -> the frozen SAMPLE-stage precondition binds
	 * (R4 round): incomplete ACK table refuses after exactly one read... */
	round.target_feature_bitmap = UINT64_C(1);
	ut_ack_complete_ok = false;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);

	/* ...and a complete SAMPLE round still cannot lift the whitelist:
	 * create_authority requires bit22 in the target for every round. */
	ut_ack_complete_ok = true;
	ut_ack_min_stage = 0;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);
	UT_ASSERT_EQ((int)ut_ack_min_stage, (int)CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE);

	/* Target with an UNKNOWN feature bit + bit22 -> refused (whitelist
	 * gate), with no ACK read (exempt round). */
	round.target_feature_bitmap
		= (UINT64_C(1) << 20) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_create_authority_current_v1(&image, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);
	round.target_feature_bitmap
		= UINT64_C(1) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
}

UT_TEST(test_activate_authority_requires_complete_ack_round)
{
	/* RF-ROOT P7 G3 (verification + contract follow-up): the activate
	 * proof demands coordinator identity, the PREPARED-stage all-member
	 * COMPLETE ACK bound to the round (W6 clause 3 CLOSED binding), and
	 * bit22 in the target.  The runtime census gate was REMOVED here in
	 * batch 3 — the census is the post-bit22 proof and binds inside the
	 * cutover round at the latch apply
	 * (cluster_r4_bit22_cutover_latch_apply), not as a pre-bit22
	 * precondition on activate.  Fail-closed on each remaining check. */
	ClusterControlRootFileToken token;
	ClusterControlRootMigrationRoundV1 round;
	uint8 sha[32];

	memset(&token, 0, sizeof(token));
	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 7;
	round.transition_epoch = 3;
	round.source_feature_bitmap = UINT64_C(1);
	round.target_feature_bitmap
		= UINT64_C(1) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = UINT64_C(0x03);
	round.admitted_bitmap_high = 0;
	round.capability_sample_digest = UINT64_C(0xabcd);
	round.coordinator_node_id = 0;
	round.coordinator_incarnation = 99;
	cluster_node_id = 0;
	memset(sha, 0x11, sizeof(sha));

	/* implementation: the census no longer gates activate — a RED census (deferred
	 * hw_remaster still linked) must NOT refuse the ACK-bound proof. */
	ut_census_ok = false;
	ut_ack_complete_ok = true;
	ut_ack_complete_calls = 0;
	UT_ASSERT(cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);

	/* ACK COMPLETE + bit22 target -> granted, and the activate proof
	 * demands the PREPARED stage (W6 clause 3). */
	ut_census_ok = true;
	ut_ack_min_stage = 0;
	UT_ASSERT(cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ((int)ut_ack_min_stage, (int)CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED);

	/* ACK not COMPLETE -> refused. */
	ut_ack_complete_ok = false;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);
	ut_ack_complete_ok = true;

	/* Non-coordinator -> refused BEFORE any ACK read (fail-fast). */
	cluster_node_id = 1;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);
	cluster_node_id = 0;

	/* Target WITHOUT bit22 -> refused (the bit22 cutover carrier). */
	round.target_feature_bitmap = UINT64_C(1);
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);
	round.target_feature_bitmap
		= UINT64_C(1) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;

	/* Target with an UNKNOWN feature bit -> refused (whitelist gate). */
	round.target_feature_bitmap
		= (UINT64_C(1) << 20) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, sha, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 1);
	round.target_feature_bitmap
		= UINT64_C(1) | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;

	/* NULL round / NULL sha -> refused before any read. */
	ut_ack_complete_calls = 0;
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, sha, NULL));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);
	UT_ASSERT(!cluster_control_root_activate_authority_current_v1(&token, NULL, &round));
	UT_ASSERT_EQ(ut_ack_complete_calls, 0);
}

UT_TEST(test_owner_rejoin_requires_jcmk_and_publishes_exact_root_cas)
{
	uint64 old_incarnation = UINT64_C(70);
	uint64 new_incarnation = UINT64_C(77);

	setup_owner_rejoin(old_incarnation, new_incarnation);
	UT_ASSERT(cluster_recovery_owner_rejoin_v1(3, new_incarnation));
	UT_ASSERT_EQ(ut_owner_read_calls, 1);
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(ut_root_publish_context_authorized);
	UT_ASSERT(!cluster_control_root_publish_authority_current_v1(
		&ut_root_token, &ut_root_published_patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN));
	UT_ASSERT_EQ(ut_root_published_patch.mask, UINT64_C(0x3b));
	UT_ASSERT_EQ(ut_root_published_patch.expected_lifecycle,
				 CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(ut_root_published_patch.desired.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.origin_owner_incarnation,
				 new_incarnation);
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.root_lineage_seq,
				 ut_root_identity.root_lineage_seq + 1);
}

UT_TEST(test_owner_rejoin_publication_context_rejects_token_drift)
{
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_publish_mutate_token = true;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(!ut_root_publish_context_authorized);
	UT_ASSERT(!cluster_control_root_publish_authority_current_v1(
		&ut_root_token, &ut_root_published_patch, CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN));
}

UT_TEST(test_recovery_complete_uses_exact_private_publish_authority)
{
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken published_token;
	ClusterControlRootResult result;

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	ut_root_token.lifecycle = ut_root_snapshot.lifecycle;
	memset(&patch, 0, sizeof(patch));
	patch.mask
		= CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.root_flags
		= ut_root_snapshot.root_flags | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	patch.desired.recovered_tli = ut_root_snapshot.tail_tli;
	patch.desired.recovered_through_lsn_exclusive = ut_root_snapshot.validated_tail_lsn_exclusive;
	result = cluster_control_root_recovery_complete_publish_v1(&ut_root_token, &patch, &published,
															   &published_token);
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(ut_root_publish_context_authorized);
	UT_ASSERT_EQ(ut_root_published_reason, CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
}

UT_TEST(test_owner_rejoin_rejects_open_stale_owner_frozen)
{
	/* recovery contract (recovery path, verification): the
	 * increment-21 coordinator-side missed-clean-close repair is removed —
	 * the OWNER (checkpointer) closes its own thread with a bounded retry.
	 * OPEN under any owner other than admitted stays fail-closed (the
	 * frozen OWNER_REJOIN shape), clean-departed or not. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_clean_departed = true;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
	UT_ASSERT_EQ(ut_owner_read_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_clean_departed = false;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
	ut_clean_departed = false;
}

UT_TEST(test_clean_close_retry_transient_refusal_then_success)
{
	/* recovery path: a transient S1 serving-stale refusal of THREAD_CLEAN_CLOSE
	 * is retried (with the leaver serving rebind + backoff) until it lands,
	 * bounded by the 5s deadline. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_publish_fail_once = true;
	ut_serving_rebind_ok = true;
	ut_now_us = 1700000000000000LL;
	ut_waitlatch_calls = 0;
	UT_ASSERT(cluster_control_root_thread_clean_close_publish_retry());
	UT_ASSERT_EQ(ut_root_publish_calls, 2); /* refused attempt + retry */
	UT_ASSERT_EQ(ut_waitlatch_calls, 1);	/* one backoff between attempts */
	ut_publish_fail_once = false;
}

UT_TEST(test_clean_close_retry_deadline_gives_up_fail_closed)
{
	/* recovery path: a persistent refusal expires at the bounded deadline — the
	 * retry gives up, the root stays OPEN and the shutdown proceeds
	 * (fail-closed).  The fake latch advances the fake clock 100ms per
	 * wait, so the 5s window is ~50 backoffs. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	ut_serving_rebind_ok = true;
	ut_now_us = 1700000000000000LL;
	ut_waitlatch_calls = 0;
	UT_ASSERT(!cluster_control_root_thread_clean_close_publish_retry());
	UT_ASSERT(ut_root_publish_calls > 1); /* multiple attempts */
	UT_ASSERT(ut_waitlatch_calls >= 40);  /* bounded: ~50 backoffs, never unbounded */
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

UT_TEST(test_owner_rejoin_closed_lifecycle_routes_to_thread_open)
{
	/* STOP-02 §17.4 frozen shape (adjudication 2026-08-18): OWNER_REJOIN
	 * admits ONLY the RECOVERY_COMPLETE pre-lifecycle.  A CLOSED root is
	 * the THREAD_CLEAN_CLOSE release — its reopen is the STOP-01
	 * THREAD_OPEN mainline (CLOSED -> OPEN, frozen shape), routed here at
	 * the commit-time re-vet (Stage 8 contract, corrected design):
	 * same proof set as the crash-rejoin (monotonic newer incarnation +
	 * write-once claim CRC + durable JCMK majority). */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT(cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 1);
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
	UT_ASSERT(ut_root_publish_context_authorized);
	UT_ASSERT_EQ((int)ut_root_published_reason, (int)CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN);
	UT_ASSERT_EQ(ut_root_published_patch.expected_lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(ut_root_published_patch.desired.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.origin_owner_incarnation, UINT64_C(77));
	UT_ASSERT_EQ(ut_root_published_patch.desired.identity.root_lineage_seq,
				 ut_root_identity.root_lineage_seq + 1);

	/* Stale / same-incarnation reopen on a CLOSED root still fails closed:
	 * the head gate no longer pre-rejects the CLOSED branch (implementation review
	 * note 18 split), so the CAS monotonicity rejects it (desired owner
	 * must strictly exceed the current owner) — the verdict is identical,
	 * one CAS attempt later. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(70));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(70)));
	UT_ASSERT_EQ(ut_owner_read_calls, 1);
	UT_ASSERT_EQ(ut_root_publish_calls, 1); /* CAS_CONFLICT at the publish */
	setup_owner_rejoin(UINT64_C(77), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 1); /* same-incarnation CAS_CONFLICT */

	/* Bootstrap / retired roots are not in the reopen allowlist. */
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);
}

UT_TEST(test_owner_rejoin_fails_closed_on_non_jcmk_drift_or_exhaustion)
{
	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_owner_read_result = CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(78));
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 0);
	setup_owner_rejoin(UINT64_C(77), UINT64_C(77));
	ut_root_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT(cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_snapshot.identity.root_lineage_seq = UINT64_MAX;
	ut_root_identity = ut_root_snapshot.identity;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_owner_read_calls, 0);

	setup_owner_rejoin(UINT64_C(70), UINT64_C(77));
	ut_root_publish_result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	UT_ASSERT(!cluster_recovery_owner_rejoin_v1(3, UINT64_C(77)));
	UT_ASSERT_EQ(ut_root_publish_calls, 1);
}

static void
build_expected_encoding(const ClusterRecoveryDutyKey *key,
						uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES])
{
	memset(out, 0, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	put_u64_le(out, key->system_identifier);
	memcpy(out + 8, key->storage_uuid, 16);
	memcpy(out + 24, key->authority_uuid, 16);
	out[40] = (uint8)key->origin_thread_id;
	out[41] = (uint8)(key->origin_thread_id >> 8);
	put_u32_le(out + 42, (uint32)key->origin_node_id);
	put_u64_le(out + 46, (uint64)key->thread_claim_created_at);
	put_u32_le(out + 54, key->thread_claim_crc32c);
	put_u64_le(out + 58, key->origin_owner_incarnation);
	put_u64_le(out + 66, key->root_lineage_seq);
}

static void
build_valid_formation(ClusterFormationSnapshotV1 *snapshot, ClusterFenceAuthorityProof *proof,
					  uint16 origin_thread)
{
	int32 origin_node = (int32)origin_thread - 1;

	memset(snapshot, 0, sizeof(*snapshot));
	snapshot->applied.event_id = UINT64_C(0xAA);
	snapshot->applied.coordinator_node_id = 0;
	snapshot->applied.new_epoch = 9;
	snapshot->applied.cssd_dead_generation = 5;
	snapshot->applied.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	snapshot->applied.dead_bitmap[origin_node / 8] |= (uint8)(1u << (origin_node % 8));
	snapshot->membership.membership_state[0] = CLUSTER_MEMBER_MEMBER;
	snapshot->membership.membership_state[origin_node] = CLUSTER_MEMBER_DEAD;
	snapshot->membership.last_admitted_incarnation[origin_node] = UINT64_C(77);
	snapshot->victim_incarnation = UINT64_C(77);
	snapshot->local_epoch = 9;
	memcpy(snapshot->excluded_bitmap, snapshot->applied.dead_bitmap,
		   sizeof(snapshot->excluded_bitmap));
	snapshot->self_join_admitted = 1;

	memset(proof, 0, sizeof(*proof));
	proof->marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	proof->marker.version = CLUSTER_FENCE_MARKER_VERSION;
	proof->marker.fence_epoch = 9;
	proof->marker.fence_event_id = UINT64_C(0xAA);
	proof->marker.fence_generation = 5;
	proof->marker.issuer_node_id = 0;
	memcpy(proof->marker.fenced_dead_bitmap, snapshot->excluded_bitmap,
		   sizeof(proof->marker.fenced_dead_bitmap));
	proof->agree_disk_count = 2;
	proof->total_disk_count = 3;
}

static void
build_owner_samples(ClusterRecoveryOwnerDiskSampleV1 samples[3], int32 node_id, uint64 incarnation)
{
	int i;

	memset(samples, 0, sizeof(*samples) * 3);
	for (i = 0; i < 3; i++) {
		samples[i].join_io_state = CLUSTER_VOTING_DISK_IO_OK;
		samples[i].slot_io_state = CLUSTER_VOTING_DISK_IO_OK;
		samples[i].slot.node_id = (uint32)node_id;
		samples[i].slot.incarnation = incarnation;
	}
}

static void
set_committed_join_marker(ClusterJoinCommitMarker *marker, int32 node_id, uint64 incarnation,
						  uint64 nonce)
{
	pg_crc32c crc;

	memset(marker, 0, sizeof(*marker));
	marker->magic = CLUSTER_JCMK_MAGIC;
	marker->version = CLUSTER_JCMK_VERSION;
	marker->node_id = node_id;
	marker->phase = CLUSTER_JCMK_PHASE_COMMITTED;
	marker->generation = 1;
	marker->admitted_incarnation = incarnation;
	marker->admitted_epoch = 9;
	marker->commit_nonce = nonce;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	marker->crc32c = (uint32)crc;
}

UT_TEST(test_owner_import_prefers_exact_jcmk_majority)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	set_committed_join_marker(&samples[0].join_marker, 3, 77, 11);
	samples[1].join_marker = samples[0].join_marker;
	set_committed_join_marker(&samples[2].join_marker, 3, 88, 12);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, UINT64_C(1) << 3, 0, samples, 3,
														 &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_JCMK);
	UT_ASSERT_EQ(incarnation, 77);
}

UT_TEST(test_owner_import_never_falls_back_from_split_jcmk)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = UINT64_C(0xdeadbeef);

	build_owner_samples(samples, 3, 70);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	set_committed_join_marker(&samples[0].join_marker, 3, 77, 11);
	set_committed_join_marker(&samples[1].join_marker, 3, 88, 12);
	set_committed_join_marker(&samples[2].join_marker, 3, 99, 13);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, UINT64_C(1) << 3, 0, samples, 3,
														 &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN);
	UT_ASSERT_EQ(incarnation, 0);
}

UT_TEST(test_owner_import_slot_fallback_requires_absent_jcmk_and_claim)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	samples[2].slot.incarnation = 71;
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, UINT64_C(1) << 3, 0, samples, 3,
														 &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT);
	UT_ASSERT_EQ(incarnation, 70);
	claim.node_id = 4;
	incarnation = UINT64_C(0xdeadbeef);
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, UINT64_C(1) << 3, 0, samples, 3,
														 &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH);
	UT_ASSERT_EQ(incarnation, 0);
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, 0, 0, samples, 3, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN);
}

UT_TEST(test_owner_import_cannot_prove_jcmk_absence_with_unreadable_disk)
{
	ClusterRecoveryOwnerDiskSampleV1 samples[3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	build_owner_samples(samples, 3, 70);
	samples[2].join_io_state = CLUSTER_VOTING_DISK_IO_FAILED;
	cluster_wal_thread_claim_fill(&claim, 4, 3, INT64_C(12345));
	UT_ASSERT_EQ(cluster_recovery_owner_import_select_v1(3, &claim, UINT64_C(1) << 3, 0, samples, 3,
														 &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED);
	UT_ASSERT_EQ(incarnation, 0);
}

UT_TEST(test_exact_74_byte_encoding)
{
	ClusterRecoveryDutyKey key;
	uint8 actual[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 expected[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&key);
	build_expected_encoding(&key, expected);
	memset(actual, 0xee, sizeof(actual));
	UT_ASSERT(cluster_recovery_duty_key_encode_v1(&key, actual));
	UT_ASSERT(memcmp(actual, expected, sizeof(actual)) == 0);
	UT_ASSERT_EQ(sizeof(actual), 74);
}

UT_TEST(test_domain_separated_digest)
{
	ClusterRecoveryDutyKey key;
	ClusterRecoveryDutyDigest actual;
	uint8 identity[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 preimage[19 + 4 + CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 expected[PG_SHA256_DIGEST_LENGTH];

	build_valid_key(&key);
	build_expected_encoding(&key, identity);
	memset(preimage, 0, sizeof(preimage));
	memcpy(preimage, "PGRAC-ROOT-DUTY-V1", 18);
	put_u32_le(preimage + 19, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	memcpy(preimage + 23, identity, sizeof(identity));
	sha256_bytes(preimage, sizeof(preimage), expected);
	memset(&actual, 0xee, sizeof(actual));
	UT_ASSERT(cluster_recovery_duty_digest_v1(&key, &actual));
	UT_ASSERT(memcmp(actual.bytes, expected, sizeof(expected)) == 0);
}

UT_TEST(test_full_key_compare_has_no_numeric_order)
{
	ClusterRecoveryDutyKey expected;
	ClusterRecoveryDutyKey observed;

	build_valid_key(&expected);
	observed = expected;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_EXACT);
	observed.root_lineage_seq++;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
	observed = expected;
	observed.root_lineage_seq--;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
	observed = expected;
	observed.authority_uuid[15] ^= 1;
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(&expected, &observed),
				 CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT);
}

static void
assert_invalid_key(ClusterRecoveryDutyKey *key)
{
	ClusterRecoveryDutyKey valid;
	ClusterRecoveryDutyDigest digest;
	uint8 encoded[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&valid);
	memset(encoded, 0xee, sizeof(encoded));
	memset(&digest, 0xee, sizeof(digest));
	UT_ASSERT(!cluster_recovery_duty_key_encode_v1(key, encoded));
	UT_ASSERT(memcmp(encoded, (uint8[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES]){ 0 }, sizeof(encoded))
			  == 0);
	UT_ASSERT(!cluster_recovery_duty_digest_v1(key, &digest));
	UT_ASSERT(
		memcmp(digest.bytes, (uint8[CLUSTER_RECOVERY_DUTY_DIGEST_BYTES]){ 0 }, sizeof(digest.bytes))
		== 0);
	UT_ASSERT_EQ(cluster_recovery_duty_key_compare(key, &valid),
				 CLUSTER_RECOVERY_DUTY_COMPARE_INVALID);
}

UT_TEST(test_zero_and_reserved_fields_are_invalid)
{
	ClusterRecoveryDutyKey key;

	build_valid_key(&key);
	key.system_identifier = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	memset(key.storage_uuid, 0, sizeof(key.storage_uuid));
	assert_invalid_key(&key);
	build_valid_key(&key);
	memset(key.authority_uuid, 0, sizeof(key.authority_uuid));
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_owner_incarnation = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.root_lineage_seq = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.reserved42 = 1;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.reserved60 = 1;
	assert_invalid_key(&key);
}

UT_TEST(test_thread_node_and_claim_binding_are_exact)
{
	ClusterRecoveryDutyKey key;

	build_valid_key(&key);
	key.origin_thread_id = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_thread_id = 5;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.origin_node_id = 128;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.thread_claim_created_at = 0;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.thread_claim_crc32c ^= 1;
	assert_invalid_key(&key);
}

UT_TEST(test_authority_uuid_must_be_v4_and_max_lineage_is_valid)
{
	ClusterRecoveryDutyKey key;
	uint8 encoded[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	build_valid_key(&key);
	key.authority_uuid[6] = 0x36;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.authority_uuid[8] = 0xca;
	assert_invalid_key(&key);
	build_valid_key(&key);
	key.root_lineage_seq = UINT64_MAX;
	UT_ASSERT(cluster_recovery_duty_key_encode_v1(&key, encoded));
}

UT_TEST(test_formation_f1_majority_f2_ready)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_READY);
}

UT_TEST(test_formation_snapshot_or_marker_drift_is_rejected)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f2 = f1;
	f2.local_epoch++;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	f2 = f1;
	proof.marker.fence_event_id++;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN);
}

UT_TEST(test_formation_new_duty_requires_current_event_delta)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f1.applied.dead_bitmap[0] &= (uint8)~0x08;
	/* The origin remains in the full excluded set, but not this event's delta. */
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED);
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, false),
				 CLUSTER_FORMATION_WITNESS_READY);
}

UT_TEST(test_formation_pending_owner_and_full_outage_fail_closed)
{
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
	ClusterFenceAuthorityProof proof;

	build_valid_formation(&f1, &proof, 4);
	f1.prebump_sync_active = 1;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_UNSTABLE);
	build_valid_formation(&f1, &proof, 4);
	f1.victim_incarnation++;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH);
	build_valid_formation(&f1, &proof, 4);
	f1.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
	f2 = f1;
	UT_ASSERT_EQ(cluster_formation_witness_decide_v1(&f1, &proof, &f2, 4, true),
				 CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED);
}

int
main(void)
{
	UT_PLAN(26);
	UT_RUN(test_exact_74_byte_encoding);
	UT_RUN(test_domain_separated_digest);
	UT_RUN(test_full_key_compare_has_no_numeric_order);
	UT_RUN(test_zero_and_reserved_fields_are_invalid);
	UT_RUN(test_thread_node_and_claim_binding_are_exact);
	UT_RUN(test_authority_uuid_must_be_v4_and_max_lineage_is_valid);
	UT_RUN(test_owner_import_prefers_exact_jcmk_majority);
	UT_RUN(test_owner_import_never_falls_back_from_split_jcmk);
	UT_RUN(test_owner_import_slot_fallback_requires_absent_jcmk_and_claim);
	UT_RUN(test_owner_import_cannot_prove_jcmk_absence_with_unreadable_disk);
	UT_RUN(test_checkpoint_advance_publishes_canonical_bound);
	UT_RUN(test_fpw_sticky_publishes_canonical_flag);
	UT_RUN(test_create_authority_requires_complete_ack_round);
	UT_RUN(test_activate_authority_requires_complete_ack_round);
	UT_RUN(test_owner_rejoin_requires_jcmk_and_publishes_exact_root_cas);
	UT_RUN(test_owner_rejoin_rejects_open_stale_owner_frozen);
	UT_RUN(test_clean_close_retry_transient_refusal_then_success);
	UT_RUN(test_clean_close_retry_deadline_gives_up_fail_closed);
	UT_RUN(test_owner_rejoin_closed_lifecycle_routes_to_thread_open);
	UT_RUN(test_owner_rejoin_publication_context_rejects_token_drift);
	UT_RUN(test_recovery_complete_uses_exact_private_publish_authority);
	UT_RUN(test_owner_rejoin_fails_closed_on_non_jcmk_drift_or_exhaustion);
	UT_RUN(test_formation_f1_majority_f2_ready);
	UT_RUN(test_formation_snapshot_or_marker_drift_is_rejected);
	UT_RUN(test_formation_new_duty_requires_current_event_delta);
	UT_RUN(test_formation_pending_owner_and_full_outage_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
