/* Real bufmgr acquisition capture, snapshots and ownership reservations.
 * PGPROC, private pins and admission observations are explicit environment
 * leaves. Captured identity is never a grant, IO permission or native C. */
int native_acquire_legacy_main(void);
#define main native_acquire_legacy_main
#include "test_cluster_pcm_own.c"
#undef main
#include "storage/proc.h"
#include "storage/backendid.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_qvotec.h"

PGPROC *MyProc;
BackendId MyBackendId;
int MyProcPid;
int MaxBackends = 8;
bool cluster_shared_catalog = false;

typedef struct NativeIntentFixture {
	BufferDesc buf;
	ClusterPcmOwnEntry own;
	ClusterPcmOwnSnapshot base;
	ClusterSemanticAdmissionToken admission;
	uint64 token;
} NativeIntentFixture;

static NativeIntentFixture *intent_fixture;
static PGPROC intent_proc;
static uint64 intent_boot, intent_epoch;
static unsigned intent_rechecks;
static bool intent_admitted;
static int intent_pins;
static void (*intent_interleave)(void);

uint64
cluster_qvotec_get_self_incarnation(void)
{
	UT_ASSERT((pg_atomic_read_u32(&intent_fixture->buf.state) & BM_LOCKED) == 0);
	return intent_boot;
}

uint64
cluster_epoch_get_current(void)
{
	UT_ASSERT((pg_atomic_read_u32(&intent_fixture->buf.state) & BM_LOCKED) == 0);
	return intent_epoch;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT(token == &intent_fixture->admission);
	UT_ASSERT((pg_atomic_read_u32(&intent_fixture->buf.state) & BM_LOCKED) == 0);
	if (++intent_rechecks == 2 && intent_interleave != NULL)
		intent_interleave();
	return intent_admitted;
}

static int
intent_private_pin(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, intent_fixture->buf.buf_id + 1);
	UT_ASSERT((pg_atomic_read_u32(&intent_fixture->buf.state) & BM_LOCKED) == 0);
	return intent_pins;
}

#define GetPrivateRefCount(buffer) intent_private_pin(buffer)
#define LockBufHdr transition_lock_header
#include "test_cluster_native_acquire_intent.inc"
#undef LockBufHdr
#undef GetPrivateRefCount

static void
intent_start(NativeIntentFixture *f, unsigned profile)
{
	uint32 state;

	memset(f, 0, sizeof(*f)); memset(&intent_proc, 0, sizeof(intent_proc));
	intent_fixture = f; ClusterPcmOwnArray = &f->own;
	MyProc = &intent_proc; MyBackendId = intent_proc.backendId = 4;
	MyProcPid = intent_proc.pid = 1234; intent_proc.pgprocno = 3;
	intent_proc.cluster_grd_generation = 71;
	intent_boot = 101; intent_epoch = 17; intent_admitted = true;
	intent_rechecks = 0; intent_interleave = NULL; intent_pins = 1;
	f->admission.entered = true; f->admission.side = CLUSTER_SEMANTIC_TARGET_SIDE;
	f->admission.feature_bit = CLUSTER_SEMANTIC_FEATURE_R11_RESOURCE_X_D5_CUTOVER_V1;
	f->admission.record_generation = 77; f->admission.formation_epoch = 17;
	f->buf.tag.spcOid = 1663; f->buf.tag.dbOid = 5;
	f->buf.tag.relNumber = 16392; f->buf.tag.blockNum = 94;
	f->buf.tag.forkNum = MAIN_FORKNUM;
	f->buf.pcm_state = profile < 3 ? PCM_STATE_N : profile == 3 ? PCM_STATE_S : PCM_STATE_X;
	f->buf.buffer_type = profile == 2 ? BUF_TYPE_PI : profile == 3 ? BUF_TYPE_SCUR
		: profile == 4 ? BUF_TYPE_XCUR : BUF_TYPE_CURRENT;
	pg_atomic_init_u32(&f->buf.state, BM_TAG_VALID | BUF_REFCOUNT_ONE
		| (profile == 0 ? BM_IO_IN_PROGRESS : BM_VALID));
	pg_atomic_init_u64(&f->own.generation, 48);
	pg_atomic_init_u64(&f->own.reservation_token, 48);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&f->buf, &f->base), CLUSTER_PCM_OWN_OK);
	state = transition_lock_header(&f->buf);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_begin_exact(f->buf.buf_id, f->base.generation,
		PCM_OWN_FLAG_GRANT_PENDING, &f->token), CLUSTER_PCM_OWN_OK);
	UnlockBufHdr(&f->buf, state);
}

static ClusterPcmOwnResult
intent_capture(NativeIntentFixture *f, uint8 path,
	const ResourceXDecodedAcquireIntent *expected, ResourceXDecodedAcquireIntent *out)
{
	return cluster_bufmgr_pcm_own_acquire_intent_exact(&f->buf, &f->base, f->token,
		path, &f->admission, expected, out);
}

static void
intent_refused(NativeIntentFixture *f, uint8 path, ClusterPcmOwnResult wanted)
{
	ResourceXDecodedAcquireIntent out, sentinel;
	ClusterPcmOwnEntry own = f->own;
	uint32 state = pg_atomic_read_u32(&f->buf.state);
	memset(&out, 0xa5, sizeof(out)); sentinel = out;
	UT_ASSERT_EQ(intent_capture(f, path, NULL, &out), wanted);
	UT_ASSERT(memcmp(&sentinel, &out, sizeof(out)) == 0);
	UT_ASSERT(memcmp(&own, &f->own, sizeof(own)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&f->buf.state), state);
}

UT_TEST(test_native_intent_captures_existing_first_fill_and_resident_reservations)
{
	for (unsigned profile = 0; profile < 5; profile++) {
		NativeIntentFixture f; ResourceXDecodedAcquireIntent out;
		ClusterPcmOwnEntry before;
		intent_start(&f, profile); before = f.own;
		UT_ASSERT_EQ(intent_capture(&f, profile ? RESOURCE_X_ACQUIRE_RESIDENT
			: RESOURCE_X_ACQUIRE_FIRST_FILL, NULL, &out), CLUSTER_PCM_OWN_OK);
		UT_ASSERT_EQ(out.buffer_id, f.buf.buf_id); UT_ASSERT_EQ(out.backend_id, MyBackendId);
		UT_ASSERT_EQ(out.backend_generation, intent_proc.cluster_grd_generation);
		UT_ASSERT_EQ(out.requester_incarnation, intent_boot);
		UT_ASSERT_EQ(out.base_generation, f.base.generation);
		UT_ASSERT_EQ(out.reservation_token, f.token);
		UT_ASSERT_EQ(out.cluster_epoch, intent_epoch); UT_ASSERT_EQ(out.r4_generation, 77);
		UT_ASSERT_EQ(out.path, profile ? RESOURCE_X_ACQUIRE_RESIDENT : RESOURCE_X_ACQUIRE_FIRST_FILL);
		UT_ASSERT_EQ(intent_rechecks, 2);
		UT_ASSERT(memcmp(&before, &f.own, sizeof(before)) == 0);
	}
}

UT_TEST(test_native_intent_exact_recheck_is_idempotent_and_allows_alias)
{
	NativeIntentFixture f; ResourceXDecodedAcquireIntent out, before;
	intent_start(&f, 0);
	UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, NULL, &out), CLUSTER_PCM_OWN_OK);
	before = out;
	UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, &out, &out), CLUSTER_PCM_OWN_OK);
	UT_ASSERT(memcmp(&before, &out, sizeof(out)) == 0);
}

UT_TEST(test_native_intent_recheck_compares_all_semantic_episode_fields)
{
	NativeIntentFixture f; ResourceXDecodedAcquireIntent out, wrong;
	intent_start(&f, 0);
	UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, NULL, &out), CLUSTER_PCM_OWN_OK);
	for (unsigned field = 0; field < 9; field++) {
		ResourceXDecodedAcquireIntent saved;

		wrong = out;
		if (field == 0) wrong.requester_incarnation++;
		if (field == 1) wrong.backend_generation++;
		if (field == 2) wrong.base_generation++;
		if (field == 3) wrong.reservation_token++;
		if (field == 4) wrong.cluster_epoch++;
		if (field == 5) wrong.r4_generation++;
		if (field == 6) wrong.backend_id++;
		if (field == 7) wrong.buffer_id++;
		if (field == 8) wrong.path = RESOURCE_X_ACQUIRE_RESIDENT;
		saved = wrong;
		UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, &wrong, &wrong), CLUSTER_PCM_OWN_STALE);
		UT_ASSERT(memcmp(&wrong, &saved, sizeof(wrong)) == 0);
	}
}

UT_TEST(test_native_intent_stale_base_token_and_tag_are_not_rebound)
{
	for (unsigned field = 0; field < 5; field++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (field == 0) f.base.generation++;
		if (field == 1) f.base.tag.blockNum++;
		if (field == 2) f.token++;
		if (field == 3) f.base.reservation_token++;
		if (field == 4) f.base.buffer_type = BUF_TYPE_PI;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_STALE);
	}
}

UT_TEST(test_native_intent_ended_revoke_and_combined_flags_keep_original_owner)
{
	const uint32 values[] = {0, PCM_OWN_FLAG_REVOKING,
		PCM_OWN_FLAG_GRANT_PENDING | PCM_OWN_FLAG_REVOKING, 0x80};
	for (unsigned i = 0; i < lengthof(values); i++) {
		NativeIntentFixture f; intent_start(&f, 0);
		pg_atomic_write_u32(&f.own.flags, values[i]);
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, values[i] == 0 ? CLUSTER_PCM_OWN_STALE
			: values[i] == PCM_OWN_FLAG_REVOKING ? CLUSTER_PCM_OWN_BUSY : CLUSTER_PCM_OWN_CORRUPT);
	}
}

UT_TEST(test_native_intent_activation_delivery_and_write_io_wait_without_clearing)
{
	for (unsigned debt = 0; debt < 4; debt++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (debt == 0) pg_atomic_write_u64(&f.own.writer_activation_token, 49);
		if (debt == 1) pg_atomic_write_u64(&f.own.resource_x_activation_generation, 9);
		if (debt == 2) pg_atomic_write_u64(&f.own.delivery_attempt, 11);
		if (debt == 3) pg_atomic_write_u64(&f.own.write_io_generation, 1);
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_BUSY);
	}
}

UT_TEST(test_native_intent_first_fill_requires_invalid_clean_current_and_io)
{
	for (unsigned shape = 0; shape < 7; shape++) {
		NativeIntentFixture f; uint32 state;
		intent_start(&f, 0);
		state = pg_atomic_read_u32(&f.buf.state);
		if (shape == 0) state |= BM_VALID;
		if (shape == 1) state &= ~BM_IO_IN_PROGRESS;
		if (shape == 2) state |= BM_DIRTY;
		if (shape == 3) state |= BM_JUST_DIRTIED;
		if (shape == 4) state |= BM_CHECKPOINT_NEEDED;
		if (shape == 5) state |= BM_IO_ERROR;
		if (shape == 6) f.buf.buffer_type = BUF_TYPE_PI;
		pg_atomic_write_u32(&f.buf.state, state);
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_CORRUPT);
	}
}

UT_TEST(test_native_intent_resident_io_error_and_invalid_shape_do_not_capture)
{
	for (unsigned shape = 0; shape < 5; shape++) {
		NativeIntentFixture f; uint32 state;
		intent_start(&f, 1);
		state = pg_atomic_read_u32(&f.buf.state);
		if (shape == 0) state |= BM_IO_IN_PROGRESS;
		if (shape == 1) state |= BM_IO_ERROR;
		if (shape == 2) state &= ~BM_VALID;
		if (shape == 3) state |= BM_DIRTY;
		if (shape == 4) f.buf.buffer_type = BUF_TYPE_CR;
		pg_atomic_write_u32(&f.buf.state, state);
		intent_refused(&f, RESOURCE_X_ACQUIRE_RESIDENT, shape == 0 ? CLUSTER_PCM_OWN_BUSY : CLUSTER_PCM_OWN_CORRUPT);
	}
}

UT_TEST(test_native_intent_does_not_legalize_legacy_s_new_finish)
{
	NativeIntentFixture f; ResourceXDecodedAcquireIntent out; ClusterPcmOwnSnapshot live;
	intent_start(&f, 3);
	UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_RESIDENT, NULL, &out), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_bufmgr_pcm_own_snapshot(&f.buf, &live), CLUSTER_PCM_OWN_OK);
	UT_ASSERT_EQ(cluster_pcm_x_grant_reservation_kind(&live, &f.base, f.token), CLUSTER_PCM_X_GRANT_RESERVATION_INVALID);
}

UT_TEST(test_native_intent_requires_real_current_backend_and_private_pin)
{
	for (unsigned field = 0; field < 8; field++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (field == 0) intent_pins = 0;
		if (field == 1) MyProc = NULL;
		if (field == 2) MyProcPid++;
		if (field == 3) MyBackendId++;
		if (field == 4) intent_proc.cluster_grd_generation = 0;
		if (field == 5) intent_proc.cluster_grd_generation = UINT64_MAX;
		if (field == 6) intent_proc.backendId = MyBackendId = InvalidBackendId;
		if (field == 7) intent_proc.pgprocno = -1;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_STALE);
	}
}

UT_TEST(test_native_intent_requires_original_target_admission_epoch_and_boot)
{
	for (unsigned field = 0; field < 8; field++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (field == 0) f.admission.entered = false;
		if (field == 1) f.admission.side = CLUSTER_SEMANTIC_SOURCE_SIDE;
		if (field == 2) f.admission.feature_bit = 0;
		if (field == 3) f.admission.record_generation = 0;
		if (field == 4) intent_epoch++;
		if (field == 5) intent_boot = 0;
		if (field == 6) intent_boot = UINT64_MAX;
		if (field == 7) intent_admitted = false;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_NOT_READY);
	}
}

static unsigned intent_race;
static void
intent_domain_race(void)
{
	if (intent_race == 0) intent_proc.cluster_grd_generation++;
	if (intent_race == 1) intent_boot++;
	if (intent_race == 2) intent_epoch++;
	if (intent_race == 3) intent_fixture->admission.record_generation++;
	if (intent_race == 4) intent_pins = 0;
	if (intent_race == 5) intent_admitted = false;
	if (intent_race == 6) intent_proc.pid++;
}

UT_TEST(test_native_intent_domain_turnover_between_header_samples_preserves_output)
{
	for (intent_race = 0; intent_race < 7; intent_race++) {
		NativeIntentFixture f; intent_start(&f, 0); intent_interleave = intent_domain_race;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, intent_race == 1 || intent_race == 2
			|| intent_race == 3 || intent_race == 5 ? CLUSTER_PCM_OWN_NOT_READY : CLUSTER_PCM_OWN_STALE);
	}
}

static void
intent_native_race(void)
{
	if (intent_race == 0) pg_atomic_fetch_add_u64(&intent_fixture->own.generation, 1);
	if (intent_race == 1) pg_atomic_fetch_add_u64(&intent_fixture->own.reservation_token, 1);
	if (intent_race == 2) intent_fixture->buf.tag.blockNum++;
	if (intent_race == 3) pg_atomic_write_u32(&intent_fixture->own.flags, PCM_OWN_FLAG_REVOKING);
	if (intent_race == 4) pg_atomic_write_u64(&intent_fixture->own.delivery_attempt, 3);
}

UT_TEST(test_native_intent_native_turnover_between_samples_never_binds_successor)
{
	for (intent_race = 0; intent_race < 5; intent_race++) {
		NativeIntentFixture f; ResourceXDecodedAcquireIntent out, before;
		intent_start(&f, 0); intent_interleave = intent_native_race;
		memset(&out, 0xa5, sizeof(out)); before = out;
		UT_ASSERT_EQ(intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, NULL, &out),
			intent_race < 3 ? CLUSTER_PCM_OWN_STALE : CLUSTER_PCM_OWN_BUSY);
		UT_ASSERT(memcmp(&before, &out, sizeof(out)) == 0);
		UT_ASSERT((pg_atomic_read_u32(&f.buf.state) & BM_LOCKED) == 0);
	}
}

UT_TEST(test_native_intent_capacity_bounds_and_untracked_forks_precede_array_access)
{
	for (unsigned field = 0; field < 5; field++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (field == 0) f.buf.buf_id = -1;
		if (field == 1) f.buf.buf_id = NBuffers;
		if (field == 2) f.base.tag.forkNum = f.buf.tag.forkNum = FSM_FORKNUM;
		if (field == 3) f.base.tag.forkNum = f.buf.tag.forkNum = MAX_FORKNUM + 1;
		if (field == 4) f.base.tag.relNumber = f.buf.tag.relNumber = 1260;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_INVALID);
	}
}

UT_TEST(test_native_intent_exhaustion_never_wraps_or_allocates_a_token)
{
	for (unsigned field = 0; field < 3; field++) {
		NativeIntentFixture f; intent_start(&f, 0);
		if (field == 0) f.base.generation = UINT64_MAX - 1;
		if (field == 1) f.base.reservation_token = UINT64_MAX - 1;
		if (field == 2) f.token = UINT64_MAX;
		intent_refused(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, CLUSTER_PCM_OWN_EXHAUSTED);
	}
}

static void
intent_observation_error(void)
{
	pg_re_throw();
}

UT_TEST(test_native_intent_observation_error_leaves_native_reservation_owned)
{
	static NativeIntentFixture f;
	ResourceXDecodedAcquireIntent out, before;
	volatile bool caught = false;
	intent_start(&f, 0); intent_interleave = intent_observation_error;
	memset(&out, 0xa5, sizeof(out)); before = out;
	PG_TRY(); { (void)intent_capture(&f, RESOURCE_X_ACQUIRE_FIRST_FILL, NULL, &out); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
	UT_ASSERT_EQ(cluster_pcm_own_flags_get(0), PCM_OWN_FLAG_GRANT_PENDING);
	UT_ASSERT_EQ(cluster_pcm_own_reservation_token_get(0), f.token);
	UT_ASSERT((pg_atomic_read_u32(&f.buf.state) & BM_LOCKED) == 0);
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_native_intent_captures_existing_first_fill_and_resident_reservations);
	UT_RUN(test_native_intent_exact_recheck_is_idempotent_and_allows_alias);
	UT_RUN(test_native_intent_recheck_compares_all_semantic_episode_fields);
	UT_RUN(test_native_intent_stale_base_token_and_tag_are_not_rebound);
	UT_RUN(test_native_intent_ended_revoke_and_combined_flags_keep_original_owner);
	UT_RUN(test_native_intent_activation_delivery_and_write_io_wait_without_clearing);
	UT_RUN(test_native_intent_first_fill_requires_invalid_clean_current_and_io);
	UT_RUN(test_native_intent_resident_io_error_and_invalid_shape_do_not_capture);
	UT_RUN(test_native_intent_does_not_legalize_legacy_s_new_finish);
	UT_RUN(test_native_intent_requires_real_current_backend_and_private_pin);
	UT_RUN(test_native_intent_requires_original_target_admission_epoch_and_boot);
	UT_RUN(test_native_intent_domain_turnover_between_header_samples_preserves_output);
	UT_RUN(test_native_intent_native_turnover_between_samples_never_binds_successor);
	UT_RUN(test_native_intent_capacity_bounds_and_untracked_forks_precede_array_access);
	UT_RUN(test_native_intent_exhaustion_never_wraps_or_allocates_a_token);
	UT_RUN(test_native_intent_observation_error_leaves_native_reservation_owned);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
