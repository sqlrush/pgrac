/*-------------------------------------------------------------------------
 * test_cluster_freeze_writer_context.c
 *    Original backend-local writer context with controlled pin/owner leaves.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ko.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/bufmgr.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
void
ExceptionalCondition(const char *condition pg_attribute_unused(),
	const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}
int cluster_node_id = 0;
int NBuffers = 1, NLocBuffer = 1;
bool cluster_enabled, cluster_shared_config;
int cluster_interconnect_tier;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static ClusterPcmOwnSnapshot fixture_own;
static ResourceXWriterUseContext fixture_node_context;
static bool lock_available, error_lock, error_terminal, drift_on_lock;
static unsigned lock_attempts, unlocks, unpins, object_samples;
static uint64 current_object_revision, original_object_revision;
static bool original_object_checked;
static bool fixture_node_matches(void);
static bool fixture_ledger_live(void);
void
pg_re_throw(void)
{
	if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1);
	abort();
}
static BufferDesc fixture_buf;
static bool pinned, exclusive, terminal_current, object_current;
static uint64 terminal_generation;
static int terminal_reads, descriptor_reads;

static BufferDesc *
fixture_descriptor(int id)
{
	UT_ASSERT_EQ(id, 0);
	descriptor_reads++;
	return &fixture_buf;
}
#define GetBufferDescriptor(id) fixture_descriptor(id)

bool
ClusterBufferHasSharedPin(Buffer buffer)
{
	return buffer == 1 && pinned;
}

bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return exclusive;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	return exclusive;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&fixture_buf));
	UT_ASSERT(exclusive && pinned && !fixture_ledger_live());
	exclusive = false; unlocks++;
}
bool
ConditionalLockBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(pinned && !exclusive);
	lock_attempts++; exclusive = lock_available;
	if (drift_on_lock) terminal_generation++;
	if (error_lock) siglongjmp(*PG_exception_stack, 1);
	return exclusive;
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1); UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
	pinned = false; unpins++;
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&fixture_buf.tag)));
	UT_ASSERT_EQ(backend, InvalidBackendId); UT_ASSERT(!exclusive);
	return 1;
}
bool
cluster_ko_peer_delivery_begin_v3(RelFileLocator locator, uint64 *revision)
{
	UT_ASSERT(RelFileLocatorEquals(locator, BufTagGetRelFileLocator(&fixture_buf.tag)));
	UT_ASSERT(!exclusive); object_samples++; *revision = current_object_revision;
	return object_current;
}
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == &fixture_buf); UT_ASSERT(!exclusive);
	*out = fixture_own; return CLUSTER_PCM_OWN_OK;
}

bool cluster_ko_peer_delivery_current_v3(uint64 revision);
bool
cluster_ko_peer_delivery_current_v3(uint64 revision)
{
	return object_current && revision == current_object_revision;
}

bool cluster_bufmgr_terminal_stamp_authority(Buffer, const BufferTag *, uint64 *, uint64 *, uint8 *);
bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(BufferTagsEqual(tag, &fixture_buf.tag));
	UT_ASSERT(pinned && exclusive);
	terminal_reads++;
	if (error_terminal) siglongjmp(*PG_exception_stack, 1);
	*generation = terminal_generation;
	*epoch = 9;
	*state = PCM_STATE_X;
	return terminal_current && fixture_node_matches();
}
#include "test_cluster_freeze_writer_context.inc"

/* The acquisition/OS leaves remain controlled.  Compile the original cleanup
 * pin checks and capture/resume bodies, rather than granting a writer to the
 * read-only conditional-X path. */
typedef struct LVRelState
{
	bool versioned;
	bool shared_maintenance_restricted;
} LVRelState;
static ClusterPcmXWriterLedgerEntry fixture_acquired_writer;
static uint32 fixture_private_pins, fixture_global_pins;
static unsigned fixture_writer_acquisitions, fixture_header_checks;
static int32 fixture_local_pins[1];
int32 *LocalRefCount = fixture_local_pins;

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT(pinned);
	if (mode == BUFFER_LOCK_EXCLUSIVE)
	{
		UT_ASSERT(!exclusive && !fixture_ledger_live());
		fixture_writer_acquisitions++;
		cluster_bufmgr_pcm_x_writer_ledger[0] = fixture_acquired_writer;
		exclusive = true;
	}
	else
	{
		ClusterPcmXWriterLedgerEntry *entry;

		UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
		entry = cluster_bufmgr_pcm_x_writer_find(&fixture_buf);
		if (entry != NULL)
			cluster_bufmgr_pcm_x_writer_clear(entry);
		LWLockRelease(BufferDescriptorGetContentLock(&fixture_buf));
	}
}

static int32
fixture_private_refcount(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return pinned ? fixture_private_pins : 0;
}

uint32
LockBufHdr(BufferDesc *buf)
{
	UT_ASSERT(buf == &fixture_buf && exclusive && pinned);
	fixture_header_checks++;
	return fixture_global_pins | BM_LOCKED;
}

#define GetPrivateRefCount fixture_private_refcount
#include "test_cluster_vacuum_cleanup_pin.inc"
#undef GetPrivateRefCount
#include "test_cluster_vacuum_initial_writer.inc"

static bool
fixture_capture_context(Buffer buffer, ResourceXWriterUseContext *out)
{
	return ClusterBufferCaptureMutationContext(buffer, out,
		&original_object_revision, &original_object_checked);
}

static bool
fixture_ledger_live(void)
{
	return cluster_bufmgr_pcm_x_writer_find(&fixture_buf) != NULL;
}
static bool
fixture_node_matches(void)
{
	ClusterPcmXWriterLedgerEntry *entry = cluster_bufmgr_pcm_x_writer_find(&fixture_buf);
	const ResourceXWriterUseContext *a = &entry->authority;
	const ResourceXWriterUseContext *b = &fixture_node_context;
	return resource_x_assertion_equal(&a->ref.assertion, &b->ref.assertion)
		&& a->ref.formation == b->ref.formation
		&& a->ref.acquisition_generation == b->ref.acquisition_generation
		&& a->r4_record_generation == b->r4_record_generation
		&& a->buffer_ownership_generation == b->buffer_ownership_generation;
}


static void
setup(void)
{
	ClusterPcmXWriterLedgerEntry *entry = &cluster_bufmgr_pcm_x_writer_ledger[0];
	memset(&fixture_buf, 0, sizeof(fixture_buf));
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	fixture_buf.buf_id = 0;
	InitBufferTag(&fixture_buf.tag, &((RelFileLocator){1663, 5, 99}), MAIN_FORKNUM, 7);
	entry->phase = PCM_X_WRITER_LEDGER_ACTIVE;
	entry->buffer_id = 0;
	entry->content_lock = BufferDescriptorGetContentLock(&fixture_buf);
	entry->authority.ref.assertion.resource = fixture_buf.tag;
	entry->authority.ref.assertion.requester_node = 0;
	entry->authority.ref.formation = 3;
	entry->authority.ref.acquisition_generation = 4;
	entry->authority.r4_record_generation = 5;
	entry->authority.buffer_ownership_generation = 6;
	entry->granted.tag = fixture_buf.tag;
	entry->granted.generation = terminal_generation = 6;
	entry->granted.pcm_state = PCM_STATE_X;
	entry->object_checked = true;
	entry->object_revision = 7;
	pinned = exclusive = terminal_current = object_current = true;
	terminal_reads = descriptor_reads = 0;
	current_object_revision = 7;
	fixture_own = entry->granted;
	fixture_node_context = entry->authority;
	cluster_enabled = cluster_shared_config = true;
	cluster_interconnect_tier = CLUSTER_IC_TIER_1;
	lock_available = true;
	error_lock = error_terminal = drift_on_lock = false;
	lock_attempts = unlocks = unpins = object_samples = 0;
	PG_exception_stack = NULL; error_context_stack = NULL;
}

static void
refused_without_output(Buffer buffer)
{
	ResourceXWriterUseContext out, before;
	memset(&out, 0xa5, sizeof(out));
	before = out;
	UT_ASSERT(!fixture_capture_context(buffer, &out));
	UT_ASSERT(memcmp(&out, &before, sizeof(out)) == 0);
}

UT_TEST(reject_pin_before_descriptor)
{
	setup(); pinned = false; refused_without_output(1);
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
	setup(); refused_without_output(0); refused_without_output(-1); refused_without_output(2);
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
}
UT_TEST(reject_share_or_local_x_without_writer)
{
	setup(); exclusive = false; refused_without_output(1);
	UT_ASSERT_EQ(terminal_reads, 0);
	setup(); cluster_bufmgr_pcm_x_writer_ledger[0].phase = PCM_X_WRITER_LEDGER_UNUSED;
	refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
}
UT_TEST(reject_inactive_or_changed_ledger)
{
	for (int phase = PCM_X_WRITER_LEDGER_HANDOFF; phase <= PCM_X_WRITER_LEDGER_RELEASING; phase++)
	{
		if (phase == PCM_X_WRITER_LEDGER_ACTIVE) continue;
		setup(); cluster_bufmgr_pcm_x_writer_ledger[0].phase = phase;
		refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
	}
	setup(); cluster_bufmgr_pcm_x_writer_ledger[0].authority.ref.assertion.resource.blockNum++;
	refused_without_output(1); UT_ASSERT_EQ(terminal_reads, 0);
}
UT_TEST(reject_stale_original_object_or_terminal)
{
	setup(); object_current = false; refused_without_output(1);
	UT_ASSERT_EQ(terminal_reads, 0);
	setup(); terminal_current = false; refused_without_output(1);
	setup(); terminal_generation++; refused_without_output(1);
}
UT_TEST(capture_and_match_original_active_writer)
{
	ResourceXWriterUseContext out;
	setup();
	UT_ASSERT(fixture_capture_context(1, &out));
	UT_ASSERT_EQ(out.ref.acquisition_generation, 4);
	UT_ASSERT_EQ(out.r4_record_generation, 5);
	UT_ASSERT_EQ(out.buffer_ownership_generation, 6);
	UT_ASSERT(ClusterBufferMutationContextMatches(1, &out, original_object_revision, original_object_checked));
}
UT_TEST(reject_every_changed_logical_context_field)
{
	ResourceXWriterUseContext out;
	for (int field = 0; field < 9; field++)
	{
		setup(); UT_ASSERT(fixture_capture_context(1, &out));
		switch (field)
		{
			case 0: out.ref.assertion.resource.blockNum++; break;
			case 1: out.ref.assertion.requester_node++; break;
			case 2: out.ref.formation++; break;
			case 3: out.ref.acquisition_generation++; break;
			case 4: out.r4_record_generation++; break;
			case 5: out.buffer_ownership_generation++; break;
			case 6: out.writer_activation_token++; break;
			case 7: out.resource_x_activation_generation++; break;
			case 8: terminal_current = false; break;
		}
		UT_ASSERT(!ClusterBufferMutationContextMatches(1, &out, original_object_revision, original_object_checked));
	}
}
UT_TEST(reject_null_output_and_expectation)
{
	setup(); UT_ASSERT(!fixture_capture_context(1, NULL));
	UT_ASSERT(!ClusterBufferMutationContextMatches(1, NULL, original_object_revision, original_object_checked));
	UT_ASSERT_EQ(descriptor_reads + terminal_reads, 0);
}
static ResourceXWriterUseContext
resume_setup(void)
{
	ResourceXWriterUseContext context;
	setup(); UT_ASSERT(fixture_capture_context(1, &context));
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	exclusive = false; terminal_reads = descriptor_reads = 0;
	return context;
}
UT_TEST(unlock_only_preserves_original_pin_and_original_node_grant)
{
	ResourceXWriterUseContext context = resume_setup();
	ResourceXWriterUseContext original = fixture_node_context;
	UT_ASSERT(ClusterConditionalLockBufferWithMutationContext(1, &context,
		original_object_revision, original_object_checked));
	ClusterUnlockBufferAfterConditionalMutation(1);
	UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
	UT_ASSERT(memcmp(&original, &fixture_node_context, sizeof(original)) == 0);
	/* The caller can still retire its one original pin after local cleanup. */
	ReleaseBuffer(1);
	UT_ASSERT(!pinned);
}

UT_TEST(resume_original_and_cleanup_only_its_local_scope)
{
	ResourceXWriterUseContext context = resume_setup();
	UT_ASSERT(ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT(pinned && exclusive && fixture_ledger_live());
	UT_ASSERT(ClusterBufferMutationContextMatches(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT_EQ(lock_attempts, 1); UT_ASSERT_EQ(object_samples, 1);
	ClusterUnlockReleaseBufferAfterConditionalMutation(1);
	UT_ASSERT(!pinned && !exclusive && !fixture_ledger_live());
	UT_ASSERT_EQ(unlocks, 1); UT_ASSERT_EQ(unpins, 1);
}
UT_TEST(resume_refuses_without_original_pin_or_quiescent_scope)
{
	ResourceXWriterUseContext context = resume_setup();
	pinned = false;
	UT_ASSERT(!ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT_EQ(descriptor_reads + object_samples + lock_attempts, 0);
	setup(); context = fixture_node_context;
	UT_ASSERT(!ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT(pinned && exclusive && fixture_ledger_live());
	UT_ASSERT_EQ(unlocks + unpins + lock_attempts + object_samples, 0);
}
UT_TEST(resume_refuses_full_ledger_without_creating_or_waiting)
{
	ResourceXWriterUseContext context = resume_setup();
	for (int i = 0; i < lengthof(cluster_bufmgr_pcm_x_writer_ledger); i++) {
		cluster_bufmgr_pcm_x_writer_ledger[i].phase = PCM_X_WRITER_LEDGER_ACQUIRING;
		cluster_bufmgr_pcm_x_writer_ledger[i].buffer_id = i + 1;
	}
	UT_ASSERT(!ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT(pinned && !exclusive);
	UT_ASSERT_EQ(lock_attempts + object_samples + unlocks + unpins, 0);
}
UT_TEST(resume_refusals_remove_only_new_lock_and_ledger)
{
	for (int which = 0; which < 8; which++) {
		ResourceXWriterUseContext context = resume_setup();
		switch (which) {
			case 0: lock_available = false; break;
			case 1: terminal_current = false; break;
			case 2: object_current = false; break;
			case 3: drift_on_lock = true; break;
			case 4: context.ref.acquisition_generation++; break;
			case 5: fixture_own.flags = 1; break;
			case 6: original_object_checked = false; original_object_revision = 0; break;
			case 7: original_object_revision++; break;
		}
		UT_ASSERT(!ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
		UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
		UT_ASSERT_EQ(unpins, 0);
		UT_ASSERT_EQ(lock_attempts, which >= 5 ? 0 : 1);
		UT_ASSERT_EQ(unlocks, which == 0 || which >= 5 ? 0 : 1);
	}
}
UT_TEST(resume_error_unwinds_inner_lock_or_bound_ledger_before_rethrow)
{
	for (int which = 0; which < 2; which++) {
		ResourceXWriterUseContext context = resume_setup();
		volatile bool caught = false;
		error_lock = which == 0; error_terminal = which == 1;
		PG_TRY(); {
			(void)ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked);
			UT_ASSERT(false);
		} PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught && pinned && !exclusive && !fixture_ledger_live());
		UT_ASSERT_EQ(unlocks, 1); UT_ASSERT_EQ(unpins, 0);
		ReleaseBuffer(1); UT_ASSERT_EQ(unpins, 1);
	}
}

UT_TEST(resume_does_not_replace_original_object_fence_with_new_sample)
{
	ResourceXWriterUseContext context = resume_setup();
	/* The original writer was admitted at revision 7. A newly valid KO
	 * sample at 8 cannot authorize continuation of that older proof. */
	current_object_revision = 8;
	UT_ASSERT(!ClusterConditionalLockBufferWithMutationContext(1, &context, original_object_revision, original_object_checked));
	UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
	UT_ASSERT_EQ(lock_attempts, 0);
}

static LVRelState
vacuum_lock_setup(bool versioned, bool restricted)
{
	LVRelState vacrel = {versioned, restricted};

	setup();
	fixture_acquired_writer = cluster_bufmgr_pcm_x_writer_ledger[0];
	cluster_bufmgr_pcm_x_writer_clear(&cluster_bufmgr_pcm_x_writer_ledger[0]);
	exclusive = false;
	fixture_private_pins = fixture_global_pins = 1;
	fixture_writer_acquisitions = fixture_header_checks = 0;
	return vacrel;
}

UT_TEST(vacuum_initial_writer_capture_and_original_resume)
{
	LVRelState vacrel = vacuum_lock_setup(true, true);
	ResourceXWriterUseContext context = {0};
	bool captured;

	UT_ASSERT(lazy_lock_buffer_for_cleanup(&vacrel, 1));
	UT_ASSERT(pinned && exclusive && fixture_ledger_live());
	UT_ASSERT_EQ(fixture_writer_acquisitions, 1);
	UT_ASSERT_EQ(fixture_header_checks, 1);
	UT_ASSERT_EQ(lock_attempts + object_samples + unpins, 0);
	captured = fixture_capture_context(1, &context);
	UT_ASSERT(captured);
	if (!captured)
	{
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(1);
		return;
	}
	UT_ASSERT(memcmp(&context, &fixture_node_context, sizeof(context)) == 0);
	/* The later apply may resume this original writer, without current-X. */
	LockBuffer(1, BUFFER_LOCK_UNLOCK);
	UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
	UT_ASSERT(ClusterConditionalLockBufferWithMutationContext(1, &context,
		original_object_revision, original_object_checked));
	UT_ASSERT(ClusterBufferMutationContextMatches(1, &context,
		original_object_revision, original_object_checked));
	UT_ASSERT_EQ(fixture_writer_acquisitions, 1);
	ClusterUnlockReleaseBufferAfterConditionalMutation(1);
	UT_ASSERT(!pinned && !exclusive && !fixture_ledger_live());
	UT_ASSERT_EQ(unlocks, 2);
	UT_ASSERT_EQ(unpins, 1);
}

UT_TEST(vacuum_cleanup_pin_refusal_keeps_original_pin)
{
	for (int which = 0; which < 2; which++)
	{
		LVRelState vacrel = vacuum_lock_setup(true, true);

		if (which == 0) fixture_private_pins = 2;
		else fixture_global_pins = 2;
		UT_ASSERT(!lazy_lock_buffer_for_cleanup(&vacrel, 1));
		UT_ASSERT(pinned && !exclusive && !fixture_ledger_live());
		UT_ASSERT_EQ(fixture_writer_acquisitions, 1);
		UT_ASSERT_EQ(fixture_header_checks, which);
		UT_ASSERT_EQ(unlocks, 1);
		UT_ASSERT_EQ(unpins + lock_attempts, 0);
		refused_without_output(1);
		/* The original SHARE/aggressive fallback still owns this pin. */
		ReleaseBuffer(1);
	}
}

UT_TEST(vacuum_other_relations_keep_conditional_cleanup)
{
	for (int which = 0; which < 3; which++)
	{
		LVRelState vacrel = vacuum_lock_setup(which == 1, which == 0);

		UT_ASSERT(lazy_lock_buffer_for_cleanup(&vacrel, 1));
		UT_ASSERT(pinned && exclusive && !fixture_ledger_live());
		UT_ASSERT_EQ(fixture_writer_acquisitions, 0);
		UT_ASSERT_EQ(lock_attempts, 1);
		UT_ASSERT_EQ(fixture_header_checks, 1);
		refused_without_output(1);
		LockBuffer(1, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(1);
	}
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(unlock_only_preserves_original_pin_and_original_node_grant);
	UT_RUN(resume_does_not_replace_original_object_fence_with_new_sample);
	UT_RUN(resume_original_and_cleanup_only_its_local_scope);
	UT_RUN(resume_refuses_without_original_pin_or_quiescent_scope);
	UT_RUN(resume_refuses_full_ledger_without_creating_or_waiting);
	UT_RUN(resume_refusals_remove_only_new_lock_and_ledger);
	UT_RUN(resume_error_unwinds_inner_lock_or_bound_ledger_before_rethrow);
	UT_RUN(reject_pin_before_descriptor);
	UT_RUN(reject_share_or_local_x_without_writer);
	UT_RUN(reject_inactive_or_changed_ledger);
	UT_RUN(reject_stale_original_object_or_terminal);
	UT_RUN(capture_and_match_original_active_writer);
	UT_RUN(reject_every_changed_logical_context_field);
	UT_RUN(reject_null_output_and_expectation);
	UT_RUN(vacuum_initial_writer_capture_and_original_resume);
	UT_RUN(vacuum_cleanup_pin_refusal_keeps_original_pin);
	UT_RUN(vacuum_other_relations_keep_conditional_cleanup);
	UT_DONE(); return ut_failed_count != 0;
}
