/*-------------------------------------------------------------------------
 * test_cluster_vm_writer_activation.c
 *    Original post-T3 writer constructors through VM continuation and clear.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Complete bufmgr constructors, capture/resume, heap VM helpers and bitmap
 * clear execute. T3 responses, native locks/pins, KO/terminal observations
 * and dirty marking are controlled leaves. This is not native Resource-X,
 * remote invalidation, a full LockBuffer, WAL, SQL or durability evidence.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)true)
#undef AssertMacro
#define AssertMacro(condition) ((void)true)
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif
#include "access/visibilitymap.h"
#include "cluster/cluster_content_trace.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ko.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/storage/cluster_smgr.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
int cluster_node_id = 0;
int NBuffers = 1, NLocBuffer;
bool cluster_enabled = true, cluster_shared_config = true, cluster_shared_catalog;
int cluster_interconnect_tier = CLUSTER_IC_TIER_1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static BufferDesc descriptor;
static PGAlignedBlock image, before;
static RelationData relation;
static FormData_pg_class relform;
static ClusterPcmOwnSnapshot own;
static ResourceXWriterUseContext terminal;
static ResourceXGateSnapshot gate;
static ResourceXWriterPath path;
static ResourceXApplyResult acquire_result, context_result;
static uint64 path_generation, object_revision;
static bool pinned, held, heap_held, object_current, terminal_current;
static bool busy, terminal_error, tracked;
static unsigned acquisitions, activations, samples, locks, unlocks, dirties;
static unsigned terminal_checks, fail_closed, failures, generic_unlocks;
static bool ledger_live(void);
static bool terminal_matches_ledger(void);
static void fixture_raise(void) pg_attribute_noreturn();

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
void
pg_re_throw(void)
{
	fixture_raise();
}
static void
fixture_raise(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}
#undef elog
#undef ereport
#define elog(level, ...) fixture_raise()
#define ereport(level, rest) fixture_raise()

static BufferDesc *
fixture_descriptor(int id)
{
	UT_ASSERT_EQ(id, 0);
	return &descriptor;
}
#undef GetBufferDescriptor
#define GetBufferDescriptor(id) fixture_descriptor(id)

bool
ClusterBufferHasSharedPin(Buffer buffer)
{
	return buffer == 1 && pinned;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&descriptor));
	return held;
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE);
	return LWLockHeldByMe(lock);
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&descriptor));
	UT_ASSERT(mode == LW_EXCLUSIVE && !held && pinned);
	held = true;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(&descriptor));
	UT_ASSERT(held && pinned && !ledger_live());
	held = false;
	unlocks++;
}
bool
ConditionalLockBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && pinned && !held);
	locks++;
	held = !busy;
	return held;
}
void
ReleaseBuffer(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && pinned && !held && !ledger_live());
	pinned = false;
}
static bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	UT_ASSERT(buf == &descriptor);
	return tracked;
}
ClusterPcmOwnResult
cluster_bufmgr_pcm_own_snapshot(BufferDesc *buf, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(buf == &descriptor && pinned);
	*out = own;
	return CLUSTER_PCM_OWN_OK;
}
int
cluster_smgr_which_for(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	UT_ASSERT_EQ(backend, InvalidBackendId);
	return 1;
}
bool
cluster_ko_peer_delivery_begin_v3(RelFileLocator locator, uint64 *revision)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation.rd_locator));
	/* The original local object sample also runs during resume below heap
	 * content; the VM content lock must still be absent. No block acquire. */
	UT_ASSERT(!held);
	samples++;
	*revision = object_revision;
	return object_current;
}
bool
cluster_ko_peer_delivery_current_v3(uint64 revision)
{
	return object_current && revision == object_revision;
}
bool
cluster_pcm_lock_resource_x_gate_snapshot(ResourceXGateSnapshot *out)
{
	*out = gate;
	return true;
}
ResourceXWriterPath
cluster_resource_x_writer_path_snapshot(uint64 *generation)
{
	*generation = path_generation;
	return path;
}
static bool
context_matches(const ResourceXWriterUseContext *a)
{
	return resource_x_assertion_equal(&a->ref.assertion, &terminal.ref.assertion)
		&& a->ref.formation == terminal.ref.formation
		&& a->ref.acquisition_generation == terminal.ref.acquisition_generation
		&& a->r4_record_generation == terminal.r4_record_generation
		&& a->buffer_ownership_generation == terminal.buffer_ownership_generation
		&& a->writer_activation_token == terminal.writer_activation_token
		&& a->resource_x_activation_generation == terminal.resource_x_activation_generation;
}
bool
cluster_gcs_resource_x_target_context_recheck_exact(const ResourceXWriterUseContext *context)
{
	return context_matches(context) && context_result == RESOURCE_X_APPLY_APPLIED;
}
ResourceXApplyResult
cluster_gcs_resource_x_target_context_recheck_result_exact(const ResourceXWriterUseContext *context)
{
	return context_matches(context) ? context_result : RESOURCE_X_APPLY_STALE;
}
ResourceXApplyResult
cluster_gcs_resource_x_target_acquire_reobserve_exact(BufferDesc *buf, const BufferTag *tag,
	uint64 r4_generation, ResourceXAuxiliaryAcquireContext *context, ResourceXAcquisitionRef *out)
{
	UT_ASSERT(buf == &descriptor && BufferTagsEqual(tag, &descriptor.tag));
	UT_ASSERT(!held && !heap_held && pinned);
	UT_ASSERT_EQ(r4_generation, 5);
	if (context->active)
		UT_ASSERT_EQ(context->absolute_deadline_us, 4242);
	context->active = true;
	context->absolute_deadline_us = 4242;
	context->reobserve = acquire_result == RESOURCE_X_APPLY_NOT_FOUND;
	*out = terminal.ref;
	acquisitions++;
	return acquire_result;
}
ResourceXApplyResult
cluster_gcs_resource_x_target_acquire_until_exact(BufferDesc *buf, const BufferTag *tag,
	uint64 r4_generation, uint64 *deadline, ResourceXAcquisitionRef *out)
{
	(void)buf; (void)tag; (void)r4_generation; (void)deadline; (void)out;
	abort(); /* This input domain always uses the auxiliary owner. */
}
static void
cluster_bufmgr_resource_x_writer_report_failure(ResourceXApplyResult result,
	BufferDesc *buf, const char *operation)
{
	(void)result; (void)operation;
	UT_ASSERT(buf == &descriptor);
	failures++;
	if (PG_exception_stack == NULL)
		fprintf(stderr, "# unhandled writer fixture failure: %s result=%d\n", operation, (int)result);
	fixture_raise();
}
static void
cluster_bufmgr_resource_x_fail_closed_exact(const ResourceXGateSnapshot *expected)
{
	UT_ASSERT(expected != NULL);
	fail_closed++;
}
#define cluster_content_trace_pair_event(buffer, event, result) ((void)0)
#define cluster_bufmgr_resource_x_wait_retry(lock, id, age, refused) (abort(), false)
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
void
cluster_pcm_lock_resource_x_trace_ref(uint16 kind, const ResourceXAcquisitionRef *ref, uint64 generation)
{
	UT_ASSERT_EQ(kind, RESOURCE_X_TRACE_PREUSE);
	UT_ASSERT(resource_x_assertion_equal(&ref->assertion, &terminal.ref.assertion));
	UT_ASSERT_EQ(generation, terminal.buffer_ownership_generation);
	activations++;
}
bool cluster_bufmgr_terminal_stamp_authority(Buffer, const BufferTag *, uint64 *, uint64 *, uint8 *);
bool
cluster_bufmgr_terminal_stamp_authority(Buffer buffer, const BufferTag *tag,
	uint64 *generation, uint64 *epoch, uint8 *state)
{
	UT_ASSERT(buffer == 1 && pinned && held && BufferTagsEqual(tag, &descriptor.tag));
	terminal_checks++;
	if (terminal_error)
		fixture_raise();
	*generation = own.generation;
	*epoch = gate.formation;
	*state = own.pcm_state;
	return terminal_current && own.flags == 0 && own.generation == terminal.buffer_ownership_generation
		&& terminal_matches_ledger();
}
#ifndef VM_WRITER_CONTEXT_FIXTURE
#define VM_WRITER_CONTEXT_FIXTURE "test_cluster_freeze_writer_context.inc"
#endif
#include VM_WRITER_CONTEXT_FIXTURE
#ifndef VM_WRITER_ACTIVATION_FIXTURE
#define VM_WRITER_ACTIVATION_FIXTURE "test_cluster_vm_writer_activation.inc"
#endif
#include VM_WRITER_ACTIVATION_FIXTURE

static bool
ledger_live(void)
{
	return cluster_bufmgr_pcm_x_writer_find(&descriptor) != NULL;
}
static bool
terminal_matches_ledger(void)
{
	ClusterPcmXWriterLedgerEntry *entry = cluster_bufmgr_pcm_x_writer_find(&descriptor);
	return entry != NULL && context_matches(&entry->authority);
}

/* Controlled LockBuffer leaf: execute the original constructors, then the
 * original local cleanup. It does not emulate native acquisition or unwind. */
bool
ClusterLockBufferExclusiveAuxiliaryAliasAware(Buffer *buffer, Buffer *alias,
	const ClusterBufferBarrierSiteId *site, ResourceXAuxiliaryAcquireContext *context)
{
	ClusterPcmXWriterLedgerEntry *entry;
	uint64 deadline = context->absolute_deadline_us;
	bool barrier = false, transient = false;

	UT_ASSERT(*buffer == 1 && alias == NULL && site == NULL);
	entry = cluster_bufmgr_pcm_x_writer_prepare_target(&descriptor, &descriptor.tag,
		PCM_LOCK_MODE_X, 5, &deadline, &barrier, &transient, context);
	if (entry == NULL)
		return false;
	(void)LWLockAcquire(entry->content_lock, LW_EXCLUSIVE);
	if (!cluster_bufmgr_pcm_x_writer_activate(entry, true)) {
		LWLockRelease(BufferDescriptorGetContentLock(&descriptor));
		return false;
	}
	return true;
}
void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 1 && mode == BUFFER_LOCK_UNLOCK);
	generic_unlocks++;
	ClusterUnlockBufferAfterConditionalMutation(buffer);
}
#include "test_cluster_heap_insert_vm_authority.inc"

static Page
fixture_page(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && pinned && held && ledger_live());
	return image.data;
}
#undef BufferGetPage
#define BufferGetPage(buffer) fixture_page(buffer)
void
BufferGetTag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	UT_ASSERT(buffer == 1 && pinned);
	*locator = BufTagGetRelFileLocator(&descriptor.tag);
	*forknum = descriptor.tag.forkNum;
	*block = descriptor.tag.blockNum;
}
void
MarkBufferDirty(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && held && heap_held && ledger_live());
	UT_ASSERT_EQ(acquisitions, 1);
	dirties++;
}
#include "test_cluster_vm_writer_clear.inc"

static void
setup(void)
{
	PageHeader header;

	memset(&descriptor, 0, sizeof(descriptor));
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&own, 0, sizeof(own));
	memset(&terminal, 0, sizeof(terminal));
	memset(&gate, 0, sizeof(gate));
	memset(cluster_bufmgr_pcm_x_writer_ledger, 0, sizeof(cluster_bufmgr_pcm_x_writer_ledger));
	relation.rd_locator = (RelFileLocator){1663, 5, 19000};
	relation.rd_rel = &relform;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	InitBufferTag(&descriptor.tag, &relation.rd_locator, VISIBILITYMAP_FORKNUM, 0);
	own.tag = descriptor.tag;
	own.generation = terminal.buffer_ownership_generation = 6;
	own.pcm_state = PCM_STATE_X;
	terminal.ref.assertion.resource = descriptor.tag;
	terminal.ref.assertion.requester_node = 0;
	terminal.ref.formation = gate.formation = 3;
	terminal.ref.acquisition_generation = 4;
	terminal.r4_record_generation = path_generation = 5;
	gate.phase = RESOURCE_X_GATE_OPEN;
	path = RESOURCE_X_WRITER_TARGET;
	acquire_result = context_result = RESOURCE_X_APPLY_APPLIED;
	object_revision = 7;
	pinned = object_current = terminal_current = tracked = true;
	held = heap_held = busy = terminal_error = false;
	acquisitions = activations = samples = locks = unlocks = dirties = 0;
	terminal_checks = fail_closed = failures = generic_unlocks = 0;
	memset(&image, 0, sizeof(image));
	header = (PageHeader)image.data;
	header->pd_lower = MAXALIGN(SizeOfPageHeaderData);
	header->pd_upper = header->pd_special = BLCKSZ;
	PageSetPageSizeAndVersion(image.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	header->pd_block_scn = 100;
	memset(PageGetContents(image.data), 0xff, MAPSIZE);
	before = image;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
}
static ClusterHeapVmMutationState
prepare(void)
{
	ClusterHeapVmMutationState state = {0};
	ResourceXAuxiliaryAcquireContext context = {0};
	Buffer buffer = 1;

	cluster_heap_vm_mutation_prepare(&buffer, NULL, &state, &context);
	return state;
}
static bool
resume(ClusterHeapVmMutationState *state)
{
	ResourceXAuxiliaryAcquireContext context = {0};
	Buffer buffer = 1;

	heap_held = true;
	return cluster_heap_vm_mutation_lock(&buffer, true, state, &context);
}
static void
unchanged(void)
{
	UT_ASSERT(memcmp(&image, &before, sizeof(image)) == 0);
	UT_ASSERT_EQ(dirties, 0);
}
static void
local_cleanup(void)
{
	/* Controlled outer owner; does not certify native ERROR cleanup. */
	cluster_bufmgr_pcm_x_writer_clear(cluster_bufmgr_pcm_x_writer_find(&descriptor));
	if (held)
		LWLockRelease(BufferDescriptorGetContentLock(&descriptor));
}
UT_TEST(cold_scope_cannot_clear_before_original_preparation)
{
	ClusterHeapVmMutationState state = {0};
	setup();
	UT_ASSERT(!resume(&state));
	UT_ASSERT_EQ(acquisitions + locks, 0);
	UT_ASSERT(pinned && !held && !ledger_live());
	unchanged();
}
UT_TEST(original_constructors_capture_then_resume_without_second_acquire)
{
	ClusterHeapVmMutationState state;
	setup(); state = prepare();
	UT_ASSERT(state.prepared && pinned && !held && !ledger_live());
	UT_ASSERT(context_matches(&state.writer));
	UT_ASSERT_EQ(state.object_revision, 7);
	UT_ASSERT_EQ(acquisitions, 1); UT_ASSERT_EQ(activations, 1);
	UT_ASSERT_EQ(generic_unlocks, 1);
	unchanged();
	UT_ASSERT(resume(&state));
	UT_ASSERT(ClusterBufferMutationContextMatches(1, &state.writer, 7, true));
	UT_ASSERT(visibilitymap_clear_locked(&relation, 1, 1, VISIBILITYMAP_VALID_BITS));
	UT_ASSERT_EQ((uint8)PageGetContents(image.data)[0], 0xf3);
	UT_ASSERT_EQ(dirties, 1); UT_ASSERT_EQ(acquisitions, 1);
	ClusterUnlockBufferAfterConditionalMutation(1);
	UT_ASSERT(pinned && !held && !ledger_live());
}
UT_TEST(acquiring_ledger_is_never_a_captured_writer)
{
	for (int phase = PCM_X_WRITER_LEDGER_HANDOFF; phase <= PCM_X_WRITER_LEDGER_ACQUIRING; phase++) {
		ResourceXAuxiliaryAcquireContext aux = {0};
		ResourceXWriterUseContext out, sentinel;
		uint64 deadline = 0, revision = 99;
		bool barrier = false, transient = false, checked = false;
		ClusterPcmXWriterLedgerEntry *entry;

		setup();
		entry = cluster_bufmgr_pcm_x_writer_prepare_target(&descriptor, &descriptor.tag,
			PCM_LOCK_MODE_X, 5, &deadline, &barrier, &transient, &aux);
		UT_ASSERT(entry != NULL); if (entry == NULL) continue;
		UT_ASSERT_EQ(entry->phase, PCM_X_WRITER_LEDGER_ACQUIRING);
		entry->phase = phase;
		held = true;
		memset(&out, 0xa5, sizeof(out)); sentinel = out;
		UT_ASSERT(!ClusterBufferCaptureMutationContext(1, &out, &revision, &checked));
		UT_ASSERT(memcmp(&out, &sentinel, sizeof(out)) == 0);
		UT_ASSERT_EQ(revision, 99); UT_ASSERT(!checked);
		UT_ASSERT_EQ(activations, 0); unchanged(); local_cleanup();
	}
}
UT_TEST(refused_acquisition_leaves_no_writer_or_bitmap_change)
{
	ResourceXApplyResult results[] = {RESOURCE_X_APPLY_BAD_STATE, RESOURCE_X_APPLY_NOT_FOUND};
	for (unsigned i = 0; i < lengthof(results); i++) {
		ClusterHeapVmMutationState state;
		setup(); acquire_result = results[i]; state = prepare();
		UT_ASSERT(!state.prepared && pinned && !held && !ledger_live());
		UT_ASSERT_EQ(acquisitions, 1); UT_ASSERT_EQ(activations + generic_unlocks, 0);
		unchanged();
	}
}
UT_TEST(duplicate_terminal_response_still_needs_original_activation)
{
	ClusterHeapVmMutationState state;
	setup(); acquire_result = RESOURCE_X_APPLY_DUPLICATE; state = prepare();
	UT_ASSERT(state.prepared); UT_ASSERT_EQ(activations, 1);
	UT_ASSERT(resume(&state));
	UT_ASSERT(visibilitymap_clear_locked(&relation, 0, 1, VISIBILITYMAP_ALL_FROZEN));
	UT_ASSERT_EQ((uint8)PageGetContents(image.data)[0], 0xfd);
	local_cleanup();
}
UT_TEST(activation_drift_cannot_export_a_writer)
{
	for (int why = 0; why < 7; why++) {
		ResourceXAuxiliaryAcquireContext aux = {0};
		ClusterPcmXWriterLedgerEntry *entry;
		uint64 deadline = 0;
		bool barrier = false, transient = false;
		volatile bool activated = false;

		setup();
		entry = cluster_bufmgr_pcm_x_writer_prepare_target(&descriptor, &descriptor.tag,
			PCM_LOCK_MODE_X, 5, &deadline, &barrier, &transient, &aux);
		UT_ASSERT(entry != NULL); if (entry == NULL) continue;
		held = true;
		switch (why) {
			case 0: gate.formation++; break;
			case 1: path_generation++; break;
			case 2: own.generation++; break;
			case 3: own.flags = PCM_OWN_FLAG_GRANT_PENDING; break;
			case 4: context_result = RESOURCE_X_APPLY_BAD_STATE; break;
			case 5: object_revision++; break;
			case 6: object_current = false; break;
		}
		PG_TRY(); { activated = cluster_bufmgr_pcm_x_writer_activate(entry, true); }
		PG_CATCH(); { } PG_END_TRY();
		UT_ASSERT(!activated); UT_ASSERT_EQ(activations, 0);
		unchanged(); local_cleanup();
	}
}
UT_TEST(changed_original_resume_context_never_authorizes_clear)
{
	for (int why = 0; why < 7; why++) {
		ClusterHeapVmMutationState state;
		setup(); state = prepare();
		switch (why) {
			case 0: state.writer.ref.acquisition_generation++; break;
			case 1: state.writer.ref.formation++; break;
			case 2: state.writer.r4_record_generation++; break;
			case 3: state.writer.buffer_ownership_generation++; break;
			case 4: state.writer.writer_activation_token++; break;
			case 5: state.writer.resource_x_activation_generation++; break;
			case 6: terminal_current = false; break;
		}
		UT_ASSERT(!resume(&state)); UT_ASSERT(!state.prepared);
		UT_ASSERT(pinned && !held && !ledger_live());
		UT_ASSERT_EQ(acquisitions, 1); unchanged(); local_cleanup();
	}
}
UT_TEST(new_object_sample_cannot_refresh_original_vm_proof)
{
	ClusterHeapVmMutationState state;
	setup(); state = prepare(); object_revision++;
	UT_ASSERT(!resume(&state)); UT_ASSERT(!state.prepared);
	UT_ASSERT(pinned && !held && !ledger_live());
	UT_ASSERT_EQ(locks, 0); UT_ASSERT_EQ(acquisitions, 1); unchanged();
}
UT_TEST(local_contention_keeps_pin_and_requires_reprepare)
{
	ClusterHeapVmMutationState state;
	setup(); state = prepare(); busy = true;
	UT_ASSERT(!resume(&state)); UT_ASSERT(!state.prepared);
	UT_ASSERT(pinned && !held && !ledger_live());
	UT_ASSERT_EQ(locks, 1); UT_ASSERT_EQ(acquisitions, 1); unchanged();
}
UT_TEST(capture_error_uses_original_heap_helper_finally)
{
	volatile bool caught = false;
	setup(); terminal_error = true;
	PG_TRY(); { (void)prepare(); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && pinned && !held && !ledger_live());
	UT_ASSERT_EQ(generic_unlocks, 1); unchanged();
}
UT_TEST(resume_error_uses_original_bufmgr_finally)
{
	ClusterHeapVmMutationState state;
	volatile bool caught = false;
	setup(); state = prepare(); terminal_error = true;
	PG_TRY(); { (void)resume(&state); } PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught && pinned && !held && !ledger_live());
	UT_ASSERT_EQ(acquisitions, 1); UT_ASSERT_EQ(unlocks, 2); unchanged();
}
UT_TEST(invalid_vm_image_fails_before_even_a_noop_clear)
{
	ClusterHeapVmMutationState state;
	volatile bool caught = false;
	setup(); state = prepare(); UT_ASSERT(resume(&state));
	((PageHeader)image.data)->pd_block_scn = 0;
	memset(PageGetContents(image.data), 0, MAPSIZE); before = image;
	PG_TRY(); { (void)visibilitymap_clear_locked(&relation, 0, 1, VISIBILITYMAP_VALID_BITS); }
	PG_CATCH(); { caught = true; } PG_END_TRY();
	UT_ASSERT(caught); unchanged(); local_cleanup();
}
int
main(void)
{
	UT_PLAN(12);
	UT_RUN(cold_scope_cannot_clear_before_original_preparation);
	UT_RUN(original_constructors_capture_then_resume_without_second_acquire);
	UT_RUN(acquiring_ledger_is_never_a_captured_writer);
	UT_RUN(refused_acquisition_leaves_no_writer_or_bitmap_change);
	UT_RUN(duplicate_terminal_response_still_needs_original_activation);
	UT_RUN(activation_drift_cannot_export_a_writer);
	UT_RUN(changed_original_resume_context_never_authorizes_clear);
	UT_RUN(new_object_sample_cannot_refresh_original_vm_proof);
	UT_RUN(local_contention_keeps_pin_and_requires_reprepare);
	UT_RUN(capture_error_uses_original_heap_helper_finally);
	UT_RUN(resume_error_uses_original_bufmgr_finally);
	UT_RUN(invalid_vm_image_fails_before_even_a_noop_clear);
	UT_DONE();
	return ut_failed_count != 0;
}
