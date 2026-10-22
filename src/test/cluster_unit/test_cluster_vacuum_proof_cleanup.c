/*-------------------------------------------------------------------------
 * test_cluster_vacuum_proof_cleanup.c
 *    Exercise real VACUUM proof ownership and resident-page recheck cleanup.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * The helpers, runtime guard cleanup callback, buffer release wrappers and
 * PG exception macros are real. The generic unlock's cache-off tail is also
 * extracted verbatim; its PCM release boundary can raise a distinct ERROR.
 * Acquisition/native unpin/route/admission/proof providers are leaves: this
 * tests cleanup order and exception preservation, not real transport,
 * shared admission counters or physical proof validity.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

bool cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
bool cluster_enabled, cluster_gcs_block_local_cache;
int cluster_node_id, cluster_pcm_grd_max_entries;
int NBuffers = 1, NLocBuffer = 1;
static ClusterConf fixture_conf;
ClusterConf *ClusterConfShmem = &fixture_conf;
static BufferDescPadded fixture_descriptors[1];
BufferDescPadded *BufferDescriptors = fixture_descriptors;

/* Only fields consumed by the extracted production helper. */
typedef struct LVRelState
{
	Relation rel;
	ClusterSpaceIdentity identity;
	ClusterHeapFreezePageInputV1 shared_freeze_input;
	ClusterHeapFreezePageBatchV1 shared_freeze_batch;
	ClusterHeapFreezeProofBatchV1 shared_freeze_proofs;
	ClusterHeapFreezeFinalFenceV1 shared_freeze_fence;
	bool shared_freeze_fence_valid;
} LVRelState;

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

static RelationData relation;
static LVRelState vacrel;
static unsigned enters, leaves, debt, resolves, routes;
static unsigned fail_resolve;
static PGAlignedBlock resident_page;
static unsigned repins, lock_attempts, page_checks, validations;
static unsigned generic_unlocks, remote_releases, ownership_mirrors;
static bool pin_held, content_locked, resident, lock_available;
static bool remote_release_error, leave_with_ownership;
static bool changed_holder, changed_master, changed_admission;
static bool error_page_check, validation_under_lock;
static ClusterHeapFreezePageMatchV1 page_match;
static bool reject_admission, reject_route_after, unknown_outcome;
static bool error_preflight, error_validate, guard_held;
static int inject_code, caught_code;
static ClusterHeapFreezeProofStatusV1 collected_status;
static char cleanup_order[16];
static unsigned cleanup_count;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;

static void
note_cleanup(char event)
{
	if (cleanup_count + 1 >= sizeof(cleanup_order))
		abort();
	cleanup_order[cleanup_count++] = event;
	cleanup_order[cleanup_count] = '\0';
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack == NULL)
		abort();
	siglongjmp(*PG_exception_stack, 1);
}

bool
errstart(int level, const char *domain)
{
	(void)domain;
	return level >= ERROR;
}

bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}

int
errcode(int code)
{
	caught_code = code;
	return 0;
}

int
errmsg(const char *format, ...)
{
	(void)format;
	return 0;
}

void
errfinish(const char *file, int line, const char *function)
{
	(void)file;
	(void)line;
	(void)function;
	pg_re_throw();
}

static void
throw_error(void)
{
	ereport(ERROR, (errcode(inject_code), errmsg("proof wait fault injection")));
}

void
before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (exit_callback != NULL)
		abort();
	exit_callback = callback;
	exit_arg = arg;
}

void
cancel_before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (callback != exit_callback || arg != exit_arg)
		abort();
	exit_callback = NULL;
}

void
cluster_undo_block0_current_cancel(ClusterUndoBlock0CurrentGuard *guard)
{
	(void)guard;
	if (!guard_held || debt != 1)
		abort();
	guard_held = false;
	note_cleanup('G');
}

#include "test_cluster_vacuum_proof_guard.inc"

bool
cluster_pcm_lock_r4_route_snapshot(BufferTag tag, PcmAuthoritySnapshot *authority,
								 uint64 *master, SCN *page_scn)
{
	(void)tag;
	routes++;
	authority->transition_count = routes > 1 && changed_holder ? 8 : 7;
	*master = routes > 1 && changed_master ? 12 : 11;
	*page_scn = 13;
	return !(routes > 1 && reject_route_after);
}

bool
cluster_pcm_lock_authority_matches(BufferTag tag, const PcmAuthoritySnapshot *expected)
{
	(void)tag;
	return expected->transition_count == 7 && !changed_holder;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken *token)
{
	enters++;
	if (reject_admission)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (debt != 0)
		abort();
	token->record_generation = 17;
	token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token->entered = true;
	debt++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (guard_held || !token->entered || token->record_generation != 17 || debt != 1)
		abort();
	/* Record a violated ordering so the exact-parent RED can report it. */
	leave_with_ownership |= pin_held || content_locked;
	leaves++;
	debt--;
	token->entered = false;
	note_cleanup('A');
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	return token->entered && debt == 1 && !changed_admission;
}

void
cluster_tx_resolve_terminal_census_batch_preflight(void)
{
	if (error_preflight)
		throw_error();
}

ClusterTxOutcome
cluster_tx_resolve_exact_admitted(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
								const ClusterSemanticAdmissionToken *admission,
								ClusterTxResolution *out, ClusterTxResolveReason *reason)
{
	ClusterUndoBlock0CurrentGuard guard;
	ClusterRuntimeCandidateCleanup cleanup;

	(void)locator;
	if (mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS || !admission->entered || debt != 1)
		abort();
	resolves++;
	memset(&guard, 0, sizeof(guard));
	cleanup.guard = &guard;
	cleanup.active = true;
	guard_held = true;
	/* Match the runtime's existing guarded wait/error unwind ownership. */
	PG_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
							PointerGetDatum(&cleanup));
	{
		if (resolves == fail_resolve)
			throw_error();
		guard_held = false;
		cleanup.active = false;
	}
	PG_END_ENSURE_ERROR_CLEANUP(cluster_runtime_visibility_candidate_cleanup,
								PointerGetDatum(&cleanup));
	memset(out, 0, sizeof(*out));
	*reason = unknown_outcome ? CLUSTER_TX_RESOLVE_PROTOCOL : CLUSTER_TX_RESOLVE_NONE;
	return unknown_outcome ? CLUSTER_TX_UNKNOWN : CLUSTER_TX_COMMITTED;
}

bool
cluster_heap_freeze_build_canonical_proof_v1(
	const ClusterTxLocator *locator, const ClusterTxResolution *resolution,
	const ClusterHeapFreezeFinalFenceV1 *expected,
	const ClusterHeapFreezeFinalFenceV1 *current, bool finalized,
	bool authority_current, bool admission_current, ClusterHeapFreezeCanonicalProofV1 *out)
{
	(void)locator;
	(void)resolution;

	(void)finalized;

	memset(out, 0, sizeof(*out));
	out->expected_fence = *expected;
	out->current_fence = *current;
	out->authority_current = authority_current;
	out->admission_current = admission_current;
	return true;
}

ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_conditional_apply_v1(
	const ClusterHeapFreezePageBatchV1 *page, const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence)
{
	(void)page;
	(void)fence;
	validations++;
	validation_under_lock = pin_held && content_locked;
	if (error_validate)
		throw_error();
	if (!proofs->proofs[0].authority_current || !proofs->proofs[0].admission_current
		|| proofs->proofs[0].expected_fence.page_lsn != fence->page_lsn
		|| proofs->proofs[0].expected_fence.page_version != fence->page_version)
		return CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED;
	return CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
}

/* Storage leaves never obtain a current block or wait for its ownership. */
bool
ReadRecentBuffer(RelFileLocator locator, ForkNumber fork, BlockNumber block, Buffer buffer)
{
	if (pin_held || content_locked || guard_held || debt != 1 || buffer != 1
		|| fork != MAIN_FORKNUM || block != vacrel.shared_freeze_batch.block
		|| !RelFileLocatorEquals(locator, relation.rd_locator))
		abort();
	repins++;
	if (!resident)
		return false;
	pin_held = true;
	return true;
}

bool
ConditionalLockBuffer(Buffer buffer)
{
	if (buffer != 1 || !pin_held || content_locked)
		abort();
	lock_attempts++;
	content_locked = lock_available;
	return content_locked;
}

static void
UnpinBuffer(BufferDesc *buf)
{
	if (buf != GetBufferDescriptor(0) || !pin_held || content_locked)
		abort();
	pin_held = false;
	note_cleanup('P');
}

void
UnpinLocalBuffer(Buffer buffer)
{
	(void)buffer;
	abort();
}

void
LWLockRelease(LWLock *lock)
{
	if (lock != BufferDescriptorGetContentLock(GetBufferDescriptor(0))
		|| !pin_held || !content_locked || debt != 1)
		abort();
	content_locked = false;
	note_cleanup('L');
}

static bool
cluster_bufmgr_should_pcm_track(BufferDesc *buf)
{
	return buf == GetBufferDescriptor(0);
}

void
cluster_pcm_lock_unlock_content_buffer(BufferDesc *buf, PcmLockMode mode)
{
	if (buf != GetBufferDescriptor(0) || mode != PCM_LOCK_MODE_X || content_locked)
		abort();
	remote_releases++;
	if (remote_release_error)
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cache-off PCM release fault injection")));
}

PcmLockMode
cluster_pcm_lock_query(BufferTag tag)
{
	(void)tag;
	return PCM_LOCK_MODE_N;
}

static void
cluster_pcm_own_transition(BufferDesc *buf, uint8 state, uint32 set_flags,
						   uint32 clear_flags, const char *context)
{
	(void)set_flags;
	(void)clear_flags;
	(void)context;
	ownership_mirrors++;
	buf->pcm_state = state;
}

/* Only this tail is real; acquisition and writer-ledger machinery are not. */
#include "test_cluster_vacuum_proof_unlock_tail.inc"

void
LockBuffer(Buffer buffer, int mode)
{
	if (buffer != 1 || mode != BUFFER_LOCK_UNLOCK)
		abort();
	generic_unlocks++;
	LWLockRelease(BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	fixture_lockbuffer_unlock_tail(GetBufferDescriptor(0), mode, false);
}

#include "test_cluster_vacuum_proof_release.inc"

static Page
fixture_buffer_page(Buffer buffer)
{
	if (buffer != 1 || !pin_held || !content_locked)
		abort();
	return (Page)resident_page.data;
}

#undef BufferGetPage
#define BufferGetPage(buffer) fixture_buffer_page(buffer)

ClusterHeapFreezePageMatchV1
cluster_heap_freeze_validate_page_v1(const ClusterHeapFreezePageInputV1 *input,
									 const ClusterSpaceIdentity *identity, BlockNumber block, Page page)
{
	if (!pin_held || !content_locked || input != &vacrel.shared_freeze_input
		|| identity != &vacrel.identity || block != vacrel.shared_freeze_batch.block
		|| page != (Page)resident_page.data)
		abort();
	page_checks++;
	if (error_page_check)
		throw_error();
	return page_match;
}

#include "test_cluster_vacuum_proof_cleanup.inc"

static void
reset_fixture(void)
{
	memset(&relation, 0, sizeof(relation));
	memset(&vacrel, 0, sizeof(vacrel));
	vacrel.rel = &relation;
	relation.rd_isvalid = true;
	relation.rd_locator.relNumber = 42;
	vacrel.identity.key.locator = relation.rd_locator;
	vacrel.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	vacrel.shared_freeze_batch.identity = vacrel.identity;
	vacrel.shared_freeze_batch.block = 3;
	vacrel.shared_freeze_batch.page_lsn = 101;
	vacrel.shared_freeze_batch.page_version = 13;
	memset(&resident_page, 0, sizeof(resident_page));
	PageSetLSN((Page)resident_page.data, 101);
	((PageHeader)resident_page.data)->pd_block_scn = 13;
	repins = lock_attempts = page_checks = validations = 0;
	generic_unlocks = remote_releases = ownership_mirrors = 0;
	remote_release_error = leave_with_ownership = false;
	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_pcm_grd_max_entries = -1;
	cluster_gcs_block_local_cache = true;
	fixture_conf.node_count = 2;
	memset(fixture_descriptors, 0, sizeof(fixture_descriptors));
	GetBufferDescriptor(0)->pcm_state = (uint8) PCM_STATE_X;
	pin_held = content_locked = false;
	resident = lock_available = true;
	changed_holder = changed_master = changed_admission = false;
	error_page_check = validation_under_lock = false;
	page_match = CLUSTER_HEAP_FREEZE_PAGE_MATCHED;
	vacrel.shared_freeze_batch.carrier_request_count = 1;
	enters = leaves = debt = resolves = routes = fail_resolve = 0;
	reject_admission = reject_route_after = unknown_outcome = false;
	error_preflight = error_validate = guard_held = false;
	inject_code = ERRCODE_INTERNAL_ERROR;
	caught_code = 0;
	collected_status = CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	cleanup_count = 0;
	cleanup_order[0] = '\0';
	exit_callback = NULL;
	PG_exception_stack = NULL;
	error_context_stack = NULL;
}

static bool
collect_catches_error(void)
{
	volatile bool caught = false;

	PG_TRY();
	{
		collected_status = lazy_collect_shared_freeze_proofs(&vacrel, 1);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

static void
check_error_cleanup(const char *order)
{
	UT_ASSERT(collect_catches_error());
	UT_ASSERT_EQ(caught_code, inject_code);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT(!guard_held);
	UT_ASSERT(!pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_STR_EQ(cleanup_order, order);
	UT_ASSERT(PG_exception_stack == NULL);
	UT_ASSERT(error_context_stack == NULL);
	UT_ASSERT(exit_callback == NULL);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
}

UT_TEST(invalid_input_does_not_leave)
{
	reset_fixture();
	vacrel.shared_freeze_batch.carrier_request_count = 0;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(enters, 0);
	UT_ASSERT_EQ(leaves, 0);
}

UT_TEST(rejected_admission_does_not_leave)
{
	reset_fixture();
	reject_admission = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(leaves, 0);
}

UT_TEST(normal_success_leaves_once)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT(PG_exception_stack == NULL);
}

UT_TEST(unknown_resolver_leaves_once)
{
	reset_fixture();
	unknown_outcome = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT(PG_exception_stack == NULL);
}

UT_TEST(missing_final_route_leaves_once)
{
	reset_fixture();
	reject_route_after = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
				 CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT(PG_exception_stack == NULL);
}

UT_TEST(preflight_error_leaves_once)
{
	reset_fixture();
	error_preflight = true;
	check_error_cleanup("A");
	UT_ASSERT_EQ(resolves, 0);
}

UT_TEST(resolver_error_unwinds_guard_before_admission)
{
	reset_fixture();
	fail_resolve = 1;
	check_error_cleanup("GA");
}

UT_TEST(query_cancel_unwinds_guard_before_admission)
{
	reset_fixture();
	fail_resolve = 1;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("GA");
}

UT_TEST(later_carrier_cancel_does_not_publish_partial_proofs)
{
	reset_fixture();
	vacrel.shared_freeze_batch.carrier_request_count = 2;
	fail_resolve = 2;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("GA");
	UT_ASSERT_EQ(resolves, 2);
}

UT_TEST(validation_error_leaves_once)
{
	reset_fixture();
	error_validate = true;
	check_error_cleanup("LPA");
}

UT_TEST(same_backend_can_collect_again_after_cancel)
{
	reset_fixture();
	fail_resolve = 1;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("GA");
	/* Do not reset the admission debt or exception/exit stacks. */
	if (debt != 0)
		return;
	fail_resolve = 0;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "GALPA");
	UT_ASSERT(PG_exception_stack == NULL);
}


UT_TEST(final_check_owns_resident_pin_and_local_x)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(repins, 1);
	UT_ASSERT_EQ(lock_attempts, 1);
	UT_ASSERT_EQ(page_checks, 1);
	UT_ASSERT(validation_under_lock);
	UT_ASSERT(!pin_held && !content_locked);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(evicted_page_requests_retry_without_fetch)
{
	reset_fixture();
	resident = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(repins, 1);
	UT_ASSERT_EQ(lock_attempts, 0);
	UT_ASSERT_EQ(validations, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_STR_EQ(cleanup_order, "A");
}

UT_TEST(unavailable_local_x_releases_pin_and_requests_retry)
{
	reset_fixture();
	lock_available = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(lock_attempts, 1);
	UT_ASSERT_EQ(page_checks, 0);
	UT_ASSERT_EQ(validations, 0);
	UT_ASSERT(!pin_held && !content_locked);
	UT_ASSERT_STR_EQ(cleanup_order, "PA");
}

UT_TEST(changed_page_bytes_requests_retry)
{
	reset_fixture();
	page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(page_checks, 1);
	UT_ASSERT_EQ(validations, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(invalid_page_input_keeps_hard_error_status)
{
	reset_fixture();
	page_match = CLUSTER_HEAP_FREEZE_PAGE_INVALID;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR);
	UT_ASSERT_EQ(validations, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(current_lsn_is_read_under_local_x)
{
	reset_fixture();
	PageSetLSN((Page)resident_page.data, 102);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(vacrel.shared_freeze_fence.page_lsn, 102);
	UT_ASSERT(validation_under_lock);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(current_version_is_read_under_local_x)
{
	reset_fixture();
	((PageHeader)resident_page.data)->pd_block_scn = 14;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(vacrel.shared_freeze_fence.page_version, 14);
	UT_ASSERT(validation_under_lock);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(changed_pcm_authority_and_admission_request_retry)
{
	reset_fixture();
	changed_holder = changed_master = changed_admission = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(vacrel.shared_freeze_fence.holder_sequence, 8);
	UT_ASSERT_EQ(vacrel.shared_freeze_fence.master_sequence, 12);
	UT_ASSERT(validation_under_lock);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(local_page_check_cancel_releases_buffer_before_admission)
{
	reset_fixture();
	error_page_check = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("LPA");
	UT_ASSERT_EQ(page_checks, 1);
	UT_ASSERT(!pin_held && !content_locked);
}

UT_TEST(invalidated_relation_skips_resident_lookup)
{
	reset_fixture();
	relation.rd_isvalid = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(repins, 0);
	UT_ASSERT_EQ(validations, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "A");
}

static bool
generic_release_catches_error(void)
{
	volatile bool caught = false;

	PG_TRY();
	{
		UnlockReleaseBuffer(1);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(actual_generic_cache_off_error_precedes_native_unpin)
{
	ClusterSemanticAdmissionToken admission = {0};

	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&admission),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	pin_held = content_locked = true;
	UT_ASSERT(generic_release_catches_error());
	UT_ASSERT_EQ(caught_code, ERRCODE_CONNECTION_FAILURE);
	UT_ASSERT_EQ(generic_unlocks, 1);
	UT_ASSERT_EQ(remote_releases, 1);
	UT_ASSERT_EQ(ownership_mirrors, 0);
	UT_ASSERT(pin_held && !content_locked);
	UT_ASSERT_STR_EQ(cleanup_order, "L");
	/* The fixture owns the retained pin; do not reset it to hide the failure. */
	ReleaseBuffer(1);
	cluster_semantic_activation_leave(&admission);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	UT_ASSERT(!leave_with_ownership && debt == 0);
	UT_ASSERT(PG_exception_stack == NULL);
}

UT_TEST(actual_generic_cache_off_success_releases_pcm)
{
	ClusterSemanticAdmissionToken admission = {0};

	reset_fixture();
	cluster_gcs_block_local_cache = false;
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&admission),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	pin_held = content_locked = true;
	UT_ASSERT(!generic_release_catches_error());
	cluster_semantic_activation_leave(&admission);
	UT_ASSERT_EQ(generic_unlocks, 1);
	UT_ASSERT_EQ(remote_releases, 1);
	UT_ASSERT_EQ(ownership_mirrors, 1);
	UT_ASSERT_EQ(GetBufferDescriptor(0)->pcm_state, PCM_STATE_N);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	UT_ASSERT(!pin_held && !content_locked && !leave_with_ownership && debt == 0);
}

static void
check_local_cleanup_without_pcm_release(const char *order)
{
	UT_ASSERT(!pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(generic_unlocks, 0);
	UT_ASSERT_EQ(remote_releases, 0);
	UT_ASSERT_EQ(ownership_mirrors, 0);
	UT_ASSERT_EQ(GetBufferDescriptor(0)->pcm_state, PCM_STATE_X);
	UT_ASSERT_STR_EQ(cleanup_order, order);
	UT_ASSERT(PG_exception_stack == NULL);
}

UT_TEST(cache_off_success_uses_only_local_cleanup)
{
	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(cache_off_changed_page_uses_only_local_cleanup)
{
	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(cache_off_validation_error_preserves_original_error)
{
	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	error_validate = true;
	check_error_cleanup("LPA");
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(cache_off_page_cancel_preserves_error_and_backend_reuse)
{
	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	error_page_check = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("LPA");
	check_local_cleanup_without_pcm_release("LPA");
	/* Keep the pin/admission/exception stacks: reuse only after real cleanup. */
	if (pin_held || content_locked || debt != 0)
		return;
	error_page_check = false;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	check_local_cleanup_without_pcm_release("LPALPA");
}

UT_TEST(cache_off_refused_local_lock_releases_only_pin)
{
	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	lock_available = false;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(page_checks, 0);
	check_local_cleanup_without_pcm_release("PA");
}

int
main(void)
{
	UT_PLAN(28);
	UT_RUN(invalid_input_does_not_leave);
	UT_RUN(rejected_admission_does_not_leave);
	UT_RUN(normal_success_leaves_once);
	UT_RUN(unknown_resolver_leaves_once);
	UT_RUN(missing_final_route_leaves_once);
	UT_RUN(preflight_error_leaves_once);
	UT_RUN(resolver_error_unwinds_guard_before_admission);
	UT_RUN(query_cancel_unwinds_guard_before_admission);
	UT_RUN(later_carrier_cancel_does_not_publish_partial_proofs);
	UT_RUN(validation_error_leaves_once);
	UT_RUN(same_backend_can_collect_again_after_cancel);
	UT_RUN(final_check_owns_resident_pin_and_local_x);
	UT_RUN(evicted_page_requests_retry_without_fetch);
	UT_RUN(unavailable_local_x_releases_pin_and_requests_retry);
	UT_RUN(changed_page_bytes_requests_retry);
	UT_RUN(invalid_page_input_keeps_hard_error_status);
	UT_RUN(current_lsn_is_read_under_local_x);
	UT_RUN(current_version_is_read_under_local_x);
	UT_RUN(changed_pcm_authority_and_admission_request_retry);
	UT_RUN(local_page_check_cancel_releases_buffer_before_admission);
	UT_RUN(invalidated_relation_skips_resident_lookup);
	UT_RUN(actual_generic_cache_off_error_precedes_native_unpin);
	UT_RUN(actual_generic_cache_off_success_releases_pcm);
	UT_RUN(cache_off_success_uses_only_local_cleanup);
	UT_RUN(cache_off_changed_page_uses_only_local_cleanup);
	UT_RUN(cache_off_validation_error_preserves_original_error);
	UT_RUN(cache_off_page_cancel_preserves_error_and_backend_reuse);
	UT_RUN(cache_off_refused_local_lock_releases_only_pin);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
