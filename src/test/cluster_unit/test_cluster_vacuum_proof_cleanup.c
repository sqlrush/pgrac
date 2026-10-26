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
 * Acquisition/native unpin/route/proof providers remain controlled leaves.
 * Opt-in cases run original admission snapshot/enter/leave and shared/local
 * counters through the real collector finally. Startup, epoch, OS callbacks
 * and injected ERROR/cancel remain controlled, not native qualification.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_heap_freeze.h"
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_scn.h"
#include "commands/vacuum.h"
#include "cluster/cluster_itl_cleanout.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_space_storage.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/ipc.h"
#include "utils/rel.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
#include "test_cluster_heap_small_scn.inc"

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
	ClusterHeapFreezeCohortObservation shared_freeze_cohort;
	FullTransactionId shared_freeze_candidate_limit_full;
	ClusterHeapFreezeXminCandidateBatchV1 shared_freeze_xmin_candidates;
	ClusterHeapFreezeAbortedXmaxCandidateBatchV1 shared_freeze_aborted_xmax_candidates;
	bool shared_freeze_cohort_observed;
	uint64 shared_freeze_proof_calls;
	uint64 shared_freeze_proof_requests;
	uint64 shared_freeze_proof_exchanges;
	bool shared_freeze_proof_cost_incomplete;
	struct VacuumCutoffs cutoffs;
	bool shared_freeze_fence_valid;
} LVRelState;

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
volatile uint32 CritSectionCount;

static RelationData relation;
static LVRelState vacrel;
static unsigned enters, leaves, debt, resolves, routes, target_bindings;
static bool fixture_original_admission_enabled;
static unsigned fixture_original_admission_debt(void);
static void fixture_original_admission_setup(void);
static void fixture_original_admission_event(char event);
static unsigned aborted_target_bindings;
static unsigned aborted_candidate_selections;
static bool refuse_aborted_candidates;
static unsigned writer_resumes, writer_releases;
static bool writer_scope_active;
static bool writer_resume_allowed, writer_resume_error;
static unsigned fail_resolve;
static PGAlignedBlock resident_page;
static unsigned repins, lock_attempts, page_checks, validations;
static unsigned generic_unlocks, remote_releases, ownership_mirrors;
static bool pin_held, content_locked, resident, lock_available;

/* Original backend-private pin reader; only its refcount storage is a leaf. */
static int32
GetPrivateRefCount(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	return pin_held ? 1 : 0;
}
#include "test_cluster_heap_borrowed_pin.inc"

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
static unsigned hint_begins, hint_finishes;
static ClusterSpaceHintResult hint_result;
static bool error_hint_begin, mismatch_requested_locator;
static ClusterTxOutcome slot_outcomes[CLUSTER_ITL_INITRANS_DEFAULT];
static SCN slot_scns[CLUSTER_ITL_INITRANS_DEFAULT];
static bool slot_requires_retained_scn[CLUSTER_ITL_INITRANS_DEFAULT];
static SCN retained_scns[CLUSTER_ITL_INITRANS_DEFAULT];
static unsigned retained_resolves;
static unsigned retained_shape_fault;
static ClusterHeapFreezeCohortObservation cohort_value;
static ClusterHeapFreezeCohortStatus cohort_status;
static unsigned cohort_calls, bindings_at_cohort;
static bool error_cohort, revoke_cohort;
static bool policy_sample_current, revoke_on_policy_check;
static unsigned policy_checks;
static unsigned final_policy_checks, final_cohort_checks, final_cohort_fence_checks;
static unsigned target_source_change;
static bool after_target_recheck, error_final_policy;
static bool cohort_fence_current, revoke_on_cohort_fence_check, error_cohort_fence;
static unsigned cohort_fence_checks, policy_checks_at_cohort_fence;
static unsigned cohort_receipt_checks;
static ClusterHeapFreezeCohortStatus cohort_receipt_status;
static bool revoke_on_receipt_check;
static unsigned candidate_selections;
static bool use_candidate_value;
static ClusterHeapFreezeXminCandidateBatchV1 candidate_value;
static bool select_candidates;
static FullTransactionId candidate_limit_seen;
static SCN candidate_horizon_seen;
static SCN fixture_clock, clock_horizon_seen;
static unsigned clock_reads, clock_observations;
static bool refuse_clock_observe, error_clock_observe;
static bool revoke_on_clock_observe, stale_policy_on_clock_observe;
static bool stale_cohort_on_clock_observe;
static ClusterGcsUndoHeaderPathCounters transport_header;
static ClusterGcsUndoVerdictPathCounters transport_verdict;
static unsigned header_snapshots, verdict_snapshots;
static bool charge_transport, reset_transport_during_resolve, overflow_transport_sum;
static bool target_admission_current, error_target_recheck, revoke_on_target_recheck;
static unsigned target_admission_checks;

/* Backend-local transport snapshots are leaves; no reset is requested by
 * the real VACUUM owner. Counter changes below model already paid work. */
void
cluster_gcs_block_undo_header_counters_snapshot(ClusterGcsUndoHeaderPathCounters *out)
{
	if (guard_held || pin_held || content_locked || (debt != 0 && debt != 1)) abort();
	header_snapshots++;
	*out = transport_header;
}

void
cluster_gcs_block_undo_verdict_counters_snapshot(ClusterGcsUndoVerdictPathCounters *out)
{
	if (guard_held || pin_held || content_locked || (debt != 0 && debt != 1)) abort();
	verdict_snapshots++;
	*out = transport_verdict;
}

static void
fixture_charge_transport(void)
{
	ClusterGcsUndoVerdictExchangeCounters *paths[] = {
		&transport_verdict.owner_verdict, &transport_verdict.freshref_c1b_pair,
		&transport_verdict.authority_verdict, &transport_verdict.r4_tx_resolve
	};

	if (!charge_transport) return;
	if (reset_transport_during_resolve || overflow_transport_sum) {
		memset(&transport_header, 0, sizeof(transport_header));
		memset(&transport_verdict, 0, sizeof(transport_verdict));
	}
	if (overflow_transport_sum) {
		transport_header.calls = UINT64_MAX;
		transport_verdict.owner_verdict.calls = 1;
		return;
	}
	transport_header.calls += 2;
	transport_header.request_enqueues++;
	transport_header.sync_exchanges++;
	transport_header.matched_replies = UINT64_MAX; /* never folded as requests */
	for (unsigned i = 0; i < lengthof(paths); i++) {
		paths[i]->calls += i + 3;
		paths[i]->request_enqueues += i + 2;
		paths[i]->sync_exchanges += i + 2;
		paths[i]->verified_replies = UINT64_MAX; /* not terminal proof */
	}
}

static void
note_cleanup(char event)
{
	if (cleanup_count + 1 >= sizeof(cleanup_order))
		abort();
	cleanup_order[cleanup_count++] = event;
	cleanup_order[cleanup_count] = '\0';
	if (fixture_original_admission_enabled)
		fixture_original_admission_event(event);
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

/* Diagnostic formatting leaf; errfinish still propagates the original ERROR. */
int
errhint(const char *format, ...)
{
	(void) format;
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
	if (fixture_original_admission_enabled)
		UT_ASSERT_EQ(fixture_original_admission_debt(), 1);
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

/* Original admission state and functions are opt-in for the owner bridge.
 * Publication, descriptor registration, epoch/Startup and OS exit remain leaves. */
#include "test_cluster_active_sample_types.inc"
static ClusterSemanticActivationShmem fixture_admission_shared;
static ClusterSemanticActivationShmem *SemanticActivationShmem;
static uint32 semantic_activation_local_inflight[2][64];
static int semantic_activation_exit_hook_pid;
static const void *fixture_admission_descriptors[64];
static int fixture_admission_pid, fixture_admission_holdoff;
static uint64 fixture_admission_epoch;
static bool fixture_admission_serving, fixture_admission_bootstrap;
static unsigned fixture_admission_registrations, fixture_admission_foreign_debt;
static pg_on_exit_callback fixture_admission_exit_callback;
static Datum fixture_admission_exit_arg;
static const ClusterSemanticAdmissionToken *fixture_admission_token;
static ClusterSemanticAdmissionToken fixture_admission_last_token;
static bool fixture_actual_target_current(const ClusterSemanticAdmissionToken *);
static bool fixture_actual_census_current(const ClusterSemanticAdmissionToken *);
static bool fixture_actual_backend_has_admission(void);
static ClusterSemanticAdmissionResult fixture_actual_census_enter(ClusterSemanticAdmissionToken *);
static void fixture_actual_census_leave(ClusterSemanticAdmissionToken *);

static bool
fixture_admission_is_serving(void)
{
	return fixture_admission_serving && !reject_admission;
}
static bool
fixture_admission_is_bootstrap(void)
{
	return fixture_admission_bootstrap;
}
static uint64
fixture_admission_current_epoch(void)
{
	return fixture_admission_epoch;
}
static void
fixture_admission_register_exit(pg_on_exit_callback callback, Datum arg)
{
	UT_ASSERT(fixture_admission_exit_callback == NULL && callback != NULL);
	UT_ASSERT_EQ(fixture_admission_holdoff, 0);
	fixture_admission_exit_callback = callback; fixture_admission_exit_arg = arg;
	fixture_admission_registrations++;
}

#define SemanticActivationDescriptors fixture_admission_descriptors
#define normal_start_serving_allowed fixture_admission_is_serving
#define normal_start_bootstrap_allowed fixture_admission_is_bootstrap
#define cluster_epoch_get_current fixture_admission_current_epoch
#define MyProcPid fixture_admission_pid
#define InterruptHoldoffCount fixture_admission_holdoff
#define on_shmem_exit fixture_admission_register_exit
#define cluster_semantic_activation_recheck fixture_actual_target_current
#define cluster_semantic_activation_recheck_r4_terminal_census fixture_actual_census_current
#define cluster_semantic_activation_backend_has_admission fixture_actual_backend_has_admission
#define cluster_semantic_activation_enter_r4_terminal_census fixture_actual_census_enter
#define cluster_semantic_activation_leave fixture_actual_census_leave
#include "test_cluster_vacuum_admission_snapshot.inc"
#include "test_cluster_heap_freeze_admission_current.inc"
#include "test_cluster_heap_freeze_admission_lifecycle.inc"
#undef cluster_semantic_activation_leave
#undef cluster_semantic_activation_enter_r4_terminal_census
#undef cluster_semantic_activation_backend_has_admission
#undef cluster_semantic_activation_recheck_r4_terminal_census
#undef cluster_semantic_activation_recheck
#undef on_shmem_exit
#undef InterruptHoldoffCount
#undef MyProcPid
#undef cluster_epoch_get_current
#undef normal_start_bootstrap_allowed
#undef normal_start_serving_allowed
#undef SemanticActivationDescriptors

static int
fixture_admission_feature(void)
{
	int feature = -1;
	UT_ASSERT(semantic_activation_feature_bit_index(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1, &feature));
	return feature;
}

static unsigned
fixture_original_admission_debt(void)
{
	uint32 shared = pg_atomic_read_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()]);
	UT_ASSERT(shared >= fixture_admission_foreign_debt);
	return shared - fixture_admission_foreign_debt;
}

/* Observe the original counters at each actual resource cleanup boundary. */
static void
fixture_original_admission_event(char event)
{
	bool entered = event != 'A';

	UT_ASSERT_EQ(fixture_original_admission_debt(), entered ? 1 : 0);
	UT_ASSERT_EQ(semantic_activation_local_inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], entered ? 1 : 0);
	UT_ASSERT_EQ(fixture_actual_backend_has_admission(), entered);
	UT_ASSERT_EQ(fixture_admission_holdoff, 0);
	if (entered) {
		UT_ASSERT(fixture_admission_token != NULL && fixture_admission_token->entered);
	} else {
		UT_ASSERT(fixture_admission_token == NULL);
		UT_ASSERT(!guard_held && !pin_held && !content_locked && !writer_scope_active);
		UT_ASSERT(exit_callback == NULL);
	}
}

static void
fixture_original_admission_setup(void)
{
	UT_ASSERT(!fixture_actual_backend_has_admission()); UT_ASSERT_EQ(fixture_admission_holdoff, 0);
	memset(&fixture_admission_shared, 0, sizeof(fixture_admission_shared));
	SemanticActivationShmem = &fixture_admission_shared;
	pg_atomic_init_u64(&fixture_admission_shared.admission_seq, 2);
	pg_atomic_init_u64(&fixture_admission_shared.active_bits, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	pg_atomic_init_u64(&fixture_admission_shared.record_generation, 17);
	pg_atomic_init_u64(&fixture_admission_shared.formation_epoch, 9);
	pg_atomic_init_u32(&fixture_admission_shared.transition_closed, 0);
	for (unsigned side = 0; side < 2; side++)
		for (unsigned bit = 0; bit < 64; bit++)
			pg_atomic_init_u32(&fixture_admission_shared.inflight[side][bit], 0);
	memset(fixture_admission_descriptors, 0, sizeof(fixture_admission_descriptors));
	fixture_admission_descriptors[fixture_admission_feature()] = &fixture_admission_shared;
	fixture_admission_pid = 4401; fixture_admission_epoch = 9;
	fixture_admission_serving = true; fixture_admission_bootstrap = false;
	fixture_admission_foreign_debt = fixture_admission_registrations = 0;
	fixture_admission_exit_callback = NULL; fixture_admission_exit_arg = (Datum)0;
	fixture_admission_token = NULL; memset(&fixture_admission_last_token, 0, sizeof(fixture_admission_last_token));
	semantic_activation_exit_hook_pid = 0;
	fixture_original_admission_enabled = true;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken *token)
{
	enters++;
	if (fixture_original_admission_enabled) {
		ClusterSemanticAdmissionResult result = fixture_actual_census_enter(token);
		fixture_admission_last_token = *token;
		if (result == CLUSTER_SEMANTIC_ADMISSION_OK) {
			UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(fixture_original_admission_debt(), 1);
			UT_ASSERT(token->entered); fixture_admission_token = token; debt++;
		}
		return result;
	}
	if (reject_admission)
		return CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	if (debt != 0)
		abort();
	token->record_generation = 17;
	token->formation_epoch = 9;
	token->side = CLUSTER_SEMANTIC_TARGET_SIDE;
	token->entered = true;
	debt++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (fixture_original_admission_enabled)
		UT_ASSERT_EQ(fixture_original_admission_debt(), 1);
	if (guard_held || !token->entered || token->record_generation != 17 || debt != 1)
		abort();
	/* Record a violated ordering so the exact-parent RED can report it. */
	leave_with_ownership |= pin_held || content_locked || writer_scope_active;
	leaves++;
	if (fixture_original_admission_enabled) {
		ClusterSemanticAdmissionToken zero = {0};
		UT_ASSERT(token == fixture_admission_token);
		fixture_actual_census_leave(token); fixture_admission_last_token = *token;
		UT_ASSERT(memcmp(token, &zero, sizeof(zero)) == 0);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		fixture_admission_token = NULL;
	} else token->entered = false;
	debt--;
	note_cleanup('A');
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	return token->entered && debt == 1 && !changed_admission
		&& (!fixture_original_admission_enabled || fixture_actual_census_current(token));
}

/* Full TARGET readiness is distinct from terminal-census read admission. */
bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	if (guard_held || pin_held || content_locked || debt != 1 || target_bindings != 0
		|| clock_reads == 0 || cohort_receipt_checks == 0)
		abort();
	target_admission_checks++;
	after_target_recheck = true;
	switch (target_source_change) {
	case 1: policy_sample_current = false; break;
	case 2: cohort_fence_current = false; break;
	case 3: revoke_on_policy_check = true; break;
	case 4: revoke_on_cohort_fence_check = true; break;
	case 5: error_final_policy = true; break;
	case 6: error_cohort_fence = true; break;
	}
	if (error_target_recheck) throw_error();
	if (revoke_on_target_recheck) changed_admission = true;
	return token->entered && token->side == CLUSTER_SEMANTIC_TARGET_SIDE
		&& target_admission_current && !changed_admission
		&& (!fixture_original_admission_enabled || fixture_actual_target_current(token));
}

void
cluster_tx_resolve_terminal_census_batch_preflight(void)
{
	if (error_preflight)
		throw_error();
}

static ClusterTxOutcome
fixture_resolve_admitted(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
						const ClusterSemanticAdmissionToken *admission,
						ClusterTxResolution *out, ClusterTxResolveReason *reason,
						SCN retained_scn)
{
	ClusterUndoBlock0CurrentGuard guard;
	ClusterRuntimeCandidateCleanup cleanup;

	if (mode != CLUSTER_TX_RESOLVE_TERMINAL_CENSUS || !admission->entered || debt != 1)
		abort();
	resolves++;
	fixture_charge_transport();
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
	out->locator_echo = *locator;
	out->outcome = slot_outcomes[locator->itl_slot_index];
	out->commit_scn = slot_scns[locator->itl_slot_index];
	if (slot_requires_retained_scn[locator->itl_slot_index]
		&& retained_scn != slot_scns[locator->itl_slot_index])
	{
		*reason = CLUSTER_TX_RESOLVE_AUTHORITY_UNAVAILABLE;
		return CLUSTER_TX_UNKNOWN;
	}
	out->top_xid = locator->xid;
	out->proof_kind = CLUSTER_TX_PROOF_ORIGIN_DURABLE_TT_CLOG;
	out->authority.origin_epoch = admission->formation_epoch;
	out->authority.live_hwm_lsn = 123;
	out->authority.authority_scn = 321;
	/* The real retained fallback has no canonical TT metadata or wrap.
	 * Ordinary exact success supplies those separately. */
	if (!slot_requires_retained_scn[locator->itl_slot_index])
	{
		out->local_canonical_valid = true;
		out->locator_echo.tt_wrap = 4 + locator->itl_slot_index;
	}
	else if (locator->itl_slot_index == 1)
	{
		if (retained_shape_fault == 1) out->locator_echo.xid++;
		if (retained_shape_fault == 2) out->top_xid++;
		if (retained_shape_fault == 3) out->proof_kind = CLUSTER_TX_PROOF_NONE;
		if (retained_shape_fault == 4) out->authority.origin_epoch++;
		if (retained_shape_fault == 5) out->authority.live_hwm_lsn = InvalidXLogRecPtr;
		if (retained_shape_fault == 6) out->commit_scn++;
		if (retained_shape_fault == 7) out->locator_echo.tt_wrap = 4;
		if (retained_shape_fault == 8) out->locator_echo.uba.raw[1]++;
	}
	*reason = unknown_outcome ? CLUSTER_TX_RESOLVE_PROTOCOL : CLUSTER_TX_RESOLVE_NONE;
	return unknown_outcome ? CLUSTER_TX_UNKNOWN : out->outcome;
}

ClusterTxOutcome
cluster_tx_resolve_exact_admitted(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
								const ClusterSemanticAdmissionToken *admission,
								ClusterTxResolution *out, ClusterTxResolveReason *reason)
{
	return fixture_resolve_admitted(locator, mode, admission, out, reason, InvalidScn);
}

ClusterTxOutcome
cluster_tx_resolve_terminal_census_retained_admitted(const ClusterTxLocator *locator,
	SCN retained_scn, const ClusterSemanticAdmissionToken *admission,
	ClusterTxResolution *out, ClusterTxResolveReason *reason)
{
	retained_resolves++;
	retained_scns[locator->itl_slot_index] = retained_scn;
	return fixture_resolve_admitted(locator, CLUSTER_TX_RESOLVE_TERMINAL_CENSUS,
		admission, out, reason, retained_scn);
}

bool
cluster_heap_freeze_build_canonical_proof_v1(
	const ClusterTxLocator *locator, const ClusterTxResolution *resolution,
	const ClusterHeapFreezeFinalFenceV1 *expected,
	const ClusterHeapFreezeFinalFenceV1 *current, bool finalized,
	bool authority_current, bool admission_current, ClusterHeapFreezeCanonicalProofV1 *out)
{
	memset(out, 0, sizeof(*out));
	out->requested_locator = *locator;
	out->resolution = *resolution;
	out->c1b_finalized = finalized;
	if (mismatch_requested_locator && locator->itl_slot_index == 1)
		out->requested_locator.tt_wrap = 3;
	out->expected_fence = *expected;
	out->current_fence = *current;
	out->authority_current = authority_current;
	out->admission_current = admission_current;
	return true;
}

static ClusterHeapFreezeProofStatusV1
validate_fixture_proofs(
	const ClusterHeapFreezePageBatchV1 *page, const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence, bool require_all)
{
	validations++;
	validation_under_lock = pin_held && content_locked;
	if (error_validate)
		throw_error();
	if (proofs->proof_count == 0)
		return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	/* Match the actual canonical validator's two reviewed requirements.
	 * Its full unchanged contract is exercised by shared-MX's real object. */
	for (unsigned index = 0; index < proofs->proof_count; index++)
		if (!proofs->proofs[index].resolution.local_canonical_valid
			|| proofs->proofs[index].resolution.locator_echo.tt_wrap == TT_WRAP_INVALID)
			return CLUSTER_HEAP_FREEZE_PROOF_HARD_ERROR;
	if (!proofs->proofs[0].authority_current || !proofs->proofs[0].admission_current
		|| proofs->proofs[0].expected_fence.page_lsn != fence->page_lsn
		|| proofs->proofs[0].expected_fence.page_version != fence->page_version)
		return CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED;
	if (require_all && proofs->proof_count != page->carrier_request_count)
		return CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	return CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
}

ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_conditional_apply_v1(
	const ClusterHeapFreezePageBatchV1 *page, const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence)
{
	return validate_fixture_proofs(page, proofs, fence, true);
}

ClusterHeapFreezeProofStatusV1
cluster_heap_freeze_validate_carrier_cleanout_v1(
	const ClusterHeapFreezePageBatchV1 *page, const ClusterHeapFreezeProofBatchV1 *proofs,
	const ClusterHeapFreezeFinalFenceV1 *fence)
{
	return validate_fixture_proofs(page, proofs, fence, false);
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
ClusterConditionalLockBufferWithMutationContext(Buffer buffer,
	const struct ResourceXWriterUseContext *expected, uint64 object_revision, bool object_checked)
{
	UT_ASSERT(expected == &vacrel.shared_freeze_input.writer_context);
	UT_ASSERT_EQ(object_revision, vacrel.shared_freeze_input.writer_object_revision);
	UT_ASSERT_EQ(object_checked, vacrel.shared_freeze_input.writer_object_checked);
	writer_resumes++;
	if (writer_resume_error)
		ereport(ERROR, (errcode(inject_code), errmsg("pre-lock writer resume fixture")));
	if (!writer_resume_allowed)
		return false;
	if (!ConditionalLockBuffer(buffer)) return false;
	writer_scope_active = true;
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
		|| !pin_held || !content_locked || debt != 1 || writer_scope_active)
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

/* The paired release body is real; its backend-local ledger is a leaf here
 * and independently exercised with real ledger bodies in the writer fixture. */
enum { PCM_X_WRITER_LEDGER_ACTIVE = 3 };
typedef struct { int phase; } ClusterPcmXWriterLedgerEntry;
static ClusterPcmXWriterLedgerEntry fixture_writer_entry = { PCM_X_WRITER_LEDGER_ACTIVE };
static ClusterPcmXWriterLedgerEntry *
cluster_bufmgr_pcm_x_writer_find(BufferDesc *buf)
{
	UT_ASSERT(buf == GetBufferDescriptor(0));
	return writer_scope_active ? &fixture_writer_entry : NULL;
}
static void
cluster_bufmgr_pcm_x_writer_clear(ClusterPcmXWriterLedgerEntry *entry)
{
	UT_ASSERT(entry == &fixture_writer_entry && writer_scope_active);
	writer_scope_active = false; writer_releases++; note_cleanup('W');
}
bool
LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == BufferDescriptorGetContentLock(GetBufferDescriptor(0)));
	UT_ASSERT_EQ(mode, LW_EXCLUSIVE); return content_locked;
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

/* SPACE/WAL are leaves: assert the caller's real ownership and balanced critical
 * section.  No transport, storage durability or physical FPI is claimed. */
ClusterSpaceHintResult
cluster_space_hint_begin(Buffer buffer, RfPageProducerBatchV1 *batch)
{
	if (buffer != 1 || !pin_held || !content_locked || debt != 1 || CritSectionCount != 0)
		abort();
	hint_begins++;
	if (error_hint_begin)
		throw_error();
	if (hint_result == CLUSTER_SPACE_HINT_VERSIONED)
	{
		memset(batch, 0, sizeof(*batch));
		batch->prepared = batch->stamped = true;
		batch->entry_count = 1;
		CritSectionCount++;
		((PageHeader)resident_page.data)->pd_block_scn++;
	}
	return hint_result;
}

void
cluster_space_hint_finish(Buffer buffer, bool standard, const RfPageProducerBatchV1 *batch)
{
	if (buffer != 1 || !standard || !pin_held || !content_locked || debt != 1
		|| CritSectionCount != 1 || !batch->prepared || !batch->stamped || batch->entry_count != 1)
		abort();
	hint_finishes++;
	PageSetLSN((Page)resident_page.data, 102);
	CritSectionCount--;
}

#include "test_cluster_vacuum_proof_cleanout.inc"

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


uint16
cluster_heap_freeze_bind_creation_targets_v1(ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs, const ClusterHeapFreezeFinalFenceV1 *fence)
{
	if (pin_held || content_locked || debt != 1 || page != &vacrel.shared_freeze_batch
		|| proofs == NULL || fence == NULL)
		abort();
	target_bindings++;
	return 0;
}

uint16
cluster_heap_freeze_bind_aborted_xmax_targets_v1(ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs, const ClusterHeapFreezeFinalFenceV1 *fence)
{
	if (guard_held || pin_held || content_locked || debt != 1 || target_bindings == 0
		|| page != &vacrel.shared_freeze_batch || proofs == NULL || fence == NULL)
		abort();
	aborted_target_bindings++;
	page->tuples[0].aborted_xmax_target_proof_index = 0;
	return 1;
}

bool
cluster_heap_freeze_select_aborted_xmax_candidates_v1(const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs, const ClusterHeapFreezeFinalFenceV1 *fence,
	ClusterHeapFreezeAbortedXmaxCandidateBatchV1 *out)
{
	if (guard_held || pin_held || content_locked || debt != 1 || aborted_target_bindings == 0
		|| page != &vacrel.shared_freeze_batch || proofs == NULL || fence == NULL)
		abort();
	aborted_candidate_selections++;
	if (refuse_aborted_candidates) return false;
	memset(out, 0, sizeof(*out));
	out->target_count = 1;
	return true;
}

bool
cluster_heap_freeze_select_xmin_candidates_v1(const ClusterHeapFreezePageBatchV1 *page,
	const ClusterHeapFreezeProofBatchV1 *proofs, const ClusterHeapFreezeFinalFenceV1 *fence,
	FullTransactionId limit, SCN horizon, ClusterHeapFreezeXminCandidateBatchV1 *out)
{
	if (guard_held || pin_held || content_locked || debt != 1
		|| target_bindings != 1 || page != &vacrel.shared_freeze_batch
		|| proofs == NULL || fence == NULL || !vacrel.shared_freeze_cohort_observed)
		abort();
	candidate_selections++;
	candidate_limit_seen = limit;
	candidate_horizon_seen = horizon;
	if (!select_candidates) return false;
	memset(out, 0, sizeof(*out));
	out->target_count = 1;
	out->targets[0].tuple_index = 0;
	out->targets[0].original_creator_full_xid = FullTransactionIdFromEpochAndXid(1, 77);
	if (use_candidate_value) *out = candidate_value;
	return true;
}

/* Clock and injected callbacks are controlled leaves. The real collector
 * must have released page/proof ownership while retaining its admission. */
SCN
cluster_scn_current(void)
{
	if (guard_held || pin_held || content_locked || debt != 1
		|| target_bindings != 0 || policy_checks == 0 || cohort_receipt_checks == 0)
		abort();
	clock_reads++;
	return fixture_clock;
}

void
cluster_scn_observe(SCN horizon)
{
	if (guard_held || pin_held || content_locked || debt != 1
		|| target_bindings != 0 || clock_reads == 0)
		abort();
	clock_observations++;
	clock_horizon_seen = horizon;
	if (revoke_on_clock_observe) changed_admission = true;
	if (stale_policy_on_clock_observe) policy_sample_current = false;
	if (stale_cohort_on_clock_observe)
		cohort_receipt_status = CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
	if (error_clock_observe) throw_error();
	if (!refuse_clock_observe && scn_local(horizon) < SCN_MAX_LOCAL)
		fixture_clock = scn_encode(0, scn_local(horizon) + 1);
}

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_observe(ClusterHeapFreezeCohortObservation *out)
{
	if (guard_held || pin_held || content_locked || debt != 1)
		abort();
	after_target_recheck = false;
	cohort_calls++;
	bindings_at_cohort = target_bindings;
	if (revoke_cohort)
		changed_admission = true;
	if (error_cohort)
		throw_error();
	if (cohort_status == CLUSTER_HEAP_FREEZE_COHORT_READY)
		*out = cohort_value;
	return cohort_status;
}

bool
cluster_heap_freeze_policy_sample_current_v1(const ClusterHeapFreezePolicySampleV1 *sample)
{
	if (guard_held || pin_held || content_locked || debt != 1)
		abort();
	if (after_target_recheck) final_policy_checks++;
	else policy_checks++;
	if (after_target_recheck && error_final_policy) throw_error();
	UT_ASSERT_EQ(sample->allocation_fence.incarnation, 81);
	if (revoke_on_policy_check)
		changed_admission = true;
	return policy_sample_current;
}

bool
cluster_semantic_activation_active_sample_current(const ClusterSemanticActiveSampleFence *expected)
{
	if (guard_held || pin_held || content_locked || debt != 1)
		abort();
	if (after_target_recheck) final_cohort_fence_checks++;
	else cohort_fence_checks++;
	UT_ASSERT(policy_checks + final_policy_checks > policy_checks_at_cohort_fence);
	policy_checks_at_cohort_fence = policy_checks + final_policy_checks;
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT_EQ(memcmp(expected, &cohort_value.admission_fence, sizeof(*expected)), 0);
	if (revoke_on_cohort_fence_check)
		changed_admission = true;
	if (error_cohort_fence)
		throw_error();
	return cohort_fence_current;
}

ClusterHeapFreezeCohortStatus
cluster_heap_freeze_cohort_current(const ClusterHeapFreezeCohortObservation *expected)
{
	if (guard_held || pin_held || content_locked || debt != 1) abort();
	if (after_target_recheck) final_cohort_checks++;
	else cohort_receipt_checks++;
	UT_ASSERT_EQ(memcmp(expected, &cohort_value, sizeof(*expected)), 0);
	UT_ASSERT(policy_checks > 0);
	UT_ASSERT_EQ(target_bindings, 0);
	if (revoke_on_receipt_check) changed_admission = true;
	if (cohort_receipt_status != CLUSTER_HEAP_FREEZE_COHORT_READY)
		return cohort_receipt_status;
	return cluster_semantic_activation_active_sample_current(&expected->admission_fence)
		? CLUSTER_HEAP_FREEZE_COHORT_READY : CLUSTER_HEAP_FREEZE_COHORT_IDENTITY_CHANGED;
}

#include "test_cluster_vacuum_observed_bounds.inc"

#include "test_cluster_vacuum_proof_cleanup.inc"

static void
reset_fixture(void)
{
	fixture_original_admission_enabled = false;
	writer_resumes = writer_releases = 0;
	writer_scope_active = false;
	writer_resume_allowed = true;
	writer_resume_error = false;
	memset(&relation, 0, sizeof(relation));
	memset(&vacrel, 0, sizeof(vacrel));
	memset(&transport_header, 0, sizeof(transport_header));
	memset(&transport_verdict, 0, sizeof(transport_verdict));
	header_snapshots = verdict_snapshots = 0;
	target_admission_current = true;
	final_policy_checks = final_cohort_checks = final_cohort_fence_checks = 0;
	target_source_change = 0;
	after_target_recheck = error_final_policy = false;
	error_target_recheck = revoke_on_target_recheck = false;
	target_admission_checks = 0;
	charge_transport = reset_transport_during_resolve = overflow_transport_sum = false;
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
	vacrel.shared_freeze_batch.tuple_count = 1;
	vacrel.shared_freeze_batch.tuples[0].creation_target_proof_index = 0;
	vacrel.shared_freeze_batch.tuples[0].aborted_xmax_target_proof_index = 3;
	aborted_target_bindings = 0;
	aborted_candidate_selections = 0;
	refuse_aborted_candidates = false;
	target_bindings = 0;
	memset(&cohort_value, 0, sizeof(cohort_value));
	cohort_value.future_full_xid_max = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 1000));
	cohort_value.requested_full_xid_max = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 1100));
	cohort_value.active_full_xid_min = U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 60));
	cohort_value.local_registration_cut.status = CTRC_ACTIVE_SCAN_COMPLETE;
	cohort_value.local_registration_cut.min_full = FullTransactionIdFromU64(cohort_value.active_full_xid_min);
	cohort_value.local_registration_cut.future_full = FullTransactionIdFromU64(cohort_value.future_full_xid_max);
	cohort_value.source_peer_active_full_xid_min[1] = cohort_value.active_full_xid_min;
	cohort_value.source_peer_future_full_xid[1] = cohort_value.future_full_xid_max;
	cohort_value.horizon_scn = 500;
	cohort_value.source_local_horizon_scn = 600;
	cohort_value.source_peer_horizon_scn[1] = 500;
	cohort_value.source_local_history.sample_clock_scn = 700;
	cohort_value.source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
	cohort_value.source_local_history.contributor_token = 11;
	cohort_value.source_peer_history[1].sample_clock_scn = 800;
	cohort_value.source_peer_history[1].contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED;
	cohort_value.source_peer_history[1].contributor_token = 17;
	cohort_value.record_generation = 17;
	cohort_value.formation_epoch = 9;
	cohort_value.admitted_members_lo = 3;
	cohort_value.admission_fence.system_identifier = 9101;
	cohort_value.admission_fence.admission_publication_seq = 9102;
	cohort_value.admission_fence.active_bits = CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1;
	cohort_value.admission_fence.record_generation = cohort_value.record_generation;
	cohort_value.admission_fence.formation_epoch = cohort_value.formation_epoch;
	cohort_value.admission_fence.membership_cut_generation = 9103;
	cohort_value.admission_fence.local_boot_incarnation = 81;
	cohort_value.admission_fence.admitted_members_lo = cohort_value.admitted_members_lo;
	cohort_value.admission_fence.admitted_incarnation[0] = 81;
	cohort_value.admission_fence.admitted_incarnation[1] = 82;
	cohort_value.admission_fence.local_node_id = 0;
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	cohort_calls = bindings_at_cohort = 0;
	fixture_clock = scn_encode(0, 1000);
	clock_horizon_seen = InvalidScn;
	clock_reads = clock_observations = 0;
	refuse_clock_observe = error_clock_observe = false;
	revoke_on_clock_observe = stale_policy_on_clock_observe = false;
	stale_cohort_on_clock_observe = false;
	error_cohort = revoke_cohort = false;
	policy_sample_current = true;
	revoke_on_policy_check = false;
	policy_checks = 0;
	cohort_fence_current = true;
	revoke_on_cohort_fence_check = error_cohort_fence = false;
	cohort_fence_checks = policy_checks_at_cohort_fence = 0;
	cohort_receipt_checks = 0;
	cohort_receipt_status = CLUSTER_HEAP_FREEZE_COHORT_READY;
	revoke_on_receipt_check = false;
	candidate_selections = 0;
	use_candidate_value = false;
	memset(&candidate_value, 0, sizeof(candidate_value));
	select_candidates = true;
	candidate_limit_seen = InvalidFullTransactionId;
	candidate_horizon_seen = InvalidScn;
	enters = leaves = debt = resolves = routes = fail_resolve = 0;
	reject_admission = reject_route_after = unknown_outcome = false;
	error_preflight = error_validate = guard_held = false;
	inject_code = ERRCODE_INTERNAL_ERROR;
	caught_code = 0;
	collected_status = CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN;
	cleanup_count = 0;
	cleanup_order[0] = '\0';
	exit_callback = NULL;
	hint_begins = hint_finishes = retained_resolves = retained_shape_fault = 0;
	hint_result = CLUSTER_SPACE_HINT_VERSIONED;
	error_hint_begin = mismatch_requested_locator = false;
	CritSectionCount = 0;
	for (unsigned index = 0; index < CLUSTER_ITL_INITRANS_DEFAULT; index++)
	{
		slot_outcomes[index] = CLUSTER_TX_COMMITTED;
		slot_scns[index] = 200 + index;
		slot_requires_retained_scn[index] = false;
		retained_scns[index] = InvalidScn;
	}
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

static ClusterItlSlotData *
setup_cleanout_page(void)
{
	PageHeader header;
	ClusterItlSlotData *slots;

	reset_fixture();
	cluster_gcs_block_local_cache = false;
	remote_release_error = true;
	header = (PageHeader)resident_page.data;
	header->pd_lower = SizeOfPageHeaderData;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_upper = header->pd_special;
	header->pd_flags |= PD_HAS_ITL;
	memset(resident_page.data + header->pd_lower, 0xa5, header->pd_upper - header->pd_lower);
	slots = ClusterPageGetItlSlots((Page)resident_page.data);
	vacrel.shared_freeze_batch.carrier_request_count = 2;
	for (uint8 index = 0; index < 2; index++)
	{
		ClusterHeapFreezeCarrierRequestV1 *request =
			&vacrel.shared_freeze_batch.carrier_requests[index];

		/* Same raw XID, different physical carriers and different proven SCNs. */
		slots[index].xid = 47;
		slots[index].wrap = 4 + index;
		slots[index].flags = ITL_FLAG_ACTIVE;
		slots[index].lock_count = 2 + index;
		slots[index].undo_segment_head.raw[0] = 100 + index;
		slots[index].undo_segment_head.raw[1] = 900 + index;
		slots[index].write_scn = 80 + index;
		slots[index].first_change_lsn = 70 + index;
		request->carrier = slots[index];
		request->locator.uba = slots[index].undo_segment_head;
		request->locator.xid = slots[index].xid;
		request->locator.tt_wrap = TT_WRAP_INVALID;
		request->locator.itl_kind = slots[index].flags;
		request->locator.itl_slot_index = index;
	}
	return slots;
}

static void
expected_cleanout(PGAlignedBlock *expected, bool second)
{
	ClusterItlSlotData *slots;

	memcpy(expected, &resident_page, sizeof(*expected));
	slots = ClusterPageGetItlSlots((Page)expected->data);
	slots[0].flags = ITL_FLAG_COMMITTED;
	slots[0].commit_scn = slot_scns[0];
	if (second)
	{
		slots[1].flags = ITL_FLAG_COMMITTED;
		slots[1].commit_scn = slot_scns[1];
	}
	((PageHeader)expected->data)->pd_block_scn++;
	PageSetLSN((Page)expected->data, 102);
}

UT_TEST(needs_cleanout_uses_captured_scn_under_original_admission)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slots = setup_cleanout_page();

	vacrel.shared_freeze_batch.carrier_request_count = 1;
	slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[0].commit_scn = slot_scns[0];
	vacrel.shared_freeze_batch.carrier_requests[0].carrier = slots[0];
	vacrel.shared_freeze_batch.carrier_requests[0].locator.itl_kind = slots[0].flags;
	slot_requires_retained_scn[0] = true;
	expected_cleanout(&expected, false);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(retained_resolves, 1);
	UT_ASSERT_EQ(retained_scns[0], slot_scns[0]);
	UT_ASSERT_EQ(resolves, 1);
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(retained_scn_never_merges_equal_xids_in_different_carriers)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slots = setup_cleanout_page();

	for (unsigned index = 0; index < 2; index++)
	{
		slots[index].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[index].commit_scn = slot_scns[index];
		vacrel.shared_freeze_batch.carrier_requests[index].carrier = slots[index];
		vacrel.shared_freeze_batch.carrier_requests[index].locator.itl_kind = slots[index].flags;
		slot_requires_retained_scn[index] = true;
	}
	expected_cleanout(&expected, true);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(retained_resolves, 2);
	UT_ASSERT_EQ(retained_scns[0], slot_scns[0]);
	UT_ASSERT_EQ(retained_scns[1], slot_scns[1]);
	UT_ASSERT(retained_scns[0] != retained_scns[1]);
	UT_ASSERT_EQ(resolves, 2);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(unrelated_or_incomplete_carriers_keep_original_exact_route)
{
	for (unsigned variant = 0; variant < 5; variant++)
	{
		ClusterItlSlotData *slots = setup_cleanout_page();
		ClusterHeapFreezeCarrierRequestV1 *request =
			&vacrel.shared_freeze_batch.carrier_requests[0];

		vacrel.shared_freeze_batch.carrier_request_count = 1;
		slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[0].commit_scn = slot_scns[0];
		request->carrier = slots[0];
		request->locator.itl_kind = slots[0].flags;
		slot_requires_retained_scn[0] = true;
		if (variant == 0) request->carrier.flags = ITL_FLAG_ACTIVE;
		if (variant == 1) request->locator.itl_kind = ITL_FLAG_ACTIVE;
		if (variant == 2) request->carrier.commit_scn = InvalidScn;
		if (variant == 3) request->locator.tt_wrap = 4;
		if (variant == 4)
		{
			request->carrier.flags = ITL_FLAG_COMMITTED;
			request->locator.itl_kind = ITL_FLAG_COMMITTED;
		}
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(retained_resolves, 0);
		UT_ASSERT_EQ(resolves, 1);
		UT_ASSERT_EQ(hint_begins, 0);
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(refused_retained_pair_keeps_other_exact_carrier_without_retrying)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slots = setup_cleanout_page();

	for (unsigned index = 0; index < 2; index++)
	{
		slots[index].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[index].commit_scn = slot_scns[index] + index;
		vacrel.shared_freeze_batch.carrier_requests[index].carrier = slots[index];
		vacrel.shared_freeze_batch.carrier_requests[index].locator.itl_kind = slots[index].flags;
		slot_requires_retained_scn[index] = true;
	}
	expected_cleanout(&expected, false);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(retained_resolves, 2);
	UT_ASSERT_EQ(resolves, 2);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(retained_pair_error_and_cancel_unwind_guard_before_admission)
{
	const int codes[] = {ERRCODE_INTERNAL_ERROR, ERRCODE_QUERY_CANCELED};

	for (unsigned index = 0; index < lengthof(codes); index++)
	{
		ClusterItlSlotData *slots = setup_cleanout_page();

		vacrel.shared_freeze_batch.carrier_request_count = 1;
		slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[0].commit_scn = slot_scns[0];
		vacrel.shared_freeze_batch.carrier_requests[0].carrier = slots[0];
		vacrel.shared_freeze_batch.carrier_requests[0].locator.itl_kind = slots[0].flags;
		slot_requires_retained_scn[0] = true;
		fail_resolve = 1;
		inject_code = codes[index];
		check_error_cleanup("GA");
		UT_ASSERT_EQ(retained_resolves, 1);
		UT_ASSERT_EQ(resolves, 1);
		UT_ASSERT_EQ(hint_begins, 0);
	}
}

UT_TEST(retained_pair_still_requires_final_resident_authority)
{
	ClusterItlSlotData *slots = setup_cleanout_page();

	vacrel.shared_freeze_batch.carrier_request_count = 1;
	slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[0].commit_scn = slot_scns[0];
	vacrel.shared_freeze_batch.carrier_requests[0].carrier = slots[0];
	vacrel.shared_freeze_batch.carrier_requests[0].locator.itl_kind = slots[0].flags;
	slot_requires_retained_scn[0] = true;
	changed_holder = true;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(retained_resolves, 1);
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(canonical_and_retained_evidence_share_only_one_hint_write)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slots = setup_cleanout_page();

	for (unsigned index = 0; index < 2; index++)
	{
		slots[index].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[index].commit_scn = slot_scns[index];
		vacrel.shared_freeze_batch.carrier_requests[index].carrier = slots[index];
		vacrel.shared_freeze_batch.carrier_requests[index].locator.itl_kind = slots[index].flags;
	}
	slot_requires_retained_scn[1] = true;
	expected_cleanout(&expected, true);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(retained_resolves, 2);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT(!vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(retained_only_hint_requires_original_lsn_version_and_admission)
{
	for (unsigned variant = 0; variant < 4; variant++)
	{
		ClusterItlSlotData *slots = setup_cleanout_page();

		vacrel.shared_freeze_batch.carrier_request_count = 1;
		slots[0].flags = ITL_FLAG_NEEDS_CLEANOUT;
		slots[0].commit_scn = slot_scns[0];
		vacrel.shared_freeze_batch.carrier_requests[0].carrier = slots[0];
		vacrel.shared_freeze_batch.carrier_requests[0].locator.itl_kind = slots[0].flags;
		slot_requires_retained_scn[0] = true;
		if (variant == 0) PageSetLSN((Page)resident_page.data, 102);
		if (variant == 1) ((PageHeader)resident_page.data)->pd_block_scn++;
		if (variant == 2) changed_admission = true;
		if (variant == 3) changed_master = true;
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(hint_begins, 0);
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(malformed_retained_pair_cannot_poison_other_canonical_carrier)
{
	for (unsigned fault = 1; fault <= 8; fault++)
	{
		PGAlignedBlock expected;
		ClusterItlSlotData *slots = setup_cleanout_page();

		for (unsigned index = 0; index < 2; index++)
		{
			slots[index].flags = ITL_FLAG_NEEDS_CLEANOUT;
			slots[index].commit_scn = slot_scns[index];
			vacrel.shared_freeze_batch.carrier_requests[index].carrier = slots[index];
			vacrel.shared_freeze_batch.carrier_requests[index].locator.itl_kind = slots[index].flags;
		}
		slot_requires_retained_scn[1] = true;
		retained_shape_fault = fault;
		expected_cleanout(&expected, false);
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(hint_finishes, 1);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(committed_carriers_use_one_versioned_hint_and_invalidate_saved_fence)
{
	PGAlignedBlock expected;
	ClusterItlSlotData *slots = setup_cleanout_page();

	slots[1].flags = ITL_FLAG_NEEDS_CLEANOUT;
	slots[1].commit_scn = slot_scns[1];
	vacrel.shared_freeze_batch.carrier_requests[1].carrier = slots[1];
	vacrel.shared_freeze_batch.carrier_requests[1].locator.itl_kind = slots[1].flags;
	expected_cleanout(&expected, true);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 2);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(every_carrier_witness_byte_is_checked_before_first_stamp)
{
	for (unsigned byte = 0; byte < sizeof(ClusterItlSlotData); byte++)
	{
		PGAlignedBlock expected;
		ClusterItlSlotData *slots = setup_cleanout_page();

		((unsigned char *)&slots[1])[byte] ^= 1;
		memcpy(&expected, &resident_page, sizeof(expected));
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(hint_begins, 0);
		UT_ASSERT_EQ(hint_finishes, 0);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(requested_locator_must_match_exactly_before_any_stamp)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	mismatch_requested_locator = true;
	memcpy(&expected, &resident_page, sizeof(expected));
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(aborted_proof_cannot_stamp_a_committed_hint)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[1] = CLUSTER_TX_ABORTED;
	expected_cleanout(&expected, false);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(non_data_or_ineligible_slots_keep_all_original_bytes)
{
	const uint8 flags[] = {ITL_FLAG_FREE, ITL_FLAG_COMMITTED, ITL_FLAG_ABORTED,
		ITL_FLAG_LOCK_ONLY_ACTIVE, ITL_FLAG_LOCK_ONLY_COMMITTED,
		ITL_FLAG_LOCK_ONLY_ABORTED, ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI,
		ITL_FLAG_NEEDS_CLEANOUT};

	for (unsigned index = 0; index < lengthof(flags); index++)
	{
		PGAlignedBlock expected;
		ClusterItlSlotData *slots = setup_cleanout_page();

		slots[1].flags = flags[index];
		/* NEEDS_CLEANOUT with a different retained SCN must also be skipped. */
		slots[1].commit_scn = 177;
		vacrel.shared_freeze_batch.carrier_requests[1].carrier = slots[1];
		vacrel.shared_freeze_batch.carrier_requests[1].locator.itl_kind = slots[1].flags;
		expected_cleanout(&expected, false);
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(hint_begins, 1);
		UT_ASSERT_EQ(hint_finishes, 1);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(no_eligible_carrier_does_not_start_hint_publication)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[0] = slot_outcomes[1] = CLUSTER_TX_ABORTED;
	memcpy(&expected, &resident_page, sizeof(expected));
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(hint_finishes, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(shared_cleanout_has_no_native_or_cache_miss_fallback)
{
	const ClusterSpaceHintResult refused[] = {CLUSTER_SPACE_HINT_SKIPPED, CLUSTER_SPACE_HINT_NATIVE};

	for (unsigned index = 0; index < lengthof(refused); index++)
	{
		PGAlignedBlock expected;

		setup_cleanout_page();
		hint_result = refused[index];
		memcpy(&expected, &resident_page, sizeof(expected));
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(hint_begins, 1);
		UT_ASSERT_EQ(hint_finishes, 0);
		UT_ASSERT_EQ(CritSectionCount, 0);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(hint_begin_error_releases_pin_before_admission_without_remote_cleanup)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	error_hint_begin = true;
	memcpy(&expected, &resident_page, sizeof(expected));
	check_error_cleanup("LPA");
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(hint_begin_cancel_preserves_error_and_allows_backend_reuse)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	error_hint_begin = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("LPA");
	check_local_cleanup_without_pcm_release("LPA");
	if (pin_held || content_locked || debt != 0)
		return;
	error_hint_begin = false;
	expected_cleanout(&expected, true);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(hint_begins, 2);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPALPA");
}

UT_TEST(changed_final_fence_does_not_start_cleanout)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	changed_holder = true;
	memcpy(&expected, &resident_page, sizeof(expected));
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

static void
expected_single_cleanout(PGAlignedBlock *expected, uint8 slot_index)
{
	ClusterItlSlotData *slots;

	memcpy(expected, &resident_page, sizeof(*expected));
	slots = ClusterPageGetItlSlots((Page)expected->data);
	slots[slot_index].flags = ITL_FLAG_COMMITTED;
	slots[slot_index].commit_scn = slot_scns[slot_index];
	((PageHeader)expected->data)->pd_block_scn++;
	PageSetLSN((Page)expected->data, 102);
}

static void
check_mixed_cleanout(const PGAlignedBlock *expected)
{
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(resolves, 2);
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected->data, BLCKSZ), 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(unproven_later_carrier_does_not_discard_exact_committed_cleanout)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[1] = CLUSTER_TX_UNKNOWN;
	expected_single_cleanout(&expected, 0);
	UT_ASSERT(!collect_catches_error());
	check_mixed_cleanout(&expected);
}

UT_TEST(unproven_earlier_carrier_does_not_hide_later_exact_committed_cleanout)
{
	const ClusterTxOutcome unproven[] = {
		CLUSTER_TX_UNKNOWN, CLUSTER_TX_IN_PROGRESS, CLUSTER_TX_PREPARED};

	for (unsigned index = 0; index < lengthof(unproven); index++)
	{
		PGAlignedBlock expected;

		setup_cleanout_page();
		slot_outcomes[0] = unproven[index];
		expected_single_cleanout(&expected, 1);
		UT_ASSERT(!collect_catches_error());
		check_mixed_cleanout(&expected);
	}
}

UT_TEST(all_unproven_carriers_do_not_repin_or_begin_hint)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[0] = slot_outcomes[1] = CLUSTER_TX_UNKNOWN;
	memcpy(&expected, &resident_page, sizeof(expected));
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(resolves, 2);
	UT_ASSERT_EQ(repins, 0);
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	check_local_cleanup_without_pcm_release("A");
}

UT_TEST(unproven_carrier_witness_change_blocks_every_partial_stamp)
{
	for (unsigned byte = 0; byte < sizeof(ClusterItlSlotData); byte++)
	{
		PGAlignedBlock expected;
		ClusterItlSlotData *slots = setup_cleanout_page();

		slot_outcomes[1] = CLUSTER_TX_UNKNOWN;
		((unsigned char *)&slots[1])[byte] ^= 1;
		memcpy(&expected, &resident_page, sizeof(expected));
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(hint_begins, 0);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(partial_proofs_still_require_current_pcm_and_admission)
{
	for (unsigned change = 0; change < 3; change++)
	{
		PGAlignedBlock expected;

		setup_cleanout_page();
		slot_outcomes[0] = CLUSTER_TX_UNKNOWN;
		changed_holder = change == 0;
		changed_master = change == 1;
		changed_admission = change == 2;
		memcpy(&expected, &resident_page, sizeof(expected));
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
		UT_ASSERT_EQ(hint_begins, 0);
		UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
		check_local_cleanup_without_pcm_release("LPA");
	}
}

UT_TEST(partial_cleanout_hint_error_releases_ownership_before_admission)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[0] = CLUSTER_TX_UNKNOWN;
	error_hint_begin = true;
	memcpy(&expected, &resident_page, sizeof(expected));
	check_error_cleanup("LPA");
	UT_ASSERT_EQ(hint_begins, 1);
	UT_ASSERT_EQ(hint_finishes, 0);
	UT_ASSERT_EQ(CritSectionCount, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}

UT_TEST(partial_cleanout_cancel_preserves_error_and_backend_reuse)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[1] = CLUSTER_TX_UNKNOWN;
	error_hint_begin = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("LPA");
	check_local_cleanup_without_pcm_release("LPA");
	if (pin_held || content_locked || debt != 0)
		return;
	error_hint_begin = false;
	expected_single_cleanout(&expected, 0);
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(hint_begins, 2);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	check_local_cleanup_without_pcm_release("LPALPA");
}

UT_TEST(partial_proof_cannot_stamp_a_different_requested_locator)
{
	PGAlignedBlock expected;

	setup_cleanout_page();
	slot_outcomes[0] = CLUSTER_TX_UNKNOWN;
	mismatch_requested_locator = true;
	memcpy(&expected, &resident_page, sizeof(expected));
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(hint_begins, 0);
	UT_ASSERT_EQ(memcmp(resident_page.data, expected.data, BLCKSZ), 0);
	check_local_cleanup_without_pcm_release("LPA");
}


UT_TEST(creation_binding_runs_after_local_release_before_admission_leave)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_batch.tuples[0].creation_target_proof_index,
		CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(rejected_or_cleaned_collection_cannot_reuse_creation_indexes)
{
	for (int variant = 0; variant < 5; variant++) {
		reset_fixture();
		switch (variant) {
		case 0: vacrel.shared_freeze_batch.carrier_request_count = 0; break;
		case 1: reject_admission = true; break;
		case 2: unknown_outcome = true; break;
		case 3: page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED; break;
		case 4: setup_cleanout_page(); break;
		}
		vacrel.shared_freeze_batch.tuples[0].original_creator_full_xid =
			FullTransactionIdFromEpochAndXid(9, 77);
		(void)lazy_collect_shared_freeze_proofs(&vacrel, 1);
		UT_ASSERT_EQ(target_bindings, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_batch.tuples[0].creation_target_proof_index,
			CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
		UT_ASSERT(!FullTransactionIdIsValid(
			vacrel.shared_freeze_batch.tuples[0].original_creator_full_xid));
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(aborted_modifier_binding_runs_after_local_release_under_original_admission)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(aborted_target_bindings, 1);
	UT_ASSERT_EQ(aborted_candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_batch.tuples[0].aborted_xmax_target_proof_index, 0);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(rejected_or_cleaned_collection_retires_aborted_modifier_association)
{
	for (int variant = 0; variant < 6; variant++) {
		reset_fixture();
		switch (variant) {
		case 0: vacrel.shared_freeze_batch.carrier_request_count = 0; break;
		case 1: reject_admission = true; break;
		case 2: unknown_outcome = true; break;
		case 3: page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED; break;
		case 4: setup_cleanout_page(); break;
		case 5: changed_master = true; break;
		}
		vacrel.shared_freeze_aborted_xmax_candidates.target_count = 7;
		(void)lazy_collect_shared_freeze_proofs(&vacrel, 1);
		UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 0);
		UT_ASSERT_EQ(aborted_candidate_selections, 0);
		UT_ASSERT_EQ(aborted_target_bindings, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_batch.tuples[0].aborted_xmax_target_proof_index,
			CLUSTER_HEAP_FREEZE_NO_ABORTED_XMAX_TARGET);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(aborted_candidate_refusal_keeps_original_canonical_proof)
{
	reset_fixture();
	refuse_aborted_candidates = true;
	vacrel.shared_freeze_aborted_xmax_candidates.target_count = 7;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(aborted_candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 0);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(aborted_candidates_next_collection_cannot_reuse_preceding_rows)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 1);
	unknown_outcome = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 0);
	UT_ASSERT_EQ(aborted_candidate_selections, 1);
	UT_ASSERT_EQ(debt, 0);
}

static void
set_shared_policy(TransactionId xid)
{
	vacrel.cutoffs.SharedFreezeLimitFull = FullTransactionIdFromEpochAndXid(1, xid);
	vacrel.cutoffs.FreezeLimit = xid;
	vacrel.cutoffs.SharedFreezePolicySample.legal_next = FullTransactionIdFromEpochAndXid(1, 1000);
	vacrel.cutoffs.SharedFreezePolicySample.freeze_limit = vacrel.cutoffs.SharedFreezeLimitFull;
	vacrel.cutoffs.SharedFreezePolicySample.allocation_fence.incarnation = 81;
}

static void
seed_stale_cohort(void)
{
	vacrel.shared_freeze_cohort = cohort_value;
	vacrel.shared_freeze_candidate_limit_full = FullTransactionIdFromEpochAndXid(8, 90);
	vacrel.shared_freeze_cohort_observed = true;
}

static void
check_no_cohort(void)
{
	ClusterHeapFreezeCohortObservation empty = {0};

	UT_ASSERT(!vacrel.shared_freeze_cohort_observed);
	UT_ASSERT(!FullTransactionIdIsValid(vacrel.shared_freeze_candidate_limit_full));
	UT_ASSERT_EQ(memcmp(&vacrel.shared_freeze_cohort, &empty, sizeof(empty)), 0);
}

UT_TEST(cohort_observation_uses_original_admission_after_local_release)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_calls, 1);
	UT_ASSERT_EQ(bindings_at_cohort, 0);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(memcmp(&vacrel.shared_freeze_cohort, &cohort_value, sizeof(cohort_value)), 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(vacrel.shared_freeze_candidate_limit_full),
		cohort_value.active_full_xid_min);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(saved_cohort_receipt_checked_before_creation_binding)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_receipt_checks, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(saved_cohort_receipt_refusals_keep_independent_proof_only)
{
	for (int status = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE; status <= CLUSTER_HEAP_FREEZE_COHORT_INVALID; status++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		cohort_receipt_status = status;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(cohort_receipt_checks, 1);
		check_no_cohort();
		UT_ASSERT_EQ(target_bindings, 1);
		UT_ASSERT(vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_EQ(leaves, 1);
	}
}

UT_TEST(admission_recheck_follows_refused_saved_cohort_receipt)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	cohort_receipt_status = CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
	revoke_on_receipt_check = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(cohort_receipt_checks, 1);
	UT_ASSERT_EQ(target_bindings, 0);
	check_no_cohort();
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(cohort_source_fence_is_rechecked_after_policy_before_binding)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_fence_checks, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(memcmp(&vacrel.shared_freeze_cohort.admission_fence,
		&cohort_value.admission_fence, sizeof(cohort_value.admission_fence)), 0);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(stale_cohort_source_fence_keeps_proof_without_saving_candidate)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	cohort_fence_current = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_fence_checks, 1);
	check_no_cohort();
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(admission_change_during_cohort_fence_recheck_precedes_binding)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	revoke_on_cohort_fence_check = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(cohort_fence_checks, 1);
	check_no_cohort();
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(cohort_source_fence_error_and_cancel_unwind_original_admission)
{
	for (unsigned variant = 0; variant < 2; variant++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		error_cohort_fence = true;
		if (variant == 1) inject_code = ERRCODE_QUERY_CANCELED;
		check_error_cleanup("LPA");
		UT_ASSERT_EQ(cohort_fence_checks, 1);
		UT_ASSERT_EQ(target_bindings, 0);
		check_no_cohort();
		error_cohort_fence = false;
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(enters, 2);
		UT_ASSERT_EQ(leaves, 2);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(cohort_candidate_cannot_raise_original_full_policy_bound)
{
	reset_fixture();
	set_shared_policy(40);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_calls, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT(FullTransactionIdEquals(vacrel.shared_freeze_candidate_limit_full,
		vacrel.cutoffs.SharedFreezeLimitFull));
}

UT_TEST(cohort_refusals_clear_observation_without_rejecting_original_proof)
{
	for (int status = CLUSTER_HEAP_FREEZE_COHORT_INCOMPLETE;
		 status <= CLUSTER_HEAP_FREEZE_COHORT_INVALID; status++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		cohort_status = (ClusterHeapFreezeCohortStatus)status;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(cohort_calls, 1);
		check_no_cohort();
		UT_ASSERT(vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(target_bindings, 1);
		UT_ASSERT_EQ(leaves, 1);
	}
}

UT_TEST(mismatched_or_malformed_cohort_never_publishes_candidate)
{
	for (int variant = 0; variant < 14; variant++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		switch (variant) {
		case 0: cohort_value.record_generation++; break;
		case 1: cohort_value.formation_epoch++; break;
		case 2: cohort_value.horizon_scn = 0; break;
		case 3: cohort_value.horizon_scn = UINT64_MAX; break;
		case 4: cohort_value.active_full_xid_min = 0; break;
		case 5: cohort_value.active_full_xid_min = (UINT64CONST(1) << 32) | 1; break;
		case 6: cohort_value.future_full_xid_max = 0; break;
		case 7: cohort_value.future_full_xid_max = UINT64_MAX; break;
		case 8: cohort_value.future_full_xid_max = (UINT64CONST(1) << 32) | 2; break;
		case 9: cohort_value.active_full_xid_min = cohort_value.future_full_xid_max + 1; break;
		case 10: cohort_value.admitted_members_lo = 0; break;
		case 11: cohort_value.admitted_members_lo = 2; break;
		case 12: cohort_value.local_unqualified_entries = 1; break;
		case 13: cohort_value.local_unqualified_entries = UINT64_MAX; break;
		}
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(cohort_calls, 1);
		check_no_cohort();
		UT_ASSERT(vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(revoked_or_mismatched_original_policy_cannot_publish_cohort)
{
	for (unsigned variant = 0; variant < 3; variant++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		if (variant == 0) policy_sample_current = false;
		if (variant == 1)
			vacrel.cutoffs.SharedFreezePolicySample.freeze_limit = InvalidFullTransactionId;
		if (variant == 2)
			vacrel.cutoffs.SharedFreezePolicySample.freeze_limit = FullTransactionIdFromEpochAndXid(2, 80);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		check_no_cohort();
		UT_ASSERT_EQ(cohort_calls, variant == 0 ? 1 : 0);
		UT_ASSERT_EQ(policy_checks, variant == 0 ? 1 : 0);
		UT_ASSERT_EQ(target_bindings, 1);
		UT_ASSERT(vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_EQ(leaves, 1);
	}
}

UT_TEST(original_admission_recheck_follows_policy_fence_recheck)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	revoke_on_policy_check = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	check_no_cohort();
	UT_ASSERT_EQ(policy_checks, 1);
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(unavailable_or_mismatched_full_policy_skips_cohort_sample)
{
	for (int variant = 0; variant < 4; variant++)
	{
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		switch (variant) {
		case 0: vacrel.cutoffs.SharedFreezeLimitFull = InvalidFullTransactionId; break;
		case 1: vacrel.cutoffs.FreezeLimit++; break;
		case 2: set_shared_policy(FrozenTransactionId); break;
		case 3: vacrel.cutoffs.SharedFreezeLimitFull = FullTransactionIdFromU64(UINT64_MAX);
			vacrel.cutoffs.FreezeLimit = MaxTransactionId; break;
		}
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(cohort_calls, 0);
		check_no_cohort();
		UT_ASSERT_EQ(target_bindings, 1);
	}
}

UT_TEST(rejected_changed_or_cleaned_collection_retires_old_cohort)
{
	for (int variant = 0; variant < 5; variant++)
	{
		reset_fixture();
		switch (variant) {
		case 0: vacrel.shared_freeze_batch.carrier_request_count = 0; break;
		case 1: reject_admission = true; break;
		case 2: unknown_outcome = true; break;
		case 3: page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED; break;
		case 4: setup_cleanout_page(); break;
		}
		set_shared_policy(80);
		seed_stale_cohort();
		(void)lazy_collect_shared_freeze_proofs(&vacrel, 1);
		UT_ASSERT_EQ(cohort_calls, 0);
		check_no_cohort();
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(admission_change_during_cohort_sample_requests_retry_before_binding)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	revoke_cohort = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(cohort_calls, 1);
	check_no_cohort();
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_batch.tuples[0].creation_target_proof_index,
		CLUSTER_HEAP_FREEZE_NO_CREATION_TARGET);
	UT_ASSERT(!FullTransactionIdIsValid(vacrel.shared_freeze_batch.tuples[0].original_creator_full_xid));
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(refused_cohort_still_rechecks_original_admission)
{
	reset_fixture();
	set_shared_policy(80);
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
	revoke_cohort = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(cohort_calls, 1);
	check_no_cohort();
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(cohort_error_leaves_original_admission_after_page_cleanup)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	error_cohort = true;
	check_error_cleanup("LPA");
	UT_ASSERT_EQ(cohort_calls, 1);
	UT_ASSERT_EQ(target_bindings, 0);
	check_no_cohort();
}

UT_TEST(cohort_cancel_preserves_error_and_allows_same_backend_reuse)
{
	reset_fixture();
	set_shared_policy(80);
	error_cohort = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("LPA");
	check_no_cohort();
	error_cohort = false;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_calls, 2);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
}

UT_TEST(next_collection_cannot_reuse_previous_cohort_on_sample_refusal)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	cohort_status = CLUSTER_HEAP_FREEZE_COHORT_BUSY_PUBLISH;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(cohort_calls, 2);
	check_no_cohort();
	UT_ASSERT_EQ(debt, 0);
}


static void
setup_partial_xmin_candidates(void)
{
	setup_cleanout_page();
	slot_outcomes[1] = CLUSTER_TX_UNKNOWN;
	hint_result = CLUSTER_SPACE_HINT_SKIPPED;
	set_shared_policy(80);
}

UT_TEST(partial_xmin_candidate_keeps_page_unproven_after_original_release)
{
	PGAlignedBlock before;
	setup_partial_xmin_candidates();
	memcpy(&before, &resident_page, sizeof(before));
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(target_bindings, 1); UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(aborted_target_bindings + aborted_candidate_selections, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(hint_finishes, 0);
	UT_ASSERT_EQ(memcmp(&before, &resident_page, sizeof(before)), 0);
	UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(partial_xmin_candidates_require_successful_resident_and_pcm_recheck)
{
	for (int variant = 0; variant < 4; variant++) {
		setup_partial_xmin_candidates();
		if (variant == 0) reject_route_after = true;
		if (variant == 1) page_match = CLUSTER_HEAP_FREEZE_PAGE_CHANGED;
		if (variant == 2) changed_holder = true;
		if (variant == 3) changed_admission = true;
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(candidate_selections + target_bindings, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT(!vacrel.shared_freeze_cohort_observed || !vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(partial_xmin_candidates_keep_original_target_and_source_revocation)
{
	for (int variant = 0; variant < 4; variant++) {
		setup_partial_xmin_candidates();
		if (variant == 0) target_admission_current = false;
		if (variant == 1) revoke_on_target_recheck = true;
		if (variant == 2) target_source_change = 1;
		if (variant == 3) target_source_change = 2;
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(target_admission_checks, 1);
		UT_ASSERT_EQ(candidate_selections, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT(!vacrel.shared_freeze_cohort_observed);
		UT_ASSERT(!vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(partial_xmin_target_cancel_unwinds_original_admission)
{
	setup_partial_xmin_candidates();
	error_target_recheck = true;
	inject_code = ERRCODE_QUERY_CANCELED;
	UT_ASSERT(collect_catches_error());
	UT_ASSERT_EQ(caught_code, inject_code);
	UT_ASSERT_EQ(candidate_selections + target_bindings, 0);
	UT_ASSERT(!guard_held && !pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(partial_xmin_next_unknown_attempt_retires_previous_subset)
{
	setup_partial_xmin_candidates();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	slot_outcomes[0] = CLUSTER_TX_UNKNOWN;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT(!vacrel.shared_freeze_cohort_observed && !vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(leaves, 2);
}

UT_TEST(partial_xmin_successful_hint_retires_proofs_and_candidates)
{
	setup_partial_xmin_candidates();
	hint_result = CLUSTER_SPACE_HINT_VERSIONED;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(hint_finishes, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 0);
	UT_ASSERT_EQ(candidate_selections + target_bindings, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT(!vacrel.shared_freeze_cohort_observed && !vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(xmin_candidate_batch_uses_same_bound_proof_and_cohort_after_release)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT(FullTransactionIdEquals(candidate_limit_seen, vacrel.shared_freeze_candidate_limit_full));
	UT_ASSERT_EQ(candidate_horizon_seen, vacrel.shared_freeze_cohort.horizon_scn);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(xmin_candidate_selection_requires_current_cohort_and_policy)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset_fixture();
		set_shared_policy(80);
		vacrel.shared_freeze_xmin_candidates.target_count = 7;
		if (variant == 0) cohort_receipt_status = CLUSTER_HEAP_FREEZE_COHORT_H_STALE;
		if (variant == 1) policy_sample_current = false;
		if (variant == 2) cohort_value.local_unqualified_entries = 1;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(target_bindings, 1);
		UT_ASSERT_EQ(candidate_selections, 0);
		UT_ASSERT_EQ(clock_reads + clock_observations, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(xmin_candidate_refusal_preserves_proof_without_stale_selection)
{
	reset_fixture();
	set_shared_policy(80);
	vacrel.shared_freeze_xmin_candidates.target_count = 7;
	select_candidates = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(xmin_candidate_next_attempt_cannot_reuse_previous_targets)
{
	for (unsigned variant = 0; variant < 2; variant++) {
		reset_fixture();
		set_shared_policy(80);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
		if (variant == 0) reject_route_after = true;
		else changed_admission = true;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
			variant == 0 ? CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN
			: CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
		UT_ASSERT_EQ(candidate_selections, 1);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(xmin_candidate_collection_without_full_policy_clears_old_targets)
{
	reset_fixture();
	vacrel.shared_freeze_xmin_candidates.target_count = 7;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(candidate_selections, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(clock_observation_uses_same_h_after_local_cleanup)
{
	for (int unknown = 0; unknown < 2; unknown++) {
		reset_fixture();
		set_shared_policy(80);
		fixture_clock = unknown ? InvalidScn : scn_encode(7, 499);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(clock_reads, 2);
		UT_ASSERT_EQ(clock_observations, 1);
		UT_ASSERT_EQ(clock_horizon_seen, cohort_value.horizon_scn);
		UT_ASSERT(scn_time_cmp(fixture_clock, cohort_value.horizon_scn) >= 0);
		UT_ASSERT_EQ(policy_checks, 2);
		UT_ASSERT_EQ(cohort_receipt_checks, 2);
		UT_ASSERT(vacrel.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(candidate_selections, 1);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(ready_clock_skips_observe_and_preserves_current_checks)
{
	for (int ahead = 0; ahead < 2; ahead++) {
		reset_fixture();
		set_shared_policy(80);
		fixture_clock = scn_encode(7, 500 + ahead);
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(clock_reads, 1);
		UT_ASSERT_EQ(clock_observations, 0);
		UT_ASSERT_EQ(policy_checks, 1);
		UT_ASSERT_EQ(cohort_receipt_checks, 1);
		UT_ASSERT(vacrel.shared_freeze_cohort_observed);
		UT_ASSERT_EQ(candidate_selections, 1);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(clock_refusal_keeps_independent_proof_without_candidates)
{
	reset_fixture();
	set_shared_policy(80);
	seed_stale_cohort();
	fixture_clock = scn_encode(7, 499);
	refuse_clock_observe = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(clock_reads, 2);
	UT_ASSERT_EQ(clock_observations, 1);
	check_no_cohort();
	UT_ASSERT_EQ(candidate_selections, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(clock_error_and_cancel_unwind_original_admission)
{
	const int codes[] = { ERRCODE_INTERNAL_ERROR, ERRCODE_QUERY_CANCELED };

	for (unsigned i = 0; i < lengthof(codes); i++) {
		reset_fixture();
		set_shared_policy(80);
		seed_stale_cohort();
		fixture_clock = InvalidScn;
		error_clock_observe = true;
		inject_code = codes[i];
		UT_ASSERT(collect_catches_error());
		UT_ASSERT_EQ(caught_code, inject_code);
		UT_ASSERT_EQ(clock_observations, 1);
		UT_ASSERT_EQ(enters, 1);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_EQ(target_bindings, 0);
		UT_ASSERT_EQ(candidate_selections, 0);
		check_no_cohort();
		UT_ASSERT(!guard_held && !pin_held && !content_locked);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(clock_observe_revocation_rechecks_same_protection_before_binding)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		reset_fixture();
		set_shared_policy(80);
		fixture_clock = InvalidScn;
		stale_policy_on_clock_observe = variant == 0;
		stale_cohort_on_clock_observe = variant == 1;
		revoke_on_clock_observe = variant == 2;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1),
			variant == 2 ? CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED : CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(clock_observations, 1);
		UT_ASSERT_EQ(policy_checks, 2);
		UT_ASSERT_EQ(cohort_receipt_checks, variant == 0 ? 1 : 2);
		UT_ASSERT_EQ(target_bindings, variant == 2 ? 0 : 1);
		UT_ASSERT_EQ(candidate_selections, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		check_no_cohort();
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(proof_cost_counts_all_five_transport_paths_once)
{
	reset_fixture();
	charge_transport = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 20);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 15);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 15);
	UT_ASSERT(!vacrel.shared_freeze_proof_cost_incomplete);
	UT_ASSERT_EQ(header_snapshots, 2);
	UT_ASSERT_EQ(verdict_snapshots, 2);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(proof_cost_charges_error_and_cancel_before_original_leave)
{
	const int codes[] = { ERRCODE_INTERNAL_ERROR, ERRCODE_QUERY_CANCELED };

	for (unsigned i = 0; i < lengthof(codes); i++) {
		reset_fixture();
		charge_transport = true;
		vacrel.shared_freeze_batch.carrier_request_count = 2;
		fail_resolve = 2;
		inject_code = codes[i];
		UT_ASSERT(collect_catches_error());
		UT_ASSERT_EQ(caught_code, inject_code);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 40);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 30);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 30);
		UT_ASSERT_EQ(header_snapshots, 2);
		UT_ASSERT_EQ(verdict_snapshots, 2);
		UT_ASSERT_EQ(transport_verdict.r4_tx_resolve.calls, 12);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "GA");
	}
}

UT_TEST(proof_cost_accumulates_refusal_and_next_success_without_reset)
{
	reset_fixture();
	charge_transport = unknown_outcome = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	unknown_outcome = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 40);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 30);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 30);
	UT_ASSERT_EQ(header_snapshots, 4);
	UT_ASSERT_EQ(verdict_snapshots, 4);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT(!vacrel.shared_freeze_proof_cost_incomplete);
}

UT_TEST(proof_cost_no_admission_does_not_sample_or_change_totals)
{
	for (unsigned i = 0; i < 2; i++) {
		reset_fixture();
		charge_transport = true;
		vacrel.shared_freeze_proof_calls = 7;
		vacrel.shared_freeze_proof_requests = 8;
		vacrel.shared_freeze_proof_exchanges = 9;
		if (i == 0) reject_admission = true;
		else vacrel.shared_freeze_batch.carrier_request_count = 0;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(header_snapshots + verdict_snapshots, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 7);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 8);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 9);
		UT_ASSERT(!vacrel.shared_freeze_proof_cost_incomplete);
	}
}

UT_TEST(proof_cost_counter_reset_is_unknown_and_sticky_not_free)
{
	reset_fixture();
	charge_transport = reset_transport_during_resolve = true;
	transport_header.calls = 100;
	vacrel.shared_freeze_proof_calls = 7;
	vacrel.shared_freeze_proof_requests = 8;
	vacrel.shared_freeze_proof_exchanges = 9;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(vacrel.shared_freeze_proof_cost_incomplete);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 7);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 8);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 9);
	reset_transport_during_resolve = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(vacrel.shared_freeze_proof_cost_incomplete);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 27);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 23);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 24);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(proof_cost_accumulator_overflow_preserves_all_totals)
{
	for (unsigned i = 0; i < 3; i++) {
		uint64 totals[] = {7, 8, 9};

		reset_fixture();
		charge_transport = true;
		totals[i] = UINT64_MAX - 1;
		vacrel.shared_freeze_proof_calls = totals[0];
		vacrel.shared_freeze_proof_requests = totals[1];
		vacrel.shared_freeze_proof_exchanges = totals[2];
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(vacrel.shared_freeze_proof_cost_incomplete);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, totals[0]);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, totals[1]);
		UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, totals[2]);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(proof_cost_transport_sum_overflow_is_unknown)
{
	reset_fixture();
	charge_transport = overflow_transport_sum = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(vacrel.shared_freeze_proof_cost_incomplete);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_calls, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_requests, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proof_exchanges, 0);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(target_callback_stale_sources_cannot_retain_freeze_candidate)
{
	for (unsigned change = 1; change <= 2; change++) {
		reset_fixture(); set_shared_policy(80); seed_stale_cohort();
		target_source_change = change;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT_EQ(target_admission_checks, 1); UT_ASSERT_EQ(final_policy_checks, 1);
		UT_ASSERT_EQ(final_cohort_checks, change == 1 ? 0 : 1);
		check_no_cohort(); UT_ASSERT_EQ(candidate_selections, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT(vacrel.shared_freeze_fence_valid); UT_ASSERT_EQ(target_bindings, 1);
		UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(final_source_checks_recheck_original_census_last)
{
	for (unsigned change = 3; change <= 4; change++) {
		reset_fixture(); set_shared_policy(80); seed_stale_cohort();
		target_source_change = change;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
		UT_ASSERT_EQ(final_policy_checks, 1); UT_ASSERT_EQ(final_cohort_checks, 1);
		check_no_cohort(); UT_ASSERT_EQ(candidate_selections + target_bindings, 0);
		UT_ASSERT(!vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT(!guard_held && !pin_held && !content_locked);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(final_source_error_or_cancel_uses_original_admission_cleanup)
{
	const int codes[] = { ERRCODE_INTERNAL_ERROR, ERRCODE_QUERY_CANCELED };
	for (unsigned change = 5; change <= 6; change++) {
		for (unsigned i = 0; i < lengthof(codes); i++) {
			reset_fixture(); set_shared_policy(80); seed_stale_cohort();
			target_source_change = change; inject_code = codes[i];
			UT_ASSERT(collect_catches_error()); UT_ASSERT_EQ(caught_code, inject_code);
			UT_ASSERT_EQ(final_policy_checks, 1);
			UT_ASSERT_EQ(final_cohort_checks, change == 5 ? 0 : 1);
			check_no_cohort(); UT_ASSERT_EQ(candidate_selections + target_bindings, 0);
			UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
			UT_ASSERT(!guard_held && !pin_held && !content_locked);
			UT_ASSERT_STR_EQ(cleanup_order, "LPA");
		}
	}
}

UT_TEST(target_success_rechecks_same_sources_before_candidate_retention)
{
	reset_fixture(); set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(final_policy_checks, 1); UT_ASSERT_EQ(final_cohort_checks, 1);
	UT_ASSERT_EQ(final_cohort_fence_checks, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed && vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	reset_fixture(); set_shared_policy(80); target_admission_current = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(final_policy_checks + final_cohort_checks, 0);
	UT_ASSERT_EQ(candidate_selections, 0); check_no_cohort();
}

UT_TEST(positive_candidate_requires_full_target_admission)
{
	reset_fixture();
	set_shared_policy(80);
	target_admission_current = false;
	seed_stale_cohort();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(target_admission_checks, 1);
	check_no_cohort();
	UT_ASSERT_EQ(candidate_selections, 0);
	UT_ASSERT_EQ(target_bindings, 1);
	UT_ASSERT(vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(positive_candidate_full_target_recheck_uses_original_token_after_clock)
{
	reset_fixture();
	set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(target_admission_checks, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT_EQ(enters, 1);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(positive_target_recheck_error_and_cancel_use_original_cleanup)
{
	const int codes[] = {ERRCODE_INTERNAL_ERROR, ERRCODE_QUERY_CANCELED};
	for (unsigned i = 0; i < lengthof(codes); i++)
	{
		reset_fixture();
		set_shared_policy(80);
		error_target_recheck = true;
		inject_code = codes[i];
		UT_ASSERT(collect_catches_error());
		UT_ASSERT_EQ(caught_code, codes[i]);
		UT_ASSERT_EQ(target_admission_checks, 1);
		check_no_cohort();
		UT_ASSERT_EQ(target_bindings, 0);
		UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(positive_target_recheck_generation_change_is_retry)
{
	reset_fixture();
	set_shared_policy(80);
	revoke_on_target_recheck = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(target_admission_checks, 1);
	check_no_cohort();
	UT_ASSERT_EQ(target_bindings, 0);
	UT_ASSERT(!vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(leaves, 1);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(positive_target_recheck_skips_unproven_or_unbound_inputs)
{
	reset_fixture();
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(target_admission_checks, 0);
	reset_fixture();
	set_shared_policy(80);
	unknown_outcome = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
	UT_ASSERT_EQ(target_admission_checks, 0);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(positive_target_readiness_change_allows_same_backend_next_attempt)
{
	reset_fixture();
	set_shared_policy(80);
	target_admission_current = false;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	check_no_cohort();
	/* Per-attempt observation only; retain the original backend/debt stacks. */
	target_bindings = 0;
	target_admission_current = true;
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(target_admission_checks, 2);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT_EQ(enters, 2);
	UT_ASSERT_EQ(leaves, 2);
	UT_ASSERT_EQ(debt, 0);
}

UT_TEST(test_original_writer_input_uses_conditional_writer_resume)
{
	reset_fixture();
	vacrel.shared_freeze_input.writer_context_valid = true;
	/* The physical input now names an original writer use. The actual
	 * recheck must resume that exact use instead of taking read-only X. */
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(writer_resumes, 1);
	UT_ASSERT_EQ(writer_releases, 1);
	UT_ASSERT(!writer_scope_active && !pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_EQ(generic_unlocks + remote_releases, 0);
}

UT_TEST(test_refused_original_writer_never_falls_back_to_read_only_x)
{
	reset_fixture();
	vacrel.shared_freeze_input.writer_context_valid = true;
	writer_resume_allowed = false;
	UT_ASSERT(!collect_catches_error());
	UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED);
	UT_ASSERT_EQ(writer_resumes, 1);
	UT_ASSERT_EQ(lock_attempts + writer_releases + generic_unlocks + remote_releases, 0);
	UT_ASSERT(!pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_EQ(debt, 0);
}
UT_TEST(test_original_writer_error_cancel_cleans_local_scope_before_admission)
{
	for (int which = 0; which < 2; which++) {
		reset_fixture();
		vacrel.shared_freeze_input.writer_context_valid = true;
		error_page_check = true;
		inject_code = which ? ERRCODE_QUERY_CANCELED : ERRCODE_DATA_EXCEPTION;
		cluster_gcs_block_local_cache = false; remote_release_error = true;
		UT_ASSERT(collect_catches_error());
		UT_ASSERT_EQ(caught_code, inject_code);
		UT_ASSERT_EQ(writer_resumes, 1); UT_ASSERT_EQ(writer_releases, 1);
		UT_ASSERT_EQ(generic_unlocks + remote_releases, 0);
		UT_ASSERT(!writer_scope_active && !pin_held && !content_locked && !leave_with_ownership);
		UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT(strstr(cleanup_order, "WLPA") != NULL);
	}
}

UT_TEST(test_writer_resume_error_before_lock_still_unpins_before_admission)
{
	reset_fixture();
	vacrel.shared_freeze_input.writer_context_valid = true;
	writer_resume_error = true; inject_code = ERRCODE_QUERY_CANCELED;
	UT_ASSERT(collect_catches_error());
	UT_ASSERT_EQ(caught_code, inject_code);
	UT_ASSERT_EQ(writer_resumes, 1);
	UT_ASSERT_EQ(lock_attempts + writer_releases + generic_unlocks + remote_releases, 0);
	UT_ASSERT(!writer_scope_active && !pin_held && !content_locked && !leave_with_ownership);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_EQ(leaves, 1);
}

UT_TEST(bound_sources_borrow_original_owned_policy_and_cohort)
{
	reset_fixture(); set_shared_policy(80);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed);
	UT_ASSERT(vacrel.shared_freeze_input.freeze_policy == &vacrel.cutoffs.SharedFreezePolicySample);
	UT_ASSERT(vacrel.shared_freeze_input.freeze_cohort == &vacrel.shared_freeze_cohort);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_local_history.sample_clock_scn, 700);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_local_history.contributor_kind,
		CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_local_history.contributor_token, 11);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_peer_history[1].sample_clock_scn, 800);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_peer_history[1].contributor_kind,
		CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED);
	UT_ASSERT_EQ(vacrel.shared_freeze_input.freeze_cohort->source_peer_history[1].contributor_token, 17);
	UT_ASSERT_EQ(cohort_calls, 1);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(refused_collection_retires_previous_bound_source_borrows)
{
	for (int early = 0; early < 2; early++) {
		reset_fixture(); set_shared_policy(80);
		vacrel.shared_freeze_input.freeze_policy = &vacrel.cutoffs.SharedFreezePolicySample;
		vacrel.shared_freeze_input.freeze_cohort = &vacrel.shared_freeze_cohort;
		if (early) vacrel.shared_freeze_batch.carrier_request_count = 0;
		else target_admission_current = false;
		(void)lazy_collect_shared_freeze_proofs(&vacrel, 1);
		UT_ASSERT(vacrel.shared_freeze_input.freeze_policy == NULL);
		UT_ASSERT(vacrel.shared_freeze_input.freeze_cohort == NULL);
		UT_ASSERT_EQ(debt, 0);
	}
}

UT_TEST(native_window_candidate_subset_keeps_physical_order)
{
	uint64 next;
	const uint64 distances[] = {UINT64CONST(0x80000001), UINT64CONST(0x7fffffff),
		UINT64CONST(0x80000000), 923};

	reset_fixture(); set_shared_policy(80);
	next = U64FromFullTransactionId(vacrel.cutoffs.SharedFreezePolicySample.legal_next);
	use_candidate_value = true;
	candidate_value.target_count = lengthof(distances);
	vacrel.shared_freeze_batch.tuple_count = lengthof(distances);
	for (unsigned i = 0; i < lengthof(distances); i++) {
		candidate_value.targets[i].tuple_index = i;
		candidate_value.targets[i].original_creator_full_xid =
			FullTransactionIdFromU64(next - distances[i]);
	}
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 2);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.targets[0].tuple_index, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.targets[1].tuple_index, 3);
	UT_ASSERT(FullTransactionIdEquals(vacrel.shared_freeze_xmin_candidates.targets[0].original_creator_full_xid,
		candidate_value.targets[1].original_creator_full_xid));
	UT_ASSERT(FullTransactionIdEquals(vacrel.shared_freeze_xmin_candidates.targets[1].original_creator_full_xid,
		candidate_value.targets[3].original_creator_full_xid));
	UT_ASSERT_EQ(candidate_selections, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed && vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(native_window_empty_subset_keeps_other_proof_observations)
{
	reset_fixture(); set_shared_policy(80);
	use_candidate_value = true;
	candidate_value.target_count = 1;
	candidate_value.targets[0].tuple_index = 0;
	candidate_value.targets[0].original_creator_full_xid = FullTransactionIdFromU64(
		U64FromFullTransactionId(vacrel.cutoffs.SharedFreezePolicySample.legal_next)
		- UINT64CONST(0x80000000));
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_aborted_xmax_candidates.target_count, 1);
	UT_ASSERT(vacrel.shared_freeze_cohort_observed && vacrel.shared_freeze_fence_valid);
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(observed_cutoff_outside_native_window_keeps_proofs_without_candidates)
{
	for (unsigned variant = 0; variant < 3; variant++) {
		uint64 legal;
		reset_fixture(); set_shared_policy(80);
		legal = U64FromFullTransactionId(vacrel.cutoffs.SharedFreezePolicySample.legal_next);
		cohort_value.active_full_xid_min = legal - UINT64CONST(0x80000000) - variant;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
		UT_ASSERT(!vacrel.shared_freeze_cohort_observed);
		UT_ASSERT(!FullTransactionIdIsValid(vacrel.shared_freeze_candidate_limit_full));
		UT_ASSERT(vacrel.shared_freeze_input.freeze_policy == NULL);
		UT_ASSERT(vacrel.shared_freeze_input.freeze_cohort == NULL);
		UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 0);
		UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
		UT_ASSERT(vacrel.shared_freeze_fence_valid);
		UT_ASSERT_EQ(debt, 0); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
	}
}

UT_TEST(native_window_subset_uses_original_legal_next_not_freeze_limit)
{
	reset_fixture(); set_shared_policy(80);
	vacrel.cutoffs.SharedFreezePolicySample.legal_next = FullTransactionIdFromU64(
		U64FromFullTransactionId(FullTransactionIdFromEpochAndXid(1, 77))
		+ UINT64CONST(0x7fffffff));
	/* Keep the selected cutoff inside this original N_legal window; only
	 * the second creator falls outside it. */
	cohort_value.active_full_xid_min = U64FromFullTransactionId(
		FullTransactionIdFromEpochAndXid(1, 79));
	cohort_value.local_registration_cut.min_full = FullTransactionIdFromU64(cohort_value.active_full_xid_min);
	cohort_value.source_peer_active_full_xid_min[1] = cohort_value.active_full_xid_min;
	use_candidate_value = true;
	candidate_value.target_count = 2;
	vacrel.shared_freeze_batch.tuple_count = 2;
	for (unsigned i = 0; i < 2; i++) {
		candidate_value.targets[i].tuple_index = i;
		candidate_value.targets[i].original_creator_full_xid = FullTransactionIdFromEpochAndXid(1, 77 - i);
	}
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs(&vacrel, 1), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.target_count, 1);
	UT_ASSERT_EQ(vacrel.shared_freeze_xmin_candidates.targets[0].tuple_index, 0);
	UT_ASSERT(FullTransactionIdEquals(vacrel.shared_freeze_xmin_candidates.targets[0].original_creator_full_xid,
		candidate_value.targets[0].original_creator_full_xid));
	UT_ASSERT_EQ(debt, 0); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(original_admission_owner_finally_releases_debt_after_guard_cancel)
{
	reset_fixture(); fixture_original_admission_setup();
	fail_resolve = 1; inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("GA");
	UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
}

UT_TEST(original_admission_normal_and_refused_paths_release_after_local_owners)
{
	const char *orders[] = {"LPA", "A", "A", "PA", "LPA", "WLPA", "PA"};
	const ClusterHeapFreezeProofStatusV1 statuses[] = {
		CLUSTER_HEAP_FREEZE_PROOF_PROVEN, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN,
		CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED, CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED,
		CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED, CLUSTER_HEAP_FREEZE_PROOF_PROVEN,
		CLUSTER_HEAP_FREEZE_PROOF_RETRY_CHANGED
	};

	for (unsigned which = 0; which < lengthof(orders); which++) {
		reset_fixture(); fixture_original_admission_setup();
		switch (which) {
		case 1: unknown_outcome = true; break;
		case 2: resident = false; break;
		case 3: lock_available = false; break;
		case 4: PageSetLSN((Page)resident_page.data, 102); break;
		case 5: vacrel.shared_freeze_input.writer_context_valid = true; break;
		case 6:
			vacrel.shared_freeze_input.writer_context_valid = true;
			writer_resume_allowed = false; break;
		}
		UT_ASSERT(!collect_catches_error()); UT_ASSERT_EQ(collected_status, statuses[which]);
		UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_STR_EQ(cleanup_order, orders[which]);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT_EQ(fixture_admission_registrations, 1);
		UT_ASSERT(fixture_admission_exit_callback != NULL);
		UT_ASSERT(!leave_with_ownership);
	}
}

UT_TEST(original_admission_error_and_cancel_preserve_original_exception)
{
	const int codes[] = {ERRCODE_DATA_EXCEPTION, ERRCODE_QUERY_CANCELED};
	const char *orders[] = {"A", "GA", "LPA", "LPA", "LPA", "LPA", "WLPA", "PA"};

	for (unsigned code = 0; code < lengthof(codes); code++) {
		for (unsigned which = 0; which < lengthof(orders); which++) {
			reset_fixture(); fixture_original_admission_setup(); inject_code = codes[code];
			switch (which) {
			case 0: error_preflight = true; break;
			case 1: fail_resolve = 1; break;
			case 2: error_validate = true; break;
			case 3: error_page_check = true; break;
			case 4: set_shared_policy(80); error_cohort = true; break;
			case 5: set_shared_policy(80); error_target_recheck = true; break;
			case 6:
				vacrel.shared_freeze_input.writer_context_valid = true;
				error_page_check = true; break;
			case 7:
				vacrel.shared_freeze_input.writer_context_valid = true;
				writer_resume_error = true; break;
			}
			cluster_gcs_block_local_cache = false; remote_release_error = true;
			check_error_cleanup(orders[which]);
			UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
			UT_ASSERT_EQ(fixture_admission_registrations, 1);
			UT_ASSERT_EQ(generic_unlocks + remote_releases, 0);
		}
	}
}

UT_TEST(original_admission_preentry_refusals_do_not_leave_or_owe_debt)
{
	ClusterSemanticAdmissionToken zero = {0};

	for (unsigned which = 0; which < 8; which++) {
		reset_fixture(); fixture_original_admission_setup();
		switch (which) {
		case 0: SemanticActivationShmem = NULL; break;
		case 1: fixture_admission_descriptors[fixture_admission_feature()] = NULL; break;
		case 2: pg_atomic_write_u64(&fixture_admission_shared.record_generation, 0); break;
		case 3: fixture_admission_epoch++; break;
		case 4: pg_atomic_write_u32(&fixture_admission_shared.transition_closed, 1); break;
		case 5: fixture_admission_pid = 0; break;
		case 6: fixture_admission_serving = false; break;
		case 7: pg_atomic_write_u64(&fixture_admission_shared.admission_seq, 3); break;
		}
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves + debt + resolves, 0);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		UT_ASSERT(memcmp(&fixture_admission_last_token, &zero, sizeof(zero)) == 0);
		UT_ASSERT_EQ(fixture_admission_holdoff, 0); UT_ASSERT_STR_EQ(cleanup_order, "");
	}
}

UT_TEST(original_admission_backend_reuses_exit_registration_after_cancel)
{
	pg_on_exit_callback original_callback;
	Datum original_arg;

	reset_fixture(); fixture_original_admission_setup();
	fail_resolve = 1; inject_code = ERRCODE_QUERY_CANCELED;
	check_error_cleanup("GA");
	original_callback = fixture_admission_exit_callback; original_arg = fixture_admission_exit_arg;
	/* Keep both original counters, the exit registration and PG stacks intact. */
	fail_resolve = 0;
	UT_ASSERT(!collect_catches_error()); UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT_EQ(enters, 2); UT_ASSERT_EQ(leaves, 2); UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_EQ(fixture_original_admission_debt(), 0); UT_ASSERT_STR_EQ(cleanup_order, "GALPA");
	UT_ASSERT_EQ(fixture_admission_registrations, 1);
	UT_ASSERT(fixture_admission_exit_callback == original_callback);
	UT_ASSERT(fixture_admission_exit_arg == original_arg);
	UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
}

UT_TEST(original_admission_finally_preserves_other_backend_debt)
{
	for (unsigned which = 0; which < 3; which++) {
		reset_fixture(); fixture_original_admission_setup();
		/* Controlled other-backend debt is not charged to this backend's ledger. */
		fixture_admission_foreign_debt = 7;
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 7);
		if (which) {
			fail_resolve = 1;
			inject_code = which == 1 ? ERRCODE_DATA_EXCEPTION : ERRCODE_QUERY_CANCELED;
			check_error_cleanup("GA");
		} else {
			UT_ASSERT(!collect_catches_error()); UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_STR_EQ(cleanup_order, "LPA");
		}
		UT_ASSERT_EQ(pg_atomic_read_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()]), 7);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		/* Retire only the controlled foreign input before the next isolated case. */
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 0);
		fixture_admission_foreign_debt = 0;
	}
}

UT_TEST(original_registered_exit_after_collector_finally_does_not_double_release)
{
	for (unsigned which = 0; which < 3; which++) {
		unsigned completed_cleanups;

		reset_fixture(); fixture_original_admission_setup();
		fixture_admission_foreign_debt = 7;
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 7);
		if (which) {
			fail_resolve = 1;
			inject_code = which == 1 ? ERRCODE_DATA_EXCEPTION : ERRCODE_QUERY_CANCELED;
			check_error_cleanup("GA");
		} else {
			UT_ASSERT(!collect_catches_error());
			UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
			UT_ASSERT_STR_EQ(cleanup_order, "LPA");
		}
		UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		UT_ASSERT_EQ(fixture_admission_registrations, 1);
		UT_ASSERT(fixture_admission_exit_callback != NULL);
		completed_cleanups = cleanup_count;
		/* Invoke the original registered callback with its original argument.
		 * Registration/PID scheduling is controlled; no actual OS exit occurs. */
		fixture_admission_exit_callback(0, fixture_admission_exit_arg);
		UT_ASSERT_EQ(cleanup_count, completed_cleanups); UT_ASSERT_EQ(leaves, 1);
		UT_ASSERT_EQ(semantic_activation_exit_hook_pid, 0);
		UT_ASSERT_EQ(fixture_admission_holdoff, 0);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		UT_ASSERT_EQ(pg_atomic_read_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()]), 7);
		UT_ASSERT(!guard_held && !pin_held && !content_locked && !writer_scope_active);
		UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 0);
		fixture_admission_foreign_debt = 0;
	}
}

UT_TEST(original_registered_exit_after_entry_refusal_keeps_foreign_debt)
{
	for (unsigned which = 0; which < 3; which++) {
		reset_fixture(); fixture_original_admission_setup();
		fixture_admission_foreign_debt = 7;
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 7);
		switch (which) {
		case 0: pg_atomic_write_u64(&fixture_admission_shared.admission_seq, 3); break;
		case 1: fixture_admission_epoch++; break;
		case 2: pg_atomic_write_u64(&fixture_admission_shared.record_generation, 0); break;
		}
		UT_ASSERT(!collect_catches_error());
		UT_ASSERT_EQ(collected_status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves + debt + resolves, 0);
		UT_ASSERT_EQ(fixture_admission_registrations, 1);
		UT_ASSERT(fixture_admission_exit_callback != NULL);
		fixture_admission_exit_callback(0, fixture_admission_exit_arg);
		UT_ASSERT_EQ(semantic_activation_exit_hook_pid, 0);
		UT_ASSERT_EQ(fixture_admission_holdoff, 0); UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		UT_ASSERT_EQ(pg_atomic_read_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()]), 7);
		UT_ASSERT_EQ(leaves + cleanup_count, 0); UT_ASSERT_STR_EQ(cleanup_order, "");
		UT_ASSERT(PG_exception_stack == NULL && error_context_stack == NULL);
		pg_atomic_write_u32(&fixture_admission_shared.inflight[CLUSTER_SEMANTIC_TARGET_SIDE][fixture_admission_feature()], 0);
		fixture_admission_foreign_debt = 0;
	}
}

UT_TEST(admitted_phase_returns_to_original_owner_before_leave)
{
	ClusterSemanticAdmissionToken token;
	PcmAuthoritySnapshot authority;
	BufferTag tag;
	uint64 master;
	SCN page_scn;
	bool rows_current = false;

	reset_fixture(); fixture_original_admission_setup();
	InitBufferTag(&tag, &relation.rd_locator, MAIN_FORKNUM, vacrel.shared_freeze_batch.block);
	UT_ASSERT(cluster_pcm_lock_r4_route_snapshot(tag, &authority, &master, &page_scn));
	UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token), CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs_admitted(&vacrel, 1,
		&authority, master, &token, &rows_current), CLUSTER_HEAP_FREEZE_PROOF_PROVEN);
	UT_ASSERT(rows_current && token.entered);
	UT_ASSERT_EQ(vacrel.shared_freeze_proofs.proof_count, 1);
	UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 0); UT_ASSERT_EQ(debt, 1);
	UT_ASSERT_EQ(fixture_original_admission_debt(), 1);
	UT_ASSERT_STR_EQ(cleanup_order, "LP");
	cluster_semantic_activation_leave(&token);
	UT_ASSERT(!token.entered); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
	UT_ASSERT_STR_EQ(cleanup_order, "LPA");
}

UT_TEST(admitted_phase_refuses_missing_borrowed_owner_without_side_effects)
{
	for (unsigned which = 0; which < 5; which++) {
		ClusterSemanticAdmissionToken token = {0};
		PcmAuthoritySnapshot authority = {0};
		bool rows_current = true;

		reset_fixture(); fixture_original_admission_setup();
		/* Isolate each missing-context guard from the unentered-token case. */
		token.entered = which >= 2;
		UT_ASSERT_EQ(lazy_collect_shared_freeze_proofs_admitted(
			which == 3 ? NULL : &vacrel, 1, which == 2 ? NULL : &authority,
			0, which == 0 ? NULL : &token, which == 4 ? NULL : &rows_current),
			CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		if (which != 4) UT_ASSERT(!rows_current);
		UT_ASSERT_EQ(enters + leaves + debt + resolves + routes + repins, 0);
		UT_ASSERT_EQ(fixture_admission_registrations, 0);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
		UT_ASSERT(!fixture_actual_backend_has_admission());
		UT_ASSERT_STR_EQ(cleanup_order, "");
	}
}

static bool
admitted_phase_catches_error(const PcmAuthoritySnapshot *authority, uint64 master,
	const ClusterSemanticAdmissionToken *token)
{
	volatile bool caught = false;
	bool rows_current = false;

	PG_TRY();
	{
		collected_status = lazy_collect_shared_freeze_proofs_admitted(&vacrel, 1,
			authority, master, token, &rows_current);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	return caught;
}

UT_TEST(admitted_phase_error_unwinds_local_owners_but_keeps_caller_admission)
{
	const char *orders[] = {"", "G", "LP"};
	const int codes[] = {ERRCODE_DATA_EXCEPTION, ERRCODE_QUERY_CANCELED};

	for (unsigned code = 0; code < lengthof(codes); code++) {
		for (unsigned which = 0; which < lengthof(orders); which++) {
			ClusterSemanticAdmissionToken token;
			PcmAuthoritySnapshot authority;
			BufferTag tag;
			uint64 master;
			SCN page_scn;

			reset_fixture(); fixture_original_admission_setup();
			InitBufferTag(&tag, &relation.rd_locator, MAIN_FORKNUM, vacrel.shared_freeze_batch.block);
			UT_ASSERT(cluster_pcm_lock_r4_route_snapshot(tag, &authority, &master, &page_scn));
			UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token), CLUSTER_SEMANTIC_ADMISSION_OK);
			inject_code = codes[code];
			if (which == 0) error_preflight = true;
			if (which == 1) fail_resolve = 1;
			if (which == 2) error_page_check = true;
			UT_ASSERT(admitted_phase_catches_error(&authority, master, &token));
			UT_ASSERT_EQ(caught_code, codes[code]);
			UT_ASSERT(token.entered); UT_ASSERT_EQ(enters, 1); UT_ASSERT_EQ(leaves, 0);
			UT_ASSERT_EQ(debt, 1); UT_ASSERT_EQ(fixture_original_admission_debt(), 1);
			UT_ASSERT_STR_EQ(cleanup_order, orders[which]);
			UT_ASSERT(!guard_held && !pin_held && !content_locked && !writer_scope_active);
			UT_ASSERT(exit_callback == NULL && PG_exception_stack == NULL && error_context_stack == NULL);
			cluster_semantic_activation_leave(&token);
			UT_ASSERT(!token.entered); UT_ASSERT_EQ(leaves, 1); UT_ASSERT_EQ(debt, 0);
		}
	}
}

UT_TEST(admitted_phase_requires_released_resident_hint_before_proof_transport)
{
	for (int fault = 0; fault < 3; fault++) {
		ClusterSemanticAdmissionToken token = {0}; PcmAuthoritySnapshot authority;
		BufferTag tag; uint64 master; SCN page_scn;
		volatile bool caught = false; bool rows_current = true;
		ClusterHeapFreezeProofStatusV1 status = CLUSTER_HEAP_FREEZE_PROOF_PROVEN;
		reset_fixture(); fixture_original_admission_setup();
		InitBufferTag(&tag, &relation.rd_locator, MAIN_FORKNUM, vacrel.shared_freeze_batch.block);
		UT_ASSERT(cluster_pcm_lock_r4_route_snapshot(tag, &authority, &master, &page_scn));
		UT_ASSERT_EQ(cluster_semantic_activation_enter_r4_terminal_census(&token), CLUSTER_SEMANTIC_ADMISSION_OK);
		pin_held = fault == 0;
		/* Any preflight call is visible without entering a blocking resolver. */
		error_preflight = true;
		PG_TRY();
		{
			status = lazy_collect_shared_freeze_proofs_admitted(&vacrel,
				fault == 1 ? InvalidBuffer : fault == 2 ? NBuffers + 1 : 1,
				&authority, master, &token, &rows_current);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(!caught); UT_ASSERT_EQ(status, CLUSTER_HEAP_FREEZE_PROOF_KEEP_UNPROVEN);
		UT_ASSERT(!rows_current); UT_ASSERT_EQ(pin_held, fault == 0);
		UT_ASSERT(token.entered); UT_ASSERT_EQ(debt, 1);
		UT_ASSERT_EQ(fixture_original_admission_debt(), 1); UT_ASSERT_EQ(leaves, 0);
		UT_ASSERT_EQ(resolves + repins + lock_attempts, 0); UT_ASSERT_STR_EQ(cleanup_order, "");
		/* The rejected helper does not own this test caller's pin or token. */
		pin_held = false;
		cluster_semantic_activation_leave(&token);
		UT_ASSERT(!token.entered); UT_ASSERT_EQ(fixture_original_admission_debt(), 0);
	}
}

int
main(void)
{
	UT_PLAN(136);
	UT_RUN(admitted_phase_requires_released_resident_hint_before_proof_transport);
	UT_RUN(admitted_phase_refuses_missing_borrowed_owner_without_side_effects);
	UT_RUN(admitted_phase_error_unwinds_local_owners_but_keeps_caller_admission);
	UT_RUN(admitted_phase_returns_to_original_owner_before_leave);
	UT_RUN(original_registered_exit_after_collector_finally_does_not_double_release);
	UT_RUN(original_registered_exit_after_entry_refusal_keeps_foreign_debt);
	UT_RUN(original_admission_normal_and_refused_paths_release_after_local_owners);
	UT_RUN(original_admission_error_and_cancel_preserve_original_exception);
	UT_RUN(original_admission_preentry_refusals_do_not_leave_or_owe_debt);
	UT_RUN(original_admission_backend_reuses_exit_registration_after_cancel);
	UT_RUN(original_admission_finally_preserves_other_backend_debt);
	UT_RUN(original_admission_owner_finally_releases_debt_after_guard_cancel);
	UT_RUN(native_window_candidate_subset_keeps_physical_order);
	UT_RUN(native_window_empty_subset_keeps_other_proof_observations);
	UT_RUN(observed_cutoff_outside_native_window_keeps_proofs_without_candidates);
	UT_RUN(native_window_subset_uses_original_legal_next_not_freeze_limit);
	UT_RUN(bound_sources_borrow_original_owned_policy_and_cohort);
	UT_RUN(refused_collection_retires_previous_bound_source_borrows);
	UT_RUN(partial_xmin_candidate_keeps_page_unproven_after_original_release);
	UT_RUN(partial_xmin_candidates_require_successful_resident_and_pcm_recheck);
	UT_RUN(partial_xmin_candidates_keep_original_target_and_source_revocation);
	UT_RUN(partial_xmin_target_cancel_unwinds_original_admission);
	UT_RUN(partial_xmin_next_unknown_attempt_retires_previous_subset);
	UT_RUN(partial_xmin_successful_hint_retires_proofs_and_candidates);
	UT_RUN(aborted_candidate_refusal_keeps_original_canonical_proof);
	UT_RUN(aborted_candidates_next_collection_cannot_reuse_preceding_rows);
	UT_RUN(aborted_modifier_binding_runs_after_local_release_under_original_admission);
	UT_RUN(rejected_or_cleaned_collection_retires_aborted_modifier_association);
	UT_RUN(test_writer_resume_error_before_lock_still_unpins_before_admission);
	UT_RUN(test_refused_original_writer_never_falls_back_to_read_only_x);
	UT_RUN(test_original_writer_error_cancel_cleans_local_scope_before_admission);
	UT_RUN(test_original_writer_input_uses_conditional_writer_resume);
	UT_RUN(target_callback_stale_sources_cannot_retain_freeze_candidate);
	UT_RUN(final_source_checks_recheck_original_census_last);
	UT_RUN(final_source_error_or_cancel_uses_original_admission_cleanup);
	UT_RUN(target_success_rechecks_same_sources_before_candidate_retention);
	UT_RUN(positive_candidate_requires_full_target_admission);
	UT_RUN(positive_candidate_full_target_recheck_uses_original_token_after_clock);
	UT_RUN(positive_target_recheck_error_and_cancel_use_original_cleanup);
	UT_RUN(positive_target_recheck_generation_change_is_retry);
	UT_RUN(positive_target_recheck_skips_unproven_or_unbound_inputs);
	UT_RUN(positive_target_readiness_change_allows_same_backend_next_attempt);
	UT_RUN(proof_cost_counts_all_five_transport_paths_once);
	UT_RUN(proof_cost_charges_error_and_cancel_before_original_leave);
	UT_RUN(proof_cost_accumulates_refusal_and_next_success_without_reset);
	UT_RUN(proof_cost_no_admission_does_not_sample_or_change_totals);
	UT_RUN(proof_cost_counter_reset_is_unknown_and_sticky_not_free);
	UT_RUN(proof_cost_accumulator_overflow_preserves_all_totals);
	UT_RUN(proof_cost_transport_sum_overflow_is_unknown);
	UT_RUN(clock_observation_uses_same_h_after_local_cleanup);
	UT_RUN(ready_clock_skips_observe_and_preserves_current_checks);
	UT_RUN(clock_refusal_keeps_independent_proof_without_candidates);
	UT_RUN(clock_error_and_cancel_unwind_original_admission);
	UT_RUN(clock_observe_revocation_rechecks_same_protection_before_binding);
	UT_RUN(xmin_candidate_batch_uses_same_bound_proof_and_cohort_after_release);
	UT_RUN(xmin_candidate_selection_requires_current_cohort_and_policy);
	UT_RUN(xmin_candidate_refusal_preserves_proof_without_stale_selection);
	UT_RUN(xmin_candidate_next_attempt_cannot_reuse_previous_targets);
	UT_RUN(xmin_candidate_collection_without_full_policy_clears_old_targets);

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
	UT_RUN(needs_cleanout_uses_captured_scn_under_original_admission);
	UT_RUN(retained_scn_never_merges_equal_xids_in_different_carriers);
	UT_RUN(unrelated_or_incomplete_carriers_keep_original_exact_route);
	UT_RUN(refused_retained_pair_keeps_other_exact_carrier_without_retrying);
	UT_RUN(retained_pair_error_and_cancel_unwind_guard_before_admission);
	UT_RUN(retained_pair_still_requires_final_resident_authority);
	UT_RUN(canonical_and_retained_evidence_share_only_one_hint_write);
	UT_RUN(retained_only_hint_requires_original_lsn_version_and_admission);
	UT_RUN(malformed_retained_pair_cannot_poison_other_canonical_carrier);
	UT_RUN(committed_carriers_use_one_versioned_hint_and_invalidate_saved_fence);
	UT_RUN(every_carrier_witness_byte_is_checked_before_first_stamp);
	UT_RUN(requested_locator_must_match_exactly_before_any_stamp);
	UT_RUN(aborted_proof_cannot_stamp_a_committed_hint);
	UT_RUN(non_data_or_ineligible_slots_keep_all_original_bytes);
	UT_RUN(no_eligible_carrier_does_not_start_hint_publication);
	UT_RUN(shared_cleanout_has_no_native_or_cache_miss_fallback);
	UT_RUN(hint_begin_error_releases_pin_before_admission_without_remote_cleanup);
	UT_RUN(hint_begin_cancel_preserves_error_and_allows_backend_reuse);
	UT_RUN(changed_final_fence_does_not_start_cleanout);
	UT_RUN(unproven_later_carrier_does_not_discard_exact_committed_cleanout);
	UT_RUN(unproven_earlier_carrier_does_not_hide_later_exact_committed_cleanout);
	UT_RUN(all_unproven_carriers_do_not_repin_or_begin_hint);
	UT_RUN(unproven_carrier_witness_change_blocks_every_partial_stamp);
	UT_RUN(partial_proofs_still_require_current_pcm_and_admission);
	UT_RUN(partial_cleanout_hint_error_releases_ownership_before_admission);
	UT_RUN(partial_cleanout_cancel_preserves_error_and_backend_reuse);
	UT_RUN(partial_proof_cannot_stamp_a_different_requested_locator);
	UT_RUN(creation_binding_runs_after_local_release_before_admission_leave);
	UT_RUN(rejected_or_cleaned_collection_cannot_reuse_creation_indexes);
	UT_RUN(cohort_observation_uses_original_admission_after_local_release);
	UT_RUN(saved_cohort_receipt_checked_before_creation_binding);
	UT_RUN(saved_cohort_receipt_refusals_keep_independent_proof_only);
	UT_RUN(admission_recheck_follows_refused_saved_cohort_receipt);
	UT_RUN(cohort_source_fence_is_rechecked_after_policy_before_binding);
	UT_RUN(stale_cohort_source_fence_keeps_proof_without_saving_candidate);
	UT_RUN(admission_change_during_cohort_fence_recheck_precedes_binding);
	UT_RUN(cohort_source_fence_error_and_cancel_unwind_original_admission);
	UT_RUN(cohort_candidate_cannot_raise_original_full_policy_bound);
	UT_RUN(cohort_refusals_clear_observation_without_rejecting_original_proof);
	UT_RUN(mismatched_or_malformed_cohort_never_publishes_candidate);
	UT_RUN(revoked_or_mismatched_original_policy_cannot_publish_cohort);
	UT_RUN(original_admission_recheck_follows_policy_fence_recheck);
	UT_RUN(unavailable_or_mismatched_full_policy_skips_cohort_sample);
	UT_RUN(rejected_changed_or_cleaned_collection_retires_old_cohort);
	UT_RUN(admission_change_during_cohort_sample_requests_retry_before_binding);
	UT_RUN(refused_cohort_still_rechecks_original_admission);
	UT_RUN(cohort_error_leaves_original_admission_after_page_cleanup);
	UT_RUN(cohort_cancel_preserves_error_and_allows_same_backend_reuse);
	UT_RUN(next_collection_cannot_reuse_previous_cohort_on_sample_refusal);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
