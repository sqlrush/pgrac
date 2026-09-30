/*-------------------------------------------------------------------------
 * test_cluster_side_online_owner.c
 *    RF-SIDE real production callback owner and freshness ordering.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_online_owner.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/storage/cluster_undo_xlog.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();
#include "test_cluster_undo_header_identity.inc"

sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

typedef struct OwnerCapture {
	uint32 plan_operation_count;
	uint32 authority_calls;
	uint32 authority_fail_call;
	uint32 authority_throw_call;
	uint32 pushes;
	uint32 pops;
	uint32 xact_preflights;
	uint32 undo_preflights;
	uint32 projection_preflights;
	uint32 xact_applies;
	uint32 undo_applies;
	uint32 projection_applies;
	uint32 scope_enters, scope_leaves, target_reads;
	bool canonical_batch, bad_commit, scope_denied;
	uint16 legacy_terminal;
	uint32 header_writes, header_syncs, covered_commits;
	bool write_failure, post_read_mismatch;
	bool authority_throw_after_pop;
	Size scratch_budget;
} OwnerCapture;

static OwnerCapture capture;
static ClusterThreadRecoveryAuthorityV1 canonical_authority;
static ClusterUndoRecoveryScopeV1 *active_scope;
static PGAlignedBlock canonical_header;

RfPageProofDetailV1
rf_side_online_plan_prepare_undo_block_v1(const RfSideOnlinePlanV1 *plan,
	uint8 instance, uint32 segment, uint32 block, RfSideUndoBlockImageV1 *out)
{
	UT_ASSERT(false); /* This fixture contains TT records only. */
	return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
}

/* Immutable-planner boundary; the real planner's source/slot proof is covered
 * by test_cluster_side_xact. This fixture supplies one predetermined result. */
RfPageProofDetailV1
rf_side_online_plan_prepare_undo_header_v1(const RfSideOnlinePlanV1 *plan,
	uint8 instance, uint32 segment, const char *base, RfSideUndoHeaderImageV1 *out)
{
	TTSlot *slot;

	UT_ASSERT(plan != NULL && instance == 3 && segment == 513);
	if (capture.bad_commit) return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	memset(out, 0, sizeof(*out));
	memcpy(out->page.data, base, BLCKSZ);
	slot = &((UndoSegmentHeaderData *)out->page.data)->tt_slots[4];
	memset(slot, 0, sizeof(*slot));
	slot->xid = 802;
	slot->wrap = 7;
	slot->status = capture.legacy_terminal == XLOG_UNDO_TT_SLOT_ABORT
		? TT_SLOT_ABORTED : TT_SLOT_COMMITTED;
	slot->commit_scn = slot->status == TT_SLOT_COMMITTED ? 999 : InvalidScn;
	out->operation_count = 2;
	out->source_index = 1;
	return RF_PAGE_PROOF_DETAIL_OK;
}

bool
rf_side_online_plan_contains_commit_v1(const RfSideOnlinePlanV1 *plan,
	const RfSideXactOperationV1 *operation)
{
	return plan != NULL && operation->kind == RF_SIDE_XACT_COMMIT && operation->has_tt_delta;
}

Size
rf_side_online_plan_scratch_available_v1(const RfSideOnlinePlanV1 *plan)
{
	return capture.scratch_budget;
}

bool
cluster_undo_recovery_scope_enter_v1(ClusterUndoRecoveryScopeV1 *scope,
	const ClusterThreadRecoveryAuthorityV1 *authority, const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(authority == &canonical_authority);
	UT_ASSERT(plan != NULL && active_scope == NULL);
	if (capture.scope_denied) return false;
	capture.scope_enters++;
	active_scope = scope;
	return true;
}

void
cluster_undo_recovery_scope_leave_v1(ClusterUndoRecoveryScopeV1 *scope)
{
	if (scope == active_scope && scope != NULL) {
		active_scope = NULL;
		capture.scope_leaves++;
	}
}

bool
cluster_undo_smgr_read_block(ClusterUndoPathIntent intent, uint32 segment,
	uint8 owner, uint32 block, char *out)
{
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(active_scope != NULL);
	UT_ASSERT_EQ(owner, 3);
	UT_ASSERT_EQ(segment, 513);
	UT_ASSERT_EQ(block, 0);
	capture.target_reads++;
	memcpy(out, canonical_header.data, BLCKSZ);
	if (capture.post_read_mismatch && capture.header_writes != 0) out[128] ^= 1;
	return true;
}

bool
cluster_undo_smgr_write_block(ClusterUndoPathIntent intent, uint32 segment,
	uint8 instance, uint32 block, const char *data, bool sync)
{
	UT_ASSERT_EQ(intent, CLUSTER_UNDO_PATH_RECOVERY_SHARED);
	UT_ASSERT(active_scope != NULL && segment == 513 && instance == 3 && block == 0 && sync);
	/* All target checks must precede even the first canonical write. */
	UT_ASSERT_EQ(capture.projection_preflights, 1);
	capture.header_writes++;
	if (capture.write_failure) return false;
	memcpy(canonical_header.data, data, BLCKSZ);
	capture.header_syncs++;
	return true;
}

bool
cluster_undo_smgr_fsync_segment_file(uint32 segment, uint8 instance)
{
	UT_ASSERT(active_scope != NULL && segment == 513 && instance == 3);
	capture.header_syncs++;
	return !capture.write_failure;
}

RfSideXactApplyResultV1
rf_side_xact_apply_covered_commit_v1(const RfSideXactOperationV1 *operation,
	void *arg, bool (*verify)(void *, const RfSideXactOperationV1 *))
{
	if (!verify(arg, operation)) return RF_SIDE_XACT_APPLY_BLOCKED;
	UT_ASSERT_EQ(capture.header_syncs, 1);
	capture.covered_commits++;
	return RF_SIDE_XACT_APPLY_OK;
}

uint32
rf_side_online_plan_operation_count_v1(const RfSideOnlinePlanV1 *plan)
{
	UT_ASSERT(plan != NULL);
	return capture.plan_operation_count;
}

static bool
fresh_authority(void *arg)
{
	OwnerCapture *state = (OwnerCapture *)arg;

	state->authority_calls++;
	if (state->authority_throw_call == state->authority_calls
		|| (state->authority_throw_after_pop && state->pops != 0))
		pg_re_throw();
	return state->authority_fail_call == 0 || state->authority_calls != state->authority_fail_call;
}

static bool
canonical_fresh(void *arg)
{
	UT_ASSERT(arg == &canonical_authority);
	return fresh_authority(&capture);
}

void
cluster_remote_xact_online_writer_push(void)
{
	capture.pushes++;
}

void
cluster_remote_xact_online_writer_pop(void)
{
	capture.pops++;
}

bool
rf_side_online_projection_owner_init_v1(RfSideOnlineProjectionOwnerV1 *owner, uint32 cluster_epoch,
										bool failed_origin_redo_retained)
{
	memset(owner, 0, sizeof(*owner));
	owner->cluster_epoch = cluster_epoch;
	owner->failed_origin_redo_retained = failed_origin_redo_retained;
	return cluster_epoch != 0;
}

bool
rf_side_online_projection_preflight_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *projection = (RfSideOnlineProjectionOwnerV1 *)arg;

	capture.projection_preflights++;
	return projection->cluster_epoch == 19 && projection->failed_origin_redo_retained
		   && operation != NULL && operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

bool
rf_side_online_projection_apply_owned_v1(void *arg, const RfSideOnlineOperationV1 *operation)
{
	RfSideOnlineProjectionOwnerV1 *projection = (RfSideOnlineProjectionOwnerV1 *)arg;

	capture.projection_applies++;
	return projection->cluster_epoch == 19 && operation != NULL
		   && operation->kind == RF_SIDE_ONLINE_OPERATION_PROJECTION;
}

RfSideXactApplyResultV1
rf_side_xact_target_preflight_owned_v1(const RfSideXactOperationV1 *operation,
									   const uint8 *owned_payload, uint32 owned_payload_length)
{
	capture.xact_preflights++;
	return operation != NULL && owned_payload == NULL && owned_payload_length == 0
			   ? RF_SIDE_XACT_APPLY_OK
			   : RF_SIDE_XACT_APPLY_BLOCKED;
}

RfSideXactApplyResultV1
rf_side_xact_apply_owned_v1(const RfSideXactOperationV1 *operation, const uint8 *owned_payload,
							uint32 owned_payload_length)
{
	capture.xact_applies++;
	return operation != NULL && owned_payload == NULL && owned_payload_length == 0
			   ? RF_SIDE_XACT_APPLY_OK
			   : RF_SIDE_XACT_APPLY_BLOCKED;
}

ClusterUndoTargetPreflightV1
cluster_undo_preflight_tt_target_v1(const ClusterUndoDecoded *decoded)
{
	capture.undo_preflights++;
	return decoded != NULL ? CLUSTER_UNDO_TARGET_APPLY : CLUSTER_UNDO_TARGET_BLOCKED;
}

ClusterUndoApplyResultV1
cluster_undo_apply_tt_v1(const ClusterUndoDecoded *decoded)
{
	capture.undo_applies++;
	return decoded != NULL ? CLUSTER_UNDO_APPLY_OK : CLUSTER_UNDO_APPLY_BLOCKED;
}

static void
make_operations(RfSideOnlineOperationV1 operations[3])
{
	memset(operations, 0, 3 * sizeof(*operations));
	operations[0].kind = RF_SIDE_ONLINE_OPERATION_XACT;
	operations[1].kind = RF_SIDE_ONLINE_OPERATION_UNDO;
	operations[2].kind = RF_SIDE_ONLINE_OPERATION_PROJECTION;
	if (capture.canonical_batch) {
		ClusterUndoDecoded *undo = &operations[0].undo;
		RfSideXactOperationV1 *xact = &operations[1].xact;

		operations[0].kind = RF_SIDE_ONLINE_OPERATION_UNDO;
		operations[1].kind = RF_SIDE_ONLINE_OPERATION_XACT;
		undo->kind = CLUSTER_UNDO_KIND_TT_BIND;
		undo->opcode = XLOG_UNDO_TT_SLOT_BIND;
		undo->instance = 3;
		undo->segment_id = 513;
		undo->slot_offset = 4;
		undo->wrap = 7;
		undo->xid = 802;
		undo->format_version = CLUSTER_UNDO_TT_BIND_VERSION;
		xact->kind = RF_SIDE_XACT_COMMIT;
		xact->has_tt_delta = true;
		xact->tt_delta.instance = 3;
		xact->tt_delta.segment_id = 513;
		xact->tt_delta.slot_offset = 4;
		xact->tt_delta.wrap = 7;
		xact->tt_delta.xid = capture.bad_commit ? 803 : 802;
		xact->tt_delta.commit_scn = 999;
		if (capture.legacy_terminal != 0) {
			operations[1].kind = RF_SIDE_ONLINE_OPERATION_UNDO;
			operations[1].undo = *undo;
			operations[1].undo.opcode = capture.legacy_terminal;
			operations[1].undo.kind = capture.legacy_terminal == XLOG_UNDO_TT_SLOT_COMMIT
				? CLUSTER_UNDO_KIND_TT_COMMIT : CLUSTER_UNDO_KIND_TT_ABORT;
			operations[1].undo.format_version = 0;
			operations[1].undo.xid = capture.bad_commit ? 803 : 802;
			operations[1].undo.commit_scn = capture.legacy_terminal == XLOG_UNDO_TT_SLOT_COMMIT
				? 999 : InvalidScn;
		}
	}
}

RfPageProofDetailV1
rf_side_online_plan_preflight_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfSideOnlineOperationV1 operations[3];
	uint32 i;

	UT_ASSERT(plan != NULL);
	make_operations(operations);
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (i = 0; i < 3; i++) {
		bool accepted = operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT ? ops->preflight_xact(ops->arg, &operations[i])
						: operations[i].kind == RF_SIDE_ONLINE_OPERATION_UNDO ? ops->preflight_undo(ops->arg, &operations[i])
								 : ops->preflight_projection(ops->arg, &operations[i]);

		if (!accepted) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	ops->end_protected_set(ops->arg, false);
	return RF_PAGE_PROOF_DETAIL_OK;
}

RfPageProofDetailV1
rf_side_online_plan_apply_v1(const RfSideOnlinePlanV1 *plan, const RfSideOnlineApplyOpsV1 *ops)
{
	RfSideOnlineOperationV1 operations[3];
	uint32 i;

	UT_ASSERT(plan != NULL);
	if (capture.plan_operation_count == 0)
		return RF_PAGE_PROOF_DETAIL_OK;
	make_operations(operations);
	if (!ops->begin_protected_set(ops->arg))
		return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
	for (i = 0; i < 3; i++) {
		bool accepted = operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT ? ops->preflight_xact(ops->arg, &operations[i])
						: operations[i].kind == RF_SIDE_ONLINE_OPERATION_UNDO ? ops->preflight_undo(ops->arg, &operations[i])
								 : ops->preflight_projection(ops->arg, &operations[i]);

		if (!accepted) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	for (i = 0; i < 3; i++) {
		bool applied = operations[i].kind == RF_SIDE_ONLINE_OPERATION_XACT ? ops->apply_xact(ops->arg, &operations[i])
					   : operations[i].kind == RF_SIDE_ONLINE_OPERATION_UNDO ? ops->apply_undo(ops->arg, &operations[i])
								: ops->apply_projection(ops->arg, &operations[i]);

		if (!applied) {
			ops->end_protected_set(ops->arg, false);
			return RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE;
		}
	}
	ops->end_protected_set(ops->arg, true);
	return RF_PAGE_PROOF_DETAIL_OK;
}

UT_TEST(test_owner_runs_all_preflights_before_fresh_gated_mutations)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_preflight_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 4);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 8);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT_EQ(capture.xact_preflights, 1);
	UT_ASSERT_EQ(capture.undo_preflights, 1);
	UT_ASSERT_EQ(capture.projection_preflights, 1);
	UT_ASSERT_EQ(capture.xact_applies, 1);
	UT_ASSERT_EQ(capture.undo_applies, 1);
	UT_ASSERT_EQ(capture.projection_applies, 1);
	UT_ASSERT(owner.protected_set_complete);
}

UT_TEST(test_stale_authority_before_first_mutation_closes_whole_set)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	capture.authority_fail_call = 5;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.xact_preflights, 1);
	UT_ASSERT_EQ(capture.undo_preflights, 1);
	UT_ASSERT_EQ(capture.projection_preflights, 1);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT(!owner.protected_set_complete);
}

UT_TEST(test_empty_plan_closes_without_writer_barrier_or_mutation)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
	UT_ASSERT_EQ(capture.authority_calls, 1);
	UT_ASSERT_EQ(capture.pushes, 0);
	UT_ASSERT_EQ(capture.pops, 0);
	UT_ASSERT_EQ(capture.xact_preflights, 0);
	UT_ASSERT_EQ(capture.undo_preflights, 0);
	UT_ASSERT_EQ(capture.projection_preflights, 0);
	UT_ASSERT_EQ(capture.xact_applies, 0);
	UT_ASSERT_EQ(capture.undo_applies, 0);
	UT_ASSERT_EQ(capture.projection_applies, 0);
	UT_ASSERT(owner.protected_set_complete);
}

UT_TEST(test_empty_plan_requires_fresh_closure_authority)
{
	RfSideOnlineProductionOwnerV1 owner;
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

	memset(&capture, 0, sizeof(capture));
	capture.authority_fail_call = 1;
	UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &capture, fresh_authority, 19, true));
	UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
				 RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
	UT_ASSERT_EQ(capture.authority_calls, 1);
	UT_ASSERT_EQ(capture.pushes, 0);
	UT_ASSERT_EQ(capture.pops, 0);
	UT_ASSERT(!owner.protected_set_complete);
}

UT_TEST(test_completion_authority_error_cannot_keep_writer_scope)
{
	RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;
	volatile bool caught = false;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	capture.authority_throw_call = 8; /* begin + 3 preflights + 3 applies + end */
	UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &capture, fresh_authority, 19, true));
	PG_TRY();
	{
		(void)rf_side_online_production_apply_v1(plan, owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(capture.pushes, 1);
	UT_ASSERT_EQ(capture.pops, 1);
	UT_ASSERT(!owner->protected_set_active && !owner->protected_set_complete);
	free(owner);
}

UT_TEST(test_canonical_owner_evolves_private_header_before_any_apply)
{
	for (int fault = 0; fault < 4; fault++) {
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;
		PGAlignedBlock saved;

		memset(&capture, 0, sizeof(capture));
		capture.plan_operation_count = 3;
		capture.canonical_batch = true;
		capture.bad_commit = fault == 1;
		capture.scratch_budget = fault == 2 ? 1 : RF_SIDE_ONLINE_PLAN_MAX_BYTES;
		capture.scope_denied = fault == 3;
		cluster_undo_segment_make_header_bytes(513, 3, canonical_header.data);
		saved = canonical_header;
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &canonical_authority,
			canonical_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &canonical_authority));
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner),
			fault == 0 ? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(capture.xact_applies + capture.undo_applies, 0);
		UT_ASSERT_EQ(capture.header_writes, fault == 0 ? 1 : 0);
		UT_ASSERT_EQ(capture.scope_enters, fault == 3 ? 0 : 1);
		UT_ASSERT_EQ(capture.scope_leaves, capture.scope_enters);
		if (fault == 0) UT_ASSERT(capture.target_reads >= 3);
		else UT_ASSERT_EQ(capture.target_reads, fault == 1 ? 1 : 0);
		UT_ASSERT(active_scope == NULL && owner.undo_headers == NULL);
		UT_ASSERT_EQ(capture.pushes, capture.pops);
		if (fault != 0) UT_ASSERT(memcmp(canonical_header.data, saved.data, BLCKSZ) == 0);
	}
}

UT_TEST(test_canonical_owner_error_releases_qualified_scope_and_headers)
{
	RfSideOnlineProductionOwnerV1 *owner = calloc(1, sizeof(*owner));
	RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;
	volatile bool caught = false;

	memset(&capture, 0, sizeof(capture));
	capture.plan_operation_count = 3;
	capture.canonical_batch = true;
	capture.scratch_budget = RF_SIDE_ONLINE_PLAN_MAX_BYTES;
	capture.authority_throw_after_pop = true;
	cluster_undo_segment_make_header_bytes(513, 3, canonical_header.data);
	UT_ASSERT(rf_side_online_production_owner_init_v1(owner, &canonical_authority,
		canonical_fresh, 19, true));
	UT_ASSERT(rf_side_online_production_bind_undo_v1(owner, &canonical_authority));
	PG_TRY();
	{
		(void)rf_side_online_production_apply_v1(plan, owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(capture.scope_enters, 1);
	UT_ASSERT_EQ(capture.scope_leaves, 1);
	UT_ASSERT(active_scope == NULL && owner->undo_headers == NULL && owner->protected_plan == NULL);
	UT_ASSERT(!owner->protected_set_active && !owner->protected_set_complete);
	UT_ASSERT_EQ(capture.pushes, capture.pops);
	free(owner);
}

UT_TEST(test_legacy_terminal_preflight_preserves_exact_apply_admission)
{
	const uint16 opcodes[] = {XLOG_UNDO_TT_SLOT_COMMIT, XLOG_UNDO_TT_SLOT_ABORT};

	for (unsigned i = 0; i < lengthof(opcodes); i++)
		for (int wrong_xid = 0; wrong_xid < 2; wrong_xid++) {
			RfSideOnlineProductionOwnerV1 owner;
			RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

			memset(&capture, 0, sizeof(capture));
			capture.plan_operation_count = 3;
			capture.canonical_batch = true;
			capture.bad_commit = wrong_xid;
			capture.legacy_terminal = opcodes[i];
			capture.scratch_budget = RF_SIDE_ONLINE_PLAN_MAX_BYTES;
			cluster_undo_segment_make_header_bytes(513, 3, canonical_header.data);
			UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &canonical_authority,
				canonical_fresh, 19, true));
			UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &canonical_authority));
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), wrong_xid
				? RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE : RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(capture.undo_applies, 0);
			UT_ASSERT_EQ(capture.header_writes, wrong_xid ? 0 : 1);
			UT_ASSERT_EQ(capture.projection_applies, wrong_xid ? 0 : 1);
			UT_ASSERT(active_scope == NULL && owner.undo_headers == NULL);
		}
}

UT_TEST(test_canonical_header_install_precedes_covered_commit_and_is_repeatable)
{
	for (int failure = 0; failure < 3; failure++) {
		RfSideOnlineProductionOwnerV1 owner;
		RfSideOnlinePlanV1 *plan = (RfSideOnlinePlanV1 *)(uintptr_t)1;

		memset(&capture, 0, sizeof(capture));
		capture.plan_operation_count = 3;
		capture.canonical_batch = true;
		capture.scratch_budget = RF_SIDE_ONLINE_PLAN_MAX_BYTES;
		capture.write_failure = failure == 1;
		capture.post_read_mismatch = failure == 2;
		cluster_undo_segment_make_header_bytes(513, 3, canonical_header.data);
		UT_ASSERT(rf_side_online_production_owner_init_v1(&owner, &canonical_authority,
			canonical_fresh, 19, true));
		UT_ASSERT(rf_side_online_production_bind_undo_v1(&owner, &canonical_authority));
		UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), failure == 0
			? RF_PAGE_PROOF_DETAIL_OK : RF_PAGE_PROOF_DETAIL_SIDE_INCOMPLETE);
		UT_ASSERT_EQ(capture.header_writes, 1);
		UT_ASSERT_EQ(capture.covered_commits, failure == 0 ? 1 : 0);
		UT_ASSERT_EQ(capture.undo_applies + capture.xact_applies, 0);
		UT_ASSERT(active_scope == NULL && owner.undo_headers == NULL);
		if (failure == 0) {
			capture.header_syncs = capture.covered_commits = 0;
			UT_ASSERT_EQ(rf_side_online_production_apply_v1(plan, &owner), RF_PAGE_PROOF_DETAIL_OK);
			UT_ASSERT_EQ(capture.header_writes, 1);
			UT_ASSERT_EQ(capture.header_syncs, 1);
			UT_ASSERT_EQ(capture.covered_commits, 1);
		}
	}
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_canonical_header_install_precedes_covered_commit_and_is_repeatable);
	UT_RUN(test_legacy_terminal_preflight_preserves_exact_apply_admission);
	UT_RUN(test_canonical_owner_error_releases_qualified_scope_and_headers);
	UT_RUN(test_canonical_owner_evolves_private_header_before_any_apply);
	UT_RUN(test_owner_runs_all_preflights_before_fresh_gated_mutations);
	UT_RUN(test_stale_authority_before_first_mutation_closes_whole_set);
	UT_RUN(test_empty_plan_closes_without_writer_barrier_or_mutation);
	UT_RUN(test_empty_plan_requires_fresh_closure_authority);
	UT_RUN(test_completion_authority_error_cannot_keep_writer_scope);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
