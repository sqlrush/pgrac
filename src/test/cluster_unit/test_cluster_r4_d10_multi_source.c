/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_d10_multi_source.c
 *	  Shared-admission tests for the dormant MultiXact source dispatcher.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_r4_d10_multi_source.c
 *
 * NOTES
 *	  This is a pgrac-original test file.  It links the production
 *	  cluster_multixact object and replaces only its process/external
 *	  dependencies with deterministic in-process fixtures.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_multixact.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_side_projection.h"
#include "cluster/cluster_subtrans.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_visibility_resolve.h"
#include "cluster/cluster_xid_stripe.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

ProcessingMode Mode = NormalProcessing;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_multixact_member_overlay_max_entries = 16;
int cluster_multixact_member_overlay_max_members = 32;
bool cluster_crossnode_runtime_visibility = false;
bool cluster_multi_xmax_remote_resolve = false;
int cluster_subtrans_max_chain_depth = 8;

static ClusterSemanticAdmissionResult admission_result;
static ClusterSemanticAdmissionResult target_admission_result;
static ClusterSemanticAdmissionResult source_admission_result;
static bool admission_result_by_side;
static bool admission_recheck_ok;
static int admission_enter_count;
static int admission_recheck_count;
static int admission_leave_count;
static uint64 admission_feature;
static ClusterSemanticAdmissionSide admission_side;

static int fake_hash_search_count;
static int fake_hash_seq_index;
static bool fake_force_error;
static bool fake_state_found;
static bool fake_lock_found;

static union {
	uint64 align;
	unsigned char bytes[128];
} fake_state;

static LWLockPadded fake_lock;

static union {
	uint64 align;
	unsigned char bytes[8192];
} fake_hash_entry;

static HTAB *const fake_hash = (HTAB *)(uintptr_t)1;

int
cluster_mxid_origin_slot(MultiXactId mxid pg_attribute_unused())
{
	return 0;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	return 0;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(int origin_node pg_attribute_unused(),
												 TransactionId xid pg_attribute_unused(),
												 uint16 *out_seg pg_attribute_unused(),
												 uint16 *out_slot pg_attribute_unused(),
												 uint16 *out_wrap pg_attribute_unused(),
												 uint8 *out_status pg_attribute_unused())
{
	return CLUSTER_TT_DURABLE_LOCATE_MISSING;
}

static void
reset_admission(ClusterSemanticAdmissionResult result, bool recheck_ok)
{
	admission_result = result;
	target_admission_result = result;
	source_admission_result = result;
	admission_result_by_side = false;
	admission_recheck_ok = recheck_ok;
	admission_enter_count = 0;
	admission_recheck_count = 0;
	admission_leave_count = 0;
	admission_feature = 0;
	admission_side = CLUSTER_SEMANTIC_TARGET_SIDE;
}

static void
reset_admission_sides(ClusterSemanticAdmissionResult target_result,
					  ClusterSemanticAdmissionResult source_result, bool recheck_ok)
{
	reset_admission(target_result, recheck_ok);
	target_admission_result = target_result;
	source_admission_result = source_result;
	admission_result_by_side = true;
}

void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	ClusterSemanticAdmissionResult selected_result
		= admission_result_by_side
			  ? (side == CLUSTER_SEMANTIC_TARGET_SIDE ? target_admission_result
													  : source_admission_result)
			  : admission_result;

	admission_enter_count++;
	admission_feature = feature_bit;
	admission_side = side;
	memset(token, 0, sizeof(*token));
	if (selected_result == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->record_generation = 7;
		token->formation_epoch = 11;
		token->side = (uint8)side;
		token->entered = true;
	}
	return selected_result;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	admission_recheck_count++;
	return token != NULL && token->entered && admission_recheck_ok;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	admission_leave_count++;
	if (token != NULL)
		token->entered = false;
}

void *
ShmemInitStruct(const char *name, Size size, bool *found_ptr)
{
	if (strcmp(name, "ClusterMultiXactState") == 0) {
		UT_ASSERT(size <= sizeof(fake_state.bytes));
		*found_ptr = fake_state_found;
		fake_state_found = true;
		return fake_state.bytes;
	}
	UT_ASSERT(strcmp(name, "ClusterMultiXactLock") == 0);
	UT_ASSERT(size <= sizeof(fake_lock));
	*found_ptr = fake_lock_found;
	fake_lock_found = true;
	return &fake_lock;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *info pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	return fake_hash;
}

Size
hash_estimate_size(long num_entries, Size entry_size)
{
	return (Size)num_entries * entry_size;
}

Size
add_size(Size first, Size second)
{
	return first + second;
}

void *
hash_search(HTAB *hashp, const void *key_ptr, HASHACTION action, bool *found_ptr)
{
	fake_hash_search_count++;
	UT_ASSERT(hashp == fake_hash);

	if (action == HASH_ENTER_NULL) {
		if (found_ptr != NULL)
			*found_ptr = memcmp(fake_hash_entry.bytes, key_ptr, sizeof(ClusterMultiXactKey)) == 0;
		memcpy(fake_hash_entry.bytes, key_ptr, sizeof(ClusterMultiXactKey));
		return fake_hash_entry.bytes;
	}
	if (action == HASH_FIND) {
		if (memcmp(fake_hash_entry.bytes, key_ptr, sizeof(ClusterMultiXactKey)) == 0)
			return fake_hash_entry.bytes;
		return NULL;
	}
	if (action == HASH_REMOVE) {
		memset(fake_hash_entry.bytes, 0, sizeof(fake_hash_entry.bytes));
		return fake_hash_entry.bytes;
	}
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{
	fake_hash_seq_index = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	ClusterMultiXactKey zero;

	memset(&zero, 0, sizeof(zero));
	if (fake_hash_seq_index++ == 0 && memcmp(fake_hash_entry.bytes, &zero, sizeof(zero)) != 0)
		return fake_hash_entry.bytes;
	return NULL;
}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

TimestampTz
GetCurrentTimestamp(void)
{
	if (fake_force_error && PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	return (TimestampTz)12345;
}

uint64
cluster_epoch_get_current(void)
{
	return 19;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op pg_attribute_unused(),
								  const ClusterTTStatusSourceRequest *request pg_attribute_unused(),
								  ClusterTTStatusSourceResult *result)
{
	memset(result, 0, sizeof(*result));
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

ClusterTTStatusResult
cluster_subtrans_lookup_parent(const ClusterTTStatusResult *child, int depth pg_attribute_unused())
{
	return *child;
}

ClusterVisVerdict
cluster_vis_cr_xmax_verdict(ClusterTTStatus status pg_attribute_unused(),
							ClusterVisibilityDecision decision)
{
	return decision == CLUSTER_VISIBILITY_INVISIBLE ? CVV_INVISIBLE : CVV_VISIBLE;
}

bool
cluster_gcs_block_undo_multi_verdict_fetch_and_wait(
	int32 origin_node pg_attribute_unused(), MultiXactId mxid pg_attribute_unused(),
	char *page_out pg_attribute_unused(), ClusterLiveAuthority *auth_out pg_attribute_unused())
{
	return false;
}

bool
cluster_vis_live_authority_covers(SCN demand_scn pg_attribute_unused(),
								  ClusterLiveAuthority auth pg_attribute_unused())
{
	return false;
}

void
cluster_vis53r97_note_covers_refuse(void)
{}

void
cluster_vis53r97_note_multi_member_serve_ask(void)
{}

void
cluster_vis53r97_note_multi_member_serve_hit(void)
{}

void
cluster_vis_bump_covers_scn_refuse_count(void)
{}

ClusterVisibilityDecision
cluster_multixact_resolve_visibility_served(
	const ClusterMultiXactServedMember *members pg_attribute_unused(),
	uint16 member_count pg_attribute_unused(), SCN read_scn pg_attribute_unused())
{
	return CLUSTER_VISIBILITY_UNKNOWN;
}

int
scn_time_cmp(SCN first, SCN second)
{
	return first < second ? -1 : first > second ? 1 : 0;
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errcode(int sqlerrcode)
{
	return sqlerrcode;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static ClusterMultiXactSourceRequest
make_install_request(ClusterMultiXactKey *key, ClusterMultiXactMember *member)
{
	ClusterMultiXactSourceRequest request;

	memset(&request, 0, sizeof(request));
	request.key = key;
	request.member_count = 1;
	request.members = member;
	return request;
}

UT_TEST(t1_frozen_dispatch_surface)
{
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL, 0);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP, 1);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_RESOLVE_VISIBILITY, 2);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_GET_MEMBER_COUNT, 3);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_REMOTE_XMAX_RESOLVE, 4);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_NOTE_HALFSPACE_REFUSE, 5);
	UT_ASSERT_EQ((int)CLUSTER_MULTI_SOURCE_NOTE_UNDERIVABLE_READ, 6);
}

UT_TEST(t2_dormant_refuses_before_request_and_mutation)
{
	ClusterMultiXactSourceResult result;
	uint64 before = cluster_multixact_get_overlay_install_count();
	int searches_before = fake_hash_search_count;

	memset(&result, 0x7f, sizeof(result));
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(
					 (ClusterMultiXactSourceOp)CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL,
					 (const ClusterMultiXactSourceRequest *)(uintptr_t)1, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
	UT_ASSERT_EQ(admission_enter_count, 1);
	UT_ASSERT_EQ(admission_recheck_count, 0);
	UT_ASSERT_EQ(admission_leave_count, 0);
	UT_ASSERT_EQ((uint64)admission_feature, (uint64)CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ((int)admission_side, (int)CLUSTER_SEMANTIC_SOURCE_SIDE);
	UT_ASSERT_EQ((int)result.bool_value, 0);
	UT_ASSERT_EQ((int)result.member_count, 0);
	UT_ASSERT_EQ((int)result.visibility, 0);
	UT_ASSERT_EQ((int)result.overlay_hit, 0);
	UT_ASSERT_EQ((uint64)cluster_multixact_get_overlay_install_count(), before);
	UT_ASSERT_EQ(fake_hash_search_count, searches_before);
}

UT_TEST(t3_invalid_after_admission_closes_and_leaves)
{
	ClusterMultiXactSourceResult result;

	memset(&result, 0x7f, sizeof(result));
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ(
		(int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL, NULL, &result),
		(int)CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(admission_enter_count, 1);
	UT_ASSERT_EQ(admission_recheck_count, 0);
	UT_ASSERT_EQ(admission_leave_count, 1);
	UT_ASSERT(!result.bool_value);

	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ(
		(int)cluster_multixact_source_dispatch((ClusterMultiXactSourceOp)99, NULL, &result),
		(int)CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(admission_leave_count, 1);
}

UT_TEST(t4_source_install_and_lookup_are_positive)
{
	ClusterMultiXactKey key;
	ClusterMultiXactMember member;
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;
	union {
		uint64 align;
		unsigned char bytes[offsetof(ClusterMultiXactMemberOverlayResult, members)
							+ sizeof(ClusterMultiXactMember)];
	} output;
	ClusterMultiXactMemberOverlayResult *overlay
		= (ClusterMultiXactMemberOverlayResult *)output.bytes;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 3;
	key.multixact_id = 71;
	key.cluster_epoch = 19;
	memset(&member, 0, sizeof(member));
	member.xid = FirstNormalTransactionId;
	member.status = MultiXactStatusForShare;
	request = make_install_request(&key, &member);
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT_EQ(admission_recheck_count, 1);
	UT_ASSERT_EQ(admission_leave_count, 1);

	memset(&request, 0, sizeof(request));
	memset(overlay, 0, sizeof(output.bytes));
	request.key = &key;
	request.overlay_out = overlay;
	request.max_members_buf = 1;
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT_EQ((int)result.member_count, 1);
	UT_ASSERT(overlay->authoritative);
	UT_ASSERT_EQ((int)overlay->members[0].xid, (int)member.xid);
}

UT_TEST(t5_source_visibility_count_and_remote_map_results)
{
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;
	SnapshotData snapshot;
	union {
		uint64 align;
		unsigned char bytes[offsetof(ClusterMultiXactMemberOverlayResult, members)
							+ sizeof(ClusterMultiXactMember)];
	} input;
	ClusterMultiXactMemberOverlayResult *overlay
		= (ClusterMultiXactMemberOverlayResult *)input.bytes;
	ClusterMultiXactKey key;

	memset(&snapshot, 0, sizeof(snapshot));
	memset(overlay, 0, sizeof(input.bytes));
	overlay->authoritative = true;
	overlay->member_count = 1;
	overlay->members[0].xid = FirstNormalTransactionId;
	overlay->members[0].status = MultiXactStatusForShare;
	memset(&request, 0, sizeof(request));
	request.overlay_in = overlay;
	request.snapshot = &snapshot;
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_RESOLVE_VISIBILITY,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ((int)result.visibility, (int)CLUSTER_VISIBILITY_VISIBLE);

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 3;
	key.multixact_id = 71;
	key.cluster_epoch = 19;
	memset(&request, 0, sizeof(request));
	request.key = &key;
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_GET_MEMBER_COUNT,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ((int)result.member_count, 1);

	memset(&request, 0, sizeof(request));
	request.snapshot = &snapshot;
	request.origin_slot = 3;
	request.mxid = 71;
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_REMOTE_XMAX_RESOLVE,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ((int)result.visibility, (int)CLUSTER_VISIBILITY_VISIBLE);
	UT_ASSERT(result.overlay_hit);
}

UT_TEST(t6_source_counter_ops_execute_only_after_admission)
{
	ClusterMultiXactSourceResult result;
	uint64 halfspace_before = cluster_multixact_get_mxid_halfspace_refuse_count();
	uint64 underivable_before = cluster_multixact_get_mxid_underivable_read_count();

	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_NOTE_HALFSPACE_REFUSE,
														NULL, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ((uint64)cluster_multixact_get_mxid_halfspace_refuse_count(), halfspace_before + 1);
	UT_ASSERT_EQ(admission_leave_count, 1);

	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_NOTE_UNDERIVABLE_READ,
														NULL, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ((uint64)cluster_multixact_get_mxid_underivable_read_count(),
				 underivable_before + 1);
}

UT_TEST(t7_generation_drift_keeps_fixed_result_canonical)
{
	ClusterMultiXactKey key;
	ClusterMultiXactMember member;
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 4;
	key.multixact_id = 72;
	key.cluster_epoch = 19;
	memset(&member, 0, sizeof(member));
	member.xid = FirstNormalTransactionId + 1;
	request = make_install_request(&key, &member);
	memset(&result, 0x7f, sizeof(result));
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, false);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT(!result.bool_value);
	UT_ASSERT_EQ((int)result.member_count, 0);
	UT_ASSERT_EQ((int)result.visibility, 0);
	UT_ASSERT(!result.overlay_hit);
	UT_ASSERT_EQ(admission_recheck_count, 1);
	UT_ASSERT_EQ(admission_leave_count, 1);
}

UT_TEST(t8_error_path_leaves_once_before_rethrow)
{
	ClusterMultiXactKey key;
	ClusterMultiXactMember member;
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;
	volatile bool caught = false;

	memset(&key, 0, sizeof(key));
	key.cluster_epoch = 19;
	memset(&member, 0, sizeof(member));
	member.xid = FirstNormalTransactionId;
	request = make_install_request(&key, &member);
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	fake_force_error = true;
	PG_TRY();
	{
		(void)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL, &request,
												&result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	fake_force_error = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(admission_leave_count, 1);
}

UT_TEST(t9_null_result_is_closed_after_balanced_admission)
{
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_NOTE_HALFSPACE_REFUSE,
														NULL, NULL),
				 (int)CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT_EQ(admission_enter_count, 1);
	UT_ASSERT_EQ(admission_recheck_count, 0);
	UT_ASSERT_EQ(admission_leave_count, 1);
}

UT_TEST(t10_recovery_projection_create_has_exact_postread)
{
	ClusterSideProjectionOperationV1 operation;
	MultiXactMember members[2];
	ClusterMultiXactKey key;
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;
	union {
		uint64 align;
		unsigned char bytes[offsetof(ClusterMultiXactMemberOverlayResult, members)
							+ 2 * sizeof(ClusterMultiXactMember)];
	} output;
	ClusterMultiXactMemberOverlayResult *overlay
		= (ClusterMultiXactMemberOverlayResult *)output.bytes;

	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	operation.action = CLUSTER_SIDE_PROJECTION_ACTION_CREATE;
	operation.normalized_info = XLOG_MULTIXACT_CREATE_ID;
	operation.multixact_id = 33;
	operation.member_offset = 71;
	operation.member_count = 2;
	memset(members, 0, sizeof(members));
	members[0].xid = 800;
	members[0].status = MultiXactStatusForKeyShare;
	members[1].xid = 816;
	members[1].status = MultiXactStatusForShare;
	UT_ASSERT(cluster_multixact_recovery_projection_apply(
		NULL, 0, 19, &operation, (const uint8 *)members, sizeof(members), 100, 200));
	UT_ASSERT(cluster_multixact_recovery_projection_verify(
		NULL, 0, 19, &operation, (const uint8 *)members, sizeof(members), 100, 200));
	UT_ASSERT(!cluster_multixact_recovery_projection_verify(
		NULL, 0, 19, &operation, (const uint8 *)members, sizeof(members), 100, 201));
	/* The immediate recovery post-read may inspect the row, but ordinary
	 * serving has no retained-source freshness carrier.  RFSIDE-V2-A keeps
	 * this projection unserved rather than treating its bytes as truth. */
	memset(&key, 0, sizeof(key));
	key.origin_node_id = 0;
	key.multixact_id = 33;
	key.cluster_epoch = 19;
	memset(&request, 0, sizeof(request));
	memset(overlay, 0, sizeof(output.bytes));
	request.key = &key;
	request.overlay_out = overlay;
	request.max_members_buf = 2;
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_LOOKUP,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!result.bool_value);
	UT_ASSERT(!overlay->authoritative);
	memset(&operation, 0, sizeof(operation));
	operation.kind = CLUSTER_SIDE_PROJECTION_MULTIXACT;
	operation.action = CLUSTER_SIDE_PROJECTION_ACTION_ZERO_PAGE;
	operation.normalized_info = XLOG_MULTIXACT_ZERO_OFF_PAGE;
	operation.page_number = 0;
	UT_ASSERT(
		cluster_multixact_recovery_projection_apply(NULL, 0, 19, &operation, NULL, 0, 201, 202));
	UT_ASSERT(
		cluster_multixact_recovery_projection_verify(NULL, 0, 19, &operation, NULL, 0, 201, 202));
}

UT_TEST(t11_remote_visibility_follows_current_semantic_side)
{
	ClusterMultiXactKey key;
	ClusterMultiXactMember member;
	ClusterMultiXactSourceRequest request;
	ClusterMultiXactSourceResult result;
	SnapshotData snapshot;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 3;
	key.multixact_id = 91;
	key.cluster_epoch = 19;
	memset(&member, 0, sizeof(member));
	member.xid = FirstNormalTransactionId + 9;
	member.status = MultiXactStatusForShare;
	request = make_install_request(&key, &member);
	reset_admission(CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_source_dispatch(CLUSTER_MULTI_SOURCE_OVERLAY_INSTALL,
														&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);

	memset(&snapshot, 0, sizeof(snapshot));
	memset(&request, 0, sizeof(request));
	request.snapshot = &snapshot;
	request.origin_slot = 3;
	request.mxid = 91;

	/* OPEN R4 owns the frozen reader operation through TARGET admission. */
	reset_admission_sides(CLUSTER_SEMANTIC_ADMISSION_OK, CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT,
						  true);
	UT_ASSERT_EQ((int)cluster_multixact_remote_xmax_visibility_dispatch(&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(admission_enter_count, 1);
	UT_ASSERT_EQ((int)admission_side, (int)CLUSTER_SEMANTIC_TARGET_SIDE);
	UT_ASSERT_EQ(admission_recheck_count, 1);
	UT_ASSERT_EQ(admission_leave_count, 1);
	UT_ASSERT_EQ((int)result.visibility, (int)CLUSTER_VISIBILITY_VISIBLE);
	UT_ASSERT(result.overlay_hit);

	/* Before activation, exact TARGET_DISABLED may fall back to SOURCE. */
	reset_admission_sides(CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED, CLUSTER_SEMANTIC_ADMISSION_OK,
						  true);
	UT_ASSERT_EQ((int)cluster_multixact_remote_xmax_visibility_dispatch(&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(admission_enter_count, 2);
	UT_ASSERT_EQ((int)admission_side, (int)CLUSTER_SEMANTIC_SOURCE_SIDE);
	UT_ASSERT_EQ(admission_recheck_count, 1);
	UT_ASSERT_EQ(admission_leave_count, 1);
	UT_ASSERT_EQ((int)result.visibility, (int)CLUSTER_VISIBILITY_VISIBLE);

	/* Generation drift is not a license to cross the cutover boundary. */
	memset(&result, 0x7f, sizeof(result));
	reset_admission_sides(CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED,
						  CLUSTER_SEMANTIC_ADMISSION_OK, true);
	UT_ASSERT_EQ((int)cluster_multixact_remote_xmax_visibility_dispatch(&request, &result),
				 (int)CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT_EQ(admission_enter_count, 1);
	UT_ASSERT_EQ(admission_recheck_count, 0);
	UT_ASSERT_EQ(admission_leave_count, 0);
	UT_ASSERT_EQ((int)result.visibility, 0);
	UT_ASSERT(!result.overlay_hit);
}

int
main(void)
{
	memset(&fake_state, 0, sizeof(fake_state));
	memset(&fake_lock, 0, sizeof(fake_lock));
	memset(&fake_hash_entry, 0, sizeof(fake_hash_entry));
	cluster_multixact_shmem_init();

	UT_PLAN(11);
	UT_RUN(t1_frozen_dispatch_surface);
	UT_RUN(t2_dormant_refuses_before_request_and_mutation);
	UT_RUN(t3_invalid_after_admission_closes_and_leaves);
	UT_RUN(t4_source_install_and_lookup_are_positive);
	UT_RUN(t5_source_visibility_count_and_remote_map_results);
	UT_RUN(t6_source_counter_ops_execute_only_after_admission);
	UT_RUN(t7_generation_drift_keeps_fixed_result_canonical);
	UT_RUN(t8_error_path_leaves_once_before_rethrow);
	UT_RUN(t9_null_result_is_closed_after_balanced_admission);
	UT_RUN(t10_recovery_projection_create_has_exact_postread);
	UT_RUN(t11_remote_visibility_follows_current_semantic_side);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
