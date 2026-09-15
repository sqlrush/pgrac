/*-------------------------------------------------------------------------
 *
 * test_cluster_multixact_current.c
 *	  ABI and public-symbol gates for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_multixact_current.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <stdio.h>

#include "access/htup_details.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/cluster_write_fence.h"
#include "storage/lock.h"
#include "utils/memutils.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"


UT_DEFINE_GLOBALS();

static bool stop_new_read_allowed = true;
static int stop_new_read_calls;
static int stop_allocation_calls;

bool
cluster_normal_stop_service_new_work(bool modifies_data)
{
	Assert(!modifies_data);
	stop_new_read_calls++;
	return stop_new_read_allowed;
}

static char test_memory_context_storage;
MemoryContext TopMemoryContext = (MemoryContext)&test_memory_context_storage;
MemoryContext CurrentMemoryContext = (MemoryContext)&test_memory_context_storage;
sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

pg_attribute_noreturn() void
pg_re_throw(void)
{
	abort();
}

MemoryContext
AllocSetContextCreateInternal(MemoryContext parent pg_attribute_unused(),
							  const char *name pg_attribute_unused(),
							  Size min_context_size pg_attribute_unused(),
							  Size init_block_size pg_attribute_unused(),
							  Size max_block_size pg_attribute_unused())
{
	return (MemoryContext)&test_memory_context_storage;
}

void
MemoryContextReset(MemoryContext context pg_attribute_unused())
{}

void
FlushErrorState(void)
{}

void *
palloc0(Size size)
{
	stop_allocation_calls++;
	return calloc(1, size);
}


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel,
	const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
	const char *funcname pg_attribute_unused())
{
	abort();
}


/*
 * Runtime seams for the descriptor-authority routing test.  They keep this
 * unit binary independent of shmem, pg_multixact SLRU, and DATA transport.
 */
int cluster_node_id = -1;
BackendId MyBackendId = 7;
int cluster_lms_workers = 1;
int cluster_subtrans_max_chain_depth = 32;
int cluster_gcs_reply_timeout_ms = 1000;
static uint64 test_runtime_epoch;
static int test_runtime_mxid_origin = -1;
static bool test_runtime_mxid_mine;
static int test_runtime_native_describe_calls;
static int test_runtime_native_describe_count = 2;
static int test_runtime_remote_describe_calls;
static bool test_runtime_remote_describe_ok;
static int test_runtime_proof_calls;
static int test_runtime_proof_fail_call;
static int test_runtime_origin_proof_source_calls;
static bool test_runtime_target_owner_enabled;
static bool test_runtime_local_grant_publishable = true;
static int test_runtime_target_owner_calls;
static TransactionId test_runtime_target_owner_active_xid;
static TransactionId test_runtime_local_terminal_xid;
static ClusterTTStatus test_runtime_local_terminal_status;
static bool test_runtime_local_terminal_rolled;
static int test_runtime_local_terminal_calls;
static TransactionId test_runtime_self_xid;
static ClusterCurrentMemberState test_runtime_remote_member_state = CCM_ABORTED;
static int test_runtime_describe_send_calls;
static int test_runtime_describe_capability_calls;
static int test_runtime_describe_generation_match_calls;
static uint32 test_runtime_describe_dest;
static uint32 test_runtime_describe_payload_len;
static int test_runtime_materialize_calls;
static int test_runtime_materialize_nmembers;
static MultiXactMember test_runtime_materialize_member;
static uint8 test_runtime_describe_payload[
	sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE];

TimestampTz
GetCurrentTimestamp(void)
{
	return UINT64_C(1000000);
}

static void test_member(ClusterCurrentMxMemberDesc *member, TransactionId xid, uint8 status);
static void test_proof(ClusterCurrentMemberProof *proof,
					   const ClusterCurrentMxMemberDesc *member, uint16 ordinal,
					   ClusterCurrentMemberState state, uint16 origin, uint32 slot);

extern ClusterMxDescribeResult
cluster_cr_server_test_current_mx_build_describe_page(
	uint16 source_node_id, uint64 request_id, const ClusterCurrentMxKey *key,
	const MultiXactMember *native_members, int native_count,
	ClusterCurrentMxDescribeReplyPage *page);
extern ClusterMxResolveResult
cluster_cr_server_test_current_mx_build_proof_page(
	uint16 source_node_id, const ClusterCurrentMxProofForwardV2 *request,
	ClusterMxResolveResult result, uint32 requester_capability_generation,
	const ClusterCurrentMemberProof *proofs,
	uint16 proof_count, const ClusterCurrentUpdaterProof *updater_proof,
	ClusterCurrentMxProofReplyPage *page);
extern bool cluster_runtime_visibility_current_owner_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result);
extern bool cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result,
	uint32 *ctrc_grant_out);
MultiXactId cluster_multixact_test_materialize_local_current(
	int nmembers, MultiXactMember *members);

MultiXactId
cluster_multixact_test_materialize_local_current(int nmembers,
	MultiXactMember *members)
{
	test_runtime_materialize_calls++;
	test_runtime_materialize_nmembers = nmembers;
	if (nmembers == 1 && members != NULL)
		test_runtime_materialize_member = members[0];
	return (MultiXactId) 901;
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
cluster_multixact_current_stats_bump(ClusterCurrentMxStatId stat pg_attribute_unused())
{}

bool
cluster_gcs_block_family_on_data_plane(void)
{
	return true;
}

int
cluster_ic_tier1_my_data_channel(void)
{
	return 0;
}

int
cluster_lms_shard_for_tag(const BufferTag *tag pg_attribute_unused(), int n_workers)
{
	return n_workers > 0 ? 0 : -1;
}

bool
cluster_write_fence_enforcing(void)
{
	return false;
}

bool
cluster_write_fence_allowed(void)
{
	return true;
}

bool
cluster_sf_peer_multixact_current_capability_generation(
	int32 peer_id pg_attribute_unused(), uint32 *generation_out)
{
	test_runtime_describe_capability_calls++;
	if (generation_out != NULL)
		*generation_out = 61;
	return generation_out != NULL;
}

bool
cluster_sf_peer_capability_generation_matches(
	int32 peer_id pg_attribute_unused(), uint32 required_capabilities,
	uint32 expected_generation)
{
	test_runtime_describe_generation_match_calls++;
	return required_capabilities
			   == PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1
		   && expected_generation == 61;
}

uint32
cluster_gcs_block_compute_checksum(const char *data)
{
	uint32 hash = UINT32_C(2166136261);
	int i;

	for (i = 0; i < GCS_BLOCK_DATA_SIZE; i++) {
		hash ^= (uint8)data[i];
		hash *= UINT32_C(16777619);
	}
	return hash;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id,
						 const void *payload, uint32 payload_len)
{
	test_runtime_describe_send_calls++;
	test_runtime_describe_dest = (uint32)dest_node_id;
	test_runtime_describe_payload_len = payload_len;
	if (msg_type != PGRAC_IC_MSG_GCS_BLOCK_REPLY || payload == NULL
		|| payload_len > sizeof(test_runtime_describe_payload))
		return CLUSTER_IC_SEND_HARD_ERROR;
	memcpy(test_runtime_describe_payload, payload, payload_len);
	return CLUSTER_IC_SEND_DONE;
}

uint64
cluster_epoch_get_current(void)
{
	return test_runtime_epoch;
}

int
cluster_mxid_origin_slot(MultiXactId mxid pg_attribute_unused())
{
	return test_runtime_mxid_origin;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	return 4 + (int)(xid % 3);
}

bool
cluster_mxid_is_mine(MultiXactId mxid pg_attribute_unused())
{
	return test_runtime_mxid_mine;
}

int
GetMultiXactIdMembers(MultiXactId multi pg_attribute_unused(), MultiXactMember **members,
					  bool from_pgupgrade pg_attribute_unused(),
					  bool isLockOnly pg_attribute_unused())
{
	MultiXactMember *out;
	int count = test_runtime_native_describe_count;

	test_runtime_native_describe_calls++;
	if (members == NULL || count < 1 || count > 2)
		return -1;
	out = (MultiXactMember *)malloc((size_t)count * sizeof(*out));
	memset(out, 0, (size_t)count * sizeof(*out));
	out[0].xid = 100;
	out[0].status = MultiXactStatusForShare;
	if (count == 2)
	{
		out[1].xid = 101;
		out[1].status = MultiXactStatusNoKeyUpdate;
	}
	*members = out;
	return count;
}

ClusterMxDescribeResult
cluster_gcs_current_mx_describe_fetch_and_wait(
	int32 origin_node pg_attribute_unused(), const ClusterCurrentMxKey *key pg_attribute_unused(),
	ClusterCurrentMxMemberDesc *members pg_attribute_unused(),
	uint16 members_cap pg_attribute_unused(), uint16 *members_count pg_attribute_unused(),
	uint32 *reported_total_members pg_attribute_unused())
{
	test_runtime_remote_describe_calls++;
	if (!test_runtime_remote_describe_ok)
		return CMX_DESC_UNKNOWN;
	if (members == NULL || members_count == NULL || reported_total_members == NULL
		|| members_cap < 2)
		return CMX_DESC_UNKNOWN;
	memset(members, 0, 2 * sizeof(*members));
	members[0].xid = 200;
	members[0].member_status = MultiXactStatusForShare;
	members[1].xid = 201;
	members[1].member_status = MultiXactStatusNoKeyUpdate;
	*members_count = 2;
	*reported_total_members = 2;
	return CMX_DESC_OK;
}

ClusterMxResolveResult
cluster_gcs_current_mx_member_proof_fetch_and_wait(
	int32 origin_node, ClusterCurrentMxProofForwardV2 *request,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out,
	TimestampTz deadline pg_attribute_unused())
{
	ClusterCurrentMxProofForwardV2 decoded;
	uint8 i;

	test_runtime_proof_calls++;
	if (proof_count != NULL)
		*proof_count = 0;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (requester_capability_generation_out != NULL)
		*requester_capability_generation_out = 0;
	if (test_runtime_proof_fail_call == test_runtime_proof_calls
		|| !cluster_multixact_current_wire_validate_proof_forward(
			request, sizeof(*request), request->prefix.original_requester_node, origin_node,
			request->prefix.epoch, &decoded))
		return CMX_RESOLVE_UNKNOWN;

	if (decoded.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		if (proofs == NULL || proof_count == NULL
			|| proofs_cap < decoded.prefix.entry_count)
			return CMX_RESOLVE_UNKNOWN;
		for (i = 0; i < decoded.prefix.entry_count; i++) {
			ClusterCurrentMxMemberDesc member;

			test_member(&member, decoded.trailer.body.asks[i].xid,
						decoded.trailer.body.asks[i].member_status);
			test_proof(&proofs[i], &member,
					   decoded.trailer.body.asks[i].member_ordinal,
					   test_runtime_remote_member_state,
					   (uint16)origin_node, 20 + i);
		}
		*proof_count = decoded.prefix.entry_count;
		*requester_capability_generation_out
			= (uint32)(100 + origin_node);
		return CMX_RESOLVE_OK;
	}

	if (proofs == NULL || proof_count == NULL || proofs_cap < 1 || updater_proof == NULL)
		return CMX_RESOLVE_UNKNOWN;
	{
		ClusterCurrentMxMemberDesc member;

		test_member(&member, decoded.trailer.body.updater.challenge.updater_xid,
					decoded.trailer.body.updater.challenge.member_status);
		test_proof(&proofs[0], &member,
				   decoded.trailer.body.updater.challenge.member_ordinal, CCM_COMMITTED,
				   (uint16)origin_node, 20);
	}
	*proof_count = 1;
	updater_proof->mxkey = decoded.prefix.mxkey;
	updater_proof->candidate_next_xmin_alias
		= decoded.trailer.body.updater.challenge.candidate_next_xmin_alias;
	updater_proof->candidate_next_xmin_locator
		= decoded.trailer.body.updater.challenge.candidate_next_xmin_locator;
	updater_proof->candidate_next_xmin_locator.tt_wrap = 3;
	updater_proof->updater_xid = decoded.trailer.body.updater.challenge.updater_xid;
	updater_proof->member_ordinal = decoded.trailer.body.updater.challenge.member_ordinal;
	updater_proof->verdict = CUCP_MATCH;
	*requester_capability_generation_out = (uint32)(100 + origin_node);
	return CMX_RESOLVE_OK;
}

bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	return TransactionIdIsNormal(test_runtime_self_xid)
		&& TransactionIdEquals(xid, test_runtime_self_xid);
}

bool
cluster_runtime_visibility_current_owner_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result)
{
	test_runtime_target_owner_calls++;
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(result);
	memset(key, 0, sizeof(*key));
	memset(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	result->commit_scn = InvalidScn;
	if (!test_runtime_target_owner_enabled)
		return false;
	if (TransactionIdIsNormal(test_runtime_target_owner_active_xid)
		&& !TransactionIdEquals(xid, test_runtime_target_owner_active_xid))
		return false;
	key->origin_node_id = (uint16)cluster_node_id;
	key->undo_segment_id = 2;
	key->tt_slot_id = (uint32)(xid % 8) + 1;
	key->cluster_epoch = (uint32)test_runtime_epoch;
	key->local_xid = xid;
	result->status = CLUSTER_TT_STATUS_IN_PROGRESS;
	result->status_epoch = (uint32)test_runtime_epoch;
	result->commit_scn = InvalidScn;
	result->authoritative = true;
	return true;
}

bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result,
	uint32 *ctrc_grant_out)
{
	bool found;

	if (ctrc_grant_out != NULL)
		*ctrc_grant_out = 0;
	found = cluster_runtime_visibility_current_owner_lookup_exact(
		xid, key, result);
	if (found && ctrc_grant_out != NULL
		&& result->status == CLUSTER_TT_STATUS_IN_PROGRESS)
		*ctrc_grant_out = 7;
	return found;
}

bool
cluster_runtime_visibility_local_terminal_lookup_exact(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result)
{
	test_runtime_local_terminal_calls++;
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(result);
	memset(key, 0, sizeof(*key));
	memset(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	result->commit_scn = InvalidScn;
	if (!TransactionIdIsNormal(test_runtime_local_terminal_xid)
		|| !TransactionIdEquals(xid, test_runtime_local_terminal_xid)
		|| (test_runtime_local_terminal_status != CLUSTER_TT_STATUS_COMMITTED
			&& test_runtime_local_terminal_status != CLUSTER_TT_STATUS_ABORTED))
		return false;
	key->origin_node_id = (uint16)cluster_node_id;
	key->undo_segment_id = test_runtime_local_terminal_rolled ? 9 : 2;
	key->tt_slot_id = (uint32)(xid % 8) + 1;
	key->cluster_epoch = (uint32)test_runtime_epoch;
	key->local_xid = xid;
	result->status = test_runtime_local_terminal_status;
	result->status_epoch = (uint32)test_runtime_epoch;
	result->commit_scn = test_runtime_local_terminal_status
		== CLUSTER_TT_STATUS_COMMITTED ? UINT64_C(901) : InvalidScn;
	result->authoritative = true;
	return true;
}

bool
cluster_runtime_visibility_current_mx_updater_provenance_exact(
	const ClusterTxLocator *locator, TimestampTz deadline pg_attribute_unused(),
	ClusterTTStatusKey *key, ClusterTTStatusResult *result,
	uint32 *ctrc_grant_out,
	uint32 *participant_capability_generation_out,
	ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterTxLocator *canonical_locator_out,
	bool *cross_segment_out)
{
	uint32 data_segment;
	uint32 block_no;
	uint16 tt_slot_offset;
	uint16 row_offset;

	UT_ASSERT_NOT_NULL(locator);
	UT_ASSERT_NOT_NULL(ctrc_grant_out);
	UT_ASSERT_NOT_NULL(participant_capability_generation_out);
	UT_ASSERT_NOT_NULL(ctrc_key_out);
	UT_ASSERT_NOT_NULL(canonical_locator_out);
	UT_ASSERT_NOT_NULL(cross_segment_out);
	*ctrc_grant_out = 0;
	*participant_capability_generation_out = 0;
	memset(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	*canonical_locator_out = *locator;
	canonical_locator_out->tt_wrap = 3;
	*cross_segment_out = false;
	if (!uba_decode(locator->uba, &data_segment, &block_no,
					&tt_slot_offset, &row_offset))
		return false;
	if (cluster_runtime_visibility_local_terminal_lookup_exact(
			locator->xid, key, result)) {
		*cross_segment_out = key->undo_segment_id != data_segment;
		return true;
	}
	if (!cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
			locator->xid, key, result, ctrc_grant_out))
		return false;
	ctrc_key_out->format_version = CLUSTER_CTRC_FORMAT_VERSION;
	ctrc_key_out->owner_instance = (uint8)(key->origin_node_id + 1);
	ctrc_key_out->origin_node_id = key->origin_node_id;
	ctrc_key_out->segment_id = key->undo_segment_id;
	ctrc_key_out->segment_generation = 17;
	ctrc_key_out->slot_offset = cluster_tt_slot_id_to_offset(key->tt_slot_id);
	ctrc_key_out->slot_wrap = 3;
	ctrc_key_out->xid = key->local_xid;
	ctrc_key_out->cluster_epoch = key->cluster_epoch;
	*participant_capability_generation_out = 55;
	*cross_segment_out = key->undo_segment_id != data_segment;
	return true;
}

bool
cluster_runtime_visibility_current_owner_lookup_exact_ctrc_full(
	TransactionId xid, ClusterTTStatusKey *key, ClusterTTStatusResult *result,
	uint32 *ctrc_grant_out, ClusterCtrcTxnKeyV1 *ctrc_key_out,
	ClusterCtrcParticipantIdentity *participant_out)
{
	bool found;

	UT_ASSERT_NOT_NULL(ctrc_key_out);
	UT_ASSERT_NOT_NULL(participant_out);
	memset(ctrc_key_out, 0, sizeof(*ctrc_key_out));
	memset(participant_out, 0, sizeof(*participant_out));
	found = cluster_runtime_visibility_current_owner_lookup_exact_ctrc(
		xid, key, result, ctrc_grant_out);
	if (found && ctrc_grant_out != NULL && *ctrc_grant_out != 0)
	{
		ctrc_key_out->format_version = CLUSTER_CTRC_FORMAT_VERSION;
		ctrc_key_out->owner_instance = (uint8)(key->origin_node_id + 1);
		ctrc_key_out->origin_node_id = key->origin_node_id;
		ctrc_key_out->segment_id = key->undo_segment_id;
		ctrc_key_out->segment_generation = 17;
		ctrc_key_out->slot_offset
			= cluster_tt_slot_id_to_offset(key->tt_slot_id);
		ctrc_key_out->slot_wrap = 3;
		ctrc_key_out->xid = key->local_xid;
		ctrc_key_out->cluster_epoch = key->cluster_epoch;
		participant_out->node_id = (uint16)cluster_node_id;
		participant_out->capability_record_generation = 55;
		participant_out->boot_incarnation = 9;
		participant_out->formation_epoch = test_runtime_epoch;
		participant_out->admission_record_generation = 77;
	}
	return found;
}

bool
cluster_ctrc_origin_grant_publishable(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	uint32 grant_generation)
{
	UT_ASSERT_NOT_NULL(key);
	UT_ASSERT_NOT_NULL(participant);
	UT_ASSERT(grant_generation != 0);
	return test_runtime_local_grant_publishable;
}

ClusterSemanticAdmissionResult
cluster_tt_status_source_dispatch(ClusterTTStatusSourceOp op,
								  const ClusterTTStatusSourceRequest *request,
								  ClusterTTStatusSourceResult *result)
{
	(void)op;
	(void)request;
	test_runtime_origin_proof_source_calls++;
	if (result != NULL)
		memset(result, 0, sizeof(*result));
	return CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT;
}


/*
 * Standalone unit reference for PostgreSQL's LockConflicts[] source.  The
 * production object resolves this symbol to lock.c.
 */
bool
DoLockModesConflict(LOCKMODE mode1, LOCKMODE mode2)
{
	switch (mode1) {
	case AccessShareLock:
		return mode2 == AccessExclusiveLock;
	case RowShareLock:
		return mode2 == ExclusiveLock || mode2 == AccessExclusiveLock;
	case ExclusiveLock:
		return mode2 == RowShareLock || mode2 == ExclusiveLock || mode2 == AccessExclusiveLock;
	case AccessExclusiveLock:
		return true;
	default:
		return true;
	}
}


bool
TransactionIdPrecedes(TransactionId id1, TransactionId id2)
{
	return NormalTransactionIdPrecedes(id1, id2);
}


/*
 * Compile-time ABI gates.  Wire structs keep explicit reserved fields so
 * every receiver can reject nonzero future semantics.
 */
StaticAssertDecl(sizeof(ClusterCurrentMxKey) == 16, "current MX key must remain 16 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, origin_node_id) == 0,
				 "current MX key origin offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, reserved16) == 2,
				 "current MX key reserved16 offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, multixact_id) == 4,
				 "current MX key mxid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, cluster_epoch) == 8,
				 "current MX key epoch offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxKey, reserved32) == 12,
				 "current MX key reserved32 offset changed");

StaticAssertDecl(sizeof(ClusterCurrentMxMemberDesc) == 8,
				 "current MX descriptor entry must remain 8 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, xid) == 0,
				 "current MX descriptor xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, member_status) == 4,
				 "current MX descriptor status offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMxMemberDesc, reserved8) == 5,
				 "current MX descriptor reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentMemberProof) == 48,
				 "current MX member proof must remain 48 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, key) == 0,
				 "current MX proof key offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, commit_scn) == 24,
				 "current MX proof commit SCN offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_xid) == 32,
				 "current MX proof xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_ordinal) == 36,
				 "current MX proof ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, member_status) == 38,
				 "current MX proof status offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, state) == 39,
				 "current MX proof state offset changed");
StaticAssertDecl(offsetof(ClusterCurrentMemberProof, reserved8) == 40,
				 "current MX proof reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentMxSuccessorAlias) == 24,
				 "current MX successor alias must remain 24 bytes");
StaticAssertDecl(offsetof(ClusterCurrentMxSuccessorAlias, origin_node_id) == 0
				 && offsetof(ClusterCurrentMxSuccessorAlias,
							 undo_record_segment_id) == 2
				 && offsetof(ClusterCurrentMxSuccessorAlias, tt_slot_id) == 4
				 && offsetof(ClusterCurrentMxSuccessorAlias, cluster_epoch) == 8
				 && offsetof(ClusterCurrentMxSuccessorAlias, local_xid) == 12
				 && offsetof(ClusterCurrentMxSuccessorAlias, reserved32) == 16
				 && offsetof(ClusterCurrentMxSuccessorAlias, reserved32_2) == 20,
				 "current MX successor alias offsets changed");

StaticAssertDecl(sizeof(ClusterCurrentUpdaterProof) == 72,
				 "current MX updater proof must remain 72 bytes");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, mxkey) == 0,
				 "current MX updater proof mxkey offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof,
						 candidate_next_xmin_alias) == 16,
				 "current MX updater proof alias offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof,
						 candidate_next_xmin_locator) == 40,
				 "current MX updater proof locator offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, updater_xid) == 64,
				 "current MX updater proof xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, member_ordinal) == 68,
				 "current MX updater proof ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, verdict) == 70,
				 "current MX updater proof verdict offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterProof, reserved8) == 71,
				 "current MX updater proof reserved offset changed");

StaticAssertDecl(sizeof(ClusterCurrentUpdaterChallenge) == 56,
				 "current MX updater challenge must remain 56 bytes");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge,
						 candidate_next_xmin_alias) == 0,
				 "current MX updater challenge alias offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge,
						 candidate_next_xmin_locator) == 24,
				 "current MX updater challenge locator offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, updater_xid) == 48,
				 "current MX updater challenge xid offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, member_ordinal) == 52,
				 "current MX updater challenge ordinal offset changed");
StaticAssertDecl(offsetof(ClusterCurrentUpdaterChallenge, reserved16) == 54,
				 "current MX updater challenge reserved offset changed");

StaticAssertDecl(CLUSTER_CURRENT_MX_MAX_MEMBERS == 256,
				 "current MX positive-path member limit changed");
StaticAssertDecl(CLUSTER_CURRENT_MX_MAX_CHUNKS == 256
					 && CLUSTER_CURRENT_MX_MAX_PROOFS_PER_CHUNK == 32,
				 "current MX proof chunk limits changed");
StaticAssertDecl(CCM_SELF == 0 && CCM_ACTIVE == 1 && CCM_COMMITTED == 2 && CCM_ABORTED == 3
					 && CCM_UNKNOWN == 4,
				 "current MX member-state values changed");
StaticAssertDecl(CMX_DESC_OK == 0 && CMX_DESC_DENIED == 1 && CMX_DESC_SUPPORTED_LIMIT == 2
					 && CMX_DESC_TIMEOUT == 3 && CMX_DESC_UNKNOWN == 4,
				 "current MX describe-result endpoints changed");
StaticAssertDecl(CMX_RESOLVE_OK == 0 && CMX_RESOLVE_DENIED == 1 && CMX_RESOLVE_SUPPORTED_LIMIT == 2
					 && CMX_RESOLVE_TIMEOUT == 3 && CMX_RESOLVE_UNKNOWN == 4
					 && CMX_RESOLVE_RETRY == 5,
				 "current MX resolve-result endpoints changed");
StaticAssertDecl(CCM_ACTION_UPDATE == 0 && CCM_ACTION_DELETE == 1 && CCM_ACTION_LOCK == 2
					 && CCM_ACTION_HOT_FOLLOW == 3,
				 "current MX action values changed");
StaticAssertDecl(CCM_SHAPE_LOCK_ONLY == 0 && CCM_SHAPE_UPDATED == 1 && CCM_SHAPE_DELETED == 2,
				 "current MX tuple-shape values changed");
StaticAssertDecl(CUCP_MATCH == 0 && CUCP_MISMATCH == 1 && CUCP_UNKNOWN == 2,
				 "current MX updater verdict values changed");
StaticAssertDecl(CMDL_CONTINUE == 0 && CMDL_INVISIBLE == 1 && CMDL_SELF_MODIFIED == 2
					 && CMDL_BEING_MODIFIED == 3 && CMDL_WAIT_MEMBER == 4 && CMDL_WOULD_BLOCK == 5
					 && CMDL_LOCK_NOT_AVAILABLE == 6 && CMDL_UPDATED == 7 && CMDL_DELETED == 8
					 && CMDL_UNKNOWN == 9,
				 "current MX decision values changed");
StaticAssertDecl(GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF == 7
					 && GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS == 8
					 && GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE == 9,
				 "current MX request kinds must occupy the approved post-R4 domain");
StaticAssertDecl(GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT == 27
					 && GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT == 28
					 && GCS_BLOCK_REPLY_CURRENT_MX_STATS_RESULT == 29,
				 "current MX reply statuses must occupy the approved post-R4 domain");
StaticAssertDecl(GCS_BLOCK_REPLY_R4_DENIED < GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT,
				 "current MX replies must not alias the closed R4 reply domain");


UT_TEST(test_current_multixact_public_symbols_link)
{
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_descriptor);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_proof_set);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_descriptor_hash);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_validate_updater_proof);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_decide);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_describe);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_members_resolve);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_recompose);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_forward);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_build_proof_requests);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_reply);
	UT_ASSERT_NOT_NULL(cluster_multixact_current_wire_validate_proof_reply_frame);
	UT_ASSERT_EQ(CLUSTER_CURRENT_MX_WIRE_VERSION, 5);
}


UT_TEST(test_current_multixact_router_domain_binding)
{
	GcsBlockForwardPayload forward;

	memset(&forward, 0, sizeof(forward));
	forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	UT_ASSERT(GcsBlockForwardPayloadIsCurrentMxMemberProof(&forward));
	UT_ASSERT(GcsBlockForwardPayloadIsCurrentMxRuntime(&forward));
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxDescribe(&forward));

	forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	UT_ASSERT(GcsBlockForwardPayloadIsCurrentMxDescribe(&forward));
	UT_ASSERT(GcsBlockForwardPayloadIsCurrentMxRuntime(&forward));
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxMemberProof(&forward));

	forward.reserved_0[6] = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_STATS;
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxRuntime(&forward));
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxDescribe(&forward));
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxMemberProof(&forward));

	forward.reserved_0[6] = CLUSTER_R4_FORWARD_EXTENDED;
	UT_ASSERT(!GcsBlockForwardPayloadIsCurrentMxRuntime(&forward));
}

static ClusterCurrentMxKey
test_mxkey(void)
{
	ClusterCurrentMxKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 2;
	key.multixact_id = 17;
	key.cluster_epoch = 9;
	return key;
}


static ClusterTTStatusKey
test_ttkey(uint16 origin, TransactionId xid, uint32 slot)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = origin;
	key.undo_segment_id = 1;
	key.tt_slot_id = slot;
	key.cluster_epoch = 9;
	key.local_xid = xid;
	return key;
}

static void
test_updater_challenge(ClusterCurrentUpdaterChallenge *challenge,
					  uint16 origin, TransactionId xid, uint32 slot)
{
	uint32 data_segment
		= (uint32)origin * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;

	UT_ASSERT(slot > 0 && slot <= TT_SLOTS_PER_SEGMENT);
	memset(challenge, 0, sizeof(*challenge));
	challenge->candidate_next_xmin_alias.origin_node_id = origin;
	challenge->candidate_next_xmin_alias.undo_record_segment_id
		= (uint16)data_segment;
	challenge->candidate_next_xmin_alias.tt_slot_id = slot;
	challenge->candidate_next_xmin_alias.cluster_epoch = 9;
	challenge->candidate_next_xmin_alias.local_xid = xid;
	challenge->candidate_next_xmin_locator.uba
		= uba_encode(data_segment, 2, (uint16)(slot - 1), 0);
	challenge->candidate_next_xmin_locator.xid = xid;
	challenge->candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;
	challenge->candidate_next_xmin_locator.itl_kind = ITL_FLAG_ACTIVE;
	challenge->candidate_next_xmin_locator.itl_slot_index = 0;
	challenge->updater_xid = xid;
}


static void
test_member(ClusterCurrentMxMemberDesc *member, TransactionId xid, uint8 status)
{
	memset(member, 0, sizeof(*member));
	member->xid = xid;
	member->member_status = status;
}

static void
test_proof_request(ClusterCurrentMxProofForwardV2 *request, uint8 body_kind, uint8 entry_count)
{
	ClusterCurrentMxKey key = test_mxkey();
	uint8 i;

	memset(request, 0, sizeof(*request));
	request->prefix.request_id = 700;
	request->prefix.epoch = 9;
	request->prefix.mxkey = key;
	request->prefix.original_requester_node = 3;
	request->prefix.requester_backend_id = 7;
	request->prefix.total_count = 8;
	request->prefix.chunk_count_minus_one = 2;
	request->prefix.entry_count = entry_count;
	request->prefix.body_kind = body_kind;
	request->prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&request->prefix,
											 UINT64CONST(0x8877665544332211));
	request->trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request->trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	if (body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		for (i = 0; i < entry_count; i++) {
			request->trailer.body.asks[i].xid = (TransactionId)(100 + 3 * i);
			request->trailer.body.asks[i].member_ordinal = i;
			request->trailer.body.asks[i].member_status = MultiXactStatusForShare;
		}
	} else {
		ClusterCurrentUpdaterChallenge challenge;

		test_updater_challenge(&challenge, 5, 100, 20);
		request->trailer.body.updater.challenge.candidate_next_xmin_alias
			= challenge.candidate_next_xmin_alias;
		request->trailer.body.updater.challenge.candidate_next_xmin_locator
			= challenge.candidate_next_xmin_locator;
		request->trailer.body.updater.challenge.updater_xid
			= challenge.updater_xid;
		request->trailer.body.updater.challenge.member_ordinal = 0;
		request->trailer.body.updater.challenge.member_status
			= MultiXactStatusNoKeyUpdate;
	}
}


static void
test_proof(ClusterCurrentMemberProof *proof, const ClusterCurrentMxMemberDesc *member,
		   uint16 ordinal, ClusterCurrentMemberState state, uint16 origin, uint32 slot)
{
	ClusterTTStatusKey status_key;

	memset(proof, 0, sizeof(*proof));
	proof->member_xid = member->xid;
	proof->member_ordinal = ordinal;
	proof->member_status = member->member_status;
	proof->state = state;

	if (state == CCM_SELF || state == CCM_ACTIVE) {
		status_key = test_ttkey(origin, member->xid, slot);
		proof->key.origin_node_id = status_key.origin_node_id;
		proof->key.undo_segment_id = status_key.undo_segment_id;
		proof->key.tt_slot_id = status_key.tt_slot_id;
		proof->key.cluster_epoch = status_key.cluster_epoch;
		proof->key.local_xid = status_key.local_xid;
		ClusterCurrentMemberProofSetCtrcBinding(proof, 17, 3);
		proof->reserved8[0] = 7;
	} else if (state == CCM_COMMITTED)
		proof->commit_scn = 100 + ordinal;
}


typedef struct TestExactLookupEntry {
	ClusterTTStatusKey key;
	ClusterTTStatusResult result;
} TestExactLookupEntry;


typedef struct TestExactLookupTable {
	TestExactLookupEntry entries[4];
	uint16 count;
	uint16 calls;
} TestExactLookupTable;


static bool
test_exact_lookup(const ClusterTTStatusKey *key, ClusterTTStatusResult *result, void *arg)
{
	TestExactLookupTable *table = (TestExactLookupTable *)arg;
	uint16 i;

	table->calls++;
	for (i = 0; i < table->count; i++)
		if (memcmp(key, &table->entries[i].key, sizeof(*key)) == 0) {
			*result = table->entries[i].result;
			return true;
		}
	memset(result, 0, sizeof(*result));
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	return false;
}


UT_TEST(test_current_multixact_descriptor_validation)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];

	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);

	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(cluster_multixact_current_descriptor_hash(&key, members, 2),
				 UINT64CONST(3535096824512523990));
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 1, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 10, members, 2, 2),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, NULL, 0, 0),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 1, 1),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, NULL, 0, 257),
				 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 257),
				 CMX_DESC_DENIED);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 3),
				 CMX_DESC_DENIED);

	members[1].xid = members[0].xid;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[1].xid = 101;

	members[1].member_status = MaxMultiXactStatus + 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[1].member_status = MultiXactStatusNoKeyUpdate;

	members[0].member_status = MultiXactStatusUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[0].member_status = MultiXactStatusForShare;

	members[0].reserved8[2] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	members[0].reserved8[2] = 0;

	key.reserved32 = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members, 2, 2),
				 CMX_DESC_DENIED);
	key = test_mxkey();
	key.origin_node_id = 200;
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 200, 9, members, 2, 2),
				 CMX_DESC_DENIED);
}


UT_TEST(test_current_multixact_descriptor_accepts_cap_and_hashes_order)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint64 hash;
	uint16 i;

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++)
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForKeyShare);

	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(&key, 2, 9, members,
															   CLUSTER_CURRENT_MX_MAX_MEMBERS,
															   CLUSTER_CURRENT_MX_MAX_MEMBERS),
				 CMX_DESC_OK);

	hash = cluster_multixact_current_descriptor_hash(&key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS);
	UT_ASSERT_NE(hash, 0);
	UT_ASSERT_EQ(hash, cluster_multixact_current_descriptor_hash(&key, members,
																 CLUSTER_CURRENT_MX_MAX_MEMBERS));

	{
		ClusterCurrentMxMemberDesc tmp = members[0];

		members[0] = members[1];
		members[1] = tmp;
	}
	UT_ASSERT_NE(hash, cluster_multixact_current_descriptor_hash(&key, members,
																 CLUSTER_CURRENT_MX_MAX_MEMBERS));
}


UT_TEST(test_current_multixact_proof_binding_and_order)
{
	struct {
		ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
		uint64 canary;
		uint8 guard[64];
	} capped_output;
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proof01[2];
	ClusterCurrentMemberProof proof2[1];
	ClusterCurrentMemberProof ordered[3];
	uint32 proof_capability_generations[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentProofChunkView chunks[3];
	ClusterCurrentUpdaterProof limit_updater_proof;
	uint16 member_origins[3] = { 4, 5, 6 };
	uint64 hash;

	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_member(&members[2], 102, MultiXactStatusForKeyShare);
	hash = cluster_multixact_current_descriptor_hash(&key, members, 3);

	test_proof(&proof01[0], &members[0], 0, CCM_ACTIVE, 4, 20);
	test_proof(&proof01[1], &members[1], 1, CCM_COMMITTED, 5, 21);
	test_proof(&proof2[0], &members[2], 2, CCM_ABORTED, 6, 22);

	memset(chunks, 0, sizeof(chunks));
	chunks[0].request_id = 77;
	chunks[0].mxkey = key;
	chunks[0].descriptor_hash = hash;
	chunks[0].total_count = 3;
	chunks[0].source_node_id = 4;
	chunks[0].chunk_ordinal = 0;
	chunks[0].chunk_count = 3;
	chunks[0].proof_count = 1;
	chunks[0].proofs = proof01;
	chunks[1] = chunks[0];
	chunks[1].source_node_id = 5;
	chunks[1].chunk_ordinal = 1;
	chunks[1].proof_count = 1;
	chunks[1].proofs = &proof01[1];
	chunks[2] = chunks[0];
	chunks[2].source_node_id = 6;
	chunks[2].chunk_ordinal = 2;
	chunks[2].proof_count = 1;
	chunks[2].proofs = proof2;

	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(ordered[0].member_xid, 100);
	UT_ASSERT_EQ(ordered[1].member_xid, 101);
	UT_ASSERT_EQ(ordered[2].member_xid, 102);

	memset(proof01[0].reserved8, 0, sizeof(proof01[0].reserved8));
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(
					 &key, members, member_origins, 3, 77, hash, chunks, 3,
					 ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[0].reserved8[0] = 7;
	proof01[1].reserved8[0] = 7;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(
					 &key, members, member_origins, 3, 77, hash, chunks, 3,
					 ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[1].reserved8[0] = 0;

	chunks[0].request_id++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].member_xid, InvalidTransactionId);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);

	memset(&capped_output, 0xa5, sizeof(capped_output));
	capped_output.canary = UINT64CONST(0x1122334455667788);
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(
					 &key, members, member_origins, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, 77, hash,
					 chunks, 3, capped_output.proofs),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(capped_output.canary, UINT64CONST(0x1122334455667788));

	memset(&capped_output, 0xa5, sizeof(capped_output));
	capped_output.canary = UINT64CONST(0x8877665544332211);
	memset(&limit_updater_proof, 0, sizeof(limit_updater_proof));
	limit_updater_proof.verdict = CUCP_MATCH;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, hash, NULL,
					 capped_output.proofs, &limit_updater_proof,
					 proof_capability_generations),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(capped_output.canary, UINT64CONST(0x8877665544332211));
	UT_ASSERT_EQ(limit_updater_proof.verdict, CUCP_UNKNOWN);
	chunks[0].request_id--;

	chunks[0].descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[0].descriptor_hash--;

	chunks[0].mxkey.multixact_id++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[0].mxkey.multixact_id--;

	chunks[1].total_count++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].total_count--;

	chunks[1].chunk_count++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].chunk_count--;

	chunks[1].source_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].source_node_id = 5;

	chunks[1].chunk_ordinal = 0;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	chunks[1].chunk_ordinal = 1;

	proof2[0].member_ordinal = 1;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof2[0].member_ordinal = 2;

	proof01[1].member_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[1].member_xid--;

	proof01[1].member_status = MultiXactStatusUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	proof01[1].member_status = MultiXactStatusNoKeyUpdate;

	proof01[0].key.local_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_validate_proof_set(&key, members, member_origins, 3, 77,
															  hash, chunks, 3, ordered),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);

	memset(ordered, 0xff, sizeof(ordered));
	UT_ASSERT_EQ(
		cluster_multixact_current_members_resolve(
			&key, members, 3, hash, NULL, ordered, NULL,
			proof_capability_generations),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(ordered[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[1].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(ordered[2].state, CCM_UNKNOWN);
}


UT_TEST(test_current_multixact_origin_member_proof_follows_exact_parent)
{
	ClusterTTStatusKey child_key = test_ttkey(5, 100, 20);
	ClusterTTStatusKey parent_key = test_ttkey(5, 90, 21);
	ClusterTTStatusResult initial;
	ClusterCurrentMemberProof proof;
	TestExactLookupTable table;

	memset(&initial, 0, sizeof(initial));
	initial.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	initial.authoritative = true;
	initial.has_parent_key = true;
	initial.parent_key = parent_key;
	initial.status_epoch = 9;

	memset(&table, 0, sizeof(table));
	table.entries[0].key = parent_key;
	table.entries[0].result.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	table.entries[0].result.authoritative = true;
	table.entries[0].result.status_epoch = 9;
	table.count = 1;

	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.member_xid, 100);
	UT_ASSERT_EQ(proof.member_ordinal, 3);
	UT_ASSERT_EQ(proof.key.local_xid, 90);
	UT_ASSERT_EQ(table.calls, 1);

	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, true, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_SELF);

	table.entries[0].result.status = CLUSTER_TT_STATUS_COMMITTED;
	table.entries[0].result.commit_scn = 500;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_COMMITTED);
	UT_ASSERT_EQ(proof.commit_scn, 500);
	UT_ASSERT_EQ(proof.key.local_xid, InvalidTransactionId);

	table.entries[0].result.status = CLUSTER_TT_STATUS_ABORTED;
	table.entries[0].result.commit_scn = InvalidScn;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 true);
	UT_ASSERT_EQ(proof.state, CCM_ABORTED);

	table.entries[0].result.status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	table.entries[0].result.has_parent_key = true;
	table.entries[0].result.parent_key = child_key;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	UT_ASSERT_EQ(proof.state, CCM_UNKNOWN);

	initial.parent_key.origin_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_resolve_origin_member_proof(
					 100, MultiXactStatusForShare, 3, 5, 9, false, &child_key, &initial,
					 test_exact_lookup, &table, &proof),
				 false);
	UT_ASSERT_EQ(proof.state, CCM_UNKNOWN);
}


UT_TEST(test_current_multixact_updater_candidate_requires_current_exact_binding)
{
	ClusterTTStatusKey current = test_ttkey(5, 100, 5);
	ClusterTTStatusKey candidate = current;
	ClusterTTStatusKey selected;
	ClusterTTStatusResult selected_result;

	cluster_node_id = 5;
	test_runtime_epoch = 9;
	current.undo_segment_id = 2;
	candidate = current;
	test_runtime_target_owner_enabled = true;
	test_runtime_target_owner_calls = 0;
	test_runtime_origin_proof_source_calls = 0;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_MATCH);
	UT_ASSERT_EQ(memcmp(&selected, &current, sizeof(current)), 0);
	UT_ASSERT_EQ(test_runtime_target_owner_calls, 1);
	UT_ASSERT_EQ(test_runtime_origin_proof_source_calls, 0);

	candidate.tt_slot_id++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	UT_ASSERT_EQ(selected_result.status, CLUSTER_TT_STATUS_UNKNOWN);
	candidate = current;

	test_runtime_target_owner_enabled = false;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	test_runtime_target_owner_enabled = true;
	candidate.local_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	candidate = current;
	candidate.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);

	cluster_node_id = -1;
	test_runtime_target_owner_enabled = false;
}


/* MXA-T38: a page alias names the undo DATA record, not the canonical TT.
 * Without the exact successor locator and durable record edge, D != T is
 * incomplete evidence and must stay UNKNOWN rather than becoming a false
 * MISMATCH. */
UT_TEST(test_current_mx_updater_provenance_alias_canonical_matrix)
{
	ClusterTTStatusKey candidate = test_ttkey(5, 100, 5);
	ClusterTTStatusKey selected;
	ClusterTTStatusResult selected_result;

	cluster_node_id = 5;
	test_runtime_epoch = 9;
	test_runtime_target_owner_enabled = true;
	test_runtime_target_owner_active_xid = 100;
	candidate.undo_segment_id = 1;

	UT_ASSERT_EQ(cluster_multixact_current_updater_candidate_verdict(
					 &candidate, 100, 5, 9, &selected, &selected_result),
				 CUCP_UNKNOWN);
	UT_ASSERT_EQ(selected_result.status, CLUSTER_TT_STATUS_UNKNOWN);

	test_runtime_target_owner_active_xid = InvalidTransactionId;
	test_runtime_target_owner_enabled = false;
	cluster_node_id = -1;
}


UT_TEST(test_current_multixact_proof_forward_wire_binding)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofForwardV2 decoded;

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 7);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 true);
	UT_ASSERT_EQ(decoded.prefix.entry_count, 7);
	UT_ASSERT_EQ(ClusterCurrentMxProofPrefixGetDescriptorHash(&decoded.prefix),
				 UINT64CONST(0x8877665544332211));
	request.prefix.epoch = 0;
	request.prefix.mxkey.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 0, &decoded),
				 true);
	request.prefix.epoch = 9;
	request.prefix.mxkey.cluster_epoch = 9;

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) - 1, 3, 5, 9, &decoded),
				 false);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request) + 1, 3, 5, 9, &decoded),
				 false);
	request.trailer.body.asks[6].xid = 101;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 false);

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE, 1);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 true);
	request.trailer.body.updater.challenge.member_status = MultiXactStatusForShare;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
					 &request, sizeof(request), 3, 5, 9, &decoded),
				 false);
}


UT_TEST(test_current_multixact_proof_reply_wire_binding)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMxProofReplyPage page;
	ClusterCurrentMemberProof out[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
	ClusterCurrentUpdaterProof updater_out;
	ClusterMxResolveResult frame_result = CMX_RESOLVE_UNKNOWN;
	uint32 requester_capability_generation = 0;
	uint16 out_count = 0;
	uint8 i;

	test_proof_request(&request, CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS, 2);
	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	page.header.result = CMX_RESOLVE_OK;
	page.header.source_node_id = 5;
	page.header.request_id = request.prefix.request_id;
	page.header.mxkey = request.prefix.mxkey;
	page.header.descriptor_hash = ClusterCurrentMxProofPrefixGetDescriptorHash(&request.prefix);
	page.header.total_count = request.prefix.total_count;
	page.header.entry_count = request.prefix.entry_count;
	page.header.chunk_ordinal = request.prefix.chunk_ordinal;
	page.header.chunk_count_minus_one = request.prefix.chunk_count_minus_one;
	page.header.requester_capability_generation = 41;
	page.header.wire_length
		= sizeof(page.header) + page.header.entry_count * sizeof(ClusterCurrentMemberProof);
	for (i = 0; i < page.header.entry_count; i++) {
		ClusterCurrentMxMemberDesc member;

		test_member(&member, request.trailer.body.asks[i].xid,
					request.trailer.body.asks[i].member_status);
		test_proof(&page.body.proofs[i], &member,
				   request.trailer.body.asks[i].member_ordinal, CCM_ACTIVE, 5, 20 + i);
	}

	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply(
					 &page, sizeof(page), 5, 9, &request, out, lengthof(out), &out_count,
					 &updater_out, &requester_capability_generation),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(requester_capability_generation, (uint32)41);
	UT_ASSERT_EQ(out_count, 2);
	UT_ASSERT_EQ(out[1].member_xid, request.trailer.body.asks[1].xid);

	page.header.source_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_reply_frame(
					 &page, sizeof(page), 5, 9, &request, &frame_result, out, lengthof(out),
					 &out_count, &updater_out,
					 &requester_capability_generation),
				 false);
	UT_ASSERT_EQ(out_count, 0);
	UT_ASSERT_EQ(out[0].state, CCM_UNKNOWN);
}


UT_TEST(test_current_multixact_members_resolve_all_or_nothing)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proofs[3];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;
	uint32 proof_capability_generations[3];
	uint64 hash;
	uint16 i;

	cluster_node_id = 3;
	test_runtime_epoch = 9;
	test_member(&members[0], 100, MultiXactStatusForShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_member(&members[2], 102, MultiXactStatusForKeyShare);
	test_updater_challenge(
		&challenge, (uint16)cluster_xid_origin_slot(members[1].xid),
		members[1].xid, 20);
	challenge.member_ordinal = 1;
	hash = cluster_multixact_current_descriptor_hash(&key, members, lengthof(members));

	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 0;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, lengthof(members), hash, &challenge, proofs,
					 &updater_proof, proof_capability_generations),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(test_runtime_proof_calls, 3);
	UT_ASSERT_EQ(proofs[1].state, CCM_COMMITTED);
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_MATCH);

	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater_proof, 0xa5, sizeof(updater_proof));
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 2;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
					 &key, members, lengthof(members), hash, &challenge, proofs,
					 &updater_proof, proof_capability_generations),
				 CMX_RESOLVE_UNKNOWN);
	for (i = 0; i < lengthof(proofs); i++)
		UT_ASSERT_EQ(proofs[i].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_UNKNOWN);

	test_runtime_proof_fail_call = 0;
	cluster_node_id = -1;
}


/* MXA-T40: removing transport must not remove any provenance edge.  Feed the
 * same cross-segment successor observation through local and serialized
 * member-origin adapters and require an identical updater result. */
UT_TEST(test_current_mx_updater_provenance_local_remote_parity)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[1];
	ClusterCurrentMemberProof local_proofs[1];
	ClusterCurrentMemberProof remote_proofs[1];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof local_updater;
	ClusterCurrentUpdaterProof remote_updater;
	uint32 local_generations[1];
	uint32 remote_generations[1];
	uint64 hash;

	test_runtime_epoch = 9;
	test_member(&members[0], 100, MultiXactStatusNoKeyUpdate);
	test_updater_challenge(&challenge, 5, 100, 5);
	challenge.member_ordinal = 0;
	hash = cluster_multixact_current_descriptor_hash(&key, members,
											  lengthof(members));

	cluster_node_id = 5;
	test_runtime_target_owner_enabled = false;
	test_runtime_local_terminal_xid = 100;
	test_runtime_local_terminal_status = CLUSTER_TT_STATUS_COMMITTED;
	test_runtime_local_terminal_rolled = true;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, members, lengthof(members), hash, &challenge, local_proofs,
		&local_updater, local_generations), CMX_RESOLVE_OK);

	cluster_node_id = 3;
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 0;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, members, lengthof(members), hash, &challenge, remote_proofs,
		&remote_updater, remote_generations), CMX_RESOLVE_OK);
	UT_ASSERT_EQ(memcmp(&local_updater, &remote_updater,
					  sizeof(local_updater)), 0);

	test_runtime_local_terminal_xid = InvalidTransactionId;
	test_runtime_local_terminal_status = CLUSTER_TT_STATUS_UNKNOWN;
	test_runtime_local_terminal_rolled = false;
	cluster_node_id = -1;
}

UT_TEST(test_current_multixact_local_member_uses_target_canonical_not_source)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater_proof;
	uint32 proof_capability_generations[2];
	TimestampTz operation_deadline = 0;
	uint64 hash;
	uint16 i;

	cluster_node_id = 4;
	test_runtime_epoch = 9;
	key.cluster_epoch = (uint32)test_runtime_epoch;
	test_member(&members[0], 102, MultiXactStatusForShare);
	test_member(&members[1], 105, MultiXactStatusForKeyShare);
	hash = cluster_multixact_current_descriptor_hash(
		&key, members, lengthof(members));
	test_runtime_origin_proof_source_calls = 0;
	test_runtime_target_owner_enabled = true;
	test_runtime_target_owner_calls = 0;
	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater_proof, 0xa5, sizeof(updater_proof));

	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, members, lengthof(members), hash, NULL, proofs,
		&updater_proof, proof_capability_generations), CMX_RESOLVE_OK);
	UT_ASSERT_EQ(test_runtime_target_owner_calls, 2);
	UT_ASSERT_EQ(test_runtime_origin_proof_source_calls, 0);
	UT_ASSERT_EQ(proofs[0].state, CCM_ACTIVE);
	UT_ASSERT_EQ(proofs[1].state, CCM_ACTIVE);
	UT_ASSERT_EQ(proofs[0].key.local_xid, members[0].xid);
	UT_ASSERT_EQ(proofs[1].key.local_xid, members[1].xid);
	UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0]), 7);
	UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[1]), 7);
	UT_ASSERT_EQ(proof_capability_generations[0], (uint32)55);
	UT_ASSERT_EQ(proof_capability_generations[1], (uint32)55);

	/* A complete local batch that loses final grant freshness is retryable,
	 * but remains whole-batch zero-output and cannot renew its deadline. */
	test_runtime_local_grant_publishable = false;
	memset(proofs, 0xa5, sizeof(proofs));
	memset(&updater_proof, 0xa5, sizeof(updater_proof));
	memset(proof_capability_generations, 0xa5,
		   sizeof(proof_capability_generations));
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve_until(
		&key, members, lengthof(members), hash, NULL, proofs,
		&updater_proof, proof_capability_generations, &operation_deadline),
		CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(operation_deadline, (TimestampTz)UINT64_C(2000000));
	for (i = 0; i < lengthof(proofs); i++)
	{
		UT_ASSERT_EQ(proofs[i].state, CCM_UNKNOWN);
		UT_ASSERT_EQ(proof_capability_generations[i], (uint32)0);
	}
	UT_ASSERT_EQ(updater_proof.verdict, CUCP_UNKNOWN);
	cluster_gcs_reply_timeout_ms = 5000;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve_until(
		&key, members, lengthof(members), hash, NULL, proofs,
		&updater_proof, proof_capability_generations, &operation_deadline),
		CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(operation_deadline, (TimestampTz)UINT64_C(2000000));
	cluster_gcs_reply_timeout_ms = 1000;
	test_runtime_local_grant_publishable = true;
	test_runtime_target_owner_enabled = false;
	test_runtime_target_owner_active_xid = InvalidTransactionId;
	cluster_node_id = -1;
}


/* CTRC local-origin proofs must split physical ACTIVE from terminal sampling.
 * A retired current-segment owner and a rolled terminal slot carry no grant
 * and no participant capability generation, while an exact ACTIVE/SELF peer
 * in the same descriptor retains its own grant-bound identity. */
UT_TEST(test_current_multixact_local_terminal_uses_physical_current_or_rolled)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterProof updater_proof;
	uint32 proof_capability_generations[2];
	uint64 hash;
	int pass;

	cluster_node_id = 4;
	test_runtime_epoch = 9;
	key.cluster_epoch = (uint32)test_runtime_epoch;
	test_member(&members[0], 102, MultiXactStatusForShare);
	test_member(&members[1], 105, MultiXactStatusForKeyShare);
	hash = cluster_multixact_current_descriptor_hash(
		&key, members, lengthof(members));
	test_runtime_target_owner_enabled = true;
	test_runtime_target_owner_active_xid = members[0].xid;
	test_runtime_local_terminal_xid = members[1].xid;
	test_runtime_self_xid = members[0].xid;

	for (pass = 0; pass < 2; pass++)
	{
		test_runtime_local_terminal_status = pass == 0
			? CLUSTER_TT_STATUS_COMMITTED : CLUSTER_TT_STATUS_ABORTED;
		test_runtime_local_terminal_rolled = pass != 0;
		test_runtime_target_owner_calls = 0;
		test_runtime_local_terminal_calls = 0;
		memset(proofs, 0xa5, sizeof(proofs));
		memset(proof_capability_generations, 0xa5,
			   sizeof(proof_capability_generations));
		UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
			&key, members, lengthof(members), hash, NULL, proofs,
			&updater_proof, proof_capability_generations), CMX_RESOLVE_OK);
		UT_ASSERT_EQ(test_runtime_target_owner_calls, 2);
		UT_ASSERT_EQ(test_runtime_local_terminal_calls, 1);
		UT_ASSERT_EQ(proofs[0].state, CCM_SELF);
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[0]), 7);
		UT_ASSERT_EQ(proof_capability_generations[0], (uint32)55);
		UT_ASSERT_EQ(proofs[1].state,
			pass == 0 ? CCM_COMMITTED : CCM_ABORTED);
		UT_ASSERT_EQ(ClusterCurrentMemberProofGetCtrcGrant(&proofs[1]), 0);
		UT_ASSERT_EQ(proof_capability_generations[1], (uint32)0);
	}

	test_runtime_target_owner_enabled = false;
	test_runtime_target_owner_active_xid = InvalidTransactionId;
	test_runtime_local_terminal_xid = InvalidTransactionId;
	test_runtime_local_terminal_status = CLUSTER_TT_STATUS_UNKNOWN;
	test_runtime_local_terminal_rolled = false;
	test_runtime_self_xid = InvalidTransactionId;
	cluster_node_id = -1;
}


/* Each ACTIVE proof keeps the capability generation observed by its own
 * origin.  A failed leg invalidates the whole proof batch and clears every
 * ordinal instead of leaving a stale carrier beside UNKNOWN. */
UT_TEST(test_current_multixact_multi_origin_capability_generation_pairing)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proofs[3];
	ClusterCurrentUpdaterProof updater_proof;
	uint32 proof_capability_generations[3];
	uint64 hash;
	uint16 i;

	cluster_node_id = 3;
	test_runtime_epoch = 9;
	test_member(&members[0], 100, MultiXactStatusForShare); /* origin 5 */
	test_member(&members[1], 101, MultiXactStatusForKeyShare); /* origin 6 */
	test_member(&members[2], 102, MultiXactStatusForShare); /* origin 4 */
	hash = cluster_multixact_current_descriptor_hash(
		&key, members, lengthof(members));
	test_runtime_remote_member_state = CCM_ACTIVE;
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 0;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, members, lengthof(members), hash, NULL, proofs,
		&updater_proof, proof_capability_generations), CMX_RESOLVE_OK);
	for (i = 0; i < lengthof(members); i++)
	{
		UT_ASSERT_EQ(proofs[i].state, CCM_ACTIVE);
		UT_ASSERT_EQ(proof_capability_generations[i],
			(uint32)(100 + cluster_xid_origin_slot(members[i].xid)));
	}

	memset(proofs, 0xa5, sizeof(proofs));
	memset(proof_capability_generations, 0xa5,
		   sizeof(proof_capability_generations));
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 2;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, members, lengthof(members), hash, NULL, proofs,
		&updater_proof, proof_capability_generations), CMX_RESOLVE_UNKNOWN);
	for (i = 0; i < lengthof(members); i++)
	{
		UT_ASSERT_EQ(proofs[i].state, CCM_UNKNOWN);
		UT_ASSERT_EQ(proof_capability_generations[i], (uint32)0);
	}

	test_runtime_remote_member_state = CCM_ABORTED;
	test_runtime_proof_fail_call = 0;
	cluster_node_id = -1;
}


/* A: local materialization is not itself a publish authority.  The pure
 * state contract only admits PREPARE from LOCAL_DESCRIPTOR and cannot skip
 * directly to APPLY or reference publication. */
UT_TEST(test_current_multixact_publication_requires_prepared_state)
{
	ClusterCurrentMxHeapPublishStage next_stage;

	UT_ASSERT_EQ(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_LOCAL_DESCRIPTOR, CMX_HEAP_EVENT_PREPARE,
		&next_stage), true);
	UT_ASSERT_EQ(next_stage, CMX_HEAP_STAGE_RECEIPT_PREPARED);
	UT_ASSERT_EQ(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_LOCAL_DESCRIPTOR, CMX_HEAP_EVENT_APPLY,
		&next_stage), false);
	UT_ASSERT_EQ(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_LOCAL_DESCRIPTOR, CMX_HEAP_EVENT_PUBLISH,
		&next_stage), false);
}


/* B: exact receipt bytes are the complete caller-finalized header, including
 * cmax, ctid, moved/HOT cleanup and ITL slot, not predecessor bytes. */
UT_TEST(test_current_multixact_exact_target_uses_planned_successor_header)
{
	uint8 base[SizeofHeapTupleHeader]
		pg_attribute_aligned(MAXIMUM_ALIGNOF);
	uint8 actual[SizeofHeapTupleHeader]
		pg_attribute_aligned(MAXIMUM_ALIGNOF);
	uint8 planned[SizeofHeapTupleHeader]
		pg_attribute_aligned(MAXIMUM_ALIGNOF);
	HeapTupleHeader actual_header = (HeapTupleHeader)actual;
	ClusterCurrentMxHeapHeaderPlan plan;

	memset(base, 0x5a, sizeof(base));
	memcpy(actual, base, sizeof(actual));
	memset(&plan, 0, sizeof(plan));
	plan.kind = CMX_HEAP_PUBLISH_DELETE;
	plan.multixact_id = (MultiXactId)77;
	plan.command_id = (CommandId)29;
	ItemPointerSet(&plan.self_tid, 8, 3);
	plan.infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_EXCL_LOCK;
	plan.infomask2 = HEAP_KEYS_UPDATED;
	plan.itl_slot_index = 4;
	plan.command_is_combo = true;
	plan.changing_partition = true;

	actual_header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	actual_header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	actual_header->t_infomask |= plan.infomask;
	actual_header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderClearHotUpdated(actual_header);
	HeapTupleHeaderSetXmax(actual_header, plan.multixact_id);
	HeapTupleHeaderSetCmax(
		actual_header, plan.command_id, plan.command_is_combo);
	actual_header->t_ctid = plan.self_tid;
	HeapTupleHeaderSetMovedPartitions(actual_header);
	actual_header->t_itl_slot_idx = plan.itl_slot_index;

	UT_ASSERT_EQ(cluster_multixact_current_plan_heap_header(
		base, sizeof(base), &plan, planned), true);
	UT_ASSERT_EQ(memcmp(planned, actual, sizeof(planned)), 0);
	UT_ASSERT(memcmp(planned, base, sizeof(planned)) != 0);
}


/* C: receipt identity is the canonical descriptor key plus ordered members;
 * changing the real MXID or member order changes the hash. */
UT_TEST(test_current_multixact_receipt_uses_canonical_descriptor_hash)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxKey other_key;
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMxMemberDesc reversed[2];
	uint64 hash;

	test_member(&members[0], 101, MultiXactStatusForShare);
	test_member(&members[1], 102, MultiXactStatusNoKeyUpdate);
	reversed[0] = members[1];
	reversed[1] = members[0];
	hash = cluster_multixact_current_descriptor_hash(&key, members, 2);
	UT_ASSERT(hash != 0);
	UT_ASSERT_EQ(hash,
		cluster_multixact_current_descriptor_hash(&key, members, 2));
	other_key = key;
	other_key.multixact_id++;
	UT_ASSERT(hash != cluster_multixact_current_descriptor_hash(
		&other_key, members, 2));
	UT_ASSERT(hash != cluster_multixact_current_descriptor_hash(
		&key, reversed, 2));
}


/* D: a local-only descriptor needs no eager overlay: local MXID authority is
 * described on demand through native pg_multixact and never remote RPC. */
UT_TEST(test_current_multixact_local_descriptor_is_described_on_demand)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[4];
	uint16 member_count = 0;
	uint32 reported_total = 0;

	cluster_node_id = 0;
	test_runtime_epoch = key.cluster_epoch;
	test_runtime_mxid_origin = 0;
	test_runtime_mxid_mine = true;
	test_runtime_native_describe_calls = 0;
	test_runtime_native_describe_count = 1;
	test_runtime_remote_describe_calls = 0;
	key.origin_node_id = 0;
	UT_ASSERT_EQ(cluster_multixact_current_describe(
		&key, members, lengthof(members), &member_count, &reported_total),
		CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 0);
	UT_ASSERT_EQ(member_count, 1);
	UT_ASSERT_EQ(reported_total, 1);
	UT_ASSERT_EQ(members[0].xid, 100);

	cluster_node_id = -1;
	test_runtime_epoch = 0;
	test_runtime_mxid_origin = -1;
	test_runtime_mxid_mine = false;
	test_runtime_native_describe_count = 2;
}


/* E: a one-member successor is still represented by a real immutable MXID
 * descriptor, so exact targets never place a raw xid in multixact_id. */
UT_TEST(test_current_multixact_one_member_descriptor_is_valid)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc member;

	test_member(&member, 101, MultiXactStatusForShare);
	UT_ASSERT_EQ(cluster_multixact_current_validate_descriptor(
		&key, key.origin_node_id, key.cluster_epoch, &member, 1, 1),
		CMX_DESC_OK);
}


UT_TEST(test_current_multixact_one_member_local_materializer)
{
	MultiXactMember member;
	MultiXactId multi;

	MemSet(&member, 0, sizeof(member));
	member.xid = 501;
	member.status = MultiXactStatusForKeyShare;
	test_runtime_materialize_calls = 0;
	test_runtime_materialize_nmembers = 0;
	MemSet(&test_runtime_materialize_member, 0,
		sizeof(test_runtime_materialize_member));
	multi = MultiXactIdCreateLocalCurrentMembers(1, &member);
	UT_ASSERT_EQ(multi, (MultiXactId) 901);
	UT_ASSERT_EQ(test_runtime_materialize_calls, 1);
	UT_ASSERT_EQ(test_runtime_materialize_nmembers, 1);
	UT_ASSERT_EQ(test_runtime_materialize_member.xid, member.xid);
	UT_ASSERT_EQ(test_runtime_materialize_member.status, member.status);
}


UT_TEST(test_current_multixact_one_member_remote_descriptor_round_trip)
{
	ClusterCurrentMxKey key = test_mxkey();
	MultiXactMember native_member;
	ClusterCurrentMxDescribeReplyPage page;
	ClusterCurrentMxMemberDesc decoded[1];
	uint16 decoded_count = 0;
	uint32 reported_total = 0;
	const uint64 request_id = UINT64_C(0x0200000000000046);

	memset(&native_member, 0, sizeof(native_member));
	native_member.xid = 501;
	native_member.status = MultiXactStatusForShare;
	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_describe_page(
		key.origin_node_id, request_id, &key, &native_member, 1, &page),
		CMX_DESC_OK);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
		&page, sizeof(page), key.origin_node_id, key.cluster_epoch,
		request_id, &key, decoded, lengthof(decoded), &decoded_count,
		&reported_total), CMX_DESC_OK);
	UT_ASSERT_EQ(decoded_count, (uint16)1);
	UT_ASSERT_EQ(reported_total, (uint32)1);
	UT_ASSERT_EQ(decoded[0].xid, native_member.xid);
}


UT_TEST(test_current_multixact_one_member_proof_resolves_on_demand)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc member;
	ClusterCurrentMemberProof proof;
	ClusterCurrentUpdaterProof updater_proof;
	uint32 proof_capability_generation = 0;
	uint64 hash;

	cluster_node_id = 3;
	test_runtime_epoch = key.cluster_epoch;
	test_member(&member, 100, MultiXactStatusForShare);
	hash = cluster_multixact_current_descriptor_hash(&key, &member, 1);
	test_runtime_remote_member_state = CCM_ACTIVE;
	test_runtime_proof_calls = 0;
	test_runtime_proof_fail_call = 0;
	UT_ASSERT_EQ(cluster_multixact_current_members_resolve(
		&key, &member, 1, hash, NULL, &proof, &updater_proof,
		&proof_capability_generation), CMX_RESOLVE_OK);
	UT_ASSERT_EQ(test_runtime_proof_calls, 1);
	UT_ASSERT_EQ(proof.state, CCM_ACTIVE);
	UT_ASSERT_EQ(proof.member_xid, member.xid);
	UT_ASSERT_EQ(proof_capability_generation,
		(uint32)(100 + cluster_xid_origin_slot(member.xid)));

	test_runtime_remote_member_state = CCM_ABORTED;
	cluster_node_id = -1;
}


typedef union TestCurrentMxFixedHeader {
	uint64 align;
	uint8 bytes[SizeofHeapTupleHeader];
} TestCurrentMxFixedHeader;


static void
test_current_mx_fixed_header_init(TestCurrentMxFixedHeader *storage)
{
	HeapTupleHeader header = (HeapTupleHeader) storage->bytes;

	MemSet(storage, 0, sizeof(*storage));
	HeapTupleHeaderSetXmin(header, 41);
	HeapTupleHeaderSetXmax(header, 42);
	HeapTupleHeaderSetCmin(header, 7);
	ItemPointerSet(&header->t_ctid, 11, 3);
	header->t_infomask = HEAP_XMAX_INVALID;
	header->t_infomask2 = 5 | HEAP_HOT_UPDATED | HEAP_ONLY_TUPLE;
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 6;
}


static void
test_current_mx_header_plan_init(ClusterCurrentMxHeapHeaderPlan *plan,
	ClusterCurrentMxHeapPublishKind kind)
{
	MemSet(plan, 0, sizeof(*plan));
	plan->kind = kind;
	plan->multixact_id = 701;
	plan->xmin = 301;
	plan->command_id = 19;
	plan->infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_EXCL_LOCK;
	plan->infomask2 = HEAP_KEYS_UPDATED;
	plan->itl_slot_index = 2;
	ItemPointerSet(&plan->self_tid, 12, 4);
	ItemPointerSet(&plan->successor_tid, 13, 5);
}


/* Task 4 caller equivalence: DELETE hashes exactly the fixed header that its
 * critical section will publish, including cmax, ctid/moved and ITL. */
UT_TEST(test_current_multixact_delete_header_plan_matches_publication_bytes)
{
	TestCurrentMxFixedHeader base;
	TestCurrentMxFixedHeader expected;
	TestCurrentMxFixedHeader planned;
	ClusterCurrentMxHeapHeaderPlan plan;
	HeapTupleHeader header;

	test_current_mx_fixed_header_init(&base);
	expected = base;
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_DELETE);
	plan.command_is_combo = true;
	plan.changing_partition = true;
	header = (HeapTupleHeader) expected.bytes;
	header->t_itl_slot_idx = plan.itl_slot_index;
	header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	header->t_infomask |= plan.infomask;
	header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderClearHotUpdated(header);
	HeapTupleHeaderSetXmax(header, plan.multixact_id);
	HeapTupleHeaderSetCmax(header, plan.command_id, plan.command_is_combo);
	header->t_ctid = plan.self_tid;
	HeapTupleHeaderSetMovedPartitions(header);

	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT_EQ(memcmp(planned.bytes, expected.bytes, sizeof(planned.bytes)), 0);
}


/* UPDATE binds both the old-version successor edge and the new-version
 * heap-only/ITL header before either tuple becomes reachable. */
UT_TEST(test_current_multixact_update_headers_match_publication_bytes)
{
	TestCurrentMxFixedHeader base;
	TestCurrentMxFixedHeader expected;
	TestCurrentMxFixedHeader planned;
	ClusterCurrentMxHeapHeaderPlan plan;
	HeapTupleHeader header;

	test_current_mx_fixed_header_init(&base);
	expected = base;
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_UPDATE_OLD);
	plan.command_is_combo = true;
	plan.hot_update = true;
	header = (HeapTupleHeader) expected.bytes;
	header->t_itl_slot_idx = plan.itl_slot_index;
	HeapTupleHeaderSetHotUpdated(header);
	header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	header->t_infomask |= plan.infomask;
	header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderSetXmax(header, plan.multixact_id);
	HeapTupleHeaderSetCmax(header, plan.command_id, plan.command_is_combo);
	header->t_ctid = plan.successor_tid;
	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT_EQ(memcmp(planned.bytes, expected.bytes, sizeof(planned.bytes)), 0);

	expected = base;
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_UPDATE_NEW);
	plan.hot_update = true;
	header = (HeapTupleHeader) expected.bytes;
	header->t_infomask &= ~HEAP_XACT_MASK;
	header->t_infomask2 &= ~HEAP2_XACT_MASK;
	HeapTupleHeaderSetXmin(header, plan.xmin);
	HeapTupleHeaderSetCmin(header, plan.command_id);
	header->t_infomask |= HEAP_UPDATED | plan.infomask;
	header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderSetXmax(header, plan.multixact_id);
	HeapTupleHeaderSetHeapOnly(header);
	header->t_itl_slot_idx = plan.itl_slot_index;
	header->t_ctid = plan.self_tid;
	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT_EQ(memcmp(planned.bytes, expected.bytes, sizeof(planned.bytes)), 0);
}


UT_TEST(test_current_multixact_temp_lock_header_plan_matches_publication_bytes)
{
	TestCurrentMxFixedHeader base;
	TestCurrentMxFixedHeader expected;
	TestCurrentMxFixedHeader planned;
	ClusterCurrentMxHeapHeaderPlan plan;
	HeapTupleHeader header;

	test_current_mx_fixed_header_init(&base);
	expected = base;
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_TEMP_LOCK);
	plan.itl_slot_index = CLUSTER_ITL_SLOT_UNALLOCATED;
	header = (HeapTupleHeader) expected.bytes;
	header->t_infomask &= ~(HEAP_XMAX_BITS | HEAP_MOVED);
	header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	HeapTupleHeaderClearHotUpdated(header);
	HeapTupleHeaderSetXmax(header, plan.multixact_id);
	header->t_infomask |= plan.infomask;
	header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderSetCmax(header, plan.command_id, plan.command_is_combo);
	header->t_ctid = plan.self_tid;
	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT_EQ(memcmp(planned.bytes, expected.bytes, sizeof(planned.bytes)), 0);
}


UT_TEST(test_current_multixact_tuple_lock_header_plan_matches_publication_bytes)
{
	TestCurrentMxFixedHeader base;
	TestCurrentMxFixedHeader expected;
	TestCurrentMxFixedHeader planned;
	ClusterCurrentMxHeapHeaderPlan plan;
	HeapTupleHeader header;

	test_current_mx_fixed_header_init(&base);
	expected = base;
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_TUPLE_LOCK);
	plan.infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY
		| HEAP_XMAX_SHR_LOCK;
	plan.infomask2 = 0;
	header = (HeapTupleHeader) expected.bytes;
	header->t_infomask &= ~HEAP_XMAX_BITS;
	header->t_infomask2 &= ~HEAP_KEYS_UPDATED;
	header->t_infomask |= plan.infomask;
	header->t_infomask2 |= plan.infomask2;
	HeapTupleHeaderClearHotUpdated(header);
	HeapTupleHeaderSetXmax(header, plan.multixact_id);
	header->t_ctid = plan.self_tid;
	header->t_itl_slot_idx = plan.itl_slot_index;
	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT_EQ(memcmp(planned.bytes, expected.bytes, sizeof(planned.bytes)), 0);
}


UT_TEST(test_current_multixact_one_member_heap_plan_stays_multixact)
{
	TestCurrentMxFixedHeader base;
	TestCurrentMxFixedHeader planned;
	ClusterCurrentMxHeapHeaderPlan plan;
	HeapTupleHeader header = (HeapTupleHeader) planned.bytes;

	test_current_mx_fixed_header_init(&base);
	test_current_mx_header_plan_init(&plan, CMX_HEAP_PUBLISH_TUPLE_LOCK);
	plan.infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY
		| HEAP_XMAX_KEYSHR_LOCK;
	plan.infomask2 = 0;
	UT_ASSERT(cluster_multixact_current_plan_heap_header(
		base.bytes, sizeof(base.bytes), &plan, planned.bytes));
	UT_ASSERT((header->t_infomask & HEAP_XMAX_IS_MULTI) != 0);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(header), plan.multixact_id);
}


UT_TEST(test_current_multixact_applied_stage_rejects_retry)
{
	ClusterCurrentMxHeapPublishStage next;

	UT_ASSERT(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_RECEIPT_PREPARED, CMX_HEAP_EVENT_RETRY, &next));
	UT_ASSERT_EQ(next, CMX_HEAP_STAGE_CANCELLED);
	UT_ASSERT(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_RECEIPT_PREPARED, CMX_HEAP_EVENT_ERROR, &next));
	UT_ASSERT_EQ(next, CMX_HEAP_STAGE_CANCELLED);
	UT_ASSERT(!cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_RECEIPT_APPLIED, CMX_HEAP_EVENT_RETRY, &next));
	UT_ASSERT(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_RECEIPT_APPLIED, CMX_HEAP_EVENT_ERROR, &next));
	UT_ASSERT_EQ(next, CMX_HEAP_STAGE_RECEIPT_APPLIED);
	UT_ASSERT(cluster_multixact_current_heap_publish_transition(
		CMX_HEAP_STAGE_RECEIPT_APPLIED, CMX_HEAP_EVENT_PUBLISH, &next));
	UT_ASSERT_EQ(next, CMX_HEAP_STAGE_REFERENCE_PUBLISHED);
}


UT_TEST(test_current_multixact_proof_request_batches_by_member_origin)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[8];
	uint16 origins[8];
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	ClusterCurrentUpdaterChallenge challenge;
	bool seen[8];
	uint64 hash;
	uint16 plan_count = 0;
	uint16 seen_count = 0;
	uint16 i;
	uint16 j;

	for (i = 0; i < lengthof(members); i++) {
		test_member(&members[i], (TransactionId)(100 + i), MultiXactStatusForShare);
		origins[i] = (uint16)cluster_xid_origin_slot(members[i].xid);
	}
	members[5].member_status = MultiXactStatusNoKeyUpdate;
	test_updater_challenge(
		&challenge, origins[5], members[5].xid, 20);
	challenge.member_ordinal = 5;
	hash = cluster_multixact_current_descriptor_hash(&key, members, lengthof(members));

	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, lengthof(members), hash, &challenge, 801, 9, 3, 7,
					 plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, 4);
	memset(seen, 0, sizeof(seen));
	for (i = 0; i < plan_count; i++) {
		ClusterCurrentMxProofForwardV2 decoded;

		UT_ASSERT_EQ(plans[i].request.prefix.chunk_ordinal, i);
		UT_ASSERT_EQ(plans[i].request.prefix.chunk_count_minus_one, plan_count - 1);
		UT_ASSERT_EQ(cluster_multixact_current_wire_validate_proof_forward(
						 &plans[i].request, sizeof(plans[i].request), 3,
						 plans[i].destination_node_id, 9, &decoded),
					 true);
		if (decoded.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
			UT_ASSERT_EQ(decoded.trailer.body.updater.challenge.member_ordinal, 5);
			UT_ASSERT(!seen[5]);
			seen[5] = true;
			seen_count++;
		} else {
			for (j = 0; j < decoded.prefix.entry_count; j++) {
				uint16 ordinal = decoded.trailer.body.asks[j].member_ordinal;

				UT_ASSERT(ordinal < lengthof(members));
				UT_ASSERT(!seen[ordinal]);
				seen[ordinal] = true;
				seen_count++;
			}
		}
	}
	UT_ASSERT_EQ(seen_count, lengthof(members));

	origins[0] = (uint16)((origins[0] + 1) % CLUSTER_MAX_NODES);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, lengthof(members), hash, &challenge, 802, 9, 3, 7,
					 plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(plan_count, 0);
	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
					 &key, members, origins, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, hash, &challenge,
					 803, 9, 3, 7, plans, lengthof(plans), &plan_count),
				 CMX_RESOLVE_SUPPORTED_LIMIT);
}


/* MXA-K05/K13: the auxiliary undo cleaner has no BackendId, but an exact
 * same-node member sample is an internal plan and never becomes a wire frame.
 * A foreign destination must remain fail-closed without a routable endpoint. */
UT_TEST(test_current_multixact_aux_cleaner_local_proof_plan_stays_nonwire)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc member;
	uint16 origin;
	ClusterCurrentMxProofRequestPlan plans[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	ClusterCurrentMxProofForwardV2 decoded;
	uint64 hash;
	uint16 plan_count = 0;

	key.cluster_epoch = 0;
	test_member(&member, 100, MultiXactStatusForShare);
	origin = (uint16)cluster_xid_origin_slot(member.xid);
	hash = cluster_multixact_current_descriptor_hash(&key, &member, 1);

	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
		&key, &member, &origin, 1, hash, NULL, 804, 0, origin,
		InvalidBackendId, plans, lengthof(plans), &plan_count),
		CMX_RESOLVE_OK);
	UT_ASSERT_EQ(plan_count, (uint16)1);
	UT_ASSERT_EQ(plans[0].destination_node_id, origin);
	UT_ASSERT_EQ(plans[0].request.prefix.requester_backend_id,
		InvalidBackendId);
	UT_ASSERT(!cluster_multixact_current_wire_validate_proof_forward(
		&plans[0].request, sizeof(plans[0].request), origin, origin, 0,
		&decoded));

	UT_ASSERT_EQ(cluster_multixact_current_wire_build_proof_requests(
		&key, &member, &origin, 1, hash, NULL, 805, 0,
		(uint16)((origin + 1) % CLUSTER_MAX_NODES), InvalidBackendId,
		plans, lengthof(plans), &plan_count),
		CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(plan_count, (uint16)0);
}


UT_TEST(test_current_multixact_describe_wire_binding)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxDescribeForwardV2 forward;
	ClusterCurrentMxDescribeForwardV2 decoded;
	ClusterCurrentMxDescribeReplyPage page;
	ClusterCurrentMxMemberDesc out[4];
	uint16 out_count;
	uint32 out_total;

	memset(&forward, 0, sizeof(forward));
	forward.prefix.request_id = 501;
	forward.prefix.epoch = 9;
	forward.prefix.mxkey = key;
	forward.prefix.original_requester_node = 1;
	forward.prefix.requester_backend_id = 44;
	forward.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	forward.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	forward.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 1, 2, 9, &decoded),
				 true);
	UT_ASSERT_EQ(memcmp(&decoded, &forward, sizeof(decoded)), 0);
	forward.prefix.epoch = 0;
	forward.prefix.mxkey.cluster_epoch = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 1, 2, 0, &decoded),
				 true);
	forward.prefix.epoch = 9;
	forward.prefix.mxkey.cluster_epoch = 9;

	forward.prefix.reserved_b[3] = 1;
	memset(&decoded, 0xa5, sizeof(decoded));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 1, 2, 9, &decoded),
				 false);
	UT_ASSERT_EQ(decoded.prefix.request_id, 0);
	forward.prefix.reserved_b[3] = 0;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_forward(
					 &forward, sizeof(forward), 3, 2, 9, &decoded),
				 false);

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.result = CMX_DESC_OK;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = 2;
	page.header.entry_count = 2;
	page.header.wire_length = sizeof(page.header) + 2 * sizeof(page.members[0]);
	test_member(&page.members[0], 100, MultiXactStatusForShare);
	test_member(&page.members[1], 101, MultiXactStatusNoKeyUpdate);
	page.header.descriptor_hash
		= cluster_multixact_current_descriptor_hash(&key, page.members, 2);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(out_count, 2);
	UT_ASSERT_EQ(out_total, 2);
	UT_ASSERT_EQ(out[1].xid, 101);

	page.header.descriptor_hash++;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(out_count, 0);
	page.header.descriptor_hash--;

	memset(&page, 0, sizeof(page));
	page.header.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	page.header.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	page.header.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	page.header.result = CMX_DESC_SUPPORTED_LIMIT;
	page.header.source_node_id = 2;
	page.header.request_id = 501;
	page.header.mxkey = key;
	page.header.total_count = CLUSTER_CURRENT_MX_MAX_MEMBERS + 1;
	page.header.wire_length = sizeof(page.header);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(out_total, CLUSTER_CURRENT_MX_MAX_MEMBERS + 1);
	page.reserved[0] = 1;
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 9, 501, &key, out, lengthof(out), &out_count,
					 &out_total),
				 CMX_DESC_UNKNOWN);
}


UT_TEST(test_current_multixact_describe_routes_by_mxid_authority)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[4];
	uint16 members_count;
	uint32 reported_total;

	cluster_node_id = 0;
	test_runtime_epoch = 9;
	test_runtime_native_describe_calls = 0;
	test_runtime_remote_describe_calls = 0;
	test_runtime_remote_describe_ok = false;

	/* Local origin is the only route allowed to touch local pg_multixact. */
	key.origin_node_id = 0;
	test_runtime_mxid_origin = 0;
	test_runtime_mxid_mine = true;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
												&members_count, &reported_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 0);
	UT_ASSERT_EQ(members_count, 2);
	UT_ASSERT_EQ(members[1].xid, 101);

	/* Foreign origin must use the authority RPC and never local SLRU. */
	key.origin_node_id = 2;
	test_runtime_mxid_origin = 2;
	test_runtime_mxid_mine = false;
	test_runtime_remote_describe_ok = true;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
												&members_count, &reported_total),
				 CMX_DESC_OK);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 1);
	UT_ASSERT_EQ(members[0].xid, 200);

	/* Underivable/wrong-origin identity fails before either authority read. */
	test_runtime_mxid_origin = -1;
	UT_ASSERT_EQ(cluster_multixact_current_describe(&key, members, lengthof(members),
												&members_count, &reported_total),
				 CMX_DESC_UNKNOWN);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_remote_describe_calls, 1);
	UT_ASSERT_EQ(members_count, 0);

	cluster_node_id = -1;
	test_runtime_epoch = 0;
	test_runtime_mxid_origin = -1;
	test_runtime_mxid_mine = false;
	test_runtime_remote_describe_ok = false;
}


UT_TEST(test_current_multixact_native_conflict_matrix)
{
	static const bool expected[MaxMultiXactStatus + 1][LockTupleExclusive + 1] = {
		{ false, false, false, true }, { false, false, true, true }, { false, true, true, true },
		{ true, true, true, true },	   { false, true, true, true },	 { true, true, true, true },
	};
	int status;
	int mode;

	for (status = 0; status <= MaxMultiXactStatus; status++)
		for (mode = 0; mode <= LockTupleExclusive; mode++) {
			bool valid = false;

			UT_ASSERT_EQ(cluster_multixact_current_status_conflicts((uint8)status,
																	(LockTupleMode)mode, &valid),
						 expected[status][mode]);
			UT_ASSERT_EQ(valid, true);
		}

	{
		bool valid = true;

		UT_ASSERT_EQ(cluster_multixact_current_status_conflicts(MaxMultiXactStatus + 1,
																LockTupleExclusive, &valid),
					 false);
		UT_ASSERT_EQ(valid, false);
	}
}


static MultiXactStatus
test_lock_status(LockTupleMode mode)
{
	switch (mode) {
	case LockTupleKeyShare:
		return MultiXactStatusForKeyShare;
	case LockTupleShare:
		return MultiXactStatusForShare;
	case LockTupleNoKeyExclusive:
		return MultiXactStatusForNoKeyUpdate;
	case LockTupleExclusive:
		return MultiXactStatusForUpdate;
	}
	abort();
}


static void
test_context(ClusterCurrentMxRequestContext *ctx, const ClusterCurrentMxKey *key,
			 ClusterCurrentTupleAction action, MultiXactStatus desired_status, LockTupleMode mode)
{
	memset(ctx, 0, sizeof(*ctx));
	ctx->mxkey = *key;
	ctx->top_xid = 900;
	ctx->current_member_xid = 900;
	ctx->curcid = 5;
	ctx->tuple_cmax = 5;
	ctx->precheck_result = TM_Ok;
	ctx->desired_status = desired_status;
	ctx->lock_mode = mode;
	ctx->wait_policy = LockWaitBlock;
	ctx->action = action;
	ctx->tuple_shape = CCM_SHAPE_LOCK_ONLY;
	ctx->wait_for_conflict = 1;
	ctx->updater_origin_node_id = -1;
}


UT_TEST(test_current_multixact_compositor_status_mode_state_cross_product)
{
	static const bool conflict[MaxMultiXactStatus + 1][LockTupleExclusive + 1] = {
		{ false, false, false, true }, { false, false, true, true }, { false, true, true, true },
		{ true, true, true, true },	   { false, true, true, true },	 { true, true, true, true },
	};
	ClusterCurrentMxKey key = test_mxkey();
	int status;
	int mode;
	int state;

	for (status = 0; status <= MaxMultiXactStatus; status++)
		for (mode = 0; mode <= LockTupleExclusive; mode++)
			for (state = CCM_SELF; state <= CCM_UNKNOWN; state++) {
				ClusterCurrentMxMemberDesc members[2];
				ClusterCurrentMemberProof proofs[2];
				ClusterCurrentMxRequestContext ctx;
				ClusterCurrentMxDecision expected;
				TransactionId xid = state == CCM_SELF ? 900 : 100;

				test_member(&members[0], xid, (uint8)status);
				test_member(&members[1], 101, MultiXactStatusForKeyShare);
				test_proof(&proofs[0], &members[0], 0, (ClusterCurrentMemberState)state, 2, 70);
				test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 71);
				test_context(&ctx, &key, CCM_ACTION_LOCK, test_lock_status((LockTupleMode)mode),
							 (LockTupleMode)mode);
				ctx.tuple_shape
					= ISUPDATE_from_mxstatus(status) ? CCM_SHAPE_DELETED : CCM_SHAPE_LOCK_ONLY;

				switch ((ClusterCurrentMemberState)state) {
				case CCM_SELF:
					expected = ISUPDATE_from_mxstatus(status) ? CMDL_SELF_MODIFIED : CMDL_CONTINUE;
					break;
				case CCM_ACTIVE:
					expected = conflict[status][mode] ? CMDL_WAIT_MEMBER : CMDL_CONTINUE;
					break;
				case CCM_COMMITTED:
					expected = ISUPDATE_from_mxstatus(status) && conflict[status][mode]
								   ? CMDL_DELETED
								   : CMDL_CONTINUE;
					break;
				case CCM_ABORTED:
					expected = CMDL_CONTINUE;
					break;
				case CCM_UNKNOWN:
					expected = CMDL_UNKNOWN;
					break;
				}

				UT_ASSERT_EQ(
					cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
					expected);
			}
}


UT_TEST(test_current_multixact_active_wait_policies_and_stable_key)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterTTStatusKey wait_key;

	test_member(&members[0], 100, MultiXactStatusForUpdate);
	test_member(&members[1], 101, MultiXactStatusForUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 7, 30);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 3, 31);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_WAIT_MEMBER);
	UT_ASSERT_EQ(wait_key.origin_node_id, 3);
	UT_ASSERT_EQ(wait_key.local_xid, 101);
	UT_ASSERT_EQ(wait_key._reserved, (uint32)0);
	UT_ASSERT_EQ(wait_key._reserved2, (uint32)0);

	ctx.wait_for_conflict = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_BEING_MODIFIED);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForUpdate;
	ctx.wait_for_conflict = 1;
	ctx.wait_policy = LockWaitSkip;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_WOULD_BLOCK);
	ctx.wait_policy = LockWaitError;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, &wait_key),
				 CMDL_LOCK_NOT_AVAILABLE);
}


UT_TEST(test_current_multixact_terminal_nonconflict_and_unknown_precedence)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 60);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 61);
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForNoKeyUpdate,
				 LockTupleNoKeyExclusive);

	/* A terminal locker is gone; the active KeyShare holder is compatible. */
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	/* An ACTIVE updater uses the same matrix and can also be compatible. */
	members[0].member_status = MultiXactStatusNoKeyUpdate;
	proofs[0].member_status = MultiXactStatusNoKeyUpdate;
	ctx.desired_status = MultiXactStatusForKeyShare;
	ctx.lock_mode = LockTupleKeyShare;
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	proofs[0].state = CCM_ABORTED;
	memset(&proofs[0].key, 0, sizeof(proofs[0].key));
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[0], 0);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	/* Any UNKNOWN member makes the complete proof set fail closed. */
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 60);
	proofs[1].state = CCM_UNKNOWN;
	proofs[1].commit_scn = InvalidScn;
	ctx.action = CCM_ACTION_DELETE;
	ctx.desired_status = MultiXactStatusUpdate;
	ctx.lock_mode = LockTupleExclusive;
	ctx.wait_for_conflict = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	/* A SELF locker is retained for recomposition but never waited on. */
	test_member(&members[0], 900, MultiXactStatusForUpdate);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 62);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 63);
	test_context(&ctx, &key, CCM_ACTION_HOT_FOLLOW, MultiXactStatusForKeyShare, LockTupleKeyShare);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);
}


UT_TEST(test_current_multixact_member_states_and_self_cid)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;

	test_member(&members[0], 900, MultiXactStatusUpdate);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 40);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 41);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);
	ctx.tuple_shape = CCM_SHAPE_UPDATED;

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_SELF_MODIFIED);
	ctx.tuple_cmax = 4;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_INVISIBLE);

	proofs[0].state = CCM_ABORTED;
	memset(&proofs[0].key, 0, sizeof(proofs[0].key));
	ClusterCurrentMemberProofSetCtrcGrant(&proofs[0], 0);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_CONTINUE);

	proofs[1].state = CCM_UNKNOWN;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	proofs[1].state = CCM_ABORTED;
	ctx.precheck_result = TM_Invisible;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_INVISIBLE);
	ctx.precheck_result = TM_SelfModified;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_SELF_MODIFIED);
}


UT_TEST(test_current_multixact_committed_updater_requires_exact_hot_proof)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 51);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate, LockTupleExclusive);
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	ctx.updater_origin_node_id = 3;

	test_updater_challenge(&challenge, 3, 101, 21);
	challenge.member_ordinal = 1;
	memset(&updater_proof, 0, sizeof(updater_proof));
	updater_proof.mxkey = key;
	updater_proof.candidate_next_xmin_alias
		= challenge.candidate_next_xmin_alias;
	updater_proof.candidate_next_xmin_locator
		= challenge.candidate_next_xmin_locator;
	updater_proof.candidate_next_xmin_locator.tt_wrap = 3;
	updater_proof.updater_xid = 101;
	updater_proof.member_ordinal = 1;
	updater_proof.verdict = CUCP_MATCH;

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UPDATED);

	/*
	 * KeyShare is compatible with a committed NoKeyUpdate on the old
	 * version.  Native follow_updates=false may lock that old version, but
	 * follow_updates=true must authenticate the successor before advancing.
	 */
	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForKeyShare;
	ctx.lock_mode = LockTupleKeyShare;
	ctx.follow_updates = 0;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_CONTINUE);
	ctx.follow_updates = 1;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UPDATED);
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_UPDATE;
	ctx.desired_status = MultiXactStatusUpdate;
	ctx.lock_mode = LockTupleExclusive;
	ctx.follow_updates = 0;
	updater_proof.candidate_next_xmin_alias.tt_slot_id++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.candidate_next_xmin_alias.tt_slot_id--;
	updater_proof.verdict = CUCP_MISMATCH;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);

	updater_proof.verdict = CUCP_MATCH;
	updater_proof.mxkey.cluster_epoch++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.mxkey.cluster_epoch--;
	updater_proof.updater_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	updater_proof.updater_xid--;
	ctx.updater_origin_node_id = 4;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, &challenge,
												  &updater_proof, NULL),
				 CMDL_UNKNOWN);
	ctx.updater_origin_node_id = 3;

	ctx.tuple_shape = CCM_SHAPE_DELETED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_DELETED);
}


/* MXA-T44: the page-derived request cannot claim a canonical TT wrap.  The
 * origin upgrades that one field from the exact undo record; every other
 * locator and alias field remains an exact request/reply binding. */
UT_TEST(test_current_mx_updater_provenance_bootstraps_canonical_tt_wrap)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentUpdaterChallenge challenge;
	ClusterCurrentUpdaterProof updater_proof;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 3, 51);
	test_updater_challenge(&challenge, 3, 101, 21);
	challenge.member_ordinal = 1;
	challenge.candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;

	memset(&updater_proof, 0, sizeof(updater_proof));
	updater_proof.mxkey = key;
	updater_proof.candidate_next_xmin_alias
		= challenge.candidate_next_xmin_alias;
	updater_proof.candidate_next_xmin_locator
		= challenge.candidate_next_xmin_locator;
	updater_proof.candidate_next_xmin_locator.tt_wrap = 4;
	updater_proof.updater_xid = 101;
	updater_proof.member_ordinal = 1;
	updater_proof.verdict = CUCP_MATCH;

	UT_ASSERT(cluster_multixact_current_successor_provenance_well_formed(
		&challenge.candidate_next_xmin_alias,
		&challenge.candidate_next_xmin_locator, 101, 3, key.cluster_epoch));
	UT_ASSERT(cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, lengthof(members), &challenge, &updater_proof,
		3));

	challenge.candidate_next_xmin_locator.tt_wrap = 0;
	UT_ASSERT(!cluster_multixact_current_successor_provenance_well_formed(
		&challenge.candidate_next_xmin_alias,
		&challenge.candidate_next_xmin_locator, 101, 3, key.cluster_epoch));
	UT_ASSERT(!cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, lengthof(members), &challenge, &updater_proof,
		3));

	challenge.candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;
	updater_proof.candidate_next_xmin_locator.tt_wrap = TT_WRAP_INVALID;
	UT_ASSERT(!cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, lengthof(members), &challenge, &updater_proof,
		3));

	updater_proof.candidate_next_xmin_locator.tt_wrap = 4;
	updater_proof.candidate_next_xmin_locator.itl_slot_index++;
	UT_ASSERT(!cluster_multixact_current_validate_updater_proof(
		&key, members, proofs, lengthof(members), &challenge, &updater_proof,
		3));
}


UT_TEST(test_current_multixact_rejects_context_mode_mismatch)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 51);
	test_context(&ctx, &key, CCM_ACTION_LOCK, MultiXactStatusForKeyShare, LockTupleExclusive);

	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_DELETE;
	ctx.desired_status = MultiXactStatusNoKeyUpdate;
	ctx.lock_mode = LockTupleNoKeyExclusive;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_UPDATE;
	ctx.desired_status = MultiXactStatusForNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);

	ctx.action = CCM_ACTION_LOCK;
	ctx.desired_status = MultiXactStatusForNoKeyUpdate;
	ctx.tuple_shape = CCM_SHAPE_UPDATED;
	UT_ASSERT_EQ(cluster_multixact_current_decide(members, proofs, 2, &ctx, NULL, NULL, NULL),
				 CMDL_UNKNOWN);
}


UT_TEST(test_current_multixact_unknown_trace_names_exact_rejection)
{
	ClusterCurrentMxKey key = test_mxkey();
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	ClusterCurrentMxRequestContext ctx;
	ClusterCurrentMxDecisionTrace trace;

	test_member(&members[0], 100, MultiXactStatusForKeyShare);
	test_member(&members[1], 101, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_ABORTED, 2, 50);
	test_proof(&proofs[1], &members[1], 1, CCM_ABORTED, 3, 51);
	test_context(&ctx, &key, CCM_ACTION_UPDATE, MultiXactStatusUpdate,
				 LockTupleExclusive);
	ctx.tuple_shape = CCM_SHAPE_UPDATED;

	UT_ASSERT_EQ(cluster_multixact_current_decide_observed(
		members, proofs, 2, &ctx, NULL, NULL, NULL, &trace), CMDL_UNKNOWN);
	UT_ASSERT_EQ(trace.unknown_reason, CMX_UNKNOWN_TUPLE_SHAPE);
	UT_ASSERT_EQ(trace.member_ordinal, -1);

	ctx.tuple_shape = CCM_SHAPE_LOCK_ONLY;
	proofs[1].member_xid++;
	UT_ASSERT_EQ(cluster_multixact_current_decide_observed(
		members, proofs, 2, &ctx, NULL, NULL, NULL, &trace), CMDL_UNKNOWN);
	UT_ASSERT_EQ(trace.unknown_reason, CMX_UNKNOWN_PROOF_ENTRY);
	UT_ASSERT_EQ(trace.member_ordinal, 1);
}


UT_TEST(test_current_multixact_recompose_filters_terminal_members)
{
	ClusterCurrentMxMemberDesc members[3];
	ClusterCurrentMemberProof proofs[3];
	MultiXactMember normalized[4];
	uint16 normalized_count = 99;

	test_member(&members[0], 501, MultiXactStatusForShare);
	test_member(&members[1], 502, MultiXactStatusForKeyShare);
	test_member(&members[2], 503, MultiXactStatusNoKeyUpdate);
	test_proof(&proofs[0], &members[0], 0, CCM_COMMITTED, 2, 31);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 2, 32);
	test_proof(&proofs[2], &members[2], 2, CCM_ABORTED, 2, 33);

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 3, 504, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 502);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(normalized[1].xid, 504);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForShare);
}


UT_TEST(test_current_multixact_recompose_preserves_one_active_member)
{
	ClusterCurrentMxMemberDesc member;
	ClusterCurrentMemberProof proof;
	MultiXactMember normalized[2];
	uint16 normalized_count = 0;

	test_member(&member, 551, MultiXactStatusForKeyShare);
	test_proof(&proof, &member, 0, CCM_ACTIVE, 2, 35);
	UT_ASSERT_EQ(cluster_multixact_current_recompose(
		&member, &proof, 1, 552, MultiXactStatusForShare, normalized,
		lengthof(normalized), &normalized_count), CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, member.xid);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForKeyShare);
	UT_ASSERT_EQ(normalized[1].xid, (TransactionId) 552);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForShare);
}


UT_TEST(test_current_multixact_recompose_upgrades_requester_member)
{
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	MultiXactMember normalized[3];
	uint16 normalized_count = 0;

	test_member(&members[0], 601, MultiXactStatusForKeyShare);
	test_member(&members[1], 602, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_SELF, 2, 41);
	test_proof(&proofs[1], &members[1], 1, CCM_ACTIVE, 2, 42);

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 601, MultiXactStatusForUpdate, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 601);
	UT_ASSERT_EQ(normalized[0].status, MultiXactStatusForUpdate);
	UT_ASSERT_EQ(normalized[1].xid, 602);
	UT_ASSERT_EQ(normalized[1].status, MultiXactStatusForShare);
}


UT_TEST(test_current_multixact_recompose_fails_closed_on_incomplete_proof)
{
	ClusterCurrentMxMemberDesc members[2];
	ClusterCurrentMemberProof proofs[2];
	MultiXactMember normalized[3];
	uint16 normalized_count = 99;

	test_member(&members[0], 701, MultiXactStatusForKeyShare);
	test_member(&members[1], 702, MultiXactStatusForShare);
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 51);
	test_proof(&proofs[1], &members[1], 1, CCM_UNKNOWN, 2, 52);

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 703, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_UNKNOWN);
	UT_ASSERT_EQ(normalized_count, 0);
	UT_ASSERT_EQ(normalized[0].xid, InvalidTransactionId);

	test_proof(&proofs[1], &members[1], 1, CCM_COMMITTED, 2, 52);
	members[1].member_status = MultiXactStatusNoKeyUpdate;
	proofs[1].member_status = MultiXactStatusNoKeyUpdate;
	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, 2, 703, MultiXactStatusForShare, normalized,
					 lengthof(normalized), &normalized_count),
				 CMX_RECOMPOSE_DENIED);
	UT_ASSERT_EQ(normalized_count, 0);
}


UT_TEST(test_current_multixact_recompose_filters_before_cap_check)
{
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	MultiXactMember normalized[2];
	uint16 normalized_count = 0;
	uint16 i;

	for (i = 0; i < CLUSTER_CURRENT_MX_MAX_MEMBERS; i++) {
		test_member(&members[i], (TransactionId)(1000 + i), MultiXactStatusForKeyShare);
		test_proof(&proofs[i], &members[i], i, CCM_ABORTED, 2, (uint16)(100 + i));
	}
	test_proof(&proofs[0], &members[0], 0, CCM_ACTIVE, 2, 100);

	UT_ASSERT_EQ(cluster_multixact_current_recompose(
					 members, proofs, CLUSTER_CURRENT_MX_MAX_MEMBERS, 5000,
					 MultiXactStatusForShare, normalized, lengthof(normalized),
					 &normalized_count),
				 CMX_RECOMPOSE_OK);
	UT_ASSERT_EQ(normalized_count, 2);
	UT_ASSERT_EQ(normalized[0].xid, 1000);
	UT_ASSERT_EQ(normalized[1].xid, 5000);
}

UT_TEST(test_current_multixact_origin_builds_strict_describe_page)
{
	ClusterCurrentMxKey key;
	MultiXactMember native_members[2];
	ClusterCurrentMxDescribeReplyPage page;
	ClusterCurrentMxMemberDesc decoded[2];
	uint16 decoded_count = 0;
	uint32 reported_total = 0;
	const uint64 request_id = UINT64_C(0x0200000000000042);

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 2;
	key.multixact_id = (MultiXactId)91;
	key.cluster_epoch = 17;
	memset(native_members, 0, sizeof(native_members));
	native_members[0].xid = 501;
	native_members[0].status = MultiXactStatusForShare;
	native_members[1].xid = 502;
	native_members[1].status = MultiXactStatusNoKeyUpdate;
	memset(&page, 0xa5, sizeof(page));

	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_describe_page(
					 2, request_id, &key, native_members,
					 lengthof(native_members), &page),
			 CMX_DESC_OK);
	UT_ASSERT_EQ(page.header.magic, CLUSTER_CURRENT_MX_WIRE_MAGIC);
	UT_ASSERT_EQ(page.header.version, CLUSTER_CURRENT_MX_WIRE_VERSION);
	UT_ASSERT_EQ(page.header.kind,
				 GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE);
	UT_ASSERT_EQ(page.header.result, CMX_DESC_OK);
	UT_ASSERT_EQ(page.header.source_node_id, (uint32)2);
	UT_ASSERT_EQ(page.header.request_id, request_id);
	UT_ASSERT_EQ(page.header.total_count, (uint32)2);
	UT_ASSERT_EQ(page.header.entry_count, (uint16)2);
	UT_ASSERT(page.header.descriptor_hash != 0);
	UT_ASSERT_EQ(page.header.wire_length,
				 sizeof(page.header) + 2 * sizeof(page.members[0]));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 17, request_id, &key,
					 decoded, lengthof(decoded), &decoded_count,
					 &reported_total),
			 CMX_DESC_OK);
	UT_ASSERT_EQ(decoded_count, (uint16)2);
	UT_ASSERT_EQ(reported_total, (uint32)2);
	UT_ASSERT_EQ(decoded[0].xid, (TransactionId)501);
	UT_ASSERT_EQ(decoded[1].xid, (TransactionId)502);

	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_describe_page(
					 2, request_id, &key, NULL,
					 CLUSTER_CURRENT_MX_MAX_MEMBERS + 1, &page),
			 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 &page, sizeof(page), 2, 17, request_id, &key,
					 decoded, lengthof(decoded), &decoded_count,
					 &reported_total),
			 CMX_DESC_SUPPORTED_LIMIT);
	UT_ASSERT_EQ(reported_total,
				 (uint32)CLUSTER_CURRENT_MX_MAX_MEMBERS + 1);
}

UT_TEST(test_current_multixact_origin_serves_describe_on_capability_bound_reply)
{
	ClusterCurrentMxDescribeForwardV2 request;
	ClusterICEnvelope env;
	const GcsBlockReplyHeader *outer;
	const ClusterCurrentMxDescribeReplyPage *page;
	ClusterCurrentMxMemberDesc decoded[2];
	uint16 decoded_count = 0;
	uint32 reported_total = 0;

	test_runtime_epoch = 17;
	test_runtime_mxid_mine = true;
	test_runtime_native_describe_calls = 0;
	test_runtime_describe_send_calls = 0;
	test_runtime_describe_capability_calls = 0;
	test_runtime_describe_generation_match_calls = 0;
	memset(test_runtime_describe_payload, 0,
		   sizeof(test_runtime_describe_payload));
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UINT64_C(0x0200000000000043);
	request.prefix.epoch = test_runtime_epoch;
	request.prefix.mxkey.origin_node_id = 2;
	request.prefix.mxkey.multixact_id = (MultiXactId)92;
	request.prefix.mxkey.cluster_epoch = (uint32)test_runtime_epoch;
	request.prefix.original_requester_node = 3;
	request.prefix.requester_backend_id = 7;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.flags = CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE;
	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = 3;
	env.dest_node_id = 2;
	env.epoch = test_runtime_epoch;
	env.payload_length = sizeof(request);
	cluster_node_id = 2;

	stop_new_read_allowed = false;
	stop_new_read_calls = stop_allocation_calls = 0;
	cluster_gcs_current_mx_describe_serve_inline(&env, &request);
	UT_ASSERT_EQ(stop_new_read_calls, 1);
	UT_ASSERT_EQ(stop_allocation_calls, 0);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 0);
	UT_ASSERT_EQ(test_runtime_describe_send_calls, 0);
	stop_new_read_allowed = true;
	stop_new_read_calls = 0;
	test_runtime_native_describe_calls = test_runtime_describe_send_calls = 0;
	test_runtime_describe_capability_calls = test_runtime_describe_generation_match_calls = 0;
	cluster_gcs_current_mx_describe_serve_inline(&env, &request);
	UT_ASSERT_EQ(stop_new_read_calls, 1);
	UT_ASSERT_EQ(test_runtime_native_describe_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_capability_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_generation_match_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_send_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_dest, (uint32)3);
	UT_ASSERT_EQ(test_runtime_describe_payload_len,
				 sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	outer = (const GcsBlockReplyHeader *)test_runtime_describe_payload;
	page = (const ClusterCurrentMxDescribeReplyPage *)(
		test_runtime_describe_payload + sizeof(*outer));
	UT_ASSERT_EQ(outer->status,
				 GCS_BLOCK_REPLY_CURRENT_MX_DESCRIBE_RESULT);
	UT_ASSERT_EQ(outer->sender_node, 2);
	UT_ASSERT_EQ(outer->requester_backend_id, 7);
	UT_ASSERT_EQ(outer->request_id, request.prefix.request_id);
	UT_ASSERT_EQ(outer->epoch, test_runtime_epoch);
	UT_ASSERT_EQ(outer->checksum,
				 cluster_gcs_block_compute_checksum((const char *)page));
	UT_ASSERT_EQ(cluster_multixact_current_wire_validate_describe_reply(
					 page, sizeof(*page), 2, test_runtime_epoch,
					 request.prefix.request_id, &request.prefix.mxkey,
					 decoded, lengthof(decoded), &decoded_count,
					 &reported_total),
			 CMX_DESC_OK);
	UT_ASSERT_EQ(decoded_count, (uint16)2);
	UT_ASSERT_EQ(decoded[0].xid, (TransactionId)100);
	UT_ASSERT_EQ(decoded[1].xid, (TransactionId)101);
}

UT_TEST(test_current_multixact_origin_builds_strict_member_proof_page)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterCurrentMemberProof proof;
	ClusterCurrentMxProofReplyPage page;
	ClusterCurrentMemberProof decoded[1];
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult decoded_result = CMX_RESOLVE_UNKNOWN;
	uint32 decoded_capability_generation = 0;
	uint16 decoded_count = 0;

	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UINT64_C(0x0400000000000044);
	request.prefix.epoch = 17;
	request.prefix.mxkey.origin_node_id = 1;
	request.prefix.mxkey.multixact_id = (MultiXactId)93;
	request.prefix.mxkey.cluster_epoch = 17;
	request.prefix.original_requester_node = 3;
	request.prefix.requester_backend_id = 7;
	request.prefix.total_count = 2;
	ClusterCurrentMxProofPrefixSetDescriptorHash(
		&request.prefix, UINT64_C(0x12345678));
	request.prefix.entry_count = 1;
	request.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.body.asks[0].xid = (TransactionId)501;
	request.trailer.body.asks[0].member_ordinal = 0;
	request.trailer.body.asks[0].member_status = MultiXactStatusForShare;
	memset(&proof, 0, sizeof(proof));
	proof.member_xid = request.trailer.body.asks[0].xid;
	proof.member_ordinal = request.trailer.body.asks[0].member_ordinal;
	proof.member_status = request.trailer.body.asks[0].member_status;
	proof.state = CCM_ABORTED;
	memset(&page, 0xa5, sizeof(page));

	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_proof_page(
					 4, &request, CMX_RESOLVE_OK, 313, &proof, 1, NULL,
					 &page),
			 CMX_RESOLVE_OK);
	UT_ASSERT_EQ(page.header.magic, CLUSTER_CURRENT_MX_WIRE_MAGIC);
	UT_ASSERT_EQ(page.header.version, CLUSTER_CURRENT_MX_WIRE_VERSION);
	UT_ASSERT_EQ(page.header.kind,
				 GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF);
	UT_ASSERT_EQ(page.header.result, CMX_RESOLVE_OK);
	UT_ASSERT_EQ(page.header.source_node_id, (uint32)4);
	UT_ASSERT_EQ(page.header.request_id, request.prefix.request_id);
	UT_ASSERT_EQ(page.header.entry_count, (uint16)1);
	UT_ASSERT_EQ(page.header.requester_capability_generation,
				 (uint32)313);
	UT_ASSERT_EQ(page.header.wire_length,
				 sizeof(page.header) + sizeof(proof));
	UT_ASSERT(cluster_multixact_current_wire_validate_proof_reply_frame(
		&page, sizeof(page), 4, 17, &request, &decoded_result,
		decoded, lengthof(decoded), &decoded_count, &updater,
		&decoded_capability_generation));
	UT_ASSERT_EQ(decoded_result, CMX_RESOLVE_OK);
	UT_ASSERT_EQ(decoded_capability_generation, (uint32)313);
	UT_ASSERT_EQ(decoded_count, (uint16)1);
	UT_ASSERT_EQ(decoded[0].member_xid, (TransactionId)501);
	UT_ASSERT_EQ(decoded[0].state, CCM_ABORTED);

	page.header.requester_capability_generation = 0;
	decoded_result = CMX_RESOLVE_OK;
	decoded_capability_generation = 313;
	decoded_count = 1;
	UT_ASSERT(!cluster_multixact_current_wire_validate_proof_reply_frame(
		&page, sizeof(page), 4, 17, &request, &decoded_result,
		decoded, lengthof(decoded), &decoded_count, &updater,
		&decoded_capability_generation));
	UT_ASSERT_EQ(decoded_result, CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(decoded_capability_generation, (uint32)0);
	UT_ASSERT_EQ(decoded_count, (uint16)0);

	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_proof_page(
					 4, &request, CMX_RESOLVE_DENIED, 313, NULL, 0,
					 NULL, &page),
			 CMX_RESOLVE_UNKNOWN);

	/* Adjustment 21: final send-freshness loss is a typed, whole-batch,
	 * zero-output result.  It is not UNKNOWN and cannot carry a stale proof or
	 * a capability generation. */
	memset(&page, 0xa5, sizeof(page));
	decoded_result = CMX_RESOLVE_UNKNOWN;
	decoded_capability_generation = 313;
	decoded_count = 1;
	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_proof_page(
					 4, &request, CMX_RESOLVE_RETRY, 0, NULL, 0,
					 NULL, &page),
			 CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(page.header.result, CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(page.header.entry_count, (uint16)0);
	UT_ASSERT_EQ(page.header.requester_capability_generation, (uint32)0);
	UT_ASSERT_EQ(page.header.wire_length, sizeof(page.header));
	UT_ASSERT(cluster_multixact_current_wire_validate_proof_reply_frame(
		&page, sizeof(page), 4, 17, &request, &decoded_result,
		decoded, lengthof(decoded), &decoded_count, &updater,
		&decoded_capability_generation));
	UT_ASSERT_EQ(decoded_result, CMX_RESOLVE_RETRY);
	UT_ASSERT_EQ(decoded_capability_generation, (uint32)0);
	UT_ASSERT_EQ(decoded_count, (uint16)0);
	UT_ASSERT_EQ(decoded[0].state, CCM_UNKNOWN);
	UT_ASSERT_EQ(cluster_cr_server_test_current_mx_build_proof_page(
					 4, &request, CMX_RESOLVE_RETRY, 313, &proof, 1,
					 NULL, &page),
			 CMX_RESOLVE_UNKNOWN);
}

UT_TEST(test_current_multixact_origin_serves_member_proof_on_capability_bound_reply)
{
	ClusterCurrentMxProofForwardV2 request;
	ClusterICEnvelope env;
	const GcsBlockReplyHeader *outer;
	const ClusterCurrentMxProofReplyPage *page;
	ClusterCurrentMemberProof decoded[1];
	ClusterCurrentUpdaterProof updater;
	ClusterMxResolveResult decoded_result = CMX_RESOLVE_UNKNOWN;
	uint32 decoded_capability_generation = 0;
	uint16 decoded_count = 0;

	test_runtime_epoch = 17;
	test_runtime_origin_proof_source_calls = 0;
	test_runtime_describe_send_calls = 0;
	test_runtime_describe_capability_calls = 0;
	test_runtime_describe_generation_match_calls = 0;
	memset(test_runtime_describe_payload, 0,
		   sizeof(test_runtime_describe_payload));
	memset(&request, 0, sizeof(request));
	request.prefix.request_id = UINT64_C(0x0400000000000045);
	request.prefix.epoch = test_runtime_epoch;
	request.prefix.mxkey.origin_node_id = 1;
	request.prefix.mxkey.multixact_id = (MultiXactId)94;
	request.prefix.mxkey.cluster_epoch = (uint32)test_runtime_epoch;
	request.prefix.original_requester_node = 3;
	request.prefix.requester_backend_id = 7;
	request.prefix.total_count = 2;
	ClusterCurrentMxProofPrefixSetDescriptorHash(
		&request.prefix, UINT64_C(0x12345678));
	request.prefix.entry_count = 1;
	request.prefix.body_kind = CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS;
	request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
	request.trailer.body.asks[0].xid = (TransactionId)501;
	request.trailer.body.asks[0].member_ordinal = 0;
	request.trailer.body.asks[0].member_status = MultiXactStatusForShare;
	memset(&env, 0, sizeof(env));
	env.magic = PGRAC_IC_ENVELOPE_MAGIC;
	env.version = PGRAC_IC_ENVELOPE_VERSION_V1;
	env.msg_type = PGRAC_IC_MSG_GCS_BLOCK_FORWARD;
	env.source_node_id = 3;
	env.dest_node_id = 4;
	env.epoch = test_runtime_epoch;
	env.payload_length = sizeof(request);
	cluster_node_id = 4;

	stop_new_read_allowed = false;
	stop_new_read_calls = stop_allocation_calls = 0;
	cluster_gcs_current_mx_member_proof_serve_inline(&env, &request);
	UT_ASSERT_EQ(stop_new_read_calls, 1);
	UT_ASSERT_EQ(stop_allocation_calls, 0);
	UT_ASSERT_EQ(test_runtime_origin_proof_source_calls, 0);
	UT_ASSERT_EQ(test_runtime_describe_send_calls, 0);
	stop_new_read_allowed = true;
	stop_new_read_calls = 0;
	test_runtime_describe_send_calls = test_runtime_describe_capability_calls = 0;
	test_runtime_describe_generation_match_calls = 0;
	cluster_gcs_current_mx_member_proof_serve_inline(&env, &request);
	UT_ASSERT_EQ(stop_new_read_calls, 1);
	UT_ASSERT_EQ(test_runtime_origin_proof_source_calls, 0);
	UT_ASSERT_EQ(test_runtime_describe_capability_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_generation_match_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_send_calls, 1);
	UT_ASSERT_EQ(test_runtime_describe_dest, (uint32)3);
	UT_ASSERT_EQ(test_runtime_describe_payload_len,
				 sizeof(GcsBlockReplyHeader) + GCS_BLOCK_DATA_SIZE);
	outer = (const GcsBlockReplyHeader *)test_runtime_describe_payload;
	page = (const ClusterCurrentMxProofReplyPage *)(
		test_runtime_describe_payload + sizeof(*outer));
	UT_ASSERT_EQ(outer->status,
				 GCS_BLOCK_REPLY_CURRENT_MX_MEMBER_PROOF_RESULT);
	UT_ASSERT_EQ(outer->sender_node, 4);
	UT_ASSERT_EQ(outer->requester_backend_id, 7);
	UT_ASSERT_EQ(outer->request_id, request.prefix.request_id);
	UT_ASSERT_EQ(outer->epoch, test_runtime_epoch);
	UT_ASSERT_EQ(outer->checksum,
				 cluster_gcs_block_compute_checksum((const char *)page));
	UT_ASSERT(cluster_multixact_current_wire_validate_proof_reply_frame(
		page, sizeof(*page), 4, test_runtime_epoch, &request,
		&decoded_result, decoded, lengthof(decoded), &decoded_count,
		&updater, &decoded_capability_generation));
	UT_ASSERT_EQ(decoded_result, CMX_RESOLVE_UNKNOWN);
	UT_ASSERT_EQ(decoded_count, (uint16)0);
}

int
main(void)
{
	UT_PLAN(51);
	UT_RUN(test_current_multixact_public_symbols_link);
	UT_RUN(test_current_multixact_router_domain_binding);
	UT_RUN(test_current_multixact_descriptor_validation);
	UT_RUN(test_current_multixact_descriptor_accepts_cap_and_hashes_order);
	UT_RUN(test_current_multixact_proof_binding_and_order);
	UT_RUN(test_current_multixact_origin_member_proof_follows_exact_parent);
	UT_RUN(test_current_multixact_updater_candidate_requires_current_exact_binding);
	UT_RUN(test_current_mx_updater_provenance_alias_canonical_matrix);
	UT_RUN(test_current_multixact_proof_forward_wire_binding);
	UT_RUN(test_current_multixact_proof_reply_wire_binding);
	UT_RUN(test_current_multixact_members_resolve_all_or_nothing);
	UT_RUN(test_current_mx_updater_provenance_local_remote_parity);
	UT_RUN(test_current_multixact_local_member_uses_target_canonical_not_source);
	UT_RUN(test_current_multixact_local_terminal_uses_physical_current_or_rolled);
	UT_RUN(test_current_multixact_multi_origin_capability_generation_pairing);
	UT_RUN(test_current_multixact_publication_requires_prepared_state);
	UT_RUN(test_current_multixact_exact_target_uses_planned_successor_header);
	UT_RUN(test_current_multixact_receipt_uses_canonical_descriptor_hash);
	UT_RUN(test_current_multixact_local_descriptor_is_described_on_demand);
	UT_RUN(test_current_multixact_one_member_descriptor_is_valid);
	UT_RUN(test_current_multixact_one_member_local_materializer);
	UT_RUN(test_current_multixact_one_member_remote_descriptor_round_trip);
	UT_RUN(test_current_multixact_one_member_proof_resolves_on_demand);
	UT_RUN(test_current_multixact_delete_header_plan_matches_publication_bytes);
	UT_RUN(test_current_multixact_update_headers_match_publication_bytes);
	UT_RUN(test_current_multixact_temp_lock_header_plan_matches_publication_bytes);
	UT_RUN(test_current_multixact_tuple_lock_header_plan_matches_publication_bytes);
	UT_RUN(test_current_multixact_one_member_heap_plan_stays_multixact);
	UT_RUN(test_current_multixact_applied_stage_rejects_retry);
	UT_RUN(test_current_multixact_proof_request_batches_by_member_origin);
	UT_RUN(test_current_multixact_aux_cleaner_local_proof_plan_stays_nonwire);
	UT_RUN(test_current_multixact_describe_wire_binding);
	UT_RUN(test_current_multixact_describe_routes_by_mxid_authority);
	UT_RUN(test_current_multixact_native_conflict_matrix);
	UT_RUN(test_current_multixact_compositor_status_mode_state_cross_product);
	UT_RUN(test_current_multixact_active_wait_policies_and_stable_key);
	UT_RUN(test_current_multixact_terminal_nonconflict_and_unknown_precedence);
	UT_RUN(test_current_multixact_member_states_and_self_cid);
	UT_RUN(test_current_multixact_committed_updater_requires_exact_hot_proof);
	UT_RUN(test_current_mx_updater_provenance_bootstraps_canonical_tt_wrap);
	UT_RUN(test_current_multixact_rejects_context_mode_mismatch);
	UT_RUN(test_current_multixact_unknown_trace_names_exact_rejection);
	UT_RUN(test_current_multixact_recompose_filters_terminal_members);
	UT_RUN(test_current_multixact_recompose_preserves_one_active_member);
	UT_RUN(test_current_multixact_recompose_upgrades_requester_member);
	UT_RUN(test_current_multixact_recompose_fails_closed_on_incomplete_proof);
	UT_RUN(test_current_multixact_recompose_filters_before_cap_check);
	UT_RUN(test_current_multixact_origin_builds_strict_describe_page);
	UT_RUN(test_current_multixact_origin_serves_describe_on_capability_bound_reply);
	UT_RUN(test_current_multixact_origin_builds_strict_member_proof_page);
	UT_RUN(test_current_multixact_origin_serves_member_proof_on_capability_bound_reply);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
