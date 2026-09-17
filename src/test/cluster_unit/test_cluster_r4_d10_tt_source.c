/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_d10_tt_source.c
 *	  R4 D10 TT dormant-source dispatch tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_r4_d10_tt_source.c
 *
 * NOTES
 *	  This is a pgrac-original file.  It links the production TT-status
 *	  object and replaces only its PostgreSQL runtime dependencies.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/xlog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_recovery_merge.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_xid_stripe.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

#include <string.h>


UT_DEFINE_GLOBALS();


bool cluster_enabled = true;
int cluster_node_id = 1;
bool cluster_enable_adg = false;
int cluster_dg_role = CLUSTER_DG_ROLE_PRIMARY;
bool cluster_tt_durable_lookup = false;
bool cluster_cf_terminal_authority = false;
int cluster_tt_status_overlay_max_entries = 16;
int cluster_tt_status_overlay_ttl_ms = 0;
ProcessingMode Mode = NormalProcessing;

sigjmp_buf *PG_exception_stack = NULL;
ErrorContextCallback *error_context_stack = NULL;

static char fake_state_storage[4096] pg_attribute_aligned(64);
static char fake_lock_storage[sizeof(LWLockPadded)] pg_attribute_aligned(64);
static char fake_hash_storage[4][256] pg_attribute_aligned(64);
static char fake_hash_handle[64] pg_attribute_aligned(64);
static bool fake_state_found;
static bool fake_lock_found;
static uint16 fake_hash_count;
static bool fake_hash_raise;
static uint16 fake_seq_index;
static int fake_xid_origin = 1;
static TimestampTz fake_now = 1000000;
static ClusterTTDurableLocate fake_durable_locate;
static uint16 fake_durable_segment;
static uint16 fake_durable_slot;
static uint16 fake_durable_wrap;
static uint8 fake_durable_status;
static bool fake_current_owner_found;
static ClusterTTSlotCurrentOwner fake_current_owner;

static ClusterSemanticAdmissionResult fake_admission;
static bool fake_recheck;
static int fake_enter_count;
static int fake_recheck_count;
static int fake_leave_count;


void
ExceptionalCondition(const char *condition_name pg_attribute_unused(),
					 const char *file_name pg_attribute_unused(),
					 int line_number pg_attribute_unused())
{
	abort();
}

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return true;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
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

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature_bit, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	fake_enter_count++;
	UT_ASSERT_EQ(feature_bit, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_SOURCE_SIDE);
	memset(token, 0, sizeof(*token));
	if (fake_admission == CLUSTER_SEMANTIC_ADMISSION_OK) {
		token->feature_bit = feature_bit;
		token->record_generation = 19;
		token->formation_epoch = 7;
		token->side = (uint8)side;
		token->entered = true;
	}
	return fake_admission;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	fake_recheck_count++;
	UT_ASSERT(token->entered);
	return fake_recheck;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	fake_leave_count++;
	UT_ASSERT(token->entered);
	token->entered = false;
}

void *
ShmemInitStruct(const char *name, Size size pg_attribute_unused(), bool *found_ptr)
{
	if (strcmp(name, "ClusterTTStatusState") == 0) {
		*found_ptr = fake_state_found;
		fake_state_found = true;
		return fake_state_storage;
	}

	UT_ASSERT(strcmp(name, "ClusterTTStatusLock") == 0);
	*found_ptr = fake_lock_found;
	fake_lock_found = true;
	return fake_lock_storage;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *info_ptr pg_attribute_unused(),
			  int hash_flags pg_attribute_unused())
{
	return (HTAB *)fake_hash_handle;
}

void *
hash_search(HTAB *hashp pg_attribute_unused(), const void *key_ptr pg_attribute_unused(),
			HASHACTION action, bool *found_ptr)
{
	uint16 i;

	if (fake_hash_raise)
		siglongjmp(*PG_exception_stack, 1);

	switch (action) {
	case HASH_FIND:
		for (i = 0; i < fake_hash_count; i++)
			if (memcmp(fake_hash_storage[i], key_ptr, sizeof(ClusterTTStatusKey)) == 0) {
				if (found_ptr != NULL)
					*found_ptr = true;
				return fake_hash_storage[i];
			}
		if (found_ptr != NULL)
			*found_ptr = false;
		return NULL;
	case HASH_ENTER:
	case HASH_ENTER_NULL:
		for (i = 0; i < fake_hash_count; i++)
			if (memcmp(fake_hash_storage[i], key_ptr, sizeof(ClusterTTStatusKey)) == 0) {
				if (found_ptr != NULL)
					*found_ptr = true;
				return fake_hash_storage[i];
			}
		if (found_ptr != NULL)
			*found_ptr = false;
		if (fake_hash_count >= lengthof(fake_hash_storage))
			return NULL;
		memset(fake_hash_storage[fake_hash_count], 0, sizeof(fake_hash_storage[fake_hash_count]));
		memcpy(fake_hash_storage[fake_hash_count], key_ptr, sizeof(ClusterTTStatusKey));
		return fake_hash_storage[fake_hash_count++];
	case HASH_REMOVE:
		for (i = 0; i < fake_hash_count; i++)
			if (memcmp(fake_hash_storage[i], key_ptr, sizeof(ClusterTTStatusKey)) == 0) {
				if (found_ptr != NULL)
					*found_ptr = true;
				if (i + 1 < fake_hash_count)
					memmove(fake_hash_storage[i], fake_hash_storage[i + 1],
							(sizeof(fake_hash_storage[0]) * (fake_hash_count - i - 1)));
				fake_hash_count--;
				return fake_hash_storage[fake_hash_count];
			}
		if (found_ptr != NULL)
			*found_ptr = false;
		return NULL;
	default:
		abort();
	}
}

void
hash_seq_init(HASH_SEQ_STATUS *status pg_attribute_unused(), HTAB *hashp pg_attribute_unused())
{
	fake_seq_index = 0;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	if (fake_seq_index >= fake_hash_count)
		return NULL;
	return fake_hash_storage[fake_seq_index++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

Size
hash_estimate_size(long num_entries, Size entry_size)
{
	return (Size)num_entries * entry_size;
}

Size
add_size(Size size_a, Size size_b)
{
	return size_a + size_b;
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
	return fake_now;
}

bool
RecoveryInProgress(void)
{
	return false;
}

bool
TransactionIdDidCommit(TransactionId xid pg_attribute_unused())
{
	return false;
}

uint64
cluster_epoch_get_current(void)
{
	return 7;
}

int
cluster_xid_origin_slot(TransactionId xid pg_attribute_unused())
{
	return fake_xid_origin;
}

bool
cluster_merged_instance_is_materialized(int node_id pg_attribute_unused())
{
	return false;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_terminal_authorized(int origin_node pg_attribute_unused(),
										   TransactionId xid pg_attribute_unused(),
										   uint64 observed_epoch pg_attribute_unused(),
										   uint64 current_epoch pg_attribute_unused(),
										   bool retention_required pg_attribute_unused(),
										   bool retention_proven pg_attribute_unused(),
										   SCN *out_scn pg_attribute_unused())
{
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin_node pg_attribute_unused(),
									   TransactionId xid pg_attribute_unused(),
									   SCN *out_scn pg_attribute_unused())
{
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

bool
cluster_tt_slot_durable_lookup_committed_stable(
	uint32 segment_id pg_attribute_unused(), uint16 slot_offset pg_attribute_unused(),
	TransactionId xid pg_attribute_unused(), uint32 expected_wrap pg_attribute_unused(),
	ClusterTTDurableXidCommitCheck xid_committed pg_attribute_unused(),
	SCN *commit_scn pg_attribute_unused())
{
	return false;
}

ClusterTTDurableResolve
cluster_tt_slot_durable_resolve_by_xid_origin(int origin_node pg_attribute_unused(),
											  TransactionId xid pg_attribute_unused(),
											  uint32 expected_wrap pg_attribute_unused(),
											  SCN *commit_scn pg_attribute_unused(),
											  uint16 *out_seg pg_attribute_unused(),
											  uint16 *out_slot pg_attribute_unused(),
											  uint16 *out_wrap pg_attribute_unused())
{
	return CLUSTER_TT_DURABLE_RECYCLED_ZERO_MATCH;
}

ClusterTTDurableLocate
cluster_tt_slot_durable_locate_any_by_xid_origin(int origin_node, TransactionId xid,
												 uint16 *out_seg, uint16 *out_slot,
												 uint16 *out_wrap, uint8 *out_status)
{
	UT_ASSERT_EQ(origin_node, cluster_node_id);
	UT_ASSERT(TransactionIdIsNormal(xid));
	if (fake_durable_locate != CLUSTER_TT_DURABLE_LOCATE_FOUND)
		return fake_durable_locate;
	if (out_seg != NULL)
		*out_seg = fake_durable_segment;
	if (out_slot != NULL)
		*out_slot = fake_durable_slot;
	if (out_wrap != NULL)
		*out_wrap = fake_durable_wrap;
	if (out_status != NULL)
		*out_status = fake_durable_status;
	return CLUSTER_TT_DURABLE_LOCATE_FOUND;
}

bool
cluster_tt_slot_current_owner_by_xid(int node_id, TransactionId xid, ClusterTTSlotCurrentOwner *out)
{
	UT_ASSERT_EQ(node_id, cluster_node_id);
	UT_ASSERT(TransactionIdIsNormal(xid));
	UT_ASSERT(out != NULL);
	memset(out, 0, sizeof(*out));
	if (!fake_current_owner_found)
		return false;
	UT_ASSERT_EQ(fake_current_owner.xid, xid);
	*out = fake_current_owner;
	return true;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

static void
reset_fixture(void)
{
	memset(fake_state_storage, 0, sizeof(fake_state_storage));
	memset(fake_lock_storage, 0, sizeof(fake_lock_storage));
	memset(fake_hash_storage, 0, sizeof(fake_hash_storage));
	memset(fake_hash_handle, 0, sizeof(fake_hash_handle));
	fake_state_found = false;
	fake_lock_found = false;
	fake_hash_count = 0;
	fake_hash_raise = false;
	fake_seq_index = 0;
	fake_xid_origin = 1;
	fake_durable_locate = CLUSTER_TT_DURABLE_LOCATE_MISSING;
	fake_durable_segment = 0;
	fake_durable_slot = 0;
	fake_durable_wrap = 0;
	fake_durable_status = TT_SLOT_INVALID;
	fake_current_owner_found = false;
	memset(&fake_current_owner, 0, sizeof(fake_current_owner));
	fake_admission = CLUSTER_SEMANTIC_ADMISSION_OK;
	fake_recheck = true;
	fake_enter_count = 0;
	fake_recheck_count = 0;
	fake_leave_count = 0;
	cluster_tt_status_shmem_init();
}

static ClusterTTStatusKey
make_key(TransactionId xid)
{
	ClusterTTStatusKey key;

	memset(&key, 0, sizeof(key));
	key.origin_node_id = 1;
	key.undo_segment_id = 2;
	key.tt_slot_id = 3;
	key.cluster_epoch = 7;
	key.local_xid = xid;
	return key;
}

static bool
bytes_are_zero(const void *ptr, Size size)
{
	const unsigned char *bytes = ptr;

	while (size-- > 0) {
		if (*bytes++ != 0)
			return false;
	}
	return true;
}

UT_TEST(test_frozen_tt_source_operation_domain)
{
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_LOOKUP, 0);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_INSTALL_LOCAL, 1);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_INSTALL_SUBCOMMITTED, 2);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_DELETE_EXACT, 3);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_RESOLVE_PREPARED_COMMIT, 4);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_BUMP_SELF_CONSUMER_HIT, 5);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_BUMP_PARENT_CHAIN_FOLLOW, 6);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID, 7);
	UT_ASSERT_EQ(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE, 8);
}

UT_TEST(test_active_refuses_before_request_inspection_and_mutation)
{
	ClusterTTStatusSourceResult result;
	ClusterSemanticAdmissionResult admission;

	reset_fixture();
	fake_admission = CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT;
	memset(&result, 0xA5, sizeof(result));
	admission = cluster_tt_status_source_dispatch(
		CLUSTER_TT_SOURCE_LOOKUP, (const ClusterTTStatusSourceRequest *)(uintptr_t)1, &result);

	UT_ASSERT_EQ(admission, CLUSTER_SEMANTIC_ADMISSION_SOURCE_DORMANT);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
	UT_ASSERT_EQ(fake_enter_count, 1);
	UT_ASSERT_EQ(fake_recheck_count, 0);
	UT_ASSERT_EQ(fake_leave_count, 0);
	UT_ASSERT_EQ(cluster_tt_status_get_lookup_hit_count(), 0);
}

UT_TEST(test_disabled_source_executes_matching_counter_arm)
{
	ClusterTTStatusSourceResult result;

	reset_fixture();
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_BUMP_SELF_CONSUMER_HIT, NULL, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
	UT_ASSERT_EQ(cluster_tt_status_get_self_consumer_hit_count(), 1);
	UT_ASSERT_EQ(fake_enter_count, 1);
	UT_ASSERT_EQ(fake_recheck_count, 1);
	UT_ASSERT_EQ(fake_leave_count, 1);
}

UT_TEST(test_install_lookup_and_delete_publish_only_after_recheck)
{
	ClusterTTStatusKey key = make_key((TransactionId)601);
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;

	reset_fixture();
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_COMMITTED;
	request.commit_scn = UINT64_C(9001);
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);

	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT(result.lookup.authoritative);
	UT_ASSERT_EQ(result.lookup.status, CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ(result.lookup.commit_scn, UINT64_C(9001));

	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_DELETE_EXACT, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT_EQ(fake_enter_count, 3);
	UT_ASSERT_EQ(fake_recheck_count, 3);
	UT_ASSERT_EQ(fake_leave_count, 3);
}

UT_TEST(test_missing_required_pointer_closes_after_admission)
{
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;

	reset_fixture();
	memset(&request, 0, sizeof(request));
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
	UT_ASSERT_EQ(fake_enter_count, 1);
	UT_ASSERT_EQ(fake_recheck_count, 0);
	UT_ASSERT_EQ(fake_leave_count, 1);
}

UT_TEST(test_unknown_operation_closes_after_admission)
{
	ClusterTTStatusSourceResult result;

	reset_fixture();
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch((ClusterTTStatusSourceOp)99, NULL, &result),
				 CLUSTER_SEMANTIC_ADMISSION_CLOSED);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
	UT_ASSERT_EQ(fake_enter_count, 1);
	UT_ASSERT_EQ(fake_recheck_count, 0);
	UT_ASSERT_EQ(fake_leave_count, 1);
}

UT_TEST(test_generation_drift_discards_fixed_result)
{
	ClusterTTStatusKey key = make_key((TransactionId)602);
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;

	reset_fixture();
	fake_recheck = false;
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_COMMITTED;
	request.commit_scn = UINT64_C(9002);
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
	UT_ASSERT_EQ(cluster_tt_status_get_install_count(), 1);
	UT_ASSERT_EQ(fake_recheck_count, 1);
	UT_ASSERT_EQ(fake_leave_count, 1);
}

UT_TEST(test_current_own_candidate_is_exact_and_admission_bound)
{
	ClusterTTStatusKey key = make_key((TransactionId)604);
	ClusterTTStatusKey wrong = key;
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;

	reset_fixture();
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	request.commit_scn = InvalidScn;
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);

	request.xid = key.local_xid;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID,
												   &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT_EQ(memcmp(&result.current_key, &key, sizeof(key)), 0);
	UT_ASSERT(result.lookup.authoritative);

	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(
					 CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(result.current_key_verdict, CLUSTER_TT_CURRENT_KEY_MATCH);
	UT_ASSERT_EQ(memcmp(&result.current_key, &key, sizeof(key)), 0);

	wrong.tt_slot_id++;
	request.key = &wrong;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(
					 CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT_EQ(result.current_key_verdict, CLUSTER_TT_CURRENT_KEY_UNKNOWN);
	UT_ASSERT_EQ(memcmp(&result.current_key, &key, sizeof(key)), 0);

	fake_xid_origin = 2;
	request.key = &key;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(
					 CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!result.bool_value);
	UT_ASSERT_EQ(result.current_key_verdict, CLUSTER_TT_CURRENT_KEY_UNKNOWN);

	fake_xid_origin = 1;
	fake_recheck = false;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(
					 CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID_CANDIDATE, &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_GENERATION_CHANGED);
	UT_ASSERT(bytes_are_zero(&result, sizeof(result)));
}

UT_TEST(test_current_own_uses_exact_allocator_canonical_among_matching_data_aliases)
{
	ClusterTTStatusKey canonical = make_key((TransactionId)605);
	ClusterTTStatusKey data_alias = canonical;
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;

	reset_fixture();
	data_alias.undo_segment_id = 9;
	memset(&request, 0, sizeof(request));
	request.status = CLUSTER_TT_STATUS_IN_PROGRESS;
	request.commit_scn = InvalidScn;
	request.key = &canonical;
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	request.key = &data_alias;
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);

	/* The bounded current allocator owner names the canonical binding.  The
	 * DATA alias remains present but cannot be selected by hash order or
	 * segment order. */
	fake_current_owner_found = true;
	fake_current_owner.segment_id = canonical.undo_segment_id;
	fake_current_owner.xid = canonical.local_xid;
	fake_current_owner.commit_scn = InvalidScn;
	fake_current_owner.slot_offset = cluster_tt_slot_id_to_offset(canonical.tt_slot_id);
	fake_current_owner.wrap = 4;
	fake_current_owner.status = CTS_ACTIVE;
	memset(&request, 0, sizeof(request));
	request.xid = canonical.local_xid;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID,
												   &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	UT_ASSERT_EQ(memcmp(&result.current_key, &canonical, sizeof(canonical)), 0);
	UT_ASSERT_EQ(result.lookup.status, CLUSTER_TT_STATUS_IN_PROGRESS);
	UT_ASSERT(result.lookup.authoritative);

	/* Same-status aliases without the physical canonical owner remain
	 * ambiguous; semantic agreement alone never grants authority. */
	fake_current_owner_found = false;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID,
												   &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!result.bool_value);

	/* A status-divergent alias is rejected even when the allocator owner still
	 * names the canonical slot. */
	fake_current_owner_found = true;
	request.key = &data_alias;
	request.status = CLUSTER_TT_STATUS_ABORTED;
	UT_ASSERT_EQ(
		cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result),
		CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(result.bool_value);
	memset(&request, 0, sizeof(request));
	request.xid = canonical.local_xid;
	memset(&result, 0xA5, sizeof(result));
	UT_ASSERT_EQ(cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP_CURRENT_OWN_XID,
												   &request, &result),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	UT_ASSERT(!result.bool_value);
}

UT_TEST(test_error_path_runs_single_leave_funnel)
{
	ClusterTTStatusKey key = make_key((TransactionId)603);
	ClusterTTStatusSourceRequest request;
	ClusterTTStatusSourceResult result;
	volatile bool caught = false;

	reset_fixture();
	memset(&request, 0, sizeof(request));
	request.key = &key;
	request.status = CLUSTER_TT_STATUS_COMMITTED;
	request.commit_scn = UINT64_C(9003);
	fake_hash_raise = true;

	PG_TRY();
	{
		(void)cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_INSTALL_LOCAL, &request, &result);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();

	UT_ASSERT(caught);
	UT_ASSERT_EQ(fake_enter_count, 1);
	UT_ASSERT_EQ(fake_recheck_count, 0);
	UT_ASSERT_EQ(fake_leave_count, 1);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_frozen_tt_source_operation_domain);
	UT_RUN(test_active_refuses_before_request_inspection_and_mutation);
	UT_RUN(test_disabled_source_executes_matching_counter_arm);
	UT_RUN(test_install_lookup_and_delete_publish_only_after_recheck);
	UT_RUN(test_missing_required_pointer_closes_after_admission);
	UT_RUN(test_unknown_operation_closes_after_admission);
	UT_RUN(test_generation_drift_discards_fixed_result);
	UT_RUN(test_current_own_candidate_is_exact_and_admission_bound);
	UT_RUN(test_current_own_uses_exact_allocator_canonical_among_matching_data_aliases);
	UT_RUN(test_error_path_runs_single_leave_funnel);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
