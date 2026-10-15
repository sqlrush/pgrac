/*-------------------------------------------------------------------------
 *
 * test_cluster_ax_binding.c
 *    Real S5, grant predicates, native registration and AX binding reader.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ax_binding.c
 *
 * Readiness, GRD promotion/cleanup and the native hash are explicit fixtures.
 * This exercises the transfer of an already granted request, not wire delivery.
 *-------------------------------------------------------------------------
 */
int acquire_fixture_main(int argc, char **argv);
#define main acquire_fixture_main
#define cluster_ges_relation_grant_is_current fixture_relation_grant_is_current
#define cluster_ges_retained_grant_check fixture_retained_grant_check
#define cluster_grd_promote_remote_grant_mode_exact fixture_remote_promote
#define cluster_ges_hw_grant_abandon fixture_grant_abandon
#include "test_cluster_lock_acquire.c"
#undef main
#undef cluster_ges_relation_grant_is_current
#undef cluster_ges_retained_grant_check
#undef cluster_grd_promote_remote_grant_mode_exact
#undef cluster_ges_hw_grant_abandon

/* Original prototypes were renamed with the dependency fixtures above. */
bool cluster_ges_relation_grant_is_current(const ClusterGesHwGrant *grant,
	const ClusterResId *resid, const ClusterGrdHolderId *holder, uint64 request_id,
	uint32 mode, bool dontwait);
bool cluster_ges_retained_grant_check(const ClusterGesHwGrant *grant,
	const ClusterResId *resid, const ClusterGrdHolderId *holder, uint64 request_id,
	uint32 mode, bool dontwait, bool *pending);
ClusterGrdEntryResult cluster_grd_promote_remote_grant_mode_exact(
	const ClusterResId *resid, const ClusterGrdHolderId *holder, LOCKMODE mode);
void cluster_ges_hw_grant_abandon(ClusterGesHwGrant *grant);

static bool serving_allowed = true;
static LOCALLOCK native_lock;
static PGPROC native_proc;
static HTAB *LockMethodLocalHash = (HTAB *)1;
static unsigned promotion_calls, abandon_calls;
static unsigned receipt_fault;

static bool
ges_serving_observe(bool *pending)
{
	*pending = false;
	return serving_allowed;
}

static bool
ges_readiness_allows_local_origin(uint32 opcode pg_attribute_unused(),
								 const ClusterResId *resid pg_attribute_unused(),
								 LOCKMODE mode pg_attribute_unused(),
								 LOCKMODE current pg_attribute_unused(), bool serving)
{
	return serving;
}

#include "test_cluster_ax_binding_grant.inc"

ClusterGrdEntryResult
cluster_grd_promote_remote_grant_mode_exact(const ClusterResId *resid,
											const ClusterGrdHolderId *holder, LOCKMODE mode)
{
	UT_ASSERT_EQ(resid->type, LOCKTAG_RELATION);
	UT_ASSERT_EQ(resid->field1, 5);
	UT_ASSERT_EQ(resid->field2, 16385);
	UT_ASSERT_EQ(holder->request_id, UINT64CONST(0x100000023));
	UT_ASSERT(mode == AccessExclusiveLock || mode == AccessShareLock);
	promotion_calls++;
	return owner_promote_result;
}

void
cluster_ges_hw_grant_abandon(ClusterGesHwGrant *grant)
{
	/* The transport cleanup boundary does not manufacture a grant. */
	UT_ASSERT(!grant->consumed && !grant->registration_proof);
	abandon_calls++;
	grant->cleanup_pending = false;
}

void
cluster_ges_send_convert_rollback(const ClusterResId *resid pg_attribute_unused(),
	uint32 upgraded_mode pg_attribute_unused(), uint32 old_mode pg_attribute_unused(),
	const ClusterGrdHolderId *holder pg_attribute_unused(),
	uint64 old_request_id pg_attribute_unused(), uint64 convert_request_id pg_attribute_unused())
{
	abort(); /* This fixture exercises only the original successful convert tail. */
}

void *
hash_search(HTAB *table, const void *key, HASHACTION action, bool *found)
{
	bool match = table == LockMethodLocalHash && action == HASH_FIND
		&& memcmp(key, &native_lock.tag, sizeof(native_lock.tag)) == 0;

	if (found != NULL)
		*found = match;
	return match ? &native_lock : NULL;
}

bool
LockRelease(const LOCKTAG *tag, LOCKMODE mode, bool session pg_attribute_unused())
{
	UT_ASSERT(memcmp(tag, &native_lock.tag.lock, sizeof(*tag)) == 0);
	UT_ASSERT_EQ(mode, native_lock.tag.mode);
	native_lock.nLocks = 0;
	return true;
}

static void pg_attribute_noreturn()
pgrac_cluster_post_native_fail(ClusterLockAcquireResult result pg_attribute_unused(),
							   const ClusterLockAcquireRequest *request pg_attribute_unused())
{
	abort(); /* No test treats an ERROR exit as successful registration. */
}

static ClusterLockAcquireResult
promote_with_receipt_fault(const ClusterLockAcquireRequest *request)
{
	ClusterLockAcquireResult result = cluster_lock_acquire_s5_promote(request);
	ClusterGesHwGrant *grant = &((ClusterLockAcquireRequest *)request)->hw_grant;

	/* Faults are injected only after the real S5 has accepted the exact grant. */
	if (result == CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
		switch (receipt_fault) {
		case 1: grant->registration_proof = false; break;
		case 2: grant->consumed = false; break;
		case 3: grant->local_promoted = false; break;
		case 4: grant->cleanup_pending = true; break;
		case 5: grant->master = -1; break;
		case 6: grant->master_generation = 0; break;
		}
	}
	return result;
}

#define cluster_lock_acquire_s5_promote promote_with_receipt_fault
#include "test_cluster_ax_binding_native.inc"
#undef cluster_lock_acquire_s5_promote

static void
binding_setup(ClusterLockAcquireRequest *req, int master, bool nowait, LOCKMODE mode)
{
	ClusterGesHwGrant *grant;

	memset(req, 0, sizeof(*req));
	memset(&native_lock, 0, sizeof(native_lock));
	memset(&native_proc, 0, sizeof(native_proc));
	MyProc = &native_proc;
	MyProc->pgprocno = 7;
	pg_atomic_init_u32(&MyProc->cluster_grd_registered_count, 0);
	cluster_node_id = 0;
	stub_master_node = master;
	stub_redeclare_epoch = 1;
	serving_allowed = true;
	owner_promote_result = CLUSTER_GRD_ENTRY_OK;
	ko_ax_pending = false;
	ko_ax_pending_after = ko_ax_reads = 0;
	promotion_calls = abandon_calls = receipt_fault = 0;
	LockMethodLocalHash = (HTAB *)1;
	SET_LOCKTAG_RELATION(req->locktag, 5, 16385);
	req->resid.type = LOCKTAG_RELATION;
	req->resid.lockmethodid = DEFAULT_LOCKMETHOD;
	req->resid.field1 = 5;
	req->resid.field2 = 16385;
	req->op = CLUSTER_LOCK_OP_REQUEST;
	req->lockmode = mode;
	req->dontwait = nowait;
	req->request_id = UINT64CONST(0x100000023);
	req->holder = (ClusterGrdHolderId){ 0, 7, 1, UINT64CONST(0x100000023) };
	grant = &req->hw_grant;
	grant->master = master;
	grant->master_generation = 1;
	grant->cleanup_pending = grant->grant_observed = true;
	grant->key.source_node_id = 0;
	grant->key.dest_node_id = master;
	grant->key.cluster_epoch = 1;
	grant->key.request_id = req->request_id;
	grant->key.request_opcode = nowait ? GES_REQ_OPCODE_REQUEST_NOWAIT : GES_REQ_OPCODE_REQUEST;
	grant->request.opcode = grant->key.request_opcode;
	grant->request.lockmode = mode;
	grant->request.holder_node_id = 0;
	grant->request.holder_procno = 7;
	grant->request.holder_cluster_epoch_lo = 1;
	grant->request.holder_request_id_lo = 35;
	grant->request.holder_request_id_hi = 1;
	grant->request.shard_master_generation_lo = 1;
	memcpy(grant->request.resid, &req->resid, sizeof(req->resid));
	native_lock.tag.lock = req->locktag;
	native_lock.tag.mode = mode;
	native_lock.nLocks = 1; /* PG native acquisition has already succeeded. */
}

static bool
read_binding(const LOCKTAG *tag)
{
	ClusterGrdHolderId holder = { 0 };
	uint64 request = 0, generation = 0;
	int32 master = -1;
	bool ok = cluster_lock_current_ax_binding(tag, &holder, &request, &master, &generation);

	if (ok) {
		UT_ASSERT_EQ(holder.node_id, 0);
		UT_ASSERT_EQ(holder.procno, 7);
		UT_ASSERT_EQ(holder.cluster_epoch, 1);
		UT_ASSERT_EQ(holder.request_id, UINT64CONST(0x100000023));
		UT_ASSERT_EQ(request, UINT64CONST(0x100000023));
		UT_ASSERT_EQ(master, stub_master_node);
		UT_ASSERT_EQ(generation, 1);
	}
	return ok;
}

UT_TEST(test_successful_s5_transfers_exact_ax_binding)
{
	for (int remote = 0; remote < 2; remote++)
		for (int nowait = 0; nowait < 2; nowait++) {
			ClusterLockAcquireRequest req;

			binding_setup(&req, remote ? 2 : 0, nowait, AccessExclusiveLock);
			UT_ASSERT(cluster_ges_relation_grant_is_current(&req.hw_grant, &req.resid,
				&req.holder, req.request_id, AccessExclusiveLock, nowait));
			pgrac_cluster_post_native_register(&native_lock, &req, NULL,
				&req.locktag, AccessExclusiveLock, false);
			UT_ASSERT(req.hw_grant.registration_proof && req.hw_grant.local_promoted
				&& req.hw_grant.consumed && !req.hw_grant.cleanup_pending);
			/* The old ownership predicate must continue to reject consumed grants. */
			UT_ASSERT(!cluster_ges_relation_grant_is_current(&req.hw_grant, &req.resid,
				&req.holder, req.request_id, AccessExclusiveLock, nowait));
			UT_ASSERT(read_binding(&req.locktag));
			UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
			UT_ASSERT_EQ(promotion_calls, remote);
			UT_ASSERT_EQ(abandon_calls, 0);
		}
}

UT_TEST(test_incomplete_s5_receipt_never_publishes_ax_binding)
{
	for (unsigned fault = 1; fault <= 6; fault++) {
		ClusterLockAcquireRequest req;

		binding_setup(&req, 0, false, AccessExclusiveLock);
		receipt_fault = fault;
		pgrac_cluster_post_native_register(&native_lock, &req, NULL,
			&req.locktag, AccessExclusiveLock, false);
		UT_ASSERT(!read_binding(&req.locktag));
	}
}

UT_TEST(test_original_exact_grant_refusals_do_not_make_a_binding)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		ClusterLockAcquireRequest req;

		binding_setup(&req, 0, false, AccessExclusiveLock);
		switch (fault) {
		case 0: req.hw_grant.request.resid[1] ^= 1; break;
		case 1: req.hw_grant.request.holder_procno++; break;
		case 2: req.hw_grant.request.holder_request_id_hi++; break;
		case 3: stub_redeclare_epoch++; break;
		case 4: req.hw_grant.master_generation++; break;
		case 5: req.hw_grant.request.opcode = GES_REQ_OPCODE_RELEASE; break;
		case 6: serving_allowed = false; break;
		}
		UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
		UT_ASSERT(!req.hw_grant.registration_proof && !req.hw_grant.consumed);
		UT_ASSERT(!read_binding(&req.locktag));
		UT_ASSERT_EQ(abandon_calls, 1);
	}
}

UT_TEST(test_non_ax_or_native_only_lock_has_no_ax_binding)
{
	ClusterLockAcquireRequest req;
	LOCKTAG absent;

	binding_setup(&req, 0, false, AccessShareLock);
	pgrac_cluster_post_native_register(&native_lock, &req, NULL,
		&req.locktag, AccessShareLock, false);
	UT_ASSERT(req.hw_grant.registration_proof && req.hw_grant.consumed);
	UT_ASSERT(!read_binding(&req.locktag));
	binding_setup(&req, 0, false, AccessExclusiveLock);
	UT_ASSERT(!read_binding(&req.locktag));
	absent = req.locktag;
	absent.locktag_field2++;
	UT_ASSERT(!read_binding(&absent));
}

UT_TEST(test_reader_rejects_released_or_changed_local_binding)
{
	for (unsigned fault = 0; fault < 5; fault++) {
		ClusterLockAcquireRequest req;

		binding_setup(&req, 0, false, AccessExclusiveLock);
		pgrac_cluster_post_native_register(&native_lock, &req, NULL,
			&req.locktag, AccessExclusiveLock, false);
		UT_ASSERT(read_binding(&req.locktag));
		switch (fault) {
		case 0: native_lock.nLocks = 0; break;
		case 1: native_lock.cluster_registered = false; break;
		case 2: native_lock.cluster_request_id++; break;
		case 3: native_lock.cluster_ax_binding_valid = false; break;
		case 4: LockMethodLocalHash = NULL; break;
		}
		UT_ASSERT(!read_binding(&req.locktag));
	}
}

UT_TEST(test_convert_without_registration_receipt_keeps_ax_refused)
{
	ClusterLockAcquireRequest req;
	LOCALLOCK old = { 0 };

	binding_setup(&req, 0, false, AccessExclusiveLock);
	req.op = CLUSTER_LOCK_OP_CONVERT;
	req.current_mode = AccessShareLock;
	req.convert_old_request_id = 99;
	memset(&req.hw_grant, 0, sizeof(req.hw_grant));
	old.tag.lock = req.locktag;
	old.tag.mode = AccessShareLock;
	old.nLocks = 1;
	old.cluster_registered = true;
	old.cluster_request_id = 99;
	pg_atomic_write_u32(&MyProc->cluster_grd_registered_count, 1);
	stub_ges_reject_reason = GES_REJECT_REASON_NONE;
	pgrac_cluster_post_native_register(&native_lock, &req, &old,
		&req.locktag, AccessExclusiveLock, false);
	UT_ASSERT(native_lock.cluster_registered && !old.cluster_registered);
	UT_ASSERT_EQ(native_lock.cluster_convert_old_request_id, 99);
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 1);
	UT_ASSERT(!read_binding(&req.locktag));
}

UT_TEST(test_consumed_grant_cannot_be_promoted_again)
{
	ClusterLockAcquireRequest req;

	binding_setup(&req, 2, false, AccessExclusiveLock);
	pgrac_cluster_post_native_register(&native_lock, &req, NULL,
		&req.locktag, AccessExclusiveLock, false);
	UT_ASSERT(read_binding(&req.locktag));
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL);
	UT_ASSERT_STR_EQ(req.registration_failure_reason, "GRANT_ALREADY_CONSUMED");
	UT_ASSERT_EQ(promotion_calls, 1);
	UT_ASSERT_EQ(abandon_calls, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_successful_s5_transfers_exact_ax_binding);
	UT_RUN(test_incomplete_s5_receipt_never_publishes_ax_binding);
	UT_RUN(test_original_exact_grant_refusals_do_not_make_a_binding);
	UT_RUN(test_non_ax_or_native_only_lock_has_no_ax_binding);
	UT_RUN(test_reader_rejects_released_or_changed_local_binding);
	UT_RUN(test_convert_without_registration_receipt_keeps_ax_refused);
	UT_RUN(test_consumed_grant_cannot_be_promoted_again);
	UT_DONE();
	return ut_failed_count != 0;
}
