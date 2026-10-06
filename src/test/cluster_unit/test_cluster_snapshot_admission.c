/* PGRAC: actual snapshot manager lifecycle and foreign-read evidence.
 * Allocation and ResourceOwner bookkeeping are explicit fixture boundaries;
 * Active/Registered containers, reference counts and retention are native.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "unit_test.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_undo_horizon.h"
#include "../../backend/utils/time/snapmgr.c"
#include "test_cluster_snapshot_admission_horizon.inc"

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
int cluster_node_id = 1;
PGPROC *MyProc;
ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
MemoryContext TopTransactionContext = (MemoryContext)1;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static PGPROC proc;
static unsigned remembered;
static ClusterUndoHorizonShmem horizon;
static ClusterMembershipState self_member = CLUSTER_MEMBER_MEMBER;
static bool peer_capable = true;
static int error_code;

ClusterMembershipState
cluster_membership_get_state(int node)
{
	return node == cluster_node_id ? self_member : CLUSTER_MEMBER_MEMBER;
}
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int node)
{
	static ClusterNodeInfo peer;
	return node == 0 ? &peer : NULL;
}
bool
cluster_sf_peer_supports_undo_horizon(int node)
{
	return peer_capable;
}
bool
errstart(int elevel, const char *domain)
{
	return elevel >= ERROR;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
int
errcode(int code)
{
	error_code = code;
	return 0;
}
int
errmsg(const char *fmt, ...)
{
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	return 0;
}
int
errhint(const char *fmt, ...)
{
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	pg_re_throw();
}

/* Native xid ordering; snapshot lifetime and retention are not stubbed. */
bool
TransactionIdPrecedes(TransactionId a, TransactionId b)
{
	return !TransactionIdIsNormal(a) || !TransactionIdIsNormal(b) ? a < b : (int32)(a - b) < 0;
}
bool
TransactionIdFollows(TransactionId a, TransactionId b)
{
	return !TransactionIdIsNormal(a) || !TransactionIdIsNormal(b) ? a > b : (int32)(a - b) > 0;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
void *
MemoryContextAlloc(MemoryContext context, Size size)
{
	return calloc(1, size);
}
void *
palloc(Size size)
{
	return MemoryContextAlloc(CurrentMemoryContext, size);
}
void
pfree(void *p)
{
	free(p);
}
void
ResourceOwnerEnlargeSnapshots(ResourceOwner owner)
{
	Assert(owner != NULL);
}
void
ResourceOwnerRememberSnapshot(ResourceOwner owner, Snapshot snapshot)
{
	remembered++;
}
void
ResourceOwnerForgetSnapshot(ResourceOwner owner, Snapshot snapshot)
{
	Assert(remembered);
	remembered--;
}
int
GetCurrentTransactionNestLevel(void)
{
	return 1;
}
int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) < scn_local(b) ? -1 : scn_local(a) > scn_local(b) ? 1 : 0;
}
void
pg_re_throw(void)
{
	Assert(PG_exception_stack != NULL);
	siglongjmp(*PG_exception_stack, 1);
}

static Snapshot
registered(SCN scn)
{
	SnapshotData value = { 0 };
	value.snapshot_type = SNAPSHOT_MVCC;
	value.xmin = 10;
	value.xmax = 20;
	value.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	value.cluster_snapshot_session_local = 1;
	value.read_scn = scn;
	value.read_epoch = 7;
	return RegisterSnapshot(&value);
}

static bool
evidence(SCN scn, Snapshot expected)
{
	Snapshot actual = NULL;
	SCN floor = InvalidScn;
	const char *reason = NULL;
	bool ok = cluster_snapshot_read_evidence_v1(scn, &actual, &floor, &reason);
	if (ok) {
		UT_ASSERT(actual == expected);
		UT_ASSERT(reason == NULL);
		UT_ASSERT(SCN_VALID(floor) && scn_time_cmp(floor, actual->read_scn) <= 0);
	} else
		UT_ASSERT(reason != NULL);
	return ok;
}

UT_TEST(registered_catalog_is_live_without_becoming_active)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT_EQ(s->regd_count, 1);
	UT_ASSERT_EQ(s->active_count, 0);
	cluster_snapshot_read_enter_v1(&scope, s);
	UT_ASSERT(evidence(100, s));
	UT_ASSERT(!ActiveSnapshotSet());
	cluster_snapshot_read_exit_v1(&scope);
	UT_ASSERT(!evidence(100, NULL));
	UnregisterSnapshot(s);
}

UT_TEST(evaluated_registered_snapshot_overrides_an_unrelated_active_snapshot)
{
	Snapshot active = registered(70), scanned = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	PushActiveSnapshot(active);
	cluster_snapshot_read_enter_v1(&scope, scanned);
	UT_ASSERT(evidence(100, scanned));
	UT_ASSERT(!evidence(70, active));
	UT_ASSERT(GetActiveSnapshot() == active);
	cluster_snapshot_read_exit_v1(&scope);
	UT_ASSERT(evidence(70, active));
	PopActiveSnapshot();
	UnregisterSnapshot(scanned);
	UnregisterSnapshot(active);
}

UT_TEST(forged_reference_counts_do_not_establish_liveness)
{
	SnapshotData fake = { 0 };
	Snapshot keep = registered(50);
	ClusterSnapshotReadScopeV1 scope;
	fake = *keep;
	cluster_snapshot_read_enter_v1(&scope, &fake);
	UT_ASSERT(!evidence(50, &fake));
	cluster_snapshot_read_exit_v1(&scope);
	UnregisterSnapshot(keep);
}

UT_TEST(released_snapshot_cannot_be_dereferenced_or_reused_as_a_proof)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	cluster_snapshot_read_enter_v1(&scope, s);
	UnregisterSnapshot(s);
	UT_ASSERT(!evidence(100, NULL));
	s = registered(100); /* allocator is free to reuse the same address */
	UT_ASSERT(!evidence(100, NULL));
	cluster_snapshot_read_exit_v1(&scope);
	UnregisterSnapshot(s);
}

UT_TEST(snapshot_identity_and_retention_cannot_drift_during_evaluation)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		Snapshot s = registered(100);
		ClusterSnapshotReadScopeV1 scope;
		cluster_snapshot_read_enter_v1(&scope, s);
		switch (fault) {
		case 0:
			s->read_scn++;
			break;
		case 1:
			s->read_epoch++;
			break;
		case 2:
			s->cluster_source = SNAPSHOT_SOURCE_LOCAL;
			break;
		case 3:
			pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 0);
			break;
		case 4:
			pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 101);
			break;
		case 5:
			CurrentResourceOwner = (ResourceOwner)2;
			break;
		case 6:
			s->snapshot_type = SNAPSHOT_SELF;
			break;
		}
		UT_ASSERT(!evidence(100, s));
		CurrentResourceOwner = (ResourceOwner)1;
		cluster_snapshot_read_exit_v1(&scope);
		UnregisterSnapshot(s);
	}
}

UT_TEST(nested_evaluations_restore_the_actual_outer_snapshot)
{
	Snapshot a = registered(90), b = registered(100);
	ClusterSnapshotReadScopeV1 outer, inner;
	cluster_snapshot_read_enter_v1(&outer, a);
	cluster_snapshot_read_enter_v1(&inner, b);
	UT_ASSERT(evidence(100, b));
	cluster_snapshot_read_exit_v1(&inner);
	UT_ASSERT(evidence(90, a));
	cluster_snapshot_read_exit_v1(&outer);
	UT_ASSERT(!evidence(90, NULL));
	UnregisterSnapshot(b);
	UnregisterSnapshot(a);
}

UT_TEST(error_cleanup_restores_the_outer_evaluation)
{
	Snapshot a = registered(90), b = registered(100);
	ClusterSnapshotReadScopeV1 outer, inner;
	volatile bool caught = false;
	cluster_snapshot_read_enter_v1(&outer, a);
	PG_TRY();
	{
		cluster_snapshot_read_enter_v1(&inner, b);
		PG_TRY(inner);
		{
			siglongjmp(*PG_exception_stack, 1);
		}
		PG_FINALLY(inner);
		{
			cluster_snapshot_read_exit_v1(&inner);
		}
		PG_END_TRY(inner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT(evidence(90, a));
	cluster_snapshot_read_exit_v1(&outer);
	UnregisterSnapshot(b);
	UnregisterSnapshot(a);
}

UT_TEST(original_active_lifetime_remains_valid_after_unregister)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	PushActiveSnapshot(s);
	cluster_snapshot_read_enter_v1(&scope, s);
	UnregisterSnapshot(s);
	UT_ASSERT(evidence(100, s));
	PopActiveSnapshot();
	UT_ASSERT(!evidence(100, NULL));
	cluster_snapshot_read_exit_v1(&scope);
}

static bool
admission_rejected(SCN scn)
{
	volatile bool caught = false;
	error_code = 0;
	PG_TRY();
	{
		(void)cluster_undo_horizon_read_admission_enforce(scn);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(!caught || error_code == ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS);
	return caught;
}

UT_TEST(actual_registered_admission_preserves_epoch_and_member_gates)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	cluster_snapshot_read_enter_v1(&scope, s);
	UT_ASSERT(cluster_undo_horizon_read_admission_enforce(100));
	pg_atomic_write_u64(&horizon.self_admitted_epoch, 9);
	UT_ASSERT(admission_rejected(100));
	pg_atomic_write_u64(&horizon.self_admitted_epoch, 0);
	UT_ASSERT(admission_rejected(100));
	pg_atomic_write_u64(&horizon.self_admitted_epoch, 8);
	self_member = CLUSTER_MEMBER_JOINING;
	UT_ASSERT(admission_rejected(100));
	self_member = CLUSTER_MEMBER_MEMBER;
	UT_ASSERT(admission_rejected(101));
	UT_ASSERT(cluster_undo_horizon_read_admission_enforce(100));
	cluster_snapshot_read_exit_v1(&scope);
	UT_ASSERT(admission_rejected(100));
	UnregisterSnapshot(s);
}

UT_TEST(actual_registered_admission_preserves_current_peer_capability_gate)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	uint64 before = pg_atomic_read_u64(&horizon.admission_refuse_count);
	cluster_snapshot_read_enter_v1(&scope, s);
	peer_capable = false;
	UT_ASSERT(!cluster_undo_horizon_read_admission_enforce(100));
	UT_ASSERT_EQ(pg_atomic_read_u64(&horizon.admission_refuse_count), before + 1);
	peer_capable = true;
	UT_ASSERT(cluster_undo_horizon_read_admission_enforce(100));
	cluster_snapshot_read_exit_v1(&scope);
	UnregisterSnapshot(s);
}

UT_TEST(catalog_invalidation_ends_static_snapshot_evidence)
{
	ClusterSnapshotReadScopeV1 scope;
	Snapshot s = registered(100);
	CatalogSnapshotData = *s;
	CatalogSnapshotData.copied = false;
	CatalogSnapshotData.regd_count = 0;
	CatalogSnapshot = &CatalogSnapshotData;
	UnregisterSnapshot(s);
	pairingheap_add(&RegisteredSnapshots, &CatalogSnapshot->ph_node);
	cluster_recompute_proc_read_scn();
	cluster_snapshot_read_enter_v1(&scope, CatalogSnapshot);
	UT_ASSERT(evidence(100, CatalogSnapshot));
	InvalidateCatalogSnapshot();
	UT_ASSERT(!evidence(100, NULL));
	cluster_snapshot_read_exit_v1(&scope);
}

UT_TEST(terminal_consumption_retains_only_original_active_boundary)
{
	Snapshot s = registered(100);
	ClusterSnapshotReadScopeV1 scope;
	UT_ASSERT(!evidence(InvalidScn, NULL));
	PushActiveSnapshot(s);
	UT_ASSERT(evidence(InvalidScn, s));
	cluster_snapshot_read_enter_v1(&scope, s);
	UT_ASSERT(!evidence(InvalidScn, s));
	cluster_snapshot_read_exit_v1(&scope);
	PopActiveSnapshot();
	UnregisterSnapshot(s);
}

int
main(void)
{
	MyProc = &proc;
	pg_atomic_init_u64(&proc.cluster_read_scn_atomic, 0);
	UndoHorizonShmem = &horizon;
	pg_atomic_init_u64(&horizon.self_admitted_epoch, 8);
	pg_atomic_init_u64(&horizon.admission_refuse_count, 0);
	UT_PLAN(12);
	UT_RUN(registered_catalog_is_live_without_becoming_active);
	UT_RUN(evaluated_registered_snapshot_overrides_an_unrelated_active_snapshot);
	UT_RUN(forged_reference_counts_do_not_establish_liveness);
	UT_RUN(released_snapshot_cannot_be_dereferenced_or_reused_as_a_proof);
	UT_RUN(snapshot_identity_and_retention_cannot_drift_during_evaluation);
	UT_RUN(nested_evaluations_restore_the_actual_outer_snapshot);
	UT_RUN(error_cleanup_restores_the_outer_evaluation);
	UT_RUN(original_active_lifetime_remains_valid_after_unregister);
	UT_RUN(actual_registered_admission_preserves_epoch_and_member_gates);
	UT_RUN(actual_registered_admission_preserves_current_peer_capability_gate);
	UT_RUN(catalog_invalidation_ends_static_snapshot_evidence);
	UT_RUN(terminal_consumption_retains_only_original_active_boundary);
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(!ActiveSnapshotSet());
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
