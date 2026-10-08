/* PGRAC: actual snapshot manager lifecycle and foreign-read evidence.
 * Allocation and ResourceOwner bookkeeping are explicit fixture boundaries;
 * Active/Registered containers, reference counts and retention are native.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "unit_test.h"
#include "cluster/cluster_adg.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_mrp.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_mode.h"
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
static OldSnapshotControlData snapshot_control;
static bool snapshot_control_found;
static CommandId current_command;
static bool poison_allocations;
int cluster_injection_armed_count;
bool cluster_enable_adg;
int cluster_adg_lag_threshold_sec;

bool
cluster_mrp_should_start(void)
{
	return false;
}
SCN
cluster_mrp_standby_consistent_scn(void)
{
	return InvalidScn;
}
int64
cluster_mrp_apply_lag_ms(void)
{
	return 0;
}
bool
cluster_mrp_read_service_available(void)
{
	return false;
}
SCN
cluster_scn_current(void)
{
	return 100;
}
bool
cluster_cr_injection_armed(const char *name, uint64 *param)
{
	return false;
}
#include "test_cluster_snapshot_admission_refresh.inc"


void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	Assert(strcmp(name, "OldSnapshotControlData") == 0);
	Assert(size == offsetof(OldSnapshotControlData, xid_by_minute));
	*found = snapshot_control_found;
	snapshot_control_found = true;
	return &snapshot_control;
}

Size
add_size(Size a, Size b)
{
	return a + b;
}

Size
mul_size(Size a, Size b)
{
	return a * b;
}

CommandId
GetCurrentCommandId(bool used)
{
	return current_command;
}

bool
IsInParallelMode(void)
{
	return false;
}


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
	void *allocation = malloc(size);
	Assert(allocation != NULL);
	memset(allocation, poison_allocations ? 0xa5 : 0, size);
	return allocation;
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

static bool
cr_identity(Snapshot snapshot, uint64 *identity)
{
	ClusterSnapshotReadScopeV1 scope;
	bool result;

	cluster_snapshot_read_enter_v1(&scope, snapshot);
	result = cluster_snapshot_cr_identity_v1(snapshot, identity);
	cluster_snapshot_read_exit_v1(&scope);
	return result;
}

UT_TEST(cr_identity_is_stable_only_for_the_same_live_snapshot)
{
	Snapshot a = registered(100), b = registered(100);
	uint64 first = 0, again = 0, other = 0;

	UT_ASSERT(cr_identity(a, &first));
	UT_ASSERT(first != 0);
	UT_ASSERT(cr_identity(a, &again));
	UT_ASSERT_EQ(first, again);
	UT_ASSERT(cr_identity(b, &other));
	UT_ASSERT(other != 0 && other != first);
	UT_ASSERT(!ActiveSnapshotSet());
	UnregisterSnapshot(b);
	UnregisterSnapshot(a);
}

UT_TEST(cr_identity_copy_and_catalog_address_reuse_do_not_alias)
{
	Snapshot a = registered(100);
	Snapshot copy;
	uint64 first = 0, second = 0, third = 0;

	UT_ASSERT(cr_identity(a, &first));
	copy = CopySnapshot(a);
	UT_ASSERT_EQ(copy->cluster_cr_identity, 0);
	copy = RegisterSnapshot(copy);
	UT_ASSERT(cr_identity(copy, &second));
	UT_ASSERT(second != first);
	UnregisterSnapshot(copy);
	CatalogSnapshotData = *a;
	CatalogSnapshotData.copied = false;
	CatalogSnapshotData.regd_count = 0;
	CatalogSnapshotData.cluster_cr_identity = 0;
	CatalogSnapshot = &CatalogSnapshotData;
	pairingheap_add(&RegisteredSnapshots, &CatalogSnapshot->ph_node);
	cluster_recompute_proc_read_scn();
	UT_ASSERT(cr_identity(CatalogSnapshot, &second));
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(CatalogSnapshotData.cluster_cr_identity, 0);
	CatalogSnapshot = &CatalogSnapshotData;
	pairingheap_add(&RegisteredSnapshots, &CatalogSnapshot->ph_node);
	cluster_recompute_proc_read_scn();
	UT_ASSERT(cr_identity(CatalogSnapshot, &third));
	UT_ASSERT(third != 0 && third != second);
	InvalidateCatalogSnapshot();
	UnregisterSnapshot(a);
}

UT_TEST(cr_identity_command_changes_retire_the_old_identity)
{
	Snapshot a = registered(100);
	uint64 first = 0, second = 0, again = 0;

	CurrentSnapshot = a;
	FirstSnapshotSet = true;
	UT_ASSERT(cr_identity(a, &first));
	SnapshotSetCommandId(1);
	UT_ASSERT(cr_identity(a, &second));
	UT_ASSERT(second != 0 && second != first);
	SnapshotSetCommandId(1);
	UT_ASSERT(cr_identity(a, &again));
	UT_ASSERT_EQ(second, again);
	CurrentSnapshot = NULL;
	FirstSnapshotSet = false;

	PushActiveSnapshot(a);
	UnregisterSnapshot(a);
	current_command = 2;
	UpdateActiveSnapshotCommandId();
	UT_ASSERT(cr_identity(a, &first));
	UT_ASSERT(first != 0 && first != second);
	PopActiveSnapshot();
}

UT_TEST(cr_identity_requires_the_actual_live_evaluator)
{
	Snapshot a = registered(100), b = registered(100);
	SnapshotData fake = *a;
	uint64 identity = 777;

	UT_ASSERT(!cluster_snapshot_cr_identity_v1(a, &identity));
	UT_ASSERT_EQ(identity, 777);
	PushActiveSnapshot(b);
	UT_ASSERT(!cluster_snapshot_cr_identity_v1(a, &identity));
	UT_ASSERT_EQ(identity, 777);
	UT_ASSERT(!cr_identity(&fake, &identity));
	UT_ASSERT_EQ(identity, 777);
	UT_ASSERT(!cluster_snapshot_cr_identity_v1(NULL, &identity));
	UT_ASSERT_EQ(identity, 777);
	UT_ASSERT(cr_identity(a, &identity));
	UT_ASSERT(identity != 777 && identity != 0);
	UnregisterSnapshot(a);
	UT_ASSERT(!cluster_snapshot_cr_identity_v1(a, &identity));
	PopActiveSnapshot();
	UnregisterSnapshot(b);
}

UT_TEST(cr_identity_preserves_snapshot_and_retention_refusals)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		Snapshot a = registered(100);
		ClusterSnapshotReadScopeV1 scope;
		uint64 identity = 777;
		cluster_snapshot_read_enter_v1(&scope, a);
		switch (fault) {
		case 0:
			a->read_scn++;
			break;
		case 1:
			a->read_epoch++;
			break;
		case 2:
			a->cluster_source = SNAPSHOT_SOURCE_LOCAL;
			break;
		case 3:
			a->snapshot_type = SNAPSHOT_SELF;
			break;
		case 4:
			pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 0);
			break;
		case 5:
			pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 101);
			break;
		case 6:
			CurrentResourceOwner = (ResourceOwner)2;
			break;
		case 7:
			a->read_epoch = scope.read_epoch = 6;
			break;
		}
		UT_ASSERT(!cluster_snapshot_cr_identity_v1(a, &identity));
		UT_ASSERT_EQ(identity, 777);
		UT_ASSERT_EQ(a->cluster_cr_identity, 0);
		CurrentResourceOwner = (ResourceOwner)1;
		cluster_snapshot_read_exit_v1(&scope);
		UnregisterSnapshot(a);
	}
}

UT_TEST(cr_identity_exhaustion_does_not_wrap_or_erase_existing_identity)
{
	Snapshot a = registered(100), b = registered(100);
	uint64 saved, first = 0, again = 777;

	UT_ASSERT(cr_identity(a, &first));
	saved = pg_atomic_read_u64(&snapshot_control.cr_identity_generation);
	pg_atomic_write_u64(&snapshot_control.cr_identity_generation, PG_UINT64_MAX);
	UT_ASSERT(!cr_identity(b, &again));
	UT_ASSERT_EQ(again, 777);
	UT_ASSERT_EQ(b->cluster_cr_identity, 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&snapshot_control.cr_identity_generation), PG_UINT64_MAX);
	UT_ASSERT(cr_identity(a, &again));
	UT_ASSERT_EQ(again, first);
	pg_atomic_write_u64(&snapshot_control.cr_identity_generation, saved);
	UnregisterSnapshot(b);
	UnregisterSnapshot(a);
}

UT_TEST(cr_identity_shmem_attach_preserves_the_allocator)
{
	Snapshot a = registered(100), b = registered(100);
	uint64 first = 0, second = 0;

	pg_atomic_write_u64(&snapshot_control.cr_identity_generation, 100);
	snapshot_control_found = false;
	SnapMgrInit();
	UT_ASSERT_EQ(pg_atomic_read_u64(&snapshot_control.cr_identity_generation), 0);
	UT_ASSERT(cr_identity(a, &first));
	UT_ASSERT(first != 0);
	SnapMgrInit();
	UT_ASSERT(cr_identity(b, &second));
	UT_ASSERT(second > first);
	UT_ASSERT_EQ(SnapMgrShmemSize(), offsetof(OldSnapshotControlData, xid_by_minute));
	UnregisterSnapshot(b);
	UnregisterSnapshot(a);
}

UT_TEST(cr_identity_restore_and_snapshot_refresh_start_new_lifetimes)
{
	Snapshot a = registered(100), restored;
	SerializedSnapshotData serialized = { 0 };
	SnapshotData refreshed = { 0 };
	uint64 first = 0, second = 0;

	UT_ASSERT(cr_identity(a, &first));
	UT_ASSERT_EQ(EstimateSnapshotSpace(a), sizeof(SerializedSnapshotData));
	SerializeSnapshot(a, (char *)&serialized);
	poison_allocations = true;
	restored = RestoreSnapshot((char *)&serialized);
	poison_allocations = false;
	UT_ASSERT_EQ(restored->cluster_cr_identity, 0);
	restored = RegisterSnapshot(restored);
	UT_ASSERT(cr_identity(restored, &second));
	UT_ASSERT(first != second && second != 0);
	UnregisterSnapshot(restored);

	refreshed = *a;
	ClusterSnapshotRefreshFields(&refreshed);
	UT_ASSERT_EQ(refreshed.cluster_cr_identity, 0);
	UT_ASSERT_EQ(refreshed.read_scn, a->read_scn);
	UT_ASSERT_EQ(refreshed.read_epoch, a->read_epoch);
	refreshed.cluster_cr_identity = first;
	cluster_enabled = false;
	ClusterSnapshotRefreshFields(&refreshed);
	cluster_enabled = true;
	UT_ASSERT_EQ(refreshed.cluster_cr_identity, 0);
	UT_ASSERT_EQ(refreshed.cluster_source, SNAPSHOT_SOURCE_LOCAL);
	UT_ASSERT_EQ(refreshed.read_scn, InvalidScn);
	UnregisterSnapshot(a);
}

int
main(void)
{
	MyProc = &proc;
	pg_atomic_init_u64(&proc.cluster_read_scn_atomic, 0);
	UndoHorizonShmem = &horizon;
	pg_atomic_init_u64(&horizon.self_admitted_epoch, 8);
	pg_atomic_init_u64(&horizon.admission_refuse_count, 0);
	oldSnapshotControl = &snapshot_control;
	pg_atomic_init_u64(&snapshot_control.cr_identity_generation, 0);
	UT_PLAN(20);
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
	UT_RUN(cr_identity_is_stable_only_for_the_same_live_snapshot);
	UT_RUN(cr_identity_copy_and_catalog_address_reuse_do_not_alias);
	UT_RUN(cr_identity_command_changes_retire_the_old_identity);
	UT_RUN(cr_identity_requires_the_actual_live_evaluator);
	UT_RUN(cr_identity_preserves_snapshot_and_retention_refusals);
	UT_RUN(cr_identity_exhaustion_does_not_wrap_or_erase_existing_identity);
	UT_RUN(cr_identity_shmem_attach_preserves_the_allocator);
	UT_RUN(cr_identity_restore_and_snapshot_refresh_start_new_lifetimes);
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(!ActiveSnapshotSet());
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
