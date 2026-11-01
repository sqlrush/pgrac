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
#include "cluster/cluster_heap_horizon.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "access/twophase.h"
#include "test_cluster_pgproc_active_types.inc"
/* Original source uses barriers unchanged; optional test scheduling is NULL
 * outside the new identity-recheck cases. No product injection hook. */
static void (*snapshot_return_schedule)(void);
static void snapshot_return_read_barrier(void)
{
	pg_read_barrier_impl();
	if (snapshot_return_schedule != NULL) snapshot_return_schedule();
}
#undef pg_read_barrier
#define pg_read_barrier() snapshot_return_read_barrier()
#include "../../backend/utils/time/snapmgr.c"
#include "test_cluster_shared_admission_observe.inc"
#include "test_cluster_backend_return_gate.inc"
#undef pg_read_barrier
#define pg_read_barrier() pg_read_barrier_impl()
#include "test_cluster_snapshot_admission_horizon.inc"

/* Use the original private owner and BufferIO arrays. Snapshot and buffer-pin
 * accounting remain the existing host boundaries; their unused native twins
 * are renamed rather than substituting the BufferIO lifecycle under test. */
#define CurTransactionResourceOwner fixture_native_transaction_owner
#define TopTransactionResourceOwner fixture_native_top_owner
#define AuxProcessResourceOwner fixture_native_aux_owner
#define ResourceOwnerEnlargeSnapshots fixture_native_enlarge_snapshots
#define ResourceOwnerRememberSnapshot fixture_native_remember_snapshot
#define ResourceOwnerForgetSnapshot fixture_native_forget_snapshot
#define ResourceOwnerRememberBuffer fixture_native_remember_buffer
#define ResourceOwnerForgetBuffer fixture_native_forget_buffer
void fixture_native_enlarge_snapshots(ResourceOwner owner);
void fixture_native_remember_snapshot(ResourceOwner owner, Snapshot snapshot);
void fixture_native_forget_snapshot(ResourceOwner owner, Snapshot snapshot);
void fixture_native_remember_buffer(ResourceOwner owner, Buffer buffer);
void fixture_native_forget_buffer(ResourceOwner owner, Buffer buffer);
#include "../../backend/utils/resowner/resowner.c"
#undef ResourceOwnerForgetBuffer
#undef ResourceOwnerRememberBuffer
#undef ResourceOwnerForgetSnapshot
#undef ResourceOwnerRememberSnapshot
#undef ResourceOwnerEnlargeSnapshots
#undef AuxProcessResourceOwner
#undef TopTransactionResourceOwner
#undef CurTransactionResourceOwner
#include "../../common/hashfn.c"

/* Execute the original private transaction state accessors. Existing snapshot
 * command/parallel/nesting host boundaries remain; their unused native twins
 * are renamed. No transaction begin/commit/abort effects are stubbed as success. */
#define XactIsoLevel fixture_native_xact_iso_level
#define GetTopTransactionIdIfAny fixture_native_xact_top_id
#define IsSubTransaction fixture_native_xact_is_sub
#define xactGetCommittedChildren fixture_native_xact_children
TransactionId fixture_native_xact_top_id(void);
bool fixture_native_xact_is_sub(void);
int fixture_native_xact_children(TransactionId **ptr);
#define GetCurrentCommandId fixture_native_xact_command_id
#define GetCurrentTransactionNestLevel fixture_native_xact_nesting
#define IsInParallelMode fixture_native_xact_parallel
CommandId fixture_native_xact_command_id(bool used);
int fixture_native_xact_nesting(void);
bool fixture_native_xact_parallel(void);
#include "../../backend/access/transam/xact.c"
#undef IsInParallelMode
#undef GetCurrentTransactionNestLevel
#undef GetCurrentCommandId
#undef xactGetCommittedChildren
#undef IsSubTransaction
#undef GetTopTransactionIdIfAny
#undef XactIsoLevel

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
int cluster_node_id = 1;
PGPROC *MyProc;
static ResourceOwner backend_resource_one, backend_resource_two;
MemoryContext TopMemoryContext = (MemoryContext)1;
MemoryContext TopTransactionContext = (MemoryContext)1;
MemoryContext CurrentMemoryContext = (MemoryContext)1;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static PGPROC snapshot_procs[2];
#define proc snapshot_procs[0]
static PGPROC *allProcs = &proc;
static ProcArrayStruct *procArray;
PROC_HDR *ProcGlobal;
static PROC_HDR snapshot_proc_header;
static uint8 snapshot_status_flags[2];
static bool import_install_active, import_restored, import_probe_after_install;
static bool import_gap_refused, import_gap_xmin_installed;
static bool import_observe_install;
static SCN import_scn_at_unlock;
static LWLockMode import_install_mode;
static void snapshot_import_after_procarray_unlock(void);
static bool retention_lock, retention_probe_before_publish, retention_gap_refused;
static unsigned retention_acquires, retention_releases, retention_clocks;
static unsigned remembered;
static ClusterUndoHorizonShmem horizon;
static ClusterMembershipState self_member = CLUSTER_MEMBER_MEMBER;
static bool peer_capable = true;
static int error_code;
static OldSnapshotControlData snapshot_control;
static bool snapshot_control_found;
static CommandId current_command;
static bool poison_allocations;
static SCN snapshot_clock = 100;
static bool catalog_services = true;
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
	if (retention_lock)
		retention_clocks++;
	return snapshot_clock;
}
/* Only service posture, resolver recursion and catalogue membership are
 * boundary doubles.  The original catalog registration/retirement and
 * PGPROC retention publisher execute from snapmgr.c below. */
bool
cluster_catalog_services_ready(void)
{
	return catalog_services;
}
bool
cluster_vis_resolve_in_flight(void)
{
	return false;
}
bool
RelationInvalidatesSnapshotsOnly(Oid relid)
{
	return false;
}
bool
RelationHasSysCache(Oid relid)
{
	return true;
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
static uint64 snapshot_return_epoch = 7;
uint64
cluster_epoch_get_current(void)
{
	return snapshot_return_epoch;
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
void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *allocation = MemoryContextAlloc(context, size);
	memset(allocation, 0, size);
	return allocation;
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
			CurrentResourceOwner = backend_resource_two;
			break;
		case 6:
			s->snapshot_type = SNAPSHOT_SELF;
			break;
		}
		UT_ASSERT(!evidence(100, s));
		CurrentResourceOwner = backend_resource_one;
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
			CurrentResourceOwner = backend_resource_two;
			break;
		case 7:
			a->read_epoch = scope.read_epoch = 6;
			break;
		}
		UT_ASSERT(!cluster_snapshot_cr_identity_v1(a, &identity));
		UT_ASSERT_EQ(identity, 777);
		UT_ASSERT_EQ(a->cluster_cr_identity, 0);
		CurrentResourceOwner = backend_resource_one;
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

/* PGRAC: catalogue I/O leaves around the real maintenance entry and snapshots. */
#include "test_cluster_snapshot_vacuum_fixture.inc"
#undef proc
#include "test_cluster_retention_sample.inc"
#include "test_cluster_snapshot_import_xmin.inc"
#define proc snapshot_procs[0]

/* Use the actual catalog getter, pairing heap, publisher and invalidation;
 * GetSnapshotData supplies controlled source fields through the existing
 * actual ClusterSnapshotRefreshFields leaf, not a retention publication. */
UT_TEST(catalog_getter_publishes_its_original_snapshot_before_return)
{
	Snapshot snapshot;

	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	snapshot = GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT(snapshot == CatalogSnapshot);
	UT_ASSERT(pairingheap_is_singular(&RegisteredSnapshots));
	UT_ASSERT(!ActiveSnapshotSet());
	UT_ASSERT_EQ(snapshot->cluster_source, SNAPSHOT_SOURCE_CLUSTER);
	UT_ASSERT_EQ(snapshot->read_scn, 100);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), snapshot->read_scn);
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
}

UT_TEST(catalog_publication_keeps_an_older_original_registered_protection)
{
	Snapshot original = registered(60);
	Snapshot catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);

	UT_ASSERT_EQ(catalog->read_scn, 100);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 60);
	UnregisterSnapshot(original);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), catalog->read_scn);
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
}

UT_TEST(catalog_service_posture_rebuild_publishes_only_the_original_cluster_snapshot)
{
	Snapshot local, cluster;

	catalog_services = false;
	local = GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT_EQ(local->cluster_source, SNAPSHOT_SOURCE_LOCAL);
	UT_ASSERT_EQ(local->read_scn, InvalidScn);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	catalog_services = true;
	cluster = GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT_EQ(cluster->cluster_source, SNAPSHOT_SOURCE_CLUSTER);
	UT_ASSERT_EQ(cluster->read_scn, 100);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), cluster->read_scn);
	UT_ASSERT(pairingheap_is_singular(&RegisteredSnapshots));
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
}

UT_TEST(catalog_cache_keeps_its_original_h_until_actual_invalidation)
{
	Snapshot first = GetNonHistoricCatalogSnapshot(RelationRelationId);
	Snapshot cached, replacement;

	snapshot_clock = 200;
	cached = GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT(cached == first);
	UT_ASSERT_EQ(cached->read_scn, 100);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 100);
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	replacement = GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT_EQ(replacement->read_scn, 200);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 200);
	UT_ASSERT(pairingheap_is_singular(&RegisteredSnapshots));
	InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	snapshot_clock = 100;
}

/* Execute the actual original sampler, not a replacement for its H result.
 * The single-entry ProcArray and lock scheduling below remain controlled. */
static bool
catalog_history_sample(ClusterUndoRetentionSample *sample)
{
	unsigned acquires = retention_acquires;
	unsigned releases = retention_releases;
	unsigned clocks = retention_clocks;
	bool valid;

	valid = cluster_undo_retention_horizon_sample(sample);
	UT_ASSERT_EQ(retention_acquires, acquires + 1);
	UT_ASSERT_EQ(retention_releases, releases + 1);
	UT_ASSERT_EQ(retention_clocks, clocks + 1);
	UT_ASSERT(!retention_lock);
	return valid;
}

#include "test_cluster_snapshot_import_fixture.inc"
#include "test_cluster_snapshot_export_fixture.inc"

UT_TEST(catalog_original_publication_completes_the_real_h_sample)
{
	ClusterUndoRetentionSample sample;
	Snapshot catalog;

	retention_probe_before_publish = true;
	retention_gap_refused = false;
	catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);
	retention_probe_before_publish = false;
	UT_ASSERT(retention_gap_refused);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, catalog->read_scn);
	UT_ASSERT_EQ(sample.clock_scn, 100);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	InvalidateCatalogSnapshot();
}

UT_TEST(catalog_h_sample_preserves_other_original_snapshot_protection)
{
	ClusterUndoRetentionSample sample;
	Snapshot older = registered(60);
	Snapshot catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);

	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, older->read_scn);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UnregisterSnapshot(older);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, catalog->read_scn);
	InvalidateCatalogSnapshot();
}

UT_TEST(catalog_actual_retirement_releases_only_its_original_h_pin)
{
	ClusterUndoRetentionSample sample;
	Snapshot catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);

	snapshot_clock = 300;
	UT_ASSERT(GetNonHistoricCatalogSnapshot(RelationRelationId) == catalog);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 100);
	UT_ASSERT_EQ(sample.clock_scn, 300);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	InvalidateCatalogSnapshot();
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 300);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	UT_ASSERT_EQ(sample.contributor_token, 0);
	snapshot_clock = 100;
}

UT_TEST(catalog_local_source_cannot_borrow_the_clock_as_h_evidence)
{
	ClusterUndoRetentionSample sample, zero = { 0 };

	catalog_services = false;
	GetNonHistoricCatalogSnapshot(RelationRelationId);
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!catalog_history_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	catalog_services = true;
	GetNonHistoricCatalogSnapshot(RelationRelationId);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 100);
	InvalidateCatalogSnapshot();
}

UT_TEST(catalog_scn_does_not_supply_independent_replication_history)
{
	ClusterUndoRetentionSample sample, zero = { 0 };
	Snapshot catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);

	procArray->replication_slot_catalog_xmin = catalog->xmin;
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!catalog_history_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	procArray->replication_slot_catalog_xmin = InvalidTransactionId;
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, catalog->read_scn);
	InvalidateCatalogSnapshot();
}

UT_TEST(original_catalog_pin_cannot_supply_a_missing_finite_clock)
{
	ClusterUndoRetentionSample sample, zero = { 0 };
	Snapshot catalog = GetNonHistoricCatalogSnapshot(RelationRelationId);

	snapshot_clock = (SCN) UINT64_MAX;
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!catalog_history_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), catalog->read_scn);
	snapshot_clock = 300;
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, catalog->read_scn);
	UT_ASSERT_EQ(sample.clock_scn, 300);
	InvalidateCatalogSnapshot();
	snapshot_clock = 100;
}

UT_TEST(mixed_registered_sources_cannot_hide_unqualified_h)
{
	const SCN unknown[] = { InvalidScn, (SCN) UINT64_MAX };

	for (unsigned variant = 0; variant < lengthof(unknown); variant++)
	{
		for (unsigned reverse = 0; reverse < 2; reverse++)
		{
			ClusterUndoRetentionSample sample, rejected, zero = { 0 };
			Snapshot finite, unqualified;
			SCN published;
			bool refused;

			/* The original native xmin remains installed throughout this cut. */
			proc.xmin = 10;
			finite = reverse ? NULL : registered(40);
			unqualified = registered(unknown[variant]);
			if (reverse)
				finite = registered(40);
			published = pg_atomic_read_u64(&proc.cluster_read_scn_atomic);
			memset(&rejected, 0xa5, sizeof(rejected));
			refused = !catalog_history_sample(&rejected);
			/* Clean up before assertions so exact-parent RED stays isolated. */
			UnregisterSnapshot(unqualified);
			UT_ASSERT(catalog_history_sample(&sample));
			UT_ASSERT_EQ(sample.horizon_scn, 40);
			UnregisterSnapshot(finite);
			UT_ASSERT(catalog_history_sample(&sample));
			UT_ASSERT_EQ(sample.horizon_scn, 100);
			UT_ASSERT_EQ(published, unknown[variant]);
			UT_ASSERT(refused);
			UT_ASSERT_EQ(memcmp(&rejected, &zero, sizeof(rejected)), 0);
		}
	}
}

UT_TEST(mixed_active_sources_cannot_hide_unqualified_h)
{
	const SCN unknown[] = { InvalidScn, (SCN) UINT64_MAX };

	for (unsigned variant = 0; variant < lengthof(unknown); variant++)
	{
		for (unsigned reverse = 0; reverse < 2; reverse++)
		{
			SnapshotData finite = { 0 }, unqualified;
			ClusterUndoRetentionSample sample, zero = { 0 }, rejected;
			SCN published;
			bool refused, remaining_known;

			finite.snapshot_type = SNAPSHOT_MVCC;
			finite.xmin = 10;
			finite.xmax = 20;
			finite.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
			finite.read_epoch = 7;
			finite.read_scn = 40;
			unqualified = finite;
			unqualified.read_scn = unknown[variant];
			proc.xmin = 10;
			PushActiveSnapshot(reverse ? &unqualified : &finite);
			PushActiveSnapshot(reverse ? &finite : &unqualified);
			published = pg_atomic_read_u64(&proc.cluster_read_scn_atomic);
			memset(&rejected, 0xa5, sizeof(rejected));
			refused = !catalog_history_sample(&rejected);
			PopActiveSnapshot();
			remaining_known = catalog_history_sample(&sample);
			PopActiveSnapshot();
			UT_ASSERT_EQ(published, unknown[variant]);
			UT_ASSERT(refused);
			UT_ASSERT_EQ(memcmp(&rejected, &zero, sizeof(rejected)), 0);
			UT_ASSERT_EQ(remaining_known, !reverse);
			if (!reverse)
				UT_ASSERT_EQ(sample.horizon_scn, 40);
			UT_ASSERT(catalog_history_sample(&sample));
			UT_ASSERT_EQ(sample.horizon_scn, 100);
		}
	}
}

UT_TEST(original_registered_nonfinite_reader_keeps_h_unqualified_until_retirement)
{
	ClusterUndoRetentionSample sample, zero = { 0 };
	Snapshot original = registered((SCN) UINT64_MAX);

	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), (SCN) UINT64_MAX);
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!catalog_history_sample(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	UnregisterSnapshot(original);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 100);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
}

#include "test_cluster_snapshot_return.inc"
#include "test_cluster_backend_return.inc"
#include "test_cluster_survivor_producer_backend.inc"

#include "test_cluster_survivor_buffer_fixture.h"
#include "test_cluster_survivor_buffer_return.inc"
#include "test_cluster_survivor_return_context.inc"
#include "test_cluster_survivor_xact_return.inc"
int
main(void)
{
	setvbuf(stdout, NULL, _IOLBF, 0);
	backend_resource_one = ResourceOwnerCreate(NULL, "backend-one");
	backend_resource_two = ResourceOwnerCreate(NULL, "backend-two");
	CurrentResourceOwner = backend_resource_one;
	MyProc = &proc;
	proc.pid = 1234;
	procArray = calloc(1, sizeof(ProcArrayStruct) + 2 * sizeof(int));
	if (procArray == NULL)
		abort();
	procArray->maxProcs = 2;
	procArray->numProcs = 1;
	procArray->pgprocnos[0] = 0;
	procArray->pgprocnos[1] = 1;
	ProcGlobal = &snapshot_proc_header;
	ProcGlobal->statusFlags = snapshot_status_flags;
	pg_atomic_init_u64(&proc.cluster_read_scn_atomic, 0);
	UndoHorizonShmem = &horizon;
	pg_atomic_init_u64(&horizon.self_admitted_epoch, 8);
	pg_atomic_init_u64(&horizon.admission_refuse_count, 0);
	oldSnapshotControl = &snapshot_control;
	pg_atomic_init_u64(&snapshot_control.cr_identity_generation, 0);
	UT_PLAN(132);
	UT_RUN(export_original_registration_publishes_h_before_file_visibility);
	UT_RUN(export_original_registration_preserves_other_older_snapshot);
	UT_RUN(export_errors_retain_original_h_until_transaction_cleanup);
	UT_RUN(imported_snapshot_original_xmin_handoff_survives_source_exit);
	UT_RUN(imported_xmin_does_not_borrow_the_receivers_previous_scn);
	UT_RUN(restored_xmin_does_not_borrow_the_receivers_previous_scn);
	UT_RUN(refused_xmin_install_preserves_the_receivers_original_scn);
	UT_RUN(restored_snapshot_original_xmin_handoff_survives_source_exit);
	UT_RUN(imported_finite_history_requires_original_source_scn_coverage);
	UT_RUN(restored_finite_history_requires_original_source_scn_coverage);
	UT_RUN(original_native_xmin_contract_and_older_source_protection_remain);
	UT_RUN(parallel_serialized_snapshot_retains_original_handoff);
	UT_RUN(parallel_serialized_unknown_history_cannot_borrow_clock);
	UT_RUN(parallel_serialized_copy_cannot_replace_original_source_admission);
	UT_RUN(imported_snapshot_rejects_missing_or_unprotected_original_source);
	UT_RUN(restored_snapshot_rejects_unprotected_original_source);
	UT_RUN(original_catalog_pin_cannot_supply_a_missing_finite_clock);
	UT_RUN(original_registered_nonfinite_reader_keeps_h_unqualified_until_retirement);
	UT_RUN(mixed_registered_sources_cannot_hide_unqualified_h);
	UT_RUN(mixed_active_sources_cannot_hide_unqualified_h);
	UT_RUN(catalog_original_publication_completes_the_real_h_sample);
	UT_RUN(catalog_h_sample_preserves_other_original_snapshot_protection);
	UT_RUN(catalog_actual_retirement_releases_only_its_original_h_pin);
	UT_RUN(catalog_local_source_cannot_borrow_the_clock_as_h_evidence);
	UT_RUN(catalog_scn_does_not_supply_independent_replication_history);
	UT_RUN(catalog_getter_publishes_its_original_snapshot_before_return);
	UT_RUN(catalog_publication_keeps_an_older_original_registered_protection);
	UT_RUN(catalog_service_posture_rebuild_publishes_only_the_original_cluster_snapshot);
	UT_RUN(catalog_cache_keeps_its_original_h_until_actual_invalidation);
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
	UT_RUN(vacuum_foreign_terminal_read_has_owned_snapshot);
	UT_RUN(vacuum_registered_only_is_not_terminal_admission);
	UT_RUN(vacuum_early_and_clean_returns_balance_ownership);
	UT_RUN(vacuum_borrowed_snapshot_keeps_identity);
	UT_RUN(vacuum_errors_use_original_abort_cleanup);
	UT_RUN(vacuum_misplaced_owned_snapshot_is_runtime_error);
	UT_RUN(vacuum_nontarget_modes_keep_original_body);
	UT_RUN(vacuum_direct_worker_entry_needs_no_table_snapshot);
	UT_RUN(vacuum_stale_borrowed_snapshot_is_not_refreshed);
	UT_RUN(vacuum_shared_relstats_keeps_newer_locked_frozen_xid);
	UT_RUN(vacuum_shared_database_keeps_newer_locked_frozen_xid);
	UT_RUN(vacuum_shared_relstats_keeps_invalid_and_equal_candidates);
	UT_RUN(vacuum_shared_relstats_retains_native_wrap_order);
	UT_RUN(vacuum_shared_frozen_merge_keeps_independent_mx_path);
	UT_RUN(vacuum_nontarget_frozen_merge_keeps_native_repair);
	UT_RUN(vacuum_relstats_rejects_future_candidate);
	UT_RUN(vacuum_relstats_rejects_future_locked_current);
	UT_RUN(vacuum_relstats_rejects_half_space_current);
	UT_RUN(vacuum_relstats_missing_promise_preserves_xid);
	UT_RUN(vacuum_relstats_revoked_promise_preserves_xid);
	UT_RUN(vacuum_relstats_invalid_reference_and_current_preserve_xid);
	UT_RUN(vacuum_relstats_uses_durable_legal_bound);
	UT_RUN(vacuum_relstats_unknown_window_keeps_stats_and_mx);
	UT_RUN(vacuum_relstats_accepts_legal_next_boundary);
	UT_RUN(vacuum_database_rejects_future_values);
	UT_RUN(vacuum_database_rejects_half_space_current);
	UT_RUN(vacuum_database_unavailable_promise_preserves_xid);
	UT_RUN(vacuum_database_invalid_window_preserves_xid);
	UT_RUN(vacuum_database_accepts_durable_bound_and_next_boundary);
	UT_RUN(vacuum_database_retains_full_xid_wrap_order);
	UT_RUN(vacuum_database_unknown_window_keeps_independent_mx);
	UT_RUN(vacuum_database_scan_accepts_foreign_xid_and_keeps_seed);
	UT_RUN(vacuum_database_scan_uses_full_xid_minimum_across_wrap);
	UT_RUN(vacuum_database_scan_rejects_invalid_later_xid);
	UT_RUN(vacuum_database_scan_unknown_window_keeps_independent_mx);
	UT_RUN(vacuum_database_scan_rechecks_fence_after_catalog_read);
	UT_RUN(vacuum_database_scan_rejects_invalid_seed_before_fold);
	UT_RUN(vacuum_database_scan_nontarget_keeps_native_future_refusal);
	UT_RUN(vacuum_database_scan_keeps_mx_future_refusal);
	UT_RUN(original_registered_snapshot_requires_independent_return_observation);
	UT_RUN(snapshot_return_keeps_active_refs_after_original_unregister);
	UT_RUN(snapshot_return_keeps_catalog_until_original_invalidation);
	UT_RUN(snapshot_return_keeps_transaction_and_exported_refs_until_eoxact);
	UT_RUN(snapshot_return_keeps_historic_and_evaluator_owners);
	UT_RUN(snapshot_return_rejects_wrong_original_backend_identity);
	UT_RUN(snapshot_return_rechecks_identity_without_keeping_stale_samples);
	UT_RUN(snapshot_return_never_closes_an_empty_or_unknown_producer);
	UT_RUN(backend_collection_retains_original_snapshots_after_gate_drains);
	UT_RUN(backend_collection_keeps_active_catalog_and_evaluator_owners);
	UT_RUN(backend_collection_preserves_all_original_shared_counter_cells);
	UT_RUN(backend_collection_binds_original_gate_and_current_native_epochs);
	UT_RUN(backend_collection_refuses_wrong_owner_scope_or_locked_context);
	UT_RUN(backend_collection_discards_owner_and_gate_drift_at_return);
	UT_RUN(backend_collection_discards_unknown_or_contended_shared_samples);
	UT_RUN(backend_collection_never_retires_original_debt_or_grants_closure);
	UT_RUN(survivor_backend_poll_keeps_actual_snapshot_and_original_gate);
	UT_RUN(survivor_backend_poll_discards_final_owner_drift_and_leaf_stale);
	UT_RUN(backend_collection_retains_original_buffer_pin_until_native_unpin);
	UT_RUN(backend_collection_keeps_nested_native_pin_until_last_return);
	UT_RUN(backend_collection_retains_original_overflow_pins_after_hot_array_drains);
	UT_RUN(backend_collection_discards_pin_arrival_and_return_during_shared_observation);
	UT_RUN(survivor_backend_poll_preserves_native_buffer_owner);
	UT_RUN(backend_collection_preserves_snapshot_and_pin_responsibilities_together);
	UT_RUN(backend_collection_never_converts_unknown_local_pin_array_to_clear);
	UT_RUN(backend_collection_rejects_locked_or_changed_owner_without_returning_pin_sample);
	UT_RUN(backend_return_rejects_resource_owner_change_without_retiring_native_resources);
	UT_RUN(backend_return_rejects_exit_during_original_shared_cut_without_cleanup);
	UT_RUN(backend_return_refuses_already_exiting_before_observing_original_owners);
	UT_RUN(backend_return_keeps_stable_null_and_nonnull_owner_points_pending);
	UT_RUN(survivor_backend_rejects_resource_owner_change_at_final_return);
	UT_RUN(survivor_backend_rejects_exit_after_leaf_sample_without_retiring_resources);
	UT_RUN(survivor_backend_refuses_already_exiting_before_leaf_observation);
	UT_RUN(survivor_backend_stable_original_owner_keeps_pin_and_snapshot_debt);
	UT_RUN(backend_bufferio_observation_follows_original_remember_and_forget);
	UT_RUN(backend_bufferio_current_owner_does_not_claim_parent_or_null_closure);
	UT_RUN(backend_bufferio_change_during_shared_cut_discards_the_point);
	UT_RUN(survivor_backend_final_refusal_retains_original_bufferio);
	UT_RUN(backend_bufferio_never_dereferences_a_retired_entry_owner);
	UT_RUN(backend_return_retains_each_original_transaction_phase);
	UT_RUN(backend_return_retains_each_original_transaction_block_phase);
	UT_RUN(backend_return_retains_original_subtransaction_state);
	UT_RUN(backend_return_discards_transaction_change_during_shared_cut);
	UT_RUN(backend_return_transaction_observation_preserves_resources_and_empty_pending);
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(!ActiveSnapshotSet());
	free(procArray);
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(backend_resource_one);
	ResourceOwnerDelete(backend_resource_two);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
