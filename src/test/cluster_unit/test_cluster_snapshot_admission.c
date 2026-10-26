/* PGRAC: actual snapshot manager lifecycle and foreign-read evidence.
 * Allocation and ResourceOwner bookkeeping are explicit fixture boundaries;
 * Active/Registered containers, reference counts and retention are native.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "unit_test.h"
#include "access/visibilitymap.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "utils/rel.h"
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
#include "cluster/cluster_space_storage.h"
#include "cluster/cluster_undo_horizon.h"
#include "cluster/cluster_undo_retention.h"
#include "access/twophase.h"
#include "test_cluster_pgproc_active_types.inc"
#include "../../backend/utils/time/snapmgr.c"
#include "test_cluster_snapshot_admission_horizon.inc"

#include "test_cluster_space_identity_observation_available.inc"
#ifndef FIXTURE_HAS_SPACE_IDENTITY_OBSERVATION
/* Parent has an unguarded identity cache, without an operation observation. */
typedef struct ClusterSpaceIdentityObservationV1 {
	ClusterSpaceIdentity identity;
	uint64 invalidations;
	Oid relid;
} ClusterSpaceIdentityObservationV1;
extern bool cluster_space_relation_identity_capture_v1(Relation rel,
														 ClusterSpaceIdentityObservationV1 *out);
extern bool cluster_space_relation_identity_current_v1(
	Relation rel, const ClusterSpaceIdentityObservationV1 *expected);
#endif

UT_DEFINE_GLOBALS();

bool cluster_enabled = true;
int cluster_node_id = 1;
PGPROC *MyProc;
ResourceOwner CurrentResourceOwner = (ResourceOwner)1;
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
static bool vm_fixture, vm_locked, vm_shared, vm_refuse;
static bool vm_caught, vm_result;
static int vm_drift, vm_error;
static unsigned vm_begins, vm_ends, vm_admissions;
static Snapshot vm_expected;
static uint64 vm_member_cut = 2, vm_cap_cut = 10;
static int vm_admission_change;
static bool vm_space_available, vm_space_error;
static uint64 vm_space_generation;
static unsigned vm_space_captures, vm_space_checks;
static Relation vm_space_relation;
int NBuffers = 2, NLocBuffer;
/* Joint native snapshot/VM chain: the transport and storage remain scheduled. */
static bool vm_chain_enabled, vm_chain_pcm, vm_chain_grant, vm_chain_released;
static int vm_chain_drift, vm_chain_error;
static unsigned vm_chain_reads, vm_chain_grants, vm_chain_unlocks, vm_chain_releases;
static PGAlignedBlock vm_chain_disk, vm_chain_current, vm_chain_image;
static SMgrRelationData vm_chain_smgr;
static RelFileLocator vm_chain_locator;
static ForkNumber vm_chain_fork;
static BlockNumber vm_chain_block;
static Buffer vm_chain_read_buffer;

static bool vm_chain_read_begin(Relation rel, BlockNumber block, Buffer *buffer,
								VisibilityMapReadScope *scope, uint8 *status);
static void vm_chain_read_end(VisibilityMapReadScope *scope);
static uint8 vm_chain_raw_status(Relation rel, BlockNumber block, Buffer *buffer);
static bool vm_chain_pinned_status(Relation rel, BlockNumber block, Buffer buffer, uint8 *status);
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
uint64
cluster_epoch_get_current(void)
{
	return 7;
}
uint64
cluster_membership_cut_generation(void)
{
	return vm_member_cut;
}
bool
cluster_membership_cut_generation_current(uint64 expected)
{
	return expected != 0 && expected == vm_member_cut;
}
uint64
cluster_sf_peer_capability_cut_generation(void)
{
	return vm_cap_cut;
}
bool
cluster_sf_peer_capability_cut_generation_current(uint64 expected)
{
	return expected != 0 && expected == vm_cap_cut;
}
const ClusterNodeInfo *
cluster_conf_lookup_node(int node)
{
	static ClusterNodeInfo peer;
	return node == 0 ? &peer : NULL;
}
bool
cluster_space_relation_identity_capture_v1(Relation rel, ClusterSpaceIdentityObservationV1 *out)
{
	UT_ASSERT(!vm_locked);
	vm_space_captures++;
	memset(out, 0, sizeof(*out));
	if (vm_space_error)
		siglongjmp(*PG_exception_stack, 1);
	if (!vm_space_available)
		return false;
	out->invalidations = vm_space_generation;
	if (vm_chain_enabled) {
		out->relid = RelationGetRelid(rel);
		out->identity.key.locator = rel->rd_locator;
	}
	return true;
}
bool
cluster_space_relation_identity_current_v1(Relation rel,
										 const ClusterSpaceIdentityObservationV1 *expected)
{
	vm_space_checks++;
	return expected->invalidations == vm_space_generation && vm_space_available
		&& (!vm_chain_enabled || (rel->rd_isvalid && RelationGetRelid(rel) == expected->relid
			&& RelFileLocatorEquals(rel->rd_locator, expected->identity.key.locator)));
}
bool
cluster_sf_peer_supports_undo_horizon(int node)
{
	if (vm_fixture) {
		UT_ASSERT(!vm_locked);
		vm_admissions++;
		if (vm_admission_change == 1)
			vm_cap_cut++;
		else if (vm_admission_change == 2)
			vm_member_cut += 2;
	}
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


/* The native snapshot manager and read-admission body above execute. Only
 * the coherent VM acquisition/storage leaf is scheduled by this fixture. */
bool
cluster_heap_maintenance_restricted(Relation rel)
{
	return vm_shared;
}

bool
visibilitymap_read_begin(Relation rel, BlockNumber block, Buffer *buffer,
						 VisibilityMapReadScope *scope, uint8 *status)
{
	if (vm_chain_enabled)
		return vm_chain_read_begin(rel, block, buffer, scope, status);
	if (status != NULL)
		*status = 0;
	if (rel == NULL || block == InvalidBlockNumber || buffer == NULL || scope == NULL
		|| status == NULL || BufferIsValid(scope->locked_buffer))
		return false;
	vm_begins++;
	if (!vm_shared) {
		*buffer = 1;
		*status = VISIBILITYMAP_VALID_BITS;
		return true;
	}
	UT_ASSERT(ClusterSnapshotReadScope != NULL
			  && ClusterSnapshotReadScope->snapshot == vm_expected);
	UT_ASSERT(!vm_locked);
	if (vm_error == 1)
		siglongjmp(*PG_exception_stack, 1);
	if (vm_refuse)
		return false;
	*buffer = scope->locked_buffer = 1;
	vm_locked = true;
	*status = VISIBILITYMAP_VALID_BITS;
	if (vm_error == 2)
		siglongjmp(*PG_exception_stack, 1);
	switch (vm_drift) {
	case 1:
		vm_expected->read_scn++;
		break;
	case 2:
		vm_expected->read_epoch++;
		break;
	case 3:
		vm_expected->cluster_source = SNAPSHOT_SOURCE_LOCAL;
		break;
	case 4:
		vm_expected->cluster_cr_identity++;
		break;
	case 5:
		CurrentResourceOwner = (ResourceOwner)2;
		break;
	case 6:
		pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 101);
		break;
	default:
		break;
	}
	return true;
}

void
visibilitymap_read_end(VisibilityMapReadScope *scope)
{
	if (vm_chain_enabled) {
		vm_chain_read_end(scope);
		return;
	}
	if (scope == NULL || !BufferIsValid(scope->locked_buffer))
		return;
	UT_ASSERT(vm_locked);
	vm_ends++;
	vm_locked = false;
	scope->locked_buffer = InvalidBuffer;
}

#include "test_cluster_vm_snapshot_read.inc"
#ifndef FIXTURE_HAS_VM_SNAPSHOT_READER
/* API-absent parent: the existing VM observation does not bind a snapshot. */
static bool
fixture_vm_snapshot_observation(Relation rel, BlockNumber block, Snapshot snapshot, Buffer *buffer,
								VisibilityMapReadScope *scope, uint8 *status)
{
	return visibilitymap_read_begin(rel, block, buffer, scope, status);
}
#define visibilitymap_read_snapshot_begin fixture_vm_snapshot_observation
#endif

static void
vm_setup(Snapshot snapshot)
{
	vm_fixture = vm_shared = true;
	vm_locked = vm_refuse = vm_caught = vm_result = false;
	vm_drift = vm_error = 0;
	vm_begins = vm_ends = vm_admissions = 0;
	vm_expected = snapshot;
	peer_capable = true;
	self_member = CLUSTER_MEMBER_MEMBER;
	pg_atomic_write_u64(&horizon.self_admitted_epoch, 8);
	CurrentResourceOwner = (ResourceOwner)1;
	cluster_node_id = 1;
	vm_member_cut = 2;
	vm_cap_cut = 10;
	vm_admission_change = 0;
	vm_space_available = true;
	vm_space_error = false;
	vm_space_generation = 100;
	vm_space_captures = vm_space_checks = 0;
	vm_space_relation = NULL;
}

static void
vm_observe(Snapshot snapshot, Buffer *buffer, VisibilityMapReadScope *scope, uint8 *status)
{
	PG_TRY();
	{
		vm_result
			= visibilitymap_read_snapshot_begin((Relation)1, 0, snapshot, buffer, scope, status);
	}
	PG_CATCH();
	{
		vm_caught = true;
	}
	PG_END_TRY();
}

UT_TEST(vm_observation_binds_registered_snapshot_and_restores_outer_evaluator)
{
	Snapshot a = registered(90), s = registered(100);
	ClusterSnapshotReadScopeV1 outer;
	VisibilityMapReadScope scope = { 0 };
	Buffer buffer = InvalidBuffer;
	uint8 status = 77;

	vm_setup(s);
	PushActiveSnapshot(a);
	cluster_snapshot_read_enter_v1(&outer, a);
	for (int reuse = 0; reuse < 2; reuse++) {
		vm_observe(s, &buffer, &scope, &status);
		UT_ASSERT(!vm_caught && vm_result && vm_locked);
		UT_ASSERT_EQ(status, VISIBILITYMAP_VALID_BITS);
		UT_ASSERT_EQ(scope.locked_buffer, 1);
		UT_ASSERT_EQ(buffer, 1);
		UT_ASSERT(ClusterSnapshotReadScope == &outer);
		UT_ASSERT(evidence(90, a));
		UT_ASSERT(GetActiveSnapshot() == a);
		UT_ASSERT_EQ(s->regd_count, 1);
		UT_ASSERT_EQ(s->active_count, 0);
		visibilitymap_read_end(&scope);
	}
	UT_ASSERT_EQ(vm_begins, 2);
	UT_ASSERT_EQ(vm_admissions, 2);
	UT_ASSERT_EQ(vm_ends, 2);
	cluster_snapshot_read_exit_v1(&outer);
	PopActiveSnapshot();
	vm_fixture = false;
	UnregisterSnapshot(s);
	UnregisterSnapshot(a);
}

UT_TEST(vm_observation_refuses_unretained_or_unqualified_snapshot_before_vm)
{
	for (int variant = 0; variant < 8; variant++) {
		SnapshotData forged = { 0 };
		Snapshot s = variant == 0 ? &forged : registered(100);
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_setup(s);
		if (variant == 0) {
			forged.snapshot_type = SNAPSHOT_MVCC;
			forged.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
			forged.read_scn = 100;
			forged.read_epoch = 7;
			forged.active_count = forged.regd_count = 100;
		} else if (variant == 1)
			s->snapshot_type = SNAPSHOT_SELF;
		else if (variant == 2)
			s->cluster_source = SNAPSHOT_SOURCE_LOCAL;
		else if (variant == 3)
			s->read_scn = InvalidScn;
		else if (variant == 4)
			s->read_epoch = 0;
		else if (variant == 5)
			s->read_epoch++;
		else if (variant == 6)
			UnregisterSnapshot(s);
		vm_observe(variant == 7 ? NULL : s, &buffer, &scope, &status);
		UT_ASSERT(!vm_caught && !vm_result && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_begins + vm_admissions + vm_ends, 0);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		visibilitymap_read_end(&scope);
		vm_fixture = false;
		if (variant != 0 && variant != 6)
			UnregisterSnapshot(s);
	}
}

UT_TEST(vm_observation_keeps_real_membership_and_peer_capability_admission)
{
	for (int variant = 0; variant < 4; variant++) {
		Snapshot s = registered(100);
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_setup(s);
		if (variant == 0)
			self_member = CLUSTER_MEMBER_JOINING;
		else if (variant == 1)
			pg_atomic_write_u64(&horizon.self_admitted_epoch, 0);
		else if (variant == 2)
			pg_atomic_write_u64(&horizon.self_admitted_epoch, 9);
		else
			peer_capable = false;
		vm_observe(s, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_locked);
		UT_ASSERT_EQ(vm_caught, variant != 3);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_begins + vm_ends, 0);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		visibilitymap_read_end(&scope);
		vm_fixture = false;
		self_member = CLUSTER_MEMBER_MEMBER;
		peer_capable = true;
		pg_atomic_write_u64(&horizon.self_admitted_epoch, 8);
		UnregisterSnapshot(s);
	}
}

UT_TEST(vm_observation_discards_snapshot_drift_after_vm_acquisition)
{
	for (int drift = 1; drift <= 6; drift++) {
		Snapshot s = registered(100);
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_setup(s);
		vm_drift = drift;
		vm_observe(s, &buffer, &scope, &status);
		UT_ASSERT(!vm_caught && !vm_result && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT_EQ(buffer, 1);
		UT_ASSERT_EQ(vm_begins, 1);
		UT_ASSERT_EQ(vm_admissions, 1);
		UT_ASSERT_EQ(vm_ends, 1);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		visibilitymap_read_end(&scope);
		vm_fixture = false;
		CurrentResourceOwner = (ResourceOwner)1;
		s->read_scn = 100;
		s->read_epoch = 7;
		s->cluster_source = SNAPSHOT_SOURCE_CLUSTER;
		UnregisterSnapshot(s);
	}
}

UT_TEST(vm_observation_unwinds_refusal_and_errors_to_the_original_outer_scope)
{
	for (int fault = 0; fault < 3; fault++) {
		Snapshot a = registered(90), s = registered(100);
		ClusterSnapshotReadScopeV1 outer;
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_setup(s);
		vm_refuse = fault == 0;
		vm_error = fault;
		cluster_snapshot_read_enter_v1(&outer, a);
		vm_observe(s, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_locked);
		UT_ASSERT_EQ(vm_caught, fault != 0);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT_EQ(vm_begins, 1);
		UT_ASSERT_EQ(vm_ends, fault == 2);
		UT_ASSERT(ClusterSnapshotReadScope == &outer);
		UT_ASSERT(evidence(90, a));
		visibilitymap_read_end(&scope);
		cluster_snapshot_read_exit_v1(&outer);
		vm_fixture = false;
		UnregisterSnapshot(s);
		UnregisterSnapshot(a);
	}
}

UT_TEST(vm_observation_preserves_active_vm_scope_and_native_observation)
{
	Snapshot s = registered(100);
	VisibilityMapReadScope scope = { 0 };
	Buffer buffer = 1;
	uint8 status = 77;

	vm_setup(s);
	scope.locked_buffer = 1;
	vm_locked = true;
	vm_observe(s, &buffer, &scope, &status);
	UT_ASSERT(!vm_caught && !vm_result && vm_locked);
	UT_ASSERT_EQ(scope.locked_buffer, 1);
	UT_ASSERT_EQ(vm_begins + vm_admissions + vm_ends, 0);
	visibilitymap_read_end(&scope);
	vm_fixture = false;
	UnregisterSnapshot(s);
	vm_setup(NULL);
	vm_shared = false;
	vm_observe(NULL, &buffer, &scope, &status);
	UT_ASSERT(!vm_caught && vm_result && !vm_locked);
	UT_ASSERT_EQ(status, VISIBILITYMAP_VALID_BITS);
	UT_ASSERT_EQ(vm_begins, 1);
	UT_ASSERT_EQ(vm_admissions, 0);
	UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
	vm_fixture = false;
}




/* Only storage/transport leaves are scheduled in the joint source profile. */
static Buffer
vm_chain_read(Relation rel, ForkNumber fork, BlockNumber block, ReadBufferMode mode,
			  BufferAccessStrategy strategy)
{
	UT_ASSERT(!vm_locked);
	UT_ASSERT(ClusterSnapshotReadScope != NULL
			  && ClusterSnapshotReadScope->snapshot == vm_expected);
	UT_ASSERT_EQ(vm_admissions, 1);
	UT_ASSERT_EQ(fork, VISIBILITYMAP_FORKNUM);
	UT_ASSERT_EQ(mode, RBM_NORMAL);
	UT_ASSERT(strategy == NULL);
	vm_chain_reads++;
	if (vm_chain_error == 1)
		siglongjmp(*PG_exception_stack, 1);
	vm_chain_locator = rel->rd_locator;
	vm_chain_fork = fork;
	vm_chain_block = block;
	vm_chain_image = vm_chain_disk;
	return vm_chain_read_buffer;
}

static void
vm_chain_release(Buffer buffer)
{
	UT_ASSERT(!vm_locked);
	UT_ASSERT(BufferIsValid(buffer));
	vm_chain_releases++;
}

static void
vm_chain_tag(Buffer buffer, RelFileLocator *locator, ForkNumber *fork, BlockNumber *block)
{
	UT_ASSERT(BufferIsValid(buffer));
	*locator = vm_chain_locator;
	*fork = vm_chain_fork;
	*block = vm_chain_block;
}

static bool
vm_chain_acquire(Buffer buffer)
{
	UT_ASSERT(BufferIsValid(buffer) && !vm_locked);
	UT_ASSERT(ClusterSnapshotReadScope != NULL
			  && ClusterSnapshotReadScope->snapshot == vm_expected);
	if (vm_chain_error == 2)
		siglongjmp(*PG_exception_stack, 1);
	if (!vm_chain_grant)
		return false;
	vm_chain_grants++;
	vm_locked = true;
	vm_chain_image = vm_chain_current;
	if (vm_chain_drift == 1)
		vm_expected->read_scn++;
	else if (vm_chain_drift == 2)
		CurrentResourceOwner = (ResourceOwner)2;
	else if (vm_chain_drift == 3) {
		UnregisterSnapshot(vm_expected);
		vm_chain_released = true;
	} else if (vm_chain_drift == 4) vm_cap_cut++;
	else if (vm_chain_drift == 5) vm_member_cut += 2;
	else if (vm_chain_drift == 6) pg_atomic_write_u64(&horizon.self_admitted_epoch, 9);
	else if (vm_chain_drift == 7) cluster_node_id = 2;
	else if (vm_chain_drift == 8) vm_space_generation++;
	else if (vm_chain_drift == 9) vm_space_relation->rd_isvalid = false;
	else if (vm_chain_drift == 10) vm_space_relation->rd_id++;
	else if (vm_chain_drift == 11) vm_space_relation->rd_locator.relNumber++;
	return true;
}

static void
vm_chain_unlock(Buffer buffer, int mode)
{
	UT_ASSERT(BufferIsValid(buffer) && vm_locked);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
	vm_locked = false;
	vm_chain_unlocks++;
}

#pragma push_macro("BufferGetPage")
#undef BufferGetPage
#define BufferGetPage(buf) ((void)(buf), (Page)vm_chain_image.data)
#pragma push_macro("BufferGetBlockNumber")
#undef BufferGetBlockNumber
#define BufferGetBlockNumber(buf) ((void)(buf), vm_chain_block)
#pragma push_macro("BufferGetTag")
#undef BufferGetTag
#define BufferGetTag vm_chain_tag
#pragma push_macro("ReadBufferExtended")
#undef ReadBufferExtended
#define ReadBufferExtended vm_chain_read
#pragma push_macro("ReleaseBuffer")
#undef ReleaseBuffer
#define ReleaseBuffer vm_chain_release
#pragma push_macro("LockBuffer")
#undef LockBuffer
#define LockBuffer vm_chain_unlock
#pragma push_macro("ClusterLockBufferShareBarrierAware")
#undef ClusterLockBufferShareBarrierAware
#define ClusterLockBufferShareBarrierAware vm_chain_acquire
#pragma push_macro("RelationGetSmgr")
#undef RelationGetSmgr
#define RelationGetSmgr(rel) ((void)(rel), &vm_chain_smgr)
#pragma push_macro("cluster_shared_config")
#undef cluster_shared_config
#define cluster_shared_config vm_shared
#pragma push_macro("cluster_pcm_is_active")
#undef cluster_pcm_is_active
#define cluster_pcm_is_active() vm_chain_pcm
#pragma push_macro("cluster_smgr_which_for")
#undef cluster_smgr_which_for
#define cluster_smgr_which_for(...) 1
#pragma push_macro("cluster_space_relation_get_identity")
#undef cluster_space_relation_get_identity
#define cluster_space_relation_get_identity(rel, identity) ((void)(rel), (void)(identity), false)
#pragma push_macro("cluster_space_init_vm_buffer_wal")
#undef cluster_space_init_vm_buffer_wal
#define cluster_space_init_vm_buffer_wal(identity, buf) ((void)(identity), (void)(buf), false)
#pragma push_macro("vm_extend")
#undef vm_extend
#define vm_extend(...) (abort(), InvalidBuffer)
#pragma push_macro("smgrexists")
#undef smgrexists
#define smgrexists(...) true
#pragma push_macro("smgrnblocks")
#undef smgrnblocks
#define smgrnblocks(...) (abort(), 0)
#pragma push_macro("LockBufferForVisibilityMapPageInit")
#undef LockBufferForVisibilityMapPageInit
#define LockBufferForVisibilityMapPageInit(buf) (abort(), (buf))
#pragma push_macro("PageInit")
#undef PageInit
#define PageInit(...) abort()
#pragma push_macro("CHECK_FOR_INTERRUPTS")
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#pragma push_macro("visibilitymap_read_begin")
#undef visibilitymap_read_begin
#define visibilitymap_read_begin vm_chain_read_begin
#pragma push_macro("visibilitymap_read_end")
#undef visibilitymap_read_end
#define visibilitymap_read_end vm_chain_read_end
#pragma push_macro("visibilitymap_get_status")
#undef visibilitymap_get_status
#define visibilitymap_get_status vm_chain_raw_status
#pragma push_macro("visibilitymap_get_status_pinned")
#undef visibilitymap_get_status_pinned
#define visibilitymap_get_status_pinned vm_chain_pinned_status

static Buffer vm_readbuf(Relation rel, BlockNumber block, bool extend);
#include "test_cluster_vm_native_chain.inc"
#pragma pop_macro("visibilitymap_get_status_pinned")
#pragma pop_macro("visibilitymap_get_status")
#pragma pop_macro("visibilitymap_read_end")
#pragma pop_macro("visibilitymap_read_begin")
#pragma pop_macro("CHECK_FOR_INTERRUPTS")
#pragma pop_macro("PageInit")
#pragma pop_macro("LockBufferForVisibilityMapPageInit")
#pragma pop_macro("smgrnblocks")
#pragma pop_macro("smgrexists")
#pragma pop_macro("vm_extend")
#pragma pop_macro("cluster_space_init_vm_buffer_wal")
#pragma pop_macro("cluster_space_relation_get_identity")
#pragma pop_macro("cluster_smgr_which_for")
#pragma pop_macro("cluster_pcm_is_active")
#pragma pop_macro("cluster_shared_config")
#pragma pop_macro("RelationGetSmgr")
#pragma pop_macro("ClusterLockBufferShareBarrierAware")
#pragma pop_macro("LockBuffer")
#pragma pop_macro("ReleaseBuffer")
#pragma pop_macro("ReadBufferExtended")
#pragma pop_macro("BufferGetTag")
#pragma pop_macro("BufferGetBlockNumber")
#pragma pop_macro("BufferGetPage")
#undef MAPSIZE
#undef HEAPBLOCKS_PER_BYTE
#undef HEAPBLOCKS_PER_PAGE
#undef HEAPBLK_TO_MAPBLOCK
#undef HEAPBLK_TO_MAPBYTE
#undef HEAPBLK_TO_OFFSET

static void
vm_chain_setup(RelationData *rel, FormData_pg_class *form, Snapshot snapshot)
{
	vm_setup(snapshot);
	vm_chain_enabled = vm_chain_pcm = vm_chain_grant = true;
	vm_chain_released = false;
	vm_chain_drift = vm_chain_error = 0;
	vm_chain_reads = vm_chain_grants = vm_chain_unlocks = vm_chain_releases = 0;
	vm_chain_read_buffer = 1;
	memset(rel, 0, sizeof(*rel));
	memset(form, 0, sizeof(*form));
	rel->rd_rel = form;
	rel->rd_locator = (RelFileLocator){ 1663, 123, 456 };
	rel->rd_id = 456;
	rel->rd_isvalid = true;
	vm_space_relation = rel;
	form->relpersistence = RELPERSISTENCE_PERMANENT;
	vm_chain_locator = rel->rd_locator;
	vm_chain_fork = VISIBILITYMAP_FORKNUM;
	vm_chain_block = 0;
	memset(&vm_chain_smgr, 0, sizeof(vm_chain_smgr));
	vm_chain_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 2;
	memset(&vm_chain_current, 0, sizeof(vm_chain_current));
	((PageHeader)vm_chain_current.data)->pd_lower = MAXALIGN(SizeOfPageHeaderData);
	((PageHeader)vm_chain_current.data)->pd_upper = BLCKSZ;
	((PageHeader)vm_chain_current.data)->pd_special = BLCKSZ;
	((PageHeader)vm_chain_current.data)->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	((PageHeader)vm_chain_current.data)->pd_block_scn = 17;
	vm_chain_disk = vm_chain_image = vm_chain_current;
	PageGetContents(vm_chain_disk.data)[0] = VISIBILITYMAP_VALID_BITS;
}

static void
vm_chain_observe(Relation rel, BlockNumber block, Snapshot snapshot, Buffer *buffer,
				 VisibilityMapReadScope *scope, uint8 *status)
{
	vm_result = vm_caught = false;
	PG_TRY();
	{
		vm_result = visibilitymap_read_snapshot_begin(rel, block, snapshot, buffer, scope, status);
	}
	PG_CATCH();
	{
		vm_caught = true;
	}
	PG_END_TRY();
}

static void
vm_chain_finish(Snapshot snapshot, VisibilityMapReadScope *scope)
{
	visibilitymap_read_end(scope);
	vm_chain_enabled = vm_fixture = false;
	CurrentResourceOwner = (ResourceOwner)1;
	if (!vm_chain_released) {
		snapshot->read_scn = 100;
		UnregisterSnapshot(snapshot);
	}
}

UT_TEST(vm_chain_native_snapshot_observes_current_bits_and_rechecks_reused_pin)
{
	Snapshot outer_snapshot = registered(90), snapshot = registered(100);
	ClusterSnapshotReadScopeV1 outer;
	RelationData rel;
	FormData_pg_class form;
	VisibilityMapReadScope scope = { 0 };
	Buffer buffer = InvalidBuffer;
	uint8 status = 77;

	vm_chain_setup(&rel, &form, snapshot);
	cluster_snapshot_read_enter_v1(&outer, outer_snapshot);
	for (int visit = 0; visit < 3; visit++) {
		PageGetContents(vm_chain_current.data)[0] = visit == 1 ? VISIBILITYMAP_VALID_BITS << 2 : 0;
		vm_chain_observe(&rel, visit == 1 ? 1 : 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(vm_result && !vm_caught && vm_locked);
		UT_ASSERT_EQ(status, visit == 1 ? VISIBILITYMAP_VALID_BITS : 0);
		UT_ASSERT_EQ(vm_chain_reads, 1);
		UT_ASSERT_EQ(vm_chain_grants, visit + 1);
		UT_ASSERT_EQ(vm_admissions, visit + 1);
		UT_ASSERT(ClusterSnapshotReadScope == &outer && evidence(90, outer_snapshot));
		UT_ASSERT_EQ(snapshot->regd_count, 1);
		UT_ASSERT_EQ(snapshot->active_count, 0);
		visibilitymap_read_end(&scope);
		UT_ASSERT(!vm_locked && buffer == 1 && scope.locked_buffer == InvalidBuffer);
	}
	UT_ASSERT_EQ(vm_chain_unlocks, 3);
	UT_ASSERT_EQ(PageGetContents(vm_chain_disk.data)[0], VISIBILITYMAP_VALID_BITS);
	cluster_snapshot_read_exit_v1(&outer);
	vm_chain_finish(snapshot, &scope);
	UnregisterSnapshot(outer_snapshot);
}

UT_TEST(vm_chain_rebinds_exact_relation_fork_and_map_before_native_observation)
{
	for (int mismatch = 0; mismatch < 3; mismatch++) {
		Snapshot snapshot = registered(100);
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = 1;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		vm_chain_read_buffer = 2;
		if (mismatch == 0)
			vm_chain_locator.relNumber++;
		else if (mismatch == 1)
			vm_chain_fork = MAIN_FORKNUM;
		else
			vm_chain_block = 1;
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(vm_result && !vm_caught && vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(buffer, 2);
		UT_ASSERT_EQ(scope.locked_buffer, 2);
		UT_ASSERT_EQ(vm_chain_reads, 1);
		UT_ASSERT_EQ(vm_chain_releases, 1);
		UT_ASSERT_EQ(vm_chain_grants, 1);
		UT_ASSERT(RelFileLocatorEquals(vm_chain_locator, rel.rd_locator));
		UT_ASSERT_EQ(vm_chain_fork, VISIBILITYMAP_FORKNUM);
		UT_ASSERT_EQ(vm_chain_block, 0);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		vm_chain_finish(snapshot, &scope);
	}
}

UT_TEST(vm_chain_validates_actual_vm_image_and_keeps_absence_read_only)
{
	for (int variant = 0; variant < 5; variant++) {
		Snapshot snapshot = registered(100);
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		if (variant == 0)
			((PageHeader)vm_chain_current.data)->pd_block_scn = 0;
		else if (variant == 1)
			((PageHeader)vm_chain_current.data)->pd_special--;
		else if (variant == 2)
			PageGetContents(vm_chain_current.data)[0] = VISIBILITYMAP_ALL_FROZEN;
		else if (variant == 3)
			memset(&vm_chain_current, 0, sizeof(vm_chain_current));
		else
			vm_chain_smgr.smgr_cached_nblocks[VISIBILITYMAP_FORKNUM] = 0;
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT_EQ(vm_result, variant >= 3);
		UT_ASSERT(!vm_caught);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_chain_reads, variant != 4);
		UT_ASSERT_EQ(vm_chain_grants, variant != 4);
		UT_ASSERT_EQ(vm_locked, variant == 3);
		UT_ASSERT_EQ(vm_chain_unlocks, variant < 3);
		UT_ASSERT_EQ(buffer, variant == 4 ? InvalidBuffer : 1);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		if (variant == 3)
			UT_ASSERT_EQ(memcmp(vm_chain_image.data, vm_chain_current.data, BLCKSZ), 0);
		vm_chain_finish(snapshot, &scope);
	}
}

UT_TEST(vm_chain_respects_native_admission_and_coherent_grant_refusals)
{
	for (int refusal = 0; refusal < 4; refusal++) {
		Snapshot snapshot = registered(100);
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		if (refusal == 0)
			self_member = CLUSTER_MEMBER_JOINING;
		else if (refusal == 1)
			peer_capable = false;
		else if (refusal == 2)
			vm_chain_pcm = false;
		else
			vm_chain_grant = false;
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result);
		UT_ASSERT_EQ(vm_caught, refusal == 0);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_chain_reads, refusal == 3);
		UT_ASSERT_EQ(vm_chain_grants, 0);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		/* Also clean any unexpected old controlled leaf ownership in RED. */
		visibilitymap_read_end(&scope);
		UT_ASSERT(!vm_locked);
		vm_chain_finish(snapshot, &scope);
		self_member = CLUSTER_MEMBER_MEMBER;
		peer_capable = true;
	}
}

UT_TEST(vm_chain_discards_native_snapshot_drift_and_restores_outer_on_io_errors)
{
	for (int fault = 0; fault < 5; fault++) {
		Snapshot outer_snapshot = registered(90), snapshot = registered(100);
		ClusterSnapshotReadScopeV1 outer;
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = { 0 };
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		vm_chain_drift = fault < 3 ? fault + 1 : 0;
		vm_chain_error = fault < 3 ? 0 : fault - 2;
		cluster_snapshot_read_enter_v1(&outer, outer_snapshot);
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_locked);
		UT_ASSERT_EQ(vm_caught, fault >= 3);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_chain_reads, 1);
		UT_ASSERT_EQ(vm_chain_grants, fault < 3);
		UT_ASSERT_EQ(vm_chain_unlocks, fault < 3);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT_EQ(buffer, fault == 3 ? InvalidBuffer : 1);
		UT_ASSERT(ClusterSnapshotReadScope == &outer);
		CurrentResourceOwner = (ResourceOwner)1;
		UT_ASSERT(evidence(90, outer_snapshot));
		visibilitymap_read_end(&scope);
		cluster_snapshot_read_exit_v1(&outer);
		vm_chain_finish(snapshot, &scope);
		UnregisterSnapshot(outer_snapshot);
	}
}


UT_TEST(vm_admission_cut_refuses_missing_or_changed_pre_acquire_identity)
{
	for (int variant = 0; variant < 4; variant++) {
		Snapshot snapshot = registered(100);
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = {0};
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;
		vm_chain_setup(&rel, &form, snapshot);
		if (variant == 0) vm_member_cut = 0;
		else if (variant == 1) vm_cap_cut = 0;
		else vm_admission_change = variant - 1;
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_caught && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_chain_reads + vm_chain_grants + vm_chain_unlocks, 0);
		UT_ASSERT_EQ(vm_admissions, 1);
		UT_ASSERT_EQ(buffer, InvalidBuffer);
		UT_ASSERT(ClusterSnapshotReadScope == NULL);
		vm_chain_finish(snapshot, &scope);
	}
}

UT_TEST(vm_admission_cut_rejects_capability_membership_self_admission_and_node_drift)
{
	for (int drift = 4; drift <= 7; drift++) {
		Snapshot original = registered(90), snapshot = registered(100);
		ClusterSnapshotReadScopeV1 outer;
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = {0};
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;
		vm_chain_setup(&rel, &form, snapshot);
		PageGetContents(vm_chain_current.data)[0] = VISIBILITYMAP_VALID_BITS;
		vm_chain_drift = drift;
		cluster_snapshot_read_enter_v1(&outer, original);
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_caught && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_chain_reads, 1);
		UT_ASSERT_EQ(vm_chain_grants, 1);
		UT_ASSERT_EQ(vm_chain_unlocks, 1);
		UT_ASSERT_EQ(vm_admissions, 1);
		UT_ASSERT_EQ(buffer, 1);
		UT_ASSERT(ClusterSnapshotReadScope == &outer && evidence(90, original));
		visibilitymap_read_end(&scope);
		cluster_snapshot_read_exit_v1(&outer);
		cluster_node_id = 1;
		vm_chain_finish(snapshot, &scope);
		UnregisterSnapshot(original);
	}
}


UT_TEST(vm_space_identity_refuses_missing_or_failed_capture_before_vm_acquisition)
{
	for (int fault = 0; fault < 2; fault++) {
		Snapshot original = registered(90), snapshot = registered(100);
		ClusterSnapshotReadScopeV1 outer;
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = {0};
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		vm_space_available = fault != 0;
		vm_space_error = fault == 1;
		cluster_snapshot_read_enter_v1(&outer, original);
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && vm_caught == (fault == 1) && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_space_captures, 1);
		UT_ASSERT_EQ(vm_space_checks, 0);
		UT_ASSERT_EQ(vm_chain_reads + vm_chain_grants + vm_chain_unlocks, 0);
		UT_ASSERT_EQ(buffer, InvalidBuffer);
		UT_ASSERT(ClusterSnapshotReadScope == &outer && evidence(90, original));
		cluster_snapshot_read_exit_v1(&outer);
		vm_chain_finish(snapshot, &scope);
		UnregisterSnapshot(original);
	}
}

UT_TEST(vm_space_identity_rejects_invalidation_and_relation_drift_during_grant)
{
	for (int drift = 8; drift <= 11; drift++) {
		Snapshot original = registered(90), snapshot = registered(100);
		ClusterSnapshotReadScopeV1 outer;
		RelationData rel;
		FormData_pg_class form;
		VisibilityMapReadScope scope = {0};
		Buffer buffer = InvalidBuffer;
		uint8 status = 77;

		vm_chain_setup(&rel, &form, snapshot);
		vm_chain_drift = drift;
		PageGetContents(vm_chain_current.data)[0] = VISIBILITYMAP_VALID_BITS;
		cluster_snapshot_read_enter_v1(&outer, original);
		vm_chain_observe(&rel, 0, snapshot, &buffer, &scope, &status);
		UT_ASSERT(!vm_result && !vm_caught && !vm_locked);
		UT_ASSERT_EQ(status, 0);
		UT_ASSERT_EQ(vm_space_captures, 1);
		UT_ASSERT_EQ(vm_space_checks, drift == 11 ? 0 : 1);
		UT_ASSERT_EQ(vm_chain_reads, 1);
		UT_ASSERT_EQ(vm_chain_grants, 1);
		UT_ASSERT_EQ(vm_chain_unlocks, 1);
		UT_ASSERT_EQ(scope.locked_buffer, InvalidBuffer);
		UT_ASSERT_EQ(buffer, 1);
		UT_ASSERT(ClusterSnapshotReadScope == &outer && evidence(90, original));
		cluster_snapshot_read_exit_v1(&outer);
		vm_chain_finish(snapshot, &scope);
		UnregisterSnapshot(original);
	}
}

int
main(void)
{
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
	UT_PLAN(103);
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
	UT_RUN(vm_observation_binds_registered_snapshot_and_restores_outer_evaluator);
	UT_RUN(vm_observation_refuses_unretained_or_unqualified_snapshot_before_vm);
	UT_RUN(vm_observation_keeps_real_membership_and_peer_capability_admission);
	UT_RUN(vm_observation_discards_snapshot_drift_after_vm_acquisition);
	UT_RUN(vm_observation_unwinds_refusal_and_errors_to_the_original_outer_scope);
	UT_RUN(vm_observation_preserves_active_vm_scope_and_native_observation);
	UT_RUN(vm_chain_native_snapshot_observes_current_bits_and_rechecks_reused_pin);
	UT_RUN(vm_chain_rebinds_exact_relation_fork_and_map_before_native_observation);
	UT_RUN(vm_chain_validates_actual_vm_image_and_keeps_absence_read_only);
	UT_RUN(vm_chain_respects_native_admission_and_coherent_grant_refusals);
	UT_RUN(vm_chain_discards_native_snapshot_drift_and_restores_outer_on_io_errors);
	UT_RUN(vm_admission_cut_refuses_missing_or_changed_pre_acquire_identity);
	UT_RUN(vm_admission_cut_rejects_capability_membership_self_admission_and_node_drift);
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(!ActiveSnapshotSet());
	free(procArray);
	UT_RUN(vm_space_identity_refuses_missing_or_failed_capture_before_vm_acquisition);
	UT_RUN(vm_space_identity_rejects_invalidation_and_relation_drift_during_grant);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
