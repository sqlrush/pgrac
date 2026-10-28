/* Original xact caller, with controlled admission and external-effect leaves.
 * The allocation leaf selects the dormant retained entry only in this test.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "access/xloginsert.h"
#include "cluster/cluster_reconfig.h"
#include "utils/resowner.h"
#include "test_cluster_xid_retention_xact_types.inc"

static FullTransactionId XactTopFullTransactionId = {InvalidTransactionId};
static TransactionStateData TopTransactionStateData;
static TransactionState CurrentTransactionState = &TopTransactionStateData;
static TransactionId unreportedXids[PGPROC_MAX_CACHED_SUBXIDS];
static int nUnreportedXids;
static ResourceOwner xact_source_resource_owner;
static bool xact_parallel, xact_worker, xact_leaving, xact_removed;
static ClusterJoinGateVerdict xact_join;
static unsigned xact_predicates, xact_locks, xact_links, xact_wal_records;
static TransactionId xact_child[4], xact_parent[4];
static ResourceOwner xact_lock_owner[4];
static int xact_wal_level;
static int xact_error_level;
static FullTransactionId (*xact_native_allocator)(bool child);

static void AssignTransactionId(TransactionState s);

static bool xact_parallel_mode(void) { return xact_parallel; }
static bool xact_clean_leave(void) { return xact_leaving; }
static bool xact_node_removed(void) { return xact_removed; }
static ClusterJoinGateVerdict xact_join_gate(void) { return xact_join; }
static void *xact_allocate_memory(Size bytes) { return malloc(bytes); }
static void xact_free_memory(void *pointer) { free(pointer); }
static bool
xact_start_error(int level, const char *domain)
{
	xact_error_level = level;
	return errstart(level, domain);
}

static FullTransactionId
xact_allocate_native(bool child)
{
	FullTransactionId xid;

	if (xact_native_allocator != NULL)
		xid = xact_native_allocator(child);
	else
		xid = cluster_relmap_retention_allocate_xid_v2(child,
			retention_sysid, retention_dbinc, retention_origin);

	caller_assignments++;
	return xid;
}

static void
xact_set_parent(TransactionId child, TransactionId parent)
{
	if (held || retention_held || xact_links >= lengthof(xact_child)) abort();
	xact_child[xact_links] = child;
	xact_parent[xact_links++] = parent;
}

static void
xact_register_predicate(TransactionId xid)
{
	if (held || retention_held || xid != XidFromFullTransactionId(XactTopFullTransactionId)) abort();
	xact_predicates++;
}

static void
xact_insert_lock(TransactionId xid)
{
	if (held || retention_held || !TransactionIdIsNormal(xid)
		|| xact_locks >= lengthof(xact_lock_owner)) abort();
	xact_lock_owner[xact_locks++] = xact_source_resource_owner;
}

static void xact_wal_begin(void) {}
static void xact_wal_data(char *data, uint32 bytes) { (void)data; (void)bytes; }
static XLogRecPtr
xact_wal_insert(RmgrId rmid, uint8 info)
{
	if (held || retention_held || rmid != RM_XACT_ID || info != XLOG_XACT_ASSIGNMENT) abort();
	xact_wal_records++;
	return 16384; /* Controlled assignment-record leaf, not native WAL durability. */
}

#define GetTopTransactionId xact_source_top_raw
#define GetTopFullTransactionId xact_source_top
#define GetTopFullTransactionIdIfAny xact_source_top_if_any
#define IsTransactionState xact_source_in_progress
#define IsInParallelMode xact_parallel_mode
#define IsParallelWorker() xact_worker
#define GetNewTransactionId xact_allocate_native
#define cluster_clean_leave_node_refuses_writes xact_clean_leave
#define cluster_node_remove_self_is_removed xact_node_removed
#define cluster_reconfig_self_join_gate_verdict xact_join_gate
#define palloc xact_allocate_memory
#define pfree xact_free_memory
#define SubTransSetParent xact_set_parent
#define RegisterPredicateLockingXid xact_register_predicate
#define XactLockTableInsert xact_insert_lock
#define CurrentResourceOwner xact_source_resource_owner
#define XLogBeginInsert xact_wal_begin
#define XLogRegisterData xact_wal_data
#define XLogInsert xact_wal_insert
#define wal_level xact_wal_level
#define errmsg_internal errmsg
#define errstart xact_start_error
#define errstart_cold xact_start_error
#ifndef CLUSTER_XID_RETENTION_XACT_FUNCTIONS
#define CLUSTER_XID_RETENTION_XACT_FUNCTIONS "test_cluster_xid_retention_xact_functions.inc"
#endif
#include CLUSTER_XID_RETENTION_XACT_FUNCTIONS
#undef GetTopTransactionId
#undef GetTopFullTransactionId
#undef GetTopFullTransactionIdIfAny
#undef IsTransactionState
#undef IsInParallelMode
#undef IsParallelWorker
#undef GetNewTransactionId
#undef cluster_clean_leave_node_refuses_writes
#undef cluster_node_remove_self_is_removed
#undef cluster_reconfig_self_join_gate_verdict
#undef palloc
#undef pfree
#undef SubTransSetParent
#undef RegisterPredicateLockingXid
#undef XactLockTableInsert
#undef CurrentResourceOwner
#undef XLogBeginInsert
#undef XLogRegisterData
#undef XLogInsert
#undef wal_level
#undef errmsg_internal
#undef errstart
#undef errstart_cold

static void
xact_caller_fixture(uint64 first, unsigned origin)
{
	caller_fixture(first, origin);
	memset(&TopTransactionStateData, 0, sizeof(TopTransactionStateData));
	TopTransactionStateData.state = TRANS_INPROGRESS;
	TopTransactionStateData.nestingLevel = 1;
	TopTransactionStateData.curTransactionOwner = (ResourceOwner)(uintptr_t)11;
	CurrentTransactionState = &TopTransactionStateData;
	XactTopFullTransactionId = InvalidFullTransactionId;
	caller_top_reader = xact_source_top;
	caller_top_if_any_reader = xact_source_top_if_any;
	caller_state_reader = xact_source_in_progress;
	xact_parallel = xact_worker = xact_leaving = xact_removed = false;
	xact_join = CLUSTER_JOIN_GATE_ALLOW;
	xact_predicates = xact_locks = xact_links = xact_wal_records = 0;
	nUnreportedXids = 0;
	xact_wal_level = WAL_LEVEL_MINIMAL;
	xact_error_level = 0;
	xact_native_allocator = NULL;
	xact_source_resource_owner = (ResourceOwner)(uintptr_t)99;
	memset(unreportedXids, 0, sizeof(unreportedXids));
	memset(xact_lock_owner, 0, sizeof(xact_lock_owner));
}

static void
xact_capture_error(unsigned expected)
{
	static ClusterRelmapOwnerSampleV2 output;
	ClusterRelmapOwnerSampleV2 zero = { 0 };
	volatile bool caught = false;

	retention_error_expected = true;
	memset(&output, 0, sizeof(output));
	PG_TRY();
	{
		(void)cluster_relmap_owner_capture_v2(&output);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	retention_error_expected = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(retention_error_code, expected);
	UT_ASSERT_EQ(memcmp(&output, &zero, sizeof(zero)), 0);
	UT_ASSERT(!retention_held);
	/* Native resource-owner cleanup is a controlled outer ERROR leaf here. */
	held = false;
	InterruptHoldoffCount = 0;
}

UT_TEST(original_xact_stack_keeps_top_pin_and_parent_order)
{
	for (unsigned era = 0; era < 2; era++) {
		uint64 first = ((era ? UINT64CONST(7) : 0) << 32) + 2052;
		ClusterRelmapOwnerSampleV2 sample, repeated;
		ClusterRelmapRetentionPin pins[2];
		TransactionStateData middle = { 0 }, child = { 0 };
		bool captured;

		xact_caller_fixture(first, era ? CLUSTER_MAX_NODES - 1 : 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_top_if_any()), 0);
		captured = cluster_relmap_owner_capture_v2(&sample);
		UT_ASSERT(captured);
		if (!captured) return;
		UT_ASSERT_EQ(sample.owner.top_full_xid, first);
		UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), first);
		UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT_EQ(caller_begin(&sample, false), CLUSTER_RELMAP_RETENTION_OK);
		memcpy(pins, relmap_runtime_shmem->directories[retention_origin].pins, sizeof(pins));
		middle.state = child.state = TRANS_INPROGRESS;
		middle.nestingLevel = 2; child.nestingLevel = 3;
		middle.parent = &TopTransactionStateData; child.parent = &middle;
		middle.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
		child.curTransactionOwner = (ResourceOwner)(uintptr_t)13;
		CurrentTransactionState = &child;
		xact_wal_level = WAL_LEVEL_LOGICAL;
		AssignTransactionId(&child);
		UT_ASSERT_EQ(U64FromFullTransactionId(middle.fullTransactionId), first + 1);
		UT_ASSERT_EQ(U64FromFullTransactionId(child.fullTransactionId), first + 2);
		UT_ASSERT_EQ(xact_links, 2);
		UT_ASSERT_EQ(xact_child[0], (uint32)(first + 1));
		UT_ASSERT_EQ(xact_parent[0], (uint32)first);
		UT_ASSERT_EQ(xact_child[1], (uint32)(first + 2));
		UT_ASSERT_EQ(xact_parent[1], (uint32)(first + 1));
		UT_ASSERT_EQ(xact_predicates, 1);
		UT_ASSERT_EQ(xact_locks, 3);
		for (unsigned i = 0; i < 3; i++)
			UT_ASSERT(xact_lock_owner[i] == (ResourceOwner)(uintptr_t)(11 + i));
		UT_ASSERT(xact_source_resource_owner == (ResourceOwner)(uintptr_t)99);
		UT_ASSERT_EQ(xact_wal_records, 1);
		UT_ASSERT(TopTransactionStateData.didLogXid);
		UT_ASSERT(cluster_relmap_owner_capture_v2(&repeated));
		UT_ASSERT_EQ(repeated.owner.top_full_xid, first);
		UT_ASSERT_EQ(caller_assignments, 3);
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[retention_origin].last_issued, first + 2);
		UT_ASSERT_EQ(memcmp(pins, relmap_runtime_shmem->directories[retention_origin].pins, sizeof(pins)), 0);
		UT_ASSERT_EQ(caller_owner_lock_errors + lock_errors, 0);
	}
}

UT_TEST(original_xact_admission_guards_precede_native_assignment)
{
	static const unsigned codes[] = { 0, 0, ERRCODE_CLUSTER_CLEAN_LEAVE_IN_PROGRESS,
		ERRCODE_CLUSTER_NODE_REMOVED_FENCED, ERRCODE_CLUSTER_RECONFIG_IN_PROGRESS,
		ERRCODE_CLUSTER_JOIN_REJECTED_STALE };
	static const int levels[] = { ERROR, ERROR, ERROR, FATAL, ERROR, FATAL };
	for (unsigned which = 0; which < lengthof(codes); which++) {
		ClusterRelmapRetentionDirectory before;

		xact_caller_fixture((UINT64CONST(7) << 32) + 2052, 0);
		before = relmap_runtime_shmem->directories[0];
		switch (which) {
		case 0: xact_parallel = true; break;
		case 1: xact_worker = true; break;
		case 2: xact_leaving = true; break;
		case 3: xact_removed = true; break;
		case 4: xact_join = CLUSTER_JOIN_GATE_BLOCK_53R60; break;
		case 5: xact_join = CLUSTER_JOIN_GATE_BLOCK_53R61; break;
		}
		xact_capture_error(codes[which]);
		UT_ASSERT_EQ(xact_error_level, levels[which]);
		UT_ASSERT_EQ(caller_assignments + extensions + native_publish_count + xact_locks, 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), 0);
		UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
		UT_ASSERT(xact_source_resource_owner == (ResourceOwner)(uintptr_t)99);
	}
}

UT_TEST(original_xact_failure_keeps_unassigned_top_and_existing_pin)
{
	uint64 first = (UINT64CONST(7) << 32) + 2052;
	ClusterRelmapOwnerSampleV2 sample;
	ClusterRelmapRetentionDirectory before;
	unsigned before_extensions;
	bool captured;

	xact_caller_fixture(first, 0);
	captured = cluster_relmap_owner_capture_v2(&sample);
	UT_ASSERT(captured);
	if (!captured) return;
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	before = relmap_runtime_shmem->directories[0];
	/* A fresh controlled xact stack beside the prior, still protected owner. */
	TopTransactionStateData.fullTransactionId = XactTopFullTransactionId = InvalidFullTransactionId;
	caller_top = InvalidFullTransactionId;
	variables.nextXid = FullTransactionIdFromU64(first + UINT64CONST(0x80000000));
	variables.xidVacLimit = (uint32)U64FromFullTransactionId(variables.nextXid) + 4096;
	variables.xidWarnLimit = (uint32)U64FromFullTransactionId(variables.nextXid) + 8192;
	before_extensions = extensions;
	xact_capture_error(ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE);
	UT_ASSERT_EQ(extensions, before_extensions);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	variables.nextXid = FullTransactionIdFromU64(first + UINT64CONST(0x7fffffff));
	retention_fault = 3;
	xact_capture_error(ERRCODE_DISK_FULL);
	UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), 0);
	UT_ASSERT_EQ(U64FromFullTransactionId(TopTransactionStateData.fullTransactionId), 0);
	UT_ASSERT_EQ(memcmp(&before, &relmap_runtime_shmem->directories[0], sizeof(before)), 0);
	UT_ASSERT_EQ(caller_assignments, 1);
	UT_ASSERT_EQ(xact_locks, 1);
	UT_ASSERT_EQ(caller_owner_lock_errors + lock_errors, 0);
}
