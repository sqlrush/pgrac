/* Original ResourceOwner dispatcher -> range owner -> native/G2 cleanup.
 * Reuse only the service fixture's storage/transport/backend leaves. */
#include "postgres.h"
#include "utils/resowner.h"

void k_unused_register_release(ResourceReleaseCallback callback, void *arg);
int k_service_owner_component_main(void);
#define CurrentResourceOwner k_unused_current_owner
#define AuxProcessResourceOwner k_unused_aux_owner
#define RegisterResourceReleaseCallback k_unused_register_release
#define main k_service_owner_component_main
#include "test_k_mx_g5_service.c"
#undef main
#undef RegisterResourceReleaseCallback
#undef AuxProcessResourceOwner
#undef CurrentResourceOwner

/* No replacement dispatcher, owner tree, callback list or release phases. */
#include "../../backend/utils/resowner/resowner.c"

static pg_on_exit_callback aux_exit;
static Datum aux_exit_arg;

/* The owners below contain only range resources. Other resource categories
 * must stay empty; reaching a release leaf is a fixture failure. */
#define UNEXPECTED_LEAF() do { fprintf(stderr, "unexpected resource: %s\n", __func__); abort(); } while (0)
void AbortBufferIO(Buffer buffer) { (void) buffer; UNEXPECTED_LEAF(); }
void PrintBufferLeakWarning(Buffer buffer) { (void) buffer; UNEXPECTED_LEAF(); }
void ReleaseBuffer(Buffer buffer) { (void) buffer; UNEXPECTED_LEAF(); }
void RelationClose(Relation relation) { (void) relation; UNEXPECTED_LEAF(); }
void dsm_detach(dsm_segment *segment) { (void) segment; UNEXPECTED_LEAF(); }
void jit_release_context(JitContext *context) { (void) context; UNEXPECTED_LEAF(); }
void pg_cryptohash_free(pg_cryptohash_ctx *context) { (void) context; UNEXPECTED_LEAF(); }
void pg_hmac_free(pg_hmac_ctx *context) { (void) context; UNEXPECTED_LEAF(); }
void ProcReleaseLocks(bool commit) { (void) commit; UNEXPECTED_LEAF(); }
void ReleasePredicateLocks(bool commit, bool parallel) { (void) commit; (void) parallel; UNEXPECTED_LEAF(); }
void LockReassignCurrentOwner(LOCALLOCK **locks, int count) { (void) locks; if (count != 0) UNEXPECTED_LEAF(); }
void LockReleaseCurrentOwner(LOCALLOCK **locks, int count) { (void) locks; if (count != 0) UNEXPECTED_LEAF(); }
void PrintCatCacheLeakWarning(HeapTuple tuple) { (void) tuple; UNEXPECTED_LEAF(); }
void PrintCatCacheListLeakWarning(CatCList *list) { (void) list; UNEXPECTED_LEAF(); }
void ReleaseCatCache(HeapTuple tuple) { (void) tuple; UNEXPECTED_LEAF(); }
void ReleaseCatCacheList(CatCList *list) { (void) list; UNEXPECTED_LEAF(); }
void ReleaseCachedPlan(CachedPlan *plan, ResourceOwner owner) { (void) plan; (void) owner; UNEXPECTED_LEAF(); }
void DecrTupleDescRefCount(TupleDesc desc) { (void) desc; UNEXPECTED_LEAF(); }
void UnregisterSnapshot(Snapshot snapshot) { (void) snapshot; UNEXPECTED_LEAF(); }
void FileClose(File file) { (void) file; UNEXPECTED_LEAF(); }
void on_shmem_exit(pg_on_exit_callback callback, Datum arg) { aux_exit = callback; aux_exit_arg = arg; }
int cluster_xid_origin_slot(TransactionId xid) { (void) xid; return 0; }

static ResourceOwner
owner_begin(void)
{
	ResourceOwner root;

	service_seed(257, 1);
	root = ResourceOwnerCreate(NULL, "range root");
	CurrentResourceOwner = root;
	return root;
}

static void
assert_alive(ClusterMxCompositionReceive **op, uint32 total)
{
	ClusterMxProofWindow window;

	UT_ASSERT(cluster_multixact_current_proofs_window(op, &window));
	UT_ASSERT_NOT_NULL(*op);
	UT_ASSERT_EQ(window.total, total);
	UT_ASSERT_EQ(window.first, 0);
	UT_ASSERT_EQ(window.count, 7);
	UT_ASSERT_EQ(window.members[0].xid, 100);
}

static void
assert_revoked(ClusterMxCompositionReceive **op)
{
	ClusterMxProofWindow window;
	ClusterMxProofWindow zero = {0};

	/* This entry searches the owner's list before dereferencing a borrowed
	 * operation. A pointer retained on a caller's old stack is not a grant. */
	UT_ASSERT(!cluster_multixact_current_proofs_window(op, &window));
	UT_ASSERT_NULL(*op);
	UT_ASSERT_EQ(memcmp(&window, &zero, sizeof(window)), 0);
	cluster_multixact_current_describe_finish(op);
	cluster_multixact_current_describe_finish(op);
}

static void
owner_end(ResourceOwner root)
{
	ResourceOwner saved = CurrentResourceOwner;

	ResourceOwnerRelease(root, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
	UT_ASSERT(CurrentResourceOwner == saved);
	ResourceOwnerRelease(root, RESOURCE_RELEASE_LOCKS, false, true);
	ResourceOwnerRelease(root, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(root);
	UT_ASSERT_EQ(live_contexts, 1);
	/* Only the process-lifetime registered callback remains in TopMemoryContext. */
	UT_ASSERT_EQ(live_blocks, 1);
	UT_ASSERT_EQ(io_live, 0);
}

static void
child_abort_preserves_parent_and_sibling(void)
{
	ResourceOwner root = owner_begin();
	ResourceOwner child = ResourceOwnerCreate(root, "aborted child");
	ResourceOwner sibling = ResourceOwnerCreate(root, "live sibling");
	ClusterMxCompositionReceive *parent_op = fetch(257);
	ClusterMxCompositionReceive *child_op;
	ClusterMxCompositionReceive *sibling_op;

	CurrentResourceOwner = child;
	child_op = fetch(257);
	CurrentResourceOwner = sibling;
	sibling_op = fetch(257);
	UT_ASSERT_EQ(live_contexts, 4);
	ResourceOwnerRelease(child, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	UT_ASSERT(CurrentResourceOwner == sibling);
	UT_ASSERT_EQ(live_contexts, 3);
	assert_revoked(&child_op);
	assert_alive(&parent_op, 257);
	assert_alive(&sibling_op, 257);
	owner_end(root);
	assert_revoked(&parent_op);
	assert_revoked(&sibling_op);
}

static void
root_exit_recurses_through_descendants(void)
{
	ResourceOwner root = owner_begin();
	ResourceOwner child = ResourceOwnerCreate(root, "child");
	ResourceOwner grandchild = ResourceOwnerCreate(child, "grandchild");
	ClusterMxCompositionReceive *ops[3];
	ResourceOwner owners[3] = {root, child, grandchild};

	for (unsigned i = 0; i < 3; i++)
	{
		CurrentResourceOwner = owners[i];
		ops[i] = fetch(257);
	}
	UT_ASSERT_EQ(live_contexts, 4);
	ResourceOwnerRelease(root, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	UT_ASSERT(CurrentResourceOwner == grandchild);
	UT_ASSERT_EQ(live_contexts, 1);
	for (unsigned i = 0; i < 3; i++)
		assert_revoked(&ops[i]);
	owner_end(root);
}

static void
lock_phases_preserve_operation_until_before_locks(void)
{
	ResourceOwner root = owner_begin();
	ResourceOwner child = ResourceOwnerCreate(root, "phase owner");
	ClusterMxCompositionReceive *op;

	CurrentResourceOwner = child;
	op = fetch(257);
	ResourceOwnerRelease(child, RESOURCE_RELEASE_LOCKS, false, false);
	ResourceOwnerRelease(child, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
	UT_ASSERT_EQ(live_contexts, 2);
	assert_alive(&op, 257);
	ResourceOwnerRelease(child, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	UT_ASSERT_EQ(live_contexts, 1);
	assert_revoked(&op);
	owner_end(root);
}

static void
explicit_finish_then_owner_release_is_idempotent(void)
{
	ResourceOwner root = owner_begin();
	ClusterMxCompositionReceive *op = fetch(257);
	unsigned baseline;

	cluster_multixact_current_describe_finish(&op);
	UT_ASSERT_NULL(op);
	UT_ASSERT_EQ(live_contexts, 1);
	baseline = live_blocks;
	cluster_multixact_current_describe_finish(&op);
	ResourceOwnerRelease(root, RESOURCE_RELEASE_BEFORE_LOCKS, true, true);
	ResourceOwnerRelease(root, RESOURCE_RELEASE_BEFORE_LOCKS, false, true);
	UT_ASSERT_EQ(live_blocks, baseline);
	owner_end(root);
}

static void
failed_describe_then_child_abort(bool cancel)
{
	ResourceOwner root = owner_begin();
	ResourceOwner child = ResourceOwnerCreate(root, "cancelled child");
	ClusterMxCompositionReceive *parent_op = fetch(257);
	ClusterMxCompositionReceive *child_op;
	static ClusterMxCompositionReceive *interrupted;
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterCurrentMxKey key = {.multixact_id = 1000, .cluster_epoch = 7};
	volatile bool caught = false;

	CurrentResourceOwner = child;
	child_op = fetch(257);
	interrupted = NULL;
	inject = cancel ? 12 : 0;
	native_fail_at = cancel ? 0 : 2;
	offset_reads = member_reads = 0;
	frame_calls = 0;
	PG_TRY();
	{
		(void) cluster_multixact_current_describe_full(&key, &request_tag,
			&request_snapshot, &interrupted, &stats);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_NULL(interrupted);
	UT_ASSERT_EQ(live_contexts, 3);
	UT_ASSERT_EQ(io_live, 0);
	for (unsigned i = 0; i < 4; i++)
		UT_ASSERT_EQ(held[i], 0);
	InterruptPending = false;
	native_fail_at = 0;
	ResourceOwnerRelease(child, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
	UT_ASSERT_EQ(live_contexts, 2);
	assert_revoked(&child_op);
	assert_alive(&parent_op, 257);
	owner_end(root);
	assert_revoked(&parent_op);
}

static void
error_then_child_abort_retires_previous_operation(void)
{
	failed_describe_then_child_abort(false);
}

static void
cancel_then_child_abort_retires_previous_operation(void)
{
	failed_describe_then_child_abort(true);
}

static void
aux_exit_cleans_fallback_owner(void)
{
	ClusterMxCompositionReceive *op;

	service_seed(257, 1);
	UT_ASSERT_NULL(CurrentResourceOwner);
	UT_ASSERT_NULL(AuxProcessResourceOwner);
	CreateAuxProcessResourceOwner();
	UT_ASSERT_NOT_NULL(aux_exit);
	CurrentResourceOwner = NULL;
	op = fetch(257);
	UT_ASSERT_EQ(live_contexts, 2);
	/* Invoke the callback registered by original CreateAuxProcessResourceOwner. */
	aux_exit(1, aux_exit_arg);
	UT_ASSERT_NULL(CurrentResourceOwner);
	UT_ASSERT_EQ(live_contexts, 1);
	assert_revoked(&op);
	aux_exit(0, aux_exit_arg);
	ResourceOwnerDelete(AuxProcessResourceOwner);
	AuxProcessResourceOwner = NULL;
	UT_ASSERT_EQ(live_blocks, 1);
}

static void
missing_owner_refuses_without_retaining_output(void)
{
	ClusterMxCompositionReceive *op = NULL;
	ClusterMxCompositionReceiveStats stats = {0};
	ClusterCurrentMxKey key = {.multixact_id = 1000, .cluster_epoch = 7};
	unsigned baseline = live_blocks;

	service_seed(257, 1);
	UT_ASSERT_NULL(CurrentResourceOwner);
	UT_ASSERT_NULL(AuxProcessResourceOwner);
	UT_ASSERT(!cluster_multixact_current_describe_full(&key, &request_tag,
		&request_snapshot, &op, &stats));
	UT_ASSERT_NULL(op);
	UT_ASSERT_EQ(live_contexts, 1);
	UT_ASSERT_EQ(live_blocks, baseline);
	UT_ASSERT_EQ(frame_calls, 0);
}

int
main(void)
{
	TopMemoryContext = AllocSetContextCreate(CurrentMemoryContext, "owner test top",
		ALLOCSET_DEFAULT_SIZES);
	CurrentMemoryContext = TopMemoryContext;
	UT_PLAN(8);
	UT_RUN(child_abort_preserves_parent_and_sibling);
	UT_RUN(root_exit_recurses_through_descendants);
	UT_RUN(lock_phases_preserve_operation_until_before_locks);
	UT_RUN(explicit_finish_then_owner_release_is_idempotent);
	UT_RUN(error_then_child_abort_retires_previous_operation);
	UT_RUN(cancel_then_child_abort_retires_previous_operation);
	UT_RUN(aux_exit_cleans_fallback_owner);
	UT_RUN(missing_owner_refuses_without_retaining_output);
	MemoryContextDelete(TopMemoryContext);
	UT_ASSERT_EQ(live_contexts, 0);
	UT_ASSERT_EQ(live_blocks, 0);
	UT_DONE();
	return ut_failed_count == 0 && ut_current_failed == 0 ? 0 : 1;
}
