/* Native index owner and immutable CR producer; transport/storage boundaries
 * are controlled here, with their original owners tested separately.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_itl.h"
#include "storage/buf_internals.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

bool cluster_enabled;
int cluster_node_id;
bool cluster_shared_config = true;
bool cluster_shared_catalog = true;
ResourceOwner CurrentResourceOwner;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
int XactIsoLevel;
int NBuffers = 32;
int NLocBuffer;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

static RelationData relation;
static FormData_pg_class relform;
static SnapshotData snapshot;
static IndexFetchHeapData scan;
static uint64 next_scope;
static uint64 snapshot_identity;
static uint64 epoch;
static bool retained, target, locked, catalog, visible;
static TransactionId own_xid;
static int admission_depth, snapshot_depth, pins;
static unsigned copies, reserves, fetches, publishes, searches, releases;
static unsigned fault;
static ClusterCrBuildResult build_result;
static ClusterCrBuildReason build_reason;
static ClusterSemanticAdmissionResult entry_result;
static BufferCrKey stored_key;
static bool stored;
static PGAlignedBlock stored_page;
static HeapHotSearchResult result;
static ItemPointerData tid;
static unsigned native_reads, native_locks, native_searches, slot_stores, frees;
static bool current_pin, content_share;
static unsigned native_mode; /* 0 original FULL, 1 live tuple, 2 live absence */
static PGAlignedBlock full_page;
static TupleTableSlot test_slot = { .tts_ops = &TTSOpsBufferHeapTuple };

const TupleTableSlotOps TTSOpsBufferHeapTuple = { 0 };

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

static pg_attribute_noreturn() void heap_hot_r4_unknown(const char *reason pg_attribute_unused())
{
	pg_re_throw();
}

static void
heap_hot_r4_full_failure(SCN scn pg_attribute_unused(),
						 ClusterCrBuildResult rc pg_attribute_unused(),
						 ClusterCrBuildReason reason pg_attribute_unused())
{
	pg_re_throw();
}

bool
IsCatalogRelation(Relation rel pg_attribute_unused())
{
	return catalog;
}
TransactionId
GetTopTransactionIdIfAny(void)
{
	return own_xid;
}
bool
CheckRelationLockedByMe(Relation rel pg_attribute_unused(), LOCKMODE mode, bool stronger)
{
	UT_ASSERT_EQ(mode, AccessShareLock);
	UT_ASSERT(stronger);
	return locked;
}
bool
BufTableNewCRScope(uint64 *id)
{
	*id = ++next_scope;
	return true;
}

void
cluster_snapshot_read_enter_v1(ClusterSnapshotReadScopeV1 *scope, Snapshot snap)
{
	memset(scope, 0, sizeof(*scope));
	scope->snapshot = snap;
	snapshot_depth++;
}

void
cluster_snapshot_read_exit_v1(ClusterSnapshotReadScopeV1 *scope pg_attribute_unused())
{
	snapshot_depth--;
}

bool
cluster_snapshot_cr_identity_v1(Snapshot snap, uint64 *id)
{
	UT_ASSERT_EQ(snapshot_depth, 1);
	if (!retained || snap != &snapshot || snap->read_epoch != epoch)
		return false;
	*id = snapshot_identity;
	return true;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter(uint64 feature, ClusterSemanticAdmissionSide side,
								  ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_EQ(feature, CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1);
	UT_ASSERT_EQ(side, CLUSTER_SEMANTIC_TARGET_SIDE);
	memset(token, 0, sizeof(*token));
	if (entry_result != CLUSTER_SEMANTIC_ADMISSION_OK)
		return entry_result;
	if (!target)
		return CLUSTER_SEMANTIC_ADMISSION_TARGET_DISABLED;
	token->feature_bit = feature;
	token->side = side;
	token->formation_epoch = epoch;
	token->record_generation = 12;
	token->entered = true;
	admission_depth++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck(const ClusterSemanticAdmissionToken *token)
{
	UT_ASSERT_EQ(admission_depth, 1);
	return target && token->entered && token->record_generation == 12
		   && token->formation_epoch == epoch;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	if (token->entered)
		admission_depth--;
	memset(token, 0, sizeof(*token));
}

Buffer
cluster_bufmgr_cr_reserve_v1(void)
{
	UT_ASSERT(!content_share);
	reserves++;
	pins++;
	return 1;
}

void
ReleaseBuffer(Buffer buffer)
{
	if (buffer == 2) {
		UT_ASSERT(current_pin);
		current_pin = false;
		return;
	}
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(pins, 1);
	pins--;
	releases++;
}

bool
cluster_bufmgr_cr_copy_v1(const BufferCrKey *key, void *page)
{
	copies++;
	if (fault == 1)
		pg_re_throw();
	if (!stored || memcmp(key, &stored_key, sizeof(*key)) != 0)
		return false;
	memcpy(page, stored_page.data, BLCKSZ);
	return true;
}

static void
build_page(char *data)
{
	Page page = (Page)data;
	PageHeader header = (PageHeader)page;
	Size length = MAXALIGN(SizeofHeapTupleHeader + 1);
	HeapTupleHeader tuple;
	memset(data, 0, BLCKSZ);
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = header->pd_special - length;
	ItemIdSetNormal(PageGetItemId(page, 1), header->pd_upper, length);
	tuple = (HeapTupleHeader)(data + header->pd_upper);
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_infomask = HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(tuple, 99);
	ItemPointerSet(&tuple->t_ctid, 7, 1);
}

ClusterCrBuildResult
cluster_gcs_block_cr_fetch_and_wait(BufferTag tag, SCN scn, char *page,
									ClusterCrBuildReason *reason)
{
	UT_ASSERT_EQ(pins, 0);
	UT_ASSERT(current_pin && !content_share);
	UT_ASSERT(native_reads > 0 && native_locks > 0);
	UT_ASSERT_EQ(admission_depth, 1);
	UT_ASSERT_EQ(tag.blockNum, 7);
	UT_ASSERT_EQ(scn, snapshot.read_scn);
	fetches++;
	if (fault == 2)
		pg_re_throw();
	if (fault == 3)
		epoch++;
	if (fault == 4)
		snapshot.curcid++;
	if (fault == 5)
		relation.rd_locator.relNumber++;
	if (fault == 6)
		snapshot_identity++;
	if (fault == 7)
		retained = false;
	if (fault == 8)
		target = false;
	if (fault == 9)
		CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
	memcpy(page, full_page.data, BLCKSZ);
	*reason = build_reason;
	return build_result;
}

bool
HeapTupleSatisfiesMVCCScratch(HeapTuple tuple, Snapshot snap,
							  const ClusterR4HotScratchTestContext *context)
{
	UT_ASSERT_EQ(admission_depth, 1);
	UT_ASSERT_EQ(snapshot_depth, 1);
	UT_ASSERT(snap == &snapshot);
	UT_ASSERT(context->already_full && !context->allow_hint && !context->allow_cleanout);
	UT_ASSERT_EQ(tuple->t_tableOid, RelationGetRelid(&relation));
	searches++;
	if (fault == 10)
		pg_re_throw();
	if (fault == 11)
		epoch++;
	return visible;
}

static void
heap_hot_r4_snapshot_too_old(SCN read_scn pg_attribute_unused(),
							 SCN recycle_scn pg_attribute_unused())
{
	pg_re_throw();
}

#include "test_cluster_heap_small_scn.inc"
#include "test_cluster_heap_cr_reuse_scratch.inc"

bool
cluster_bufmgr_cr_publish_v1(Buffer buffer, const BufferCrKey *key, const void *page)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(pins, 1);
	UT_ASSERT_EQ(admission_depth, 1);
	publishes++;
	if (fault == 12)
		pg_re_throw();
	stored_key = *key;
	memcpy(stored_page.data, page, BLCKSZ);
	stored = true;
	return true;
}

#include "test_cluster_heap_cr_reuse_owner.inc"

Buffer
ReleaseAndReadBuffer(Buffer buffer pg_attribute_unused(), Relation rel pg_attribute_unused(),
					 BlockNumber block pg_attribute_unused())
{
	native_reads++;
	current_pin = true;
	return 2;
}

void
heap_page_prune_opt(Relation rel pg_attribute_unused(), Buffer buffer pg_attribute_unused())
{}
void
LockBuffer(Buffer buffer pg_attribute_unused(), int mode)
{
	if (mode == BUFFER_LOCK_SHARE)
		native_locks++;
	content_share = mode == BUFFER_LOCK_SHARE;
}
bool
ClusterLockBufferShareBarrierAware(Buffer buffer)
{
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	return true;
}

/* The original current/FULL implementation has its own production-body
 * lock-order suite. Here its output boundary models FULL vs live, and the
 * actual handler/cache owner must never turn a live result into a FULL. */
HeapHotSearchResultKind
heap_hot_search_buffer_result(ItemPointer root, Relation rel, Buffer buffer, Snapshot snap,
							  HeapHotSearchResult *out, bool *all_dead,
							  bool first pg_attribute_unused())
{
	ClusterSemanticAdmissionToken admission;
	ClusterSnapshotReadScopeV1 read_scope;
	BufferTag tag;
	ClusterCrBuildReason reason;
	ClusterCrBuildResult rc;
	bool found = false;

	UT_ASSERT(current_pin && content_share);
	native_searches++;
	memset(out, 0, sizeof(*out));
	if (all_dead != NULL)
		*all_dead = native_mode == 2;
	if (native_mode != 0) {
		out->kind = native_mode == 1 ? HEAP_HOT_SEARCH_OWNED_CURRENT : HEAP_HOT_SEARCH_NOT_FOUND;
		return out->kind;
	}
	InitBufferTag(&tag, &rel->rd_locator, MAIN_FORKNUM, ItemPointerGetBlockNumber(root));
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	UT_ASSERT_EQ(cluster_semantic_activation_enter(CLUSTER_SEMANTIC_FEATURE_R4_SYNC_CR_V1,
												   CLUSTER_SEMANTIC_TARGET_SIDE, &admission),
				 CLUSTER_SEMANTIC_ADMISSION_OK);
	cluster_snapshot_read_enter_v1(&read_scope, snap);
	PG_TRY();
	{
		rc = cluster_gcs_block_cr_fetch_and_wait(tag, snap->read_scn, out->scratch_page, &reason);
		if (rc != CLUSTER_CR_BUILD_FULL || reason != CLUSTER_CR_BUILD_NONE)
			heap_hot_r4_full_failure(snap->read_scn, rc, reason);
		found = heap_hot_r4_search_scratch(&tag, root, rel, snap, out, false);
		out->cr_full_page = true;
	}
	PG_FINALLY();
	{
		cluster_snapshot_read_exit_v1(&read_scope);
		cluster_semantic_activation_leave(&admission);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
	}
	PG_END_TRY();
	out->kind = found ? HEAP_HOT_SEARCH_OWNED_SCRATCH : HEAP_HOT_SEARCH_NOT_FOUND;
	return out->kind;
}

/* The original slot-copy implementation has separate R4 runtime tests. */
static TableIndexFetchTupleResult
heapam_store_hot_search_result(HeapHotSearchResult *out, TupleTableSlot *slot pg_attribute_unused(),
							   Buffer buffer, bool *call_again,
							   bool *all_dead pg_attribute_unused())
{
	slot_stores++;
	if (fault == 13)
		pg_re_throw();
	UT_ASSERT(buffer == InvalidBuffer || buffer == 2);
	result = *out;
	*call_again = false;
	return out->kind == HEAP_HOT_SEARCH_NOT_FOUND ? TABLE_INDEX_FETCH_NOT_FOUND
												  : TABLE_INDEX_FETCH_FOUND;
}

void
pfree(void *ptr pg_attribute_unused())
{
	frees++;
}

#undef ereport
#define ereport(level, rest) pg_re_throw()
#include "test_cluster_heap_cr_reuse_handler.inc"

static void
setup(void)
{
	memset(&relation, 0, sizeof(relation));
	memset(&relform, 0, sizeof(relform));
	memset(&snapshot, 0, sizeof(snapshot));
	memset(&scan, 0, sizeof(scan));
	memset(&result, 0, sizeof(result));
	relation.rd_rel = &relform;
	relation.rd_id = 18000;
	relation.rd_refcnt = 1;
	relation.rd_locator = (RelFileLocator){ 1663, 5, 18001 };
	relform.relkind = RELKIND_RELATION;
	relform.relpersistence = RELPERSISTENCE_PERMANENT;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_scn = 100;
	snapshot.read_epoch = epoch = 4;
	snapshot.curcid = 2;
	snapshot_identity = 17;
	scan.xs_base.rel = &relation;
	scan.xs_cbuf = InvalidBuffer;
	CurrentResourceOwner = (ResourceOwner)(uintptr_t)1;
	cluster_shared_config = cluster_shared_catalog = true;
	retained = target = locked = cluster_enabled = visible = true;
	catalog = stored = false;
	own_xid = InvalidTransactionId;
	XactIsoLevel = XACT_READ_COMMITTED;
	admission_depth = snapshot_depth = pins = 0;
	copies = reserves = fetches = publishes = searches = releases = fault = 0;
	native_reads = native_locks = native_searches = slot_stores = frees = 0;
	current_pin = content_share = false;
	native_mode = 0;
	build_page(full_page.data);
	build_result = CLUSTER_CR_BUILD_FULL;
	build_reason = CLUSTER_CR_BUILD_NONE;
	entry_result = CLUSTER_SEMANTIC_ADMISSION_OK;
	ItemPointerSet(&tid, 7, 1);
}

static bool
call_throws(void)
{
	volatile bool threw = false;
	PG_TRY();
	{
		(void)heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result);
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	return threw;
}

/* Seed an existing version without asking the miss path to construct one. */
static void
seed_cached_page(void)
{
	HeapReadOnlyCrScope *scope = &scan.cr_scope;
	scope->relation = &relation;
	scope->owner = CurrentResourceOwner;
	scope->locator = relation.rd_locator;
	scope->relation_oid = relation.rd_id;
	scope->command_id = snapshot.curcid;
	scope->snapshot_id = snapshot_identity;
	scope->read_scn = snapshot.read_scn;
	scope->read_epoch = snapshot.read_epoch;
	scope->scan_id = ++next_scope;
	memset(&stored_key, 0, sizeof(stored_key));
	InitBufferTag(&stored_key.tag, &scope->locator, MAIN_FORKNUM, 7);
	stored_key.scan_identity = scope->scan_id;
	stored_key.snapshot_identity = snapshot_identity;
	stored_key.read_scn = snapshot.read_scn;
	stored_key.read_epoch = snapshot.read_epoch;
	build_page(stored_page.data);
	stored = true;
}

static TableIndexFetchTupleResult
handler_fetch(void)
{
	bool again = false, dead = true;
	return heapam_index_fetch_tuple_internal(&scan.xs_base, &tid, &snapshot, &test_slot, &again,
											 &dead, false, NULL, NULL);
}

static bool
handler_throws(void)
{
	volatile bool threw = false;
	PG_TRY();
	{
		(void)handler_fetch();
	}
	PG_CATCH();
	{
		threw = true;
	}
	PG_END_TRY();
	return threw;
}

UT_TEST(miss_full_then_same_scan_hit_keeps_original_visibility)
{
	setup();
	UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(fetches, 1);
	UT_ASSERT_EQ(publishes, 1);
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(scan.cr_scope.scan_id, next_scope);
	UT_ASSERT(heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
	UT_ASSERT_EQ(fetches, 1);
	UT_ASSERT_EQ(copies, 2);
	UT_ASSERT_EQ(searches, 2);
	UT_ASSERT_EQ(reserves, 1);
	UT_ASSERT_EQ(releases, 1);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
}

UT_TEST(scope_changes_never_reuse_the_previous_image)
{
	for (unsigned change = 0; change < 6; change++) {
		uint64 old;
		setup();
		seed_cached_page();
		old = scan.cr_scope.scan_id;
		if (change == 0)
			snapshot_identity++;
		if (change == 1)
			snapshot.curcid++;
		if (change == 2)
			snapshot.read_scn++;
		if (change == 3)
			relation.rd_locator.relNumber++;
		if (change == 4)
			CurrentResourceOwner = (ResourceOwner)(uintptr_t)2;
		if (change == 5)
			memset(&scan.cr_scope, 0, sizeof(scan.cr_scope));
		UT_ASSERT(!heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
		UT_ASSERT(scan.cr_scope.scan_id != old);
		UT_ASSERT_EQ(fetches + publishes + searches, 0);
	}
}

UT_TEST(late_full_or_error_cannot_publish_or_keep_scope)
{
	for (unsigned injection = 1; injection <= 12; injection++) {
		setup();
		fault = injection;
		UT_ASSERT(handler_throws());
		UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
		UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
		UT_ASSERT_EQ(publishes, injection == 12 ? 1 : 0);
		UT_ASSERT(!stored);
	}
}

UT_TEST(retention_or_snapshot_epoch_refuses_before_cache_access)
{
	for (unsigned invalid = 0; invalid < 2; invalid++) {
		setup();
		if (invalid == 0)
			retained = false;
		else
			epoch++;
		UT_ASSERT(call_throws());
		UT_ASSERT_EQ(copies + fetches + publishes, 0);
		UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
	}
}

UT_TEST(nonfull_or_nonpositive_result_never_becomes_cached)
{
	for (unsigned invalid = 0; invalid < 3; invalid++) {
		setup();
		if (invalid == 0)
			build_result = CLUSTER_CR_BUILD_RETRYABLE;
		if (invalid == 1)
			build_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
		if (invalid == 2)
			build_reason = CLUSTER_CR_BUILD_PROTOCOL;
		UT_ASSERT(handler_throws());
		UT_ASSERT_EQ(publishes, 0);
		UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
		UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
	}
}

UT_TEST(ineligible_paths_do_not_enter_cr_or_keep_old_scope)
{
	for (unsigned invalid = 0; invalid < 11; invalid++) {
		setup();
		scan.cr_scope.scan_id = 123;
		if (invalid == 0)
			cluster_shared_config = false;
		if (invalid == 1)
			cluster_shared_catalog = false;
		if (invalid == 2)
			catalog = true;
		if (invalid == 3)
			relform.relpersistence = RELPERSISTENCE_TEMP;
		if (invalid == 4)
			snapshot.snapshot_type = SNAPSHOT_DIRTY;
		if (invalid == 5)
			own_xid = 99;
		if (invalid == 6)
			XactIsoLevel = XACT_SERIALIZABLE;
		if (invalid == 7)
			locked = false;
		if (invalid == 8)
			cluster_enabled = false;
		if (invalid == 9)
			relform.relisshared = true;
		if (invalid == 10)
			relation.rd_refcnt = 0;
		UT_ASSERT(!heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
		UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
		UT_ASSERT_EQ(copies + fetches + publishes + searches, 0);
	}
}

UT_TEST(target_disabled_does_not_create_or_consume_cr)
{
	setup();
	target = false;
	scan.cr_scope.scan_id = 55;
	UT_ASSERT(!heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
	UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
	UT_ASSERT_EQ(copies + fetches + publishes, 0);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
}

UT_TEST(closed_target_is_not_a_dormant_source_fallback)
{
	setup();
	entry_result = CLUSTER_SEMANTIC_ADMISSION_CLOSED;
	scan.cr_scope.scan_id = 55;
	UT_ASSERT(call_throws());
	UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
	UT_ASSERT_EQ(copies + fetches + publishes, 0);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
}

UT_TEST(full_invisible_root_preserves_not_found)
{
	setup();
	visible = false;
	UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_NOT_FOUND);
	UT_ASSERT_EQ(publishes, 1);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
}

UT_TEST(native_handler_hits_before_current_read_or_share)
{
	bool again = false, dead = true;
	setup();
	seed_cached_page();
	UT_ASSERT_EQ(heapam_index_fetch_tuple_internal(&scan.xs_base, &tid, &snapshot, &test_slot,
												   &again, &dead, true, NULL, NULL),
				 TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(fetches, 0);
	UT_ASSERT_EQ(searches, 1);
	UT_ASSERT_EQ(native_reads + native_locks + native_searches, 0);
	UT_ASSERT(!again && !dead);
	UT_ASSERT_EQ(slot_stores, 1);
	UT_ASSERT_EQ(scan.xs_cbuf, InvalidBuffer);
	UT_ASSERT_EQ(sizeof(HeapReadOnlyCrScope), 72);
}

UT_TEST(original_reset_and_end_retire_scope_and_current_pin)
{
	setup();
	scan.cr_scope.scan_id = 44;
	scan.xs_cbuf = 2;
	current_pin = true;
	heapam_index_fetch_reset(&scan.xs_base);
	UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
	UT_ASSERT_EQ(scan.xs_cbuf, InvalidBuffer);
	UT_ASSERT(!current_pin);
	scan.cr_scope.scan_id = 55;
	heapam_index_fetch_end(&scan.xs_base);
	UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
	UT_ASSERT_EQ(frees, 1);
}

UT_TEST(slot_error_retires_scope_after_reservation_has_been_released)
{
	setup();
	fault = 13;
	UT_ASSERT(handler_throws());
	UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
	UT_ASSERT_EQ(releases, 1);
}

UT_TEST(ordinary_noneligible_handler_preserves_current_path)
{
	setup();
	own_xid = 88;
	native_mode = 2;
	UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(native_locks, 1);
	UT_ASSERT_EQ(native_searches, 1);
	UT_ASSERT_EQ(copies + fetches + publishes + searches, 0);
	heapam_index_fetch_reset(&scan.xs_base);
	UT_ASSERT(!current_pin);
}

UT_TEST(cached_invisible_result_never_marks_index_entry_dead)
{
	bool again = false, dead = true;
	setup();
	seed_cached_page();
	visible = false;
	UT_ASSERT_EQ(heapam_index_fetch_tuple_internal(&scan.xs_base, &tid, &snapshot, &test_slot,
												   &again, &dead, false, NULL, NULL),
				 TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT(!again && !dead);
	UT_ASSERT_EQ(native_reads + native_locks, 0);
}

UT_TEST(cached_page_before_concurrent_insert_ignores_new_index_root)
{
	setup();
	seed_cached_page();
	ItemPointerSetOffsetNumber(&tid, 2);
	UT_ASSERT(!call_throws());
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_NOT_FOUND);
	UT_ASSERT_EQ(fetches + searches, 0);
	UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
}

UT_TEST(cached_page_still_refuses_broken_hot_edges_and_invalid_roots)
{
	for (unsigned invalid = 0; invalid < 6; invalid++) {
		Page page;
		HeapTupleHeader tuple;
		setup();
		seed_cached_page();
		page = (Page)stored_page.data;
		tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
		visible = false;
		if (invalid == 0) {
			tuple->t_infomask = 0;
			tuple->t_infomask2 |= HEAP_HOT_UPDATED;
			HeapTupleHeaderSetXmax(tuple, 100);
			ItemPointerSet(&tuple->t_ctid, 7, 2);
		}
		if (invalid == 1)
			ItemIdSetRedirect(PageGetItemId(page, 1), 2);
		if (invalid == 2)
			ItemPointerSetOffsetNumber(&tid, MaxHeapTuplesPerPage + 1);
		if (invalid == 3)
			ClusterPageGetItlHeader(page)->itl_recycle_watermark_scn = snapshot.read_scn + 1;
		if (invalid == 4)
			((PageHeader)page)->pd_pagesize_version = 0;
		if (invalid == 5) {
			((PageHeader)page)->pd_lower += sizeof(ItemIdData);
			ItemIdSetDead(PageGetItemId(page, 2));
			ItemIdSetRedirect(PageGetItemId(page, 1), 2);
		}
		UT_ASSERT(call_throws());
		UT_ASSERT_EQ(scan.cr_scope.scan_id, 0);
		UT_ASSERT_EQ(fetches, 0);
		UT_ASSERT_EQ(pins + admission_depth + snapshot_depth, 0);
	}
}

UT_TEST(miss_without_current_holder_returns_to_native_acquisition)
{
	/* Cold/evicted and ambiguous holder both lack a usable FULL source.
	 * Cache miss must reach current acquisition without consulting that source. */
	for (unsigned state = 0; state < 2; state++) {
		setup();
		build_result = CLUSTER_CR_BUILD_RETRYABLE;
		native_mode = state == 0 ? 1 : 2;
		UT_ASSERT(!call_throws());
		UT_ASSERT_EQ(fetches + reserves + publishes, 0);
		UT_ASSERT(!heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
		UT_ASSERT_EQ(handler_fetch(),
					 state == 0 ? TABLE_INDEX_FETCH_FOUND : TABLE_INDEX_FETCH_NOT_FOUND);
		UT_ASSERT_EQ(native_reads, 1);
		UT_ASSERT_EQ(native_searches, 1);
		UT_ASSERT_EQ(fetches + reserves + publishes, 0);
	}
}

UT_TEST(cached_dead_root_is_not_found)
{
	bool again = false, dead = true;
	setup();
	seed_cached_page();
	ItemIdSetDead(PageGetItemId((Page)stored_page.data, 1));
	UT_ASSERT(!call_throws());
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_NOT_FOUND);
	UT_ASSERT_EQ(heapam_index_fetch_tuple_internal(&scan.xs_base, &tid, &snapshot, &test_slot,
												   &again, &dead, false, NULL, NULL),
				 TABLE_INDEX_FETCH_NOT_FOUND);
	UT_ASSERT(!dead && !again);
	UT_ASSERT_EQ(searches + fetches + publishes + native_reads, 0);
}

UT_TEST(cached_effective_multixact_returns_to_original_visibility)
{
	HeapTupleHeader tuple;
	setup();
	seed_cached_page();
	tuple = (HeapTupleHeader)PageGetItem((Page)stored_page.data,
										 PageGetItemId((Page)stored_page.data, 1));
	tuple->t_infomask = HEAP_XMAX_IS_MULTI;
	HeapTupleHeaderSetXmax(tuple, 45);
	UT_ASSERT(!heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
	UT_ASSERT_EQ(searches + fetches + publishes, 0);
	native_mode = 1;
	UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(native_searches, 1);
	UT_ASSERT_EQ(searches + fetches + publishes, 0);
}

UT_TEST(live_point_fetch_and_rescan_add_no_full_or_publication)
{
	setup();
	native_mode = 1;
	for (unsigned i = 0; i < 3; i++) {
		UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_FOUND);
		heapam_index_fetch_reset(&scan.xs_base);
	}
	UT_ASSERT_EQ(native_reads, 3);
	UT_ASSERT_EQ(fetches + publishes + reserves, 0);
}

UT_TEST(lock_only_multixact_cache_remains_eligible)
{
	HeapTupleHeader tuple;
	setup();
	seed_cached_page();
	tuple = (HeapTupleHeader)PageGetItem((Page)stored_page.data,
										 PageGetItemId((Page)stored_page.data, 1));
	tuple->t_infomask = HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY;
	UT_ASSERT(heap_index_fetch_cr_result(&scan, &tid, &snapshot, &result));
	UT_ASSERT_EQ(result.kind, HEAP_HOT_SEARCH_OWNED_SCRATCH);
	UT_ASSERT_EQ(searches, 1);
	UT_ASSERT_EQ(fetches + native_reads, 0);
}

UT_TEST(full_with_unsupported_other_tuple_is_not_published)
{
	Page page;
	PageHeader header;
	HeapTupleHeader tuple;
	Size length = MAXALIGN(SizeofHeapTupleHeader + 1);
	setup();
	page = (Page)full_page.data;
	header = (PageHeader)page;
	header->pd_lower += sizeof(ItemIdData);
	header->pd_upper -= length;
	ItemIdSetNormal(PageGetItemId(page, 2), header->pd_upper, length);
	tuple = (HeapTupleHeader)(full_page.data + header->pd_upper);
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_infomask = HEAP_XMAX_IS_MULTI;
	HeapTupleHeaderSetXmin(tuple, 99);
	HeapTupleHeaderSetXmax(tuple, 45);
	UT_ASSERT_EQ(handler_fetch(), TABLE_INDEX_FETCH_FOUND);
	UT_ASSERT_EQ(fetches, 1);
	UT_ASSERT_EQ(publishes + reserves, 0);
	UT_ASSERT(!stored);
}

int
main(void)
{
	UT_RUN(miss_full_then_same_scan_hit_keeps_original_visibility);
	UT_RUN(scope_changes_never_reuse_the_previous_image);
	UT_RUN(late_full_or_error_cannot_publish_or_keep_scope);
	UT_RUN(retention_or_snapshot_epoch_refuses_before_cache_access);
	UT_RUN(nonfull_or_nonpositive_result_never_becomes_cached);
	UT_RUN(ineligible_paths_do_not_enter_cr_or_keep_old_scope);
	UT_RUN(target_disabled_does_not_create_or_consume_cr);
	UT_RUN(closed_target_is_not_a_dormant_source_fallback);
	UT_RUN(full_invisible_root_preserves_not_found);
	UT_RUN(native_handler_hits_before_current_read_or_share);
	UT_RUN(original_reset_and_end_retire_scope_and_current_pin);
	UT_RUN(slot_error_retires_scope_after_reservation_has_been_released);
	UT_RUN(ordinary_noneligible_handler_preserves_current_path);
	UT_RUN(cached_invisible_result_never_marks_index_entry_dead);
	UT_RUN(cached_page_before_concurrent_insert_ignores_new_index_root);
	UT_RUN(cached_page_still_refuses_broken_hot_edges_and_invalid_roots);
	UT_RUN(miss_without_current_holder_returns_to_native_acquisition);
	UT_RUN(cached_dead_root_is_not_found);
	UT_RUN(cached_effective_multixact_returns_to_original_visibility);
	UT_RUN(live_point_fetch_and_rescan_add_no_full_or_publication);
	UT_RUN(lock_only_multixact_cache_remains_eligible);
	UT_RUN(full_with_unsupported_other_tuple_is_not_published);
	printf("1..22\n");
	UT_DONE();
	return ut_failed_count != 0;
}
