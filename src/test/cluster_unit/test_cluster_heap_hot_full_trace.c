/* Exact FULL-cycle choreography and diagnostic byte readers. Transport and
 * scratch visibility are controlled boundaries, not replacement authority.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "storage/proc.h"
#include "access/heapam.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_guc.h"
#include "storage/buf_internals.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "../../backend/access/heap/heapam_r4_private.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_trace_normal_stop;
bool cluster_shared_config, cluster_recmerge_window_active, cluster_recmerge_apply_foreign;
uint64 cluster_recmerge_window_scn, cluster_recmerge_window_own_lsn;
int cluster_node_id = 2, MyProcPid = 4242;
PGPROC *MyProc = NULL;
BackendId MyBackendId = 0;
BackendType MyBackendType = B_BACKEND;
volatile uint32 CritSectionCount;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static int locked, fetches, searches, events, log_error_event, clock_reads;
static bool fetch_error, found;
static ClusterCrBuildResult fetch_result;
static ClusterCrBuildReason fetch_reason;
static char logs[32768], message[8192];
static PGAlignedBlock before, full;
static SnapshotData snapshot;
static RelationData relation;
static BufferTag tag;
static ItemPointerData root;
static OffsetNumber queried;
static HeapHotSearchResult result;
static ClusterCrBuildResult actual_result;
static ClusterCrBuildReason actual_reason;

void ExceptionalCondition(const char *c pg_attribute_unused(),
						  const char *f pg_attribute_unused(), int l pg_attribute_unused())
{ abort(); }
void pg_re_throw(void)
{ if (PG_exception_stack) siglongjmp(*PG_exception_stack, 1); abort(); }
bool errstart(int level, const char *domain pg_attribute_unused()) { return level == LOG; }
bool errstart_cold(int level, const char *domain) { return errstart(level, domain); }
int errmsg_internal(const char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(message, sizeof(message), fmt, args);
	va_end(args);
	return 0;
}
void errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
			   const char *func pg_attribute_unused())
{
	if (locked != 0) abort();
	events++;
	if (events == log_error_event) siglongjmp(*PG_exception_stack, 1);
	strncat(logs, message, sizeof(logs) - strlen(logs) - 2);
	strncat(logs, "\n", sizeof(logs) - strlen(logs) - 1);
}
TimestampTz GetCurrentTimestamp(void) { clock_reads++; return 12500; }
static uint64 cluster_heap_itl_now_us(void) pg_attribute_unused();
static uint64 cluster_heap_itl_now_us(void) { clock_reads++; return 22200; }
void LockBuffer(Buffer buffer pg_attribute_unused(), int mode)
{
	if (mode == BUFFER_LOCK_UNLOCK && locked == 1) locked = 0;
	else if (mode == BUFFER_LOCK_SHARE && locked == 0) locked = 1;
	else abort();
}
ClusterCrBuildResult
cluster_gcs_block_cr_fetch_projection(BufferTag input, SCN scn, char out[BLCKSZ],
									  ClusterR4ProjectionScratch *projection,
									  ClusterCrBuildReason *reason)
{
	if (locked || !BufferTagsEqual(&input, &tag) || scn != snapshot.read_scn) abort();
	fetches++;
	memset(projection, 0x5a, sizeof(*projection));
	projection->valid = true;
	if (fetch_error) siglongjmp(*PG_exception_stack, 1);
	memcpy(out, full.data, BLCKSZ);
	*reason = fetch_reason;
	return fetch_result;
}
static bool
heap_hot_r4_search_scratch(const BufferTag *input, const ItemPointerData *logical_root,
						 Relation rel, Snapshot snap, HeapHotSearchResult *out,
						 bool allow_current pg_attribute_unused())
{
	if (locked || !BufferTagsEqual(input, &tag) || rel != &relation
		|| snap != &snapshot
		|| ItemPointerGetBlockNumberNoCheck(logical_root) != ItemPointerGetBlockNumber(&root)
		|| ItemPointerGetOffsetNumberNoCheck(logical_root) != ItemPointerGetOffsetNumber(&root)) abort();
	searches++;
	out->visibility_trace.total = 1;
	out->visibility_trace.items[0].offset = queried;
	out->visibility_trace.items[0].xmin = 31337;
	out->visibility_trace.items[0].complete = true;
	out->visibility_trace.items[0].visible = found;
	return found;
}

#include "test_cluster_heap_hot_full_trace_format.inc"
#include "test_cluster_heap_hot_full_trace.inc"

static HeapHotR4CycleOutcome
run_cycle(void)
{
#ifdef HEAP_HOT_R4_FULL_TRACE_PRESENT
	return heap_hot_r4_full_cycle(7, &tag, &root, queried, before.data,
								 &relation, &snapshot, &result, &actual_result, &actual_reason);
#else
	return heap_hot_r4_full_cycle(7, &tag, &root, &relation, &snapshot,
								 &result, &actual_result, &actual_reason);
#endif
}
static bool
run_catching(void)
{
	volatile bool caught = false;
	PG_TRY(); { (void)run_cycle(); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	return caught;
}
static HeapTupleHeader
tuple_at(Page page)
{ return (HeapTupleHeader)((char *)page + ItemIdGetOffset(PageGetItemId(page, 3))); }
static void
reset_fixture(bool trace)
{
	PageHeader h = (PageHeader)before.data;
	HeapTupleHeader tuple;
	ClusterItlSlotData *slot;
	memset(&before, 0, sizeof(before));
	h->pd_lower = SizeOfPageHeaderData + 3 * sizeof(ItemIdData);
	h->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	h->pd_upper = h->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	h->pd_flags = PD_HAS_ITL;
	PageSetPageSizeAndVersion(before.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	PageSetLSN(before.data, 123456);
	h->pd_block_scn = 1234;
	ItemIdSetRedirect(PageGetItemId(before.data, 1), 3);
	ItemIdSetUnused(PageGetItemId(before.data, 2));
	ItemIdSetNormal(PageGetItemId(before.data, 3), h->pd_upper, MAXALIGN(SizeofHeapTupleHeader));
	tuple = tuple_at(before.data);
	tuple->t_hoff = SizeofHeapTupleHeader;
	HeapTupleHeaderSetXmin(tuple, 31337);
	HeapTupleHeaderSetXmax(tuple, 31338);
	HeapTupleHeaderSetCmin(tuple, 7);
	tuple->t_infomask |= HEAP_COMBOCID;
	ItemPointerSet(&tuple->t_ctid, 137, 3);
	tuple->t_itl_slot_idx = 2;
	slot = &ClusterPageGetItlSlots(before.data)[2];
	slot->xid = 31337; slot->wrap = 9; slot->flags = 1;
	slot->write_scn = 1200; slot->commit_scn = 1250;
	slot->undo_segment_head.raw[0] = UINT64_C(8589935105);
	slot->undo_segment_head.raw[1] = UINT64_C(196613);
	ClusterPageGetItlHeader(before.data)->itl_recycle_watermark_scn = 77;
	memcpy(full.data, before.data, BLCKSZ);
	PageSetLSN(full.data, 654321);
	((PageHeader)full.data)->pd_block_scn = 900;
	HeapTupleHeaderSetXmin(tuple_at(full.data), 30100);
	memset(&snapshot, 0, sizeof(snapshot));
	snapshot.read_scn = 999; snapshot.read_epoch = 71; snapshot.curcid = 8;
	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663; tag.dbOid = 5; tag.relNumber = 16388;
	tag.forkNum = MAIN_FORKNUM; tag.blockNum = 137;
	ItemPointerSet(&root, 137, 1); queried = 3;
	cluster_trace_normal_stop = trace;
	CritSectionCount = 0;
	locked = 1; fetches = searches = events = log_error_event = clock_reads = 0;
	fetch_error = false; found = true;
	fetch_result = CLUSTER_CR_BUILD_FULL; fetch_reason = CLUSTER_CR_BUILD_NONE;
	logs[0] = '\0';
	if (!heap_hot_r4_scratch_page_valid(before.data)) abort();
}

UT_TEST(default_off_has_no_log_or_clock_and_preserves_full)
{
	reset_fixture(false);
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_FOUND);
	UT_ASSERT(locked == 1 && fetches == 1 && searches == 1);
	UT_ASSERT(events == 0 && clock_reads == 0);
	UT_ASSERT(result.projection.valid);
	UT_ASSERT(result.cr_private_projection);
}
UT_TEST(actual_tag_root_and_queried_item_are_distinct)
{
	reset_fixture(true); (void)run_cycle();
	UT_ASSERT_EQ(events, 3);
	UT_ASSERT(strstr(logs, "spc=1663 db=5 rel=16388 fork=0 block=137") != NULL);
	UT_ASSERT(strstr(logs, "root_block=137 root_offset=1 queried_offset=3") != NULL);
	UT_ASSERT(strstr(logs, "before_root={item_known=1 lp=2") != NULL);
}
UT_TEST(owned_before_and_full_page_are_separate_observations)
{
	reset_fixture(true); (void)run_cycle();
	UT_ASSERT(strstr(logs, "before_lsn=123456 before_scn=1234") != NULL);
	UT_ASSERT(strstr(logs, "full_lsn=654321 full_scn=900") != NULL);
	UT_ASSERT(strstr(logs, "xmin=31337 xmax=31338") && strstr(logs, "xmin=30100 xmax=31338"));
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(tuple_at(before.data)), 31337);
}
UT_TEST(raw_cid_combo_and_uba_remain_projections)
{
	reset_fixture(true); (void)run_cycle();
	UT_ASSERT(strstr(logs, "raw_cid=7 combo=1") && strstr(logs, "snapshot_curcid=8"));
	UT_ASSERT(strstr(logs, "slot_known=1 slot_xid=31337 wrap=9"));
	UT_ASSERT(strstr(logs, "uba_raw=8589935105/196613"));
	UT_ASSERT(strstr(logs, "authoritative=0") && strstr(logs, "full_xid_known=0"));
}
UT_TEST(fetch_failure_keeps_refusal_and_hides_unqualified_scratch)
{
	reset_fixture(true); fetch_result = CLUSTER_CR_BUILD_FAIL_CLOSED;
	fetch_reason = CLUSTER_CR_BUILD_PROTOCOL;
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_FAILED);
	UT_ASSERT(searches == 0 && locked == 1 && events == 2);
	UT_ASSERT(actual_result == fetch_result && actual_reason == fetch_reason);
	UT_ASSERT(!result.projection.valid);
	UT_ASSERT(strstr(logs, "event=full.fetch.end") && !strstr(logs, "full_known=1"));
}
UT_TEST(dormant_keeps_target_disabled_exception)
{
	reset_fixture(true); fetch_result = CLUSTER_CR_BUILD_RETRYABLE;
	fetch_reason = CLUSTER_CR_BUILD_TARGET_DISABLED;
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_DORMANT);
	UT_ASSERT(searches == 0 && events == 2 && locked == 1);
}
UT_TEST(not_found_logs_real_retained_decision_formatter)
{
	reset_fixture(true); found = false;
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_NOT_FOUND);
	UT_ASSERT(strstr(logs, "event=full.search.end") && strstr(logs, "total=1 kept=1;off=3"));
	UT_ASSERT(strstr(logs, "done=1,visible=0"));
	UT_ASSERT(!result.projection.valid);
	UT_ASSERT(result.cr_private_projection);
}
UT_TEST(begin_log_error_restores_share_before_rethrow)
{
	reset_fixture(true); log_error_event = 1;
	UT_ASSERT(run_catching());
	UT_ASSERT(locked == 1 && fetches == 0 && searches == 0);
}
UT_TEST(fetch_error_restores_share_before_rethrow)
{
	reset_fixture(true); fetch_error = true;
	UT_ASSERT(run_catching());
	UT_ASSERT(locked == 1 && fetches == 1 && searches == 0);
	UT_ASSERT(!result.projection.valid);
}
UT_TEST(fetch_end_log_error_restores_share_before_rethrow)
{
	reset_fixture(true); log_error_event = 2;
	UT_ASSERT(run_catching());
	UT_ASSERT(locked == 1 && fetches == 1 && searches == 0);
	UT_ASSERT(!result.projection.valid);
}
UT_TEST(search_end_log_error_restores_share_before_rethrow)
{
	reset_fixture(true); log_error_event = 3;
	UT_ASSERT(run_catching());
	UT_ASSERT(locked == 1 && fetches == 1 && searches == 1);
	UT_ASSERT(!result.projection.valid);
	UT_ASSERT(result.cr_private_projection);
}
UT_TEST(invalid_page_is_unavailable_not_a_diagnostic_rejection)
{
	reset_fixture(true); ((PageHeader)before.data)->pd_lower = BLCKSZ;
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_FOUND);
	UT_ASSERT(strstr(logs, "before_known=0") && strstr(logs, "before_root={item_known=0}"));
}
UT_TEST(unused_item_is_not_dereferenced_as_tuple)
{
	reset_fixture(true); queried = 2; (void)run_cycle();
	UT_ASSERT(strstr(logs, "before_queried={item_known=1 lp=0"));
}
UT_TEST(invalid_tuple_bounds_remain_unknown)
{
	reset_fixture(true);
	ItemIdSetNormal(PageGetItemId(before.data, 3), BLCKSZ - 1, 100);
	(void)run_cycle();
	UT_ASSERT(strstr(logs, "before_queried={item_known=1 lp=1 tuple_known=0}"));
}
UT_TEST(invalid_itl_index_does_not_borrow_slot_zero)
{
	reset_fixture(true); tuple_at(before.data)->t_itl_slot_idx = 255; (void)run_cycle();
	UT_ASSERT(strstr(logs, "itl=255 slot_known=0"));
}
UT_TEST(invalid_full_shape_is_unknown_even_after_controlled_fetch_success)
{
	reset_fixture(true); ((PageHeader)full.data)->pd_special = 0; (void)run_cycle();
	UT_ASSERT(!strstr(logs, "full_known=1") && strstr(logs, "event=full.search.end"));
}
UT_TEST(critical_suppression_preserves_work_and_reports_gap_later)
{
	reset_fixture(true); CritSectionCount = 1;
	UT_ASSERT_EQ(run_cycle(), HEAP_HOT_R4_CYCLE_FOUND);
	UT_ASSERT(events == 0 && fetches == 1 && searches == 1);
	reset_fixture(true); (void)run_cycle();
	UT_ASSERT(strstr(logs, "trace_critical_skipped=3"));
}
UT_TEST(cycle_events_remain_before_unchanged_caller_revalidation)
{
	char *a, *b, *c;
	reset_fixture(true); (void)run_cycle();
	a = strstr(logs, "event=full.fetch.begin"); b = strstr(logs, "event=full.fetch.end");
	c = strstr(logs, "event=full.search.end");
	UT_ASSERT(a && b && c && a < b && b < c);
	UT_ASSERT(strstr(logs, "input_revalidated=0") && strstr(logs, "request_id_known=0"));
}

#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
static HeapHotR4UpdateTrace update_view;

static void
capture_update_fixture(bool cross_page)
{
	BufferTag next_tag;
	memset(&update_view, 0, sizeof(update_view));
	update_view.enabled = cluster_trace_normal_stop;
	update_view.raw_xid = 31337;
	update_view.canonical_xid = 31337;
	update_view.command_id = 8;
	update_view.requested_tid = root;
	update_view.old_tid = root;
	ItemPointerSet(&update_view.new_tid, cross_page ? 138 : 137, 3);
	update_view.hot = !cross_page;
	update_view.cross_page = cross_page;
	update_view.undo = (UBA){{8589935105, 196613}};
	next_tag = tag; next_tag.blockNum = cross_page ? 138 : 137;
	heap_hot_r4_update_capture(&update_view.before_old, &tag, before.data, 3);
	heap_hot_r4_update_capture(&update_view.before_new, &next_tag, before.data, InvalidOffsetNumber);
	heap_hot_r4_update_capture(&update_view.after_old, &tag, full.data, 3);
	heap_hot_r4_update_capture(&update_view.after_new, &next_tag, full.data, 3);
	update_view.wal_lsn = 654321;
}
#endif

UT_TEST(update_default_off_never_reads_view_or_clock)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(false); locked = 0;
	heap_hot_r4_update_trace((const HeapHotR4UpdateTrace *)1);
	UT_ASSERT(events == 0 && clock_reads == 0);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_capture_stores_owned_values_without_log_or_clock)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true);
	capture_update_fixture(false);
	UT_ASSERT(events == 0 && clock_reads == 0 && locked == 1);
	UT_ASSERT(update_view.before_old.page_known);
	UT_ASSERT(update_view.before_old.lsn == 123456 && update_view.after_old.lsn == 654321);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_emission_does_not_read_released_pages)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0;
	memset(&before, 0x7f, sizeof(before)); memset(&full, 0x7f, sizeof(full));
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(events == 1);
	UT_ASSERT(strstr(logs, "before_old_lsn=123456") != NULL);
	UT_ASSERT(strstr(logs, "after_old_lsn=654321") != NULL);
	UT_ASSERT(strstr(logs, "xmin=31337") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_records_actual_tags_and_same_page_hot_result)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "hot=1 cross_page=0") != NULL);
	UT_ASSERT(strstr(logs, "old_tag=1663/5/16388/0/137") != NULL);
	UT_ASSERT(strstr(logs, "new_tag=1663/5/16388/0/137") != NULL);
	UT_ASSERT(strstr(logs, "before_new_offset=0") != NULL);
	UT_ASSERT(strstr(logs, "after_new_offset=3") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_cross_page_target_is_not_borrowed_from_old_page)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(true); locked = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "hot=0 cross_page=1") != NULL);
	UT_ASSERT(strstr(logs, "old_tag=1663/5/16388/0/137") != NULL);
	UT_ASSERT(strstr(logs, "new_tag=1663/5/16388/0/138") != NULL);
	UT_ASSERT(strstr(logs, "new_tid=138/3") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_raw_cid_itl_and_uba_are_not_authority)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "raw_cid=7 combo=1") != NULL);
	UT_ASSERT(strstr(logs, "slot_xid=31337 wrap=9") != NULL);
	UT_ASSERT(strstr(logs, "undo_uba_raw=8589935105/196613") != NULL);
	UT_ASSERT(strstr(logs, "authoritative=0 transport_receipt=0 commit_receipt=0") != NULL);
	UT_ASSERT(strstr(logs, "full_xid_known=0") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_non_wal_operation_does_not_claim_insert_or_durability)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0;
	update_view.wal_lsn = InvalidXLogRecPtr;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "wal_inserted=0 wal_lsn=0") != NULL);
	UT_ASSERT(strstr(logs, "wal_flush_known=0") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_unused_and_redirect_items_are_not_dereferenced)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false);
	heap_hot_r4_update_capture(&update_view.before_old, &tag, before.data, 1);
	heap_hot_r4_update_capture(&update_view.after_old, &tag, before.data, 2);
	locked = 0; heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "before_old={item_known=1 lp=2 off=3 len=0 tuple_known=0}") != NULL);
	UT_ASSERT(strstr(logs, "after_old={item_known=1 lp=0 off=0 len=0 tuple_known=0}") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_invalid_page_and_tuple_bounds_are_unknown_only)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false);
	ItemIdSetNormal(PageGetItemId(before.data, 3), BLCKSZ - 1, 100);
	heap_hot_r4_update_capture(&update_view.before_old, &tag, before.data, 3);
	((PageHeader)full.data)->pd_special = 0;
	heap_hot_r4_update_capture(&update_view.after_old, &tag, full.data, 3);
	locked = 0; heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "before_old={item_known=1 lp=1 tuple_known=0}") != NULL);
	UT_ASSERT(strstr(logs, "after_old_page_known=0") != NULL);
	UT_ASSERT(strstr(logs, "after_old={item_known=0}") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_disabled_view_is_quiet_without_clock)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0; update_view.enabled = false;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(events == 0 && clock_reads == 0);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_critical_suppression_keeps_snapshot_and_discloses_gap)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0; CritSectionCount = 1;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(events == 0 && update_view.wal_lsn == 654321);
	CritSectionCount = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "trace_critical_skipped=1") != NULL);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_log_error_happens_without_content_lock_or_page_access)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	volatile bool caught = false;
	reset_fixture(true); capture_update_fixture(false); locked = 0; log_error_event = 1;
	PG_TRY(); { heap_hot_r4_update_trace(&update_view); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught && locked == 0 && update_view.wal_lsn == 654321);
#else
	UT_ASSERT(false);
#endif
}
UT_TEST(update_first_event_keeps_request_and_root_unknown)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	reset_fixture(true); capture_update_fixture(false); locked = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "request_id_known=0 root_identity_known=0") != NULL);
	UT_ASSERT(strstr(logs, "sequence_domain=R4_HEAP_UPDATE") != NULL);
	UT_ASSERT(strstr(logs, "emission_scope=after_original_release") != NULL);
#else
	UT_ASSERT(false);
#endif
}
/* Source binding only: not a full heap_update/real-WAL dynamic execution. */
UT_TEST(update_integration_captures_locked_then_emits_after_original_release)
{
	FILE *file = fopen(HEAPAM_SOURCE_PATH, "rb");
	char *source;
	long length;
	const char *update, *capture, *critical, *publish, *unlock, *release, *tuple_unlock, *emit, *stats;
	UT_ASSERT(file != NULL); if (!file) return;
	fseek(file, 0, SEEK_END); length = ftell(file); rewind(file);
	source = malloc((size_t)length + 1);
	UT_ASSERT(source != NULL); if (!source) { fclose(file); return; }
	UT_ASSERT(fread(source, 1, (size_t)length, file) == (size_t)length);
	fclose(file); source[length] = '\0';
	update = strstr(source, "\nheap_update(Relation ");
	capture = update ? strstr(update, "heap_hot_r4_update_capture(&cluster_update_trace.before_old") : NULL;
	critical = capture ? strstr(capture, "START_CRIT_SECTION();") : NULL;
	publish = critical ? strstr(critical, "heap_hot_r4_update_capture(&cluster_update_trace.after_old") : NULL;
	unlock = publish ? strstr(publish, "LockBuffer(buffer, BUFFER_LOCK_UNLOCK);") : NULL;
	release = unlock ? strstr(unlock, "ReleaseBuffer(buffer);") : NULL;
	tuple_unlock = release ? strstr(release, "UnlockTupleTuplock(") : NULL;
	emit = tuple_unlock ? strstr(tuple_unlock, "heap_hot_r4_update_trace(&cluster_update_trace)") : NULL;
	stats = emit ? strstr(emit, "pgstat_count_heap_update(") : NULL;
	UT_ASSERT(capture && critical && publish && unlock && release && tuple_unlock && emit && stats);
	free(source);
}

/* Provenance contract plus real formatter; not an executed heap_update. */
UT_TEST(update_input_cid_stays_distinct_from_combo_header_cid)
{
#ifdef HEAP_HOT_R4_UPDATE_TRACE_PRESENT
	FILE *file;
	char *source;
	long length;
	const char *update, *assignment;
	reset_fixture(true);
	HeapTupleHeaderSetCmin(tuple_at(before.data), 73);
	tuple_at(before.data)->t_infomask |= HEAP_COMBOCID;
	capture_update_fixture(false); locked = 0;
	heap_hot_r4_update_trace(&update_view);
	UT_ASSERT(strstr(logs, "input_command_id=8") != NULL);
	UT_ASSERT(strstr(logs, "raw_cid=73 combo=1") != NULL);
	file = fopen(HEAPAM_SOURCE_PATH, "rb");
	UT_ASSERT(file != NULL); if (!file) return;
	fseek(file, 0, SEEK_END); length = ftell(file); rewind(file);
	source = malloc((size_t)length + 1);
	UT_ASSERT(source != NULL); if (!source) { fclose(file); return; }
	UT_ASSERT(fread(source, 1, (size_t)length, file) == (size_t)length);
	fclose(file); source[length] = '\0';
	update = strstr(source, "\nheap_update(Relation ");
	assignment = update ? strstr(update, "cluster_update_trace.command_id = pgrac_entry_cid;") : NULL;
	UT_ASSERT(assignment != NULL);
	free(source);
#else
	UT_ASSERT(false);
#endif
}

int main(void)
{
	UT_PLAN(33);
	UT_RUN(default_off_has_no_log_or_clock_and_preserves_full);
	UT_RUN(actual_tag_root_and_queried_item_are_distinct);
	UT_RUN(owned_before_and_full_page_are_separate_observations);
	UT_RUN(raw_cid_combo_and_uba_remain_projections);
	UT_RUN(fetch_failure_keeps_refusal_and_hides_unqualified_scratch);
	UT_RUN(dormant_keeps_target_disabled_exception);
	UT_RUN(not_found_logs_real_retained_decision_formatter);
	UT_RUN(begin_log_error_restores_share_before_rethrow);
	UT_RUN(fetch_error_restores_share_before_rethrow);
	UT_RUN(fetch_end_log_error_restores_share_before_rethrow);
	UT_RUN(search_end_log_error_restores_share_before_rethrow);
	UT_RUN(invalid_page_is_unavailable_not_a_diagnostic_rejection);
	UT_RUN(unused_item_is_not_dereferenced_as_tuple);
	UT_RUN(invalid_tuple_bounds_remain_unknown);
	UT_RUN(invalid_itl_index_does_not_borrow_slot_zero);
	UT_RUN(invalid_full_shape_is_unknown_even_after_controlled_fetch_success);
	UT_RUN(critical_suppression_preserves_work_and_reports_gap_later);
	UT_RUN(cycle_events_remain_before_unchanged_caller_revalidation);
	UT_RUN(update_default_off_never_reads_view_or_clock);
	UT_RUN(update_capture_stores_owned_values_without_log_or_clock);
	UT_RUN(update_emission_does_not_read_released_pages);
	UT_RUN(update_records_actual_tags_and_same_page_hot_result);
	UT_RUN(update_cross_page_target_is_not_borrowed_from_old_page);
	UT_RUN(update_raw_cid_itl_and_uba_are_not_authority);
	UT_RUN(update_non_wal_operation_does_not_claim_insert_or_durability);
	UT_RUN(update_unused_and_redirect_items_are_not_dereferenced);
	UT_RUN(update_invalid_page_and_tuple_bounds_are_unknown_only);
	UT_RUN(update_disabled_view_is_quiet_without_clock);
	UT_RUN(update_critical_suppression_keeps_snapshot_and_discloses_gap);
	UT_RUN(update_log_error_happens_without_content_lock_or_page_access);
	UT_RUN(update_first_event_keeps_request_and_root_unknown);
	UT_RUN(update_integration_captures_locked_then_emits_after_original_release);
	UT_RUN(update_input_cid_stays_distinct_from_combo_header_cid);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
