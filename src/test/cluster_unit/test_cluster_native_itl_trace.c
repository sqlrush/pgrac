/* Actual finite recorder; only backend environment is supplied by the test. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_native_itl_trace.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include <assert.h>
#undef printf
#undef fprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_trace_tx_identity;
int cluster_node_id = 2, MyProcPid = 700, NBuffers, NLocBuffer;
TimestampTz MyStartTimestamp = 900;
ProcessingMode Mode = NormalProcessing;
PGPROC *MyProc;
BufferDescPadded *BufferDescriptors;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
static PGPROC proc;
static const ClusterShmemRegion *registered;
void ExceptionalCondition(const char *c, const char *f, int l)
{ fprintf(stderr, "%s:%d: %s\n", f, l, c); abort(); }
void *ShmemInitStruct(const char *name, Size size, bool *found)
{ (void) name; *found = false; return calloc(1, size); }
void cluster_shmem_register_region(const ClusterShmemRegion *r) { registered = r; }

#include "../../backend/cluster/cluster_native_itl_trace.c"

static void reset(void)
{
	free(native_itl_trace); native_itl_trace = NULL;
	memset(&proc, 0, sizeof(proc)); MyProc = &proc;
	proc.pgprocno = 4; proc.backendId = 5; proc.cluster_grd_generation = 6;
	cluster_trace_tx_identity = true;
	cluster_native_itl_trace_shmem_register(); registered->init_fn();
}
static ClusterNativeItlTraceEvent sample(void)
{
	ClusterNativeItlTraceEvent row = {0};
	row.kind = CLNITL_TT_ACTIVE_PUBLISHED; row.key_known = true;
	row.key.xid = 42; row.key.slot_wrap = 7; row.key.segment_generation = 8;
	row.key.origin_boot_incarnation = 9; row.key.root_generation = 10;
	row.tt_slot.xid = 42; row.tt_slot.wrap = 7; row.bind_lsn = 123;
	return row;
}
static void disabled_is_inert(void)
{
	ClusterNativeItlTraceEvent row = sample(); uint64 seq = 999;
	reset(); cluster_trace_tx_identity = false;
	UT_ASSERT(!cluster_native_itl_trace_append(&row, &seq));
	UT_ASSERT_EQ(seq, 999); UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), 0);
}
static void immutable_owned_copy(void)
{
	ClusterNativeItlTraceEvent row = sample(), out = {0}; uint64 seq = 1;
	reset(); UT_ASSERT(cluster_native_itl_trace_append(&row, &seq));
	memset(&row, 0, sizeof(row));
	UT_ASSERT(cluster_native_itl_trace_read(seq - 1, &out));
	UT_ASSERT_EQ(out.sequence, 1); UT_ASSERT_EQ(out.key.slot_wrap, 7);
	UT_ASSERT_EQ(out.key.segment_generation, 8); UT_ASSERT_EQ(out.key.root_generation, 10);
	UT_ASSERT_EQ(out.bind_lsn, 123); UT_ASSERT_EQ(out.tt_slot.xid, 42);
	UT_ASSERT_EQ(out.pid, 700); UT_ASSERT_EQ(out.procno, 4);
	UT_ASSERT_EQ(out.backend_start, 900); UT_ASSERT_EQ(out.process_incarnation, 6);
}
static void prefix_is_complete(void)
{
	ClusterNativeItlTraceEvent row = sample(); ClusterNativeItlTracePrefix p;
	reset(); cluster_native_itl_trace_append(&row, NULL); cluster_native_itl_trace_prefix(&p);
	UT_ASSERT(p.complete); UT_ASSERT_EQ(p.before, 1); UT_ASSERT_EQ(p.after, 1);
	UT_ASSERT_EQ(p.published, 1); UT_ASSERT_EQ(p.dropped, 0);
}
static void unpublished_hole_is_incomplete(void)
{
	ClusterNativeItlTracePrefix p; reset();
	pg_atomic_write_u64(&native_itl_trace->next, 1);
	cluster_native_itl_trace_prefix(&p); UT_ASSERT(!p.complete); UT_ASSERT_EQ(p.published, 0);
}
static void full_never_overwrites(void)
{
	ClusterNativeItlTraceEvent row = sample(), before = {0}, after = {0}; ClusterNativeItlTracePrefix p;
	reset();
	for (unsigned i = 0; i < CLUSTER_NATIVE_ITL_TRACE_CAPACITY; i++)
		UT_ASSERT(cluster_native_itl_trace_append(&row, NULL));
	UT_ASSERT(cluster_native_itl_trace_read(0, &before)); row.key.xid++;
	UT_ASSERT(!cluster_native_itl_trace_append(&row, NULL));
	UT_ASSERT(cluster_native_itl_trace_read(0, &after));
	UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
	cluster_native_itl_trace_prefix(&p); UT_ASSERT(p.overflow); UT_ASSERT(!p.complete);
	UT_ASSERT_EQ(p.dropped, 1);
}
static void unavailable_is_incomplete(void)
{
	ClusterNativeItlTracePrefix p; ClusterNativeItlTraceEvent row = sample();
	reset(); free(native_itl_trace); native_itl_trace = NULL;
	UT_ASSERT(!cluster_native_itl_trace_append(&row, NULL));
	cluster_native_itl_trace_prefix(&p); UT_ASSERT(!p.available); UT_ASSERT(!p.complete);
}
static void missing_actor_is_not_invented(void)
{
	ClusterNativeItlTraceEvent row = sample(); ClusterNativeItlTracePrefix p;
	reset(); MyProc = NULL; UT_ASSERT(!cluster_native_itl_trace_append(&row, NULL));
	cluster_native_itl_trace_prefix(&p); UT_ASSERT(p.overflow); UT_ASSERT(!p.complete);
}
static void raw_page_and_slot_wrap_are_distinct(void)
{
	PGAlignedBlock page; ClusterNativeItlTraceEvent row = {0};
	BufferTag tag = {0}; ClusterItlSlotData *slots; PageHeader h = (PageHeader)page.data;
	memset(&page, 0, sizeof(page)); h->pd_lower = SizeOfPageHeaderData;
	h->pd_upper = h->pd_special = BLCKSZ - CLUSTER_ITL_ARRAY_SIZE - sizeof(ClusterItlPageHeader);
	h->pd_flags = PD_HAS_ITL; slots = ClusterPageGetItlSlots(page.data);
	slots[2].xid = 88; slots[2].wrap = 19; slots[2].undo_segment_head.raw[0] = 12345;
	tag.relNumber = 321; row.key.slot_wrap = 41;
	UT_ASSERT(cluster_native_itl_trace_capture_page(&tag, page.data, &row));
	UT_ASSERT_EQ(row.slots[2].wrap, 19); UT_ASSERT_EQ(row.key.slot_wrap, 41);
	UT_ASSERT_EQ(row.slots[2].undo_segment_head.raw[0], 12345);
	UT_ASSERT_EQ(row.tag.relNumber, 321); slots[2].wrap = 0;
	UT_ASSERT_EQ(row.slots[2].wrap, 19);
}
static void malformed_page_is_unknown(void)
{
	PGAlignedBlock page; BufferTag tag = {0}; ClusterNativeItlTraceEvent row = {0};
	memset(&page, 0, sizeof(page));
	UT_ASSERT(!cluster_native_itl_trace_capture_page(&tag, page.data, &row));
	((PageHeader)page.data)->pd_special = UINT16_MAX;
	UT_ASSERT(!cluster_native_itl_trace_capture_page(&tag, page.data, &row));
	UT_ASSERT(!row.page_known);
}
static void native_tt_hook_keeps_full_key(void)
{
	ClusterNativeItlTraceEvent row = sample(), out = {0}; reset();
	cluster_native_itl_trace_tt(CLNITL_TT_ADMITTED, &row.key, &row.tt_slot, 0);
	cluster_native_itl_trace_tt(CLNITL_TT_ACTIVE_PUBLISHED, &row.key, &row.tt_slot, 123);
	UT_ASSERT(cluster_native_itl_trace_read(1, &out));
	UT_ASSERT_EQ(out.kind, CLNITL_TT_ACTIVE_PUBLISHED);
	UT_ASSERT_EQ(memcmp(&out.key, &row.key, sizeof(row.key)), 0);
}
static void original_selection_result_is_observed(void)
{
	PGAlignedBlock page;
	BufferDescPadded desc[1] = {0};
	ClusterNativeItlTraceEvent row = {0};
	PageHeader h = (PageHeader)page.data;
	reset(); memset(&page, 0, sizeof(page));
	h->pd_lower = SizeOfPageHeaderData;
	h->pd_upper = h->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	h->pd_flags = PD_HAS_ITL;
	NBuffers = 1; BufferDescriptors = desc; BufferBlocks = page.data;
	desc[0].bufferdesc.tag.relNumber = 567;
	ClusterPageGetItlSlots(page.data)[3].wrap = 11;
	cluster_native_itl_trace_selected(1, 89, 3, true);
	UT_ASSERT(cluster_native_itl_trace_read(0, &row));
	UT_ASSERT_EQ(row.kind, CLNITL_SLOT_SELECTED); UT_ASSERT(row.selection_succeeded);
	UT_ASSERT_EQ(row.selected_slot, 3); UT_ASSERT_EQ(row.slots[3].wrap, 11);
	UT_ASSERT_EQ(row.tag.relNumber, 567); UT_ASSERT_EQ(row.caller_xid, 89);
	cluster_native_itl_trace_selected(1, 89, 3, false);
	UT_ASSERT(cluster_native_itl_trace_read(1, &row));
	UT_ASSERT(!row.selection_succeeded); UT_ASSERT_EQ(row.selected_slot, UINT8_MAX);
	BufferBlocks = NULL; BufferDescriptors = NULL; NBuffers = 0;
}
static void invalid_page_clears_previous_payload(void)
{
	PGAlignedBlock page; BufferTag tag = {0}; ClusterNativeItlTraceEvent row = {0};
	memset(&page, 0, sizeof(page)); row.page_known = true;
	row.slots[1].xid = 99; row.tag.relNumber = 88;
	UT_ASSERT(!cluster_native_itl_trace_capture_page(&tag, page.data, &row));
	UT_ASSERT(!row.page_known); UT_ASSERT_EQ(row.slots[1].xid, 0); UT_ASSERT_EQ(row.tag.relNumber, 0);
}
static void overflow_remains_incomplete_if_counter_wraps(void)
{
	ClusterNativeItlTraceEvent row = sample(); ClusterNativeItlTracePrefix p;
	reset(); pg_atomic_write_u64(&native_itl_trace->dropped, UINT64_MAX);
	pg_atomic_write_u64(&native_itl_trace->next, UINT64_MAX);
	UT_ASSERT(!cluster_native_itl_trace_append(&row, NULL));
	cluster_native_itl_trace_prefix(&p); UT_ASSERT(p.overflow); UT_ASSERT(!p.complete);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), UINT64_MAX);
}

static ClusterCanonicalTxnBinding terminal_binding(void)
{
	ClusterCanonicalTxnBinding binding = {0};
	binding.segment_id = 12; binding.segment_generation = 8;
	binding.xid = 42; binding.slot_offset = 5; binding.slot_wrap = 7;
	binding.origin_instance = 3; binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	binding.active_lsn = 123;
	return binding;
}

static void seed_terminal_active(void)
{
	ClusterNativeItlTraceEvent row = sample();
	row.key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	row.key.owner_instance = 3; row.key.origin_node_id = 2;
	row.key.segment_id = 12; row.key.slot_offset = 5;
	row.key.system_identifier = 11; row.key.root_id = 13;
	row.key.formation_epoch = 14; row.key.admission_record_generation = 15;
	row.key.root_descriptor_incarnation = 16;
	row.tt_slot.status = TT_SLOT_ACTIVE;
	UT_ASSERT(cluster_native_itl_trace_append(&row, NULL));
}

static TTSlot terminal_slot(bool commit)
{
	TTSlot slot = {0};
	slot.xid = 42; slot.wrap = 7;
	slot.status = commit ? TT_SLOT_COMMITTED : TT_SLOT_ABORTED;
	slot.commit_scn = commit ? scn_encode(2, 99) : InvalidScn;
	return slot;
}

static void terminal_commit_links_native_active_lsn_and_owned_full_key(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
	reset(); seed_terminal_active();
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, &binding);
	memset(&binding, 0, sizeof(binding)); memset(&slot, 0, sizeof(slot));
	UT_ASSERT(cluster_native_itl_trace_read(1, &row));
	UT_ASSERT(row.key_known); UT_ASSERT(row.native_binding_known);
	UT_ASSERT_EQ(row.kind, CLNITL_TT_COMMITTED); UT_ASSERT_EQ(row.active_sequence, 1);
	UT_ASSERT_EQ(row.key.root_generation, 10); UT_ASSERT_EQ(row.key.root_descriptor_incarnation, 16);
	UT_ASSERT_EQ(row.key.origin_boot_incarnation, 9); UT_ASSERT_EQ(row.native_binding.active_lsn, 123);
	UT_ASSERT_EQ(row.bind_lsn, 123); UT_ASSERT_EQ(row.terminal_lsn, 1000);
	UT_ASSERT_EQ(row.tt_slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(row.tt_slot.commit_scn, scn_encode(2, 99));
}

static void terminal_abort_keeps_actual_lsn_and_invalid_scn(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(false);
	reset(); seed_terminal_active();
	cluster_native_itl_trace_terminal(CLNITL_TT_ABORTED, 3, 12, 8, 5, &slot, 1001, &binding);
	UT_ASSERT(cluster_native_itl_trace_read(1, &row)); UT_ASSERT(row.key_known);
	UT_ASSERT_EQ(row.kind, CLNITL_TT_ABORTED); UT_ASSERT_EQ(row.terminal_lsn, 1001);
	UT_ASSERT_EQ(row.tt_slot.status, TT_SLOT_ABORTED); UT_ASSERT(!SCN_VALID(row.tt_slot.commit_scn));
}

static void terminal_missing_active_preserves_native_binding_but_unknown_key(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
	reset();
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, &binding);
	UT_ASSERT(cluster_native_itl_trace_read(0, &row)); UT_ASSERT(!row.key_known);
	UT_ASSERT(row.native_binding_known); UT_ASSERT_EQ(row.active_sequence, 0);
	UT_ASSERT_EQ(row.key.segment_id, 12); UT_ASSERT_EQ(row.key.slot_wrap, 7);
	UT_ASSERT_EQ(row.terminal_lsn, 1000);
}

static void terminal_missing_binding_never_reconstructs_from_xid(void)
{
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
	reset(); seed_terminal_active();
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, NULL);
	UT_ASSERT(cluster_native_itl_trace_read(1, &row));
	UT_ASSERT(!row.key_known); UT_ASSERT(!row.native_binding_known);
	UT_ASSERT_EQ(row.active_sequence, 0); UT_ASSERT_EQ(row.key.xid, 42);
	UT_ASSERT_EQ(row.tt_slot.status, TT_SLOT_COMMITTED);
}

static void terminal_mismatched_native_incarnations_never_join_by_xid(void)
{
	for (unsigned i = 0; i < 4; i++) {
		ClusterCanonicalTxnBinding binding = terminal_binding();
		ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
		reset(); seed_terminal_active();
		if (i == 0) binding.segment_generation++;
		if (i == 1) binding.slot_wrap++;
		if (i == 2) binding.active_lsn++;
		if (i == 3) binding.reserved16 = 1;
		cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, &binding);
		UT_ASSERT(cluster_native_itl_trace_read(1, &row)); UT_ASSERT(!row.key_known);
		UT_ASSERT_EQ(row.active_sequence, 0);
	}
}

static void terminal_actor_reuse_cannot_consume_old_active(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
	reset(); seed_terminal_active(); proc.cluster_grd_generation++;
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, &binding);
	UT_ASSERT(cluster_native_itl_trace_read(1, &row)); UT_ASSERT(!row.key_known);
	UT_ASSERT_EQ(row.process_incarnation, 7); UT_ASSERT_EQ(row.active_sequence, 0);
}

static void terminal_conflicting_active_keys_are_unknown(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}, other = {0}; TTSlot slot = terminal_slot(true);
	reset(); seed_terminal_active(); UT_ASSERT(cluster_native_itl_trace_read(0, &other));
	other.key.root_generation++; UT_ASSERT(cluster_native_itl_trace_append(&other, NULL));
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, &binding);
	UT_ASSERT(cluster_native_itl_trace_read(2, &row)); UT_ASSERT(!row.key_known);
	UT_ASSERT_EQ(row.active_sequence, 0);
}

static void terminal_disabled_is_inert_without_actor(void)
{
	TTSlot slot = terminal_slot(true);
	reset(); cluster_trace_tx_identity = false; MyProc = NULL;
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 1000, NULL);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->dropped), 0);
}

static void terminal_invalid_lsn_is_not_an_active_key_proof(void)
{
	ClusterCanonicalTxnBinding binding = terminal_binding();
	ClusterNativeItlTraceEvent row = {0}; TTSlot slot = terminal_slot(true);
	reset(); seed_terminal_active();
	cluster_native_itl_trace_terminal(CLNITL_TT_COMMITTED, 3, 12, 8, 5, &slot, 0, &binding);
	UT_ASSERT(cluster_native_itl_trace_read(1, &row)); UT_ASSERT(!row.key_known);
	UT_ASSERT_EQ(row.active_sequence, 0); UT_ASSERT_EQ(row.terminal_lsn, 0);
}

int main(void)
{
	UT_PLAN(22);
	UT_RUN(disabled_is_inert); UT_RUN(immutable_owned_copy); UT_RUN(prefix_is_complete);
	UT_RUN(unpublished_hole_is_incomplete); UT_RUN(full_never_overwrites);
	UT_RUN(unavailable_is_incomplete); UT_RUN(missing_actor_is_not_invented);
	UT_RUN(raw_page_and_slot_wrap_are_distinct); UT_RUN(malformed_page_is_unknown);
	UT_RUN(native_tt_hook_keeps_full_key);
	UT_RUN(original_selection_result_is_observed);
	UT_RUN(invalid_page_clears_previous_payload);
	UT_RUN(overflow_remains_incomplete_if_counter_wraps);
	UT_RUN(terminal_commit_links_native_active_lsn_and_owned_full_key);
	UT_RUN(terminal_abort_keeps_actual_lsn_and_invalid_scn);
	UT_RUN(terminal_missing_active_preserves_native_binding_but_unknown_key);
	UT_RUN(terminal_missing_binding_never_reconstructs_from_xid);
	UT_RUN(terminal_mismatched_native_incarnations_never_join_by_xid);
	UT_RUN(terminal_actor_reuse_cannot_consume_old_active);
	UT_RUN(terminal_conflicting_active_keys_are_unknown);
	UT_RUN(terminal_disabled_is_inert_without_actor);
	UT_RUN(terminal_invalid_lsn_is_not_an_active_key_proof);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
