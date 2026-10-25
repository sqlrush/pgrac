/* Real TT publisher and diagnostic journal; only runtime/IO boundaries are supplied. */
int durable_fixture_main(int argc, char **argv);
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main
#include "cluster/cluster_native_itl_trace.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/shmem.h"

bool cluster_enabled = true, cluster_trace_tx_identity = true;
int MyProcPid = 800, NBuffers, NLocBuffer;
TimestampTz MyStartTimestamp = 901;
ProcessingMode Mode = NormalProcessing;
BufferDescPadded *BufferDescriptors;
char *BufferBlocks;
Block *LocalBufferBlockPointers;
static const ClusterShmemRegion *trace_region;
static bool binding_available;

void *ShmemInitStruct(const char *name, Size size, bool *found)
{ (void)name; *found = false; return calloc(1, size); }
void cluster_shmem_register_region(const ClusterShmemRegion *region)
{ trace_region = region; }

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	memset(out, 0, sizeof(*out));
	if (!binding_available || xid != 100) return false;
	out->segment_id = 1; out->segment_generation = 4; out->xid = 100;
	out->slot_offset = 7; out->slot_wrap = 5; out->origin_instance = 1;
	out->publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED; out->active_lsn = 0x2000;
	return true;
}

#include "../../backend/cluster/cluster_native_itl_trace.c"

static void setup_terminal(void)
{
	ClusterCtrcTxnKeyV1 key = {0};
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	cluster_tt_slot_durable_commit_unstage();
	reset_current_write_mock(); seed_current_exact_active(7, 100, 5);
	free(native_itl_trace); native_itl_trace = NULL;
	cluster_trace_tx_identity = true; binding_available = true;
	g_fake_proc.pgprocno = 4; g_fake_proc.backendId = 5;
	g_fake_proc.cluster_grd_generation = 6;
	cluster_native_itl_trace_shmem_register(); trace_region->init_fn();
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = 1; key.origin_node_id = 0;
	key.segment_id = 1; key.segment_generation = 4;
	key.slot_offset = 7; key.slot_wrap = 5; key.xid = 100;
	key.origin_boot_incarnation = 9; key.root_generation = 10;
	cluster_native_itl_trace_tt(CLNITL_TT_ACTIVE_PUBLISHED, &key, &resident->tt_slots[7], 0x2000);
}

static void terminal_commit_is_exported_only_after_flushed_write(void)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot successor; ClusterNativeItlTraceEvent event = {0};
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	volatile bool caught = false;
	setup_terminal();
	(void)cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), 1);
	UT_ASSERT_EQ(g_write_hdr_calls, 0);
	g_flush_ptr = 0x4fff;
	PG_TRY(); { cluster_tt_slot_durable_commit_apply(0x5000); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), 1);
	apply_after_flush(0x5000);
	UT_ASSERT(cluster_native_itl_trace_read(1, &event));
	UT_ASSERT_EQ(event.kind, CLNITL_TT_COMMITTED); UT_ASSERT(event.key_known);
	UT_ASSERT(event.native_binding_known); UT_ASSERT_EQ(event.active_sequence, 1);
	UT_ASSERT_EQ(event.bind_lsn, 0x2000); UT_ASSERT_EQ(event.terminal_lsn, 0x5000);
	UT_ASSERT_EQ(event.tt_slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT_EQ(memcmp(&event.tt_slot, &g_last_written_slot, sizeof(TTSlot)), 0);
	UT_ASSERT_EQ(memcmp(&event.tt_slot, &resident->tt_slots[7], sizeof(TTSlot)), 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 1); UT_ASSERT(!g_write_before_flush);
}

static void failed_native_write_exports_no_terminal_success(void)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot successor; volatile bool caught = false;
	setup_terminal();
	(void)cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	g_write_hdr_ok = false;
	PG_TRY(); { apply_after_flush(0x5000); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	UT_ASSERT(caught); UT_ASSERT_EQ(last_ereport_elevel, PANIC);
	UT_ASSERT_EQ(pg_atomic_read_u64(&native_itl_trace->next), 1);
	g_write_hdr_ok = true; cluster_tt_slot_durable_commit_unstage();
}

static void abort_exports_actual_flushed_exact_successor(void)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot successor; ClusterNativeItlTraceEvent event = {0}; XLogRecPtr lsn;
	UndoSegmentHeaderData *resident = (UndoSegmentHeaderData *)g_current_resident;
	setup_terminal(); g_require_abort_flush_before_write = true;
	lsn = cluster_tt_slot_durable_abort_exact(1, 4, 7, 100, 5, &admission, &successor);
	UT_ASSERT(cluster_native_itl_trace_read(1, &event)); UT_ASSERT(event.key_known);
	UT_ASSERT_EQ(event.kind, CLNITL_TT_ABORTED); UT_ASSERT_EQ(event.active_sequence, 1);
	UT_ASSERT_EQ(event.terminal_lsn, lsn); UT_ASSERT_EQ(lsn, g_abort_exact_lsn);
	UT_ASSERT_EQ(g_flushed_lsn, lsn); UT_ASSERT(g_abort_flush_seen);
	UT_ASSERT_EQ(event.tt_slot.status, TT_SLOT_ABORTED); UT_ASSERT(!SCN_VALID(event.tt_slot.commit_scn));
	UT_ASSERT_EQ(memcmp(&event.tt_slot, &resident->tt_slots[7], sizeof(TTSlot)), 0);
	UT_ASSERT_EQ(g_write_hdr_calls, 1);
}

static void unavailable_observation_binding_does_not_change_native_commit(void)
{
	ClusterSemanticAdmissionToken admission = target_modifier_token();
	TTSlot successor; ClusterNativeItlTraceEvent event = {0};
	setup_terminal(); binding_available = false;
	(void)cluster_tt_slot_durable_commit_stage(1, 4, 7, 100, 5, scn_encode(1, 42), &admission, &successor);
	apply_after_flush(0x5000);
	UT_ASSERT_EQ(g_write_hdr_calls, 1); UT_ASSERT_EQ(g_last_written_slot.status, TT_SLOT_COMMITTED);
	UT_ASSERT(cluster_native_itl_trace_read(1, &event));
	UT_ASSERT(!event.key_known); UT_ASSERT(!event.native_binding_known);
	UT_ASSERT_EQ(event.key.segment_id, 1); UT_ASSERT_EQ(event.key.segment_generation, 4);
	UT_ASSERT_EQ(event.key.slot_offset, 7); UT_ASSERT_EQ(event.key.slot_wrap, 5);
	UT_ASSERT_EQ(event.terminal_lsn, 0x5000);
}

int main(void)
{
	UT_PLAN(4);
	UT_RUN(terminal_commit_is_exported_only_after_flushed_write);
	UT_RUN(failed_native_write_exports_no_terminal_success);
	UT_RUN(abort_exports_actual_flushed_exact_successor);
	UT_RUN(unavailable_observation_binding_does_not_change_native_commit);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
