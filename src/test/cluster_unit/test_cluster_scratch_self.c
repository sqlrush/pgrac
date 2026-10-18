/* Author: SqlRush <sqlrush@gmail.com>
 * Execute the production FULL evaluator, exact resolver and native CID getters.
 * Only origin service, backend publication and local xact membership are fixtures.
 */
#include "postgres.h"
#include "cluster/cluster_scn.h"
extern bool cluster_undo_horizon_read_admission_enforce(SCN read_scn);
int scratch_fixture_main(void);
#define main scratch_fixture_main
#define TransactionIdIsCurrentTransactionId self_unused_current
#define HeapTupleHeaderGetCmin self_unused_cmin
#define HeapTupleHeaderGetCmax self_unused_cmax
#define cluster_tx_resolve_exact self_unused_exact
#define uba_origin_node_id self_unused_origin
#include "test_cluster_r4_scratch_resolver.c"
#undef main
#undef TransactionIdIsCurrentTransactionId
#undef HeapTupleHeaderGetCmin
#undef HeapTupleHeaderGetCmax
#undef cluster_tx_resolve_exact
#undef uba_origin_node_id
#include "cluster/cluster_tt_local.h"
extern NodeId uba_origin_node_id(UBA address);
#include "../../backend/cluster/cluster_uba.c"

extern bool TransactionIdIsCurrentTransactionId(TransactionId xid);
extern CommandId HeapTupleHeaderGetCmin(HeapTupleHeader tuple);
extern CommandId HeapTupleHeaderGetCmax(HeapTupleHeader tuple);
extern CommandId self_real_cmin(HeapTupleHeader tuple);
extern CommandId self_real_cmax(HeapTupleHeader tuple);
extern ClusterTxOutcome cluster_tx_resolve_exact(const ClusterTxLocator *, ClusterTxResolveMode, ClusterTxResolution *, ClusterTxResolveReason *);

volatile uint32 CritSectionCount = 0;
static bool self_current;
static bool self_binding_present;
static int self_drift;
static int self_cid_calls;
static ClusterCanonicalTxnBinding self_binding;

bool
TransactionIdIsCurrentTransactionId(TransactionId xid)
{
	return self_current && xid == UT_RAW_XID;
}

#define HeapTupleHeaderGetCmin self_real_cmin
#define HeapTupleHeaderGetCmax self_real_cmax
#include "../../backend/utils/time/combocid.c"
#undef HeapTupleHeaderGetCmin
#undef HeapTupleHeaderGetCmax

CommandId
HeapTupleHeaderGetCmin(HeapTupleHeader tuple)
{
	self_cid_calls++;
	return self_real_cmin(tuple);
}
CommandId
HeapTupleHeaderGetCmax(HeapTupleHeader tuple)
{
	self_cid_calls++;
	return self_real_cmax(tuple);
}

bool
cluster_tt_local_get_published_binding(TransactionId xid, ClusterCanonicalTxnBinding *out)
{
	if (!self_binding_present || xid != self_binding.xid)
		return false;
	*out = self_binding;
	return true;
}

ClusterTxOutcome
cluster_tx_resolve_exact(const ClusterTxLocator *locator, ClusterTxResolveMode mode,
						ClusterTxResolution *out, ClusterTxResolveReason *reason)
{
	ClusterTxOutcome outcome = self_unused_exact(locator, mode, out, reason);
	out->top_xid = UT_RAW_XID;
	out->authority.origin_epoch = ut_current_epoch;
	out->authority.authority_scn = UT_READ_SCN;
	/* Origin boundary supplies its independently sampled canonical address. */
	out->local_canonical.segment_id = UT_SELF_NODE * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	out->local_canonical.xid = UT_RAW_XID;
	out->local_canonical.slot_offset = 3;
	out->local_canonical.wrap = 7;
	out->local_canonical_generation = 2;
	out->local_canonical_valid = true;
	if (self_drift == 1) out->locator_echo.uba.raw[0]++;
	if (self_drift == 2) out->locator_echo.tt_wrap++;
	if (self_drift == 3) out->top_xid++;
	if (self_drift == 4) ut_current_epoch++;
	if (self_drift == 5) out->authority.origin_epoch++;
	if (self_drift == 19) out->local_canonical_valid = false;
	return outcome;
}

static void
self_case(bool creator, bool combo, CommandId curcid, bool peer, int defect,
		  bool expected_visible, bool expected_error)
{
	HeapTupleData tuple = {0};
	SnapshotData snapshot = {0};
	ClusterR4HotScratchTestContext context = {0};
	HeapTupleHeader header;
	PageHeader page_header;
	ClusterItlSlotData *slot;
	PGAlignedBlock before;
	static ComboCidKeyData pair = {0, 2};
	volatile bool caught = false;
	volatile bool visible = false;

	ut_reset(CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn);
	ut_exit_fixture = ut_exit_exact_proof = ut_full_scratch_fixture = true;
	ut_full_scratch_scenario = 2;
	ut_current_epoch = UT_CLUSTER_EPOCH;
	ut_exit_ref = ut_exact_peer_ref();
	ut_exit_ref.origin_node_id = peer ? UT_PEER_NODE : UT_SELF_NODE;
	ut_exit_ref.cluster_epoch = ut_current_epoch;
	ut_memo_hit = false;
	ut_origin_verdict.kind = CLUSTER_UNDO_VERDICT_IN_PROGRESS;
	ut_origin_verdict.commit_scn = InvalidScn;
	cluster_crossnode_runtime_visibility = true;
	self_current = true;
	self_binding_present = defect != 6;
	self_cid_calls = 0;
	self_drift = defect <= 5 || defect == 19 ? defect : 0;
	memset(&self_binding, 0, sizeof(self_binding));
	self_binding.segment_id = UT_SELF_NODE * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	self_binding.segment_generation = 2;
	self_binding.xid = UT_RAW_XID;
	self_binding.slot_offset = 3;
	self_binding.slot_wrap = 7;
	self_binding.origin_instance = UT_SELF_NODE + 1;
	self_binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	self_binding.active_lsn = UT_ANCHOR_LSN;
	if (defect == 7) self_binding.slot_wrap++;
	if (defect == 8) self_binding.segment_generation = UINT32_MAX;
	if (defect == 9) self_binding.origin_instance++;
	if (defect == 10) self_binding.publish_state = CLUSTER_CANONICAL_TXN_RESERVED;
	if (defect == 11) self_binding.active_lsn = InvalidXLogRecPtr;
	if (defect == 16) self_binding.slot_offset++;
	if (defect == 17) self_binding.segment_id++;
	if (defect == 18) self_binding.segment_generation++;
	comboCids = &pair; usedComboCids = 1;
	memset(ut_visibility_page.data, 0, BLCKSZ);
	page_header = (PageHeader)ut_visibility_page.data;
	page_header->pd_flags = PD_HAS_ITL;
	page_header->pd_lower = SizeOfPageHeaderData;
	page_header->pd_upper = 1024;
	page_header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	page_header->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSN(ut_visibility_page.data, UT_ANCHOR_LSN);
	slot = ClusterPageGetItlSlots(ut_visibility_page.data);
	slot->flags = ITL_FLAG_ACTIVE;
	slot->xid = UT_RAW_XID;
	slot->wrap = 7;
	slot->undo_segment_head = uba_encode(
		(peer ? UT_PEER_NODE : UT_SELF_NODE) * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1, 4, 3, 64);
	header = (HeapTupleHeader)(ut_visibility_page.data + 1024);
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 0;
	header->t_infomask = creator || combo ? 0 : HEAP_XMIN_FROZEN;
	if (creator && !combo) header->t_infomask |= HEAP_XMAX_INVALID;
	if (combo) header->t_infomask |= HEAP_COMBOCID;
	if (!creator) header->t_infomask |= HEAP_UPDATED;
	HeapTupleHeaderSetXmin(header, UT_RAW_XID);
	HeapTupleHeaderSetXmax(header, creator && !combo ? InvalidTransactionId : UT_RAW_XID);
	header->t_choice.t_heap.t_field3.t_cid = combo ? 0 : 2;
	if (defect == 12) header->t_choice.t_heap.t_field3.t_cid = 999;
	if (defect == 13) comboCids = NULL;
	if (defect == 14) header->t_choice.t_heap.t_field3.t_cid = InvalidCommandId;
	if (defect == 15) header->t_infomask |= HEAP_MOVED_OFF;
	ItemPointerSet(&tuple.t_self, 0, 1); header->t_ctid = tuple.t_self;
	tuple.t_data = header; tuple.t_len = SizeofHeapTupleHeader; tuple.t_tableOid = FirstNormalObjectId;
	snapshot.snapshot_type = SNAPSHOT_MVCC; snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH; snapshot.read_scn = UT_READ_SCN; snapshot.curcid = curcid;
	context.scratch_page = ut_visibility_page.data; context.already_full = true;
	context.read_scn = snapshot.read_scn; context.logical_root = tuple.t_self; context.tag.blockNum = 0;
	memcpy(before.data, ut_visibility_page.data, BLCKSZ);
	ut_error_armed = true;
	PG_TRY(); { visible = HeapTupleSatisfiesMVCCScratch(&tuple, &snapshot, &context); }
	PG_CATCH(); { caught = true; }
	PG_END_TRY();
	ut_error_armed = false;
	UT_ASSERT_EQ(caught, expected_error);
	if (!caught) UT_ASSERT_EQ(visible, expected_visible);
	if (peer || expected_error) UT_ASSERT_EQ(self_cid_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, ut_visibility_page.data, BLCKSZ), 0);
	UT_ASSERT_EQ(ut_calls.clog, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT(!cluster_vis_resolve_in_flight());
	UT_ASSERT(PG_exception_stack == NULL);
	comboCids = NULL; usedComboCids = 0;
}

UT_TEST(test_existing_delete_current_command) { self_case(false,false,2,false,0,true,false); }
UT_TEST(test_existing_delete_next_command) { self_case(false,false,3,false,0,false,false); }
UT_TEST(test_update_old_version_next_command) { self_case(false,false,4,false,0,false,false); }
UT_TEST(test_creator_same_and_next_command) {
	self_case(true,false,2,false,0,false,false); self_case(true,false,3,false,0,true,false);
}
UT_TEST(test_combo_creator_deleter_command_boundaries) {
	self_case(true,true,0,false,0,false,false); self_case(true,true,1,false,0,true,false);
	self_case(true,true,2,false,0,true,false); self_case(true,true,3,false,0,false,false);
}
UT_TEST(test_foreign_equal_raw_xid_never_uses_local_cids) {
	self_case(false,false,3,true,0,true,false); self_case(true,true,3,true,0,false,false);
}
UT_TEST(test_exact_or_publication_drift_is_not_self) {
	for (int defect=1; defect<=11; defect++) self_case(false,false,3,false,defect,false,true);
	for (int defect=16; defect<=19; defect++) self_case(false,false,3,false,defect,false,true);
}
UT_TEST(test_unknown_combo_map_stays_fail_closed) { for (int defect=12; defect<=15; defect++) self_case(true,true,3,false,defect,false,true); }

int main(void) {
	UT_PLAN(8);
	UT_RUN(test_existing_delete_current_command); UT_RUN(test_existing_delete_next_command);
	UT_RUN(test_update_old_version_next_command); UT_RUN(test_creator_same_and_next_command);
	UT_RUN(test_combo_creator_deleter_command_boundaries); UT_RUN(test_foreign_equal_raw_xid_never_uses_local_cids);
	UT_RUN(test_exact_or_publication_drift_is_not_self); UT_RUN(test_unknown_combo_map_stays_fail_closed);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
