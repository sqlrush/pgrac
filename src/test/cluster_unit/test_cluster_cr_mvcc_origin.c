/* Author: SqlRush <sqlrush@gmail.com> */
/*-------------------------------------------------------------------------
 * test_cluster_cr_mvcc_origin.c
 *
 * Execute the real legacy CR gate, MVCC caller and exact-ref resolver.
 * Reuse the resolver fixture's transport/status boundaries, not its CR stub.
 * The generated gate is unchanged production C; the constructor is trapped
 * because declined admission must never reach it. No live cluster is mocked.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
int origin_resolver_fixture_main(void);
#define main origin_resolver_fixture_main
#define cluster_cr_satisfies_mvcc origin_unused_cr_stub
#define uba_origin_node_id origin_unused_uba_stub
#define cluster_xid_provably_foreign origin_unused_foreign_stub
#define cluster_itl_cleanout_lazy origin_unused_cleanout_stub
#include "test_cluster_r4_scratch_resolver.c"
#undef main
#undef cluster_cr_satisfies_mvcc
#undef uba_origin_node_id
#undef cluster_xid_provably_foreign
#undef cluster_itl_cleanout_lazy

#include "cluster/cluster_cr_tuple.h"
#include "cluster/cluster_cr_apply.h"
extern NodeId uba_origin_node_id(UBA address);
extern bool cluster_xid_provably_foreign(TransactionId xid);
extern bool cluster_itl_cleanout_lazy(Buffer buffer, uint8 index, TransactionId xid, SCN scn);
extern ClusterCrVerdict cluster_cr_satisfies_mvcc(HeapTuple tuple, Snapshot snapshot, Buffer buffer,
												  bool *visible);
#include "../../backend/cluster/cluster_uba.c"
#include "../../backend/cluster/cluster_itl_cleanout.c"

static int origin_cleanout_locks;
static int origin_cleanout_unlocks;

/* Only locking is a fixture; the xid recheck and page mutation are real. */
bool
ConditionalLockBuffer(Buffer buffer)
{
	UT_ASSERT_EQ(buffer, 1);
	origin_cleanout_locks++;
	return true;
}

void
LockBuffer(Buffer buffer, int mode)
{
	UT_ASSERT_EQ(buffer, 1);
	UT_ASSERT_EQ(mode, BUFFER_LOCK_UNLOCK);
	origin_cleanout_unlocks++;
}

bool cluster_cr_mvcc_gate = true;
bool cluster_cr_tuple_level_fastpath = false;

/* Same explicit origin-service fixture as the real resolver tests. */
bool
cluster_xid_provably_foreign(TransactionId xid)
{
	return xid == UT_RAW_XID;
}

bool
cluster_cr_tuple_eligible(Buffer buf pg_attribute_unused(), HeapTuple tuple pg_attribute_unused(),
						  Snapshot snapshot pg_attribute_unused(),
						  const ClusterItlSlotData *slot pg_attribute_unused(),
						  int count pg_attribute_unused(),
						  ClusterCRTupleOutcome *reason pg_attribute_unused())
{
	UT_ASSERT(false);
	return false;
}

ClusterCrVerdict
cluster_cr_tuple_verdict(Buffer buf pg_attribute_unused(), HeapTuple tuple pg_attribute_unused(),
						 Snapshot snapshot pg_attribute_unused(),
						 bool *visible pg_attribute_unused(),
						 ClusterCRTupleOutcome *reason pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_CR_FAILCLOSED;
}

void
cluster_cr_tuple_stat_bump(ClusterCRTupleOutcome reason pg_attribute_unused())
{
	UT_ASSERT(false);
}

int
cluster_cr_collect_candidate_chains(const ClusterItlSlotData *slots pg_attribute_unused(),
									SCN read pg_attribute_unused(),
									ClusterCRCandidateChain *chains pg_attribute_unused(),
									int capacity pg_attribute_unused())
{
	UT_ASSERT(false);
	return 0;
}

const char *
cluster_cr_lookup_or_construct(Buffer buffer pg_attribute_unused(), SCN read pg_attribute_unused())
{
	UT_ASSERT(false);
	return NULL;
}

static ClusterCrVerdict
cluster_cr_verdict_on_image(const char *page pg_attribute_unused(),
							OffsetNumber off pg_attribute_unused(),
							const ClusterItlSlotData *slot pg_attribute_unused(),
							Snapshot snapshot pg_attribute_unused(),
							bool materialized pg_attribute_unused(),
							int32 origin pg_attribute_unused(), bool *visible pg_attribute_unused())
{
	UT_ASSERT(false);
	return CLUSTER_CR_FAILCLOSED;
}

#include "test_cluster_cr_mvcc_gate.inc"

static void
origin_case(uint8 state, bool proven, bool committed, bool post_read)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	Page page = ut_visibility_page.data;
	PageHeader ph = (PageHeader)page;
	HeapTupleHeader header;
	ClusterItlSlotData *slot;
	PGAlignedBlock before;
	volatile bool caught = false;
	volatile bool visible = false;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	origin_cleanout_locks = origin_cleanout_unlocks = 0;
	ut_exit_fixture = true;
	ut_exit_ref = ut_exact_peer_ref();
	ut_exit_ref.origin_node_id = UT_SELF_NODE;
	ut_exit_ref.local_xid += 4;
	ut_recycled_proven = proven;
	ut_recycled_committed = committed;
	ut_memo_hit = false;
	cluster_crossnode_runtime_visibility = true;
	memset(page, 0, BLCKSZ);
	ph->pd_flags = PD_HAS_ITL;
	ph->pd_lower = SizeOfPageHeaderData;
	ph->pd_special = ph->pd_upper = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	ph->pd_pagesize_version = BLCKSZ | PG_PAGE_LAYOUT_VERSION;
	PageSetLSN(page, UT_ANCHOR_LSN);
	header = (HeapTupleHeader)(page + 1024);
	header->t_hoff = SizeofHeapTupleHeader;
	header->t_itl_slot_idx = 0;
	header->t_infomask = HEAP_XMAX_INVALID;
	HeapTupleHeaderSetXmin(header, UT_RAW_XID);
	HeapTupleHeaderSetXmax(header, InvalidTransactionId);
	ItemPointerSet(&tuple.t_self, 0, 1);
	header->t_ctid = tuple.t_self;
	tuple.t_data = header;
	tuple.t_len = SizeofHeapTupleHeader;
	tuple.t_tableOid = FirstNormalObjectId;
	snapshot.snapshot_type = SNAPSHOT_MVCC;
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_epoch = UT_CLUSTER_EPOCH;
	snapshot.read_scn = post_read ? UT_COMMIT_SCN - 1 : UT_READ_SCN;
	ph->pd_block_scn = snapshot.read_scn + 2;
	slot = ClusterPageGetItlSlots(page);
	slot->xid = ut_exit_ref.local_xid;
	slot->flags = state;
	slot->write_scn = snapshot.read_scn + 1;
	slot->undo_segment_head = uba_encode(UT_SELF_NODE * 256 + 1, 1, 1, 1);
	memcpy(before.data, page, BLCKSZ);
	ut_error_armed = true;
	if (sigsetjmp(ut_error_jump, 0) == 0)
		visible = cluster_heap_test_satisfies_mvcc(&tuple, &snapshot, 1);
	else
		caught = true;
	ut_error_armed = false;
	UT_ASSERT_EQ(caught, !proven);
	UT_ASSERT_EQ(visible, proven && committed && !post_read);
	if (caught)
		UT_ASSERT_EQ(ut_error_code, ERRCODE_CLUSTER_TT_STATUS_UNKNOWN);
	UT_ASSERT(ut_recycled_asks > 0);
	UT_ASSERT_EQ(ut_native_calls, 0);
	UT_ASSERT_EQ(ut_hint_mutations, 0);
	UT_ASSERT_EQ(origin_cleanout_locks, origin_cleanout_unlocks);
	if (state == ITL_FLAG_ACTIVE && proven && committed && !post_read)
		UT_ASSERT_EQ(origin_cleanout_locks, 1);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
	ut_exit_fixture = false;
}

UT_TEST(reused_lock_carrier_uses_real_mvcc_origin_authority)
{
	uint8 state;
	for (state = ITL_FLAG_LOCK_ONLY_ACTIVE; state <= ITL_FLAG_LOCK_ONLY_ABORTED; state++)
		origin_case(state, true, true, false);
}

UT_TEST(reused_data_carrier_uses_real_mvcc_origin_authority)
{
	uint8 state;
	for (state = ITL_FLAG_ACTIVE; state <= ITL_FLAG_NEEDS_CLEANOUT; state++)
		origin_case(state, true, true, false);
}

UT_TEST(unproved_origin_still_errors_not_silent_invisible)
{
	origin_case(ITL_FLAG_LOCK_ONLY_ACTIVE, false, false, false);
}

UT_TEST(aborted_creator_is_invisible_via_origin_not_local_clog)
{
	origin_case(ITL_FLAG_LOCK_ONLY_ACTIVE, true, false, false);
}

UT_TEST(post_read_creator_is_invisible_via_origin_scn)
{
	origin_case(ITL_FLAG_LOCK_ONLY_ACTIVE, true, true, true);
}

UT_TEST(exact_data_creator_keeps_original_native_abort_guard)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	Page page = ut_visibility_page.data;
	ClusterItlSlotData *slot;
	bool visible = true;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_exit_fixture = true;
	memset(page, 0, BLCKSZ);
	((PageHeader)page)->pd_flags = PD_HAS_ITL;
	((PageHeader)page)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	((PageHeader)page)->pd_block_scn = UT_READ_SCN + 2;
	tuple.t_data = (HeapTupleHeader)(page + 1024);
	tuple.t_data->t_itl_slot_idx = 0;
	HeapTupleHeaderSetXmin(tuple.t_data, 4320003);
	slot = ClusterPageGetItlSlots(page);
	slot->xid = 4320003;
	slot->flags = ITL_FLAG_ACTIVE;
	slot->write_scn = UT_READ_SCN + 1;
	slot->undo_segment_head = uba_encode(UT_SELF_NODE * 256 + 1, 1, 1, 1);
	snapshot.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_scn = UT_READ_SCN;
	UT_ASSERT_EQ(cluster_cr_satisfies_mvcc(&tuple, &snapshot, 1, &visible), CLUSTER_CR_DECIDED);
	UT_ASSERT(!visible);
	UT_ASSERT(ut_native_calls > 0);
	ut_exit_fixture = false;
}

static void
declined_gate_case(uint8 state, TransactionId xmin, TransactionId carrier, bool local_snapshot,
				   bool gate_off)
{
	HeapTupleData tuple = { 0 };
	SnapshotData snapshot = { 0 };
	Page page = ut_visibility_page.data;
	ClusterItlSlotData *slot;
	PGAlignedBlock before;
	bool visible = true;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	ut_exit_fixture = true;
	memset(page, 0, BLCKSZ);
	((PageHeader)page)->pd_flags = PD_HAS_ITL;
	((PageHeader)page)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	((PageHeader)page)->pd_block_scn = UT_READ_SCN + 2;
	tuple.t_data = (HeapTupleHeader)(page + 1024);
	tuple.t_data->t_itl_slot_idx = 0;
	HeapTupleHeaderSetXmin(tuple.t_data, xmin);
	slot = ClusterPageGetItlSlots(page);
	slot->xid = carrier;
	slot->flags = state;
	slot->write_scn = UT_READ_SCN + 1;
	slot->undo_segment_head = uba_encode(UT_SELF_NODE * 256 + 1, 1, 1, 1);
	snapshot.cluster_source = local_snapshot ? SNAPSHOT_SOURCE_LOCAL : SNAPSHOT_SOURCE_CLUSTER;
	snapshot.read_scn = UT_READ_SCN;
	memcpy(before.data, page, BLCKSZ);
	cluster_cr_mvcc_gate = !gate_off;
	UT_ASSERT_EQ(cluster_cr_satisfies_mvcc(&tuple, &snapshot, 1, &visible),
				 CLUSTER_CR_NOT_APPLICABLE);
	cluster_cr_mvcc_gate = true;
	UT_ASSERT(visible); /* declined gate must not write a verdict */
	UT_ASSERT_EQ(ut_native_calls, 0);
	UT_ASSERT_EQ(memcmp(before.data, page, BLCKSZ), 0);
	ut_exit_fixture = false;
}

UT_TEST(complete_flag_domain_and_same_origin_replacement_decline)
{
	unsigned state;
	for (state = 0; state <= UINT8_MAX; state++) {
		/* Same origin is not enough; only exact creator DATA is admitted. */
		declined_gate_case(state, 4320003, 4320019, false, false);
		if (state < ITL_FLAG_ACTIVE || state > ITL_FLAG_NEEDS_CLEANOUT)
			declined_gate_case(state, 4320003, 4320003, false, false);
	}
}

UT_TEST(nonnormal_creator_does_not_authorize_native_status)
{
	TransactionId xmin;
	for (xmin = InvalidTransactionId; xmin < FirstNormalTransactionId; xmin++)
		declined_gate_case(ITL_FLAG_ACTIVE, xmin, xmin, false, false);
}

UT_TEST(original_master_switch_and_local_snapshot_stay_dormant)
{
	declined_gate_case(ITL_FLAG_ACTIVE, 4320003, 4320003, true, false);
	declined_gate_case(ITL_FLAG_ACTIVE, 4320003, 4320003, false, true);
}

UT_TEST(real_cleanout_positive_control_does_mutate_exact_creator)
{
	Page page = ut_visibility_page.data;
	ClusterItlSlotData *slot;

	ut_reset(CLUSTER_TT_STATUS_UNKNOWN, InvalidScn);
	origin_cleanout_locks = origin_cleanout_unlocks = 0;
	memset(page, 0, BLCKSZ);
	((PageHeader)page)->pd_flags = PD_HAS_ITL;
	((PageHeader)page)->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	slot = ClusterPageGetItlSlots(page);
	slot->flags = ITL_FLAG_ACTIVE;
	slot->xid = UT_RAW_XID;
	UT_ASSERT(cluster_itl_cleanout_lazy(1, 0, UT_RAW_XID, UT_COMMIT_SCN));
	UT_ASSERT_EQ(slot->flags, ITL_FLAG_COMMITTED);
	UT_ASSERT_EQ(slot->commit_scn, UT_COMMIT_SCN);
	UT_ASSERT_EQ(origin_cleanout_locks, 1);
	UT_ASSERT_EQ(origin_cleanout_unlocks, 1);
	UT_ASSERT_EQ(ut_hint_mutations, 1);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(reused_lock_carrier_uses_real_mvcc_origin_authority);
	UT_RUN(reused_data_carrier_uses_real_mvcc_origin_authority);
	UT_RUN(unproved_origin_still_errors_not_silent_invisible);
	UT_RUN(aborted_creator_is_invisible_via_origin_not_local_clog);
	UT_RUN(post_read_creator_is_invisible_via_origin_scn);
	UT_RUN(exact_data_creator_keeps_original_native_abort_guard);
	UT_RUN(complete_flag_domain_and_same_origin_replacement_decline);
	UT_RUN(nonnormal_creator_does_not_authorize_native_status);
	UT_RUN(original_master_switch_and_local_snapshot_stay_dormant);
	UT_RUN(real_cleanout_positive_control_does_mutate_exact_creator);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
