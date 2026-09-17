/*-------------------------------------------------------------------------
 * test_cluster_ctrc_itl_reuse.c
 *    Actual ITL cleaner and shared receipt discharge across slot reuse.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Only buffer/R4/TT/WAL services are boundary fixtures. The complete cleaner,
 * page-version guard, slot mutator and shared receipt state engine are real C.
 * This is not a replay of a captured live receipt or a disk durability test.
 *-------------------------------------------------------------------------
 */
int ctrc_cleaner_original_main(void);
#define main ctrc_cleaner_original_main
#include "test_cluster_ctrc_cleaner.c"
#undef main
#include "cluster/cluster_uba.h"
#include "cluster/cluster_cr.h"
#include "cluster/cluster_undo_retention.h"
#include "access/clog.h"
#include "access/transam.h"
#include "../../backend/cluster/cluster_undo_horizon.c"
#include "../../backend/cluster/cluster_uba.c"

int NLocBuffer = 0;

int
scn_time_cmp(SCN a, SCN b)
{
	return scn_local(a) < scn_local(b) ? -1 : scn_local(a) > scn_local(b) ? 1 : 0;
}

static PGAlignedBlock reuse_page, reuse_image;
static ClusterCtrcReceiptHandle reuse_handle;
static ClusterCtrcReceipt reuse_receipt;
static ClusterCtrcParticipantEntry reuse_participant;
static ClusterSfDepVec reuse_dependencies;
static unsigned reuse_pins, reuse_xlocks, reuse_locks, reuse_enters, reuse_leaves;
static unsigned reuse_wal_starts, reuse_wal_finishes, reuse_wal_aborts;
static unsigned reuse_rechecks, reuse_fail_recheck, reuse_fail_lock;
static bool reuse_terminal_ok, reuse_tag_ok, reuse_exists;
static bool reuse_dependency_drift;
static void (*reuse_between_rounds)(void);
static XLogRecPtr reuse_peer_flush;
static unsigned reuse_retained_notes;
static const char *reuse_retained_stage;
static SCN reuse_local_floor;
static ClusterUndoHorizonReportView reuse_peer_view;
static bool reuse_members_valid, reuse_raw_disabled, reuse_epoch_fenced;
static int reuse_origin, reuse_view_count;
static uint64 reuse_members_epoch;
static unsigned reuse_floor_samples;
static ClusterCtrcTerminalStatus reuse_terminal_status;
static bool reuse_native_allowed;
static bool reuse_native_throw;
static bool reuse_slru_held;
static bool reuse_covered;
static uint64 reuse_ungated;
static unsigned reuse_native_depth, reuse_truncation_depth, reuse_native_reads;
static unsigned reuse_guarded_cas;
static XidStatus reuse_native_status;
static LWLockPadded reuse_native_locks[NUM_INDIVIDUAL_LWLOCKS];
LWLockPadded *MainLWLockArray = reuse_native_locks;
static VariableCacheData reuse_variable_cache;
VariableCache ShmemVariableCache = &reuse_variable_cache;
/* Run the identical history/floor matrix for DATA and LOCK publications. */
static uint8 reuse_history_class = 1;
bool cluster_undo_retention_horizon_enabled = true;
int cluster_lmon_main_loop_interval = 2000;

static bool
reuse_external_lock(LWLock *lock, LWLockMode mode, bool acquire)
{
	if (lock == XactTruncationLock) {
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(reuse_xlocks, 0);
		UT_ASSERT_EQ(reuse_native_depth, 1);
		UT_ASSERT_EQ(mode, LW_SHARED);
		UT_ASSERT_EQ(reuse_truncation_depth, acquire ? 0 : 1);
		reuse_truncation_depth = acquire ? 1 : 0;
		return true;
	}
	if (acquire && lock == &CtrcShared->receipt_lock && reuse_native_depth != 0) {
		UT_ASSERT_EQ(reuse_truncation_depth, 1);
		UT_ASSERT_EQ(reuse_pins, 0);
		reuse_guarded_cas++;
	}
	return false;
}

void
cluster_cr_native_prehistory_reader_lock(void)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(reuse_xlocks, 0);
	UT_ASSERT_EQ(reuse_native_depth, 0);
	reuse_native_depth = 1;
}

void
cluster_cr_native_prehistory_reader_unlock(void)
{
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(reuse_truncation_depth, 0);
	UT_ASSERT_EQ(reuse_native_depth, 1);
	reuse_native_depth = 0;
}

bool
cluster_cr_native_origin_epoch0_provable(TransactionId xid)
{
	UT_ASSERT_EQ(reuse_native_depth, 1);
	return reuse_native_allowed && xid == reuse_receipt.key.xid && reuse_origin == cluster_node_id;
}

uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	return reuse_covered ? 1 : 0;
}

uint64
cluster_tt_slot_retention_off_recycle_count(void)
{
	return reuse_ungated;
}

XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	UT_ASSERT_EQ(xid, reuse_receipt.key.xid);
	UT_ASSERT_EQ(reuse_native_depth, 1);
	UT_ASSERT_EQ(reuse_truncation_depth, 1);
	UT_ASSERT_EQ(held_count, 0);
	reuse_native_reads++;
	if (reuse_native_throw) {
		reuse_slru_held = true;
		pg_re_throw();
	}
	*lsn = InvalidXLogRecPtr;
	return reuse_native_status;
}

void
LWLockReleaseAll(void)
{
	while (held_count != 0)
		LWLockRelease((LWLock *)held_locks[held_count - 1]);
	reuse_slru_held = false;
	if (reuse_truncation_depth != 0)
		LWLockRelease(XactTruncationLock);
	if (reuse_native_depth != 0)
		cluster_cr_native_prehistory_reader_unlock();
}

SCN
cluster_undo_retention_horizon(void)
{
	Assert(reuse_xlocks == 0);
	reuse_floor_samples++;
	return reuse_local_floor;
}

int
cluster_undo_horizon_sample_views(ClusterUndoHorizonReportView *views, int maxviews)
{
	Assert(reuse_xlocks == 0 && maxviews == CLUSTER_MAX_NODES);
	memset(views, 0, sizeof(*views) * maxviews);
	views[1] = reuse_peer_view;
	return reuse_view_count;
}

bool
cluster_undo_horizon_required_members(uint8 *required, uint64 *epoch)
{
	Assert(reuse_xlocks == 0);
	memset(required, 0, CLUSTER_RECONFIG_DEAD_BITMAP_BYTES);
	required[0] = 3;
	*epoch = reuse_members_epoch;
	return reuse_members_valid;
}

bool
cluster_undo_horizon_epoch_fence_tripped(uint64 epoch)
{
	return reuse_epoch_fenced || epoch != test_cluster_epoch;
}

uint64
cluster_epoch_get_current(void)
{
	return test_cluster_epoch;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64CONST(10000000);
}

bool
cluster_cr_native_prehistory_disabled(void)
{
	return reuse_raw_disabled;
}

int
cluster_xid_origin_slot(TransactionId xid)
{
	Assert(xid == reuse_receipt.key.xid);
	return reuse_origin;
}

static void
reuse_note_itl_retained(const ClusterCtrcReceipt *receipt, const char *stage,
						const ClusterItlSlotData *slot, XLogRecPtr page_lsn, SCN page_scn,
						int page_origin, ClusterCtrcTerminalStatus terminal_status, SCN commit_scn)
{
	Assert(reuse_pins == 0 && reuse_xlocks == 0);
	Assert(receipt == &reuse_receipt);
	(void)slot;
	(void)page_lsn;
	(void)page_scn;
	(void)page_origin;
	(void)terminal_status;
	(void)commit_scn;
	reuse_retained_notes++;
	reuse_retained_stage = stage;
}

static void
reuse_assert_unlocked(unsigned call pg_attribute_unused())
{
	if (reuse_xlocks != 0)
		abort();
}

static XLogRecPtr
reuse_peer_durable(int origin)
{
	if (origin <= 0 || reuse_xlocks != 0)
		abort();
	return reuse_peer_flush;
}

static bool
reuse_terminal(const ClusterCtrcTxnKeyV1 *key, ClusterCtrcTerminalStatus *status, SCN *scn)
{
	Assert(reuse_pins == 0 && reuse_xlocks == 0);
	Assert(memcmp(key, &reuse_receipt.key, sizeof(*key)) == 0);
	*status = reuse_terminal_status;
	*scn = reuse_terminal_status == CTRC_TERMINAL_COMMITTED ? 900 : InvalidScn;
	return reuse_terminal_ok;
}

ClusterSemanticAdmissionResult
cluster_semantic_activation_enter_r4_terminal_census(ClusterSemanticAdmissionToken *token)
{
	MemSet(token, 0, sizeof(*token));
	token->record_generation = 29;
	token->formation_epoch = 23;
	token->entered = true;
	reuse_enters++;
	return CLUSTER_SEMANTIC_ADMISSION_OK;
}

bool
cluster_semantic_activation_recheck_r4_terminal_census(const ClusterSemanticAdmissionToken *token)
{
	Assert(token->entered && reuse_xlocks == 1);
	reuse_rechecks++;
	return reuse_rechecks != reuse_fail_recheck;
}

void
cluster_semantic_activation_leave(ClusterSemanticAdmissionToken *token)
{
	Assert(token->entered && reuse_xlocks == 0 && reuse_pins == 0);
	token->entered = false;
	reuse_leaves++;
}

static SMgrRelation
reuse_smgr_open(RelFileLocator locator, BackendId backend)
{
	Assert(locator.relNumber == reuse_receipt.target.rel_number && backend == InvalidBackendId);
	return (SMgrRelation)&reuse_handle;
}

static Buffer
reuse_read(RelFileLocator locator pg_attribute_unused(), ForkNumber forknum, BlockNumber block,
		   ReadBufferMode mode, BufferAccessStrategy strategy, bool permanent)
{
	Assert(forknum == MAIN_FORKNUM && block == reuse_receipt.target.block_number);
	Assert(mode == RBM_NORMAL && strategy == NULL && permanent && reuse_pins == 0);
	reuse_pins++;
	return 1;
}

static bool
reuse_lock(Buffer buffer)
{
	Assert(buffer == 1 && reuse_pins == 1 && reuse_xlocks == 0);
	reuse_locks++;
	if (reuse_locks == reuse_fail_lock)
		return false;
	if (reuse_locks == 2 && reuse_between_rounds != NULL)
		reuse_between_rounds();
	reuse_xlocks++;
	return true;
}

static void
reuse_unlock(Buffer buffer, int mode)
{
	Assert(buffer == 1 && mode == BUFFER_LOCK_UNLOCK && reuse_xlocks == 1);
	reuse_xlocks--;
}

static void
reuse_release(Buffer buffer)
{
	Assert(buffer == 1 && reuse_xlocks == 0 && reuse_pins == 1);
	reuse_pins--;
}

static void
reuse_unlock_release(Buffer buffer)
{
	reuse_unlock(buffer, BUFFER_LOCK_UNLOCK);
	reuse_release(buffer);
}

static void
reuse_get_tag(Buffer buffer, RelFileLocator *locator, ForkNumber *forknum, BlockNumber *block)
{
	Assert(buffer == 1 && reuse_xlocks == 1);
	locator->spcOid = reuse_receipt.target.spc_oid;
	locator->dbOid = reuse_receipt.target.db_oid;
	locator->relNumber = reuse_receipt.target.rel_number + (reuse_tag_ok ? 0 : 1);
	*forknum = MAIN_FORKNUM;
	*block = reuse_receipt.target.block_number;
}

bool
cluster_sf_dep_vec_for_ship(Buffer buffer, ClusterSfDepVec *out)
{
	Assert(buffer == 1 && reuse_xlocks == 1);
	*out = reuse_dependencies;
	if (reuse_dependency_drift && reuse_locks == 2)
		out->required[1]++;
	return true;
}

static GenericXLogState *
reuse_xlog_start(bool logged)
{
	Assert(logged && reuse_xlocks == 1);
	reuse_wal_starts++;
	memcpy(&reuse_image, &reuse_page, sizeof(reuse_page));
	return (GenericXLogState *)&reuse_image;
}

static Page
reuse_xlog_register(GenericXLogState *state, Buffer buffer, int flags)
{
	Assert(state == (GenericXLogState *)&reuse_image && buffer == 1 && flags == 0);
	return (Page)reuse_image.data;
}

static XLogRecPtr
reuse_xlog_finish(GenericXLogState *state)
{
	Assert(state == (GenericXLogState *)&reuse_image && reuse_xlocks == 1);
	reuse_wal_finishes++;
	memcpy(&reuse_page, &reuse_image, sizeof(reuse_page));
	return 1500;
}

static void
reuse_xlog_abort(GenericXLogState *state)
{
	Assert(state == (GenericXLogState *)&reuse_image && reuse_xlocks == 1);
	reuse_wal_aborts++;
}

#define ctrc_cleaner_itl_page_exact reuse_real_page_exact
#define ctrc_cleaner_clean_itl_receipt reuse_real_cleaner
#define ctrc_cleaner_terminal_sample_exact reuse_terminal
#define BufferGetTag reuse_get_tag
#define BufferGetPage(buffer) ((Page)reuse_page.data)
#define smgropen reuse_smgr_open
#define smgrexists(smgr, forknum) ((void)(smgr), (void)(forknum), reuse_exists)
#define smgrnblocks(smgr, forknum)                                                                 \
	((void)(smgr), (void)(forknum), reuse_receipt.target.block_number + 1)
#define ReadBufferWithoutRelcache reuse_read
#define ClusterLockBufferExclusiveRetryAware reuse_lock
#define LockBuffer reuse_unlock
#define ReleaseBuffer reuse_release
#define UnlockReleaseBuffer reuse_unlock_release
#define GenericXLogStartLogged reuse_xlog_start
#define GenericXLogRegisterBuffer reuse_xlog_register
#define GenericXLogFinish reuse_xlog_finish
#define GenericXLogAbort reuse_xlog_abort
#define ctrc_cleaner_note_itl_retained reuse_note_itl_retained
#include "test_cluster_ctrc_itl_reuse.inc"
#undef ctrc_cleaner_note_itl_retained
#undef ctrc_cleaner_itl_page_exact
#undef ctrc_cleaner_clean_itl_receipt
#undef ctrc_cleaner_terminal_sample_exact
#undef BufferGetTag
#undef BufferGetPage
#undef smgropen
#undef smgrexists
#undef smgrnblocks
#undef ReadBufferWithoutRelcache
#undef ClusterLockBufferExclusiveRetryAware
#undef LockBuffer
#undef ReleaseBuffer
#undef UnlockReleaseBuffer
#undef GenericXLogStartLogged
#undef GenericXLogRegisterBuffer
#undef GenericXLogFinish
#undef GenericXLogAbort

static void
reuse_setup(bool replaced)
{
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcTargetV1 target;
	ClusterCtrcApplyToken token;
	UBA uba = uba_encode(1, 2, 0, 24);
	Page page = (Page)reuse_page.data;
	PageHeader header = (PageHeader)page;
	ClusterItlSlotData *slot;
	HeapTupleHeader tuple;

	reset_fixture();
	MemSet(&reuse_page, 0, sizeof(reuse_page));
	MemSet(&reuse_dependencies, 0, sizeof(reuse_dependencies));
	reuse_pins = reuse_xlocks = reuse_locks = reuse_enters = reuse_leaves = 0;
	reuse_wal_starts = reuse_wal_finishes = reuse_wal_aborts = 0;
	reuse_rechecks = reuse_fail_recheck = reuse_fail_lock = 0;
	reuse_terminal_ok = reuse_tag_ok = reuse_exists = true;
	reuse_dependency_drift = false;
	reuse_between_rounds = NULL;
	reuse_retained_notes = 0;
	reuse_retained_stage = NULL;
	reuse_terminal_status = CTRC_TERMINAL_COMMITTED;
	reuse_local_floor = 901;
	reuse_peer_view = (ClusterUndoHorizonReportView){
		.valid = true,
		.stable = true,
		.has_capability = true,
		.epoch = test_cluster_epoch,
		.horizon_scn = 901,
		.recv_at_us = 9999999,
		.sender_interval_ms = 2000,
	};
	reuse_members_valid = true;
	reuse_raw_disabled = reuse_epoch_fenced = false;
	reuse_members_epoch = test_cluster_epoch;
	reuse_origin = 0;
	reuse_view_count = CLUSTER_MAX_NODES;
	reuse_floor_samples = 0;
	reuse_native_allowed = false;
	reuse_native_throw = reuse_slru_held = false;
	reuse_covered = true;
	reuse_ungated = 0;
	reuse_native_depth = reuse_truncation_depth = reuse_native_reads = reuse_guarded_cas = 0;
	reuse_native_status = TRANSACTION_STATUS_ABORTED;
	reuse_variable_cache.oldestClogXid = FirstNormalTransactionId;
	external_lock_hook = reuse_external_lock;
	cluster_undo_retention_horizon_enabled = true;
	test_flush_lsn = 4000;
	reuse_peer_flush = 3500;
	durability_hook = reuse_assert_unlocked;
	origin_durability_hook = reuse_peer_durable;
	reuse_dependencies.required[0] = 2000;
	reuse_dependencies.required[1] = 3000;
	origin = seed_origin(0, 1);
	if (prepare_fixture_receipt(origin, 81, &reuse_handle) != CLUSTER_CTRC_PREPARE_READY)
		abort();
	target = reuse_handle.receipt->target;
	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = 0;
	target.itl_slot_wrap = 7;
	target.itl_xid = origin->key.xid;
	target.itl_class = 2;
	target.planned_predecessor_sha256[0] = 1;
	target.planned_successor_sha256[0] = 2;
	memcpy(target.uba, &uba, sizeof(uba));
	if (cluster_ctrc_receipt_apply_prepared(reuse_handle.participant, reuse_handle.receipt, &target,
											&token)
		!= CLUSTER_CTRC_APPLY_APPLIED)
		abort();
	reuse_handle.participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	reuse_participant = *reuse_handle.participant;
	reuse_receipt = *reuse_handle.receipt;
	header->pd_flags = PD_HAS_ITL;
	header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_lower = SizeOfPageHeaderData + sizeof(ItemIdData);
	header->pd_upper = header->pd_special - MAXALIGN(SizeofHeapTupleHeader);
	PageSetPageSizeAndVersion(page, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	PageSetLSNPreserveOrigin(page, 2000);
	PageSetLSNOrigin(page, 0);
	header->pd_block_scn = 31;
	ItemIdSetNormal(PageGetItemId(page, 1), header->pd_upper, SizeofHeapTupleHeader);
	tuple = (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
	tuple->t_hoff = SizeofHeapTupleHeader;
	tuple->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmin(tuple, 800);
	HeapTupleHeaderSetXmax(tuple, target.itl_xid);
	if (replaced)
		tuple->t_infomask |= HEAP_XMAX_INVALID;
	slot = &ClusterPageGetItlSlots(page)[0];
	slot->xid = target.itl_xid + (replaced ? 16 : 0);
	slot->wrap = target.itl_slot_wrap + (replaced ? 1 : 0);
	slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
	slot->commit_scn = replaced ? 950 : 900;
	slot->undo_segment_head = replaced ? uba_encode(1, 3, 1, 24) : uba;
}

static bool
reuse_run(void)
{
	bool result = reuse_real_cleaner(&reuse_participant, &reuse_receipt,
									 reuse_handle.participant_index, reuse_handle.receipt_index);
	if (reuse_pins != 0 || reuse_xlocks != 0 || held_count != 0 || reuse_enters != reuse_leaves)
		abort();
	return result;
}

static HeapTupleHeader
reuse_tuple(void)
{
	Page page = (Page)reuse_page.data;
	return (HeapTupleHeader)PageGetItem(page, PageGetItemId(page, 1));
}

static void
reuse_expect_retained(void)
{
	PGAlignedBlock before = reuse_page;
	UT_ASSERT(!reuse_run());
	UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(reuse_handle.participant->applied_count, 1);
	UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 0);
	UT_ASSERT_EQ(reuse_wal_finishes, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
}

UT_TEST(test_exact_terminal_slot_still_rewrites_and_discharges)
{
	reuse_setup(false);
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
	UT_ASSERT_EQ(reuse_wal_finishes, 1);
	UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
}

UT_TEST(test_reused_lock_slot_absence_discharges_without_page_write)
{
	PGAlignedBlock before;
	reuse_setup(true);
	before = reuse_page;
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
	UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
	UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 1);
	UT_ASSERT_EQ(reuse_wal_starts, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_TARGET_ABSENT), 1);
	UT_ASSERT_EQ(wake_count, 1);
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_TARGET_ABSENT), 1);
}

UT_TEST(test_changed_slot_is_never_rewritten_as_the_old_incarnation)
{
	ClusterItlSlotData before;
	ClusterItlSlotData *slot;
	reuse_setup(true);
	slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[0];
	before = *slot;
	UT_ASSERT_EQ(cluster_ctrc_itl_cleanout_slot(&reuse_receipt.key, &reuse_receipt.target,
												CTRC_TERMINAL_COMMITTED, 900, slot),
				 CLUSTER_CTRC_ITL_CLEANOUT_RETAIN);
	UT_ASSERT(memcmp(&before, slot, sizeof(before)) == 0);
}

UT_TEST(test_reuse_needs_strict_incarnation_and_well_formed_slots)
{
	unsigned leg;
	for (leg = 0; leg < 15; leg++) {
		ClusterItlSlotData *slots;
		reuse_setup(true);
		slots = ClusterPageGetItlSlots((Page)reuse_page.data);
		switch (leg) {
		case 0:
			slots[0].wrap = reuse_receipt.target.itl_slot_wrap;
			break;
		case 1:
			slots[0].wrap = 0;
			break;
		case 2:
			slots[0].xid = reuse_receipt.target.itl_xid;
			break;
		case 3:
			memcpy(&slots[0].undo_segment_head, reuse_receipt.target.uba, 16);
			break;
		case 4:
			slots[0].flags = ITL_FLAG_FREE;
			break;
		case 5:
			slots[0].flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			break;
		case 6:
			slots[0].flags = 255;
			break;
		case 7:
			slots[0].commit_scn = InvalidScn;
			break;
		case 8:
			slots[0].flags = ITL_FLAG_LOCK_ONLY_ACTIVE;
			break;
		case 9:
			slots[0].xid = FrozenTransactionId;
			break;
		case 10:
			slots[0].undo_segment_head.raw[1] |= UINT64_C(1) << 48;
			break;
		case 11:
			slots[1].xid = reuse_receipt.target.itl_xid;
			break;
		case 12:
			memcpy(&slots[1].undo_segment_head, reuse_receipt.target.uba, 16);
			break;
		case 13:
			slots[1].commit_scn = 10;
			break;
		case 14:
			slots[1].flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			break;
		}
		reuse_expect_retained();
	}
}

UT_TEST(test_old_tuple_reference_or_ambiguous_mx_retains)
{
	unsigned leg;
	for (leg = 0; leg < 4; leg++) {
		HeapTupleHeader tuple;
		reuse_setup(true);
		tuple = reuse_tuple();
		if (leg == 0)
			HeapTupleHeaderSetXmin(tuple, reuse_receipt.target.itl_xid);
		else {
			tuple->t_infomask &= ~HEAP_XMAX_INVALID;
			if (leg == 2)
				tuple->t_infomask &= ~(HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK);
			if (leg == 3) {
				tuple->t_infomask |= HEAP_XMAX_IS_MULTI;
				HeapTupleHeaderSetXmax(tuple, 999);
			}
		}
		reuse_expect_retained();
	}
}

UT_TEST(test_malformed_page_and_tuple_geometry_retains_without_write)
{
	unsigned leg;
	for (leg = 0; leg < 13; leg++) {
		Page page;
		PageHeader header;
		ItemId item;
		HeapTupleHeader tuple;
		reuse_setup(true);
		page = (Page)reuse_page.data;
		header = (PageHeader)page;
		item = PageGetItemId(page, 1);
		tuple = reuse_tuple();
		switch (leg) {
		case 0:
			header->pd_special = BLCKSZ;
			break;
		case 1:
			header->pd_lower = SizeOfPageHeaderData - 1;
			break;
		case 2:
			header->pd_upper = header->pd_special + 1;
			break;
		case 3:
			header->pd_lower++;
			break;
		case 4:
			header->pd_flags &= ~PD_HAS_ITL;
			break;
		case 5:
			header->pd_pagesize_version = 0;
			break;
		case 6:
			ItemIdSetNormal(item, header->pd_upper - 8, SizeofHeapTupleHeader);
			break;
		case 7:
			ItemIdSetNormal(item, header->pd_upper + 1, SizeofHeapTupleHeader);
			break;
		case 8:
			ItemIdSetNormal(item, header->pd_upper, 2);
			break;
		case 9:
			ItemIdSetNormal(item, header->pd_special, SizeofHeapTupleHeader);
			break;
		case 10:
			tuple->t_hoff = SizeofHeapTupleHeader - 1;
			break;
		case 11:
			tuple->t_hoff = SizeofHeapTupleHeader + 1;
			break;
		case 12:
			ItemIdSetRedirect(item, 2);
			break;
		}
		reuse_expect_retained();
	}
}

UT_TEST(test_authority_and_both_current_rounds_remain_mandatory)
{
	unsigned leg;
	for (leg = 0; leg < 7; leg++) {
		reuse_setup(true);
		switch (leg) {
		case 0:
			reuse_terminal_ok = false;
			break;
		case 1:
			reuse_tag_ok = false;
			break;
		case 2:
			reuse_exists = false;
			break;
		case 3:
			reuse_fail_lock = 1;
			break;
		case 4:
			reuse_fail_lock = 2;
			break;
		case 5:
			reuse_fail_recheck = 1;
			break;
		case 6:
			reuse_fail_recheck = 2;
			break;
		}
		reuse_expect_retained();
	}
}

static void
reuse_restore_old_reference(void)
{
	reuse_tuple()->t_infomask &= ~HEAP_XMAX_INVALID;
}

static void
reuse_advance_page_lsn(void)
{
	PageSetLSNPreserveOrigin((Page)reuse_page.data, 3001);
	PageSetLSNOrigin((Page)reuse_page.data, 1);
}

UT_TEST(test_recheck_rejects_reference_return_and_dependency_drift)
{
	unsigned leg;
	for (leg = 0; leg < 3; leg++) {
		reuse_setup(true);
		if (leg == 0)
			reuse_between_rounds = reuse_restore_old_reference;
		else if (leg == 1)
			reuse_between_rounds = reuse_advance_page_lsn;
		else
			reuse_dependency_drift = true;
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_handle.participant->applied_count, 1);
		UT_ASSERT_EQ(reuse_wal_starts, 0);
	}
}

UT_TEST(test_page_origin_wal_is_required_even_with_empty_pending_vector)
{
	unsigned leg;
	for (leg = 0; leg < 2; leg++) {
		reuse_setup(true);
		MemSet(&reuse_dependencies, 0, sizeof(reuse_dependencies));
		PageSetLSNOrigin((Page)reuse_page.data, 1);
		/* Local 4000 covers either value; peer 3500 covers only the latter. */
		PageSetLSNPreserveOrigin((Page)reuse_page.data, leg == 0 ? 3501 : 3000);
		if (leg == 0)
			reuse_expect_retained();
		else {
			UT_ASSERT(reuse_run());
			UT_ASSERT_EQ(reuse_handle.receipt->highest_local_wal_lsn, 4000);
			UT_ASSERT_EQ(reuse_handle.receipt->required_lsn[1], 3000);
			UT_ASSERT_EQ(flush_calls, 0);
		}
	}
}

static void
reuse_final_peer_fault(unsigned call)
{
	reuse_assert_unlocked(call);
	if (call == 2)
		reuse_peer_flush = 2999;
}

static void
reuse_final_fault(unsigned call)
{
	reuse_assert_unlocked(call);
	if (call == 2)
		test_flush_lsn = 1000;
}

static void
reuse_final_identity_drift(unsigned call)
{
	reuse_assert_unlocked(call);
	if (call == 2)
		reuse_handle.receipt->publication.journal_slot_generation++;
}

UT_TEST(test_final_shared_identity_and_durability_can_still_refuse)
{
	unsigned leg;
	for (leg = 0; leg < 3; leg++) {
		reuse_setup(true);
		durability_hook = leg == 0	 ? reuse_final_fault
						  : leg == 1 ? reuse_final_identity_drift
									 : reuse_final_peer_fault;
		reuse_expect_retained();
	}
}

UT_TEST(test_data_and_mx_receipts_do_not_gain_the_absence_path)
{
	reuse_setup(true);
	reuse_receipt.target.itl_class = 1;
	reuse_expect_retained();
}

UT_TEST(test_retained_reason_is_reported_after_page_release)
{
	reuse_setup(true);
	reuse_receipt.target.itl_class = 1;
	reuse_expect_retained();
	UT_ASSERT_EQ(reuse_retained_notes, 1);
	UT_ASSERT(reuse_retained_stage != NULL);
	if (reuse_retained_stage != NULL)
		UT_ASSERT(strcmp(reuse_retained_stage, "SLOT_REVALIDATE") == 0);
	reuse_setup(true);
	reuse_dependency_drift = true;
	reuse_expect_retained();
	UT_ASSERT_EQ(reuse_retained_notes, 1);
	UT_ASSERT(reuse_retained_stage != NULL);
	if (reuse_retained_stage != NULL)
		UT_ASSERT(strcmp(reuse_retained_stage, "DEPENDENCY_CHANGED") == 0);
	reuse_setup(true);
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_retained_notes, 0);
}

static void
reuse_data_setup(void)
{
	ClusterItlSlotData *slot;
	reuse_setup(true);
	reuse_receipt.target.itl_class = reuse_history_class;
	reuse_handle.receipt->target.itl_class = reuse_history_class;
	((PageHeader)reuse_page.data)->pd_block_scn = 1000;
	slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[0];
	slot->flags = ITL_FLAG_COMMITTED;
	slot->write_scn = 925;
	HeapTupleHeaderSetXmin(reuse_tuple(), reuse_receipt.key.xid);
}

/* This fails if a superseded DATA target has no completion path even after
 * every required reader floor passes its exact canonical commit. The raw
 * creator remains unchanged: this is not a fabricated tuple-absence proof. */
UT_TEST(test_retired_data_below_cluster_floor_discharges_without_tuple_rewrite)
{
	PGAlignedBlock before;
	reuse_data_setup();
	before = reuse_page;
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
	UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
	UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 1);
	UT_ASSERT_EQ(reuse_wal_starts, 0);
	UT_ASSERT_EQ(flush_calls, 0);
	UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	UT_ASSERT_EQ(HeapTupleHeaderGetRawXmin(reuse_tuple()), reuse_receipt.key.xid);
}

UT_TEST(test_data_retirement_keeps_all_retention_and_namespace_failures)
{
	for (unsigned leg = 0; leg < 19; leg++) {
		reuse_data_setup();
		switch (leg) {
		case 0:
			reuse_local_floor = 899;
			break;
		case 1:
			reuse_local_floor = 900;
			break;
		case 2:
			reuse_local_floor = InvalidScn;
			break;
		case 3:
			reuse_peer_view.horizon_scn = 900;
			break;
		case 4:
			reuse_peer_view.valid = false;
			break;
		case 5:
			reuse_peer_view.stable = false;
			break;
		case 6:
			reuse_peer_view.has_capability = false;
			break;
		case 7:
			reuse_peer_view.epoch++;
			break;
		case 8:
			reuse_peer_view.regression_flagged = true;
			break;
		case 9:
			reuse_peer_view.recv_at_us = 1;
			break;
		case 10:
			reuse_peer_view.horizon_scn = CLUSTER_UNDO_HORIZON_REPORT_UNCONSTRAINED;
			break;
		case 11:
			reuse_members_valid = false;
			break;
		case 12:
			reuse_members_epoch++;
			break;
		case 13:
			reuse_epoch_fenced = true;
			break;
		case 14:
			reuse_origin = -1;
			break;
		case 15:
			reuse_origin = 1;
			break;
		case 16:
			reuse_raw_disabled = true;
			break;
		case 17:
			cluster_undo_retention_horizon_enabled = false;
			break;
		case 18:
			reuse_view_count = 1;
			break;
		}
		reuse_expect_retained();
	}
}

UT_TEST(test_data_retirement_cannot_invent_canonical_terminal_status)
{
	for (unsigned leg = 0; leg < 3; leg++) {
		reuse_data_setup();
		if (leg == 0)
			reuse_terminal_ok = false;
		else
			reuse_terminal_status = leg == 1 ? CTRC_TERMINAL_ABORTED : CTRC_TERMINAL_UNKNOWN;
		reuse_expect_retained();
	}
}

UT_TEST(test_data_first_wrap_and_ordinary_successors_keep_logical_history)
{
	for (unsigned role = ITL_FLAG_ACTIVE; role <= ITL_FLAG_LOCK_ONLY_ABORTED; role++) {
		for (unsigned tuple_side = 0; tuple_side < 3; tuple_side++) {
			ClusterItlSlotData *slot;
			PGAlignedBlock before;
			reuse_data_setup();
			reuse_receipt.target.itl_slot_wrap = 0;
			reuse_handle.receipt->target.itl_slot_wrap = 0;
			slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[0];
			slot->wrap = 1;
			slot->flags = role;
			slot->commit_scn = role == ITL_FLAG_COMMITTED || role == ITL_FLAG_NEEDS_CLEANOUT
									   || role == ITL_FLAG_LOCK_ONLY_COMMITTED
								   ? 950
								   : InvalidScn;
			if (tuple_side == 1) {
				reuse_tuple()->t_infomask = 0;
				HeapTupleHeaderSetXmax(reuse_tuple(), reuse_receipt.key.xid);
			} else if (tuple_side == 2) {
				HeapTupleHeaderSetXmin(reuse_tuple(), 800);
				HeapTupleHeaderSetXmax(reuse_tuple(), InvalidTransactionId);
			}
			before = reuse_page;
			UT_ASSERT(reuse_run());
			UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
			UT_ASSERT_EQ(reuse_wal_starts, 0);
			UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
		}
	}
}

UT_TEST(test_data_surviving_reference_or_malformed_history_retains)
{
	for (unsigned leg = 0; leg < 16; leg++) {
		ClusterItlSlotData *slots;
		reuse_data_setup();
		slots = ClusterPageGetItlSlots((Page)reuse_page.data);
		switch (leg) {
		case 0:
			slots[0].wrap = 7;
			break;
		case 1:
			slots[0].wrap = 0;
			break;
		case 2:
			slots[0].flags = ITL_FLAG_FREE;
			break;
		case 3:
			slots[0].flags = ITL_FLAG_LOCK_ONLY_XMAX_IS_MULTI;
			break;
		case 4:
			slots[0].xid = reuse_receipt.key.xid;
			break;
		case 5:
			memcpy(&slots[0].undo_segment_head, reuse_receipt.target.uba, 16);
			break;
		case 6:
			slots[1].xid = reuse_receipt.key.xid;
			break;
		case 7:
			memcpy(&slots[1].undo_segment_head, reuse_receipt.target.uba, 16);
			break;
		case 8:
			slots[0].write_scn = InvalidScn;
			break;
		case 9:
			slots[0].commit_scn = InvalidScn;
			break;
		case 10:
			slots[0].undo_segment_head = uba_encode(1, 0, 0, 0);
			break;
		case 11:
			reuse_tuple()->t_itl_slot_idx = CLUSTER_ITL_SLOT_UNALLOCATED;
			break;
		case 12:
			reuse_tuple()->t_itl_slot_idx = 1;
			break;
		case 13:
			reuse_tuple()->t_infomask = HEAP_XMAX_IS_MULTI;
			break;
		case 14:
			reuse_tuple()->t_hoff = SizeofHeapTupleHeader - 1;
			break;
		case 15:
			reuse_tuple()->t_infomask |= HEAP_MOVED_IN;
			break;
		}
		reuse_expect_retained();
	}
}

static void
reuse_return_data_carrier(void)
{
	ClusterPageGetItlSlots((Page)reuse_page.data)[1].xid = reuse_receipt.key.xid;
}

static void
reuse_regress_peer_floor(void)
{
	reuse_peer_view.horizon_scn = 900;
}

static void
reuse_disable_raw_window(void)
{
	reuse_raw_disabled = true;
}

UT_TEST(test_data_rechecks_both_page_and_floor_before_shared_discharge)
{
	for (unsigned leg = 0; leg < 7; leg++) {
		reuse_data_setup();
		switch (leg) {
		case 0:
			reuse_between_rounds = reuse_return_data_carrier;
			break;
		case 1:
			reuse_between_rounds = reuse_regress_peer_floor;
			break;
		case 2:
			reuse_between_rounds = reuse_disable_raw_window;
			break;
		case 3:
			reuse_dependency_drift = true;
			break;
		case 4:
			reuse_fail_recheck = 2;
			break;
		case 5:
			reuse_fail_lock = 2;
			break;
		case 6:
			reuse_between_rounds = reuse_advance_page_lsn;
			break;
		}
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_handle.participant->applied_count, 1);
		UT_ASSERT_EQ(reuse_wal_finishes, 0);
	}
}

UT_TEST(test_data_final_shared_identity_and_origin_durability_still_guard)
{
	for (unsigned leg = 0; leg < 4; leg++) {
		reuse_data_setup();
		if (leg == 0) {
			MemSet(&reuse_dependencies, 0, sizeof(reuse_dependencies));
			PageSetLSNOrigin((Page)reuse_page.data, 1);
			PageSetLSNPreserveOrigin((Page)reuse_page.data, 3501);
		} else
			durability_hook = leg == 1	 ? reuse_final_fault
							  : leg == 2 ? reuse_final_identity_drift
										 : reuse_final_peer_fault;
		reuse_expect_retained();
	}
}

UT_TEST(test_foreign_history_waits_for_real_coverage_then_retires_same_receipt)
{
	uint8 saved_class = reuse_history_class;
	for (reuse_history_class = 1; reuse_history_class <= 2; reuse_history_class++) {
		PGAlignedBlock before;
		reuse_data_setup();
		MemSet(&reuse_dependencies, 0, sizeof(reuse_dependencies));
		PageSetLSNOrigin((Page)reuse_page.data, 1);
		PageSetLSNPreserveOrigin((Page)reuse_page.data, 3000);
		reuse_peer_flush = InvalidXLogRecPtr;
		before = reuse_page;
		reuse_expect_retained();
		UT_ASSERT_EQ(reuse_retained_notes, 1);
		UT_ASSERT(reuse_retained_stage != NULL
				  && strcmp(reuse_retained_stage, "WAL_DURABILITY") == 0);
		reuse_peer_flush = 2999;
		reuse_expect_retained();
		UT_ASSERT_EQ(reuse_retained_notes, 2);
		/* Boundary provider now exposes actual sufficient coverage. The
		 * unchanged complete cleaner, shared identity/CAS and floor run again. */
		reuse_peer_flush = 3000;
		UT_ASSERT(reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
		UT_ASSERT_EQ(reuse_handle.receipt->required_lsn[1], 3000);
		UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
		UT_ASSERT_EQ(reuse_wal_starts, 0);
		UT_ASSERT_EQ(flush_calls, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
	reuse_history_class = saved_class;
}

UT_TEST(test_history_floor_refusal_is_observed_only_after_release)
{
	reuse_data_setup();
	reuse_local_floor = 899;
	reuse_expect_retained();
	UT_ASSERT_EQ(reuse_retained_notes, 1);
	UT_ASSERT(reuse_retained_stage != NULL && strcmp(reuse_retained_stage, "HISTORY_FLOOR") == 0);
}

UT_TEST(test_data_completed_receipt_is_idempotent_and_exact_slot_is_unchanged)
{
	reuse_data_setup();
	UT_ASSERT(reuse_run());
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 1);
	UT_ASSERT_EQ(cluster_ctrc_stat_get(CTRC_STAT_TARGET_ABSENT), 1);
	reuse_setup(false);
	reuse_receipt.target.itl_class = 1;
	reuse_handle.receipt->target.itl_class = 1;
	ClusterPageGetItlSlots((Page)reuse_page.data)[0].flags = ITL_FLAG_COMMITTED;
	reuse_local_floor = InvalidScn;
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_wal_finishes, 1);
	UT_ASSERT_EQ(reuse_floor_samples, 0);
	UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_TERMINAL_REWRITE);
}

UT_TEST(test_lock_publication_with_creator_deleter_history_discharges)
{
	for (unsigned side = 0; side < 3; side++) {
		PGAlignedBlock before;
		reuse_data_setup();
		reuse_receipt.target.itl_class = 2;
		reuse_handle.receipt->target.itl_class = 2;
		reuse_tuple()->t_infomask = side == 0 ? HEAP_XMAX_INVALID : 0;
		HeapTupleHeaderSetXmin(reuse_tuple(), side == 1 ? 800 : reuse_receipt.key.xid);
		HeapTupleHeaderSetXmax(reuse_tuple(), reuse_receipt.key.xid);
		before = reuse_page;
		UT_ASSERT(reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
		UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
		UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 1);
		UT_ASSERT_EQ(reuse_floor_samples, 2);
		UT_ASSERT_EQ(reuse_wal_starts, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

UT_TEST(test_effective_old_lock_is_not_logical_data_history)
{
	for (unsigned itl_class = 1; itl_class <= 2; itl_class++) {
		for (unsigned lock_shape = 0; lock_shape < 3; lock_shape++) {
			reuse_data_setup();
			reuse_receipt.target.itl_class = itl_class;
			reuse_handle.receipt->target.itl_class = itl_class;
			reuse_tuple()->t_infomask = lock_shape == 0 ? HEAP_XMAX_LOCK_ONLY
										: lock_shape == 1
											? HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK
											: HEAP_XMAX_EXCL_LOCK;
			HeapTupleHeaderSetXmax(reuse_tuple(), reuse_receipt.key.xid);
			reuse_expect_retained();
		}
	}
}

UT_TEST(test_strict_lock_absence_keeps_its_original_terminal_contract)
{
	for (unsigned aborted = 0; aborted < 2; aborted++) {
		reuse_setup(true);
		ClusterPageGetItlSlots((Page)reuse_page.data)[0].write_scn = 925;
		reuse_terminal_status = aborted ? CTRC_TERMINAL_ABORTED : CTRC_TERMINAL_COMMITTED;
		reuse_local_floor = InvalidScn;
		reuse_members_valid = false;
		reuse_raw_disabled = true;
		UT_ASSERT(reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
		UT_ASSERT_EQ(reuse_floor_samples, 0);
		UT_ASSERT_EQ(reuse_wal_starts, 0);
	}
}

static void
reuse_return_effective_lock(void)
{
	reuse_tuple()->t_infomask = HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_EXCL_LOCK;
	HeapTupleHeaderSetXmax(reuse_tuple(), reuse_receipt.key.xid);
}

UT_TEST(test_history_recheck_rejects_effective_lock_return)
{
	for (unsigned itl_class = 1; itl_class <= 2; itl_class++) {
		reuse_data_setup();
		reuse_receipt.target.itl_class = itl_class;
		reuse_handle.receipt->target.itl_class = itl_class;
		reuse_between_rounds = reuse_return_effective_lock;
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 0);
		UT_ASSERT_EQ(reuse_wal_starts, 0);
	}
}

/* Build a distinct publication through real prepare/apply/discharge. Only
 * the external page/terminal/durability services remain fixture inputs. */
static ClusterCtrcReceiptHandle
reuse_add_companion(unsigned index, uint8 itl_class, bool cleaned)
{
	ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[0];
	ClusterCtrcReceiptHandle companion;
	ClusterCtrcTargetV1 target;
	ClusterCtrcApplyToken token;
	ClusterCtrcDurability durability;
	ClusterItlSlotData *slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[index];
	UBA uba = uba_encode(1, 5 + index, 1, 24);

	reuse_handle.participant->state = CTRC_PARTICIPANT_OPEN;
	if (prepare_fixture_receipt(origin, 200 + index, &companion) != CLUSTER_CTRC_PREPARE_READY)
		abort();
	target = companion.receipt->target;
	target.kind = CTRC_TARGET_EXACT_ITL_SLOT;
	target.itl_slot_index = index;
	target.itl_slot_wrap = 12 + index;
	target.itl_xid = reuse_receipt.key.xid;
	target.itl_class = itl_class;
	target.planned_predecessor_sha256[0] = 1;
	target.planned_successor_sha256[0] = 2;
	memcpy(target.uba, &uba, sizeof(uba));
	if (cluster_ctrc_receipt_apply_prepared(companion.participant, companion.receipt, &target,
											&token)
		!= CLUSTER_CTRC_APPLY_APPLIED)
		abort();
	*slot = (ClusterItlSlotData){
		.xid = target.itl_xid,
		.wrap = target.itl_slot_wrap,
		.flags = reuse_terminal_status == CTRC_TERMINAL_ABORTED
					 ? (itl_class == 1 ? ITL_FLAG_ABORTED : ITL_FLAG_LOCK_ONLY_ABORTED)
					 : (itl_class == 1 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_COMMITTED),
		.commit_scn = reuse_terminal_status == CTRC_TERMINAL_ABORTED ? InvalidScn : 900,
		.write_scn = 875,
		.undo_segment_head = uba
	};
	if (cleaned) {
		ctrc_participant_capture_durability(&durability);
		durability.highest_local_lsn = 3600;
		durability.required_lsn[1] = 3200;
		if (cluster_ctrc_receipt_discharge_itl(companion.participant, companion.receipt,
											   CTRC_ITL_TERMINAL_INDEPENDENT, &durability)
			!= CLUSTER_CTRC_DISCHARGE_CLEANED)
			abort();
	}
	reuse_handle.participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	reuse_handle.participant->seal_generation = origin->seal_generation;
	reuse_participant = *reuse_handle.participant;
	return companion;
}

UT_TEST(test_same_key_companion_retires_only_its_distinct_old_receipt)
{
	for (unsigned index = 0; index <= 1; index++) {
		for (uint8 itl_class = 1; itl_class <= 2; itl_class++) {
			ClusterCtrcReceiptHandle companion;
			ClusterCtrcReceipt saved;
			PGAlignedBlock before;

			reuse_setup(true);
			companion = reuse_add_companion(index, itl_class, true);
			saved = *companion.receipt;
			before = reuse_page;
			UT_ASSERT(reuse_run());
			UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
			UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
			UT_ASSERT_EQ(reuse_handle.participant->applied_count, 0);
			UT_ASSERT_EQ(reuse_handle.participant->cleaned_count, 2);
			UT_ASSERT_EQ(reuse_floor_samples, 0);
			UT_ASSERT_EQ(reuse_wal_finishes, 0);
			UT_ASSERT_EQ(flush_calls, 0);
			UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
			UT_ASSERT(memcmp(&saved, companion.receipt, sizeof(saved)) == 0);
		}
	}
}

UT_TEST(test_companion_data_history_uses_its_own_xmin_role_and_floor)
{
	ClusterCtrcReceiptHandle companion;
	PGAlignedBlock before;

	reuse_data_setup();
	/* The tuple still points at slot 0 (its newer xmax writer). Its old
	 * xmin must select the distinct DATA slot, not a lock-only carrier. */
	companion = reuse_add_companion(1, 1, true);
	before = reuse_page;
	UT_ASSERT(reuse_run());
	UT_ASSERT_EQ(reuse_floor_samples, 2);
	UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(companion.receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(reuse_handle.receipt->required_lsn[0], 3600);
	UT_ASSERT_EQ(reuse_handle.receipt->required_lsn[1], 3200);
	UT_ASSERT_EQ(reuse_wal_finishes, 0);
	UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
}

UT_TEST(test_companion_is_not_an_xid_or_state_only_permission)
{
	for (unsigned fault = 0; fault < 19; fault++) {
		ClusterCtrcReceiptHandle companion;
		PGAlignedBlock before;
		ClusterItlSlotData *slot;

		reuse_data_setup();
		companion = reuse_add_companion(1, 1, true);
		slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[1];
		switch (fault) {
		case 0:
			companion.receipt->key.segment_generation++;
			break;
		case 1:
			companion.receipt->publication.journal_slot_generation++;
			break;
		case 2:
			companion.receipt->target.block_number++;
			break;
		case 3:
			companion.receipt->target.itl_slot_wrap++;
			break;
		case 4:
			companion.receipt->target.uba[0] ^= 1;
			break;
		case 5:
			companion.receipt->target.itl_class = 2;
			break;
		case 6:
			companion.receipt->state = CTRC_RECEIPT_APPLIED;
			break;
		case 7:
			companion.receipt->disposition = CTRC_RELEASE_CLEANED_ABSENT;
			break;
		case 8:
			slot->commit_scn++;
			break;
		case 9:
			reuse_local_floor = 900;
			break;
		case 10:
			companion.receipt->highest_local_wal_lsn = 4001;
			break;
		case 11:
			companion.receipt->required_lsn[1] = 3501;
			break;
		case 12:
			ClusterPageGetItlSlots((Page)reuse_page.data)[2] = *slot;
			break;
		case 13:
			slot->flags = ITL_FLAG_ACTIVE;
			slot->commit_scn = InvalidScn;
			break;
		case 14: {
			ClusterCtrcReceiptHandle second = reuse_add_companion(2, 1, true);
			ClusterPageGetItlSlots((Page)reuse_page.data)[2].wrap = slot->wrap;
			second.receipt->target.itl_slot_wrap = slot->wrap;
			break;
		}
		case 15:
			slot->flags = ITL_FLAG_LOCK_ONLY_COMMITTED;
			companion.receipt->target.itl_class = 2;
			reuse_tuple()->t_itl_slot_idx = 1;
			reuse_tuple()->t_infomask &= ~(HEAP_XMAX_INVALID | HEAP_XMAX_LOCK_ONLY);
			HeapTupleHeaderSetXmax(reuse_tuple(), reuse_receipt.key.xid);
			break;
		case 16: {
			ClusterCtrcReceiptHandle second = reuse_add_companion(2, 1, true);
			second.receipt->target = companion.receipt->target;
			MemSet(&ClusterPageGetItlSlots((Page)reuse_page.data)[2], 0, sizeof(*slot));
			break;
		}
		case 17:
			companion.receipt->key.origin_boot_incarnation++;
			break;
		case 18:
			/* A syntactically complete locator chain cannot omit the
			 * APPLIED receipt whose lifetime it is supposed to protect. */
			ctrc_participant_receipt_heads()[reuse_handle.participant_index]
				= companion.receipt_index + 1;
			ctrc_receipt_links()[companion.receipt_index].next_plus_one = 0;
			reuse_handle.participant->receipt_count = 1;
			reuse_participant = *reuse_handle.participant;
			break;
		}
		before = reuse_page;
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_wal_finishes, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

static ClusterCtrcReceiptHandle reuse_racing_companion;
static unsigned reuse_companion_fault;

static void
reuse_companion_final_drift(unsigned call)
{
	Assert(reuse_pins == 0 || call == 1);
	Assert(reuse_xlocks == 0 && held_count == 0);
	if (call != 2)
		return;
	switch (reuse_companion_fault) {
	case 0:
		reuse_racing_companion.receipt->target.itl_slot_wrap++;
		break;
	case 1:
		reuse_racing_companion.receipt->publication.journal_slot_generation++;
		break;
	case 2:
		reuse_racing_companion.receipt->required_lsn[1]++;
		break;
	case 3:
		reuse_handle.participant->seal_generation++;
		break;
	case 4:
		reuse_handle.participant->identity.boot_incarnation++;
		break;
	case 5:
		reuse_handle.receipt->publication.journal_slot_generation++;
		break;
	case 6:
		reuse_racing_companion.receipt->state = CTRC_RECEIPT_ACK_FROZEN;
		break;
	}
}

UT_TEST(test_companions_are_revalidated_after_page_release_at_final_cas)
{
	for (reuse_companion_fault = 0; reuse_companion_fault < 7; reuse_companion_fault++) {
		PGAlignedBlock before;

		reuse_data_setup();
		reuse_racing_companion = reuse_add_companion(1, 1, true);
		before = reuse_page;
		durability_calls = 0;
		durability_hook = reuse_companion_final_drift;
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_handle.participant->applied_count, 1);
		UT_ASSERT_EQ(reuse_wal_finishes, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

static void
reuse_test_selector_order(bool companion_first)
{
	ClusterCtrcReceiptHandle old;
	ClusterCtrcReceiptHandle companion;
	ClusterCtrcParticipantEntry selected_participant;
	ClusterCtrcReceipt selected_receipt;
	uint64 selected_pi, selected_ri;
	uint64 selected[3];
	ClusterItlSlotData *slot;

	reuse_data_setup();
	old = reuse_handle;
	companion = reuse_add_companion(1, 1, false);
	slot = &ClusterPageGetItlSlots((Page)reuse_page.data)[1];
	slot->flags = ITL_FLAG_ACTIVE;
	slot->commit_scn = InvalidScn;
	/* Position the actual cursor, not either receipt's state. Exercise both
	 * legitimate selector orders with the same two APPLIED publications. */
	for (unsigned attempt = 0; attempt < 3; attempt++) {
		CtrcBatch.active = true;
		CtrcBatch.remaining[CTRC_SCAN_RECEIPT] = CtrcShared->receipt_entries;
		UT_ASSERT(ctrc_cleaner_next_applied_receipt(&selected_participant, &selected_receipt,
													&selected_pi, &selected_ri));
		if (selected_ri == (companion_first ? old.receipt_index : companion.receipt_index))
			break;
	}
	UT_ASSERT_EQ(selected_ri, companion_first ? old.receipt_index : companion.receipt_index);
	for (unsigned step = 0; step < (companion_first ? 2 : 3); step++) {
		CtrcBatch.remaining[CTRC_SCAN_RECEIPT] = CtrcShared->receipt_entries;
		UT_ASSERT(ctrc_cleaner_next_applied_receipt(&selected_participant, &selected_receipt,
													&selected_pi, &selected_ri));
		selected[step] = selected_ri;
		reuse_handle = selected_ri == old.receipt_index ? old : companion;
		reuse_participant = selected_participant;
		reuse_receipt = selected_receipt;
		if (step == 0 && !companion_first)
			UT_ASSERT(!reuse_run());
		else
			UT_ASSERT(reuse_run());
	}
	UT_ASSERT_EQ(selected[0], companion_first ? companion.receipt_index : old.receipt_index);
	UT_ASSERT_EQ(selected[1], companion_first ? old.receipt_index : companion.receipt_index);
	if (!companion_first)
		UT_ASSERT_EQ(selected[2], old.receipt_index);
	UT_ASSERT_EQ(old.receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(companion.receipt->state, CTRC_RECEIPT_CLEANED);
	UT_ASSERT_EQ(old.participant->applied_count, 0);
	UT_ASSERT_EQ(reuse_wal_finishes, 1);
}

UT_TEST(test_selector_returns_from_old_receipt_then_completes_exact_companion)
{
	reuse_test_selector_order(false);
}

UT_TEST(test_selector_companion_first_completes_the_same_exact_pair)
{
	reuse_test_selector_order(true);
}

static void
reuse_abort_history_setup(unsigned shape)
{
	reuse_history_class = shape == 0 ? 2 : 1;
	reuse_data_setup();
	reuse_terminal_status = CTRC_TERMINAL_ABORTED;
	reuse_native_allowed = true;
	reuse_local_floor = InvalidScn;
	reuse_handle.participant->seal_generation = ctrc_origin_entries()[0].seal_generation;
	if (shape == 2)
		HeapTupleHeaderSetXmin(reuse_tuple(), 800);
	if (shape == 3)
		(void)reuse_add_companion(1, 1, true);
	reuse_participant = *reuse_handle.participant;
	durability_calls = 0;
}

UT_TEST(test_aborted_history_retires_only_with_all_three_proofs_without_floor)
{
	for (unsigned sample = 0; sample < 8; sample++) {
		PGAlignedBlock before;

		reuse_abort_history_setup(sample % 4);
		reuse_covered = sample < 4;
		before = reuse_page;
		UT_ASSERT(reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_CLEANED);
		UT_ASSERT_EQ(reuse_handle.receipt->disposition, CTRC_RELEASE_CLEANED_ABSENT);
		UT_ASSERT_EQ(reuse_native_reads, 2);
		UT_ASSERT_EQ(reuse_guarded_cas, 1);
		UT_ASSERT_EQ(reuse_native_depth, 0);
		UT_ASSERT_EQ(reuse_truncation_depth, 0);
		UT_ASSERT_EQ(reuse_floor_samples, 0);
		UT_ASSERT_EQ(reuse_wal_finishes, 0);
		UT_ASSERT_EQ(flush_calls, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

UT_TEST(test_aborted_history_never_uses_absence_of_live_owner_as_abort_proof)
{
	for (unsigned fault = 0; fault < 9; fault++) {
		PGAlignedBlock before;

		reuse_abort_history_setup(1);
		reuse_covered = false;
		switch (fault) {
		case 0:
			reuse_native_allowed = false;
			break;
		case 1:
			reuse_raw_disabled = true;
			break;
		case 2:
			reuse_terminal_ok = false;
			break;
		case 3:
			cluster_undo_retention_horizon_enabled = false;
			break;
		case 4:
			reuse_ungated = 1;
			break;
		case 5:
			reuse_variable_cache.oldestClogXid = reuse_receipt.key.xid + 1;
			break;
		case 6:
			reuse_native_status = TRANSACTION_STATUS_SUB_COMMITTED;
			break;
		case 7:
			reuse_native_status = TRANSACTION_STATUS_IN_PROGRESS;
			break;
		case 8:
			reuse_native_status = TRANSACTION_STATUS_COMMITTED;
			break;
		}
		before = reuse_page;
		UT_ASSERT(!reuse_run());
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_native_depth, 0);
		UT_ASSERT_EQ(reuse_truncation_depth, 0);
		UT_ASSERT_EQ(reuse_wal_finishes, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

UT_TEST(test_aborted_history_clog_error_releases_native_locks_pin_and_admission)
{
	volatile bool caught = false;

	reuse_abort_history_setup(1);
	reuse_native_throw = true;
	PG_TRY();
	{
		(void)reuse_run();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
	UT_ASSERT_EQ(reuse_native_depth, 0);
	UT_ASSERT_EQ(reuse_truncation_depth, 0);
	UT_ASSERT(!reuse_slru_held);
	UT_ASSERT_EQ(held_count, 0);
	UT_ASSERT_EQ(reuse_pins, 0);
	UT_ASSERT_EQ(reuse_xlocks, 0);
	UT_ASSERT_EQ(reuse_enters, reuse_leaves);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}

static unsigned reuse_abort_final_fault;

static void
reuse_abort_final_drift(unsigned call)
{
	Assert(reuse_xlocks == 0 && held_count == 0);
	if (call != 2)
		return;
	Assert(reuse_pins == 0 && reuse_native_depth == 0 && reuse_truncation_depth == 0);
	switch (reuse_abort_final_fault) {
	case 0:
		reuse_native_allowed = false;
		break;
	case 1:
		reuse_raw_disabled = true;
		break;
	case 2:
		reuse_variable_cache.oldestClogXid = reuse_receipt.key.xid + 1;
		break;
	case 3:
		reuse_native_status = TRANSACTION_STATUS_SUB_COMMITTED;
		break;
	case 4:
		reuse_native_throw = true;
		break;
	case 5:
		reuse_handle.receipt->key.origin_boot_incarnation++;
		break;
	}
}

UT_TEST(test_aborted_history_final_guard_rejects_change_after_second_current)
{
	for (reuse_abort_final_fault = 0; reuse_abort_final_fault < 6; reuse_abort_final_fault++) {
		volatile bool caught = false;
		PGAlignedBlock before;

		reuse_abort_history_setup(1);
		before = reuse_page;
		durability_hook = reuse_abort_final_drift;
		PG_TRY();
		{
			UT_ASSERT(!reuse_run());
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, reuse_abort_final_fault == 4);
		UT_ASSERT_EQ(reuse_handle.receipt->state, CTRC_RECEIPT_APPLIED);
		UT_ASSERT_EQ(reuse_native_depth, 0);
		UT_ASSERT_EQ(reuse_truncation_depth, 0);
		UT_ASSERT(!reuse_slru_held);
		UT_ASSERT_EQ(held_count, 0);
		UT_ASSERT_EQ(reuse_pins, 0);
		UT_ASSERT_EQ(reuse_enters, reuse_leaves);
		UT_ASSERT_EQ(InterruptHoldoffCount, 0);
		UT_ASSERT(memcmp(&before, &reuse_page, sizeof(before)) == 0);
	}
}

int
main(void)
{
	UT_PLAN(44);
	UT_RUN(test_aborted_history_final_guard_rejects_change_after_second_current);
	UT_RUN(test_aborted_history_retires_only_with_all_three_proofs_without_floor);
	UT_RUN(test_aborted_history_never_uses_absence_of_live_owner_as_abort_proof);
	UT_RUN(test_aborted_history_clog_error_releases_native_locks_pin_and_admission);
	reuse_history_class = 1;
	UT_RUN(test_selector_returns_from_old_receipt_then_completes_exact_companion);
	UT_RUN(test_selector_companion_first_completes_the_same_exact_pair);
	UT_RUN(test_companions_are_revalidated_after_page_release_at_final_cas);
	UT_RUN(test_same_key_companion_retires_only_its_distinct_old_receipt);
	UT_RUN(test_companion_data_history_uses_its_own_xmin_role_and_floor);
	UT_RUN(test_companion_is_not_an_xid_or_state_only_permission);
	UT_RUN(test_foreign_history_waits_for_real_coverage_then_retires_same_receipt);
	UT_RUN(test_history_floor_refusal_is_observed_only_after_release);
	UT_RUN(test_lock_publication_with_creator_deleter_history_discharges);
	UT_RUN(test_effective_old_lock_is_not_logical_data_history);
	UT_RUN(test_strict_lock_absence_keeps_its_original_terminal_contract);
	UT_RUN(test_history_recheck_rejects_effective_lock_return);
	for (reuse_history_class = 1; reuse_history_class <= 2; reuse_history_class++) {
		printf("# ordinary history receipt class=%u\n", (unsigned)reuse_history_class);
		UT_RUN(test_retired_data_below_cluster_floor_discharges_without_tuple_rewrite);
		UT_RUN(test_data_retirement_keeps_all_retention_and_namespace_failures);
		UT_RUN(test_data_retirement_cannot_invent_canonical_terminal_status);
		UT_RUN(test_data_first_wrap_and_ordinary_successors_keep_logical_history);
		UT_RUN(test_data_surviving_reference_or_malformed_history_retains);
		UT_RUN(test_data_rechecks_both_page_and_floor_before_shared_discharge);
		UT_RUN(test_data_final_shared_identity_and_origin_durability_still_guard);
		UT_RUN(test_data_completed_receipt_is_idempotent_and_exact_slot_is_unchanged);
	}
	reuse_history_class = 1;
	UT_RUN(test_exact_terminal_slot_still_rewrites_and_discharges);
	UT_RUN(test_reused_lock_slot_absence_discharges_without_page_write);
	UT_RUN(test_changed_slot_is_never_rewritten_as_the_old_incarnation);
	UT_RUN(test_reuse_needs_strict_incarnation_and_well_formed_slots);
	UT_RUN(test_old_tuple_reference_or_ambiguous_mx_retains);
	UT_RUN(test_malformed_page_and_tuple_geometry_retains_without_write);
	UT_RUN(test_authority_and_both_current_rounds_remain_mandatory);
	UT_RUN(test_recheck_rejects_reference_return_and_dependency_drift);
	UT_RUN(test_page_origin_wal_is_required_even_with_empty_pending_vector);
	UT_RUN(test_final_shared_identity_and_durability_can_still_refuse);
	UT_RUN(test_data_and_mx_receipts_do_not_gain_the_absence_path);
	UT_RUN(test_retained_reason_is_reported_after_page_release);
	free(CtrcShared);
	CtrcShared = NULL;
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
