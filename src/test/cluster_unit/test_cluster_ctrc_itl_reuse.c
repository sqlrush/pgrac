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
	*status = CTRC_TERMINAL_COMMITTED;
	*scn = 900;
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

int
main(void)
{
	UT_PLAN(12);
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
