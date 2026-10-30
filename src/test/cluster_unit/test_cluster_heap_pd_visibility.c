/* Original empty publisher and complete heap PD scan consumers.
 * Synthetic rows, PCM/SPACE/snapshot services and sampling are controlled
 * leaves. This is not SQL, transport or ResourceOwner/durability evidence.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int pd_inherited_main(void);
#define VM_QUALIFIED_IOS_MAIN pd_inherited_main
#include "test_cluster_vm_qualified_ios.c"
#undef VM_QUALIFIED_IOS_MAIN

#include "access/heapam.h"
#include "access/tsmapi.h"

static HeapScanDescData pd_scan;
static PGAlignedBlock pd_current, pd_disk, pd_stored;
static BufferDesc pd_descriptor;
static ClusterPcmOwnSnapshot pd_own;
static bool pd_locked, pd_replace;
static unsigned pd_pins, pd_reads, pd_locks, pd_unlocks, pd_checks;
static unsigned pd_conflicts, pd_stores, pd_samples, pd_stats, pd_prepares;
static int pd_drift;
static bool pd_space_error;

static Buffer
pd_read(Relation rel, ForkNumber fork, BlockNumber block,
		ReadBufferMode mode, BufferAccessStrategy strategy)
{
	UT_ASSERT(rel == &relation && fork == MAIN_FORKNUM && block == 0);
	UT_ASSERT(mode == RBM_NORMAL && !pd_locked && ios_evaluators == 0);
	(void)strategy;
	pd_reads++;
	pd_pins++;
	pages[0] = pd_disk;
	return 1;
}

static void
pd_release(Buffer buffer)
{
	UT_ASSERT(buffer == 1 && !pd_locked && pd_pins > 0);
	pd_pins--;
}

static void
pd_lock(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 1 && pd_pins == 1 && ios_evaluators == 0);
	if (mode == BUFFER_LOCK_SHARE)
	{
		UT_ASSERT(!pd_locked);
		pd_locked = true;
		pd_locks++;
		pages[0] = pd_current;
		if (pd_drift == 1) ios_space_generation++;
		if (pd_drift == 2) ios_admission_generation++;
		if (pd_drift == 3) ios_snapshot.cluster_cr_identity++;
		if (pd_drift == 4) ios_snapshot.curcid++;
		if (pd_drift == 5) relation.rd_locator.relNumber++;
	}
	else
	{
		UT_ASSERT(mode == BUFFER_LOCK_UNLOCK && pd_locked);
		pd_locked = false;
		pd_unlocks++;
		if (pd_replace)
			memset(pages[0].data, '!', BLCKSZ);
	}
}

static ClusterPcmOwnResult
pd_pcm_snapshot(BufferDesc *descriptor, ClusterPcmOwnSnapshot *out)
{
	UT_ASSERT(descriptor == &pd_descriptor && pd_locked);
	*out = pd_own;
	return CLUSTER_PCM_OWN_OK;
}

static bool
pd_capture(Relation rel, ClusterSpaceIdentityObservationV1 *out)
{
	UT_ASSERT(!pd_locked);
	pd_prepares++;
	if (pd_space_error)
		elog(ERROR, "controlled identity capture cancellation");
	return ios_space_capture(rel, out);
}

static void
pd_prune(Relation rel, Buffer buffer)
{
	UT_ASSERT(rel == &relation && buffer == 1 && !pd_locked);
}

static bool
pd_visible(HeapTuple tuple, Snapshot snapshot, Buffer buffer)
{
	UT_ASSERT(pd_locked && buffer == 1 && snapshot == pd_scan.rs_base.rs_snapshot);
	pd_checks++;
	return ItemPointerGetOffsetNumber(&tuple->t_self) == 1;
}

static void
pd_conflict(bool visible, Relation rel, HeapTuple tuple, Buffer buffer, Snapshot snapshot)
{
	UT_ASSERT(pd_locked && buffer == 1 && rel == &relation);
	UT_ASSERT(snapshot == pd_scan.rs_base.rs_snapshot);
	(void)visible;
	(void)tuple;
	pd_conflicts++;
}

static void
pd_store(HeapTuple tuple, TupleTableSlot *slot)
{
	UT_ASSERT(!pd_locked && ios_evaluators == 0);
	UT_ASSERT(tuple->t_len == SizeofHeapTupleHeader + 8);
	memcpy(pd_stored.data, tuple->t_data, tuple->t_len);
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	pd_stores++;
}

static OffsetNumber
pd_next_sample(SampleScanState *state, BlockNumber block, OffsetNumber maxoffset)
{
	UT_ASSERT(block == 0 && maxoffset == 4);
	(void)state;
	return pd_samples++ == 0 ? 2 : InvalidOffsetNumber;
}

#undef ReadBufferExtended
#define ReadBufferExtended pd_read
#undef ReleaseBuffer
#define ReleaseBuffer pd_release
#undef LockBuffer
#define LockBuffer pd_lock
#define heap_page_prune_opt pd_prune
#define HeapTupleSatisfiesVisibility pd_visible
#define HeapCheckForSerializableConflictOut pd_conflict
#undef TestForOldSnapshot
#define TestForOldSnapshot(snapshot, rel, page) ((void)0)
#define GetBufferDescriptor(id) ((void)(id), &pd_descriptor)
#define cluster_bufmgr_pcm_own_snapshot pd_pcm_snapshot
#define cluster_space_relation_identity_capture_v1 pd_capture
#define cluster_space_relation_identity_current_v1 ios_space_current
#undef ExecForceStoreHeapTuple
#define ExecForceStoreHeapTuple(tuple, slot, free_tuple) pd_store(tuple, slot)
#define ExecStoreBufferHeapTuple(tuple, slot, buffer) pd_store(tuple, slot)
#undef pgstat_count_heap_getnext
#define pgstat_count_heap_getnext(rel) ((void)(rel), pd_stats++)
#include "test_cluster_heap_pd_visibility.inc"

static void
pd_reset(void)
{
	PGAlignedBlock row = {0};
	HeapTupleHeader tuple = (HeapTupleHeader)row.data;
	ios_reset(true);
	UT_ASSERT(PageIsAllVisible(pages[0].data));
	/* Original producer sets PD/header; rows are synthetic consumer inputs. */
	tuple->t_hoff = SizeofHeapTupleHeader;
	for (int n = 1; n <= 4; n++)
	{
		memset(row.data + SizeofHeapTupleHeader, '0' + n, 8);
		UT_ASSERT_EQ(PageAddItemExtended(pages[0].data, (Item)row.data,
			SizeofHeapTupleHeader + 8, InvalidOffsetNumber, PAI_IS_HEAP), n);
	}
	ItemIdSetUnused(PageGetItemId(pages[0].data, 3));
	ItemIdSetDead(PageGetItemId(pages[0].data, 4));
	pd_current = pd_disk = pages[0];
	memset(&pd_scan, 0, sizeof(pd_scan));
	pd_scan.rs_base.rs_rd = &relation;
	pd_scan.rs_base.rs_snapshot = &ios_snapshot;
	pd_scan.rs_base.rs_flags = SO_ALLOW_PAGEMODE;
	pd_scan.rs_nblocks = 1;
	pd_scan.rs_cbuf = InvalidBuffer;
	pd_locked = pd_replace = pd_space_error = false;
	pd_pins = pd_reads = pd_locks = pd_unlocks = pd_checks = 0;
	pd_conflicts = pd_stores = pd_samples = pd_stats = pd_prepares = 0;
	pd_drift = 0;
	memset(&pd_own, 0, sizeof(pd_own));
	InitBufferTag(&pd_own.tag, &relation.rd_locator, MAIN_FORKNUM, 0);
	pd_own.semantic_buf_state = BM_TAG_VALID | BM_VALID;
	pd_own.pcm_state = PCM_STATE_S;
	pd_own.buffer_type = BUF_TYPE_CURRENT;
	pd_own.generation = 3;
	pd_own.reservation_token = 5;
}

static void
pd_page(void)
{
	heapgetpage((TableScanDesc)&pd_scan, 0);
	UT_ASSERT(!pd_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	UT_ASSERT_EQ(pd_reads, 1);
	UT_ASSERT_EQ(pd_conflicts, 2);
	UT_ASSERT_EQ(pd_locks, pd_unlocks);
	UT_ASSERT_EQ(ios_reads, 0);
}

static bool
pd_sample(bool pagemode)
{
	SampleScanState state = {0};
	TsmRoutine routine = {0};
	TupleTableSlot slot = {0};
	routine.NextSampleTuple = pd_next_sample;
	state.tsmroutine = &routine;
	if (!pagemode)
	{
		pd_scan.rs_base.rs_flags = 0;
		heapgetpage((TableScanDesc)&pd_scan, 0);
	}
	UT_ASSERT_EQ(ios_evaluators, 0);
	return heapam_scan_sample_next_tuple((TableScanDesc)&pd_scan, &state, &slot);
}

UT_TEST(qualified_current_heap_skips_precise_checks)
{
	for (int state = 0; state < 3; state++)
	{
		pd_reset();
		pd_own.pcm_state = state == 0 ? PCM_STATE_S :
			state == 1 ? PCM_STATE_X : PCM_STATE_READ_IMAGE;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 0);
		UT_ASSERT_EQ(pd_scan.rs_ntuples, 2);
		UT_ASSERT_EQ(pd_scan.rs_owned_kind, 1);
	}
}

UT_TEST(disk_pd_one_current_zero_uses_precise_visibility)
{
	pd_reset();
	PageClearAllVisible(pd_current.data);
	pd_page();
	UT_ASSERT_EQ(pd_checks, 2);
	UT_ASSERT_EQ(pd_scan.rs_ntuples, 1);
}

UT_TEST(old_unknown_or_unassigned_catalog_stays_precise)
{
	for (int n = 0; n < 3; n++)
	{
		pd_reset();
		if (n == 0) control.catalog_version_no = 0;
		if (n == 1) control.catalog_version_no = 17;
		if (n == 2) ControlFile = NULL;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 2);
		UT_ASSERT_EQ(pd_prepares, 0);
	}
}

UT_TEST(non_mvcc_and_recovery_snapshots_stay_precise)
{
	SnapshotType types[] = {SNAPSHOT_ANY, SNAPSHOT_SELF, SNAPSHOT_NON_VACUUMABLE};
	for (unsigned n = 0; n < lengthof(types) + 1; n++)
	{
		pd_reset();
		if (n < lengthof(types)) ios_snapshot.snapshot_type = types[n];
		else ios_snapshot.takenDuringRecovery = true;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 2);
	}
}

UT_TEST(identity_and_command_drift_after_heap_acquire_stay_precise)
{
	for (int n = 1; n <= 5; n++)
	{
		pd_reset();
		pd_drift = n;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 2);
	}
}

UT_TEST(unknown_admission_space_snapshot_or_pcm_stays_precise)
{
	for (int n = 0; n < 4; n++)
	{
		pd_reset();
		if (n == 0) ios_admission = false;
		if (n == 1) ios_space = false;
		if (n == 2) ios_snapshot_live = false;
		if (n == 3) ios_pcm = false;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 2);
	}
}

UT_TEST(non_current_unbound_or_fenced_heap_is_not_qualification)
{
	for (int n = 0; n < 10; n++)
	{
		pd_reset();
		if (n == 0) pd_own.buffer_type = BUF_TYPE_CR;
		if (n == 1) pd_own.buffer_type = BUF_TYPE_PI;
		if (n == 2) pd_own.pcm_state = PCM_STATE_N;
		if (n == 3) pd_own.flags = PCM_OWN_FLAG_REVOKING;
		if (n == 4) pd_own.writer_activation_token = 9;
		if (n == 5) pd_own.resource_x_activation_generation = 9;
		if (n == 6) pd_own.tag.blockNum = 1;
		if (n == 7) pd_own.semantic_buf_state = BM_TAG_VALID;
		if (n == 8) pd_own.generation = 0;
		if (n == 9) pd_own.reservation_token = 0;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 2);
	}
}

UT_TEST(native_and_temporary_pd_behavior_is_unchanged)
{
	for (int n = 0; n < 2; n++)
	{
		pd_reset();
		if (n == 0) cluster_enabled = cluster_shared_config = false;
		else relation.rd_rel->relpersistence = RELPERSISTENCE_TEMP;
		pd_page();
		UT_ASSERT_EQ(pd_checks, 0);
		UT_ASSERT_EQ(pd_scan.rs_ntuples, 2);
		UT_ASSERT_EQ(pd_prepares, 0);
	}
}

UT_TEST(sample_current_qualification_and_fallback_use_original_callback)
{
	for (int n = 0; n < 2; n++)
	{
		pd_reset();
		if (n) PageClearAllVisible(pd_current.data);
		UT_ASSERT_EQ(pd_sample(false), n == 0);
		UT_ASSERT_EQ(pd_checks, n == 0 ? 0 : 1);
		UT_ASSERT_EQ(pd_conflicts, 1);
		UT_ASSERT_EQ(pd_stats, n == 0 ? 1 : 0);
		UT_ASSERT_EQ(pd_stores, n == 0 ? 1 : 0);
		UT_ASSERT(!pd_locked && ios_evaluators == 0);
	}
}

UT_TEST(sample_pagemode_uses_saved_offsets_and_bytes)
{
	pd_reset();
	pd_replace = true;
	pd_page();
	UT_ASSERT(pd_sample(true));
	UT_ASSERT_EQ(pd_stored.data[SizeofHeapTupleHeader], '2');
	UT_ASSERT_EQ(pd_checks, 0);
	UT_ASSERT_EQ(pd_locks, 1);
	UT_ASSERT_EQ(pd_stores, 1);
}

UT_TEST(saved_pd_bit_cannot_bypass_original_precise_offset_list)
{
	pd_reset();
	control.catalog_version_no = 0;
	pd_replace = true;
	pd_page();
	UT_ASSERT(PageIsAllVisible(pd_scan.rs_owned_page));
	UT_ASSERT(!pd_sample(true));
	UT_ASSERT_EQ(pd_checks, 2);
	UT_ASSERT_EQ(pd_locks, 1);
	UT_ASSERT_EQ(pd_stores, 0);
}

UT_TEST(error_exits_only_new_snapshot_evaluators)
{
	for (int n = 0; n < 2; n++)
	{
		volatile bool caught = false;
		pd_reset();
		pd_space_error = n == 0;
		ios_error = n == 1;
		expecting_error = true;
		PG_TRY();
		{
			heapgetpage((TableScanDesc)&pd_scan, 0);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT_EQ(ios_evaluators, 0);
		UT_ASSERT_EQ(ios_enters, ios_exits);
		/* Existing heap lock/pin ERROR cleanup belongs to ResourceOwner;
		 * this controlled fixture does not pretend to execute that layer. */
		pd_locked = false;
		pd_pins = 0;
	}
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0)
	{
		UT_PLAN(3);
		UT_RUN(old_unknown_or_unassigned_catalog_stays_precise);
		UT_RUN(disk_pd_one_current_zero_uses_precise_visibility);
		UT_RUN(saved_pd_bit_cannot_bypass_original_precise_offset_list);
	}
	else
	{
		UT_PLAN(12);
		UT_RUN(qualified_current_heap_skips_precise_checks);
		UT_RUN(disk_pd_one_current_zero_uses_precise_visibility);
		UT_RUN(old_unknown_or_unassigned_catalog_stays_precise);
		UT_RUN(non_mvcc_and_recovery_snapshots_stay_precise);
		UT_RUN(identity_and_command_drift_after_heap_acquire_stay_precise);
		UT_RUN(unknown_admission_space_snapshot_or_pcm_stays_precise);
		UT_RUN(non_current_unbound_or_fenced_heap_is_not_qualification);
		UT_RUN(native_and_temporary_pd_behavior_is_unchanged);
		UT_RUN(sample_current_qualification_and_fallback_use_original_callback);
		UT_RUN(sample_pagemode_uses_saved_offsets_and_bytes);
		UT_RUN(saved_pd_bit_cannot_bypass_original_precise_offset_list);
		UT_RUN(error_exits_only_new_snapshot_evaluators);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
