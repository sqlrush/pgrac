/* Native empty publisher/logger -> current VM read scope -> original IOS.
 * Storage/transport, snapshot services and the index AM are controlled leaves.
 * This is not a server, ResourceOwner, remote ACK or physical-durability test.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
int vm_publisher_inherited_main(void);
#define main vm_publisher_inherited_main
#include "test_cluster_vm_visible_version.c"
#undef main

#include "access/genam.h"
#include "access/relscan.h"
#include "catalog/catversion.h"
#include "executor/executor.h"
#include "utils/snapmgr.h"
#include "cluster/cluster_undo_horizon.h"

/* Exercise a qualified test catalog without enabling an old product catalog.
 * Production keeps 0 until the minimum catalog is assigned at integration. */
#if defined(FIXTURE_UNASSIGNED_CATALOG)
#undef PGRAC_VM_QUALIFIED_CATALOG_VERSION
#define PGRAC_VM_QUALIFIED_CATALOG_VERSION 0
#elif !defined(PGRAC_VM_QUALIFIED_CATALOG_VERSION) || PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0
#undef PGRAC_VM_QUALIFIED_CATALOG_VERSION
#define PGRAC_VM_QUALIFIED_CATALOG_VERSION UINT32_C(202610999)
#define FIXTURE_PENDING_CATALOG_ASSIGNMENT 1
#endif

static ControlFileData control;
static ControlFileData *ControlFile;
#include "test_cluster_vm_qualified_format.inc"
#ifndef FIXTURE_HAS_CONTROL_CATALOG_GETTER
static uint32
GetControlFileCatalogVersion(void)
{
	return 0;
}
#endif

static PGAlignedBlock ios_current;
static SnapshotData ios_snapshot;
static bool ios_locked, ios_pcm, ios_grant, ios_space, ios_admission;
static bool ios_heap_visible, ios_missing, ios_copy_error, ios_snapshot_live;
static bool ios_lossy, ios_qual_result;
static int ios_drift, ios_error;
static unsigned ios_reads, ios_grants, ios_unlocks, ios_releases, ios_fetches;
static unsigned ios_indexes, ios_copies, ios_evaluators, ios_enters, ios_exits;
static unsigned ios_quals;
static uint64 ios_space_generation, ios_admission_generation;
static CommandId ios_expected_cid;
static ItemPointerData ios_tid;

static Buffer
ios_vm_readbuf(Relation rel, BlockNumber block, bool extend)
{
	UT_ASSERT(rel == &relation && block == 0 && !extend && !ios_locked);
	ios_reads++;
	return ios_missing ? InvalidBuffer : 2;
}

static uint8
ios_raw_status(Relation rel, BlockNumber block, Buffer *buffer)
{
	UT_ASSERT(rel == &relation && block == 0 && !ios_locked);
	*buffer = 2;
	return (uint8)*PageGetContents(pages[1].data) & VISIBILITYMAP_VALID_BITS;
}

static void
ios_unlock(Buffer buffer, int mode)
{
	UT_ASSERT(buffer == 2 && mode == BUFFER_LOCK_UNLOCK && ios_locked);
	ios_locked = false;
	ios_unlocks++;
}

static void
ios_release(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && !ios_locked);
	ios_releases++;
}

static bool
ios_share(Buffer buffer)
{
	UT_ASSERT(buffer == 2 && !ios_locked && ios_evaluators == 1);
	ios_grants++;
	if (!ios_grant)
		return false;
	ios_locked = true;
	pages[1] = ios_current;
	if (ios_drift == 1)
		ios_space_generation++;
	else if (ios_drift == 2)
		ios_admission_generation++;
	else if (ios_drift == 3)
		ios_snapshot.cluster_cr_identity++;
	else if (ios_drift == 4)
	{
		ios_snapshot.curcid++;
		ios_expected_cid = ios_snapshot.curcid;
	}
	else if (ios_drift == 5)
		fixture_locators[1].relNumber++;
	return true;
}

void
cluster_snapshot_read_enter_v1(ClusterSnapshotReadScopeV1 *scope, Snapshot snapshot)
{
	UT_ASSERT(snapshot == &ios_snapshot && !ios_locked && ios_evaluators == 0);
	memset(scope, 0, sizeof(*scope));
	scope->snapshot = snapshot;
	scope->read_scn = snapshot->read_scn;
	ios_evaluators++;
	ios_enters++;
}

void
cluster_snapshot_read_exit_v1(ClusterSnapshotReadScopeV1 *scope)
{
	UT_ASSERT(scope->snapshot == &ios_snapshot && ios_evaluators == 1);
	ios_evaluators--;
	ios_exits++;
}

bool
cluster_snapshot_cr_identity_v1(Snapshot snapshot, uint64 *out)
{
	UT_ASSERT(snapshot == &ios_snapshot && ios_evaluators == 1);
	if (!ios_snapshot_live)
		return false;
	*out = snapshot->cluster_cr_identity;
	return *out != 0;
}

bool
cluster_undo_horizon_read_admission_capture(SCN scn, ClusterUndoReadAdmissionFence *out)
{
	UT_ASSERT(!ios_locked && ios_evaluators == 1 && scn == ios_snapshot.read_scn);
	memset(out, 0, sizeof(*out));
	out->membership_cut_generation = ios_admission_generation;
	return ios_admission;
}

bool
cluster_undo_horizon_read_admission_current(const ClusterUndoReadAdmissionFence *fence)
{
	UT_ASSERT(ios_evaluators == 1);
	return ios_admission && fence->membership_cut_generation == ios_admission_generation;
}

static bool
ios_space_capture(Relation rel, ClusterSpaceIdentityObservationV1 *out)
{
	UT_ASSERT(rel == &relation && !ios_locked && ios_evaluators == 1);
	memset(out, 0, sizeof(*out));
	out->identity = identity;
	out->invalidations = ios_space_generation;
	return ios_space;
}

static bool
ios_space_current(Relation rel,
										  const ClusterSpaceIdentityObservationV1 *saved)
{
	UT_ASSERT(rel == &relation && ios_evaluators == 1);
	if (ios_error == 1)
		elog(ERROR, "controlled cancellation after VM scope acquisition");
	return ios_space && saved->invalidations == ios_space_generation
		&& memcmp(&saved->identity, &identity, sizeof(identity)) == 0;
}

#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)0)
#endif

#define vm_readbuf ios_vm_readbuf
#define LockBuffer ios_unlock
#define ReleaseBuffer ios_release
#define ClusterLockBufferShareBarrierAware ios_share
#define cluster_pcm_is_active() ios_pcm
#define visibilitymap_get_status ios_raw_status
#define visibilitymap_set_locked run_set
#define cluster_space_relation_identity_capture_v1 ios_space_capture
#define cluster_space_relation_identity_current_v1 ios_space_current
#include "test_cluster_vm_qualified_read.inc"
#undef cluster_space_relation_identity_capture_v1
#undef cluster_space_relation_identity_current_v1
#undef visibilitymap_set_locked

static ItemPointer
ios_index_next(IndexScanDesc scan, ScanDirection direction)
{
	UT_ASSERT(scan->heapRelation == &relation && direction == ForwardScanDirection);
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	return ios_indexes++ == 0 ? &ios_tid : NULL;
}

static bool
ios_fetch(IndexScanDesc scan, TupleTableSlot *slot)
{
	UT_ASSERT(scan->heapRelation == &relation && slot != NULL);
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_snapshot.curcid, ios_expected_cid);
	ios_fetches++;
	return ios_heap_visible;
}

static void
ios_copy(TupleTableSlot *slot)
{
	/* Shared qualified data must be consumed before ending this TID's S scope. */
	UT_ASSERT(!cluster_shared_config || ios_fetches != 0 || ios_locked);
	UT_ASSERT_EQ(ios_evaluators, 0);
	ios_copies++;
	if (ios_copy_error)
		elog(ERROR, "controlled index slot allocation failure");
	slot->tts_flags &= ~TTS_FLAG_EMPTY;
}

static bool
ios_qual(void)
{
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	ios_quals++;
	return ios_qual_result;
}

#define index_getnext_tid ios_index_next
#define index_fetch_heap ios_fetch
#define index_beginscan(...) (abort(), (IndexScanDesc)NULL)
#define index_rescan(...) abort()
#undef CHECK_FOR_INTERRUPTS
#define CHECK_FOR_INTERRUPTS() ((void)0)
#define ExecQualAndReset(...) ios_qual()
#define PredicateLockPage(...) UT_ASSERT(!ios_locked && ios_evaluators == 0)
#define StoreIndexTuple(node, slot, tuple, desc) ios_copy(slot)
#define ExecForceStoreHeapTuple(tuple, slot, free_tuple) ios_copy(slot)
#define ExecClearTuple(slot) ((slot)->tts_flags |= TTS_FLAG_EMPTY, (slot))
#include "test_cluster_vm_qualified_ios.inc"

static void
ios_reset(bool publish)
{
	LVRelState state = empty_reset(false, 0);
	Buffer buffer = 2;

	if (publish) {
		UT_ASSERT(lazy_scan_new_or_empty(&state, 1, 0, pages[0].data, false, &buffer));
		UT_ASSERT_EQ(inserts, 1);
		UT_ASSERT_EQ(edge_count, 2);
		UT_ASSERT_EQ((uint8)*PageGetContents(pages[1].data), VISIBILITYMAP_VALID_BITS);
	}
	ios_current = pages[1];
	memset(held, 0, sizeof(held));
	memset(writer, 0, sizeof(writer));
	empty_fixture = false;
	ControlFile = &control;
	control.catalog_version_no = PGRAC_VM_QUALIFIED_CATALOG_VERSION;
	ios_locked = ios_missing = ios_copy_error = false;
	ios_lossy = false;
	ios_qual_result = true;
	ios_pcm = ios_grant = ios_space = ios_admission = ios_snapshot_live = true;
	ios_heap_visible = false;
	ios_drift = ios_error = 0;
	ios_reads = ios_grants = ios_unlocks = ios_releases = ios_fetches = 0;
	ios_indexes = ios_copies = ios_evaluators = ios_enters = ios_exits = 0;
	ios_quals = 0;
	ios_space_generation = ios_admission_generation = 1;
	memset(&ios_snapshot, 0, sizeof(ios_snapshot));
	ios_snapshot.snapshot_type = SNAPSHOT_MVCC;
	ios_snapshot.read_scn = 700;
	ios_snapshot.read_epoch = 9;
	ios_snapshot.cluster_cr_identity = 19;
	ios_snapshot.curcid = ios_expected_cid = 4;
	ItemPointerSet(&ios_tid, 0, 1);
}

static bool
ios_run(Buffer *cached, bool heap_index)
{
	IndexOnlyScanState node = {0};
	IndexOnlyScan plan = {0};
	EState estate = {0};
	IndexScanDescData scan = {0};
	TupleTableSlot slot = {0}, heap_slot = {0};
	IndexTupleData tuple = {0};
	HeapTupleData hitup = {0};
	TupleDescData desc = {0};
	ExprContext econtext = {0};

	estate.es_direction = ForwardScanDirection;
	estate.es_snapshot = &ios_snapshot;
	plan.indexorderdir = ForwardScanDirection;
	node.ss.ps.state = &estate;
	node.ss.ps.plan = (Plan *)&plan;
	node.ss.ps.ps_ExprContext = &econtext;
	node.ss.ss_currentRelation = &relation;
	node.ss.ss_ScanTupleSlot = &slot;
	node.ioss_TableSlot = &heap_slot;
	node.ioss_ScanDesc = &scan;
	node.ioss_VMBuffer = *cached;
	scan.heapRelation = &relation;
	scan.xs_recheck = ios_lossy;
	if (heap_index) {
		scan.xs_hitup = &hitup;
		scan.xs_hitupdesc = &desc;
		slot.tts_tupleDescriptor = &desc;
	} else
		scan.xs_itup = &tuple;
	slot.tts_flags = TTS_FLAG_EMPTY;
	UT_ASSERT(IndexOnlyNext(&node) == &slot);
	*cached = node.ioss_VMBuffer;
	UT_ASSERT(!ios_locked && ios_evaluators == 0);
	UT_ASSERT_EQ(ios_enters, ios_exits);
	return !TTS_EMPTY(&slot);
}

UT_TEST(loaded_control_catalog_is_not_the_compiled_catalog)
{
	ios_reset(false);
	control.catalog_version_no = 17;
	UT_ASSERT_EQ(GetControlFileCatalogVersion(), 17);
	ControlFile = NULL;
	UT_ASSERT_EQ(GetControlFileCatalogVersion(), 0);
}

UT_TEST(empty_native_publication_reaches_ios_without_heap_fetch)
{
	for (int heap_index = 0; heap_index <= 1; heap_index++) {
		Buffer buffer = InvalidBuffer;
		ios_reset(true);
		UT_ASSERT(ios_run(&buffer, heap_index));
		UT_ASSERT_EQ(ios_fetches, 0);
		UT_ASSERT_EQ(ios_copies, 1);
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_unlocks, 1);
	}
}

UT_TEST(old_or_unknown_catalog_does_not_consult_a_positive_vm)
{
	for (int old = 0; old < 3; old++) {
		Buffer buffer = 2;
		ios_reset(true);
		control.catalog_version_no = old == 0 ? 0 : PGRAC_VM_QUALIFIED_CATALOG_VERSION - 1;
		if (old == 2)
			ControlFile = NULL;
		UT_ASSERT(!ios_run(&buffer, false));
		UT_ASSERT_EQ(ios_fetches, 1);
		UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters, 0);
		UT_ASSERT_EQ(buffer, 2);
	}
}

UT_TEST(disk_positive_current_clear_preserves_exact_heap_visibility)
{
	for (int visible = 0; visible <= 1; visible++) {
		Buffer buffer = InvalidBuffer;
		ios_reset(true);
		*PageGetContents(ios_current.data) = 0;
		ios_heap_visible = visible;
		UT_ASSERT_EQ(ios_run(&buffer, false), visible);
		UT_ASSERT_EQ(ios_fetches, 1);
		UT_ASSERT_EQ(ios_grants, 1);
		UT_ASSERT_EQ(ios_grants, ios_unlocks);
	}
}

UT_TEST(reused_vm_pin_must_obtain_current_s_for_every_tid)
{
	Buffer buffer = InvalidBuffer;
	ios_reset(true);
	UT_ASSERT(ios_run(&buffer, false));
	*PageGetContents(ios_current.data) = 0;
	ios_indexes = 0;
	UT_ASSERT(!ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_grants, 2);
	UT_ASSERT_EQ(ios_unlocks, 2);
	UT_ASSERT_EQ(ios_fetches, 1);
	UT_ASSERT_EQ(ios_reads, 1);
}

UT_TEST(space_membership_snapshot_cid_or_target_change_cannot_grant)
{
	for (int drift = 1; drift <= 5; drift++) {
		Buffer buffer = InvalidBuffer;
		ios_reset(true);
		ios_drift = drift;
		UT_ASSERT(!ios_run(&buffer, false));
		UT_ASSERT_EQ(ios_fetches, 1);
		UT_ASSERT_EQ(ios_grants, ios_unlocks);
	}
}

UT_TEST(unknown_or_unavailable_source_uses_original_heap_path)
{
	for (int missing = 0; missing < 8; missing++) {
		Buffer buffer = InvalidBuffer;
		ios_reset(true);
		switch (missing) {
		case 0: ios_pcm = false; break;
		case 1: ios_grant = false; break;
		case 2: ios_space = false; break;
		case 3: ios_admission = false; break;
		case 4: ios_snapshot_live = false; break;
		case 5: ios_missing = true; break;
		case 6: ((PageHeader)ios_current.data)->pd_block_scn = 0; break;
		case 7: *PageGetContents(ios_current.data) = VISIBILITYMAP_ALL_FROZEN; break;
		}
		UT_ASSERT(!ios_run(&buffer, false));
		UT_ASSERT_EQ(ios_fetches, 1);
		UT_ASSERT_EQ(ios_locked, false);
	}
}

UT_TEST(other_bit_clear_keeps_current_target_usable)
{
	Buffer buffer = InvalidBuffer;
	ios_reset(true);
	*PageGetContents(pages[1].data) |= VISIBILITYMAP_VALID_BITS << 2;
	CritSectionCount = 1;
	UT_ASSERT(visibilitymap_clear_locked(&relation, 1, 2, VISIBILITYMAP_VALID_BITS));
	CritSectionCount = 0;
	ios_current = pages[1];
	UT_ASSERT_EQ((uint8)*PageGetContents(ios_current.data), VISIBILITYMAP_VALID_BITS);
	UT_ASSERT(ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_fetches, 0);
}

UT_TEST(cancel_and_index_copy_error_release_the_vm_scope)
{
	for (int error = 1; error <= 2; error++) {
		Buffer buffer = InvalidBuffer;
		volatile bool caught = false;
		ios_reset(true);
		ios_error = error == 1 ? 1 : 0;
		ios_copy_error = error == 2;
		expecting_error = true;
		PG_TRY();
		{
			(void)ios_run(&buffer, false);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		expecting_error = false;
		UT_ASSERT(caught);
		UT_ASSERT(!ios_locked && ios_evaluators == 0);
		UT_ASSERT_EQ(ios_grants, ios_unlocks);
		UT_ASSERT_EQ(ios_enters, ios_exits);
	}
}

UT_TEST(nonshared_keeps_the_native_raw_shortcut)
{
	Buffer buffer = InvalidBuffer;
	ios_reset(true);
	cluster_enabled = false;
	cluster_shared_config = false;
	UT_ASSERT(ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters + ios_fetches, 0);
}

UT_TEST(lossy_qual_and_next_tid_run_after_vm_scope_release)
{
	for (int accepted = 0; accepted <= 1; accepted++) {
		Buffer buffer = InvalidBuffer;
		ios_reset(true);
		ios_lossy = true;
		ios_qual_result = accepted;
		UT_ASSERT_EQ(ios_run(&buffer, false), accepted);
		UT_ASSERT_EQ(ios_quals, 1);
		UT_ASSERT_EQ(ios_fetches, 0);
		UT_ASSERT_EQ(ios_indexes, accepted ? 1 : 2);
	}
}

UT_TEST(unassigned_catalog_never_grants_even_with_a_new_loaded_catalog)
{
	Buffer buffer = InvalidBuffer;
	ios_reset(true);
	control.catalog_version_no = PG_UINT32_MAX;
	UT_ASSERT(!ios_run(&buffer, false));
	UT_ASSERT_EQ(ios_fetches, 1);
	UT_ASSERT_EQ(ios_grants + ios_reads + ios_enters, 0);
}

UT_TEST(retry_setter_without_publication_proof_is_a_silent_noop)
{
	ResourceXAuxiliaryAcquireContext context = {0};
	Buffer buffer = 2;
	volatile bool caught = false, result = true;

	ios_reset(false);
	expecting_error = true;
	PG_TRY();
	{
		result = visibilitymap_set_retry_aware(&relation, 0, 1, InvalidXLogRecPtr,
			&buffer, 70, VISIBILITYMAP_VALID_BITS, &context, &identity);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	expecting_error = false;
	UT_ASSERT(!caught && !result);
	UT_ASSERT_EQ(buffer, 2);
	UT_ASSERT_EQ(inserts + lock_calls + edge_count, 0);
	UT_ASSERT(memcmp(pages, before, sizeof(pages)) == 0);
}

int
main(void)
{
	if (PGRAC_VM_QUALIFIED_CATALOG_VERSION == 0)
	{
		UT_PLAN(1);
		UT_RUN(unassigned_catalog_never_grants_even_with_a_new_loaded_catalog);
	}
	else
	{
		UT_PLAN(12);
		UT_RUN(loaded_control_catalog_is_not_the_compiled_catalog);
		UT_RUN(empty_native_publication_reaches_ios_without_heap_fetch);
		UT_RUN(old_or_unknown_catalog_does_not_consult_a_positive_vm);
		UT_RUN(disk_positive_current_clear_preserves_exact_heap_visibility);
		UT_RUN(reused_vm_pin_must_obtain_current_s_for_every_tid);
		UT_RUN(space_membership_snapshot_cid_or_target_change_cannot_grant);
		UT_RUN(unknown_or_unavailable_source_uses_original_heap_path);
		UT_RUN(other_bit_clear_keeps_current_target_usable);
		UT_RUN(cancel_and_index_copy_error_release_the_vm_scope);
		UT_RUN(nonshared_keeps_the_native_raw_shortcut);
		UT_RUN(lossy_qual_and_next_tid_run_after_vm_scope_release);
		UT_RUN(retry_setter_without_publication_proof_is_a_silent_noop);
	}
	UT_DONE();
	return ut_failed_count != 0;
}
