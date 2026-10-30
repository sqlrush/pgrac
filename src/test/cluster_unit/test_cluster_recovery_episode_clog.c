/*-------------------------------------------------------------------------
 * test_cluster_recovery_episode_clog.c
 *     Original native CLOG/SLRU through the complete replay episode.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_recovery_episode_clog.c
 * NOTES
 *     Preserve original fixture bodies. Shmem, locks and fault delivery are
 *     controlled; source ownership and ROOT reads use the caller's episode.
 *-------------------------------------------------------------------------
 */
#include "test_cluster_recovery_episode_clog_namespace.h"
int d3_episode_clog_main(void);
#define TEST_CLUSTER_EPISODE_CLOG
/* This bridge supplies the same original provider with a NULL-default route. */
#define TEST_CLUSTER_SIDE_NATIVE_CLOG
#include "test_cluster_clog_recovery.c"
#include "test_cluster_recovery_episode_clog.h"

/* A complete private PGPROC keeps the original status setter's group-update
 * test valid; its invalid xid selects the ordinary native lock path. */
static PGPROC episode_native_proc;
PGPROC *MyProc = &episode_native_proc;

#undef cluster_wal_inputs_relmap_source_current_v1
#undef cluster_wal_inputs_relmap_source_fail_v1
#undef cluster_wal_inputs_relmap_source_revalidate_v1
extern bool cluster_wal_inputs_relmap_source_current_v1(const ClusterRecoveryRelmapSourceV1 *source);
extern void cluster_wal_inputs_relmap_source_fail_v1(const ClusterRecoveryRelmapSourceV1 *source);
extern bool cluster_wal_inputs_relmap_source_revalidate_v1(const ClusterRecoveryRelmapSourceV1 *source);

static void
episode_clog_unlocked(void)
{
	for (unsigned i = 0; i < fixture_lock_count; ++i)
		UT_ASSERT(!fixture_locks[i].held);
	UT_ASSERT_EQ(InterruptHoldoffCount, 0);
}

bool
d3_episode_clog_cluster_wal_inputs_relmap_source_current_v1(
	const ClusterRecoveryRelmapSourceV1 *source)
{
	return cluster_wal_inputs_relmap_source_current_v1(source);
}

void
d3_episode_clog_cluster_wal_inputs_relmap_source_fail_v1(
	const ClusterRecoveryRelmapSourceV1 *source)
{
	cluster_wal_inputs_relmap_source_fail_v1(source);
}

bool
d3_episode_clog_cluster_wal_inputs_relmap_source_revalidate_v1(
	const ClusterRecoveryRelmapSourceV1 *source)
{
	episode_clog_unlocked();
	return cluster_wal_inputs_relmap_source_revalidate_v1(source);
}

size_t
test_episode_native_clog_bytes(void)
{
	return cluster_clog_relmap_base_custody_bytes_v1();
}

bool
test_episode_native_clog_read(const ClusterRecoveryRelmapSourceV1 *source,
	uint64 full, ClusterRelmapClogBaseV1 *out, void **custody)
{
	D3EpisodeClogHandleV1 *held = *custody;
	bool ok = cluster_clog_relmap_base_read_v1(source, full, out, &held);
	*custody = held;
	episode_clog_unlocked();
	return ok;
}

void
test_episode_native_clog_release(void **custody)
{
	D3EpisodeClogHandleV1 *held = *custody;
	cluster_clog_relmap_base_release_v1(&held);
	*custody = held;
	episode_clog_unlocked();
}

void
test_episode_native_clog_start(const char *root)
{
	UT_ASSERT_EQ(allocation_count, 0);
	strlcpy(fixture_root, root, sizeof(fixture_root));
	cluster_shared_data_dir = fixture_root;
	cluster_shared_config = enableFsync = true;
	cluster_node_id = fixture_origin = 0;
	MyAuxProcType = StartupProcess;
	MyBackendType = B_STARTUP;
	InRecovery = false;
	relmap_base_cf = false;
	relmap_base_interrupts = true;
	expect_error = fail_dirsync = fail_sync = false;
	InterruptHoldoffCount = CritSectionCount = 0;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	clog_recovery_shmem_init();
	MainLWLockArray = own_main_locks;
	LWLockInitialize(XactSLRULock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XactTruncationLock, LWTRANCHE_XACT_BUFFER);
	LWLockInitialize(XidGenLock, LWTRANCHE_XACT_BUFFER);
	SimpleLruInit(XactCtl, "Xact episode fixture", 4, CLOG_LSNS_PER_PAGE,
		XactSLRULock, "pg_xact", LWTRANCHE_XACT_BUFFER, SYNC_HANDLER_NONE);
	XactCtl->PagePrecedes = CLOGPagePrecedes;
}

void
test_episode_native_clog_stop(void)
{
	episode_clog_unlocked();
	fixture_idle();
	for (unsigned i = 0; i < allocation_count; ++i)
		free(allocations[i]);
	allocation_count = fixture_lock_count = 0;
	memset(ClogRecoveryCtl, 0, sizeof(ClogRecoveryCtl));
	memset(XactCtl, 0, sizeof(*XactCtl));
	ClogRecoveryLocks = NULL;
	MainLWLockArray = NULL;
	cluster_shared_data_dir = NULL;
}

void
test_episode_native_clog_error(bool on)
{
	expect_error = fail_dirsync = on;
}

int
test_episode_native_clog_fd(void *custody)
{
	D3EpisodeClogHandleV1 *held = custody;
	episode_clog_unlocked();
	return held == NULL ? -1 : held->fd;
}

/* Execute the original rmgr body, including its original SLRU write. */
void
test_episode_native_clog_redo(XLogReaderState *reader)
{
	clog_redo(reader);
	episode_clog_unlocked();
}

/* The outcome is controlled, but its native page/cache mutation is real.
 * This is not the XACT redo path or a transaction durability certificate. */
void
test_episode_native_clog_set_committed(TransactionId xid)
{
	UT_ASSERT_EQ(episode_native_proc.xid, InvalidTransactionId);
	TransactionIdSetTreeStatus(xid, 0, NULL, TRANSACTION_STATUS_COMMITTED, InvalidXLogRecPtr);
	episode_clog_unlocked();
}

/* The original held-fd reader does not flush the possibly dirty native cache.
 * Write it through the original checkpoint callback before observing disk. */
void
test_episode_native_clog_write_all(void)
{
	CheckPointCLOG();
	episode_clog_unlocked();
}

/* Test setup restores the original cache fields before this fixture ends. */
void
test_episode_native_clog_local_bounds(bool on, uint64 next, TransactionId oldest)
{
	static bool saved;
	static FullTransactionId prior_next;
	static TransactionId prior_oldest;

	UT_ASSERT(ShmemVariableCache != NULL);
	if (on && !saved) {
		prior_next = ShmemVariableCache->nextXid;
		prior_oldest = ShmemVariableCache->oldestClogXid;
		saved = true;
	}
	UT_ASSERT(saved);
	LWLockAcquire(XidGenLock, LW_EXCLUSIVE);
	ShmemVariableCache->nextXid = on ? FullTransactionIdFromU64(next) : prior_next;
	LWLockRelease(XidGenLock);
	LWLockAcquire(XactTruncationLock, LW_EXCLUSIVE);
	ShmemVariableCache->oldestClogXid = on ? oldest : prior_oldest;
	LWLockRelease(XactTruncationLock);
	if (!on)
		saved = false;
	episode_clog_unlocked();
}

/* Complete native producer bodies keep their original locks and no-op rules. */
void
test_episode_native_clog_advance_next(TransactionId xid)
{
	bool was_postmaster = IsUnderPostmaster;
	IsUnderPostmaster = true;
	UT_ASSERT(AmStartupProcess());
	fixture_native_advance_next_full_xid(xid);
	IsUnderPostmaster = was_postmaster;
	episode_clog_unlocked();
}

void
test_episode_native_clog_advance_oldest(TransactionId xid)
{
	fixture_native_advance_oldest_clog_xid(xid);
	episode_clog_unlocked();
}

/* Cache retirement does not mutate the native file or logical generation. */
void
test_episode_native_clog_discard_resident(void)
{
	uint64 before = 0, after = 0;

	episode_clog_unlocked();
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &before));
	clog_recovery_cache_discard(XactCtl);
	UT_ASSERT(clog_relmap_mutation_snapshot(0, &after));
	UT_ASSERT_EQ(before, after);
	episode_clog_unlocked();
}

static bool episode_native_record_only;

void
test_episode_native_clog_record_guard(bool (*current)(int, const XLogReaderState *))
{
	episode_clog_unlocked();
	fixture_native_record = current;
	episode_native_record_only = current != NULL;
}

/* The opt-in route is scoped to one actual original FULL cold call. Original
 * fixture authorization remains the NULL-default provider for every old case. */
static const ClusterRecoveryRelmapSourceV1 *episode_full_source;

bool
cluster_undo_recovery_cold_origin_authorized_v1(int origin)
{
	return !episode_native_record_only && authority
		&& (episode_full_source == NULL ? origin == fixture_origin
		: origin == episode_full_source->selected.source.claim.identity.origin_node_id
			&& cluster_wal_inputs_relmap_source_current_v1(episode_full_source));
}

bool
test_episode_native_clog_full_apply(const ClusterRecoveryRelmapSourceV1 *source,
	const xl_clog_zeropage_full *birth)
{
	ClusterSideProjectionOperationV1 op;
	bool prior_recovery = InRecovery;
	volatile bool ok = false;

	UT_ASSERT(episode_full_source == NULL);
	if (source == NULL || birth == NULL
		|| !cluster_wal_inputs_relmap_source_current_v1(source))
		return false;
	op = fixture_clog_record(false, birth->pageno);
	op.normalized_info = CLOG_ZEROPAGE_FULL;
	episode_full_source = source;
	InRecovery = true;
	PG_TRY(); {
		ok = cluster_clog_native_recovery_full_record_apply_v1(
			source->selected.source.claim.identity.origin_node_id, &op,
			(const uint8 *)birth, sizeof(*birth));
	} PG_FINALLY(); {
		episode_full_source = NULL;
		InRecovery = prior_recovery;
	} PG_END_TRY();
	episode_clog_unlocked();
	return ok;
}

/* Reuse the complete extracted callback with episode-local leaves. The old
 * standalone fixture's fixed SCN/timeline checks and all its tests stay intact. */
static TestEpisodeClogReplayStats *episode_callback_stats;
static uint64 episode_callback_scn;
static TimeLineID episode_callback_timeline;
static unsigned episode_callback_fault;
static bool episode_callback_foreign;

static void episode_callback_set_scn(uint64 scn)
{
	UT_ASSERT_EQ(scn, episode_callback_scn);
}

static void episode_callback_set_foreign(bool foreign)
{
	UT_ASSERT(!foreign);
	episode_callback_foreign = foreign;
}

static void episode_callback_touch(ClusterColdTouchedV1 *touched, XLogReaderState *reader)
{
	UT_ASSERT(touched != NULL && reader != NULL);
	++episode_callback_stats->touches;
}

static void episode_callback_apply(XLogReaderState *reader, XLogRecord *header,
	TimeLineID timeline)
{
	UT_ASSERT(header == &reader->record->header && timeline == episode_callback_timeline);
	UT_ASSERT(!episode_callback_foreign);
	clog_redo(reader);
	++episode_callback_stats->redos;
	/* Inject only at completion of the original write, never before redo. */
	fail_sync = episode_callback_fault == 1;
	fail_dirsync = episode_callback_fault == 2;
}

#define cluster_typed_replay_apply_native episode_clog_original_native_callback
#define cluster_recovery_merge_set_scn episode_callback_set_scn
#define cluster_recovery_merge_set_apply_foreign episode_callback_set_foreign
#define cluster_cold_touched_add_record_v1 episode_callback_touch
#define ApplyWalRecord episode_callback_apply
#include "test_cluster_clog_native_callback.inc"
#undef ApplyWalRecord
#undef cluster_cold_touched_add_record_v1
#undef cluster_recovery_merge_set_apply_foreign
#undef cluster_recovery_merge_set_scn
#undef cluster_typed_replay_apply_native

void
test_episode_native_clog_replay(XLogReaderState *reader,
	const ClusterRecoveryRelmapSourceV1 *source, unsigned sync_fault,
	TestEpisodeClogReplayStats *stats)
{
	ClusterColdTypedReplay replay = { .apply_foreign = false };
	bool old_recovery = InRecovery, old_error = expect_error;
	bool old_sync = fail_sync, old_dirsync = fail_dirsync;
	unsigned file_syncs = syncs, directory_syncs = dirsyncs;

	UT_ASSERT(sync_fault <= 2 && stats != NULL && episode_callback_stats == NULL);
	UT_ASSERT(source != NULL && episode_full_source == NULL
		&& cluster_wal_inputs_relmap_source_current_v1(source));
	replay.replayTLI = source->selected.source.timeline;
	memset(stats, 0, sizeof(*stats));
	episode_callback_stats = stats;
	episode_callback_scn = reader->record->header.xl_scn;
	episode_callback_timeline = replay.replayTLI;
	episode_callback_fault = sync_fault;
	episode_callback_foreign = false;
	/* Use A's existing source-scoped fixture authority for this call only. */
	episode_full_source = source;
	InRecovery = expect_error = true;
	PG_TRY(); {
		episode_clog_original_native_callback(reader, &replay);
		stats->returned = true;
	} PG_FINALLY(); {
		episode_full_source = NULL;
		InRecovery = old_recovery;
		expect_error = old_error;
		fail_sync = old_sync; fail_dirsync = old_dirsync;
		stats->file_syncs = syncs - file_syncs;
		stats->directory_syncs = dirsyncs - directory_syncs;
		episode_callback_stats = NULL;
		episode_clog_unlocked();
	} PG_END_TRY();
}

/* This added test route uses the real Startup record owner. Keep the old
 * source-scoped replay API/body above intact for all existing episode cases. */
void
test_episode_native_clog_replay_record(XLogReaderState *reader, TimeLineID timeline,
	unsigned sync_fault, TestEpisodeClogReplayStats *stats)
{
	ClusterColdTypedReplay replay = { .apply_foreign = false, .replayTLI = timeline };
	bool old_recovery = InRecovery, old_error = expect_error;
	bool old_sync = fail_sync, old_dirsync = fail_dirsync;
	bool old_truncate = fixture_native_truncate;
	unsigned file_syncs = syncs, directory_syncs = dirsyncs;

	UT_ASSERT(sync_fault <= 2 && stats != NULL && episode_callback_stats == NULL);
	UT_ASSERT(episode_native_record_only && episode_full_source == NULL);
	memset(stats, 0, sizeof(*stats));
	episode_callback_stats = stats;
	episode_callback_scn = reader->record->header.xl_scn;
	episode_callback_timeline = timeline;
	episode_callback_fault = sync_fault;
	episode_callback_foreign = false;
	InRecovery = expect_error = true;
	/* Select the original native horizon provider only for this record. */
	if ((XLogRecGetInfo(reader) & ~XLR_INFO_MASK) == CLOG_TRUNCATE_FULL)
		fixture_native_truncate = true;
	PG_TRY(); {
		episode_clog_original_native_callback(reader, &replay);
		stats->returned = true;
	} PG_FINALLY(); {
		InRecovery = old_recovery;
		expect_error = old_error;
		fail_sync = old_sync; fail_dirsync = old_dirsync;
		fixture_native_truncate = old_truncate;
		stats->file_syncs = syncs - file_syncs;
		stats->directory_syncs = dirsyncs - directory_syncs;
		episode_callback_stats = NULL;
		episode_clog_unlocked();
	} PG_END_TRY();
}
