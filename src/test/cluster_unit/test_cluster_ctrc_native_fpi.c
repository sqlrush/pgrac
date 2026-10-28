/*-------------------------------------------------------------------------
 *
 * test_cluster_ctrc_native_fpi.c
 *     Native CTRC page custody through canonical whole-page preparation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_ctrc_native_fpi.c
 *
 * NOTES
 *     Real native owners/PAGE/reader/result/terminal/projector/Generic code.
 *     Start/GES, accepted LMON result and transport/token are fixture leaves.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

struct ClusterRecoveryStartV1;
struct ClusterColdTopTxCensusEntryV1;
struct ClusterRecoveryTTAbortDerivationV1;
int ctrc_native_fpi_root_fixture_main(int argc, char **argv);
bool ctrc_native_fpi_original_c0(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterRecoveryTTAbortDerivationV1 *out);
#define main ctrc_native_fpi_root_fixture_main
#define ClusterColdRecoveryTxC0Candidate ctrc_native_fpi_original_c0
#define cluster_thread_recovery_pin_projection ctrc_native_fpi_fixture_cluster_thread_recovery_pin_projection
#define cluster_thread_recovery_projection_current ctrc_native_fpi_fixture_cluster_thread_recovery_projection_current
#define cluster_recovery_checkpoint_current_v1 ctrc_native_fpi_fixture_cluster_recovery_checkpoint_current_v1
#define cluster_recovery_checkpoint_observe_v1 ctrc_native_fpi_fixture_cluster_recovery_checkpoint_observe_v1
#define cluster_recovery_stamp_worker_begin_v1 ctrc_native_fpi_fixture_cluster_recovery_stamp_worker_begin_v1
#define cluster_wal_writer_check native_insert_original_writer_check
#define XLogFlush native_insert_original_flush
#define ClusterColdRecoveryCtrcWorkCurrent native_insert_original_work_current
#define smgrread native_install_original_read
#define GetFlushRecPtr native_install_original_flush_ptr
#include "test_cluster_control_root.c"
#undef GetFlushRecPtr
#undef smgrread
#undef ClusterColdRecoveryCtrcWorkCurrent
#undef XLogFlush
#undef cluster_wal_writer_check
#undef cluster_thread_recovery_pin_projection
#undef cluster_thread_recovery_projection_current
#undef cluster_recovery_checkpoint_current_v1
#undef cluster_recovery_checkpoint_observe_v1
#undef cluster_recovery_stamp_worker_begin_v1

#undef main
#undef ClusterColdRecoveryTxC0Candidate

bool ClusterColdRecoveryCtrcWorkCurrent(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work);
#include "cluster/cluster_itl.h"
#include "cluster/cluster_page_buffer_ctrc.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/storage/cluster_undo_alloc.h"
bool ClusterColdRecoveryTxC0Candidate(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdTopTxCensusEntryV1 *subject,
	struct ClusterRecoveryTTAbortDerivationV1 *out);
bool cluster_thread_recovery_pin_projection(uint16 thread, uint64 epoch);
bool cluster_thread_recovery_projection_current(uint16 thread, uint64 epoch,
	ClusterControlRootReadToken *token, uint64 *tail, uint64 *lower,
	uint64 *lifecycle, uint32 *tail_tli, uint32 *checkpoint_tli);
bool cluster_recovery_checkpoint_current_v1(const struct ClusterRecoveryCheckpointScopeV1 *scope,
	ClusterWalWriterToken *writer);
bool cluster_recovery_checkpoint_observe_v1(ClusterRecoveryCheckpointObservationV1 *out);
bool cluster_recovery_stamp_worker_begin_v1(uint16 thread, uint64 attempt);
static void *native_stamp_alloc(Size size);
static void native_stamp_free(void *pointer);
bool native_stamp_original_durable(const ClusterRecoveryStampHandleV1 *handle);
#define cluster_recovery_stamp_durable_v1 native_stamp_original_durable
#define palloc native_stamp_alloc
#define pfree native_stamp_free
#include "../../backend/cluster/cluster_recovery_plan.c"
#undef palloc
#undef pfree
#undef cluster_recovery_stamp_durable_v1

static ClusterRecoveryPlanShmem native_fpi_region;
static PGAlignedBlock native_fpi_top_storage;
#define native_fpi_top ((ClusterColdTopTxCensusV1 *)native_fpi_top_storage.data)
static ClusterRecoveryUndoEntry native_fpi_physical[2];
static CtrcReadNativeFixture *native_fpi_live;
static bool native_fpi_proven;
static unsigned native_fpi_tokens, native_fpi_token_fault;
static unsigned native_fpi_allocs, native_fpi_frees;
static void *native_fpi_allocation;
static bool native_fpi_alloc_error;
static bool native_fpi_complete_writer;
static bool native_fpi_unretained_writer;
static bool native_install_setup;
static bool native_step_active;
static bool native_step_foreign_blocker;
static void *native_step_allocations[4];
static unsigned native_step_allocation_depth;
static void native_step_hash_gap(void);
static void *native_stamp_allocation;
static unsigned native_stamp_allocs, native_stamp_frees;
static bool native_stamp_alloc_error;

/* Single-threaded fixture: any actual spin contention is unexpected. */
int
s_lock(volatile slock_t *lock, const char *file, int line, const char *func)
{
	(void)lock; (void)file; (void)line; (void)func;
	UT_ASSERT(false);
	return 0;
}

void *ctrc_native_fpi_alloc0(MemoryContext context, Size size);
void ctrc_native_fpi_free(void *pointer);

void *
ctrc_native_fpi_alloc0(MemoryContext context, Size size)
{
	if (!native_step_active) UT_ASSERT(native_fpi_allocation == NULL);
	else UT_ASSERT(native_step_allocation_depth < lengthof(native_step_allocations));
	if (native_fpi_alloc_error) pg_re_throw();
	native_fpi_allocs++;
	native_fpi_allocation = palloc0(size);
	if (native_step_active) native_step_allocations[native_step_allocation_depth++] = native_fpi_allocation;
	return native_fpi_allocation;
}

void
ctrc_native_fpi_free(void *pointer)
{
	UT_ASSERT(pointer != NULL && pointer == native_fpi_allocation);
	native_fpi_frees++; native_fpi_allocation = NULL;
	if (native_step_active) {
		UT_ASSERT(native_step_allocation_depth != 0);
		UT_ASSERT(native_step_allocations[--native_step_allocation_depth] == pointer);
		if (native_step_allocation_depth != 0)
			native_fpi_allocation = native_step_allocations[native_step_allocation_depth - 1];
	}
	pfree(pointer);
}

bool
ClusterColdRecoveryStartOutputDistinct(const struct ClusterRecoveryStartV1 *start,
	const void *out, Size bytes)
{
	uintptr_t a = (uintptr_t)out, b = (uintptr_t)&tx_inputs_typed;
	return start == tx_inputs_start && out != NULL && bytes != 0
		&& (a <= b ? b - a >= bytes : a - b >= sizeof(tx_inputs_typed));
}

bool
ClusterColdRecoveryTxTopProofCurrent(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdTopTxCensusEntryV1 *subject)
{
	return native_fpi_proven && ClusterColdRecoveryTxCurrent(start)
		&& subject == &native_fpi_top->entries[0] && subject->loser_proven;
}

bool
ClusterColdRecoveryTxEffectsCurrent(const struct ClusterRecoveryStartV1 *start)
{
	return native_fpi_proven && ClusterColdRecoveryTxCurrent(start);
}

bool
ClusterColdRecoveryTxC0Candidate(const struct ClusterRecoveryStartV1 *start,
	const ClusterColdTopTxCensusEntryV1 *subject, ClusterRecoveryTTAbortDerivationV1 *out)
{
	ClusterRecoveryTTAbortV1 shape = tx_inputs_candidate.record;
	if (!ClusterColdRecoveryTxTopProofCurrent(start, subject)) return false;
	shape.before = subject->input.slot;
	return cluster_recovery_tt_abort_derive_fields_v1(&shape, 10000, true, false, 0, 1, out);
}

SCN
cluster_scn_advance(void)
{
	native_fpi_tokens++;
	switch (native_fpi_token_fault) {
	case 1: test_input_current = false; break;
	case 2: native_fpi_physical[1].segment_generation++; break;
	case 3: ctrc_inputs_work.entries[1].slot.first_undo_block = uba_encode(1, 99, 3, 100); break;
	case 4: ctrc_inputs_work.counts.subject_count++; break;
	case 5: native_fpi_region.checkpoint_owner.tx_accepted_generation++; break;
	case 6: native_fpi_region.checkpoint_owner.tx_result.processed_count++; break;
	case 7: native_fpi_live->space.sequence++; break;
	case 8: pg_re_throw(); break;
	case 9: cluster_wal_inputs_release_v1(&native_fpi_live->page.replay.inputs); break;
	}
	return 100000 + native_fpi_tokens;
}

void
cluster_scn_observe(SCN scn)
{
	UT_ASSERT(SCN_VALID(scn));
}

static void
native_fpi_rebind(CtrcReadNativeFixture *f)
{
	f->page.proof = ctrc_replay_stable_build(&f->page.replay);
	UT_ASSERT_EQ(f->page.proof.detail, RF_PAGE_PROOF_DETAIL_OK);
	f->page.target.stable_base = f->page.proof.proof;
	f->page.page = ctrc_page_build(&f->page);
	UT_ASSERT_EQ(f->page.page.verdict, RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f->page.page.authority), 0);
}

static void
native_fpi_fixture(CtrcReadNativeFixture *f, unsigned itls, bool checksums)
{
	PGAlignedBlock image = {0}; PageHeader header = (PageHeader)image.data;
	ClusterItlSlotData *slots;
	ClusterControlRootFileToken root;
	ctrc_read_native_fixture(f, checksums); native_fpi_live = f;
	rf_page_authority_ctrc_replays_destroy_v1(&f->page.page.authority);
	rf_page_stable_base_proof_destroy_v1(&f->page.proof.proof);
	root = f->page.replay.owners[0].root;
	cluster_wal_inputs_release_v1(&f->page.replay.inputs); ctrc_ir_original_scope = NULL;
	if (native_fpi_complete_writer) {
		/* Installation needs a representable local WAL writer, separately
		 * from the selected failed replay origins (including thread 128). */
		if (native_install_setup) ctrc_inputs_owner.writer.ref = tx_inputs_typed.sources[1];
		if (native_fpi_unretained_writer) {
			ctrc_inputs_owner.writer.ref.claim.identity.origin_node_id = 1;
			ctrc_inputs_owner.writer.ref.claim.identity.origin_thread_id = 2;
		}
		ctrc_inputs_owner.owner_boot = ctrc_inputs_owner.writer.ref.claim.identity.origin_owner_incarnation;
		ctrc_inputs_owner.writer.startup_first_lsn = 0x100;
	}
	memset(&native_fpi_top_storage, 0, sizeof(native_fpi_top_storage));
	memset(native_fpi_physical, 0, sizeof(native_fpi_physical));
	native_fpi_top->plan = tx_inputs_typed.plan;
	ctrc_inputs_work.top_tx_census = tx_inputs_typed.top_tx_census = native_fpi_top;
	ctrc_inputs_work.counts.subject_count = ctrc_inputs_work.counts.unresolved_count = itls ? 2 : 0;
	tx_inputs_typed.census_root = root; tx_inputs_typed.census_root_captured = true;
	for (unsigned i = 0; i < (itls ? 2 : 0); i++) {
		ClusterColdTtSlotInputV1 *subject = &ctrc_inputs_work.entries[i];
		*subject = tx_inputs_subject.input;
		subject->key.slot_offset += i; subject->key.xid += i; subject->key.slot_wrap += i;
		subject->slot.xid = subject->key.xid; subject->slot.wrap = subject->key.slot_wrap;
		subject->slot.status = i ? TT_SLOT_COMMITTED : TT_SLOT_ACTIVE;
		subject->slot.commit_scn = i ? 900 : InvalidScn;
		subject->slot.first_undo_block = uba_encode(subject->key.segment_id, 40, subject->key.slot_offset, 100);
		native_fpi_physical[i].owner_instance = subject->key.owner_instance;
		native_fpi_physical[i].segment_id = subject->key.segment_id;
		native_fpi_physical[i].segment_generation = subject->key.segment_generation;
		native_fpi_physical[i].slot_offset = subject->key.slot_offset;
		native_fpi_physical[i].slot = subject->slot;
		if (!i) native_fpi_physical[i].slot.status = TT_SLOT_ABORTED;
	}
	if (itls) {
		native_fpi_top->count = native_fpi_top->proven_count = 1;
		native_fpi_top->entries[0].input = ctrc_inputs_work.entries[0];
		native_fpi_top->entries[0].loser_proven = true;
	}
	UT_ASSERT_EQ(ctrc_inputs_begin(&f->page.replay.inputs), 0);
	ctrc_ir_original_scope = f->page.replay.inputs;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(f->page.replay.inputs), 0);
	for (unsigned i = 0; i < 2; i++)
		f->page.replay.owners[i] = cluster_wal_inputs_ctrc_replays_source_v1(f->page.replay.inputs, 1 - i);
	native_fpi_rebind(f);
	memset(&native_fpi_region, 0, sizeof(native_fpi_region));
	cluster_recovery_plan_shmem = &native_fpi_region;
	checkpoint_registered_start = NULL; memset(&checkpoint_registered_owner, 0, sizeof(checkpoint_registered_owner));
	native_fpi_proven = true;
	UT_ASSERT(cluster_recovery_checkpoint_register_v1(tx_inputs_start));
	UT_ASSERT(cluster_recovery_tx_result_begin_v1(tx_inputs_start));
	if (itls) UT_ASSERT(cluster_recovery_tx_result_step_v1(tx_inputs_start, 0));
	UT_ASSERT(cluster_recovery_tx_result_finish_v1(tx_inputs_start));
	/* Controlled LMON acceptance of this exact original generation. */
	native_fpi_region.checkpoint_owner.tx_accepted_generation = native_fpi_region.checkpoint_owner.tx_result.result_generation;
	header->pd_flags = PD_HAS_ITL; header->pd_special = BLCKSZ - CLUSTER_ITL_SPECIAL_SIZE;
	header->pd_lower = SizeOfPageHeaderData; header->pd_upper = header->pd_special;
	header->pd_block_scn = f->page.target.expected_result.mutation_token;
	PageSetPageSizeAndVersion((Page)image.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	slots = ClusterPageGetItlSlots((Page)image.data);
	for (unsigned i = 0; i < itls; i++) {
		const ClusterColdTtSlotInputV1 *subject = &ctrc_inputs_work.entries[i % 2];
		slots[i].xid = subject->key.xid; slots[i].wrap = 3 + i;
		slots[i].flags = i % 2 ? ITL_FLAG_ACTIVE : ITL_FLAG_LOCK_ONLY_ACTIVE;
		slots[i].undo_segment_head = uba_encode(subject->key.segment_id, 12, subject->key.slot_offset, 132);
	}
	ctrc_read_native_write(f, MAIN_FORKNUM, 7, &image);
	native_fpi_tokens = native_fpi_token_fault = 0;
	native_fpi_allocs = native_fpi_frees = 0; native_fpi_allocation = NULL; native_fpi_alloc_error = false;
}

static void
native_fpi_finish(CtrcReadNativeFixture *f)
{
	UT_ASSERT(native_fpi_allocation == NULL); UT_ASSERT_EQ(native_fpi_allocs, native_fpi_frees);
	UT_ASSERT(native_stamp_allocation == NULL); UT_ASSERT_EQ(native_stamp_allocs, native_stamp_frees);
	cluster_recovery_checkpoint_revoke_v1(tx_inputs_start);
	ctrc_read_native_finish(f); native_fpi_live = NULL;
}

static RfPageCtrcFpiV1
native_fpi_prepare(CtrcReadNativeFixture *f, Size count, Size budget)
{
	return rf_page_recovery_ctrc_replays_itl_fpi_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		f->page.page.authority, &f->space, 7, &f->page.target.expected_result,
		count ? native_fpi_physical : NULL, count, budget);
}

static void
native_fpi_refused(CtrcReadNativeFixture *f)
{
	static const PGAlignedBlock zero;
	RfPageCtrcFpiV1 result = native_fpi_prepare(f, 2, 1024 * 1024);
	UT_ASSERT(!result.ready && !result.rewritten && result.frame.result_token == 0);
	UT_ASSERT(memcmp(&result.frame.image, &zero, sizeof(zero)) == 0);
	UT_ASSERT(native_fpi_allocation == NULL); UT_ASSERT_EQ(native_fpi_allocs, native_fpi_frees);
	UT_ASSERT(native_stamp_allocation == NULL); UT_ASSERT_EQ(native_stamp_allocs, native_stamp_frees);
}

UT_TEST(test_native_fpi_projects_entire_page_from_original_c0_and_commit)
{
	for (unsigned checksums = 0; checksums < 2; checksums++)
		for (unsigned count = 1; count <= CLUSTER_ITL_INITRANS_DEFAULT; count++) {
			CtrcReadNativeFixture f; RfPageCtrcFpiV1 result;
			PGAlignedBlock disk, after; unsigned releases, grants, cf;
			native_fpi_fixture(&f, count, checksums != 0);
			UT_ASSERT_EQ(pread(f.main_fd, disk.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
			releases = test_tx_ir_releases; grants = test_tx_ir_grants; cf = test_cf_lock_calls;
			result = native_fpi_prepare(&f, 2, 1024 * 1024);
			UT_ASSERT(result.ready && result.rewritten); UT_ASSERT_EQ(native_fpi_tokens, 1);
			UT_ASSERT_EQ(result.frame.owner, GENERIC_XLOG_CTRC_ITL);
			UT_ASSERT_EQ(result.frame.version.before.mutation_token, f.page.target.expected_result.mutation_token);
			UT_ASSERT_EQ(result.frame.result_token, 100001); UT_ASSERT_EQ(ctrc_read_native_reads, 5);
			for (unsigned i = 0; i < count; i++) {
				ClusterItlSlotData *slot = &ClusterPageGetItlSlots((Page)result.frame.image.data)[i];
				UT_ASSERT_EQ(slot->flags, i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED);
				UT_ASSERT_EQ(slot->commit_scn, i % 2 ? 900 : InvalidScn); UT_ASSERT_EQ(slot->wrap, 3 + i);
			}
			UT_ASSERT_EQ(((PageHeader)disk.data)->pd_block_scn, f.page.target.expected_result.mutation_token);
			UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(test_tx_ir_grants, grants);
			UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
			UT_ASSERT_EQ(pread(f.main_fd, after.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
			UT_ASSERT(memcmp(disk.data, after.data, BLCKSZ) == 0); native_fpi_finish(&f);
		}
}

UT_TEST(test_native_fpi_empty_and_already_terminal_do_not_mint_wal_frame)
{
	for (unsigned count = 0; count <= 2; count += 2) {
		CtrcReadNativeFixture f; PGAlignedBlock disk; RfPageCtrcFpiV1 result;
		native_fpi_fixture(&f, count, true);
		if (count) {
			UT_ASSERT_EQ(pread(f.main_fd, disk.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
			for (unsigned i = 0; i < count; i++) {
				ClusterItlSlotData *slot = &ClusterPageGetItlSlots((Page)disk.data)[i];
				slot->flags = i % 2 ? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_ABORTED;
				slot->commit_scn = i % 2 ? 900 : InvalidScn;
			}
			ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &disk);
		}
		result = native_fpi_prepare(&f, count, count ? 1024 * 1024 : 0);
		UT_ASSERT(result.ready && !result.rewritten && result.frame.result_token == 0);
		UT_ASSERT_EQ(native_fpi_tokens, 0); UT_ASSERT_EQ(ctrc_read_native_reads, 5);
		native_fpi_finish(&f);
	}
}

UT_TEST(test_native_fpi_bad_count_budget_or_terminal_refuses_before_smgr)
{
	for (unsigned fault = 0; fault < 8; fault++) {
		CtrcReadNativeFixture f; RfPageCtrcFpiV1 result;
		native_fpi_fixture(&f, 2, true);
		switch (fault) {
		case 0: native_fpi_region.checkpoint_owner.tx_accepted_generation = 0; break;
		case 1: native_fpi_region.checkpoint_owner.tx_result.unresolved_count++; break;
		case 2: native_fpi_physical[0].slot.status = TT_SLOT_ACTIVE; break;
		case 3: native_fpi_physical[1].segment_generation++; break;
		case 4: native_fpi_proven = false; break;
		case 5: ctrc_inputs_work.counts.unqualified_count++; break;
		case 6: break;
		case 7: break;
		}
		result = native_fpi_prepare(&f, fault == 6 ? 1 : 2, fault == 7 ? 1 : 1024 * 1024);
		UT_ASSERT(!result.ready); UT_ASSERT_EQ(ctrc_read_native_reads + ctrc_read_native_opens + native_fpi_tokens, 0);
		native_fpi_finish(&f);
	}
}

UT_TEST(test_native_fpi_input_drift_at_each_read_never_publishes)
{
	for (unsigned kind = 0; kind < 3; kind++)
		for (unsigned read = 1; read <= 5; read++) {
			CtrcReadNativeFixture f;
			native_fpi_fixture(&f, 8, true);
			ctrc_read_native_drift_at = read; ctrc_read_native_drift_kind = kind == 0 ? 0 : kind == 1 ? 4 : 19;
			native_fpi_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, read);
			UT_ASSERT_EQ(native_fpi_tokens, 0); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
			native_fpi_finish(&f);
		}
}

UT_TEST(test_native_fpi_rechecks_all_original_inputs_after_token_producer)
{
	for (unsigned fault = 1; fault <= 7; fault++) {
		CtrcReadNativeFixture f;
		native_fpi_fixture(&f, 8, false); native_fpi_token_fault = fault;
		native_fpi_refused(&f); UT_ASSERT_EQ(native_fpi_tokens, 1);
		UT_ASSERT_EQ(ctrc_read_native_reads, 5); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		native_fpi_finish(&f);
	}
}

UT_TEST(test_native_fpi_error_frees_only_private_snapshot)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		CtrcReadNativeFixture f; volatile bool caught = false; unsigned releases;
		native_fpi_fixture(&f, 8, true); releases = test_tx_ir_releases;
		if (!fault) native_fpi_alloc_error = true;
		else if (fault == 6) native_fpi_token_fault = 8;
		else ctrc_read_native_throw_at = fault;
		PG_TRY(); { (void)native_fpi_prepare(&f, 2, 1024 * 1024); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT(native_fpi_allocation == NULL);
		UT_ASSERT_EQ(native_fpi_allocs, native_fpi_frees); UT_ASSERT_EQ(test_tx_ir_releases, releases);
		UT_ASSERT(inputs_pin_held && ctrc_set_held == 2); native_fpi_finish(&f);
	}
}

UT_TEST(test_native_fpi_retired_scope_cannot_publish_or_release_borrowed_owner)
{
	CtrcReadNativeFixture f;
	native_fpi_fixture(&f, 8, true); native_fpi_token_fault = 9;
	native_fpi_refused(&f); UT_ASSERT(f.page.replay.inputs == NULL && !inputs_pin_held && ctrc_set_held == 0);
	native_fpi_refused(&f); UT_ASSERT_EQ(native_fpi_tokens, 1); native_fpi_finish(&f);
}

bool native_insert_original_installed_writer(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation);
#define ClusterStartupInstalledRecoveryWriterV1 native_insert_original_installed_writer
#include "test_cluster_ctrc_native_stamp.inc"
#undef ClusterStartupInstalledRecoveryWriterV1
void native_install_original_held(void (*callback)(LWLock *, LWLockMode, void *), void *context);
bool native_install_original_writer_installed(ClusterWalWriterToken *writer,
	uint8 operation_uuid[16], uint64 *generation);
#define recovery_fpi_hash_create native_install_original_hash_create
#define ForEachLWLockHeldByMe native_install_original_held
#define cluster_wal_writer_check native_install_original_writer_current
#define ClusterStartupInstalledRecoveryWriterV1 native_install_original_writer_installed
#include "test_cluster_ctrc_native_insert.inc"
#undef recovery_fpi_hash_create
#undef ForEachLWLockHeldByMe
#undef cluster_wal_writer_check
#undef ClusterStartupInstalledRecoveryWriterV1
#include "test_cluster_ctrc_native_install.inc"
#include "test_cluster_ctrc_native_step.inc"
#include "test_cluster_ctrc_native_graph_step.inc"
#include "test_cluster_ctrc_native_page_fd.inc"
#include "test_cluster_ctrc_native_durable.inc"
#include "test_cluster_ctrc_native_writer_retention.inc"
#include "test_cluster_ctrc_native_unchanged_sync.inc"
#include "test_cluster_ctrc_native_cache.inc"
#include "test_cluster_ctrc_native_unchanged_cache.inc"
#include "test_cluster_ctrc_native_held_fd.inc"
#include "test_cluster_ctrc_native_readback.inc"
#include "test_cluster_ctrc_native_completed.inc"
#include "test_cluster_ctrc_native_unchanged_custody.inc"

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	if (getenv("PGRAC_PRE2_TEST_NATIVE_UNCHANGED_CUSTODY") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_native_unchanged_custody_retains_exact_raw_pages_and_same_fd);
		UT_RUN(test_native_unchanged_custody_refuses_copies_and_bad_entry_before_io);
		UT_RUN(test_native_unchanged_custody_refuses_changed_valid_main_and_space_bytes);
		UT_RUN(test_native_unchanged_custody_rechecks_binding_after_each_read);
		UT_RUN(test_native_unchanged_custody_wait_and_error_keep_fd_and_original_debt);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_COMPLETED") != NULL) {
		UT_PLAN(5);
		UT_RUN(test_native_completed_readback_uses_original_page_and_same_fds_after_f4_clear);
		UT_RUN(test_native_completed_readback_refuses_clones_lost_binding_and_pending_api);
		UT_RUN(test_native_completed_readback_refuses_changed_main_or_space_without_reinstall);
		UT_RUN(test_native_completed_readback_rechecks_binding_after_each_protected_read);
		UT_RUN(test_native_completed_readback_collision_and_error_keep_original_storage);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_READBACK") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_native_readback_repromotes_same_page_and_reads_original_fds_only);
		UT_RUN(test_native_readback_never_reinstalls_before_or_changed_result_bytes);
		UT_RUN(test_native_readback_rechecks_immutable_inputs_after_every_read);
		UT_RUN(test_native_readback_page_collision_waits_without_failing_window);
		UT_RUN(test_native_readback_rejects_lost_fd_writer_flush_and_bad_entry);
		UT_RUN(test_native_readback_error_keeps_storage_and_original_window_debt);
		UT_RUN(test_native_readback_busy_cache_waits_without_invalidating_a_buffer);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_HELD_FD") != NULL) {
		UT_PLAN(7);
		UT_RUN(test_native_held_fd_retains_same_durable_storage);
		UT_RUN(test_native_held_fd_uses_long_lived_context_and_restores_caller);
		UT_RUN(test_native_held_fd_preserves_four_older_close_contracts);
		UT_RUN(test_native_held_fd_does_not_transfer_other_phases);
		UT_RUN(test_native_held_fd_late_refusal_closes_and_keeps_debt);
		UT_RUN(test_native_held_fd_error_closes_and_keeps_debt);
		UT_RUN(test_native_held_fd_open_error_restores_caller_context);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_UNCHANGED_CACHE") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_native_unchanged_cache_retires_only_through_original_page_gate);
		UT_RUN(test_native_unchanged_cache_busy_waits_before_sync_without_retiring_duties);
		UT_RUN(test_native_unchanged_cache_refusal_never_reports_synced);
		UT_RUN(test_native_unchanged_cache_error_closes_only_private_storage);
		UT_RUN(test_native_unchanged_cache_reappearance_refuses_after_sync_or_full_postread);
		UT_RUN(test_native_unchanged_cache_preserves_all_observation_only_entries);
		UT_DONE(); return ut_failed_count != 0;
	}

	if (getenv("PGRAC_PRE2_TEST_NATIVE_CACHE") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_native_cache_retires_exact_predecessor_before_bufferless_fpi);
		UT_RUN(test_native_cache_busy_waits_without_wal_or_duty_retirement);
		UT_RUN(test_native_cache_refusal_and_error_keep_original_owners);
		UT_RUN(test_native_cache_reappearance_keeps_exact_inserted_window);
		UT_RUN(test_native_cache_noop_keeps_matching_cache_and_old_components);
		UT_RUN(test_native_cache_stale_canonical_refuses_before_cache_mutation);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_UNCHANGED_SYNC") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_native_unchanged_syncs_same_fd_and_full_image_without_wal);
		UT_RUN(test_native_unchanged_sync_rechecks_original_inputs_after_sync_and_each_read);
		UT_RUN(test_native_unchanged_sync_io_failure_error_and_close_keep_original_owners);
		UT_RUN(test_native_unchanged_sync_refuses_changed_full_main_or_space_bytes);
		UT_RUN(test_native_unchanged_sync_preserves_old_entries_and_rewritten_fpi_debt);
		UT_RUN(test_native_unchanged_sync_requires_original_entry_guards_and_canonical_image);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_WRITER_RETENTION") != NULL) {
		UT_PLAN(1);
		UT_RUN(test_native_fpi_refuses_writer_outside_original_retention);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_FPI_DURABLE") != NULL) {
		UT_PLAN(8);
		UT_RUN(test_native_durable_marks_under_original_page_and_fd);
		UT_RUN(test_native_durable_snapshot_is_exact_and_read_only);
		UT_RUN(test_native_durable_transition_refusal_keeps_inserted_debt);
		UT_RUN(test_native_durable_post_transition_drift_fails_exact_window);
		UT_RUN(test_native_durable_error_keeps_original_phase_and_debt);
		UT_RUN(test_native_durable_close_failure_retains_durable_failed_debt);
		UT_RUN(test_native_durable_requires_successful_sync_and_postread);
		UT_RUN(test_native_durable_preserves_old_entry_and_noop_contracts);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_GRAPH_STEP") != NULL) {
		UT_PLAN(4);
		UT_RUN(test_native_graph_step_accepts_only_storage_checksum_normalization);
		UT_RUN(test_native_graph_step_refuses_terminal_bytes_lsn_and_origin_mismatch);
		UT_RUN(test_native_graph_step_fixed_input_cannot_change_during_wal_preparation);
		UT_RUN(test_native_graph_step_error_keeps_original_inserted_debt);
		UT_DONE(); return ut_failed_count != 0;
	}
	if (getenv("PGRAC_PRE2_TEST_NATIVE_PAGE_FD") != NULL) {
		UT_PLAN(6);
		UT_RUN(test_native_step_fixed_fd_precedes_page_and_covers_all_io);
		UT_RUN(test_native_step_fixed_fd_open_failure_precedes_page_and_wal);
		UT_RUN(test_native_step_fixed_fd_gap_loss_cannot_insert);
		UT_RUN(test_native_step_fixed_fd_late_failure_keeps_inserted_debt);
		UT_RUN(test_native_step_fixed_fd_error_always_closes_private_storage);
		UT_RUN(test_native_step_fixed_fd_close_failure_cannot_claim_target_synced);
		UT_DONE(); return ut_failed_count != 0;
	}
	UT_PLAN(103);
	UT_RUN(test_native_unchanged_custody_retains_exact_raw_pages_and_same_fd);
	UT_RUN(test_native_unchanged_custody_refuses_copies_and_bad_entry_before_io);
	UT_RUN(test_native_unchanged_custody_refuses_changed_valid_main_and_space_bytes);
	UT_RUN(test_native_unchanged_custody_rechecks_binding_after_each_read);
	UT_RUN(test_native_unchanged_custody_wait_and_error_keep_fd_and_original_debt);
	UT_RUN(test_native_completed_readback_uses_original_page_and_same_fds_after_f4_clear);
	UT_RUN(test_native_completed_readback_refuses_clones_lost_binding_and_pending_api);
	UT_RUN(test_native_completed_readback_refuses_changed_main_or_space_without_reinstall);
	UT_RUN(test_native_completed_readback_rechecks_binding_after_each_protected_read);
	UT_RUN(test_native_completed_readback_collision_and_error_keep_original_storage);
	UT_RUN(test_native_readback_repromotes_same_page_and_reads_original_fds_only);
	UT_RUN(test_native_readback_never_reinstalls_before_or_changed_result_bytes);
	UT_RUN(test_native_readback_rechecks_immutable_inputs_after_every_read);
	UT_RUN(test_native_readback_page_collision_waits_without_failing_window);
	UT_RUN(test_native_readback_rejects_lost_fd_writer_flush_and_bad_entry);
	UT_RUN(test_native_readback_error_keeps_storage_and_original_window_debt);
	UT_RUN(test_native_readback_busy_cache_waits_without_invalidating_a_buffer);

	UT_RUN(test_native_held_fd_retains_same_durable_storage);
	UT_RUN(test_native_held_fd_uses_long_lived_context_and_restores_caller);
	UT_RUN(test_native_held_fd_preserves_four_older_close_contracts);
	UT_RUN(test_native_held_fd_does_not_transfer_other_phases);
	UT_RUN(test_native_held_fd_late_refusal_closes_and_keeps_debt);
	UT_RUN(test_native_held_fd_error_closes_and_keeps_debt);
	UT_RUN(test_native_held_fd_open_error_restores_caller_context);
	UT_RUN(test_native_unchanged_cache_retires_only_through_original_page_gate);
	UT_RUN(test_native_unchanged_cache_busy_waits_before_sync_without_retiring_duties);
	UT_RUN(test_native_unchanged_cache_refusal_never_reports_synced);
	UT_RUN(test_native_unchanged_cache_error_closes_only_private_storage);
	UT_RUN(test_native_unchanged_cache_reappearance_refuses_after_sync_or_full_postread);
	UT_RUN(test_native_unchanged_cache_preserves_all_observation_only_entries);

	UT_RUN(test_native_cache_retires_exact_predecessor_before_bufferless_fpi);
	UT_RUN(test_native_cache_busy_waits_without_wal_or_duty_retirement);
	UT_RUN(test_native_cache_refusal_and_error_keep_original_owners);
	UT_RUN(test_native_cache_reappearance_keeps_exact_inserted_window);
	UT_RUN(test_native_cache_noop_keeps_matching_cache_and_old_components);
	UT_RUN(test_native_cache_stale_canonical_refuses_before_cache_mutation);
	UT_RUN(test_native_unchanged_syncs_same_fd_and_full_image_without_wal);
	UT_RUN(test_native_unchanged_sync_rechecks_original_inputs_after_sync_and_each_read);
	UT_RUN(test_native_unchanged_sync_io_failure_error_and_close_keep_original_owners);
	UT_RUN(test_native_unchanged_sync_refuses_changed_full_main_or_space_bytes);
	UT_RUN(test_native_unchanged_sync_preserves_old_entries_and_rewritten_fpi_debt);
	UT_RUN(test_native_unchanged_sync_requires_original_entry_guards_and_canonical_image);
	UT_RUN(test_native_fpi_refuses_writer_outside_original_retention);
	UT_RUN(test_native_durable_marks_under_original_page_and_fd);
	UT_RUN(test_native_durable_snapshot_is_exact_and_read_only);
	UT_RUN(test_native_durable_transition_refusal_keeps_inserted_debt);
	UT_RUN(test_native_durable_post_transition_drift_fails_exact_window);
	UT_RUN(test_native_durable_error_keeps_original_phase_and_debt);
	UT_RUN(test_native_durable_close_failure_retains_durable_failed_debt);
	UT_RUN(test_native_durable_requires_successful_sync_and_postread);
	UT_RUN(test_native_durable_preserves_old_entry_and_noop_contracts);
	UT_RUN(test_native_step_fixed_fd_precedes_page_and_covers_all_io);
	UT_RUN(test_native_step_fixed_fd_open_failure_precedes_page_and_wal);
	UT_RUN(test_native_step_fixed_fd_gap_loss_cannot_insert);
	UT_RUN(test_native_step_fixed_fd_late_failure_keeps_inserted_debt);
	UT_RUN(test_native_step_fixed_fd_error_always_closes_private_storage);
	UT_RUN(test_native_step_fixed_fd_close_failure_cannot_claim_target_synced);

	UT_RUN(test_native_graph_step_accepts_only_storage_checksum_normalization);
	UT_RUN(test_native_graph_step_refuses_terminal_bytes_lsn_and_origin_mismatch);
	UT_RUN(test_native_graph_step_fixed_input_cannot_change_during_wal_preparation);
	UT_RUN(test_native_graph_step_error_keeps_original_inserted_debt);

	UT_RUN(test_native_fpi_projects_entire_page_from_original_c0_and_commit);
	UT_RUN(test_native_fpi_empty_and_already_terminal_do_not_mint_wal_frame);
	UT_RUN(test_native_fpi_bad_count_budget_or_terminal_refuses_before_smgr);
	UT_RUN(test_native_fpi_input_drift_at_each_read_never_publishes);
	UT_RUN(test_native_fpi_rechecks_all_original_inputs_after_token_producer);
	UT_RUN(test_native_fpi_error_frees_only_private_snapshot);
	UT_RUN(test_native_fpi_retired_scope_cannot_publish_or_release_borrowed_owner);
	UT_RUN(test_native_stamp_reference_uses_original_grant_and_page);
	UT_RUN(test_native_stamp_reference_refuses_changed_source_target_or_custody);
	UT_RUN(test_native_stamp_admits_each_original_source_without_rr);
	UT_RUN(test_native_stamp_refuses_unaccepted_or_wrong_original_owner);
	UT_RUN(test_native_stamp_requires_exact_canonical_descriptor);
	UT_RUN(test_native_stamp_rechecks_inputs_after_installed_writer_callback);
	UT_RUN(test_native_stamp_failed_window_keeps_original_debt_and_owners);
	UT_RUN(test_native_stamp_uses_unchanged_critical_insertion_transitions);
	UT_RUN(test_native_insert_registers_exact_image_edge_and_original_window);
	UT_RUN(test_native_insert_refuses_held_lwlocks_before_hash_or_capacity);
	UT_RUN(test_native_insert_page_collision_keeps_all_original_owners);
	UT_RUN(test_native_insert_rejects_bad_actor_count_budget_and_frame);
	UT_RUN(test_native_insert_rechecks_original_bytes_across_preparation);
	UT_RUN(test_native_insert_preinsert_refusal_cancels_only_its_window);
	UT_RUN(test_native_insert_errors_free_private_memory_and_keep_debt);
	UT_RUN(test_native_insert_registration_and_insert_failure_keep_failed_window);
	UT_RUN(test_native_insert_flush_failure_and_drift_keep_inserted_debt);
	UT_RUN(test_native_insert_pending_window_cannot_start_another_record);
	UT_RUN(test_native_install_writes_syncs_and_postreads_the_actual_writer_page);
	UT_RUN(test_native_install_already_result_still_syncs_and_postreads);
	UT_RUN(test_native_install_bad_entry_refuses_before_storage);
	UT_RUN(test_native_install_refuses_missing_torn_third_and_changed_result);
	UT_RUN(test_native_install_rechecks_complete_inputs_after_every_read);
	UT_RUN(test_native_install_write_and_sync_drift_retains_window);
	UT_RUN(test_native_install_io_errors_free_only_private_memory);
	UT_RUN(test_native_install_requires_current_original_inserted_window);
	UT_RUN(test_native_install_extra_lwlock_refuses_before_owner_snapshot);
	UT_RUN(test_native_step_prepares_logs_and_installs_under_original_owners);
	UT_RUN(test_native_step_terminal_page_keeps_observation_and_debt);
	UT_RUN(test_native_step_wait_keeps_original_preflight_and_native_owners);
	UT_RUN(test_native_step_bad_entry_refuses_before_owner_snapshot_or_io);
	UT_RUN(test_native_step_gap_rechecks_inputs_and_all_original_page_bytes);
	UT_RUN(test_native_step_rechecks_inputs_after_every_protected_read);
	UT_RUN(test_native_step_error_frees_private_snapshots_and_keeps_debt);
	UT_RUN(test_native_step_outstanding_operation_cannot_reinsert);
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
