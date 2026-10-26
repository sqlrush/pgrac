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
#include "test_cluster_control_root.c"
#undef cluster_thread_recovery_pin_projection
#undef cluster_thread_recovery_projection_current
#undef cluster_recovery_checkpoint_current_v1
#undef cluster_recovery_checkpoint_observe_v1
#undef cluster_recovery_stamp_worker_begin_v1

#undef main
#undef ClusterColdRecoveryTxC0Candidate

#include "cluster/cluster_itl.h"
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
#define palloc native_stamp_alloc
#define pfree native_stamp_free
#include "../../backend/cluster/cluster_recovery_plan.c"
#undef palloc
#undef pfree

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
	UT_ASSERT(native_fpi_allocation == NULL);
	if (native_fpi_alloc_error) pg_re_throw();
	native_fpi_allocs++;
	native_fpi_allocation = palloc0(size);
	return native_fpi_allocation;
}

void
ctrc_native_fpi_free(void *pointer)
{
	UT_ASSERT(pointer != NULL && pointer == native_fpi_allocation);
	native_fpi_frees++; native_fpi_allocation = NULL; pfree(pointer);
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

#include "test_cluster_ctrc_native_stamp.inc"

int
main(int argc, char **argv)
{
	(void)argc;
	setvbuf(stdout, NULL, _IOLBF, 0);
	test_program_path = argv[0];
	setup_fixture();
	UT_PLAN(15);
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
	UT_DONE(); return ut_failed_count ? 1 : 0;
}
