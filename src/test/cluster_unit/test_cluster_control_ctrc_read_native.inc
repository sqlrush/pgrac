/*-------------------------------------------------------------------------
 *
 * test_cluster_control_ctrc_read_native.inc
 *     Native CTRC PAGE/owner protection around real-file SMGR reads.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_control_ctrc_read_native.inc
 *
 * NOTES
 *     Original native set, graph, PAGE, SPACE codecs and checksum execute.
 *     Start/GES and SMGR transport remain controlled fixture leaves.
 *
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_page_install_ctrc.h"
#include "cluster/cluster_space_reservation.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"
#include "storage/smgr.h"

bool IsUnderPostmaster;

typedef struct CtrcReadNativeFixture {
	CtrcPageFixture page;
	ClusterSpaceIdentity space;
	PGAlignedBlock main;
	int space_fd, main_fd, prior_pid;
	FILE *space_file, *main_file;
	bool prior_under, prior_shared;
} CtrcReadNativeFixture;

static CtrcReadNativeFixture *ctrc_read_native_live;
static SMgrRelationData ctrc_read_native_relation;
static bool ctrc_read_native_checksums, ctrc_read_native_wrong_scope;
static bool ctrc_read_native_missing_space, ctrc_read_native_missing_main;
static unsigned ctrc_read_native_opens, ctrc_read_native_reads;
static unsigned ctrc_read_native_drift_at, ctrc_read_native_drift_kind;
static unsigned ctrc_read_native_throw_at;

struct ClusterWalInputsV1 *
ClusterColdRecoveryCtrcScope(const struct ClusterRecoveryStartV1 *start,
	const struct ClusterColdCtrcWorkV1 *work)
{
	if (ctrc_read_native_wrong_scope) return (void *)(uintptr_t)1;
	return ctrc_ir_start_current && ClusterColdRecoveryCtrcWorkCurrent(start, work)
		? ctrc_ir_original_scope : NULL;
}

bool
DataChecksumsEnabled(void)
{
	return ctrc_read_native_checksums;
}

SMgrRelation
smgropen(RelFileLocator locator, BackendId backend)
{
	UT_ASSERT(ctrc_read_native_live != NULL && backend == InvalidBackendId);
	UT_ASSERT(memcmp(&locator, &ctrc_read_native_live->space.key.locator, sizeof(locator)) == 0);
	ctrc_read_native_opens++;
	return &ctrc_read_native_relation;
}

bool
smgrexists(SMgrRelation relation, ForkNumber forknum)
{
	UT_ASSERT(relation == &ctrc_read_native_relation);
	return forknum == SPACE_FORKNUM ? !ctrc_read_native_missing_space
		: forknum == MAIN_FORKNUM && !ctrc_read_native_missing_main;
}

BlockNumber
smgrnblocks(SMgrRelation relation, ForkNumber forknum)
{
	struct stat st;
	UT_ASSERT(relation == &ctrc_read_native_relation);
	UT_ASSERT_EQ(fstat(forknum == SPACE_FORKNUM ? ctrc_read_native_live->space_fd
		: ctrc_read_native_live->main_fd, &st), 0);
	return st.st_size / BLCKSZ;
}

static void
ctrc_read_native_write(CtrcReadNativeFixture *f, ForkNumber forknum,
	BlockNumber blockno, const PGAlignedBlock *image)
{
	PGAlignedBlock copy = *image;
	PageHeader header = (PageHeader)copy.data;
	header->pd_checksum = 0;
	if (ctrc_read_native_checksums && !PageIsNew((Page)copy.data))
		header->pd_checksum = pg_checksum_page(copy.data, blockno);
	UT_ASSERT_EQ(pwrite(forknum == SPACE_FORKNUM ? f->space_fd : f->main_fd,
		copy.data, BLCKSZ, (off_t)blockno * BLCKSZ), BLCKSZ);
}

void
smgrread(SMgrRelation relation, ForkNumber forknum, BlockNumber blockno, void *buffer)
{
	CtrcReadNativeFixture *f = ctrc_read_native_live;
	PGAlignedBlock changed;
	UT_ASSERT(relation == &ctrc_read_native_relation && f != NULL);
	ctrc_read_native_reads++;
	if (ctrc_read_native_reads == ctrc_read_native_throw_at) pg_re_throw();
	UT_ASSERT_EQ(pread(forknum == SPACE_FORKNUM ? f->space_fd : f->main_fd,
		buffer, BLCKSZ, (off_t)blockno * BLCKSZ), BLCKSZ);
	if (ctrc_read_native_reads != ctrc_read_native_drift_at) return;
	switch (ctrc_read_native_drift_kind) {
	case 0: test_input_current = false; break;
	case 1: ctrc_inputs_owner.writer.epoch++; break;
	case 2: tx_inputs_generation++; break;
	case 3: tx_inputs_serial++; break;
	case 4: ctrc_inputs_work.generation++; break;
	case 5: tx_inputs_typed.sources[0].timeline++; break;
	case 6: f->space.sequence++; break;
	case 7: f->page.target.expected_result.mutation_token++; break;
	case 8: ctrc_read_native_wrong_scope = true; break;
	case 9: ctrc_inputs_owner.operation_uuid[15]++; break;
	case 10: ctrc_inputs_owner.owner_boot++; break;
	case 11: ctrc_inputs_work.counts.unresolved_count++; break;
	case 12: {
		ClusterSpaceIdentity space = f->space;
		space.sequence++;
		UT_ASSERT(cluster_space_identity_page_encode(&space, 51, changed.data, BLCKSZ));
		ctrc_read_native_write(f, SPACE_FORKNUM, 0, &changed); break;
	}
	case 13: {
		ClusterSpaceReservation reservation = {0};
		reservation.identity = f->space; reservation.next_block = 9;
		UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 52, changed.data, BLCKSZ));
		ctrc_read_native_write(f, SPACE_FORKNUM, 1, &changed); break;
	}
	case 14: cluster_wal_inputs_release_v1(&f->page.replay.inputs); break;
	case 15: ctrc_inputs_work.plan = (void *)(uintptr_t)1; break;
	case 16: ctrc_inputs_work.top_tx_census = (void *)(uintptr_t)1; break;
	case 17: ctrc_inputs_work.counts.released_count++; break;
	case 18: ctrc_inputs_work.counts.unqualified_count++; break;
	case 19: ctrc_inputs_work.counts.subject_count++; break;
	}
}

static void
ctrc_read_native_fixture(CtrcReadNativeFixture *f, bool checksums)
{
	PGAlignedBlock image;
	ClusterSpaceReservation reservation = {0};
	memset(f, 0, sizeof(*f));
	f->prior_under = IsUnderPostmaster; f->prior_shared = cluster_shared_config;
	f->prior_pid = MyProcPid;
	IsUnderPostmaster = cluster_shared_config = true;
	ShutdownRequestPending = false; CritSectionCount = 0;
	ctrc_read_native_checksums = checksums;
	ctrc_read_native_wrong_scope = ctrc_read_native_missing_space = ctrc_read_native_missing_main = false;
	ctrc_read_native_opens = ctrc_read_native_reads = 0;
	ctrc_read_native_drift_at = ctrc_read_native_drift_kind = ctrc_read_native_throw_at = 0;
	rf_page_guard_shmem_init_v1(); ctrc_replay_stable_fixture(&f->page.replay);
	IsUnderPostmaster = cluster_shared_config = true; MyProcPid = getpid();
	/* The old controlled Start fixture supplies a minimal writer. Recreate
	 * its original native scope after binding a complete fixture writer. */
	cluster_wal_inputs_release_v1(&f->page.replay.inputs);
	ctrc_inputs_owner.writer.ref = tx_inputs_typed.sources[0];
	UT_ASSERT_EQ(ctrc_inputs_begin(&f->page.replay.inputs), 0);
	ctrc_ir_original_scope = f->page.replay.inputs;
	UT_ASSERT_EQ(cluster_wal_inputs_ctrc_replays_acquire_v1(f->page.replay.inputs), 0);
	for (unsigned i = 0; i < 2; i++)
		f->page.replay.owners[i] = cluster_wal_inputs_ctrc_replays_source_v1(f->page.replay.inputs, 1 - i);
	f->page.proof = ctrc_replay_stable_build(&f->page.replay);
	UT_ASSERT_EQ(f->page.proof.detail, RF_PAGE_PROOF_DETAIL_OK);
	f->page.target.page_identity = f->page.replay.graph.page_identity;
	f->page.target.expected_result = f->page.replay.graph.expected_result;
	f->page.target.stable_base = f->page.proof.proof;
	f->page.target.source = &f->page.replay.source;
	f->page.target.contributors = &f->page.replay.vector;
	f->page.page = ctrc_page_build(&f->page);
	UT_ASSERT_EQ(f->page.page.verdict, RF_PAGE_AUTHORITY_OK);
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f->page.page.authority), 0);
	f->space.key.system_identifier = f->page.target.page_identity.system_identifier;
	f->space.key.database_incarnation = ctrc_inputs_owner.writer.ref.claim.database_incarnation;
	memcpy(f->space.key.storage_uuid, f->page.target.page_identity.storage_uuid, 16);
	f->space.key.locator = f->page.target.page_identity.locator;
	memcpy(f->space.incarnation, f->page.target.expected_result.segment_incarnation, 16);
	f->space.sequence = f->space.operation = 1; f->space.state = CLUSTER_SPACE_IDENTITY_LIVE;
	f->space_file = tmpfile(); f->main_file = tmpfile();
	UT_ASSERT(f->space_file != NULL && f->main_file != NULL);
	f->space_fd = fileno(f->space_file); f->main_fd = fileno(f->main_file);
	UT_ASSERT_EQ(ftruncate(f->space_fd, 2 * BLCKSZ), 0);
	UT_ASSERT_EQ(ftruncate(f->main_fd, 8 * BLCKSZ), 0);
	UT_ASSERT(cluster_space_identity_page_encode(&f->space, 51, image.data, BLCKSZ));
	ctrc_read_native_write(f, SPACE_FORKNUM, 0, &image);
	reservation.identity = f->space; reservation.next_block = 8;
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 52, image.data, BLCKSZ));
	ctrc_read_native_write(f, SPACE_FORKNUM, 1, &image);
	((PageHeader)f->main.data)->pd_lower = SizeOfPageHeaderData;
	((PageHeader)f->main.data)->pd_upper = ((PageHeader)f->main.data)->pd_special = BLCKSZ;
	PageSetPageSizeAndVersion((Page)f->main.data, BLCKSZ, PG_PAGE_LAYOUT_VERSION);
	((PageHeader)f->main.data)->pd_block_scn = f->page.target.expected_result.mutation_token;
	f->main.data[BLCKSZ - 1] = 0x39;
	ctrc_read_native_write(f, MAIN_FORKNUM, 7, &f->main);
	ctrc_read_native_live = f;
}

static void
ctrc_read_native_finish(CtrcReadNativeFixture *f)
{
	ctrc_page_finish(&f->page);
	/* An ERROR/retired-scope case can have released the scope already. */
	test_ctrc_set_acquire_hook = NULL; test_ctrc_set_release_hook = NULL;
	ctrc_replay_current_override = NULL; test_inputs_startup_cf = false;
	UT_ASSERT_EQ(fclose(f->space_file), 0); UT_ASSERT_EQ(fclose(f->main_file), 0);
	MyProcPid = f->prior_pid;
	ctrc_read_native_live = NULL; IsUnderPostmaster = f->prior_under;
	cluster_shared_config = f->prior_shared; ShutdownRequestPending = false;
	CritSectionCount = 0; inputs_native_recovery = false;
}

static RfPageCtrcReadV1
ctrc_read_native_image(CtrcReadNativeFixture *f)
{
	return rf_page_recovery_ctrc_replays_read_smgr_v1(tx_inputs_start, &ctrc_inputs_work,
		f->page.page.authority, &f->space, 7, &f->page.target.expected_result);
}

static void
ctrc_read_native_refused(CtrcReadNativeFixture *f)
{
	static const PGAlignedBlock zero;
	RfPageCtrcReadV1 result = ctrc_read_native_image(f);
	if (result.ready) printf("# unexpected ready: drift_kind=%u drift_at=%u reads=%u\n",
		ctrc_read_native_drift_kind, ctrc_read_native_drift_at, ctrc_read_native_reads);
	UT_ASSERT(!result.ready); UT_ASSERT(memcmp(&result.image, &zero, sizeof(zero)) == 0);
}

UT_TEST(test_ctrc_native_read_exact_five_reads_under_original_owners)
{
	for (unsigned checksums = 0; checksums < 2; checksums++) {
		CtrcReadNativeFixture f; PGAlignedBlock disk; unsigned cf, releases, grants;
		RfPageCtrcReadV1 result;
		ctrc_read_native_fixture(&f, checksums != 0);
		cf = test_cf_lock_calls; releases = test_tx_ir_releases; grants = test_tx_ir_grants;
		result = ctrc_read_native_image(&f); UT_ASSERT(result.ready);
		UT_ASSERT_EQ(pread(f.main_fd, disk.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ);
		UT_ASSERT(memcmp(result.image.data, disk.data, BLCKSZ) == 0);
		UT_ASSERT_EQ(ctrc_read_native_reads, 5); UT_ASSERT_EQ(ctrc_read_native_opens, 1);
		UT_ASSERT_EQ(test_cf_lock_calls, cf); UT_ASSERT_EQ(test_tx_ir_grants, grants);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT_EQ(ctrc_set_held, 2);
		UT_ASSERT(inputs_pin_held); ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_refuses_wrong_scope_actor_or_target_before_io)
{
	for (unsigned fault = 0; fault < 16; fault++) {
		CtrcReadNativeFixture f;
		ctrc_read_native_fixture(&f, true);
		switch (fault) {
		case 0: ctrc_read_native_wrong_scope = true; break;
		case 1: IsUnderPostmaster = false; break;
		case 2: MyBackendType = B_BACKEND; break;
		case 3: ShutdownRequestPending = true; break;
		case 4: inputs_native_recovery = true; break;
		case 5: cluster_shared_config = false; break;
		case 6: ctrc_inputs_owner.owner_pid++; break;
		case 7: f.space.key.database_incarnation++; break;
		case 8: f.space.key.storage_uuid[15]++; break;
		case 9: f.space.incarnation[15]++; break;
		case 10: f.space.state = CLUSTER_SPACE_IDENTITY_TOMBSTONED; break;
		case 11: f.page.target.expected_result.mutation_token++; break;
		case 12: tx_inputs_typed.replay_count = 0; break;
		case 13: tx_inputs_typed.participant_count = CLUSTER_COLD_MAX_PARTICIPANTS + 1; break;
		case 14: CritSectionCount = 1; break;
		case 15: tx_inputs_owner_current = false; break;
		}
		ctrc_read_native_refused(&f);
		UT_ASSERT_EQ(ctrc_read_native_reads + ctrc_read_native_opens, 0);
		CritSectionCount = 0; ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_refuses_torn_space_and_wrong_page_version)
{
	for (unsigned fault = 0; fault < 13; fault++) {
		CtrcReadNativeFixture f; PGAlignedBlock page;
		ClusterSpaceReservation reservation = {0};
		ctrc_read_native_fixture(&f, true);
		page = f.main;
		switch (fault) {
		case 0: ctrc_read_native_missing_space = true; break;
		case 1: ctrc_read_native_missing_main = true; break;
		case 2: UT_ASSERT_EQ(ftruncate(f.space_fd, BLCKSZ), 0); break;
		case 3: UT_ASSERT_EQ(ftruncate(f.space_fd, 3 * BLCKSZ), 0); break;
		case 4: UT_ASSERT_EQ(ftruncate(f.main_fd, 7 * BLCKSZ), 0); break;
		case 5: memset(&page, 0, sizeof(page)); ctrc_read_native_write(&f, SPACE_FORKNUM, 0, &page); break;
		case 6: memset(&page, 0, sizeof(page)); ctrc_read_native_write(&f, SPACE_FORKNUM, 1, &page); break;
		case 7: memset(&page, 0, sizeof(page)); ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &page); break;
		case 8: ((PageHeader)page.data)->pd_block_scn++;
			ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &page); break;
		case 9: page.data[BLCKSZ - 1]++;
			UT_ASSERT_EQ(pwrite(f.main_fd, page.data, BLCKSZ, 7 * BLCKSZ), BLCKSZ); break;
		case 10: f.space.sequence++;
			UT_ASSERT(cluster_space_identity_page_encode(&f.space, 51, page.data, BLCKSZ));
			f.space.sequence--; ctrc_read_native_write(&f, SPACE_FORKNUM, 0, &page); break;
		case 11: reservation.identity = f.space; reservation.next_block = 7;
			UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 52, page.data, BLCKSZ));
			ctrc_read_native_write(&f, SPACE_FORKNUM, 1, &page); break;
		case 12: ((PageHeader)page.data)->pd_upper = 0;
			ctrc_read_native_write(&f, MAIN_FORKNUM, 7, &page); break;
		}
		ctrc_read_native_refused(&f); ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_lost_owner_after_each_read_retains_page_and_debt)
{
	for (unsigned read = 1; read <= 5; read++) {
		CtrcReadNativeFixture f; RfPageGuardPreflightV1 pre; RfPageGuardV1 probe = {0};
		unsigned releases;
		ctrc_read_native_fixture(&f, true); releases = test_tx_ir_releases;
		ctrc_read_native_drift_at = read;
		ctrc_read_native_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, read);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT(rf_page_guard_preflight_v1(&f.page.target.page_identity, &pre));
		UT_ASSERT(!rf_page_guard_promote_nowait_v1(&pre, &probe));
		ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_rechecks_original_inputs_after_each_io)
{
	for (unsigned kind = 1; kind <= 19; kind++)
		for (unsigned read = 1; read <= 5; read++) {
			CtrcReadNativeFixture f;
			if (kind >= 12 && kind <= 14) continue;
			ctrc_read_native_fixture(&f, false);
			ctrc_read_native_drift_at = read; ctrc_read_native_drift_kind = kind;
			ctrc_read_native_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, read);
			ctrc_read_native_finish(&f);
		}
}

UT_TEST(test_ctrc_native_read_rereads_both_space_pages_before_publication)
{
	for (unsigned kind = 12; kind <= 13; kind++) {
		CtrcReadNativeFixture f;
		ctrc_read_native_fixture(&f, true);
		ctrc_read_native_drift_at = kind == 12 ? 3 : 4; ctrc_read_native_drift_kind = kind;
		ctrc_read_native_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, kind == 12 ? 4 : 5);
		ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_error_propagates_without_retiring_owners)
{
	for (unsigned read = 1; read <= 5; read++) {
		CtrcReadNativeFixture f; unsigned releases; volatile bool caught = false;
		ctrc_read_native_fixture(&f, true); releases = test_tx_ir_releases;
		ctrc_read_native_throw_at = read;
		PG_TRY(); { (void)ctrc_read_native_image(&f); }
		PG_CATCH(); { caught = true; } PG_END_TRY();
		UT_ASSERT(caught); UT_ASSERT_EQ(ctrc_read_native_reads, read);
		UT_ASSERT_EQ(test_tx_ir_releases, releases); UT_ASSERT(inputs_pin_held && ctrc_set_held == 2);
		UT_ASSERT_EQ(rf_page_authority_ctrc_replays_revalidate_nowait_v1(f.page.page.authority), 0);
		ctrc_read_native_finish(&f);
	}
}

UT_TEST(test_ctrc_native_read_retired_scope_never_revives_borrowed_authority)
{
	CtrcReadNativeFixture f;
	ctrc_read_native_fixture(&f, true);
	ctrc_read_native_drift_at = 3; ctrc_read_native_drift_kind = 14;
	ctrc_read_native_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, 3);
	UT_ASSERT(f.page.replay.inputs == NULL && !inputs_pin_held && ctrc_set_held == 0);
	ctrc_read_native_refused(&f); UT_ASSERT_EQ(ctrc_read_native_reads, 3);
	ctrc_read_native_finish(&f);
}

UT_TEST(test_ctrc_native_page_scope_match_requires_exact_live_promoted_scope)
{
	CtrcPageFixture f;
	ctrc_page_fixture(&f); f.page = ctrc_page_build(&f);
	UT_ASSERT(!rf_page_authority_ctrc_replays_scope_nowait_v1(f.page.authority, f.replay.inputs));
	UT_ASSERT_EQ(rf_page_authority_ctrc_replays_promote_nowait_v1(f.page.authority), 0);
	UT_ASSERT(!rf_page_authority_ctrc_replays_scope_nowait_v1(f.page.authority, NULL));
	UT_ASSERT(!rf_page_authority_ctrc_replays_scope_nowait_v1(f.page.authority, (void *)(uintptr_t)1));
	UT_ASSERT(rf_page_authority_ctrc_replays_scope_nowait_v1(f.page.authority, f.replay.inputs));
	cluster_wal_inputs_release_v1(&f.replay.inputs);
	UT_ASSERT(!rf_page_authority_ctrc_replays_scope_nowait_v1(f.page.authority, ctrc_ir_original_scope));
	ctrc_page_finish(&f);
	test_ctrc_set_acquire_hook = NULL; test_ctrc_set_release_hook = NULL;
	ctrc_replay_current_override = NULL; test_inputs_startup_cf = false;
}
