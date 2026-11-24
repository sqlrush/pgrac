/*-------------------------------------------------------------------------
 *
 * test_cluster_start_real_debt_readback.c
 *    Original Start observes a real published protective debt file set.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_start_real_debt_readback.c
 *
 * NOTES
 *    Original Start and control implementations share one actor and CF leaf.
 *    Source/selection services remain controlled. No AcceptPrepared or live
 *    producer is installed; a readback does not complete or retire recovery.
 *
 *-------------------------------------------------------------------------
 */
#define main start_real_debt_original_main
int start_real_debt_original_main(void);
#include "test_cluster_cold_tx_start.c"
#undef main
#include "test_cluster_start_debt_files.h"
#include <fcntl.h>

static ClusterRecoveryDebtStartV2 *files_credential;
static ClusterControlDebtTransitionV2 *files_transition;
static int files_directory;
static unsigned files_fault;
static uint8 files_before[2 * CLUSTER_RECOVERY_CONTROL_ROOT_FILE_BYTES];

/* Only forward: the original control implementation owns the entire read. */
bool
cluster_control_root_debt_transition_readback_v2(const ClusterControlDebtTransitionV2 *t,
	int fd, Size budget)
{
	return start_debt_files_readback_v2(t, fd, budget);
}

void
start_debt_files_read_step(void)
{
	unsigned fault = files_fault;
	files_fault = 0;
	if (fault == 1) page_source_request_delta[2]++;
	if (fault == 2) pg_re_throw();
}

static bool files_readback(void)
{
	return ClusterColdRecoveryDebtReadbackV2(info.cluster_tx_start, files_credential,
		files_transition, files_directory, START_DEBT_FILES_BUDGET);
}

static void files_begin(void)
{
	pg_cryptohash_ctx *hash;
	uint8 digest[32];
	files_fault = 0;
	start_debt_many_ready();
	files_directory = start_debt_files_before(files_before, sizeof(files_before));
	hash = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT(hash && pg_cryptohash_init(hash) == 0
		&& pg_cryptohash_update(hash, files_before, sizeof(files_before)) == 0
		&& pg_cryptohash_final(hash, digest, sizeof(digest)) == 0);
	pg_cryptohash_free(hash);
	memcpy(info.cluster_tx_start->relmap_table.control.root_pair_sha256, digest, sizeof(digest));
	start_table_view.control = info.cluster_tx_start->relmap_table.control;
	debt_selection_reference = start_table_view.control;
	files_credential = NULL;
	UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
		(ClusterControlRelmapSelectionV2 *)&debt_selection_object, &files_credential));
	files_transition = start_debt_files_build(info.cluster_tx_start,
		(ClusterControlRelmapSelectionV2 *)&debt_selection_object);
	UT_ASSERT(files_transition && start_debt_files_publish(files_transition));
	start_debt_files_reset();
}

static void files_end(void)
{
	UT_ASSERT(observation_scope_live && observation_cursor_live);
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	UT_ASSERT(fcntl(files_directory, F_GETFD) >= 0);
	if (files_credential->failed) {
		UT_ASSERT(info.cluster_tx_start->revoked);
		UT_ASSERT(!window_region.checkpoint_owner.valid);
	}
	/* Only explicit test teardown destroys the fixture and its files. */
	start_debt_files_end(&files_transition);
	start_table_cf_held = false;
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(original_start_reads_all_real_batches_without_mutation_or_token_changes)
{
	uint8 before[32], after[32];
	unsigned r, w, s;
	files_begin();
	ClusterControlNativeTableInfoV2 table = start_table_view;
	ClusterControlRootFileToken token = typed.census_root;
	uint64 count = start_debt_files_digest(before);
	start_debt_files_reset();
	UT_ASSERT(files_readback() && files_readback());
	start_debt_files_counts(&r, &w, &s);
	UT_ASSERT(r > 0 && w == 0 && s == 0);
	UT_ASSERT_EQ(start_debt_files_digest(after), count);
	UT_ASSERT(!memcmp(before, after, 32));
	UT_ASSERT(!memcmp(&table, &start_table_view, sizeof(table)));
	UT_ASSERT(!memcmp(&token, &typed.census_root, sizeof(token)));
	UT_ASSERT(!files_credential->failed);
	files_end();
}

UT_TEST(original_start_real_root_damage_revokes_without_repair_or_retry)
{
	for (unsigned fault = 0; fault < 2; fault++) {
		uint8 before[32], after[32]; unsigned r, w, s;
		files_begin(); start_debt_files_damage(fault);
		uint64 count = start_debt_files_digest(before);
		start_debt_files_reset();
		UT_ASSERT(!files_readback() && files_credential->failed);
		UT_ASSERT(!files_readback());
		start_debt_files_counts(&r, &w, &s);
		UT_ASSERT(w == 0 && s == 0);
		UT_ASSERT_EQ(start_debt_files_digest(after), count);
		UT_ASSERT(!memcmp(before, after, 32));
		files_end();
	}
}

UT_TEST(original_start_missing_final_page_registry_or_basis_keeps_all_protections)
{
	for (unsigned fault = 2; fault <= 4; fault++) {
		uint8 before[32], after[32]; unsigned r, w, s;
		files_begin(); start_debt_files_damage(fault);
		uint64 count = start_debt_files_digest(before);
		start_debt_files_reset();
		UT_ASSERT(!files_readback() && files_credential->failed);
		start_debt_files_counts(&r, &w, &s);
		/* A missing selected registry refuses at open, before immutable pread. */
		UT_ASSERT(fault == 3 ? r == 0 : r > 0);
		UT_ASSERT(w == 0 && s == 0);
		UT_ASSERT_EQ(start_debt_files_digest(after), count);
		UT_ASSERT(!memcmp(before, after, 32));
		files_end();
	}
}

UT_TEST(original_start_real_file_late_holder_change_and_error_are_sticky)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		volatile bool caught = false, accepted = false;
		unsigned r, w, s;
		files_begin(); files_fault = fault;
		PG_TRY(); { accepted = files_readback(); }
		PG_CATCH(); { caught = true; }
		PG_END_TRY();
		UT_ASSERT(!accepted && caught == (fault == 2) && files_credential->failed);
		UT_ASSERT(files_fault == 0 && !files_readback());
		start_debt_files_counts(&r, &w, &s);
		UT_ASSERT(r > 0 && w == 0 && s == 0);
		files_end();
	}
}

int main(void)
{
	UT_PLAN(4);
	UT_RUN(original_start_reads_all_real_batches_without_mutation_or_token_changes);
	UT_RUN(original_start_real_root_damage_revokes_without_repair_or_retry);
	UT_RUN(original_start_missing_final_page_registry_or_basis_keeps_all_protections);
	UT_RUN(original_start_real_file_late_holder_change_and_error_are_sticky);
	UT_DONE(); return ut_failed_count != 0;
}
