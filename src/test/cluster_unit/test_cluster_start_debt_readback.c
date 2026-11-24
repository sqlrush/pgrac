/* Original Start consumes a protective transition; control/storage remain
 * controlled leaves. Their actual whole-file readback has its own target. */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include "cluster/cluster_control_debt_transition.h"
#include "cluster/cluster_recovery_format.h"
#define main start_debt_readback_base_main
int start_debt_readback_base_main(void);
#include "test_cluster_cold_tx_start.c"
#undef main
#include <fcntl.h>

static int readback_object, readback_fd = -1;
static unsigned readback_gets, readback_reads, readback_fault;
static bool readback_present;
static ClusterControlDebtTransitionViewV2 readback_view;
static uint8 before_pair[2 * CLUSTER_RECOVERY_CONTROL_ROOT_FILE_BYTES];
static uint8 after_pair[sizeof(before_pair)];
static ClusterRecoveryDebtStartV2 *readback_credential;

bool
cluster_control_root_debt_transition_get_v2(const ClusterControlDebtTransitionV2 *t,
	ClusterControlDebtTransitionViewV2 *out)
{
	readback_gets++;
	if (!readback_present || t != (const ClusterControlDebtTransitionV2 *)&readback_object)
		return false;
	if (readback_fault == 9 && readback_reads) readback_view.cf_sequence++;
	*out = readback_view;
	return true;
}

bool
cluster_control_root_debt_transition_readback_v2(const ClusterControlDebtTransitionV2 *t,
	int fd, Size budget)
{
	UT_ASSERT(t == (const ClusterControlDebtTransitionV2 *)&readback_object);
	UT_ASSERT_EQ(fd, readback_fd);
	UT_ASSERT_EQ(budget, 16 * 1024 * 1024);
	UT_ASSERT(fcntl(fd, F_GETFD) >= 0 && start_table_cf_held);
	readback_reads++;
	if (readback_fault == 1) return false;
	if (readback_fault == 2) pg_re_throw();
	if (readback_fault == 3) page_source_request_delta[0]++;
	if (readback_fault == 4) page_source_request_delta[2]++;
	if (readback_fault == 5) start_table_acquires++;
	if (readback_fault == 6) start_table_view.table_generation++;
	if (readback_fault == 7) observed_writer.epoch++;
	if (readback_fault == 8) {
		readback_fault = 0;
		UT_ASSERT(!ClusterColdRecoveryDebtReadbackV2(info.cluster_tx_start,
			readback_credential, t, fd, budget));
	}
	return true;
}

static bool
readback_run(void)
{
	return ClusterColdRecoveryDebtReadbackV2(info.cluster_tx_start, readback_credential,
		(const ClusterControlDebtTransitionV2 *)&readback_object,
		readback_fd, 16 * 1024 * 1024);
}

static void
readback_setup(void)
{
	pg_cryptohash_ctx *hash;
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	start_debt_many_ready();
	readback_fd = open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	UT_ASSERT(readback_fd >= 0);
	memset(before_pair, 0x31, sizeof(before_pair));
	memset(after_pair, 0x41, sizeof(after_pair));
	hash = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT(hash != NULL && pg_cryptohash_init(hash) == 0);
	UT_ASSERT(pg_cryptohash_update(hash, before_pair, sizeof(before_pair)) == 0);
	UT_ASSERT(pg_cryptohash_final(hash, digest, sizeof(digest)) == 0);
	pg_cryptohash_free(hash);
	memcpy(info.cluster_tx_start->relmap_table.control.root_pair_sha256, digest, sizeof(digest));
	start_table_view.control = info.cluster_tx_start->relmap_table.control;
	debt_selection_reference = start_table_view.control;
	readback_credential = NULL;
	UT_ASSERT(ClusterColdRecoveryDebtBeginV2(info.cluster_tx_start,
		(ClusterControlRelmapSelectionV2 *)&debt_selection_object, &readback_credential));
	memset(&readback_view, 0, sizeof(readback_view));
	readback_view.start = info.cluster_tx_start;
	readback_view.selection = (ClusterControlRelmapSelectionV2 *)&debt_selection_object;
	readback_view.cf_sequence = cluster_cf_attempt_sequence();
	readback_view.before_root = before_pair;
	readback_view.after_root = after_pair;
	readback_view.root_pair_bytes = sizeof(before_pair);
	readback_gets = readback_reads = readback_fault = 0;
	readback_present = true;
}

static void
readback_finish(void)
{
	UT_ASSERT(observation_scope_live && observation_cursor_live && readback_fd >= 0);
	UT_ASSERT(!readback_credential->failed || (info.cluster_tx_start->revoked
		&& !window_region.checkpoint_owner.valid));
	UT_ASSERT_EQ(observation_scope_releases + observation_census_destroys, 0);
	UT_ASSERT(fcntl(readback_fd, F_GETFD) >= 0 && close(readback_fd) == 0);
	readback_fd = -1;
	start_table_cf_held = false;
	ClusterReleaseColdRecoveryInputs(&info);
}

UT_TEST(original_start_reads_after_pair_without_reselecting_before_or_changing_tokens)
{
	ClusterControlNativeTableInfoV2 table;
	ClusterControlRootFileToken root;
	unsigned old_rechecks;
	readback_setup(); table = start_table_view; root = typed.census_root;
	old_rechecks = debt_selection_rechecks;
	/* Physical ROOT is now after. Ordinary before-selection must stay strict. */
	debt_selection_fault = 1;
	UT_ASSERT(readback_run() && readback_run());
	UT_ASSERT_EQ(readback_reads, 2);
	UT_ASSERT_EQ(debt_selection_rechecks, old_rechecks);
	UT_ASSERT(!memcmp(&table, &start_table_view, sizeof(table)));
	UT_ASSERT(!memcmp(&root, &typed.census_root, sizeof(root)));
	UT_ASSERT(!memcmp(&root, &info.cluster_tx_start->ctrc_observation_root, sizeof(root)));
	UT_ASSERT(!readback_credential->failed);
	readback_finish();
}
UT_TEST(original_start_refuses_foreign_transition_and_every_before_pair_byte)
{
	for (unsigned fault = 0; fault < 7; fault++) {
		readback_setup();
		if (fault == 0) readback_view.start = NULL;
		if (fault == 1) readback_view.selection = NULL;
		if (fault == 2) readback_view.cf_sequence++;
		if (fault == 3) readback_view.root_pair_bytes--;
		if (fault == 4) before_pair[sizeof(before_pair) - 1]++;
		if (fault == 5) readback_present = false;
		if (fault == 6) debt_selection_reference.root_pair_sha256[31]++;
		UT_ASSERT(!readback_run() && readback_credential->failed);
		UT_ASSERT_EQ(readback_reads, 0);
		readback_present = true;
		UT_ASSERT(!readback_run() && readback_reads == 0);
		readback_finish();
	}
}
UT_TEST(original_start_checks_all_original_holders_before_transition_io)
{
	for (unsigned i = 0; i < 3; i++) {
		readback_setup(); page_source_request_delta[i]++;
		UT_ASSERT(!readback_run() && readback_credential->failed);
		UT_ASSERT_EQ(readback_reads, 0);
		readback_finish();
	}
}
UT_TEST(original_start_checks_first_last_holder_and_original_owner_after_io)
{
	for (unsigned fault = 3; fault <= 7; fault++) {
		readback_setup(); readback_fault = fault;
		UT_ASSERT(!readback_run() && readback_credential->failed);
		UT_ASSERT_EQ(readback_reads, 1);
		readback_finish();
	}
}
UT_TEST(original_start_failed_physical_readback_is_sticky_without_cleanup)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		volatile bool threw = false, accepted = false;
		readback_setup(); readback_fault = fault;
		PG_TRY(); { accepted = readback_run(); }
		PG_CATCH(); { threw = true; }
		PG_END_TRY();
		UT_ASSERT(!accepted && threw == (fault == 2) && readback_credential->failed);
		readback_fault = 0;
		UT_ASSERT(!readback_run() && readback_reads == 1);
		readback_finish();
	}
}
UT_TEST(original_start_copied_handle_and_locked_actor_do_not_enter_readback)
{
	ClusterRecoveryDebtStartV2 copy;
	readback_setup(); copy = *readback_credential;
	UT_ASSERT(!ClusterColdRecoveryDebtReadbackV2(info.cluster_tx_start, &copy,
		(const ClusterControlDebtTransitionV2 *)&readback_object, readback_fd, 16 * 1024 * 1024));
	ctrc_driver_lwlocks = 1; UT_ASSERT(!readback_run()); ctrc_driver_lwlocks = 0;
	CritSectionCount = 1; UT_ASSERT(!readback_run()); CritSectionCount = 0;
	UT_ASSERT_EQ(readback_gets + readback_reads, 0);
	UT_ASSERT(readback_run());
	readback_finish();
}
UT_TEST(original_start_recursive_readback_revokes_outer_attempt_without_releasing)
{
	readback_setup(); readback_fault = 8;
	UT_ASSERT(!readback_run() && readback_credential->failed);
	UT_ASSERT_EQ(readback_reads, 1);
	readback_finish();
}
UT_TEST(original_start_rechecks_transition_identity_after_physical_observation)
{
	readback_setup(); readback_fault = 9;
	UT_ASSERT(!readback_run() && readback_credential->failed);
	UT_ASSERT_EQ(readback_reads, 1);
	readback_finish();
}

int main(void)
{
	UT_PLAN(8);
	UT_RUN(original_start_reads_after_pair_without_reselecting_before_or_changing_tokens);
	UT_RUN(original_start_refuses_foreign_transition_and_every_before_pair_byte);
	UT_RUN(original_start_checks_all_original_holders_before_transition_io);
	UT_RUN(original_start_checks_first_last_holder_and_original_owner_after_io);
	UT_RUN(original_start_failed_physical_readback_is_sticky_without_cleanup);
	UT_RUN(original_start_copied_handle_and_locked_actor_do_not_enter_readback);
	UT_RUN(original_start_recursive_readback_revokes_outer_attempt_without_releasing);
	UT_RUN(original_start_rechecks_transition_identity_after_physical_observation);
	UT_DONE(); return ut_failed_count != 0;
}
