/* Actual Startup call-site slices, linked to the real normal-start producer.
 * WAL decoding/PG lifecycle, prepared scan and unrelated recovery/HW jobs are
 * explicit boundary fixtures. No copied normal-start decision or loader. */
#include "catalog/pg_control.h"
#include "access/xlogrecord.h"
#include "cluster/cluster_backup.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_hw_snapshot.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_tt_slot.h"

static int startup_prepared_input;
static unsigned startup_tt_capture_calls, startup_confirm_calls;
static bool startup_confirm_clean;
static int startup_confirm_prepared;
static unsigned startup_resolve_calls, startup_old_scn_calls, startup_rollback_calls;
static unsigned startup_backup_calls, startup_cf_calls, startup_hw_calls;
static bool startup_cf_ok, startup_hw_ok;
static int startup_fatal_level;

void
cluster_tt_slot_capture_startup_checkpoint(SCN scn, FullTransactionId xid)
{
	startup_tt_capture_calls++;
}
void
cluster_tt_slot_confirm_clean_start(bool clean, int count)
{
	startup_confirm_calls++;
	startup_confirm_clean = clean;
	startup_confirm_prepared = count;
}
TransactionId PrescanPreparedTransactions(TransactionId **xids, int *count);
TransactionId
PrescanPreparedTransactions(TransactionId **xids, int *count)
{
	*xids = NULL;
	*count = startup_prepared_input;
	return 41;
}
int
cluster_tt_recovery_resolve_active_slots(void)
{
	startup_resolve_calls++;
	return 0;
}
int
cluster_tt_recovery_observe_scn_highwater(void)
{
	startup_old_scn_calls++;
	return 0;
}
int
cluster_tt_recovery_physical_rollback(void)
{
	startup_rollback_calls++;
	return 0;
}
void
cluster_backup_recovery_observe_highwater(void)
{
	startup_backup_calls++;
}
bool
cluster_cf_owner_eor_close(void)
{
	startup_cf_calls++;
	return startup_cf_ok;
}
bool
cluster_hw_startup_complete(const char **failure)
{
	startup_hw_calls++;
	*failure = startup_hw_ok ? NULL : "HW_FIXTURE_FAILURE";
	return startup_hw_ok;
}

/* Catch the real caller's FATAL boundary without letting a unit runner exit.
 * The actual loader and its exception cleanup retain the native PG_TRY path. */
#pragma push_macro("ereport")
#undef ereport
#define ereport(level, rest)                                                                       \
	do {                                                                                           \
		startup_fatal_level = (level);                                                             \
		pg_re_throw();                                                                             \
	} while (0)

static void
startup_actual_capture(bool wasShutdown, XLogRecPtr location, XLogRecPtr redo, uint64 next_xid,
					   SCN scn)
{
	XLogRecPtr CheckPointLoc = location;
	CheckPoint checkPoint = { 0 };
	XLogRecord own_record = { 0 }, *record = &own_record;
	checkPoint.redo = redo;
	checkPoint.nextXid = FullTransactionIdFromU64(next_xid);
	own_record.xl_scn = scn;
#include "test_cluster_normal_cold_capture.inc"
	(void)CheckPointLoc;
}

static TransactionId
startup_actual_prepare(DBState boot_state, bool wasShutdown, bool InRecovery,
					   bool ArchiveRecoveryRequested, bool haveBackupLabel)
{
	TransactionId oldestActiveXID = InvalidTransactionId;
#include "test_cluster_normal_cold_prepare.inc"
	return oldestActiveXID;
}

static void
startup_actual_postjobs(void)
{
#include "test_cluster_normal_cold_postjobs.inc"
}

static void
startup_actual_finish(void)
{
#include "test_cluster_hw_complete_tail.inc"
}
#pragma pop_macro("ereport")

static void
startup_boundary_reset(void)
{
	startup_prepared_input = 0;
	startup_tt_capture_calls = startup_confirm_calls = 0;
	startup_resolve_calls = startup_old_scn_calls = startup_rollback_calls = 0;
	startup_backup_calls = startup_cf_calls = startup_hw_calls = 0;
	startup_fatal_level = 0;
	startup_cf_ok = startup_hw_ok = true;
}

UT_TEST(test_normal_actual_own_wal_capture_not_peer_control)
{
	ut_normal_start_setup();
	startup_boundary_reset();
	normal_start_checkpoint_captured = normal_start_checkpoint_invalid = false;
	memset(&normal_start_checkpoint, 0, sizeof(normal_start_checkpoint));
	startup_actual_capture(false, 0x90, 0x80, 2048, 200);
	UT_ASSERT(!normal_start_checkpoint_captured);
	UT_ASSERT_EQ(startup_tt_capture_calls, 0);
	startup_actual_capture(true, 0x90, 0x80, 2048, 200);
	UT_ASSERT(normal_start_checkpoint_captured);
	UT_ASSERT_EQ(startup_tt_capture_calls, 1);
	UT_ASSERT_EQ(normal_start_checkpoint.lsn, 0x90);
	UT_ASSERT_EQ(normal_start_checkpoint.redo, 0x80);
	UT_ASSERT_EQ(normal_start_checkpoint.next_full_xid, 2048);
	UT_ASSERT_EQ(normal_start_checkpoint.scn, 200);
	/* A later shared/adopted or changed value cannot replace the own record. */
	startup_actual_capture(true, 0xa0, 0x90, 4096, 300);
	UT_ASSERT(normal_start_checkpoint_invalid);
	UT_ASSERT_EQ(normal_start_checkpoint.lsn, 0x90);
	test_gate_reset();
}

UT_TEST(test_normal_actual_prepare_uses_all_original_clean_inputs)
{
	int input;
	for (input = 0; input < 7; input++) {
		volatile bool caught = false;
		ut_normal_start_setup();
		startup_boundary_reset();
		startup_prepared_input = input == 6 ? 1 : 0;
		PG_TRY();
		{
			UT_ASSERT_EQ(startup_actual_prepare(input == 1 ? DB_IN_PRODUCTION : DB_SHUTDOWNED,
												input != 2, input == 3, input == 4, input == 5),
						 41);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(startup_confirm_calls, 1);
		UT_ASSERT_EQ(startup_confirm_clean, input == 0 || input == 6);
		UT_ASSERT_EQ(startup_confirm_prepared, startup_prepared_input);
		UT_ASSERT_EQ(caught, input == 6);
		UT_ASSERT_EQ(cluster_semantic_normal_start_state(), input == 0 ? 3 : input == 6 ? 5 : 1);
		if (input == 6)
			UT_ASSERT_EQ(startup_fatal_level, FATAL);
		test_gate_reset();
	}
}

UT_TEST(test_normal_actual_postjobs_skip_only_classified_normal_target)
{
	int state;
	for (state = 1; state <= 3; state++) {
		ut_normal_start_setup();
		startup_boundary_reset();
		pg_atomic_write_u32(&NormalStartCompletion->state, state);
		startup_actual_postjobs();
		UT_ASSERT_EQ(startup_resolve_calls, state == 3 ? 0 : 1);
		UT_ASSERT_EQ(startup_old_scn_calls, state == 3 ? 0 : 1);
		UT_ASSERT_EQ(startup_rollback_calls, state == 3 ? 0 : 1);
		UT_ASSERT_EQ(startup_backup_calls, 1);
		test_gate_reset();
	}
}

UT_TEST(test_normal_actual_finish_requires_cf_and_hw_and_keeps_disk_history)
{
	int fault;
	for (fault = 0; fault < 3; fault++) {
		const char *failure;
		PGAlignedBlock before, after;
		ClusterNormalStartSnapshot ready;
		volatile bool caught = false;
		ut_cold_setup(1);
		ut_cold_file(255, TT_SLOT_COMMITTED);
		startup_boundary_reset();
		UT_ASSERT(cold_read_header(512, 2, before.data));
		UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
		startup_cf_ok = fault != 1;
		startup_hw_ok = fault != 2;
		PG_TRY();
		{
			startup_actual_finish();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, fault != 0);
		UT_ASSERT_EQ(startup_cf_calls, 1);
		UT_ASSERT_EQ(startup_hw_calls, fault == 1 ? 0 : 1);
		UT_ASSERT_EQ(cluster_semantic_normal_start_snapshot(&ready), fault == 0);
		UT_ASSERT_EQ(cold_read_count[255], fault == 0 ? 1 : 0);
		UT_ASSERT(cold_read_header(512, 2, after.data));
		UT_ASSERT_EQ(memcmp(before.data, after.data, BLCKSZ), 0);
		if (fault == 0) {
			UT_ASSERT_EQ(ready.census_count, 1);
			UT_ASSERT_EQ(ready.tt_commit_scn_max, 355);
			UT_ASSERT_EQ(cold_observed_scn, 355);
			UT_ASSERT(!semantic_activation_restart.opened);
		} else
			UT_ASSERT_EQ(startup_fatal_level, FATAL);
		ut_cold_cleanup();
	}
}

UT_TEST(test_normal_actual_finish_failure_is_fatal_not_ready)
{
	const char *failure;
	ClusterNormalStartSnapshot ready;
	volatile bool caught = false;
	ut_cold_setup(1);
	ut_cold_file(0, TT_SLOT_ACTIVE);
	startup_boundary_reset();
	UT_ASSERT(cluster_semantic_normal_start_prepare(true, 0, &failure));
	PG_TRY();
	{
		startup_actual_finish();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(startup_fatal_level, FATAL);
	UT_ASSERT_EQ(cluster_semantic_normal_start_state(), CLUSTER_NORMAL_START_FAILED);
	UT_ASSERT(!cluster_semantic_normal_start_snapshot(&ready));
	UT_ASSERT_EQ(cold_read_count[0], 0);
	UT_ASSERT(!cluster_undo_block0_backend_has_resources());
	ut_cold_cleanup();
}

UT_TEST(test_normal_actual_finish_preserves_unconfigured_native_startup)
{
	int mode;
	for (mode = 0; mode < 4; mode++) {
		volatile bool caught = false;
		ut_normal_start_setup();
		startup_boundary_reset();
		NormalStartCompletion = NULL; /* native standalone never initializes it */
		MyAuxProcType = NotAnAuxProcess;
		cluster_enabled = mode != 0;
		cluster_shared_data_dir = mode == 1 ? NULL : mode == 2 ? "" : "/cluster-share";
		PG_TRY();
		{
			startup_actual_finish();
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT_EQ(caught, mode == 3);
		UT_ASSERT_EQ(startup_cf_calls, 1);
		UT_ASSERT_EQ(startup_hw_calls, 1);
		test_gate_reset();
	}
}
