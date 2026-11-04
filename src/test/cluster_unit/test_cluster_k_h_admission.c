/* PGRAC: exercise the native H cut and snapshot publication together.
 * The included fixture controls only external storage/process boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
int k_inherited_snapshot_main(void);
#define main k_inherited_snapshot_main
#include "test_cluster_snapshot_admission.c"
#undef main

static void
k_new_procarray_boot(void)
{
	UT_ASSERT(!ActiveSnapshotSet() && CatalogSnapshot == NULL);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT_EQ(remembered, 0);
	free(procArray);
	procArray = calloc(1, sizeof(ProcArrayStruct) + 2 * sizeof(int));
	if (procArray == NULL)
		abort();
	pg_atomic_init_u32(&procArray->cluster_unknown_history_sources, 0);
	procArray->maxProcs = 2;
	procArray->numProcs = 1;
	procArray->pgprocnos[1] = 1;
	memset(snapshot_procs, 0, sizeof(snapshot_procs));
	MyProc = &proc;
	proc.pid = 1234;
	pg_atomic_init_u64(&proc.cluster_read_scn_atomic, InvalidScn);
	snapshot_clock = 100;
	error_code = 0;
}

/* The native entry must reject before ownership/container/PGPROC effects.
 * On the old product, retire the unexpectedly admitted snapshot so each
 * failing test still leaves the next test a valid original fixture. */
static bool
k_old_registration_refused(SCN read_scn)
{
	volatile Snapshot admitted = NULL;
	volatile bool caught = false;
	SCN before = pg_atomic_read_u64(&proc.cluster_read_scn_atomic);
	unsigned owners = remembered;

	PG_TRY();
	{
		admitted = registered(read_scn);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	if (caught) {
		UT_ASSERT_EQ(error_code, ERRCODE_CLUSTER_SNAPSHOT_TOO_OLD);
		UT_ASSERT_EQ(remembered, owners);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), before);
	}
	if (admitted != NULL)
		UnregisterSnapshot(admitted);
	return caught;
}

UT_TEST(k_native_cut_rejects_later_old_registered_reader)
{
	ClusterUndoRetentionSample cut;
	Snapshot original;

	k_new_procarray_boot();
	original = registered(40);
	UT_ASSERT(catalog_history_sample(&cut));
	UT_ASSERT_EQ(cut.horizon_scn, 40);
	UnregisterSnapshot(original);
	UT_ASSERT(catalog_history_sample(&cut));
	UT_ASSERT_EQ(cut.horizon_scn, 100);
	UT_ASSERT(k_old_registration_refused(60));
}

UT_TEST(k_native_promise_survives_unlock_and_source_retirement)
{
	ClusterUndoRetentionSample cut;
	Snapshot original, equal;

	k_new_procarray_boot();
	original = registered(80);
	UT_ASSERT(catalog_history_sample(&cut));
	UT_ASSERT(!retention_lock);
	UnregisterSnapshot(original);
	/* No replacement sample after unlocking/retiring the original source. */
	UT_ASSERT(k_old_registration_refused(79));
	equal = registered(80);
	UT_ASSERT_EQ(equal->regd_count, 1);
	UnregisterSnapshot(equal);
}

UT_TEST(k_unknown_reader_cannot_hide_a_later_old_registration)
{
	ClusterUndoRetentionSample cut;
	Snapshot unknown;

	k_new_procarray_boot();
	UT_ASSERT(catalog_history_sample(&cut));
	/* GetSnapshotData owns xmin publication; this fixture supplies that leaf. */
	proc.xmin = 10;
	unknown = registered(InvalidScn);
	UT_ASSERT(!catalog_history_sample(&cut));
	UT_ASSERT(k_old_registration_refused(99));
	UT_ASSERT_EQ(unknown->regd_count, 1);
	UnregisterSnapshot(unknown);
	proc.xmin = InvalidTransactionId;
	UT_ASSERT(k_old_registration_refused(99));
}

UT_TEST(k_transaction_cleanup_does_not_revoke_native_promise)
{
	ClusterUndoRetentionSample cut;
	Snapshot original;

	k_new_procarray_boot();
	original = registered(80);
	PushActiveSnapshot(original);
	UT_ASSERT(catalog_history_sample(&cut));
	/* Abort releases ResourceOwner registrations before AtEOXact_Snapshot.
	 * Only the original snapshot cleanup is under test here. */
	UnregisterSnapshot(original);
	AtEOXact_Snapshot(false, true);
	UT_ASSERT(!ActiveSnapshotSet() && pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT_EQ(remembered, 0);
	UT_ASSERT(k_old_registration_refused(79));
}

int
main(void)
{
	ProcGlobal = &snapshot_proc_header;
	ProcGlobal->statusFlags = snapshot_status_flags;
	UndoHorizonShmem = &horizon;
	pg_atomic_init_u64(&horizon.self_admitted_epoch, 8);
	pg_atomic_init_u64(&horizon.admission_refuse_count, 0);
	oldSnapshotControl = &snapshot_control;
	pg_atomic_init_u64(&snapshot_control.cr_identity_generation, 0);
	UT_PLAN(4);
	UT_RUN(k_native_cut_rejects_later_old_registered_reader);
	UT_RUN(k_native_promise_survives_unlock_and_source_retirement);
	UT_RUN(k_unknown_reader_cannot_hide_a_later_old_registration);
	UT_RUN(k_transaction_cleanup_does_not_revoke_native_promise);
	UT_DONE();
	free(procArray);
	return ut_failed_count != 0;
}
