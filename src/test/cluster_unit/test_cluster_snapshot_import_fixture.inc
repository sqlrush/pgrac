/* PGRAC: original snapshot import/restoration -> original H sampler.
 * Exported bytes, shared-memory storage and lock scheduling are controlled;
 * the included snapmgr, xmin installers, comparator and sampler are actual
 * product bodies. This is neither native concurrency nor global retention
 * qualification. Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_snapshot_import_compare.inc"

int
GetMaxSnapshotXidCount(void)
{
	return 16;
}

int
GetMaxSnapshotSubxidCount(void)
{
	return 16;
}

void
SetSerializableTransactionSnapshot(Snapshot snapshot, VirtualTransactionId *vxid, int pid)
{
	/* These cases select repeatable-read; predicate locks are not exercised. */
	abort();
}

static void
snapshot_import_after_procarray_unlock(void)
{
	ClusterUndoRetentionSample sample, zero = { 0 };

	if (!import_probe_after_install)
		return;
	import_probe_after_install = false;
	UT_ASSERT(!retention_lock);
	import_gap_xmin_installed = proc.xmin == 20 && TransactionXmin == 20;
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	/* Schedule the original source's exit after xmin installation and before
	 * CopySnapshot/registration/publication in SetTransactionSnapshot. */
	procArray->numProcs = 1;
	snapshot_procs[1].xmin = InvalidTransactionId;
	pg_atomic_write_u64(&snapshot_procs[1].cluster_read_scn_atomic, InvalidScn);
	memset(&sample, 0xa5, sizeof(sample));
	import_gap_refused = !catalog_history_sample(&sample)
		&& memcmp(&sample, &zero, sizeof(sample)) == 0;
}

static SnapshotData
snapshot_import_begin(void)
{
	SnapshotData source = { 0 };
	PGPROC *exporter = &snapshot_procs[1];

	UT_ASSERT(!FirstSnapshotSet && FirstXactSnapshot == NULL);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(ActiveSnapshot == NULL && CatalogSnapshot == NULL);
	memset(exporter, 0, sizeof(*exporter));
	pg_atomic_init_u64(&exporter->cluster_read_scn_atomic, 40);
	exporter->pid = 5678;
	exporter->backendId = 2;
	exporter->lxid = 3;
	exporter->databaseId = MyDatabaseId;
	exporter->xmin = 15;
	exporter->pgxactoff = 1;
	proc.backendId = 1;
	proc.lxid = 1;
	proc.databaseId = MyDatabaseId;
	proc.pgxactoff = 0;
	proc.statusFlags = snapshot_status_flags[0] = 0;
	snapshot_status_flags[1] = 0;
	procArray->numProcs = 2;
	source.snapshot_type = SNAPSHOT_MVCC;
	source.xmin = 20;
	source.xmax = 200;
	source.cluster_source = SNAPSHOT_SOURCE_CLUSTER;
	source.read_scn = 40;
	source.read_epoch = 8;
	source.cluster_snapshot_session_local = 1;
	XactIsoLevel = XACT_REPEATABLE_READ;
	import_install_active = true;
	import_probe_after_install = false;
	import_gap_refused = import_gap_xmin_installed = false;
	return source;
}

static void
snapshot_import_end(void)
{
	Snapshot owned = FirstXactSnapshot;
	ClusterUndoRetentionSample sample;

	import_install_active = import_probe_after_install = false;
	AtEOXact_Snapshot(false, true);
	/* TopTransactionContext is a fixture allocation leaf; native cleanup
	 * has already removed its private reference before this memory is freed. */
	if (owned != NULL)
		pfree(owned);
	procArray->numProcs = 1;
	proc.statusFlags = snapshot_status_flags[0] = 0;
	UT_ASSERT_EQ(proc.xmin, InvalidTransactionId);
	UT_ASSERT_EQ(TransactionXmin, InvalidTransactionId);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	UT_ASSERT(!FirstSnapshotSet && FirstXactSnapshot == NULL);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, snapshot_clock);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK);
	XactIsoLevel = XACT_READ_COMMITTED;
}

static void
snapshot_import_success(bool restored)
{
	SnapshotData source = snapshot_import_begin();
	VirtualTransactionId vxid = { 2, 3 };
	ClusterUndoRetentionSample sample;

	import_restored = restored;
	if (restored)
	{
		snapshot_procs[1].statusFlags = PROC_IN_SAFE_IC;
		snapshot_status_flags[1] = PROC_IN_SAFE_IC;
	}
	/* Before receiver xmin publication, the original source owns H=40. */
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 40);
	UT_ASSERT_EQ(sample.contributor_token, 2);
	import_probe_after_install = true;
	SetTransactionSnapshot(&source, &vxid, 5678, restored ? &snapshot_procs[1] : NULL);
	UT_ASSERT_EQ(import_install_mode, restored ? LW_EXCLUSIVE : LW_SHARED);
	UT_ASSERT(import_gap_xmin_installed && import_gap_refused);
	UT_ASSERT_EQ(procArray->numProcs, 1);
	UT_ASSERT(FirstSnapshotSet && CurrentSnapshot == FirstXactSnapshot);
	UT_ASSERT(CurrentSnapshot != &source && CurrentSnapshot != &CurrentSnapshotData);
	UT_ASSERT_EQ(CurrentSnapshot->cluster_snapshot_session_local, 0);
	UT_ASSERT_EQ(CurrentSnapshot->read_epoch, 8);
	UT_ASSERT_EQ(CurrentSnapshot->read_scn, 40);
	UT_ASSERT_EQ(CurrentSnapshot->regd_count, 1);
	UT_ASSERT_EQ(proc.xmin, 20);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 40);
	UT_ASSERT_EQ(proc.statusFlags & PROC_XMIN_FLAGS, restored ? PROC_IN_SAFE_IC : 0);
	UT_ASSERT_EQ(snapshot_status_flags[0], proc.statusFlags);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 40);
	UT_ASSERT_EQ(sample.clock_scn, snapshot_clock);
	UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	UT_ASSERT_EQ(sample.contributor_token, 1);
	snapshot_import_end();
}

UT_TEST(imported_snapshot_original_xmin_handoff_survives_source_exit)
{
	snapshot_import_success(false);
}

UT_TEST(restored_snapshot_original_xmin_handoff_survives_source_exit)
{
	snapshot_import_success(true);
}

static void
snapshot_import_refused(bool restored, unsigned fault)
{
	SnapshotData source = snapshot_import_begin();
	VirtualTransactionId vxid = { 2, 3 };
	ClusterUndoRetentionSample sample, zero = { 0 };
	volatile bool caught = false;

	import_restored = restored;
	switch (fault)
	{
		case 0: snapshot_procs[1].databaseId++; break;
		case 1: snapshot_procs[1].xmin = InvalidTransactionId; break;
		case 2: snapshot_procs[1].xmin = 21; break;
		case 3: vxid.localTransactionId++; break;
		case 4: vxid.backendId++; break;
		case 5: snapshot_status_flags[1] = PROC_IN_VACUUM; break;
		case 6: procArray->numProcs = 1; break;
		case 7: break; /* NULL original VXID is rejected before locking. */
		default: abort();
	}
	error_code = 0;
	PG_TRY();
	{
		SetTransactionSnapshot(&source, fault == 7 ? NULL : &vxid, 5678,
							   restored ? &snapshot_procs[1] : NULL);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(error_code, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
	UT_ASSERT(!retention_lock && !FirstSnapshotSet && FirstXactSnapshot == NULL);
	UT_ASSERT(pairingheap_is_empty(&RegisteredSnapshots));
	UT_ASSERT_EQ(proc.xmin, 10); /* original GetSnapshotData leaf's initial xmin */
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), InvalidScn);
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!catalog_history_sample(&sample));
	UT_ASSERT(memcmp(&sample, &zero, sizeof(sample)) == 0);
	snapshot_import_end();
}

UT_TEST(imported_snapshot_rejects_missing_or_unprotected_original_source)
{
	for (unsigned fault = 0; fault < 8; fault++)
		snapshot_import_refused(false, fault);
}

UT_TEST(restored_snapshot_rejects_unprotected_original_source)
{
	for (unsigned fault = 0; fault < 3; fault++)
		snapshot_import_refused(true, fault);
}

/* A receiver may still advertise a finite SCN from a previous local point.
 * Installing an older native xmin must not lend that SCN to the imported
 * snapshot while its original snapmgr registration has not yet published. */
static void
snapshot_import_replaces_prior_scn(bool restored)
{
	SnapshotData source = snapshot_import_begin();
	VirtualTransactionId vxid = { 2, 3 };
	ClusterUndoRetentionSample sample, zero = { 0 };
	Snapshot owned;
	bool gap_refused;

	import_restored = restored;
	proc.xmin = TransactionXmin = 80;
	pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 100);
	import_observe_install = true;
	UT_ASSERT(restored ? ProcArrayInstallRestoredXmin(source.xmin, &snapshot_procs[1])
		: ProcArrayInstallImportedXmin(source.xmin, &vxid));
	UT_ASSERT_EQ(proc.xmin, 20);
	UT_ASSERT_EQ(TransactionXmin, 20);
	UT_ASSERT_EQ(import_install_mode, restored ? LW_EXCLUSIVE : LW_SHARED);
	UT_ASSERT(!retention_lock);
	/* End the old source after the actual installer has returned. The
	 * receiver owns xmin, but does not yet own a registered SCN snapshot. */
	procArray->numProcs = 1;
	snapshot_procs[1].xmin = InvalidTransactionId;
	pg_atomic_write_u64(&snapshot_procs[1].cluster_read_scn_atomic, InvalidScn);
	memset(&sample, 0xa5, sizeof(sample));
	gap_refused = !catalog_history_sample(&sample)
		&& memcmp(&sample, &zero, sizeof(sample)) == 0
		&& pg_atomic_read_u64(&proc.cluster_read_scn_atomic) == InvalidScn
		&& import_scn_at_unlock == InvalidScn;
	/* Only original snapmgr registration can complete this history handoff.
	 * No installer copies an SCN from a raw XID or another backend. */
	owned = RegisterSnapshot(&source);
	UT_ASSERT(catalog_history_sample(&sample));
	UT_ASSERT_EQ(sample.horizon_scn, 40);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 40);
	UnregisterSnapshot(owned);
	snapshot_import_end();
	UT_ASSERT(gap_refused); /* after original cleanup, including parent RED */
}

UT_TEST(imported_xmin_does_not_borrow_the_receivers_previous_scn)
{
	snapshot_import_replaces_prior_scn(false);
}

UT_TEST(restored_xmin_does_not_borrow_the_receivers_previous_scn)
{
	snapshot_import_replaces_prior_scn(true);
}

UT_TEST(refused_xmin_install_preserves_the_receivers_original_scn)
{
	for (int restored = 0; restored < 2; restored++) {
		for (int fault = 0; fault < 3; fault++) {
			SnapshotData source = snapshot_import_begin();
			VirtualTransactionId vxid = { 2, 3 };

			import_restored = restored;
			proc.xmin = TransactionXmin = 80;
			pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 100);
			import_observe_install = true;
			if (fault == 0) snapshot_procs[1].databaseId++;
			if (fault == 1) snapshot_procs[1].xmin = InvalidTransactionId;
			if (fault == 2) snapshot_procs[1].xmin = 21;
			UT_ASSERT(!(restored ? ProcArrayInstallRestoredXmin(source.xmin, &snapshot_procs[1])
				: ProcArrayInstallImportedXmin(source.xmin, &vxid)));
			UT_ASSERT_EQ(proc.xmin, 80);
			UT_ASSERT_EQ(TransactionXmin, 80);
			UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), 100);
			UT_ASSERT_EQ(import_scn_at_unlock, 100);
			UT_ASSERT(!retention_lock);
			snapshot_import_end();
		}
	}
}
