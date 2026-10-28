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
	/* Each independently constructed source models a new ProcArray boot. */
	procArray->cluster_read_scn_floor = InvalidScn;
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

/* Exercise the real parallel carry, not a hand-built restored snapshot. */
static Snapshot
snapshot_parallel_restore(Snapshot source)
{
	Size size = EstimateSnapshotSpace(source);
	char *bytes = palloc(size);
	Snapshot restored;

	SerializeSnapshot(source, bytes);
	poison_allocations = true;
	restored = RestoreSnapshot(bytes);
	poison_allocations = false;
	pfree(bytes);
	return restored;
}

UT_TEST(parallel_serialized_snapshot_retains_original_handoff)
{
	for (unsigned shape = 0; shape < 4; shape++)
	{
		SnapshotData source = snapshot_import_begin();
		TransactionId xip[2] = { 25, 30 }, subxip[2] = { 26, 31 };
		Snapshot restored;
		ClusterUndoRetentionSample sample;
		unsigned expected_subxcnt;

		source.xip = xip;
		source.xcnt = lengthof(xip);
		source.subxip = subxip;
		source.subxcnt = lengthof(subxip);
		source.suboverflowed = (shape & 1) != 0;
		source.takenDuringRecovery = (shape & 2) != 0;
		source.curcid = 17;
		source.whenTaken = 12345;
		source.lsn = 67890;
		source.cluster_cr_identity = 99;
		expected_subxcnt = source.suboverflowed && !source.takenDuringRecovery ? 0 : 2;
		UT_ASSERT_EQ(EstimateSnapshotSpace(&source), sizeof(SerializedSnapshotData)
			+ (2 + expected_subxcnt) * sizeof(TransactionId));
		restored = snapshot_parallel_restore(&source);
		UT_ASSERT_EQ(restored->xmin, 20);
		UT_ASSERT_EQ(restored->xmax, 200);
		UT_ASSERT_EQ(restored->xcnt, 2);
		UT_ASSERT_EQ(restored->subxcnt, expected_subxcnt);
		UT_ASSERT(restored->suboverflowed == source.suboverflowed);
		UT_ASSERT(restored->takenDuringRecovery == source.takenDuringRecovery);
		UT_ASSERT_EQ(restored->curcid, 17);
		UT_ASSERT_EQ(restored->whenTaken, 12345);
		UT_ASSERT_EQ(restored->lsn, 67890);
		UT_ASSERT_EQ(restored->cluster_source, SNAPSHOT_SOURCE_CLUSTER);
		UT_ASSERT_EQ(restored->read_scn, 40);
		UT_ASSERT_EQ(restored->read_epoch, 8);
		UT_ASSERT_EQ(restored->cluster_cr_identity, 0);
		UT_ASSERT_EQ(restored->cluster_snapshot_session_local, 0);
		UT_ASSERT(restored->copied && restored->regd_count == 0 && restored->active_count == 0);
		xip[0] = subxip[0] = 150;
		source.read_scn = 100; /* source storage may change after the carry */
		UT_ASSERT_EQ(restored->xip[0], 25);
		UT_ASSERT_EQ(restored->xip[1], 30);
		if (expected_subxcnt != 0)
		{
			UT_ASSERT_EQ(restored->subxip[0], 26);
			UT_ASSERT_EQ(restored->subxip[1], 31);
		}
		import_restored = true;
		import_probe_after_install = true;
		RestoreTransactionSnapshot(restored, &snapshot_procs[1]);
		UT_ASSERT_EQ(import_install_mode, LW_EXCLUSIVE);
		UT_ASSERT(import_gap_xmin_installed && import_gap_refused);
		UT_ASSERT_EQ(procArray->numProcs, 1);
		UT_ASSERT(FirstSnapshotSet && CurrentSnapshot == FirstXactSnapshot);
		UT_ASSERT(CurrentSnapshot != restored);
		UT_ASSERT_EQ(CurrentSnapshot->read_scn, 40);
		UT_ASSERT_EQ(CurrentSnapshot->read_epoch, 8);
		UT_ASSERT_EQ(CurrentSnapshot->xmin, 20);
		UT_ASSERT_EQ(CurrentSnapshot->xcnt, 2);
		UT_ASSERT_EQ(CurrentSnapshot->xip[0], 25);
		UT_ASSERT_EQ(CurrentSnapshot->subxcnt, expected_subxcnt);
		if (expected_subxcnt != 0)
			UT_ASSERT_EQ(CurrentSnapshot->subxip[0], 26);
		pfree(restored); /* registered transaction snapshot owns its own copy */
		UT_ASSERT(catalog_history_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 40);
		UT_ASSERT_EQ(sample.contributor_kind, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		UT_ASSERT_EQ(sample.contributor_token, 1);
		snapshot_import_end();
	}
}

UT_TEST(parallel_serialized_unknown_history_cannot_borrow_clock)
{
	const SCN unknown[] = { InvalidScn, (SCN) UINT64_MAX };

	for (unsigned i = 0; i < lengthof(unknown); i++)
	{
		SnapshotData source = snapshot_import_begin();
		Snapshot restored;
		ClusterUndoRetentionSample sample, zero = { 0 };

		source.read_scn = unknown[i];
		restored = snapshot_parallel_restore(&source);
		UT_ASSERT_EQ(restored->read_scn, unknown[i]);
		import_restored = true;
		import_probe_after_install = true;
		RestoreTransactionSnapshot(restored, &snapshot_procs[1]);
		UT_ASSERT(import_gap_xmin_installed && import_gap_refused);
		UT_ASSERT_EQ(CurrentSnapshot->read_scn, unknown[i]);
		UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), unknown[i]);
		pfree(restored);
		memset(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!catalog_history_sample(&sample));
		UT_ASSERT(memcmp(&sample, &zero, sizeof(sample)) == 0);
		snapshot_import_end();
	}
}

UT_TEST(parallel_serialized_copy_cannot_replace_original_source_admission)
{
	for (unsigned fault = 0; fault < 3; fault++)
	{
		SnapshotData source = snapshot_import_begin();
		Snapshot restored = snapshot_parallel_restore(&source);
		ClusterUndoRetentionSample sample, zero = { 0 };
		volatile bool caught = false;

		import_restored = true;
		if (fault == 0)
			snapshot_procs[1].databaseId++;
		else if (fault == 1)
			snapshot_procs[1].xmin = InvalidTransactionId;
		else
			snapshot_procs[1].xmin = 21;
		error_code = 0;
		PG_TRY();
		{
			RestoreTransactionSnapshot(restored, &snapshot_procs[1]);
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
		pfree(restored);
		/* Remove the refused source before evaluating the receiver alone. */
		procArray->numProcs = 1;
		memset(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!catalog_history_sample(&sample));
		UT_ASSERT(memcmp(&sample, &zero, sizeof(sample)) == 0);
		snapshot_import_end();
	}
}

/* A finite imported history point needs the original source's published
 * protection under the same native xmin handoff lock. A live source xmin
 * by itself cannot authenticate an older serialized SCN. */
static void
snapshot_import_uncovered_source_scn(bool restored)
{
	const SCN uncovered[] = { InvalidScn, (SCN) UINT64_MAX, 60 };

	for (unsigned i = 0; i < lengthof(uncovered); i++)
	{
		SnapshotData source = snapshot_import_begin();
		VirtualTransactionId vxid = { 2, 3 };
		volatile bool caught = false;
		bool refused_without_install;
		bool saved_shared = cluster_shared_config;

		cluster_shared_config = true;
		import_restored = restored;
		pg_atomic_write_u64(&snapshot_procs[1].cluster_read_scn_atomic, uncovered[i]);
		snapshot_procs[1].statusFlags = snapshot_status_flags[1] = PROC_IN_SAFE_IC;
		error_code = 0;
		PG_TRY();
		{
			SetTransactionSnapshot(&source, &vxid, 5678,
				restored ? &snapshot_procs[1] : NULL);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		refused_without_install = caught
			&& error_code == ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE
			&& !retention_lock && !FirstSnapshotSet && FirstXactSnapshot == NULL
			&& pairingheap_is_empty(&RegisteredSnapshots)
			&& proc.xmin == 10 && proc.statusFlags == 0
			&& snapshot_status_flags[0] == 0
			&& pg_atomic_read_u64(&proc.cluster_read_scn_atomic) == InvalidScn;
		snapshot_import_end();
		cluster_shared_config = saved_shared;
		UT_ASSERT(refused_without_install); /* clean up even with exact parent */
	}
}

UT_TEST(imported_finite_history_requires_original_source_scn_coverage)
{
	snapshot_import_uncovered_source_scn(false);
}

UT_TEST(restored_finite_history_requires_original_source_scn_coverage)
{
	snapshot_import_uncovered_source_scn(true);
}

UT_TEST(original_native_xmin_contract_and_older_source_protection_remain)
{
	for (unsigned restored = 0; restored < 2; restored++)
	{
		SnapshotData source = snapshot_import_begin();
		VirtualTransactionId vxid = { 2, 3 };
		ClusterUndoRetentionSample sample, zero = { 0 };
		bool saved_shared = cluster_shared_config;

		import_restored = restored != 0;
		/* Original non-SCN native callers retain their xmin-only contract.
		 * The receiver remains unknown; this cannot publish a finite H. */
		pg_atomic_write_u64(&snapshot_procs[1].cluster_read_scn_atomic, InvalidScn);
		UT_ASSERT(restored ? ProcArrayInstallRestoredXmin(source.xmin, &snapshot_procs[1])
			: ProcArrayInstallImportedXmin(source.xmin, &vxid));
		UT_ASSERT_EQ(proc.xmin, 20);
		memset(&sample, 0xa5, sizeof(sample));
		UT_ASSERT(!catalog_history_sample(&sample));
		UT_ASSERT(memcmp(&sample, &zero, sizeof(sample)) == 0);
		snapshot_import_end();

		source = snapshot_import_begin();
		cluster_shared_config = true;
		import_restored = restored != 0;
		/* A source retaining an older point covers the imported H40. */
		pg_atomic_write_u64(&snapshot_procs[1].cluster_read_scn_atomic, 39);
		import_probe_after_install = true;
		SetTransactionSnapshot(&source, &vxid, 5678,
			restored ? &snapshot_procs[1] : NULL);
		UT_ASSERT(import_gap_xmin_installed && import_gap_refused);
		UT_ASSERT(catalog_history_sample(&sample));
		UT_ASSERT_EQ(sample.horizon_scn, 40);
		UT_ASSERT_EQ(CurrentSnapshot->read_scn, 40);
		snapshot_import_end();
		cluster_shared_config = saved_shared;
	}
}
