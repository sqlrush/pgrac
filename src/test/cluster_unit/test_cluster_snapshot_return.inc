/* Actual snapmgr containers and original release paths. PGPROC/role, epoch,
 * allocation, ResourceOwner bookkeeping and file operations are host inputs.
 * No cross-process producer closure or receipt is asserted.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
bool IsUnderPostmaster = true;
BackendType MyBackendType = B_BACKEND;
BackendId MyBackendId = 1;
volatile uint32 CritSectionCount;
static unsigned snapshot_return_fault;

static void
snapshot_return_begin(void)
{
	AtEOXact_Snapshot(false, true);
	UT_ASSERT(!ActiveSnapshotSet()); UT_ASSERT_EQ(remembered, 0);
	IsUnderPostmaster = true; MyBackendType = B_BACKEND; MyBackendId = 1;
	MyProc = &proc; MyProcPid = proc.pid = 1234; proc.pgprocno = 0;
	proc.backendId = 1; proc.cluster_grd_generation = 11;
	cluster_enabled = cluster_shared_config = true;
	CritSectionCount = 0; snapshot_return_epoch = 7; XactIsoLevel = XACT_READ_COMMITTED;
	/* Explicit single-backend host roster; not a producer closure proof. */
	procArray->numProcs = 1; procArray->pgprocnos[0] = 0;
	snapshot_return_schedule = NULL;
}
static ClusterServiceSnapshotObservation
snapshot_return_read(uint32 flags, uint64 retained)
{
	ClusterServiceSnapshotObservation out;
	UT_ASSERT_EQ(cluster_snapshot_service_observe(7, &out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.known); UT_ASSERT_EQ(out.references, flags);
	UT_ASSERT_EQ(out.retained_read_scn, retained);
	UT_ASSERT_EQ(out.identity.pid, 1234); UT_ASSERT_EQ(out.identity.pgprocno, 0);
	UT_ASSERT_EQ(out.identity.backend_id, 1); UT_ASSERT_EQ(out.identity.proc_generation, 11);
	UT_ASSERT_EQ(out.identity.epoch, 7); UT_ASSERT_EQ(out.identity.actor_bit, 0);
	return out;
}
UT_TEST(original_registered_snapshot_requires_independent_return_observation)
{
	Snapshot held, actual = NULL;
	SCN floor; const char *reason;
	snapshot_return_begin(); held = registered(100);
	/* The original foreign-read proof still requires the actual evaluator. */
	UT_ASSERT(!cluster_snapshot_read_evidence_v1(100, &actual, &floor, &reason));
	UT_ASSERT(actual == NULL && reason != NULL);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_REGISTERED, 100);
	UT_ASSERT_EQ(held->regd_count, 1); UT_ASSERT_EQ(remembered, 1);
	UnregisterSnapshot(held);
	(void)snapshot_return_read(0, 0); UT_ASSERT_EQ(remembered, 0);
}
UT_TEST(snapshot_return_keeps_active_refs_after_original_unregister)
{
	Snapshot held;
	snapshot_return_begin(); held = registered(70);
	PushActiveSnapshot(held); PushActiveSnapshot(held);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_ACTIVE | CLUSTER_SERVICE_SNAPSHOT_REGISTERED, 70);
	UnregisterSnapshot(held);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_ACTIVE, 70);
	UT_ASSERT_EQ(remembered, 0); UT_ASSERT_EQ(held->active_count, 2);
	PopActiveSnapshot(); (void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_ACTIVE, 70);
	PopActiveSnapshot(); /* Native final pop may free held: never dereference it again. */
	(void)snapshot_return_read(0, 0);
}
UT_TEST(snapshot_return_keeps_catalog_until_original_invalidation)
{
	Snapshot catalog;
	snapshot_return_begin(); snapshot_clock = 100;
	catalog = GetNonHistoricCatalogSnapshot(1);
	UT_ASSERT(catalog != NULL);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_CATALOG | CLUSTER_SERVICE_SNAPSHOT_REGISTERED, 100);
	UT_ASSERT(GetNonHistoricCatalogSnapshot(1) == catalog);
	InvalidateCatalogSnapshot(); (void)snapshot_return_read(0, 0);
}
UT_TEST(snapshot_return_keeps_transaction_and_exported_refs_until_eoxact)
{
	SnapshotData source;
	Snapshot transaction;
	char *name;
	snapshot_return_begin(); (void)GetTransactionSnapshot();
	UT_ASSERT(FirstSnapshotSet);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_XACT_STARTED, 0);
	AtEOXact_Snapshot(false, true); (void)snapshot_return_read(0, 0);
	XactIsoLevel = XACT_REPEATABLE_READ; snapshot_clock = 100;
	transaction = GetTransactionSnapshot();
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_TRANSACTION | CLUSTER_SERVICE_SNAPSHOT_REGISTERED
		| CLUSTER_SERVICE_SNAPSHOT_XACT_STARTED, 100);
	UT_ASSERT_EQ(transaction->regd_count, 1);
	AtEOXact_Snapshot(false, true); (void)snapshot_return_read(0, 0);
	pfree(transaction); /* Fixture's TopTransactionContext allocation leaf. */
	XactIsoLevel = XACT_READ_COMMITTED;
	source = snapshot_export_source(); export_expected_h = 40;
	name = ExportSnapshot(&source);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_EXPORTED | CLUSTER_SERVICE_SNAPSHOT_REGISTERED, 40);
	pfree(name); snapshot_export_cleanup();
	(void)snapshot_return_read(0, 0);
}
UT_TEST(snapshot_return_keeps_historic_and_evaluator_owners)
{
	SnapshotData historic = { 0 };
	Snapshot held;
	ClusterSnapshotReadScopeV1 scope;
	snapshot_return_begin(); historic.snapshot_type = SNAPSHOT_HISTORIC_MVCC;
	SetupHistoricSnapshot(&historic, NULL);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_HISTORIC, 0);
	TeardownHistoricSnapshot(false); (void)snapshot_return_read(0, 0);
	held = registered(60); cluster_snapshot_read_enter_v1(&scope, held);
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_EVALUATOR | CLUSTER_SERVICE_SNAPSHOT_REGISTERED, 60);
	UnregisterSnapshot(held); /* Scope remains but no longer owns a live snapshot. */
	(void)snapshot_return_read(CLUSTER_SERVICE_SNAPSHOT_EVALUATOR, 0);
	cluster_snapshot_read_exit_v1(&scope); (void)snapshot_return_read(0, 0);
}
static void
snapshot_return_mutate(void)
{
	snapshot_return_schedule = NULL;
	switch (snapshot_return_fault) {
	case 0: MyProcPid++; break;
	case 1: proc.cluster_grd_generation++; break;
	case 2: proc.backendId++; break;
	case 3: MyBackendType = B_LMON; break;
	case 4: IsUnderPostmaster = false; break;
	case 5: snapshot_return_epoch++; break;
	case 6: CritSectionCount++; break;
	case 7: MyProc = &snapshot_procs[1]; break;
	}
}
UT_TEST(snapshot_return_rejects_wrong_original_backend_identity)
{
	for (unsigned fault = 0; fault < 14; fault++) {
		ClusterServiceSnapshotObservation out, zero = { 0 };
		snapshot_return_begin();
		switch (fault) {
		case 0: IsUnderPostmaster = false; break;
		case 1: MyBackendType = B_LMON; break;
		case 2: MyProc = NULL; break;
		case 3: MyProcPid = 0; break;
		case 4: proc.pid++; break;
		case 5: proc.pgprocno = -1; break;
		case 6: MyBackendId = InvalidBackendId; break;
		case 7: proc.backendId = 2; break;
		case 8: proc.cluster_grd_generation = 0; break;
		case 9: proc.cluster_grd_generation = UINT64_MAX; break;
		case 10: CritSectionCount = 1; break;
		case 11: cluster_enabled = false; break;
		case 12: cluster_shared_config = false; break;
		case 13: MyBackendId = proc.backendId = 0; break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_snapshot_service_observe(7, &out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		/* Restore host identity before original EOXact retirement. */
		MyProc = &proc; MyProcPid = proc.pid = 1234;
	}
	snapshot_return_begin();
}
UT_TEST(snapshot_return_rechecks_identity_without_keeping_stale_samples)
{
	for (snapshot_return_fault = 0; snapshot_return_fault < 8; snapshot_return_fault++) {
		ClusterServiceSnapshotObservation out, zero = { 0 };
		Snapshot held;
		snapshot_return_begin(); held = registered(50);
		snapshot_return_schedule = snapshot_return_mutate;
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_snapshot_service_observe(7, &out), snapshot_return_fault == 5
			? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE : CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		UT_ASSERT_EQ(held->regd_count, 1); UT_ASSERT_EQ(remembered, 1);
		MyProc = &proc; MyProcPid = proc.pid = 1234;
		UnregisterSnapshot(held);
	}
	snapshot_return_begin();
}
UT_TEST(snapshot_return_never_closes_an_empty_or_unknown_producer)
{
	ClusterServiceSnapshotObservation out, zero = { 0 };
	snapshot_return_begin(); out = snapshot_return_read(0, 0);
	UT_ASSERT_EQ(strcmp(out.reason, "SNAPSHOT_PRODUCER_CLOSURE_UNPROVED"), 0);
	pg_atomic_write_u64(&proc.cluster_read_scn_atomic, UINT64_MAX);
	out = snapshot_return_read(0, UINT64_MAX);
	UT_ASSERT_EQ(strcmp(out.reason, "SNAPSHOT_RETENTION_PENDING"), 0);
	UT_ASSERT_EQ(pg_atomic_read_u64(&proc.cluster_read_scn_atomic), UINT64_MAX);
	pg_atomic_write_u64(&proc.cluster_read_scn_atomic, 0);
	for (unsigned i = 0; i < 3; i++) {
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(cluster_snapshot_service_observe(i == 0 ? 0 : i == 1 ? UINT64_MAX : 8, &out),
			i == 2 ? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE : CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
	}
	UT_ASSERT_EQ(cluster_snapshot_service_observe(7, NULL), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
	cluster_shared_config = false;
}
