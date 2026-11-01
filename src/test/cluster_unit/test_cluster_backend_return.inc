/* Same-backend snapmgr plus original shared gate. Roster, lock ownership and
 * shared publications/counters are explicit host inputs, not global closure.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterSemanticActivationShmem backend_return_gate;
static bool backend_return_lock_held;
static LWLockPadded backend_return_lock;
static unsigned backend_return_barriers, backend_return_at, backend_return_fault;
static Snapshot backend_return_changed_snapshot;

void
ForEachLWLockHeldByMe(void (*callback)(LWLock *, LWLockMode, void *), void *arg)
{
	if (backend_return_lock_held)
		callback(&backend_return_lock.lock, LW_SHARED, arg);
}

static void
backend_return_begin(void)
{
	/* Restore host identity before the original transaction-end publisher. */
	MyProc = &proc; MyProcPid = proc.pid = 1234;
	MyBackendType = B_BACKEND; MyBackendId = proc.backendId = 1;
	IsUnderPostmaster = true; CritSectionCount = 0;
	snapshot_return_begin();
	backend_return_lock_held = false;
	memset(&backend_return_gate, 0, sizeof(backend_return_gate));
	SemanticActivationShmem = &backend_return_gate;
	pg_atomic_init_u64(&backend_return_gate.admission_seq, 2);
	pg_atomic_init_u64(&backend_return_gate.record_generation, 19);
	pg_atomic_init_u64(&backend_return_gate.formation_epoch, 7);
	pg_atomic_init_u32(&backend_return_gate.transition_closed, 1);
	backend_return_barriers = 0;
}
static ClusterSemanticAdmissionObservationResult
backend_return_read(ClusterServiceBackendCollection *out)
{
	return cluster_service_backend_collect_return(snapshot_return_epoch, 7, 19, out);
}
static void
backend_return_empty(const ClusterServiceBackendCollection *out)
{
	ClusterServiceSnapshotObservation snapshot = { 0 };
	ClusterSemanticAdmissionObservation admission = { 0 };
	UT_ASSERT_EQ(memcmp(&out->snapshots, &snapshot, sizeof(snapshot)), 0);
	UT_ASSERT_EQ(memcmp(&out->admission, &admission, sizeof(admission)), 0);
	UT_ASSERT(out->reason != NULL);
}
UT_TEST(backend_collection_retains_original_snapshots_after_gate_drains)
{
	ClusterServiceBackendCollection out;
	ClusterSemanticAdmissionObservation gate;
	Snapshot held;

	backend_return_begin(); held = registered(100);
	UT_ASSERT_EQ(cluster_semantic_activation_admission_return_observe(7, 7, 19, &gate),
		CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_CURRENT);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.snapshots.known && out.admission.known);
	UT_ASSERT_EQ(out.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_REGISTERED);
	UT_ASSERT_EQ(out.snapshots.retained_read_scn, 100);
	UT_ASSERT_EQ(held->regd_count, 1); UT_ASSERT_EQ(remembered, 1);
	UnregisterSnapshot(held);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.snapshots.known && out.admission.known);
	UT_ASSERT_EQ(out.snapshots.references, 0); UT_ASSERT_EQ(out.snapshots.retained_read_scn, 0);
	UT_ASSERT_EQ(strcmp(out.reason, "BACKEND_COLLECTION_PRODUCER_CLOSURE_UNPROVED"), 0);
}
UT_TEST(backend_collection_keeps_active_catalog_and_evaluator_owners)
{
	ClusterServiceBackendCollection out;
	ClusterSnapshotReadScopeV1 scope;
	Snapshot held;

	backend_return_begin(); held = registered(60);
	PushActiveSnapshot(held); cluster_snapshot_read_enter_v1(&scope, held);
	(void)GetNonHistoricCatalogSnapshot(1);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_ACTIVE
		| CLUSTER_SERVICE_SNAPSHOT_REGISTERED | CLUSTER_SERVICE_SNAPSHOT_CATALOG
		| CLUSTER_SERVICE_SNAPSHOT_EVALUATOR);
	UnregisterSnapshot(held); InvalidateCatalogSnapshot();
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.snapshots.references, CLUSTER_SERVICE_SNAPSHOT_ACTIVE | CLUSTER_SERVICE_SNAPSHOT_EVALUATOR);
	UT_ASSERT_EQ(out.snapshots.retained_read_scn, 60);
	PopActiveSnapshot(); cluster_snapshot_read_exit_v1(&scope);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.snapshots.references, 0);
}
UT_TEST(backend_collection_preserves_all_original_shared_counter_cells)
{
	ClusterServiceBackendCollection out;

	backend_return_begin();
	for (unsigned side = 0; side < 2; side++)
		for (unsigned bit = 0; bit < 64; bit++) {
			uint32 count = bit == 63 ? UINT32_MAX : bit + 1;
			pg_atomic_write_u32(&backend_return_gate.inflight[side][bit], count);
			UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
			UT_ASSERT(out.snapshots.known && out.admission.known);
			UT_ASSERT_EQ(out.admission.inflight[side][bit], count);
			UT_ASSERT_EQ(pg_atomic_read_u32(&backend_return_gate.inflight[side][bit]), count);
			pg_atomic_write_u32(&backend_return_gate.inflight[side][bit], 0);
		}
}
UT_TEST(backend_collection_binds_original_gate_and_current_native_epochs)
{
	ClusterServiceBackendCollection out;

	backend_return_begin(); pg_atomic_write_u32(&backend_return_gate.transition_closed, 0);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.admission.known && !out.admission.transition_closed);
	snapshot_return_epoch = 8;
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT(out.snapshots.known && out.admission.known);
	UT_ASSERT_EQ(out.snapshots.identity.epoch, 8); UT_ASSERT_EQ(out.admission.formation_epoch, 7);
	UT_ASSERT_EQ(out.admission.record_generation, 19);
	pg_atomic_write_u64(&backend_return_gate.record_generation, 20);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE);
	backend_return_empty(&out);
}
UT_TEST(backend_collection_refuses_wrong_owner_scope_or_locked_context)
{
	ClusterServiceBackendCollection out;

	for (unsigned fault = 0; fault < 10; fault++) {
		backend_return_begin();
		switch (fault) {
		case 0: MyBackendType = B_LMON; break;
		case 1: MyProc = NULL; break;
		case 2: MyBackendId = InvalidBackendId; break;
		case 3: proc.backendId++; break;
		case 4: proc.cluster_grd_generation = 0; break;
		case 5: CritSectionCount = 1; break;
		case 6: backend_return_lock_held = true; break;
		case 7: cluster_shared_config = false; break;
		case 8: IsUnderPostmaster = false; break;
		case 9: proc.cluster_grd_generation = UINT64_MAX; break;
		}
		memset(&out, 0xa5, sizeof(out));
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		backend_return_empty(&out);
	}
	backend_return_begin();
	for (unsigned fault = 0; fault < 6; fault++) {
		UT_ASSERT_EQ(cluster_service_backend_collect_return(fault == 0 ? 0 : 7,
			fault == 1 ? 0 : fault == 2 ? 8 : 7, fault == 3 ? 0 : fault == 4 ? UINT64_MAX : 19,
			fault == 5 ? NULL : &out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		if (fault != 5) backend_return_empty(&out);
	}
}
static void
backend_return_mutate(void)
{
	if (++backend_return_barriers != backend_return_at) return;
	snapshot_return_schedule = NULL;
	switch (backend_return_fault) {
	case 0: snapshot_return_epoch++; break;
	case 1: MyProcPid++; break;
	case 2: proc.cluster_grd_generation++; break;
	case 3: MyBackendId++; break;
	case 4: proc.backendId++; break;
	case 5: proc.pgprocno++; break;
	case 6: MyProc = &snapshot_procs[1]; break;
	case 7: MyBackendType = B_LMON; break;
	case 8: CritSectionCount++; break;
	case 9: backend_return_lock_held = true; break;
	case 10: case 11: case 12: case 13:
		/* Explicit original shared publication, including its seqlock. */
		pg_atomic_write_u64(&backend_return_gate.admission_seq, 3);
		if (backend_return_fault == 10) pg_atomic_write_u64(&backend_return_gate.record_generation, 20);
		if (backend_return_fault == 12) pg_atomic_write_u32(&backend_return_gate.transition_closed, 0);
		if (backend_return_fault == 13) pg_atomic_write_u64(&backend_return_gate.active_bits, 1);
		pg_write_barrier(); pg_atomic_write_u64(&backend_return_gate.admission_seq, 4);
		break;
	case 14: cluster_enabled = false; break;
	case 15: cluster_shared_config = false; break;
	case 16: backend_return_changed_snapshot = registered(40); break;
	}
}
UT_TEST(backend_collection_discards_owner_and_gate_drift_at_return)
{
	ClusterServiceBackendCollection out;

	for (backend_return_fault = 0; backend_return_fault < 16; backend_return_fault++) {
		backend_return_begin(); backend_return_at = 9;
		snapshot_return_schedule = backend_return_mutate;
		UT_ASSERT_EQ(backend_return_read(&out), backend_return_fault == 0 || backend_return_fault == 10
			? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_STALE : backend_return_fault >= 11 && backend_return_fault <= 13
			? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING : CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED);
		UT_ASSERT(backend_return_barriers >= backend_return_at);
		backend_return_empty(&out);
	}
	backend_return_begin(); backend_return_fault = 16; backend_return_at = 2;
	snapshot_return_schedule = backend_return_mutate;
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	backend_return_empty(&out);
	UT_ASSERT_EQ(backend_return_changed_snapshot->regd_count, 1);
	UnregisterSnapshot(backend_return_changed_snapshot);
	backend_return_changed_snapshot = NULL;
}
UT_TEST(backend_collection_discards_unknown_or_contended_shared_samples)
{
	ClusterServiceBackendCollection out;

	for (unsigned fault = 0; fault < 3; fault++) {
		backend_return_begin();
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
		UT_ASSERT(out.snapshots.known && out.admission.known);
		if (fault == 0) SemanticActivationShmem = NULL;
		else pg_atomic_write_u64(&backend_return_gate.admission_seq, fault == 1 ? 3 : 0);
		UT_ASSERT_EQ(backend_return_read(&out), fault == 0 ? CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_REFUSED
			: CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
		backend_return_empty(&out);
	}
}
UT_TEST(backend_collection_never_retires_original_debt_or_grants_closure)
{
	ClusterServiceBackendCollection out;
	ClusterSemanticActivationShmem before;
	Snapshot held;

	backend_return_begin(); held = registered(80);
	pg_atomic_write_u32(&backend_return_gate.inflight[1][63], 5);
	before = backend_return_gate;
	for (unsigned i = 0; i < 4; i++) {
		UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
		UT_ASSERT(out.snapshots.known && out.admission.known);
		UT_ASSERT_EQ(out.snapshots.retained_read_scn, 80);
		UT_ASSERT_EQ(held->regd_count, 1); UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(memcmp(&before, &backend_return_gate, sizeof(before)), 0);
	}
	UnregisterSnapshot(held);
	UT_ASSERT_EQ(backend_return_read(&out), CLUSTER_SEMANTIC_ADMISSION_OBSERVATION_PENDING);
	UT_ASSERT_EQ(out.snapshots.references, 0); UT_ASSERT_EQ(out.admission.inflight[1][63], 5);
	UT_ASSERT_EQ(memcmp(&before, &backend_return_gate, sizeof(before)), 0);
	SemanticActivationShmem = NULL; cluster_shared_config = false;
}
