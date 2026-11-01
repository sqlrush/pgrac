/* Actual original LmonMain return point, explicit publication scheduling.
 * Rows/canaries remain untrusted syntax, never producer receipts or jobs.
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned invalidation_case;
static bool invalidation_hook_seen;
static void
invalidation_during_write(void)
{
	if (!(candidate.header.seq & 1)) return;
	invalidation_hook_seen = true; test_native_epoch_read_hook = NULL;
	if (invalidation_case == 10 || invalidation_case == 16) test_native_epoch = 4;
	if (invalidation_case == 11 || invalidation_case == 13)
		pg_atomic_write_u64(&test_collection_gate.record_generation, 12);
	if (invalidation_case == 12) MyProc->cluster_grd_generation++;
	if (invalidation_case == 14)
		pg_atomic_write_u64(&test_collection_gate.admission_seq, 3);
}
static void
invalidation_preserves_rows(void)
{
	UT_ASSERT(candidate.header.flags != 0);
	UT_ASSERT_EQ(candidate.header.staged_chunk[0], 0);
	UT_ASSERT_EQ(candidate.header.staged_chunk[1], 0);
	UT_ASSERT_EQ(memcmp(candidate.tables[0].observed, before.tables[0].observed,
		sizeof(candidate.tables[0].observed)), 0);
	UT_ASSERT_EQ(memcmp(candidate.ingress, before.ingress, sizeof(candidate.ingress)), 0);
	UT_ASSERT_EQ(candidate.header.callback_debt, before.header.callback_debt);
	UT_ASSERT_EQ(candidate.header.job_reference, before.header.job_reference);
	UT_ASSERT_EQ(memcmp(candidate.executors, before.executors, sizeof(candidate.executors)), 0);
}
static int
invalidation_wait(WaitEvent *events pg_attribute_unused())
{
	ClusterServiceProducerCollection out;
	ClusterSurvivorMessageV4 m;
	uint64 generation = MyProc->cluster_grd_generation;
	PGPROC *original_proc = MyProc;

	++test_collection_samples;
	begin_candidate(); UT_ASSERT_EQ(probe(&out), PENDING); /* Original module fanout. */
	make_ack(127); UT_ASSERT_EQ(stage(127), PENDING);
	/* Retention canaries only; not fabricated native debt or receipt proof. */
	candidate.header.callback_debt = 3; candidate.header.job_reference = 123;
	candidate.executors[127].job_generation = 77;
	before = candidate; make_ack(1); invalidation_hook_seen = false;
	switch (invalidation_case) {
	case 0: test_native_epoch = 4; break;
	case 1: pg_atomic_write_u64(&test_collection_gate.record_generation, 12); break;
	case 2: pg_atomic_write_u64(&test_collection_gate.formation_epoch, 2); break;
	case 3: SemanticActivationShmem = NULL; break;
	case 4: pg_atomic_write_u64(&test_collection_gate.admission_seq, 3); break;
	case 5: test_native_epoch = 4; make_ack(127); break;
	case 6:
		test_native_epoch = 4;
		UT_ASSERT(cluster_semantic_survivor_v4_decode(ack, sizeof(ack), format, &m));
		m.nonce++; refresh_ack(&m); UT_ASSERT_EQ(stage(1), STALE);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		make_ack(1); break;
	}
	if (invalidation_case <= 6) {
		UT_ASSERT_EQ(stage(invalidation_case == 5 ? 127 : 1),
			invalidation_case == 3 || invalidation_case == 4 ? PENDING : STALE);
		if (invalidation_case == 3 || invalidation_case == 4)
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		else invalidation_preserves_rows();
	} else if (invalidation_case <= 9 || invalidation_case == 13) {
		make_manifest(); before = candidate;
		if (invalidation_case == 7) test_native_epoch = 4;
		if (invalidation_case == 8) pg_atomic_write_u64(&test_collection_gate.record_generation, 12);
		if (invalidation_case == 9) SemanticActivationShmem = NULL;
		if (invalidation_case == 13) test_native_epoch_read_hook = invalidation_during_write;
		UT_ASSERT_EQ(cluster_semantic_survivor_candidate_begin(&candidate, manifest,
			manifest_bytes, format, 0, &candidate_reason), invalidation_case == 9 ? PENDING : STALE);
		if (invalidation_case != 13)
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
		else {
			UT_ASSERT(invalidation_hook_seen); UT_ASSERT(candidate.header.flags != 0);
			UT_ASSERT_EQ(candidate.header.seq, 2);
			UT_ASSERT_EQ(candidate.basis.open_generation, 11);
		}
	} else if (invalidation_case == 15) {
		for (unsigned fault = 0; fault < 3; ++fault) {
			memset(&out, 0xa5, sizeof(out));
			if (fault == 0) MyProc = NULL;
			if (fault == 1) candidate.header.flags = 1;
			UT_ASSERT_EQ(cluster_semantic_survivor_candidate_probe(fault == 2 ? NULL : &candidate,
				&out, NULL), fault == 1 ? STALE : REFUSED);
			for (unsigned i = 0; i < sizeof(out); ++i) UT_ASSERT_EQ(((uint8 *)&out)[i], 0);
			MyProc = original_proc; candidate.header.flags = 0;
		}
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
	} else {
		if (invalidation_case == 16) {
			candidate.header.seq = candidate.tables[0].header.seq = UINT64_MAX - 3;
			before = candidate;
		}
		test_native_epoch_read_hook = invalidation_during_write;
		UT_ASSERT_EQ(stage(1), invalidation_case == 12 ? REFUSED : invalidation_case == 14 ? PENDING : STALE);
		UT_ASSERT(invalidation_hook_seen);
		UT_ASSERT_EQ(candidate.header.seq, before.header.seq + 2);
		UT_ASSERT_EQ(candidate.header.latch_generation, before.header.latch_generation + 1);
		UT_ASSERT_EQ(memcmp(&candidate.tables[0].observed[127], &before.tables[0].observed[127],
			sizeof(candidate.tables[0].observed[127])), 0);
		UT_ASSERT_EQ(candidate.tables[0].observed[1].identity.node, 1);
		UT_ASSERT_EQ(candidate.header.callback_debt, 3);
		UT_ASSERT_EQ(candidate.header.job_reference, 123);
		UT_ASSERT_EQ(candidate.executors[127].job_generation, 77);
		if (invalidation_case != 14) {
			UT_ASSERT(candidate.header.flags != 0);
			UT_ASSERT_EQ(candidate.header.staged_chunk[0], 0);
			UT_ASSERT_EQ(candidate.header.staged_chunk[1], 0);
		} else {
			/* A raw row is retained after an unknown cut, still never confirmed. */
			UT_ASSERT_EQ(candidate.header.flags, 0);
			UT_ASSERT_EQ(candidate.header.staged_chunk[0], 2);
		}
	}
	test_native_epoch_read_hook = NULL; test_native_epoch = 3;
	MyProc = original_proc; MyProc->cluster_grd_generation = generation;
	SemanticActivationShmem = &test_collection_gate;
	pg_atomic_write_u64(&test_collection_gate.admission_seq, 2);
	pg_atomic_write_u64(&test_collection_gate.formation_epoch, 3);
	pg_atomic_write_u64(&test_collection_gate.record_generation, 11);
	unconfirmed(); ShutdownRequestPending = true; return 0;
}
static void
run_invalidation(unsigned which)
{
	candidate_case = 0; invalidation_case = which; test_collection_kind = 4;
	test_collection_extra_wait = invalidation_wait;
	for (unsigned mode = 0; mode < 2; ++mode) test_collection_run(mode != 0);
	test_collection_extra_wait = NULL;
}
UT_TEST(candidate_late_row_rechecks_actual_native_epoch_at_original_return) { run_invalidation(0); }
UT_TEST(candidate_late_row_rechecks_original_gate_generation) { run_invalidation(1); }
UT_TEST(candidate_late_row_rechecks_original_gate_epoch) { run_invalidation(2); }
UT_TEST(candidate_missing_gate_defers_without_mutation) { run_invalidation(3); }
UT_TEST(candidate_odd_gate_defers_without_mutation) { run_invalidation(4); }
UT_TEST(candidate_duplicate_rechecks_local_epoch) { run_invalidation(5); }
UT_TEST(candidate_foreign_scope_cannot_invalidate_local_candidate) { run_invalidation(6); }
UT_TEST(candidate_stale_native_request_cannot_begin) { run_invalidation(7); }
UT_TEST(candidate_stale_gate_request_cannot_begin) { run_invalidation(8); }
UT_TEST(candidate_unknown_gate_request_cannot_begin) { run_invalidation(9); }
UT_TEST(candidate_epoch_drift_during_write_retains_unconfirmed_rows) { run_invalidation(10); }
UT_TEST(candidate_gate_drift_during_write_retains_unconfirmed_rows) { run_invalidation(11); }
UT_TEST(candidate_owner_drift_during_write_revokes_candidate) { run_invalidation(12); }
UT_TEST(candidate_gate_drift_during_begin_retains_original_basis) { run_invalidation(13); }
UT_TEST(candidate_unknown_cut_during_write_never_confirms) { run_invalidation(14); }
UT_TEST(candidate_probe_clears_all_early_rejections) { run_invalidation(15); }
UT_TEST(candidate_invalidation_uses_one_bounded_generation_change) { run_invalidation(16); }

/* Real LmonMain return boundary and original native observation branch.
 * Exit flags and role selection are explicit host scheduling inputs; no
 * completion receipt, producer closure or permission is supplied here.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
static unsigned aux_exit_case;
static bool aux_exit_seen;

static void
aux_exit_now(void)
{
	aux_exit_seen = true;
	proc_exit_inprogress = true;
	survivor_poll_barrier_hook = NULL;
}

static void
aux_exit_during_write(void)
{
	if (!(candidate.header.seq & 1)) return;
	test_native_epoch_read_hook = NULL;
	aux_exit_now();
}

static int
aux_exit_wait(WaitEvent *events pg_attribute_unused())
{
	ClusterSurvivorProducerObservation observed;
	ClusterSurvivorProducerScope scope = {3, 3, 11};
	ClusterServiceProducerCollection native;
	ClusterSemanticAdmissionObservationResult result;
	BackendType backend = MyBackendType;
	AuxProcType aux = MyAuxProcType;
	const BackendType backends[] = {B_LMON, B_LMS, B_LMD, B_SINVAL_BCAST, B_LMS_WORKER};
	const AuxProcType roles[] = {LmonProcess, LmsProcess, LmdProcess,
		SinvalBcastProcess, LmsWorker1Process};
	bool lmd = cluster_lmd_enabled;
	unsigned polls;

	++test_collection_samples;
	/* Exercise the healthy original module fanout before exit scheduling.
	 * The existing transport-exit fixture requires that real observation. */
	begin_candidate(); UT_ASSERT_EQ(probe(&native), PENDING);
	if (aux_exit_case < 2) {
		cluster_lmd_enabled = true;
		for (unsigned i = 0; i < lengthof(backends); ++i) {
			MyBackendType = backends[i]; MyAuxProcType = roles[i];
			if (i != 0) UT_ASSERT(cluster_normal_stop_service_enter());
			polls = test_stop_polls; test_producer_seen = 0; aux_exit_seen = false;
			if (aux_exit_case == 0) proc_exit_inprogress = true;
			else survivor_poll_barrier_hook = aux_exit_now;
			result = cluster_semantic_survivor_producer_poll(
				CLUSTER_SURVIVOR_PRODUCER_NATIVE, scope, &observed);
			proc_exit_inprogress = false; survivor_poll_barrier_hook = NULL;
			if (i != 0) UT_ASSERT(cluster_normal_stop_service_leave(true));
			UT_ASSERT_EQ(result, REFUSED);
			survivor_poll_unknown(&observed);
			if (aux_exit_case == 0) UT_ASSERT_EQ(test_stop_polls, polls);
			else UT_ASSERT(aux_exit_seen);
		}
		MyBackendType = backend; MyAuxProcType = aux; cluster_lmd_enabled = lmd;
	} else if (aux_exit_case == 2) {
		begin_candidate(); make_ack(127); UT_ASSERT_EQ(stage(127), PENDING);
		candidate.header.callback_debt = 3; candidate.header.job_reference = 123;
		before = candidate; make_ack(1);
		for (unsigned op = 0; op < 4; ++op) {
			polls = test_stop_polls; memset(&native, 0xa5, sizeof(native));
			proc_exit_inprogress = true;
			if (op == 0) result = cluster_semantic_survivor_candidate_begin(&candidate,
				manifest, manifest_bytes, format, 0, NULL);
			else if (op == 1) result = stage(1);
			else if (op == 2) result = cluster_semantic_survivor_candidate_invalidate(&candidate, NULL);
			else result = probe(&native);
			proc_exit_inprogress = false;
			UT_ASSERT_EQ(result, REFUSED);
			UT_ASSERT_EQ(test_stop_polls, polls);
			UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
			if (op == 3)
				for (unsigned j = 0; j < sizeof(native); ++j) UT_ASSERT_EQ(((uint8 *)&native)[j], 0);
		}
	} else if (aux_exit_case == 3 || aux_exit_case == 4) {
		if (aux_exit_case == 3) make_manifest();
		else {
			begin_candidate(); make_ack(127); UT_ASSERT_EQ(stage(127), PENDING);
			candidate.header.callback_debt = 3; candidate.header.job_reference = 123;
			candidate.executors[127].job_generation = 77;
			before = candidate; make_ack(1);
		}
		aux_exit_seen = false; test_native_epoch_read_hook = aux_exit_during_write;
		result = aux_exit_case == 3
			? cluster_semantic_survivor_candidate_begin(&candidate, manifest, manifest_bytes,
				format, 0, NULL) : stage(1);
		proc_exit_inprogress = false; test_native_epoch_read_hook = NULL;
		UT_ASSERT(aux_exit_seen); UT_ASSERT_EQ(result, REFUSED);
		UT_ASSERT(candidate.header.flags != 0);
		UT_ASSERT_EQ(candidate.header.seq & 1, 0);
		UT_ASSERT_EQ(candidate.header.staged_chunk[0], 0);
		UT_ASSERT_EQ(candidate.header.staged_chunk[1], 0);
		UT_ASSERT_EQ(candidate.basis.open_generation, 11);
		if (aux_exit_case == 4) {
			UT_ASSERT_EQ(memcmp(&candidate.tables[0].observed[127], &before.tables[0].observed[127],
				sizeof(candidate.tables[0].observed[127])), 0);
			UT_ASSERT_EQ(candidate.header.callback_debt, 3);
			UT_ASSERT_EQ(candidate.header.job_reference, 123);
			UT_ASSERT_EQ(candidate.executors[127].job_generation, 77);
			UT_ASSERT_EQ(candidate.header.seq, before.header.seq + 2);
		}
	} else {
		begin_candidate(); before = candidate;
		aux_exit_seen = false; survivor_poll_barrier_hook = aux_exit_now;
		memset(&native, 0xa5, sizeof(native)); result = probe(&native);
		proc_exit_inprogress = false; survivor_poll_barrier_hook = NULL;
		UT_ASSERT(aux_exit_seen); UT_ASSERT_EQ(result, REFUSED);
		for (unsigned j = 0; j < sizeof(native); ++j) UT_ASSERT_EQ(((uint8 *)&native)[j], 0);
		UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
	}
	unconfirmed(); ShutdownRequestPending = true; return 0;
}

static void
run_aux_exit(unsigned which)
{
	candidate_case = 0; aux_exit_case = which; test_collection_kind = 4;
	test_collection_extra_wait = aux_exit_wait;
	for (unsigned mode = 0; mode < 2; ++mode) test_collection_run(mode != 0);
	test_collection_extra_wait = NULL;
}
UT_TEST(native_survivor_entry_refuses_all_exiting_auxiliary_roles) { run_aux_exit(0); }
UT_TEST(native_survivor_return_clears_all_exiting_auxiliary_roles) { run_aux_exit(1); }
UT_TEST(candidate_exiting_entry_preserves_all_original_obligations) { run_aux_exit(2); }
UT_TEST(candidate_exit_during_begin_retains_invalid_original_basis) { run_aux_exit(3); }
UT_TEST(candidate_exit_during_stage_retains_rows_jobs_and_callbacks) { run_aux_exit(4); }
UT_TEST(candidate_probe_exit_clears_native_sample_without_retiring_debt) { run_aux_exit(5); }
