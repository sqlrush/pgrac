/*-------------------------------------------------------------------------
 * test_cluster_xid_retention_owner_capture.inc
 *     Retained owner capture through original xact and native CLOG.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_xid_retention_owner_capture.inc
 * NOTES
 *     Writer/admission, qualified directory, locks and WAL insertion are controlled.
 *-------------------------------------------------------------------------
 */
static unsigned retained_capture_plain_reads, retained_capture_drift;

static FullTransactionId
retained_capture_plain(void)
{
	retained_capture_plain_reads++;
	return xact_source_top();
}

static FullTransactionId
retained_capture_allocate(uint64 system_identifier, uint64 database_incarnation, uint16 origin)
{
	FullTransactionId top = xact_source_retained(system_identifier, database_incarnation, origin);

	if (retained_capture_drift == 1) caller_writer.epoch++;
	if (retained_capture_drift == 2) caller_boot++;
	return top;
}

static void
retained_capture_start(bool fail)
{
	retained_xact_start(UINT64_C(0x500008000), false, fail);
	caller_top_reader = retained_capture_plain;
	caller_retained_reader = retained_capture_allocate;
	retained_capture_plain_reads = retained_capture_drift = 0;
}

UT_TEST(retained_capture_keeps_original_writer_top_and_child_birth)
{
	ClusterRelmapOwnerSampleV2 sample;
	ClusterWalWriterToken original;
	TransactionStateData child = { 0 };
	XidNativeClogState state;

	retained_capture_start(false);
	original = caller_writer;
	UT_ASSERT(cluster_relmap_owner_capture_retained_v2(&sample));
	UT_ASSERT_EQ(memcmp(&original, &sample.writer, sizeof(original)), 0);
	UT_ASSERT_EQ(sample.owner.top_full_xid, UINT64_C(0x500008000));
	UT_ASSERT_EQ(caller_writer_begins, 1);
	UT_ASSERT_EQ(retained_capture_plain_reads, 0);
	test_xid_native_clog_state(&state);
	retained_clog_birth(&state, 1, UINT64_C(0x500008000));
	UT_ASSERT_EQ(caller_begin(&sample, true), CLUSTER_RELMAP_RETENTION_OK);
	child.state = TRANS_INPROGRESS; child.nestingLevel = 2;
	child.parent = &TopTransactionStateData;
	child.curTransactionOwner = (ResourceOwner)(uintptr_t)12;
	CurrentTransactionState = &child;
	UT_ASSERT_EQ(U64FromFullTransactionId(xact_source_current()), UINT64_C(0x500008001));
	UT_ASSERT_EQ(xact_parent[0], 32768);
	UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500008001));
	test_xid_native_clog_state(&state);
	UT_ASSERT_EQ(state.calls, 2);
	UT_ASSERT_EQ(full_clog_fixture.raw_calls, 0);
	UT_ASSERT_EQ(caller_owner_lock_errors | lock_errors, 0);
	retained_clog_stop();
}

UT_TEST(retained_capture_refuses_before_ordinary_or_native_assignment)
{
	for (unsigned which = 0; which < 3; which++) {
		ClusterRelmapOwnerSampleV2 output, zero = { 0 };
		VariableCacheData native;
		ClusterRelmapRetentionDirectory directory;

		retained_capture_start(false);
		native = variables;
		directory = relmap_runtime_shmem->directories[4];
		if (which == 0) caller_writer_ready = false;
		if (which == 1) caller_global_x = false;
		if (which == 2) held = true; /* Caller already owns XidGenLock. */
		memset(&output, 0x5a, sizeof(output));
		UT_ASSERT(!cluster_relmap_owner_capture_retained_v2(&output));
		held = false;
		UT_ASSERT_EQ(memcmp(&output, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(memcmp(&native, &variables, sizeof(native)), 0);
		UT_ASSERT_EQ(memcmp(&directory, &relmap_runtime_shmem->directories[4], sizeof(directory)), 0);
		UT_ASSERT_EQ(full_clog_fixture.count | native_publish_count | retained_capture_plain_reads, 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_capture_never_adopts_writer_after_assignment_drift)
{
	for (unsigned which = 1; which <= 2; which++) {
		ClusterRelmapOwnerSampleV2 output, zero = { 0 };
		ClusterRelmapRetentionPin pin;

		retained_capture_start(false);
		retained_clog_pin(UINT64_C(0x500007fff));
		pin = relmap_runtime_shmem->directories[4].pins[0];
		retained_capture_drift = which;
		memset(&output, 0x5a, sizeof(output));
		UT_ASSERT(!cluster_relmap_owner_capture_retained_v2(&output));
		UT_ASSERT_EQ(memcmp(&output, &zero, sizeof(zero)), 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(XactTopFullTransactionId), UINT64_C(0x500008000));
		UT_ASSERT_EQ(relmap_runtime_shmem->directories[4].last_issued, UINT64_C(0x500008000));
		UT_ASSERT_EQ(memcmp(&pin, relmap_runtime_shmem->directories[4].pins, sizeof(pin)), 0);
		UT_ASSERT_EQ(caller_writer_begins, 1);
		UT_ASSERT_EQ(full_clog_fixture.count, 1);
		UT_ASSERT_EQ(retained_capture_plain_reads, 0);
		retained_clog_stop();
	}
}

UT_TEST(retained_capture_error_keeps_output_pins_and_failed_transaction)
{
	for (unsigned which = 0; which < 4; which++) {
		static ClusterRelmapOwnerSampleV2 output;
		ClusterRelmapOwnerSampleV2 zero = { 0 };
		ClusterRelmapRetentionPin pin;
		XidNativeClogState before, after;

		retained_capture_start(which == 1);
		retained_clog_pin(UINT64_C(0x500007fff));
		pin = relmap_runtime_shmem->directories[4].pins[0];
		if (which == 0) relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which == 2) xact_predicate_error = true;
		if (which == 3) (void)xact_source_top(); /* Ordinary top must not be adopted. */
		for (unsigned attempt = 0; attempt < 2; attempt++) {
			volatile bool caught = false;
			unsigned expected = attempt == 0 && (which == 1 || which == 2)
				? ERRCODE_DISK_FULL : ERRCODE_CLUSTER_XID_AUTHORITY_UNAVAILABLE;

			memset(&output, 0x5a, sizeof(output));
			retention_error_expected = true;
			PG_TRY();
			{
				(void)cluster_relmap_owner_capture_retained_v2(&output);
			}
			PG_CATCH();
			{
				caught = true;
			}
			PG_END_TRY();
			retention_error_expected = false;
			UT_ASSERT(caught);
			UT_ASSERT_EQ(retention_error_code, expected);
			UT_ASSERT_EQ(memcmp(&output, &zero, sizeof(zero)), 0);
			UT_ASSERT(!retention_held);
			if (held) { HOLD_INTERRUPTS(); LWLockRelease(XidGenLock); }
			UT_ASSERT_EQ(InterruptHoldoffCount, 0);
			test_xid_native_clog_state(attempt == 0 ? &before : &after);
			xact_predicate_error = false;
			relmap_runtime_shmem->directories[4].state = CLUSTER_RELMAP_RETENTION_COMPLETE;
		}
		UT_ASSERT_EQ(before.calls, after.calls);
		UT_ASSERT_EQ(before.inserts, after.inserts);
		UT_ASSERT_EQ(memcmp(&pin, relmap_runtime_shmem->directories[4].pins, sizeof(pin)), 0);
		UT_ASSERT_EQ(retained_capture_plain_reads, 0);
		retained_clog_stop();
	}
}
