/*-------------------------------------------------------------------------
 * test_cluster_ko_relation_scan.c -- object drain observes every fork.
 * Reuses the real PCM registry and owner fixture without rerunning the
 * complete PCM suite. Storage receipts retain their explicit fixture boundary.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
int pcm_relation_fixture_main(void);
#define main pcm_relation_fixture_main
#include "test_cluster_pcm_lock.c"
#undef main

UT_TEST(object_scan_checks_all_forks_and_original_refs)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		BufferTag tag = make_tag(6901), other = make_tag(6902);
		RelFileLocator locator = BufTagGetRelFileLocator(&tag);
		PcmEntryRef ref, unrelated;
		PcmEntryAcquireResult acquired;
		PcmEntryTransportRef transport;
		uint32 cursor = 0;
		pi_relation_scan_setup();
		tag.forkNum = fork;
		other.relNumber++;
		UT_ASSERT(pcm_entry_ref_acquire(&other, true, &unrelated, &acquired));
		UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
		UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 4, &cursor),
					 CLUSTER_PCM_PI_RELATION_PENDING);
		UT_ASSERT(pcm_entry_transport_ref_begin(&ref, &transport));
		pcm_entry_ref_release(&ref);
		cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 4, &cursor),
					 CLUSTER_PCM_PI_RELATION_PENDING);
		pcm_entry_transport_ref_end(&transport);
		cursor = 0;
		UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 4, &cursor),
					 CLUSTER_PCM_PI_RELATION_EMPTY);
		pcm_entry_ref_release(&unrelated);
		cluster_shared_config = false;
	}
}
UT_TEST(object_scan_never_retires_any_fork_pi)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		for (int local = 0; local < 2; local++) {
			BufferTag tag = make_tag(6903);
			PcmEntryRef ref;
			PcmEntryAcquireResult acquired;
			struct StopPcmEntryLayout *entry, before;
			uint32 cursor = 0;
			pi_relation_scan_setup();
			tag.forkNum = fork;
			UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
			pcm_entry_ref_release(&ref);
			entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
			UT_ASSERT_NOT_NULL(entry);
			if (local)
				entry->local_pi_first.source_flags = 1;
			else
				pg_atomic_write_u32(&entry->pi_holders_bitmap, 2);
			before = *entry;
			UT_ASSERT_EQ(
				cluster_pcm_lock_ko_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
				CLUSTER_PCM_PI_RELATION_PENDING);
			UT_ASSERT_EQ(memcmp(&before, entry, sizeof(before)), 0);
			UT_ASSERT_EQ(fake_lwlock_depth, 0);
			cluster_shared_config = false;
		}
	}
}
UT_TEST(object_scan_cannot_call_settled_current_empty)
{
	BufferTag tag = make_tag(6904);
	RelFileLocator locator = BufTagGetRelFileLocator(&tag);
	uint32 cursor = 0;
	struct StopPcmEntryLayout *entry, before;

	setup_pi_write_master_with_holders(tag, 0);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
	UT_ASSERT_NOT_NULL(entry);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->master_state), PCM_STATE_X);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 0);
	before = *entry;
	/* The original structural observer deliberately admits stable residency. */
	UT_ASSERT_EQ(cluster_pcm_lock_pi_relation_scan_v2(locator, 17, 4, &cursor),
				 CLUSTER_PCM_PI_RELATION_EMPTY);
	cursor = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 4, &cursor),
				 CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(memcmp(&before, entry, sizeof(before)), 0);
	cluster_shared_config = false;
}
UT_TEST(object_scan_preserves_budget_and_gate_rejection)
{
	BufferTag tag = make_tag(6905);
	RelFileLocator locator = BufTagGetRelFileLocator(&tag);
	uint32 cursor = 0;
	pi_relation_scan_setup();
	UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 18, 1, &cursor),
				 CLUSTER_PCM_PI_RELATION_INVALID);
	UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 129, &cursor),
				 CLUSTER_PCM_PI_RELATION_INVALID);
	UT_ASSERT_EQ(cursor, 0);
	for (uint32 n = 1; n <= 4; n++) {
		UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 1, &cursor),
					 n == 4 ? CLUSTER_PCM_PI_RELATION_EMPTY : CLUSTER_PCM_PI_RELATION_MORE);
		UT_ASSERT_EQ(cursor, n);
	}
	UT_ASSERT_EQ(cluster_pcm_lock_ko_relation_scan_v3(locator, 17, 1, &cursor),
				 CLUSTER_PCM_PI_RELATION_INVALID);
	cluster_shared_config = false;
}
UT_TEST(object_scan_rejects_unknown_fork_on_either_side)
{
	ForkNumber invalid[] = { InvalidForkNumber, MAX_FORKNUM + 1 };
	for (unsigned n = 0; n < lengthof(invalid); n++) {
		BufferTag tag = make_tag(6906);
		PcmEntryRef ref;
		PcmEntryAcquireResult acquired;
		uint32 cursor = 0;
		pi_relation_scan_setup();
		tag.forkNum = invalid[n];
		UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
		pcm_entry_ref_release(&ref);
		UT_ASSERT_EQ(
			cluster_pcm_lock_ko_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
			CLUSTER_PCM_PI_RELATION_INVALID);
		UT_ASSERT_EQ(cursor, 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
		cluster_shared_config = false;
	}
}

typedef ClusterPcmPiRelationScanV2 (*ObjectScan)(RelFileLocator, uint64, uint32, uint32 *);

static void
expect_roles(BufferTag tag, ClusterPcmPiRelationScanV2 local, ClusterPcmPiRelationScanV2 directory)
{
	ObjectScan scans[] = { cluster_pcm_lock_ko_local_relation_scan_v3,
						   cluster_pcm_lock_ko_directory_relation_scan_v3 };
	ClusterPcmPiRelationScanV2 expected[] = { local, directory };
	struct StopPcmEntryLayout *entry, before;
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
	UT_ASSERT_NOT_NULL(entry);
	before = *entry;
	for (unsigned i = 0; i < lengthof(scans); i++) {
		uint32 cursor = 0;
		UT_ASSERT_EQ(scans[i](BufTagGetRelFileLocator(&tag), 17, 4, &cursor), expected[i]);
		UT_ASSERT_EQ(memcmp(&before, entry, sizeof(before)), 0);
		UT_ASSERT_EQ(fake_lwlock_depth, 0);
	}
}

UT_TEST(object_roles_remote_head_does_not_reopen_local_physical_work)
{
	BufferTag tag = make_tag(6910);
	ResourceXDecodedFrame request, ack;
	uint32 cursor = 0;
	pi_relation_scan_setup();
	cluster_node_id = 0;
	UT_ASSERT_EQ(
		cluster_pcm_lock_ko_local_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
		CLUSTER_PCM_PI_RELATION_EMPTY);
	/* A local fence must not disable the original remote master service. */
	object_admission_allowed = false;
	object_admission_revision++;
	request = make_resource_x_bootstrap_request(tag, 1);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_request_exact(&request, 1, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	/* The bootstrap receipt is a directory head, not a local physical task. */
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	cursor = 0;
	UT_ASSERT_EQ(
		cluster_pcm_lock_ko_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
		CLUSTER_PCM_PI_RELATION_PENDING);
	cluster_shared_config = false;
}

UT_TEST(object_roles_settled_remote_holder_and_pi_remain_directory_debt)
{
	BufferTag tag = make_tag(6911);
	setup_pi_write_master_with_holders(tag, 4);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	release_pi_write_master(tag, false);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	cluster_shared_config = false;
}

UT_TEST(object_roles_local_settled_terminal_is_directory_only)
{
	BufferTag tag = make_tag(6917);

	/* A valid local SETTLED record with physical N state is directory history;
	 * it must not reopen local work after its local owner and PI are gone. */
	setup_local_settled_terminal_without_physical(tag);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	cluster_shared_config = false;
}

UT_TEST(object_roles_local_master_request_is_not_remote_directory_only)
{
	BufferTag tag = make_tag(6912);
	ResourceXDecodedFrame request, ack;
	pi_relation_scan_setup();
	cluster_node_id = 0;
	request = make_resource_x_bootstrap_request(tag, 0);
	UT_ASSERT_EQ(
		cluster_pcm_lock_resource_x_bootstrap_request_exact(&request, 0, 61, 77, 31, 71, &ack),
		RESOURCE_X_APPLY_APPLIED);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	cluster_shared_config = false;
}

UT_TEST(object_roles_local_pi_and_unknown_references_never_filter_out)
{
	for (int kind = 0; kind < 5; kind++) {
		BufferTag tag = make_tag(6913);
		struct StopPcmEntryLayout *entry;
		setup_pi_write_master_with_holders(tag, 4);
		entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
		switch (kind) {
		case 0:
			entry->local_pi_first.source_flags = 1;
			break;
		case 1:
			entry->local_pi_last.source_flags = 1;
			break;
		case 2:
			pg_atomic_write_u32(&entry->pin_count, 1);
			break;
		case 3:
			pg_atomic_write_u32(&entry->wait_refcount, 1);
			break;
		case 4:
			pg_atomic_write_u32(&entry->transport_refcount, 1);
			break;
		}
		expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_PENDING);
		cluster_shared_config = false;
	}
}

UT_TEST(object_roles_master_pi_accounting_does_not_reopen_local_work)
{
	for (int fork = MAIN_FORKNUM; fork <= MAX_FORKNUM; fork++) {
		for (unsigned kind = 1; kind < 16; kind++) {
			BufferTag tag = make_tag(6918);
			PcmEntryRef ref;
			PcmEntryAcquireResult acquired;
			uint32 cursor = 0;
			pi_relation_scan_setup();
			cluster_node_id = 0;
			tag.forkNum = fork;
			UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
			pcm_entry_ref_release(&ref);
			/* These are master accounting producers, not local PI storage. */
			if (kind & 1)
				cluster_pcm_lock_pi_holder_note(tag, 0);
			if (kind & 2)
				cluster_pcm_lock_pi_holder_note(tag, 2);
			if (kind & 4)
				cluster_pcm_lock_pi_watermark_lsn_advance(tag, 200);
			if (kind & 8)
				cluster_pcm_lock_pi_watermark_scn_advance(tag, 80, CLUSTER_PCM_WM_SRC_REDECLARE, 2,
														  31, 17);
			expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
			UT_ASSERT_EQ(
				cluster_pcm_lock_ko_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
				CLUSTER_PCM_PI_RELATION_PENDING);
			cluster_shared_config = false;
		}
	}
}

UT_TEST(object_roles_local_retirement_preserves_unfinished_master_cut)
{
	BufferTag tag = make_tag(6919);
	ClusterPageWalBindingV1 binding = local_pi_setup(tag);
	ClusterPcmLocalPiSnapshotV1 local, empty;
	const ClusterPageDataReceiptV1 *receipt = (const void *)&pi_receipt_fixture;
	struct StopPcmEntryLayout *entry;
	uint32 cursor = 0;

	cluster_node_id = 0;
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_gate_bind_formation_exact(17),
				 RESOURCE_X_APPLY_APPLIED);
	UT_ASSERT(cluster_pcm_local_pi_record_v1(tag, &binding));
	cluster_pcm_lock_pi_holder_note(tag, 0);
	cluster_pcm_lock_pi_holder_note(tag, 2);
	cluster_pcm_lock_pi_watermark_lsn_advance(tag, 200);
	cluster_pcm_lock_pi_watermark_scn_advance(tag, 80, CLUSTER_PCM_WM_SRC_REDECLARE, 2, 31, 17);
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(tag, &local));
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_PENDING);

	/* The real PCM retire function still requires its original DATA/ACK
	 * boundary. This fixture supplies that proof; it does not run IO or
	 * transport and does not publish READY. No full master ACK is supplied. */
	pi_receipt_valid = true;
	pi_storage_receipt_valid = false;
	memset(&pi_receipt_cut, 0, sizeof(pi_receipt_cut));
	pi_ack_cut = pi_receipt_cut;
	pi_ack_available = 1;
	pi_ack_imported = 0;
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&local, receipt, NULL, NULL, 0, pi_acks[0]));
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_PENDING);
	local_pi_covered = true;
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&local, receipt, NULL, NULL, 0, NULL));
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT(cluster_pcm_local_pi_retire_v1(&local, receipt, NULL, NULL, 0, pi_acks[0]));
	UT_ASSERT(cluster_pcm_local_pi_snapshot_v1(tag, &empty));
	UT_ASSERT_EQ(empty.first.record_start, 0);
	UT_ASSERT_EQ(empty.last.record_start, 0);
	UT_ASSERT(empty.revision > local.revision);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
	UT_ASSERT_NOT_NULL(entry);
	UT_ASSERT_EQ(pg_atomic_read_u32(&entry->pi_holders_bitmap), 5);
	UT_ASSERT_EQ(entry->pi_watermark_lsn, 200);
	UT_ASSERT_EQ(entry->pi_watermark_scn, 80);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_EMPTY, CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT_EQ(
		cluster_pcm_lock_ko_relation_scan_v3(BufTagGetRelFileLocator(&tag), 17, 4, &cursor),
		CLUSTER_PCM_PI_RELATION_PENDING);
	UT_ASSERT(!cluster_pcm_lock_pi_obligations_absent_v1(tag));

	/* A new local reference cannot borrow the earlier local retirement. */
	binding.version.mutation_token++;
	binding.record_start = 300;
	binding.record_end = 400;
	UT_ASSERT(cluster_pcm_local_pi_record_v1(tag, &binding));
	UT_ASSERT(!cluster_pcm_local_pi_retire_v1(&local, receipt, NULL, NULL, 0, pi_acks[0]));
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_PENDING);
	local_pi_writer_ready = false;
	cluster_shared_config = false;
}

UT_TEST(object_roles_local_round_is_physical_even_on_nonmaster)
{
	BufferTag tag = make_tag(6914);
	ResourceXAssertion assertion;
	ResourceXCallerWitness caller = { 0 };
	ResourceXDecodedFrame dispatch;
	ResourceXAcquisitionRef terminal;
	pi_relation_scan_setup();
	cluster_node_id = 1;
	fake_gcs_master_node = 0;
	UT_ASSERT(resource_x_assertion_init(&tag, 1, &assertion));
	UT_ASSERT_EQ(cluster_pcm_lock_resource_x_bootstrap_round_step_caller_exact(
					 &assertion, 0, 17, 31, 77, 51, 61, 1000, 900, 100, 50, 0, 0, true, true, false,
					 0, &caller, &dispatch, &terminal),
				 RESOURCE_X_BOOTSTRAP_ROUND_DISPATCH_REQUEST);
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_PENDING, CLUSTER_PCM_PI_RELATION_EMPTY);
	cluster_shared_config = false;
}

UT_TEST(object_roles_remote_debt_cannot_hide_bad_local_terminal_history)
{
	BufferTag tag = make_tag(6915);
	PcmEntryRef ref;
	PcmEntryAcquireResult acquired;
	struct StopPcmEntryLayout *entry;
	pi_relation_scan_setup();
	cluster_node_id = 0;
	UT_ASSERT(pcm_entry_ref_acquire(&tag, true, &ref, &acquired));
	pcm_entry_ref_release(&ref);
	entry = hash_search((HTAB *)&fake_pcm_htab_token, &tag, HASH_FIND, NULL);
	entry->resource_x_bootstrap_round.highest_attempt_floor = UINT64_MAX;
	expect_roles(tag, CLUSTER_PCM_PI_RELATION_INVALID, CLUSTER_PCM_PI_RELATION_INVALID);
	cluster_shared_config = false;
}

UT_TEST(object_roles_preserve_bounded_gate_and_reference_contract)
{
	ObjectScan scans[] = { cluster_pcm_lock_ko_local_relation_scan_v3,
						   cluster_pcm_lock_ko_directory_relation_scan_v3 };
	for (unsigned i = 0; i < lengthof(scans); i++) {
		BufferTag tag = make_tag(6916);
		RelFileLocator locator = BufTagGetRelFileLocator(&tag);
		uint32 cursor = 0;
		pi_relation_scan_setup();
		cluster_node_id = 0;
		UT_ASSERT_EQ(scans[i](locator, 18, 1, &cursor), CLUSTER_PCM_PI_RELATION_INVALID);
		UT_ASSERT_EQ(scans[i](locator, 17, 129, &cursor), CLUSTER_PCM_PI_RELATION_INVALID);
		UT_ASSERT_EQ(cursor, 0);
		for (uint32 n = 1; n <= 4; n++) {
			UT_ASSERT_EQ(scans[i](locator, 17, 1, &cursor),
						 n == 4 ? CLUSTER_PCM_PI_RELATION_EMPTY : CLUSTER_PCM_PI_RELATION_MORE);
			UT_ASSERT_EQ(cursor, n);
		}
		cluster_shared_config = false;
	}
}

int
main(void)
{
	UT_PLAN(20);
	UT_RUN(test_pi_relation_empty_requires_complete_bounded_scan);
	UT_RUN(test_pi_relation_scan_checks_main_vm_and_original_references);
	UT_RUN(test_pi_relation_scan_never_retires_master_or_local_pi);
	UT_RUN(test_pi_relation_scan_refuses_orphan_and_preheld_lock);
	UT_RUN(test_pi_relation_scan_rejects_wrong_gate_and_pending_transfer);
	UT_RUN(object_scan_checks_all_forks_and_original_refs);
	UT_RUN(object_scan_never_retires_any_fork_pi);
	UT_RUN(object_scan_cannot_call_settled_current_empty);
	UT_RUN(object_scan_preserves_budget_and_gate_rejection);
	UT_RUN(object_scan_rejects_unknown_fork_on_either_side);
	UT_RUN(object_roles_remote_head_does_not_reopen_local_physical_work);
	UT_RUN(object_roles_settled_remote_holder_and_pi_remain_directory_debt);
	UT_RUN(object_roles_local_settled_terminal_is_directory_only);
	UT_RUN(object_roles_local_master_request_is_not_remote_directory_only);
	UT_RUN(object_roles_local_pi_and_unknown_references_never_filter_out);
	UT_RUN(object_roles_master_pi_accounting_does_not_reopen_local_work);
	UT_RUN(object_roles_local_retirement_preserves_unfinished_master_cut);
	UT_RUN(object_roles_local_round_is_physical_even_on_nonmaster);
	UT_RUN(object_roles_remote_debt_cannot_hide_bad_local_terminal_history);
	UT_RUN(object_roles_preserve_bounded_gate_and_reference_contract);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
