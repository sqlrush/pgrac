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

int
main(void)
{
	UT_PLAN(10);
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
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
