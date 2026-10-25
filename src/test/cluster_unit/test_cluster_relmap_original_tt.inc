/* Original TT allocator/local ACTIVE publisher/durable producer beside
 * original relmap producer/runtime/retention and physical dual-copy files.
 * Native TT I/O/WAL, admission/current authority and outcome remain controlled.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "test_cluster_relmap_native_tt.h"

static void
original_tt_done(void)
{
	v2_published_binding_observer = NULL;
	test_relmap_native_tt_reset();
	physical_cleanup();
	cluster_node_id = 3;
}

static bool
original_tt_publish(void)
{
	TestRelmapNativeTTStats stats;
	bool published = test_relmap_native_tt_publish(XidFromFullTransactionId(assigned_xid));

	UT_ASSERT(published);
	stats = test_relmap_native_tt_stats();
	UT_ASSERT_EQ(stats.bindings, 1);
	UT_ASSERT_EQ(stats.bind_records, 1);
	UT_ASSERT_EQ(stats.ctrc_opens, 1);
	UT_ASSERT(stats.balanced && !stats.assertion_failed);
	return published;
}

static void
original_tt_pins(const ClusterRelmapRetentionDirectory *directory,
	const ClusterRelmapRetentionPin *unrelated, unsigned mask, bool bound)
{
	ClusterCanonicalTxnBinding receipt;
	unsigned matched = 0;

	UT_ASSERT(test_relmap_native_tt_lookup(XidFromFullTransactionId(assigned_xid), &receipt));
	UT_ASSERT_EQ(receipt.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
	UT_ASSERT_EQ(receipt.origin_instance, 1);
	UT_ASSERT(receipt.active_lsn != InvalidXLogRecPtr);
	UT_ASSERT(memcmp(unrelated, &directory->pins[0], sizeof(*unrelated)) == 0);
	for (unsigned i = 1; i < lengthof(directory->pins); i++)
	{
		const ClusterRelmapRetentionPin *pin = &directory->pins[i];

		if (!pin->occupied)
			continue;
		UT_ASSERT_EQ(pin->key.origin, cluster_node_id);
		UT_ASSERT_EQ(pin->key.top_full_xid, U64FromFullTransactionId(assigned_xid));
		UT_ASSERT_EQ(pin->has_tt_binding, bound);
		UT_ASSERT(pin->may_be_staged);
		UT_ASSERT(pin->wal_end != InvalidXLogRecPtr && pin->wal_end != receipt.active_lsn);
		if (bound)
			UT_ASSERT(memcmp(&pin->binding, &receipt, sizeof(receipt)) == 0);
		matched |= pin->key.identity.shared ? 1 : 2;
	}
	UT_ASSERT_EQ(matched, mask);
}

UT_TEST(v2_original_tt_publication_precedes_pin_registration)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;
		ClusterRelmapRetentionDirectory *directory;
		ClusterRelmapRetentionPin unrelated;
		ClusterCanonicalTxnBinding absent;
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture(which >= 3, mask, false);
		unrelated = directory->pins[0];
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		UT_ASSERT(!test_relmap_native_tt_lookup(100, &absent));
		(void)GetTopFullTransactionId();
		if (!original_tt_publish())
		{
			original_tt_done();
			continue;
		}
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (!refused)
			original_tt_pins(directory, &unrelated, mask, true);
		for (unsigned slot = 0; slot < 2; slot++)
			physical_pair_equals(slot, mask & (1U << slot) ? &terminal_images[slot] : &physical_initial[slot]);
		UT_ASSERT(!test_relmap_native_tt_lookup(101, &absent));
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bind_records, 1);
		original_tt_done();
	}
}

UT_TEST(v2_original_tt_publication_attaches_to_all_existing_staged_pins)
{
	for (unsigned which = 0; which < 6; which++)
	{
		unsigned mask = which % 3 + 1;
		ClusterRelmapRetentionDirectory *directory, before;
		ClusterRelmapRetentionPin unrelated;
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture(which >= 3, mask, false);
		unrelated = directory->pins[0];
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused || !original_tt_publish())
		{
			original_tt_done();
			continue;
		}
		original_tt_pins(directory, &unrelated, mask, false);
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		original_tt_pins(directory, &unrelated, mask, true);
		before = *directory;
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_OK);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bind_records, 1);
		original_tt_done();
	}
}

UT_TEST(v2_original_tt_publication_survives_late_pin_registration_refusal)
{
	for (unsigned which = 0; which < 6; which++)
	{
		ClusterRelmapRetentionDirectory *directory, before;
		ClusterCanonicalTxnBinding receipt, after;
		ClusterRelmapProducerArmV2 saved[2];
		bool refused;

		cluster_node_id = 0;
		directory = physical_fixture(which >= 3, 3, false);
		test_relmap_native_tt_reset();
		v2_published_binding_observer = test_relmap_native_tt_lookup;
		refused = precommit_error(false);
		UT_ASSERT(!refused);
		if (refused || !original_tt_publish())
		{
			original_tt_done();
			continue;
		}
		UT_ASSERT(test_relmap_native_tt_lookup(100, &receipt));
		if (which % 3 == 0) directory->state = CLUSTER_RELMAP_RETENTION_UNKNOWN;
		if (which % 3 == 1) directory->state = CLUSTER_RELMAP_RETENTION_BUILDING;
		if (which % 3 == 2) writer_available = false;
		before = *directory;
		memcpy(saved, cluster_relmap_v2_arms, sizeof(saved));
		UT_ASSERT_EQ(cluster_relmap_retention_runtime_bind_current_v2(), CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT(memcmp(&before, directory, sizeof(before)) == 0);
		UT_ASSERT(memcmp(saved, cluster_relmap_v2_arms, sizeof(saved)) == 0);
		UT_ASSERT(test_relmap_native_tt_lookup(100, &after));
		UT_ASSERT(memcmp(&receipt, &after, sizeof(receipt)) == 0);
		UT_ASSERT_EQ(test_relmap_native_tt_stats().bind_records, 1);
		UT_ASSERT(cluster_relmap_authority_x_held());
		UT_ASSERT_EQ(terminal_calls + unlock_calls, 0);
		for (unsigned slot = 0; slot < 2; slot++)
			physical_pair_equals(slot, &terminal_images[slot]);
		original_tt_done();
	}
}
