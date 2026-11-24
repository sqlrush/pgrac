/* Original retention core, including every original mutation entry point. */
int k_relmap_retention_original_main(void);
#define main k_relmap_retention_original_main
#include "test_cluster_relmap_retention.c"
#undef main

UT_TEST(test_history_requires_complete_empty_original_directory)
{
	ClusterRelmapRetentionDirectory d = { 0 };
	ClusterRelmapRetentionEmptyV1 observed = { 0 };
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	building(&d);
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT(cluster_relmap_retention_empty_current_v1(&d, &observed));
	UT_ASSERT_EQ(observed.system_identifier, 1234);
	UT_ASSERT_EQ(observed.database_incarnation, 5678);
	UT_ASSERT_EQ(observed.origin, 3);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT_EQ(observed.source_serial, 0);
}

UT_TEST(test_history_cannot_cross_original_object_or_scope)
{
	ClusterRelmapRetentionDirectory d, other;
	ClusterRelmapRetentionEmptyV1 observed, wrong;

	ready(&d);
	ready(&other);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&other, &observed));
#define WRONG_FIELD(field, value) do { wrong = observed; wrong.field = (value); \
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &wrong)); } while (0)
	WRONG_FIELD(system_identifier, 4321);
	WRONG_FIELD(database_incarnation, 8765);
	WRONG_FIELD(origin, 4);
	WRONG_FIELD(generation, observed.generation + 1);
	WRONG_FIELD(source_serial, observed.source_serial + 1);
	WRONG_FIELD(source, NULL);
#undef WRONG_FIELD
	UT_ASSERT(cluster_relmap_retention_empty_current_v1(&d, &observed));
}

UT_TEST(test_history_detects_begin_cancel_aba_across_unlock)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionEmptyV1 before, after;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	uint64 generation, truncate_serial;

	ready(&d);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &before));
	generation = d.generation;
	truncate_serial = d.truncate_serial;
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	UT_ASSERT_EQ(d.generation, generation);
	UT_ASSERT_EQ(d.truncate_serial, truncate_serial);
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &before));
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &after));
	UT_ASSERT(after.source_serial > before.source_serial);
}

UT_TEST(test_history_binding_stage_and_exact_retries)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(BASE + 100);
	uint64 serial;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	serial = d.source_serial;
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(d.source_serial, serial);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT(d.source_serial > serial);
	serial = d.source_serial;
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT_EQ(d.source_serial, serial);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 456), OK);
	UT_ASSERT(d.source_serial > serial);
	serial = d.source_serial;
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 456), OK);
	UT_ASSERT_EQ(d.source_serial, serial);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key),
		CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(d.source_serial, serial);
}

UT_TEST(test_history_native_cuts_and_allocation_invalidate_observation)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionEmptyV1 observed, after;
	ClusterRelmapRetentionTruncate cut;
	uint64 serial;

	ready(&d);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 100, &cut), OK);
	serial = d.source_serial;
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &observed));
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &after));
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
	UT_ASSERT(d.source_serial > serial);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &after));
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &observed));
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&d, BASE + 1001), OK);
	UT_ASSERT(cluster_relmap_retention_empty_current_v1(&d, &after));
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + 1001), OK);
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &after));
}

UT_TEST(test_history_failed_restore_and_invalidation_preserve_obligations)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionEmptyV1 observed;
	uint64 serial;

	building(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 123), OK);
	serial = d.source_serial;
	UT_ASSERT_NE(cluster_relmap_retention_build_add(&d, &key, 456), OK);
	UT_ASSERT(d.source_serial > serial);
	UT_ASSERT(has_pin(&d, 42, BASE + 100));
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	ready(&d);
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &observed));
	serial = d.source_serial;
	cluster_relmap_retention_invalidate(&d);
	UT_ASSERT(d.source_serial > serial);
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &observed));
	UT_ASSERT_NE(cluster_relmap_retention_build_start(&d, 1234, 5678, 3, 7,
		BASE + 10, BASE + 1000), OK);
}

UT_TEST(test_history_serial_exhaustion_refuses_observation_without_blocking_writer)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionEmptyV1 observed;

	ready(&d);
	d.source_serial = PG_UINT64_MAX - 1;
	UT_ASSERT(cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(d.source_serial, PG_UINT64_MAX);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	UT_ASSERT_EQ(d.source_serial, PG_UINT64_MAX);
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, &observed));
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + 1001), OK);
	UT_ASSERT_EQ(d.source_serial, PG_UINT64_MAX);
	d.source_serial = 0;
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
}

UT_TEST(test_history_output_alias_and_residual_pin_never_erase_source)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionEmptyV1 observed;

	ready(&d);
	before = d;
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d,
		(ClusterRelmapRetentionEmptyV1 *)&d.pins[1]));
	UT_ASSERT_EQ(memcmp(&d, &before, sizeof(d)), 0);
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, NULL));
	UT_ASSERT(!cluster_relmap_retention_empty_current_v1(&d, NULL));
	d.pins[0].has_tt_binding = true;
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	d.pins[0].has_tt_binding = false;
	d.pins[63].wal_end = 7;
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(&d, &observed));
	memset(&observed, 0xff, sizeof(observed));
	UT_ASSERT(!cluster_relmap_retention_observe_empty_v1(NULL, &observed));
	UT_ASSERT_EQ(observed.source_serial, 0);
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(test_history_requires_complete_empty_original_directory);
	UT_RUN(test_history_cannot_cross_original_object_or_scope);
	UT_RUN(test_history_detects_begin_cancel_aba_across_unlock);
	UT_RUN(test_history_binding_stage_and_exact_retries);
	UT_RUN(test_history_native_cuts_and_allocation_invalidate_observation);
	UT_RUN(test_history_failed_restore_and_invalidation_preserve_obligations);
	UT_RUN(test_history_serial_exhaustion_refuses_observation_without_blocking_writer);
	UT_RUN(test_history_output_alias_and_residual_pin_never_erase_source);
	UT_DONE();
	return ut_failed_count != 0;
}
