/* Exercise the real locator and its local diagnostic wrapper. */
int durable_fixture_main(int argc, char **argv);
#define main durable_fixture_main
#include "test_cluster_tt_durable.c"
#undef main

typedef struct TestLocatorFamilyRow {
	uint64 fallback_count;
	uint64 found_count;
	uint64 missing_count;
	uint64 ambiguous_count;
	uint64 unavailable_count;
} TestLocatorFamilyRow;

static bool family_dependency_cancel;

#ifdef CLUSTER_TT_DURABLE_LOCATOR_FAMILY_STATS_V1
static ClusterTTDurableLocate
family_scan_dependency(int origin, TransactionId xid, uint16 *segment, uint16 *slot, uint16 *wrap,
					   uint8 *status)
{
	/* Inject ERROR at the scanner dependency boundary, not inside smgr. */
	if (family_dependency_cancel)
		ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("injected locator caller cancel")));
	return cluster_tt_slot_durable_locate_any_by_xid_origin(origin, xid, segment, slot, wrap,
															status);
}
#define cluster_tt_slot_durable_locate_any_by_xid_origin family_scan_dependency
#endif
#define CLUSTER_TT_LOCATOR_FAMILY_TABLE_DECLARED
#include "test_cluster_tt_locator_family.inc"
#undef CLUSTER_TT_LOCATOR_FAMILY_TABLE_DECLARED
#ifdef CLUSTER_TT_DURABLE_LOCATOR_FAMILY_STATS_V1
#undef cluster_tt_slot_durable_locate_any_by_xid_origin
#endif

static ClusterTTDurableLocate
family_locate(int family, int origin, TransactionId xid, uint16 *segment, uint16 *slot,
			  uint16 *wrap, uint8 *status)
{
#ifdef CLUSTER_TT_DURABLE_LOCATOR_FAMILY_STATS_V1
	return cluster_tt_slot_durable_locate_any_by_xid_origin_profiled(
		(ClusterTTDurableLocatorFamily)family, origin, xid, segment, slot, wrap, status);
#else
	(void)family;
	return cluster_tt_slot_durable_locate_any_by_xid_origin(origin, xid, segment, slot, wrap,
															status);
#endif
}

static bool
family_rows(TestLocatorFamilyRow out[4])
{
#ifdef CLUSTER_TT_DURABLE_LOCATOR_FAMILY_STATS_V1
	TestLocatorStats stats;

	if (out == NULL || !locator_snapshot(&stats))
		return false;
	for (unsigned i = 0; i < 4; i++) {
		out[i].fallback_count = stats.families[i].fallback_count;
		out[i].found_count = stats.families[i].found_count;
		out[i].missing_count = stats.families[i].missing_count;
		out[i].ambiguous_count = stats.families[i].ambiguous_count;
		out[i].unavailable_count = stats.families[i].unavailable_count;
	}
	return true;
#else
	(void)out;
	return false;
#endif
}

UT_TEST(test_profiled_locator_keeps_real_results_and_separates_all_four_families)
{
	TestLocatorFamilyRow rows[4];
	TestLocatorStats total;

	reset_locator_stats();
	for (int family = 0; family < 4; family++) {
		uint16 segment = 0xffff, slot = 0xffff, wrap = 0xffff;
		uint8 status = TT_SLOT_INVALID;

		memset(g_canned_block, 0, sizeof(g_canned_block));
		seed_block_slot(6, TT_SLOT_ACTIVE, 12345, InvalidScn);
		for (int repeat = 0; repeat < 2; repeat++) {
			UT_ASSERT_EQ(family_locate(family, 0, 12345, &segment, &slot, &wrap, &status),
						 CLUSTER_TT_DURABLE_LOCATE_FOUND);
			UT_ASSERT_EQ(segment, 1);
			UT_ASSERT_EQ(slot, 6);
			UT_ASSERT_EQ(status, TT_SLOT_ACTIVE);
		}
		memset(g_canned_block, 0, sizeof(g_canned_block));
		segment = slot = wrap = 0xffff;
		status = TT_SLOT_INVALID;
		UT_ASSERT_EQ(family_locate(family, 0, 12345, &segment, &slot, &wrap, &status),
					 CLUSTER_TT_DURABLE_LOCATE_MISSING);
		seed_block_slot(3, TT_SLOT_ACTIVE, 12345, InvalidScn);
		seed_block_slot(9, TT_SLOT_ABORTED, 12345, InvalidScn);
		UT_ASSERT_EQ(family_locate(family, 0, 12345, &segment, &slot, &wrap, &status),
					 CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS);
		memset(g_canned_block, 0, sizeof(g_canned_block));
		g_unreadable_existing_segment = 2;
		UT_ASSERT_EQ(family_locate(family, 0, 12345, &segment, &slot, &wrap, &status),
					 CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE);
		UT_ASSERT_EQ(family_locate(family, -1, 12345, &segment, &slot, &wrap, &status),
					 CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE);
		UT_ASSERT_EQ(segment, 0xffff);
		UT_ASSERT_EQ(slot, 0xffff);
		UT_ASSERT_EQ(wrap, 0xffff);
		UT_ASSERT_EQ(status, TT_SLOT_INVALID);
		g_unreadable_existing_segment = 0;
	}
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned family = 0; family < 4; family++) {
		UT_ASSERT_EQ(rows[family].fallback_count, 6);
		UT_ASSERT_EQ(rows[family].found_count, 2);
		UT_ASSERT_EQ(rows[family].missing_count, 1);
		UT_ASSERT_EQ(rows[family].ambiguous_count, 1);
		UT_ASSERT_EQ(rows[family].unavailable_count, 2);
	}
	total = read_locator_stats();
	UT_ASSERT_EQ(total.fallback_count, 24);
	UT_ASSERT_EQ(total.found_count, 8);
	UT_ASSERT_EQ(total.missing_count, 4);
	UT_ASSERT_EQ(total.ambiguous_count, 4);
	UT_ASSERT_EQ(total.unavailable_count, 8);
}

UT_TEST(test_family_snapshot_keeps_unavailable_zero_unsigned_and_attach_distinct)
{
	TestLocatorFamilyRow rows[4], before[4];
	TestLocatorStats total;

	reset_locator_stats();
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(!family_rows(NULL));
	for (unsigned i = 0; i < 4; i++) {
		UT_ASSERT_EQ(rows[i].fallback_count, 0);
		UT_ASSERT_EQ(rows[i].found_count, 0);
		UT_ASSERT_EQ(rows[i].missing_count, 0);
		UT_ASSERT_EQ(rows[i].ambiguous_count, 0);
		UT_ASSERT_EQ(rows[i].unavailable_count, 0);
	}
	memset(rows, 0xa5, sizeof(rows));
	memcpy(before, rows, sizeof(rows));
	TTDurableShared = NULL;
	UT_ASSERT(!family_rows(rows));
	UT_ASSERT(memcmp(rows, before, sizeof(rows)) == 0);
	cluster_tt_durable_shmem_init();
	cluster_node_id = -1;
	UT_ASSERT(!family_rows(rows));
	UT_ASSERT(memcmp(rows, before, sizeof(rows)) == 0);
	cluster_node_id = 3;
	UT_ASSERT_EQ(cluster_tt_slot_durable_locate_any_by_xid_origin(0, 12345, NULL, NULL, NULL, NULL),
				 CLUSTER_TT_DURABLE_LOCATE_MISSING);
	UT_ASSERT_EQ(family_locate(-1, 0, 12345, NULL, NULL, NULL, NULL),
				 CLUSTER_TT_DURABLE_LOCATE_MISSING);
	UT_ASSERT_EQ(family_locate(4, 0, 12345, NULL, NULL, NULL, NULL),
				 CLUSTER_TT_DURABLE_LOCATE_MISSING);
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	for (unsigned i = 0; i < 4; i++)
		UT_ASSERT_EQ(rows[i].fallback_count, 0);
	total = read_locator_stats();
	UT_ASSERT_EQ(total.origin_node_id, 3);
	UT_ASSERT_EQ(total.fallback_count, 3);
#ifdef CLUSTER_TT_DURABLE_LOCATOR_FAMILY_STATS_V1
	pg_atomic_write_u64(&TTDurableShared->locator_family_fallback_count[2], UINT64_MAX);
	pg_atomic_write_u64(&TTDurableShared->locator_family_found_count[2],
						UINT64_C(9223372036854775809));
	pg_atomic_write_u64(&TTDurableShared->locator_family_missing_count[2], 3);
	pg_atomic_write_u64(&TTDurableShared->locator_family_ambiguous_count[2], 5);
	pg_atomic_write_u64(&TTDurableShared->locator_family_unavailable_count[2], 7);
#endif
	TTDurableShared = NULL;
	cluster_tt_durable_shmem_init();
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT(rows[2].fallback_count == UINT64_MAX);
	UT_ASSERT(rows[2].found_count == UINT64_C(9223372036854775809));
	UT_ASSERT_EQ(rows[2].missing_count, 3);
	UT_ASSERT_EQ(rows[2].ambiguous_count, 5);
	UT_ASSERT_EQ(rows[2].unavailable_count, 7);
	UT_ASSERT_EQ(read_locator_stats().fallback_count, 3);
	cluster_node_id = 0;
}

UT_TEST(test_profiled_locator_error_keeps_entry_without_fabricating_a_result)
{
	TestLocatorFamilyRow rows[4];
	uint16 segment = 0xffff, slot = 0xffff, wrap = 0xffff;
	uint8 status = TT_SLOT_INVALID;
	volatile bool caught = false;

	reset_locator_stats();
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	family_dependency_cancel = true;
	PG_TRY();
	{
		(void)family_locate(1, 0, 12345, &segment, &slot, &wrap, &status);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	family_dependency_cancel = false;
	UT_ASSERT(caught);
	UT_ASSERT_EQ(segment, 0xffff);
	UT_ASSERT_EQ(slot, 0xffff);
	UT_ASSERT_EQ(wrap, 0xffff);
	UT_ASSERT_EQ(status, TT_SLOT_INVALID);
	if (!family_rows(rows)) {
		UT_ASSERT(false);
		return;
	}
	UT_ASSERT_EQ(rows[1].fallback_count, 1);
	UT_ASSERT_EQ(rows[1].found_count, 0);
	UT_ASSERT_EQ(rows[1].missing_count, 0);
	UT_ASSERT_EQ(rows[1].ambiguous_count, 0);
	UT_ASSERT_EQ(rows[1].unavailable_count, 0);
	UT_ASSERT_EQ(rows[0].fallback_count, 0);
	UT_ASSERT_EQ(rows[2].fallback_count, 0);
	UT_ASSERT_EQ(rows[3].fallback_count, 0);
}

int
main(void)
{
	printf("# locator layout shared=%zu snapshot=%zu\n", sizeof(ClusterTTDurableShared),
		   sizeof(TestLocatorStats));
	UT_PLAN(3);
	UT_RUN(test_profiled_locator_keeps_real_results_and_separates_all_four_families);
	UT_RUN(test_family_snapshot_keeps_unavailable_zero_unsigned_and_attach_distinct);
	UT_RUN(test_profiled_locator_error_keeps_entry_without_fabricating_a_result);
	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
