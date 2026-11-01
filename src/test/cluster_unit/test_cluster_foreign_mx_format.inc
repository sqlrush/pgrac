/*-------------------------------------------------------------------------
 * test_cluster_foreign_mx_format.inc
 *     Foreign-MultiXact format and connection capability registration tests.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_foreign_mx_format.inc
 * NOTES
 *     Included by the original Smart Fusion capability fixture.
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_foreign_mx_format.h"
#include "cluster/cluster_recovery_registry_reservation_format.h"

UT_TEST(test_registry_reservation_has_a_separate_format_identity)
{
	UT_ASSERT_EQ(CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1, UINT32_C(0x52535631));
	UT_ASSERT(CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1
			  != CLUSTER_FOREIGN_MX_FORMAT_ID_V1);
	UT_ASSERT(CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1 != UINT32_C(0x45314332));
	UT_ASSERT(cluster_recovery_registry_reservation_format_registered_v1(
		CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1, 3, 1, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_recovery_registry_reservation_format_registered_v1(
		CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 3, 1, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_recovery_registry_reservation_format_registered_v1(UINT32_C(0x45314332), 3,
																		  1, CATALOG_VERSION_NO));
}

UT_TEST(test_registry_reservation_refuses_old_versions_and_catalogs)
{
	const uint32 id = CLUSTER_RECOVERY_REGISTRY_RESERVATION_FORMAT_ID_V1;

	UT_ASSERT_EQ(CLUSTER_RECOVERY_REGISTRY_WITH_RESERVATIONS_VERSION, 3);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_REGISTRY_RESERVATION_RECORD_VERSION_V1, 1);
	UT_ASSERT_EQ(CLUSTER_RECOVERY_REGISTRY_RESERVATION_MIN_CATALOG_VERSION, UINT32_C(202611010));
	UT_ASSERT(
		!cluster_recovery_registry_reservation_format_registered_v1(id, 2, 1, CATALOG_VERSION_NO));
	UT_ASSERT(
		!cluster_recovery_registry_reservation_format_registered_v1(id, 3, 0, CATALOG_VERSION_NO));
	UT_ASSERT(
		!cluster_recovery_registry_reservation_format_registered_v1(id, 3, 2, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_recovery_registry_reservation_format_registered_v1(id, 3, 1, 0));
	UT_ASSERT(
		!cluster_recovery_registry_reservation_format_registered_v1(id, 3, 1, UINT32_C(202610220)));
	UT_ASSERT(!cluster_recovery_registry_reservation_format_registered_v1(id, 3, 1,
																		  CATALOG_VERSION_NO + 1));
}

UT_TEST(test_foreign_mx_format_registration_is_distinct)
{
	UT_ASSERT_EQ(CLUSTER_FOREIGN_MX_FORMAT_ID_V1, UINT32_C(0x4D584731));
	UT_ASSERT_EQ(CLUSTER_FOREIGN_MX_FORMAT_VERSION_V1, 1);
	UT_ASSERT_EQ(CLUSTER_FOREIGN_MX_MIN_CATALOG_VERSION, UINT32_C(202611010));
	UT_ASSERT(cluster_foreign_mx_format_registered_v1(CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1,
													  CATALOG_VERSION_NO));
	UT_ASSERT(
		!cluster_foreign_mx_format_registered_v1(UINT32_C(0x45314332), 2, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_foreign_mx_format_registered_v1(5, 5, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_foreign_mx_format_registered_v1(6, 6, CATALOG_VERSION_NO));
	UT_ASSERT(!cluster_foreign_mx_format_registered_v1(PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1, 1,
													   CATALOG_VERSION_NO));
}

UT_TEST(test_foreign_mx_requires_every_capability_from_one_record)
{
	const uint32 bits[]
		= { PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1, PGRAC_IC_HELLO_CAP_MULTIXACT_CTRC_V1,
			PGRAC_IC_HELLO_CAP_MULTIXACT_FULL_V1 };
	uint32 generation;
	unsigned i;

	test_sf_cap_store_reset();
	for (i = 0; i < lengthof(bits); i++) {
		cluster_sf_note_peer_hello_capabilities_gen(
			TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES & ~bits[i], 73);
		generation = 999;
		UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
			TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
		UT_ASSERT_EQ(generation, 0);
	}
}

UT_TEST(test_foreign_mx_samples_exact_connection_generation)
{
	uint32 generation = 999;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, 73);
	UT_ASSERT(cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 73);
	UT_ASSERT(cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, generation));
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, 72));
}

UT_TEST(test_foreign_mx_format_and_loaded_catalog_are_exact)
{
	const uint32 wrong_catalogs[] = { 0, UINT32_C(202610220), CATALOG_VERSION_NO + 1 };
	uint32 generation;
	unsigned i;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, 73);
	for (i = 0; i < lengthof(wrong_catalogs); i++) {
		generation = 999;
		UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
			TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, wrong_catalogs[i], &generation));
		UT_ASSERT_EQ(generation, 0);
		UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
			TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, wrong_catalogs[i], 73));
	}
	generation = 999;
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 2, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, UINT32_C(0x45314332), 1, CATALOG_VERSION_NO, 73));
}

UT_TEST(test_foreign_mx_reconnect_invalidates_the_original_sample)
{
	uint32 generation = 0;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, 73);
	UT_ASSERT(cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, 74);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, generation));
	UT_ASSERT(cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 74);
}

UT_TEST(test_foreign_mx_withdrawal_clears_the_positive_sample)
{
	uint32 generation = 999;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, 73);
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER,
		CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES & ~PGRAC_IC_HELLO_CAP_MULTIXACT_FULL_V1, 74);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, 74));
}

UT_TEST(test_foreign_mx_refuses_missing_or_exhausted_connection_identity)
{
	uint32 generation = 999;

	test_sf_cap_store_reset();
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	cluster_sf_note_peer_hello_capabilities_gen(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_REQUIRED_CAPABILITIES, UINT32_MAX);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	ClusterSfDep->peer_capabilities[TEST_SF_CAP_PEER].generation = 0;
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, 0));
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_matches_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, UINT32_MAX));
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, NULL));
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(-1, CLUSTER_FOREIGN_MX_FORMAT_ID_V1,
															   1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
	ClusterSfDep = NULL;
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
}

UT_TEST(test_foreign_mx_does_not_reinterpret_current_v1_capability)
{
	uint32 generation = 999;

	test_sf_cap_store_reset();
	cluster_sf_note_peer_hello_capabilities_gen(TEST_SF_CAP_PEER,
												PGRAC_IC_HELLO_CAP_MULTIXACT_CURRENT_V1, 73);
	UT_ASSERT(
		cluster_sf_peer_multixact_current_capability_generation(TEST_SF_CAP_PEER, &generation));
	UT_ASSERT_EQ(generation, 73);
	UT_ASSERT(!cluster_sf_peer_foreign_mx_capability_sample_v1(
		TEST_SF_CAP_PEER, CLUSTER_FOREIGN_MX_FORMAT_ID_V1, 1, CATALOG_VERSION_NO, &generation));
	UT_ASSERT_EQ(generation, 0);
}
