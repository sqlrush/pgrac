/*-------------------------------------------------------------------------
 *
 * test_cluster_r4_owner_challenge.c
 *    Exact requester-owner CR wire and rejection contracts.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_r4_owner_challenge.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_grd_outbound.h"

#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

UT_TEST(test_owner_request_has_separate_exact_carrier)
{
	UT_ASSERT_EQ(sizeof(ClusterR4CrRequestPayload), 120);
}

UT_TEST(test_owner_forward_has_separate_exact_carrier)
{
	UT_ASSERT_EQ(sizeof(ClusterR4CrForwardPayload), 136);
}

UT_TEST(test_owner_route_proof_keeps_exact_challenge)
{
	UT_ASSERT_EQ(sizeof(ClusterR4CrRouteProof), 120);
}

UT_TEST(test_owner_cr_wire_version_is_new_cluster_only)
{
	UT_ASSERT_EQ(CLUSTER_R4_WIRE_VERSION, 2);
}

UT_TEST(test_owner_request_rejects_old_version)
{
	ClusterR4RequestExtension extension;
	SCN scn;

	UT_ASSERT(ClusterR4RequestExtensionSetCr(&extension, (SCN)100));
	extension.r4_version = 1;
	UT_ASSERT(!ClusterR4RequestExtensionGetCr(&extension, &scn));
}

UT_TEST(test_owner_forward_rejects_old_version)
{
	ClusterR4ForwardExtension extension;
	uint64 generation, transition;
	SCN scn;

	ClusterR4ForwardExtensionSetCrProof(&extension, UINT64CONST(0x100000001), 7, (SCN)99);
	extension.r4_version = 1;
	UT_ASSERT(!ClusterR4ForwardExtensionGetCrProof(&extension, 1, &generation, &transition, &scn));
}

UT_TEST(test_owner_request_fits_backend_outbound_ring)
{
	UT_ASSERT(PGRAC_GES_OUTBOUND_PAYLOAD_MAX >= sizeof(ClusterR4CrRequestPayload));
}

UT_TEST(test_owner_literal_le_carrier_roundtrip_and_no_owner)
{
	ClusterR4CrOwnerChallenge owner = { .top_fxid = FullTransactionIdFromEpochAndXid(9, 100),
										.active_lsn = 123,
										.segment_id = 1,
										.segment_generation = 23,
										.top_xid = 100,
										.slot_offset = 3,
										.slot_wrap = 8,
										.origin_instance = 1,
										.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,
										.flags = CLUSTER_R4_CR_OWNER_PRESENT };
	static const uint8 literal[40]
		= { 100, 0, 0, 0, 9,   0, 0, 0, 123, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
			23,	 0, 0, 0, 100, 0, 0, 0, 3,	 0, 8, 0, 1, 3, 1, 0, 1, 0, 0, 0 };
	ClusterR4CrOwnerChallenge decoded;
	ClusterR4CrOwnerWire wire;

	UT_ASSERT(ClusterR4CrOwnerWireSet(&wire, &owner));
	UT_ASSERT_EQ(memcmp(wire.bytes, literal, sizeof(literal)), 0);
	UT_ASSERT(ClusterR4CrOwnerWireGet(&wire, &decoded));
	UT_ASSERT(ClusterR4CrOwnerEqual(&owner, &decoded));
	UT_ASSERT(ClusterR4CrOwnerForRequester(&owner, 0));
	UT_ASSERT(!ClusterR4CrOwnerForRequester(&owner, 1));
	memset(&owner, 0, sizeof(owner));
	UT_ASSERT(ClusterR4CrOwnerWireSet(&wire, &owner));
	UT_ASSERT_EQ(wire.bytes[36], 1);
	UT_ASSERT(ClusterR4CrOwnerWireGet(&wire, &decoded));
	UT_ASSERT(ClusterR4CrOwnerEqual(&owner, &decoded));
	wire.bytes[0] = 1;
	UT_ASSERT(!ClusterR4CrOwnerWireGet(&wire, &decoded));
	UT_ASSERT_EQ(U64FromFullTransactionId(decoded.top_fxid), 0);
}

UT_TEST(test_owner_codec_rejects_unknown_flags_versions_and_partial_identity)
{
	ClusterR4CrOwnerChallenge owner = { .top_fxid = FullTransactionIdFromEpochAndXid(9, 100),
										.active_lsn = 123,
										.segment_id = 1,
										.segment_generation = 23,
										.top_xid = 100,
										.slot_offset = 3,
										.slot_wrap = 8,
										.origin_instance = 1,
										.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED,
										.flags = CLUSTER_R4_CR_OWNER_PRESENT };
	const unsigned offsets[] = { 0, 8, 16, 24, 32, 33, 34, 36, 37, 38, 39 };
	ClusterR4CrOwnerChallenge decoded;
	ClusterR4CrOwnerWire wire;

	for (unsigned i = 0; i < lengthof(offsets); i++) {
		UT_ASSERT(ClusterR4CrOwnerWireSet(&wire, &owner));
		if (offsets[i] == 8)
			memset(wire.bytes + 8, 0, 8);
		else if (offsets[i] == 16)
			memset(wire.bytes + 16, 0, 4);
		else if (offsets[i] == 32 || offsets[i] == 33)
			wire.bytes[offsets[i]] = 0;
		else
			wire.bytes[offsets[i]] ^= 0xff;
		memset(&decoded, 0xa5, sizeof(decoded));
		UT_ASSERT(!ClusterR4CrOwnerWireGet(&wire, &decoded));
		UT_ASSERT_EQ(decoded.flags, 0);
	}
	UT_ASSERT(!ClusterR4CrOwnerWireGet(NULL, &decoded));
	UT_ASSERT(!ClusterR4CrOwnerWireSet(&wire, NULL));
	UT_ASSERT_EQ(wire.bytes[36], 0);
	UT_ASSERT_EQ(sizeof(ClusterR4InternalForwardPayload), 96);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_owner_literal_le_carrier_roundtrip_and_no_owner);
	UT_RUN(test_owner_codec_rejects_unknown_flags_versions_and_partial_identity);
	UT_RUN(test_owner_request_has_separate_exact_carrier);
	UT_RUN(test_owner_forward_has_separate_exact_carrier);
	UT_RUN(test_owner_route_proof_keeps_exact_challenge);
	UT_RUN(test_owner_cr_wire_version_is_new_cluster_only);
	UT_RUN(test_owner_request_rejects_old_version);
	UT_RUN(test_owner_forward_rejects_old_version);
	UT_RUN(test_owner_request_fits_backend_outbound_ring);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
