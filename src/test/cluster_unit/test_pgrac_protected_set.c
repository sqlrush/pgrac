/*-------------------------------------------------------------------------
 *
 * test_pgrac_protected_set.c
 *    Real common-code tests of versioned storage route identities.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_pgrac_protected_set.c
 *
 * NOTES
 *    PGRAC-original unit. Golden bytes are independently computed from the
 *    specified length-prefixed fields, not from the production hash helper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/pgrac_external_fence_protocol.h"
#include "common/pgrac_fence_map.h"
#include "common/pgrac_protected_set.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static const uint8 golden[32] = { 0x84, 0xdc, 0x62, 0xa0, 0x17, 0x25, 0xf0, 0x36, 0x1b, 0x3f, 0xa2,
								  0xde, 0x56, 0x15, 0x63, 0x2a, 0x8f, 0xb9, 0xa9, 0x5d, 0x07, 0xe5,
								  0x64, 0xfb, 0xb3, 0xa6, 0x61, 0xd5, 0x1f, 0xd0, 0x68, 0x28 };

static PgracProtectedTextV2
text_field(const char *value)
{
	PgracProtectedTextV2 result;

	result.data = value;
	result.length = strlen(value);
	return result;
}

static void
fixture(PgracProtectedSetV2 *set, PgracProtectedRouteV2 routes[4])
{
	unsigned int i;

	memset(set, 0, sizeof(*set));
	memset(routes, 0, 4 * sizeof(*routes));
	set->backend_id = 3;
	memset(set->database_uuid, 1, 16);
	memset(set->storage_uuid, 2, 16);
	memset(set->authority_uuid, 3, 16);
	set->certification_profile = text_field("pre2-kvm-gfs2-v1");
	set->mapping_generation = 7;
	memset(set->hypervisor_uuid, 4, 16);
	memset(set->guest_uuid, 5, 16);
	set->route_count = 4;
	set->routes = routes;
	for (i = 0; i < 4; i++) {
		routes[i].kind = PGRAC_PROTECTED_ROUTE_ISCSI;
		routes[i].initiator = text_field("iqn.2026-09.test:guest0");
		routes[i].credential_ref = text_field("cred-1");
		routes[i].target = text_field("iqn.2026-09.test:target");
		routes[i].endpoint = text_field("10.0.0.10:3260");
		routes[i].tpg = i + 1;
		routes[i].lun_wwid = text_field("wwid-1");
		routes[i].lun_serial = text_field("serial-1");
		memset(routes[i].backstore_uuid, 16 + i, 16);
		routes[i].media_kind
			= i == 3 ? PGRAC_PROTECTED_MEDIA_RAW : PGRAC_PROTECTED_MEDIA_FILESYSTEM;
		memset(routes[i].filesystem_uuid, i == 3 ? 0 : 32 + i, 16);
		routes[i].roles = UINT32_C(1) << i;
	}
}

static bool
rejected_and_cleared(const PgracProtectedSetV2 *set)
{
	uint8 digest[32];
	const uint8 zero[32] = { 0 };

	memset(digest, 0xa5, sizeof(digest));
	return !pgrac_external_fence_protected_set_digest_v2(set, digest)
		   && memcmp(digest, zero, sizeof(digest)) == 0;
}

static bool
changed_digest(const PgracProtectedSetV2 *set)
{
	uint8 digest[32];

	return pgrac_external_fence_protected_set_digest_v2(set, digest)
		   && memcmp(digest, golden, sizeof(digest)) != 0;
}

UT_TEST(test_golden_and_order_invariance)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	PgracProtectedRouteV2 reversed[4];
	uint8 digest[32];
	unsigned int i;

	fixture(&set, routes);
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&set, digest));
	UT_ASSERT(memcmp(digest, golden, sizeof(digest)) == 0);
	for (i = 0; i < 4; i++)
		reversed[i] = routes[3 - i];
	set.routes = reversed;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&set, digest));
	UT_ASSERT(memcmp(digest, golden, sizeof(digest)) == 0);
	UT_ASSERT(reversed[0].tpg == 4);
}

UT_TEST(test_same_storage_different_wal_voting_and_alternate_route)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[5];
	uint8 legacy[32];
	uint8 unchanged_legacy[32];

	fixture(&set, routes);
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(3, set.storage_uuid, legacy));
	routes[1].backstore_uuid[0] ^= 1;
	UT_ASSERT(changed_digest(&set));
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(3, set.storage_uuid, unchanged_legacy));
	UT_ASSERT(memcmp(legacy, unchanged_legacy, sizeof(legacy)) == 0);
	fixture(&set, routes);
	routes[3].lun_wwid = text_field("replacement-voting-disk");
	UT_ASSERT(changed_digest(&set));
	fixture(&set, routes);
	routes[4] = routes[0];
	/* set.routes aliases routes; changed_digest hashes the added route endpoint. */
	// cppcheck-suppress unreadVariable
	routes[4].endpoint = text_field("10.0.0.11:3260");
	set.route_count = 5;
	UT_ASSERT(changed_digest(&set));
}

UT_TEST(test_every_top_level_identity_is_bound)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	unsigned int i;

	for (i = 0; i < 7; i++) {
		fixture(&set, routes);
		switch (i) {
		case 0:
			set.database_uuid[0] ^= 1;
			break;
		case 1:
			set.storage_uuid[0] ^= 1;
			break;
		case 2:
			set.authority_uuid[0] ^= 1;
			break;
		case 3:
			set.certification_profile = text_field("pre2-kvm-gfs2-v2");
			break;
		case 4:
			set.mapping_generation++;
			break;
		case 5:
			set.hypervisor_uuid[0] ^= 1;
			break;
		case 6:
			set.guest_uuid[0] ^= 1;
			break;
		}
		UT_ASSERT(changed_digest(&set));
	}
}

UT_TEST(test_every_route_identity_is_bound)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	unsigned int i;

	for (i = 0; i < 12; i++) {
		fixture(&set, routes);
		switch (i) {
		case 0:
			routes[0].kind = PGRAC_PROTECTED_ROUTE_HOST;
			routes[0].tpg = 0;
			break;
		case 1:
			routes[0].initiator = text_field("iqn.2026-09.test:guest1");
			break;
		case 2:
			routes[0].credential_ref = text_field("cred-2");
			break;
		case 3:
			routes[0].target = text_field("iqn.2026-09.test:target2");
			break;
		case 4:
			routes[0].endpoint = text_field("10.0.0.11:3260");
			break;
		case 5:
			routes[0].tpg++;
			break;
		case 6:
			routes[0].lun_wwid = text_field("wwid-2");
			break;
		case 7:
			routes[0].lun_serial = text_field("serial-2");
			break;
		case 8:
			routes[0].backstore_uuid[0] ^= 1;
			break;
		case 9:
			routes[0].media_kind = PGRAC_PROTECTED_MEDIA_RAW;
			memset(routes[0].filesystem_uuid, 0, 16);
			break;
		case 10:
			routes[0].filesystem_uuid[0] ^= 1;
			break;
		case 11:
			routes[0].roles |= PGRAC_PROTECTED_ROLE_WAL;
			break;
		}
		UT_ASSERT(changed_digest(&set));
	}
}

UT_TEST(test_missing_roles_and_duplicates_are_rejected)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[5];
	unsigned int i;

	for (i = 0; i < 4; i++) {
		fixture(&set, routes);
		routes[i].roles = i == 0 ? PGRAC_PROTECTED_ROLE_WAL : PGRAC_PROTECTED_ROLE_DATA;
		UT_ASSERT(rejected_and_cleared(&set));
	}
	fixture(&set, routes);
	routes[4] = routes[0];
	set.route_count = 5;
	UT_ASSERT(rejected_and_cleared(&set));
	fixture(&set, routes);
	/* rejected_and_cleared validates these role bits through set.routes. */
	// cppcheck-suppress unreadVariable
	routes[0].roles |= UINT32_C(16);
	UT_ASSERT(rejected_and_cleared(&set));
}

UT_TEST(test_invalid_values_clear_output)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	unsigned int i;

	UT_ASSERT(rejected_and_cleared(NULL));
	fixture(&set, routes);
	UT_ASSERT(!pgrac_external_fence_protected_set_digest_v2(&set, NULL));
	for (i = 0; i < 22; i++) {
		fixture(&set, routes);
		switch (i) {
		case 0:
			set.backend_id = 0;
			break;
		case 1:
			memset(set.database_uuid, 0, 16);
			break;
		case 2:
			memset(set.storage_uuid, 0, 16);
			break;
		case 3:
			memset(set.authority_uuid, 0, 16);
			break;
		case 4:
			set.certification_profile.length = 0;
			break;
		case 5:
			set.mapping_generation = 0;
			break;
		case 6:
			memset(set.hypervisor_uuid, 0, 16);
			break;
		case 7:
			memset(set.guest_uuid, 0, 16);
			break;
		case 8:
			set.route_count = 0;
			break;
		case 9:
			set.routes = NULL;
			break;
		case 10:
			set.route_count = UINT32_MAX;
			break;
		case 11:
			routes[0].kind = 0;
			break;
		case 12:
			routes[0].tpg = 0;
			break;
		case 13:
			routes[0].kind = PGRAC_PROTECTED_ROUTE_HOST;
			break;
		case 14:
			routes[0].initiator.data = NULL;
			break;
		case 15:
			routes[0].credential_ref = text_field("bad reference");
			break;
		case 16:
			routes[0].target.length = UINT32_MAX;
			break;
		case 17:
			memset(routes[0].backstore_uuid, 0, 16);
			break;
		case 18:
			routes[0].media_kind = 0;
			break;
		case 19:
			memset(routes[0].filesystem_uuid, 0, 16);
			break;
		case 20:
			routes[3].filesystem_uuid[0] = 1;
			break;
		case 21:
			routes[0].roles = 0;
			break;
		}
		UT_ASSERT(rejected_and_cleared(&set));
	}
}

UT_TEST(test_explicit_lengths_and_total_size_bound)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[128];
	char text[1025];
	uint8 digest[32];
	unsigned int i;

	fixture(&set, routes);
	routes[0].credential_ref.data = "cred-1ignored";
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&set, digest));
	UT_ASSERT(memcmp(digest, golden, sizeof(digest)) == 0);
	routes[0].credential_ref.length = 7;
	UT_ASSERT(rejected_and_cleared(&set) == false);
	/* Both credential values are read through set.routes by the digest assertions. */
	// cppcheck-suppress redundantAssignment
	routes[0].credential_ref.data = "cred-1\0";
	UT_ASSERT(rejected_and_cleared(&set));
	fixture(&set, routes);
	memset(text, 'x', sizeof(text));
	routes[0].credential_ref.data = text;
	routes[0].credential_ref.length = 1024;
	UT_ASSERT(changed_digest(&set));
	/* Both the 1024-byte and over-limit values are validated through set.routes. */
	// cppcheck-suppress redundantAssignment
	routes[0].credential_ref.length = 1025;
	UT_ASSERT(rejected_and_cleared(&set));
	fixture(&set, routes);
	for (i = 4; i < 128; i++) {
		routes[i] = routes[0];
		routes[i].tpg = i + 1;
		routes[i].credential_ref.data = text;
		routes[i].credential_ref.length = 1024;
	}
	set.route_count = 128;
	UT_ASSERT(rejected_and_cleared(&set));
}

UT_TEST(test_encode_decode_golden_and_truncation)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	PgracProtectedSetDecodedV2 decoded;
	uint8 bytes[879];
	uint8 digest[32];
	size_t written = 17;
	size_t i;
	bool encoded;

	fixture(&set, routes);
	encoded = pgrac_protected_set_v2_encode(&set, bytes, sizeof(bytes), &written);
	UT_ASSERT(encoded);
	if (!encoded)
		return;
	UT_ASSERT_EQ(written, 878);
	UT_ASSERT_EQ(bytes[0], 22);
	UT_ASSERT_EQ(bytes[1], 0);
	UT_ASSERT(memcmp(bytes + 4, "PGRAC-PROTECTED-SET-V2", 22) == 0);
	UT_ASSERT(pgrac_protected_set_v2_decode(bytes, written, &decoded));
	UT_ASSERT(decoded.set.routes == decoded.routes);
	UT_ASSERT_EQ(decoded.set.mapping_generation, 7);
	UT_ASSERT_EQ(decoded.set.route_count, 4);
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&decoded.set, digest));
	UT_ASSERT(memcmp(digest, golden, sizeof(digest)) == 0);
	for (i = 0; i < 878; i++) {
		memset(&decoded, 0xa5, sizeof(decoded));
		UT_ASSERT(!pgrac_protected_set_v2_decode(bytes, i, &decoded));
		UT_ASSERT(decoded.set.routes == NULL && decoded.set.route_count == 0);
	}
	bytes[878] = 0;
	UT_ASSERT(!pgrac_protected_set_v2_decode(bytes, 879, &decoded));
	bytes[0] = 0xff;
	UT_ASSERT(!pgrac_protected_set_v2_decode(bytes, 878, &decoded));
}

UT_TEST(test_decode_rejects_noncanonical_order_and_fields)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	PgracProtectedSetDecodedV2 decoded;
	uint8 bytes[878];
	uint8 changed[878];
	size_t written;
	bool encoded;

	fixture(&set, routes);
	encoded = pgrac_protected_set_v2_encode(&set, bytes, sizeof(bytes), &written);
	UT_ASSERT(encoded);
	if (!encoded)
		return;
	/* Independently fixed offsets: header 174 bytes, each fixture route 176. */
	memcpy(changed, bytes, sizeof(bytes));
	memcpy(changed + 174, bytes + 350, 176);
	memcpy(changed + 350, bytes + 174, 176);
	UT_ASSERT(!pgrac_protected_set_v2_decode(changed, sizeof(changed), &decoded));
	memcpy(changed, bytes, sizeof(bytes));
	memcpy(changed + 350, bytes + 174, 176);
	UT_ASSERT(!pgrac_protected_set_v2_decode(changed, sizeof(changed), &decoded));
	memcpy(changed, bytes, sizeof(bytes));
	changed[25] = '1';
	UT_ASSERT(!pgrac_protected_set_v2_decode(changed, sizeof(changed), &decoded));
	memcpy(changed, bytes, sizeof(bytes));
	changed[26] = 8; /* backend ID must be a four-byte field. */
	UT_ASSERT(!pgrac_protected_set_v2_decode(changed, sizeof(changed), &decoded));
	UT_ASSERT(!pgrac_protected_set_v2_decode(NULL, sizeof(bytes), &decoded));
	UT_ASSERT(!pgrac_protected_set_v2_decode(bytes, sizeof(bytes), NULL));
}

UT_TEST(test_encode_capacity_is_not_overrun)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	uint8 bytes[878];
	size_t written;
	size_t i;

	fixture(&set, routes);
	for (i = 0; i < sizeof(bytes); i++) {
		memset(bytes, 0xa5, sizeof(bytes));
		written = 17;
		UT_ASSERT(!pgrac_protected_set_v2_encode(&set, bytes, i, &written));
		UT_ASSERT_EQ(written, 0);
		UT_ASSERT_EQ(bytes[i], 0xa5);
	}
}

UT_TEST(test_max_routes_and_exact_preimage_limit)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[128];
	PgracProtectedSetDecodedV2 decoded;
	char text[1024];
	uint8 bytes[65536];
	uint8 digest[32];
	size_t written;
	unsigned int i;
	bool encoded;

	fixture(&set, routes);
	for (i = 4; i < 128; i++) {
		routes[i] = routes[0];
		routes[i].tpg = i + 1;
	}
	set.route_count = 128;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&set, digest));
	/* 174 + 128*176 + 42*(1024-6) + (84-6) = 65536. */
	memset(text, 'x', sizeof(text));
	for (i = 0; i < 43; i++) {
		routes[i].credential_ref.data = text;
		routes[i].credential_ref.length = i == 42 ? 84 : 1024;
	}
	encoded = pgrac_protected_set_v2_encode(&set, bytes, sizeof(bytes), &written);
	UT_ASSERT(encoded);
	if (encoded) {
		UT_ASSERT_EQ(written, 65536);
		UT_ASSERT(pgrac_protected_set_v2_decode(bytes, written, &decoded));
		UT_ASSERT(pgrac_external_fence_protected_set_digest_v2(&decoded.set, digest));
	}
	/* The aggregate-size rejection consumes this route through set.routes. */
	// cppcheck-suppress unreadVariable
	routes[42].credential_ref.length++;
	UT_ASSERT(rejected_and_cleared(&set));
	UT_ASSERT(!pgrac_protected_set_v2_encode(&set, bytes, sizeof(bytes), &written));
	UT_ASSERT_EQ(written, 0);
}

/* Independent Ed25519 signature of the 32-byte header and the golden preimage. */
static const uint8 map_public_key[32]
	= { 0xc0, 0x50, 0xc5, 0x63, 0x7a, 0x44, 0xfa, 0x86, 0x29, 0xff, 0xf3,
		0xcc, 0xcc, 0xe2, 0x30, 0x0c, 0xb3, 0x62, 0xa6, 0x3d, 0x99, 0xd9,
		0x5f, 0xc5, 0x41, 0x45, 0x26, 0x6f, 0x43, 0x32, 0x44, 0x5a };
static const uint8 map_signature[64]
	= { 0x7f, 0x66, 0xb1, 0x1f, 0xfe, 0x28, 0xee, 0xaf, 0x51, 0x45, 0x6e, 0x8c, 0x85,
		0xbf, 0x83, 0xa7, 0xe1, 0x42, 0x20, 0x88, 0xf5, 0x0d, 0x17, 0x00, 0x33, 0xc3,
		0x22, 0x83, 0x34, 0x61, 0x26, 0xd2, 0xa2, 0xfc, 0x39, 0xb3, 0x30, 0x6f, 0xfb,
		0x69, 0x45, 0xa4, 0x1f, 0x84, 0x05, 0xf4, 0x43, 0x34, 0x14, 0xb1, 0xa5, 0xe8,
		0xce, 0xa2, 0xf4, 0x37, 0x5d, 0x3c, 0x95, 0x01, 0x39, 0x82, 0xb5, 0x0d };

static bool
signed_fixture(uint8 packet[974], PgracFenceMapExpectedV2 *expected)
{
	PgracProtectedSetV2 set;
	PgracProtectedRouteV2 routes[4];
	size_t written;
	static const uint8 header[32]
		= { 'P',  'G',	'R',  'F', 'M', 'A', 'P', '2', 2,	 0, 32, 0, 2, 0, 0, 0,
			0x15, 0xcd, 0x5b, 7,   0,	0,	 0,	  0,   0x6e, 3, 0,	0, 0, 0, 0, 0 };

	fixture(&set, routes);
	memcpy(packet, header, sizeof(header));
	if (!pgrac_protected_set_v2_encode(&set, packet + 32, 878, &written) || written != 878)
		return false;
	memcpy(packet + 910, map_signature, sizeof(map_signature));
	memset(expected, 0, sizeof(*expected));
	expected->system_identifier = 123456789;
	expected->victim_node_id = 2;
	expected->mapping_generation = 7;
	memcpy(expected->protected_set_digest, golden, sizeof(golden));
	return true;
}

UT_TEST(test_signed_map_authenticates_before_use)
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded;
	uint8 packet[974];
	uint8 changed_key[32];
	PgracFenceMapResult result;

	UT_ASSERT(signed_fixture(packet, &expected));
	result = pgrac_fence_map_v2_verify(packet, sizeof(packet), map_public_key, &expected, &decoded);
#ifdef USE_OPENSSL
	UT_ASSERT_EQ(result, PGRAC_FENCE_MAP_OK);
	UT_ASSERT_EQ(decoded.set.route_count, 4);
#else
	UT_ASSERT_EQ(result, PGRAC_FENCE_MAP_UNSUPPORTED);
	UT_ASSERT(decoded.set.routes == NULL);
#endif
	memcpy(changed_key, map_public_key, sizeof(changed_key));
	changed_key[0] ^= 1;
	result = pgrac_fence_map_v2_verify(packet, sizeof(packet), changed_key, &expected, &decoded);
	UT_ASSERT_NE(result, PGRAC_FENCE_MAP_OK);
	UT_ASSERT(decoded.set.routes == NULL);
	packet[910] ^= 1;
	result = pgrac_fence_map_v2_verify(packet, sizeof(packet), map_public_key, &expected, &decoded);
	UT_ASSERT_NE(result, PGRAC_FENCE_MAP_OK);
	UT_ASSERT(decoded.set.routes == NULL);
}

UT_TEST(test_signed_map_rejects_replay_and_identity_changes)
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded;
	uint8 packet[974];
	unsigned int i;

	for (i = 0; i < 4; i++) {
		PgracFenceMapResult result;

		UT_ASSERT(signed_fixture(packet, &expected));
		switch (i) {
		case 0:
			expected.system_identifier++;
			break;
		case 1:
			expected.victim_node_id++;
			break;
		case 2:
			expected.mapping_generation++;
			break;
		case 3:
			expected.protected_set_digest[0] ^= 1;
			break;
		}
		result = pgrac_fence_map_v2_verify(packet, sizeof(packet), map_public_key, &expected,
										   &decoded);
#ifdef USE_OPENSSL
		UT_ASSERT_EQ(result, PGRAC_FENCE_MAP_WRONG_BINDING);
#else
		UT_ASSERT_EQ(result, PGRAC_FENCE_MAP_UNSUPPORTED);
#endif
		UT_ASSERT(decoded.set.routes == NULL && decoded.set.mapping_generation == 0);
	}
}

UT_TEST(test_signed_map_rejects_tampering_and_all_truncations)
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded;
	uint8 packet[975];
	unsigned int i;

	UT_ASSERT(signed_fixture(packet, &expected));
	for (i = 0; i < 974; i++) {
		UT_ASSERT_NE(pgrac_fence_map_v2_verify(packet, i, map_public_key, &expected, &decoded),
					 PGRAC_FENCE_MAP_OK);
		UT_ASSERT(decoded.set.routes == NULL);
	}
	packet[974] = 0;
	UT_ASSERT_EQ(pgrac_fence_map_v2_verify(packet, 975, map_public_key, &expected, &decoded),
				 PGRAC_FENCE_MAP_BAD_FORMAT);
	for (i = 0; i < 974; i++) {
		packet[i] ^= 1;
		UT_ASSERT_NE(pgrac_fence_map_v2_verify(packet, 974, map_public_key, &expected, &decoded),
					 PGRAC_FENCE_MAP_OK);
		UT_ASSERT(decoded.set.routes == NULL);
		packet[i] ^= 1;
	}
}

UT_TEST(test_identity_public_key_cannot_forge_a_map)
{
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded, zero = { 0 };
	uint8 packet[974], key[32] = { 1 };

	UT_ASSERT(signed_fixture(packet, &expected));
	memset(packet + 910, 0, 64);
	packet[910] = 1; /* R = identity, S = 0; no private key exists here. */
	memset(&decoded, 0xa5, sizeof(decoded));
	UT_ASSERT_NE(pgrac_fence_map_v2_verify(packet, sizeof(packet), key, &expected, &decoded),
				 PGRAC_FENCE_MAP_OK);
	UT_ASSERT(memcmp(&decoded, &zero, sizeof(decoded)) == 0);
}

UT_TEST(test_small_order_and_noncanonical_key_matrix)
{
	/* Independent compressed encodings, including both x signs. */
	const char *small_order[]
		= { "0000000000000000000000000000000000000000000000000000000000000000",
			"0100000000000000000000000000000000000000000000000000000000000000",
			"ecffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff7f",
			"26e8958fc2b227b045c3f489f2ef98f0d5dfac05d3c63339b13802886d53fc05",
			"c7176a703d4dd84fba3c0b760d10670f2a2053fa2c39ccc64ec7fd7792ac037a" };
	PgracFenceMapExpectedV2 expected;
	PgracProtectedSetDecodedV2 decoded, zero = { 0 };
	uint8 packet[974], key[32];

	UT_ASSERT(!pgrac_fence_ed25519_key_acceptable(NULL));
	UT_ASSERT(pgrac_fence_ed25519_key_acceptable(map_public_key));
	UT_ASSERT(signed_fixture(packet, &expected));
	memset(packet + 910, 0, 64);
	packet[910] = 1;
	for (size_t n = 0; n < lengthof(small_order) + 19; ++n) {
		if (n < lengthof(small_order)) {
			for (size_t i = 0; i < 32; ++i) {
				const char hex[3] = { small_order[n][2 * i], small_order[n][2 * i + 1], 0 };
				key[i] = strtoul(hex, NULL, 16);
			}
		} else {
			memset(key, 0xff, sizeof(key));
			key[0] = 0xed + n - lengthof(small_order); /* p .. 2^255-1 */
			key[31] = 0x7f;
		}
		for (int sign = 0; sign < 2; ++sign) {
			key[31] = (key[31] & 0x7f) | (sign << 7);
			UT_ASSERT(!pgrac_fence_ed25519_key_acceptable(key));
			memset(&decoded, 0xa5, sizeof(decoded));
			UT_ASSERT_NE(
				pgrac_fence_map_v2_verify(packet, sizeof(packet), key, &expected, &decoded),
				PGRAC_FENCE_MAP_OK);
			UT_ASSERT(memcmp(&decoded, &zero, sizeof(decoded)) == 0);
		}
	}
	/* p-2 is not filtered; EVP is still responsible for full verification. */
	key[0] = 0xeb;
	UT_ASSERT(pgrac_fence_ed25519_key_acceptable(key));
}

int
main(void)
{
	UT_PLAN(16);
	UT_RUN(test_golden_and_order_invariance);
	UT_RUN(test_same_storage_different_wal_voting_and_alternate_route);
	UT_RUN(test_every_top_level_identity_is_bound);
	UT_RUN(test_every_route_identity_is_bound);
	UT_RUN(test_missing_roles_and_duplicates_are_rejected);
	UT_RUN(test_invalid_values_clear_output);
	UT_RUN(test_explicit_lengths_and_total_size_bound);
	UT_RUN(test_encode_decode_golden_and_truncation);
	UT_RUN(test_decode_rejects_noncanonical_order_and_fields);
	UT_RUN(test_encode_capacity_is_not_overrun);
	UT_RUN(test_max_routes_and_exact_preimage_limit);
	UT_RUN(test_signed_map_authenticates_before_use);
	UT_RUN(test_signed_map_rejects_replay_and_identity_changes);
	UT_RUN(test_signed_map_rejects_tampering_and_all_truncations);
	UT_RUN(test_identity_public_key_cannot_forge_a_map);
	UT_RUN(test_small_order_and_noncanonical_key_matrix);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
