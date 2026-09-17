/*-------------------------------------------------------------------------
 *
 * test_cluster_rf_route.c
 *    STOP-06 exhaustive generated opcode-route manifest.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#if defined(__has_include)
#if __has_include("cluster/cluster_rf_route.h")
#include "cluster/cluster_rf_route.h"
#include "access/xact.h"
#include "access/xlogrecord.h"
#define TEST_HAVE_CLUSTER_RF_ROUTE 1
#endif
#endif

#include "common/cryptohash.h"
#include "common/sha2.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

#ifndef TEST_HAVE_CLUSTER_RF_ROUTE

UT_TEST(test_route_interface_capability_red)
{
	printf("# JIT_SEMANTIC_RED:T4-GENERATED-ROUTE-INTERFACE\n");
	UT_ASSERT(false);
}

int
main(void)
{
	UT_PLAN(1);
	UT_RUN(test_route_interface_capability_red);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#else

static void
sha256_bytes(const uint8 *bytes, size_t len, uint8 digest[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	UT_ASSERT(ctx != NULL);
	UT_ASSERT_EQ(pg_cryptohash_init(ctx), 0);
	UT_ASSERT_EQ(pg_cryptohash_update(ctx, bytes, len), 0);
	UT_ASSERT_EQ(pg_cryptohash_final(ctx, digest, PG_SHA256_DIGEST_LENGTH), 0);
	pg_cryptohash_free(ctx);
}

static bool
find_manifest_route(uint8 rmid, uint8 normalized_info, RfOpcodeRouteV1 *route, bool *active)
{
	size_t i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 candidate;
		bool candidate_active;

		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &candidate, &candidate_active));
		if (candidate.rmid == rmid && candidate.normalized_info == normalized_info) {
			*route = candidate;
			*active = candidate_active;
			return true;
		}
	}
	return false;
}

UT_TEST(test_route_abi_and_counts)
{
	UT_ASSERT_EQ(sizeof(RfOpcodeRouteV1), 8);
	UT_ASSERT_EQ(rf_opcode_route_manifest_count_v1(), 138);
	UT_ASSERT_EQ(rf_opcode_route_manifest_live_count_v1(), 138);
}

UT_TEST(test_route_canonical_key_stream)
{
	static const uint8 expected[PG_SHA256_DIGEST_LENGTH]
		= { 0x65, 0xb6, 0xc7, 0x28, 0xec, 0xb5, 0xe4, 0xb4, 0x84, 0x07, 0xbb,
			0xb4, 0x96, 0x5b, 0x3e, 0xf4, 0x68, 0x79, 0x25, 0x55, 0xc6, 0x07,
			0xed, 0x5f, 0xcc, 0x32, 0x06, 0xae, 0xec, 0x40, 0xe8, 0xbe };
	char stream[1024];
	uint8 digest[PG_SHA256_DIGEST_LENGTH];
	RfOpcodeRouteV1 route;
	bool active;
	size_t used = 0;
	size_t i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &route, &active));
		if (i > 0) {
			RfOpcodeRouteV1 previous;
			bool previous_active;

			UT_ASSERT(rf_opcode_route_manifest_entry_v1(i - 1, &previous, &previous_active));
			UT_ASSERT(previous.rmid < route.rmid
					  || (previous.rmid == route.rmid
						  && previous.normalized_info < route.normalized_info));
		}
		used += pg_snprintf(stream + used, sizeof(stream) - used, "%03u:%02X\n", route.rmid,
							route.normalized_info);
	}
	UT_ASSERT_EQ(used, 966);
	sha256_bytes((const uint8 *)stream, used, digest);
	if (memcmp(digest, expected, sizeof(expected)) != 0) {
		size_t j;

		printf("# route stream SHA-256: ");
		for (j = 0; j < sizeof(digest); j++)
			printf("%02x", digest[j]);
		printf("\n");
	}
	UT_ASSERT(memcmp(digest, expected, sizeof(expected)) == 0);
}

UT_TEST(test_route_page_and_side_block_policies)
{
	RfOpcodeRouteV1 route;

	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x00, true, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_PAGE_CODEC);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_HEAP);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x00, false, false, &route),
				 RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_SMGR_ID, 0x10, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.record_owner, RF_ROUTE_OWNER_SIDE_TYPED);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_SMGR_ID, 0x10, true, false, &route),
				 RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
}

UT_TEST(test_route_xact_flags_are_exact)
{
	RfOpcodeRouteV1 route;

	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_XACT_ID, 0x80, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.normalized_info, 0x00);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_XACT_ID, 0x90, false, false, &route),
				 RF_OPCODE_ROUTE_FLAG_ILLEGAL);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_XACT_ID, 0xB0, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
}

UT_TEST(test_route_ctrc_release_is_active_typed_side_record)
{
	RfOpcodeRouteV1 route;

	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_UNDO_ID, 0xA0, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_UNDO_ID, 0xA0, true, false, &route),
				 RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO);
}

UT_TEST(test_route_cluster_owners_are_not_family_defaults)
{
	RfOpcodeRouteV1 route;

	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_ADG_ID, 0x10, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_ADG_BARRIER);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_CLUSTER_XID_STRIPE_ID, 0x00, false, false, &route),
				 RF_OPCODE_ROUTE_OK);
	UT_ASSERT_EQ(route.codec_id, RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM);
}

UT_TEST(test_every_manifest_row_has_one_total_route)
{
	size_t page_count = 0;
	size_t side_count = 0;
	size_t logical_count = 0;
	size_t i;

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 expected;
		RfOpcodeRouteV1 actual;
		bool active;
		bool has_blocks;

		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &expected, &active));
		UT_ASSERT_EQ(expected.reserved_zero, 0);
		has_blocks = expected.block_policy != RF_ROUTE_BLOCKS_FORBIDDEN;
		UT_ASSERT_EQ(rf_opcode_route_lookup_v1(expected.rmid, expected.normalized_info, has_blocks,
											   !active, &actual),
					 RF_OPCODE_ROUTE_OK);
		UT_ASSERT(memcmp(&actual, &expected, sizeof(actual)) == 0);

		switch (expected.record_owner) {
		case RF_ROUTE_OWNER_PAGE_CODEC:
			page_count++;
			UT_ASSERT_EQ(expected.block_policy, RF_ROUTE_BLOCKS_REQUIRED);
			UT_ASSERT(expected.codec_id >= RF_ROUTE_CODEC_XLOG_FPI);
			UT_ASSERT(expected.codec_id <= RF_ROUTE_CODEC_GENERIC);
			break;
		case RF_ROUTE_OWNER_SIDE_TYPED:
			side_count++;
			UT_ASSERT(expected.codec_id == RF_ROUTE_CODEC_SIDE_STANDARD
					  || expected.codec_id == RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO
					  || expected.codec_id == RF_ROUTE_CODEC_SIDE_RAW_LAYOUT
					  || expected.codec_id == RF_ROUTE_CODEC_SIDE_ADG_BARRIER
					  || expected.codec_id == RF_ROUTE_CODEC_SIDE_XID_STRIPE_SHMEM);
			UT_ASSERT_EQ(expected.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
			break;
		case RF_ROUTE_OWNER_LOGICAL_NOOP:
			logical_count++;
			UT_ASSERT_EQ(expected.codec_id, RF_ROUTE_CODEC_LOGICAL_NOOP);
			UT_ASSERT_EQ(expected.block_policy, RF_ROUTE_BLOCKS_FORBIDDEN);
			break;
		default:
			UT_ASSERT(false);
		}
	}

	UT_ASSERT_EQ(page_count, 77);
	UT_ASSERT_EQ(side_count, 60);
	UT_ASSERT_EQ(logical_count, 1);
}

UT_TEST(test_every_high_nibble_is_closed)
{
	uint16 rmid;

	for (rmid = 0; rmid < RM_N_BUILTIN_IDS; rmid++) {
		uint8 nibble;

		for (nibble = 0; nibble < 16; nibble++) {
			RfOpcodeRouteV1 expected;
			RfOpcodeRouteV1 actual;
			RfOpcodeRouteV1 before;
			RfOpcodeRouteLookupResultV1 result;
			uint8 raw_info = (uint8)((nibble << 4) | XLR_INFO_MASK);
			uint8 normalized_info;
			uint8 info_flags = 0;
			bool active;
			bool found;
			bool has_blocks = false;

			if (rmid == RM_XACT_ID) {
				normalized_info = raw_info & XLOG_XACT_OPMASK;
				info_flags = raw_info & XLOG_XACT_HAS_INFO;
			} else
				normalized_info = raw_info & ~XLR_INFO_MASK;

			found = find_manifest_route((uint8)rmid, normalized_info, &expected, &active);
			if (found)
				has_blocks = expected.block_policy != RF_ROUTE_BLOCKS_FORBIDDEN;

			memset(&actual, 0xa5, sizeof(actual));
			before = actual;
			result = rf_opcode_route_lookup_v1((uint8)rmid, raw_info, has_blocks, true, &actual);

			if (!found)
				UT_ASSERT_EQ(result, RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
			else if ((info_flags & ~expected.legal_info_flags) != 0)
				UT_ASSERT_EQ(result, RF_OPCODE_ROUTE_FLAG_ILLEGAL);
			else {
				UT_ASSERT_EQ(result, RF_OPCODE_ROUTE_OK);
				UT_ASSERT(memcmp(&actual, &expected, sizeof(actual)) == 0);
				continue;
			}
			UT_ASSERT(memcmp(&actual, &before, sizeof(actual)) == 0);
		}
	}
}

UT_TEST(test_all_custom_rmids_and_wrong_block_shapes_are_closed)
{
	RfOpcodeRouteV1 route;
	RfOpcodeRouteV1 before;
	uint16 rmid;
	size_t i;

	for (rmid = RM_MIN_CUSTOM_ID; rmid <= RM_MAX_CUSTOM_ID; rmid++) {
		memset(&route, 0xa5, sizeof(route));
		before = route;
		UT_ASSERT_EQ(rf_opcode_route_lookup_v1((uint8)rmid, 0, false, false, &route),
					 RF_OPCODE_ROUTE_RMID_UNSUPPORTED);
		UT_ASSERT(memcmp(&route, &before, sizeof(route)) == 0);
	}

	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 expected;
		bool active;
		bool wrong_blocks;

		UT_ASSERT(rf_opcode_route_manifest_entry_v1(i, &expected, &active));
		if (expected.block_policy == RF_ROUTE_BLOCKS_OPTIONAL_TYPED)
			continue;
		wrong_blocks = expected.block_policy == RF_ROUTE_BLOCKS_FORBIDDEN;
		memset(&route, 0xa5, sizeof(route));
		before = route;
		UT_ASSERT_EQ(rf_opcode_route_lookup_v1(expected.rmid, expected.normalized_info,
											   wrong_blocks, true, &route),
					 RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
		UT_ASSERT(memcmp(&route, &before, sizeof(route)) == 0);
	}
}

UT_TEST(test_route_failures_leave_output_untouched)
{
	RfOpcodeRouteV1 route;
	RfOpcodeRouteV1 before;

	memset(&route, 0xa5, sizeof(route));
	before = route;
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(128, 0, false, false, &route),
				 RF_OPCODE_ROUTE_RMID_UNSUPPORTED);
	UT_ASSERT(memcmp(&route, &before, sizeof(route)) == 0);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_HEAP_ID, 0x30, true, false, &route),
				 RF_OPCODE_ROUTE_BLOCK_SHAPE_INVALID);
	UT_ASSERT(memcmp(&route, &before, sizeof(route)) == 0);
	UT_ASSERT_EQ(rf_opcode_route_lookup_v1(RM_HEAP_ID, 0xF0, true, false, &route),
				 RF_OPCODE_ROUTE_OPCODE_UNSUPPORTED);
	UT_ASSERT(memcmp(&route, &before, sizeof(route)) == 0);
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_route_abi_and_counts);
	UT_RUN(test_route_canonical_key_stream);
	UT_RUN(test_route_page_and_side_block_policies);
	UT_RUN(test_route_xact_flags_are_exact);
	UT_RUN(test_route_ctrc_release_is_active_typed_side_record);
	UT_RUN(test_route_cluster_owners_are_not_family_defaults);
	UT_RUN(test_every_manifest_row_has_one_total_route);
	UT_RUN(test_every_high_nibble_is_closed);
	UT_RUN(test_all_custom_rmids_and_wrong_block_shapes_are_closed);
	UT_RUN(test_route_failures_leave_output_untouched);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}

#endif
