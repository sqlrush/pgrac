/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_pending.c
 *    Bounded DROP/TRUNCATE relmap structural pending-set contract.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_pending.c
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres_fe.h"

#include "cluster/cluster_relmap_pending.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static ClusterSpaceIdentityKey
pending_key(uint64 database_incarnation, uint8 uuid_tag, RelFileNumber rel_number)
{
	ClusterSpaceIdentityKey key;

	memset(&key, 0, sizeof(key));
	key.system_identifier = UINT64CONST(0x0102030405060708);
	key.database_incarnation = database_incarnation;
	memset(key.storage_uuid, uuid_tag, sizeof(key.storage_uuid));
	key.locator.spcOid = 1663; /* DEFAULTTABLESPACE_OID */
	key.locator.dbOid = 42;
	key.locator.relNumber = rel_number;
	return key;
}

UT_TEST(test_pending_add_requires_main_placeholder_and_reserves_locator)
{
	ClusterRelmapPendingSet set;
	ClusterSpaceIdentityKey key = pending_key(9, 0xa1, 10001);

	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
											&key, false),
								 CLUSTER_RELMAP_PENDING_INVALID_INPUT);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
											&key, true),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&set, CLUSTER_RELMAP_PENDING_DROP, &key));
	UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&set, &key));
	UT_ASSERT(!cluster_relmap_pending_can_release(&set, CLUSTER_RELMAP_PENDING_DROP, &key));
}

UT_TEST(test_pending_release_requires_all_terminal_proofs)
{
	ClusterRelmapPendingSet set;
	ClusterSpaceIdentityKey key = pending_key(9, 0xa2, 10002);

	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_TRUNCATE,
											&key, true),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_release(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT(cluster_relmap_pending_contains(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key));

	UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
												 CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_mark_native_terminal(&set,
												 CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_mark_tt_complete(&set,
												 CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(!cluster_relmap_pending_can_release(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key));
	UT_ASSERT_EQ(cluster_relmap_pending_mark_space_complete(&set,
												 CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_can_release(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key));
	UT_ASSERT_EQ(cluster_relmap_pending_release(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(!cluster_relmap_pending_contains(&set, CLUSTER_RELMAP_PENDING_TRUNCATE, &key));
	UT_ASSERT(!cluster_relmap_pending_rel_number_reserved(&set, &key));
}

UT_TEST(test_pending_proof_marks_are_idempotent_but_exact)
{
	ClusterRelmapPendingSet set;
	ClusterSpaceIdentityKey key = pending_key(9, 0xa3, 10003);
	ClusterSpaceIdentityKey wrong = key;

	wrong.storage_uuid[0] ^= 1;
	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &key, true),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
												 CLUSTER_RELMAP_PENDING_DROP, &wrong),
							 CLUSTER_RELMAP_PENDING_NOT_FOUND);
	UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
												 CLUSTER_RELMAP_PENDING_DROP, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
												 CLUSTER_RELMAP_PENDING_DROP, &key),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(!cluster_relmap_pending_can_release(&set, CLUSTER_RELMAP_PENDING_DROP, &key));
}

UT_TEST(test_pending_locator_collision_fails_closed)
{
	ClusterRelmapPendingSet set;
	ClusterSpaceIdentityKey first = pending_key(9, 0xb1, 10004);
	ClusterSpaceIdentityKey other_uuid = pending_key(9, 0xb2, 10004);
	ClusterSpaceIdentityKey other_kind = first;
	ClusterSpaceIdentityKey other_namespace = pending_key(10, 0xb3, 10005);

	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &first, true),
							 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
											&first, true),
							 CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_TRUNCATE,
											&other_kind, true),
							 CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
											&other_uuid, true),
							 CLUSTER_RELMAP_PENDING_CONFLICT);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP,
											&other_namespace, true),
							 CLUSTER_RELMAP_PENDING_CONFLICT);
}

UT_TEST(test_pending_identity_and_kind_validation)
{
	ClusterRelmapPendingSet set;
	ClusterSpaceIdentityKey key = pending_key(9, 0xc1, 10006);
	ClusterSpaceIdentityKey invalid = key;

	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_INVALID, &key, true),
								 CLUSTER_RELMAP_PENDING_INVALID_INPUT);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, NULL, true),
								 CLUSTER_RELMAP_PENDING_INVALID_INPUT);
	invalid.storage_uuid[0] = 0;
	memset(invalid.storage_uuid + 1, 0, sizeof(invalid.storage_uuid) - 1);
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &invalid, true),
								 CLUSTER_RELMAP_PENDING_INVALID_INPUT);
	invalid = key;
	invalid.locator.relNumber = InvalidRelFileNumber;
	UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &invalid, true),
								 CLUSTER_RELMAP_PENDING_INVALID_INPUT);
}

UT_TEST(test_pending_set_is_bounded)
{
	ClusterRelmapPendingSet set;

	cluster_relmap_pending_init(&set);
	for (int i = 0; i < CLUSTER_RELMAP_PENDING_CAPACITY; i++)
	{
		ClusterSpaceIdentityKey key = pending_key(9, (uint8) (i + 1),
											(RelFileNumber) (11000 + i));

		UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &key, true),
							 CLUSTER_RELMAP_PENDING_OK);
	}
	{
		ClusterSpaceIdentityKey key = pending_key(9, 0xee, 12000);

		UT_ASSERT_EQ(cluster_relmap_pending_add(&set, CLUSTER_RELMAP_PENDING_DROP, &key, true),
							 CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_pending_add_requires_main_placeholder_and_reserves_locator);
	UT_RUN(test_pending_release_requires_all_terminal_proofs);
	UT_RUN(test_pending_proof_marks_are_idempotent_but_exact);
	UT_RUN(test_pending_locator_collision_fails_closed);
	UT_RUN(test_pending_identity_and_kind_validation);
	UT_RUN(test_pending_set_is_bounded);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
