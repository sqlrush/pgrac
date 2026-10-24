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

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_pending.h"
#include "common/cluster_relmap_v2.h"
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
	key.locator.spcOid = DEFAULTTABLESPACE_OID;
	key.locator.dbOid = 42;
	key.locator.relNumber = rel_number;
	return key;
}

static pg_crc32c
pending_crc(const void *bytes, size_t length)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return crc;
}

static RelMapFile
pending_native_map(Oid mapoid, RelFileNumber file)
{
	RelMapFile map;

	memset(&map, 0, sizeof(map));
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = mapoid;
	map.mappings[0].mapfilenumber = file;
	map.crc = pending_crc(&map, offsetof(RelMapFile, crc));
	return map;
}

static const ClusterRelmapV2Namespace pending_v2_identity = {
	UINT64CONST(0x0123456789abcdef), UINT64CONST(0x0fedcba987654321), 42, false
};

static void
pending_v2_pair(const ClusterRelmapV2Namespace *identity,
				unsigned char *primary, unsigned char *backup)
{
	ClusterRelmapV2Image image;

	memset(&image, 0, sizeof(image));
	image.identity = *identity;
	image.committed_generation = 7;
	image.pending_generation = 8;
	image.pending_owner.node = 3;
	image.pending_owner.top_full_xid = UINT64CONST(0x0000000212345678);
	image.pending_owner.membership_epoch = 91;
	image.pending_owner.admitted_incarnation = 101;
	image.pending_owner.wal_end = UINT64CONST(0x123456789);
	image.pending_owner.boot_incarnation = 202;
	image.committed = pending_native_map(1259, 40000);
	image.pending = pending_native_map(1259, 40001);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, primary,
										 CLUSTER_RELMAP_V2_FILE_SIZE),
								 CLUSTER_RELMAP_V2_VALID);
	memcpy(backup, primary, CLUSTER_RELMAP_V2_FILE_SIZE);
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

UT_TEST(test_pending_rebuild_requires_committed_pair)
{
	unsigned char primary[CLUSTER_RELMAP_V2_FILE_SIZE];
	unsigned char backup[CLUSTER_RELMAP_V2_FILE_SIZE];
	ClusterRelmapPendingSet set;
	ClusterRelmapPendingDescriptor descriptor;
	ClusterRelmapV2View view;
	ClusterSpaceIdentityKey key = pending_key(pending_v2_identity.database_incarnation,
												 0xd1, 12001);
	ClusterRelmapV2PairResult pair_result;

	key.system_identifier = pending_v2_identity.system_identifier;
	descriptor.kind = CLUSTER_RELMAP_PENDING_TRUNCATE;
	descriptor.identity = key;

	pending_v2_pair(&pending_v2_identity, primary, backup);
	pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary), backup,
												 sizeof(backup), &pending_v2_identity, &view);
	UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);

	cluster_relmap_pending_init(&set);
	UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(
										&set, &pending_v2_identity, pair_result,
										&view, &descriptor, 1),
								 CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT(cluster_relmap_pending_contains(&set,
									CLUSTER_RELMAP_PENDING_TRUNCATE, &key));
	UT_ASSERT(!cluster_relmap_pending_can_release(&set,
									CLUSTER_RELMAP_PENDING_TRUNCATE, &key));

	/* A degraded/fallback reader result cannot rebuild a release directory. */
	cluster_relmap_pending_init(&set);
	pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary), backup,
												 CLUSTER_RELMAP_V2_FILE_SIZE - 1,
												 &pending_v2_identity, &view);
	UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(
										&set, &pending_v2_identity, pair_result,
										&view, &descriptor, 1),
								 CLUSTER_RELMAP_PENDING_NOT_READY);
	UT_ASSERT(!cluster_relmap_pending_contains(&set,
									CLUSTER_RELMAP_PENDING_TRUNCATE, &key));

	/* Structural records from a different namespace cannot be adopted. */
	{
		ClusterRelmapV2Namespace wrong_namespace = pending_v2_identity;

		pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary), backup,
												 CLUSTER_RELMAP_V2_FILE_SIZE,
												 &pending_v2_identity, &view);
		UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_VALID);
		wrong_namespace.database_incarnation++;
		cluster_relmap_pending_init(&set);
		UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(
												&set, &wrong_namespace, pair_result,
												&view, &descriptor, 1),
									 CLUSTER_RELMAP_PENDING_CONFLICT);
		UT_ASSERT(!cluster_relmap_pending_contains(&set,
										 CLUSTER_RELMAP_PENDING_TRUNCATE, &key));
	}
}

UT_TEST(test_pending_rebuild_accepts_global_and_local_tablespaces)
{
	const Oid tablespaces[] = {
		GLOBALTABLESPACE_OID, DEFAULTTABLESPACE_OID, 9000
	};

	for (size_t i = 0; i < lengthof(tablespaces); i++)
	{
		ClusterRelmapV2Namespace ns = pending_v2_identity;
		unsigned char primary[CLUSTER_RELMAP_V2_FILE_SIZE];
		unsigned char backup[CLUSTER_RELMAP_V2_FILE_SIZE];
		ClusterRelmapV2View view;
		ClusterRelmapV2PairResult pair_result;

		ns.shared = tablespaces[i] == GLOBALTABLESPACE_OID;
		ns.dbid = ns.shared ? InvalidOid : 42;
		pending_v2_pair(&ns, primary, backup);
		pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary),
			backup, sizeof(backup), &ns, &view);
		UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_VALID);
		for (int kind = CLUSTER_RELMAP_PENDING_DROP;
			 kind <= CLUSTER_RELMAP_PENDING_TRUNCATE; kind++)
		{
			ClusterRelmapPendingSet set;
			ClusterRelmapPendingDescriptor descriptor;

			descriptor.kind = (ClusterRelmapPendingKind) kind;
			descriptor.identity = pending_key(ns.database_incarnation, 0xe1, 12002);
			descriptor.identity.system_identifier = ns.system_identifier;
			descriptor.identity.locator.spcOid = tablespaces[i];
			descriptor.identity.locator.dbOid = ns.dbid;
			cluster_relmap_pending_init(&set);
			UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(
				&set, &ns, pair_result, &view, &descriptor, 1),
				CLUSTER_RELMAP_PENDING_OK);
			UT_ASSERT(cluster_relmap_pending_contains(&set, descriptor.kind,
				&descriptor.identity));
			UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&set,
				&descriptor.identity));
			UT_ASSERT(!cluster_relmap_pending_can_release(&set, descriptor.kind,
				&descriptor.identity));
		}
	}
}

UT_TEST(test_pending_rebuild_rejects_foreign_physical_namespace_atomically)
{
	const struct {
		bool shared;
		Oid tablespace;
		Oid database;
	} cases[] = {
		{ true, DEFAULTTABLESPACE_OID, 42 },
		{ true, GLOBALTABLESPACE_OID, 42 },
		{ true, DEFAULTTABLESPACE_OID, InvalidOid },
		{ true, 9000, InvalidOid },
		{ false, DEFAULTTABLESPACE_OID, 43 },
		{ false, DEFAULTTABLESPACE_OID, InvalidOid },
		{ false, GLOBALTABLESPACE_OID, 42 },
		{ false, GLOBALTABLESPACE_OID, InvalidOid },
		{ false, 9000, 43 }
	};

	for (size_t i = 0; i < lengthof(cases); i++)
	{
		ClusterRelmapV2Namespace ns = pending_v2_identity;
		unsigned char primary[CLUSTER_RELMAP_V2_FILE_SIZE];
		unsigned char backup[CLUSTER_RELMAP_V2_FILE_SIZE];
		ClusterRelmapV2View view;
		ClusterRelmapV2PairResult pair_result;

		ns.shared = cases[i].shared;
		ns.dbid = ns.shared ? InvalidOid : 42;
		pending_v2_pair(&ns, primary, backup);
		pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary),
			backup, sizeof(backup), &ns, &view);
		UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_VALID);
		for (int kind = CLUSTER_RELMAP_PENDING_DROP;
			 kind <= CLUSTER_RELMAP_PENDING_TRUNCATE; kind++)
		{
			ClusterRelmapPendingSet set;
			ClusterRelmapPendingSet before;
			ClusterRelmapPendingDescriptor descriptors[2];

			descriptors[0].kind = (ClusterRelmapPendingKind) kind;
			descriptors[0].identity = pending_key(ns.database_incarnation, 0xe2, 12003);
			descriptors[0].identity.system_identifier = ns.system_identifier;
			descriptors[0].identity.locator.spcOid = ns.shared
				? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
			descriptors[0].identity.locator.dbOid = ns.dbid;
			descriptors[1] = descriptors[0];
			descriptors[1].identity.locator.relNumber++;
			descriptors[1].identity.locator.spcOid = cases[i].tablespace;
			descriptors[1].identity.locator.dbOid = cases[i].database;
			cluster_relmap_pending_init(&set);
			memcpy(&before, &set, sizeof(before));
			UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(
				&set, &ns, pair_result, &view, descriptors, lengthof(descriptors)),
				CLUSTER_RELMAP_PENDING_CONFLICT);
			UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
		}
	}
}

UT_TEST(test_pending_rebuild_never_replaces_existing_obligations)
{
	for (unsigned shared = 0; shared < 2; shared++)
	{
		ClusterRelmapV2Namespace ns = pending_v2_identity;
		unsigned char primary[CLUSTER_RELMAP_V2_FILE_SIZE];
		unsigned char backup[CLUSTER_RELMAP_V2_FILE_SIZE];
		ClusterRelmapV2View view;
		ClusterRelmapV2PairResult pair_result;

		ns.shared = shared != 0;
		ns.dbid = ns.shared ? InvalidOid : 42;
		pending_v2_pair(&ns, primary, backup);
		pair_result = cluster_relmap_v2_read_pair(primary, sizeof(primary),
			backup, sizeof(backup), &ns, &view);
		UT_ASSERT_EQ(pair_result, CLUSTER_RELMAP_V2_PAIR_VALID);
		for (unsigned proofs = 0; proofs < 16; proofs++)
		{
			for (unsigned count = 0; count <= 2; count += 2)
			{
				ClusterRelmapPendingSet set, before;
				ClusterRelmapPendingDescriptor existing[2], incoming[2];

				cluster_relmap_pending_init(&set);
				for (unsigned i = 0; i < lengthof(existing); i++)
				{
					ClusterSpaceIdentityKey *key = &existing[i].identity;

					existing[i].kind = i == 0 ? CLUSTER_RELMAP_PENDING_DROP
						: CLUSTER_RELMAP_PENDING_TRUNCATE;
					*key = pending_key(ns.database_incarnation, 0xe3 + i, 12010 + i);
					key->system_identifier = ns.system_identifier;
					key->locator.spcOid = ns.shared
						? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
					key->locator.dbOid = ns.dbid;
					UT_ASSERT_EQ(cluster_relmap_pending_add(&set, existing[i].kind,
						key, true), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 1)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_copies_cleared(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 2)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_native_terminal(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 4)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_tt_complete(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					if (proofs & 8)
						UT_ASSERT_EQ(cluster_relmap_pending_mark_space_complete(&set,
							existing[i].kind, key), CLUSTER_RELMAP_PENDING_OK);
					incoming[i] = existing[i];
					incoming[i].identity.locator.relNumber += 100;
				}
				memcpy(&before, &set, sizeof(before));
				/* An empty stream does not authorize clearing the destination.
				 * Even fully marked entries still belong to the release owner. */
				UT_ASSERT_EQ(cluster_relmap_pending_rebuild_from_committed(&set,
					&ns, pair_result, &view, count == 0 ? NULL : incoming, count),
					CLUSTER_RELMAP_PENDING_CONFLICT);
				UT_ASSERT_EQ(memcmp(&set, &before, sizeof(set)), 0);
				for (unsigned i = 0; i < lengthof(existing); i++)
				{
					ClusterSpaceIdentityKey alias = existing[i].identity;

					alias.storage_uuid[0]++;
					UT_ASSERT(cluster_relmap_pending_contains(&set, existing[i].kind,
						&existing[i].identity));
					UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&set, &alias));
					UT_ASSERT_EQ(cluster_relmap_pending_can_release(&set, existing[i].kind,
						&existing[i].identity), proofs == 15);
					UT_ASSERT(!cluster_relmap_pending_contains(&set, incoming[i].kind,
						&incoming[i].identity));
				}
			}
		}
	}
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_pending_add_requires_main_placeholder_and_reserves_locator);
	UT_RUN(test_pending_release_requires_all_terminal_proofs);
	UT_RUN(test_pending_proof_marks_are_idempotent_but_exact);
	UT_RUN(test_pending_locator_collision_fails_closed);
	UT_RUN(test_pending_identity_and_kind_validation);
	UT_RUN(test_pending_set_is_bounded);
	UT_RUN(test_pending_rebuild_requires_committed_pair);
	UT_RUN(test_pending_rebuild_accepts_global_and_local_tablespaces);
	UT_RUN(test_pending_rebuild_rejects_foreign_physical_namespace_atomically);
	UT_RUN(test_pending_rebuild_never_replaces_existing_obligations);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
