/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_root_descriptor.c
 *    Spec-8.4A A-prime PGRD V1 pure codec and file-id tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <string.h>

#include "cluster/cluster_undo_root_descriptor.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "port/pg_crc32c.h"

#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


static void
put_le32(uint8 *out, uint32 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
	out[2] = (uint8)(value >> 16);
	out[3] = (uint8)(value >> 24);
}


static void
set_image_crc(uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES])
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, image, CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET);
	FIN_CRC32C(crc);
	put_le32(image + CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET, (uint32)crc);
}


static ClusterUndoRootDescriptorV1
valid_shared_descriptor(void)
{
	ClusterUndoRootDescriptorV1 descriptor;
	int i;

	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.descriptor_incarnation = UINT64_C(1);
	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_SHARED;
	descriptor.owner_node = -1;
	descriptor.root_ordinal = 0;
	for (i = 0; i < CLUSTER_UNDO_ROOT_UUID_BYTES; i++)
		descriptor.root_uuid[i] = (uint8)(0x10 + i);
	descriptor.namespace_id = UINT64_C(1);
	descriptor.system_identifier = UINT64_C(0x1122334455667788);
	return descriptor;
}


UT_TEST(test_pgrd_v1_exact_layout_and_roundtrip)
{
	ClusterUndoRootDescriptorV1 descriptor = valid_shared_descriptor();
	ClusterUndoRootDescriptorV1 decoded;
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	static const uint8 expected_prefix[64]
		= { 0x44, 0x52, 0x47, 0x50, 0x01, 0x00, 0x40, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
			0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x01, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11 };
	int i;

	memset(image, 0xa5, sizeof(image));
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	UT_ASSERT_EQ(memcmp(image, expected_prefix, sizeof(expected_prefix)), 0);
	for (i = 64; i < CLUSTER_UNDO_ROOT_DESCRIPTOR_CRC_OFFSET; i++)
		UT_ASSERT_EQ(image[i], 0);
	UT_ASSERT_EQ(image[508], 0x2d);
	UT_ASSERT_EQ(image[509], 0x32);
	UT_ASSERT_EQ(image[510], 0xb3);
	UT_ASSERT_EQ(image[511], 0x88);

	memset(&decoded, 0, sizeof(decoded));
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_VALID);
	UT_ASSERT_EQ(decoded.descriptor_incarnation, descriptor.descriptor_incarnation);
	UT_ASSERT_EQ(decoded.root_kind, descriptor.root_kind);
	UT_ASSERT_EQ(decoded.owner_node, descriptor.owner_node);
	UT_ASSERT_EQ(decoded.root_ordinal, descriptor.root_ordinal);
	UT_ASSERT_EQ(memcmp(decoded.root_uuid, descriptor.root_uuid, CLUSTER_UNDO_ROOT_UUID_BYTES), 0);
	UT_ASSERT_EQ(decoded.namespace_id, descriptor.namespace_id);
	UT_ASSERT_EQ(decoded.system_identifier, descriptor.system_identifier);
}


UT_TEST(test_pgrd_v1_zero_and_corruption_fail_closed_without_output)
{
	ClusterUndoRootDescriptorV1 descriptor = valid_shared_descriptor();
	ClusterUndoRootDescriptorV1 sentinel;
	ClusterUndoRootDescriptorV1 decoded;
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];
	uint8 zero[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	memset(&sentinel, 0x5a, sizeof(sentinel));
	memset(zero, 0, sizeof(zero));
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(zero, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_UNPROVISIONED);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	image[32] ^= UINT8_C(1);
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	image[64] = UINT8_C(1);
	set_image_crc(image);
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);
}


UT_TEST(test_pgrd_v1_identity_relations_fail_closed)
{
	ClusterUndoRootDescriptorV1 descriptor = valid_shared_descriptor();
	ClusterUndoRootDescriptorV1 sentinel;
	ClusterUndoRootDescriptorV1 decoded;
	uint8 image[CLUSTER_UNDO_ROOT_DESCRIPTOR_BYTES];

	memset(&sentinel, 0x6b, sizeof(sentinel));
	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	memset(image + 32, 0, CLUSTER_UNDO_ROOT_UUID_BYTES);
	set_image_crc(image);
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	image[16] = CLUSTER_UNDO_ROOT_KIND_LOCAL;
	set_image_crc(image);
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	image[48] = UINT8_C(2);
	set_image_crc(image);
	decoded = sentinel;
	UT_ASSERT_EQ(cluster_undo_root_descriptor_decode(image, descriptor.system_identifier, &decoded),
				 CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);

	UT_ASSERT(cluster_undo_root_descriptor_encode(&descriptor, image));
	decoded = sentinel;
	UT_ASSERT_EQ(
		cluster_undo_root_descriptor_decode(image, descriptor.system_identifier + 1, &decoded),
		CLUSTER_UNDO_ROOT_DESCRIPTOR_HOLD);
	UT_ASSERT_EQ(memcmp(&decoded, &sentinel, sizeof(decoded)), 0);
}


UT_TEST(test_pgrd_file_ids_are_exact_and_disjoint)
{
	uint64 namespace_id;
	uint64 root_id;
	uint32 file_slot;

	UT_ASSERT(cluster_undo_root_namespace_id(1, 0, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(1));
	UT_ASSERT(cluster_undo_root_file_slot(1, 0, &file_slot));
	UT_ASSERT_EQ(file_slot, UINT32_C(0));
	UT_ASSERT(cluster_undo_root_id(namespace_id, file_slot, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(32768));

	UT_ASSERT(cluster_undo_root_file_slot(2, 0, &file_slot));
	UT_ASSERT_EQ(file_slot, UINT32_C(256));
	UT_ASSERT(cluster_undo_root_id(namespace_id, file_slot, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(33024));

	UT_ASSERT(cluster_undo_root_namespace_id(1, 1, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(2));
	UT_ASSERT(cluster_undo_root_file_slot(1, 0, &file_slot));
	UT_ASSERT(cluster_undo_root_id(namespace_id, file_slot, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(65536));

	UT_ASSERT(cluster_undo_root_namespace_id(2, 0, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(130));
	UT_ASSERT(cluster_undo_root_id(namespace_id, file_slot, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(4259840));
}


UT_TEST(test_pgrd_formula_rejects_invalid_and_exhausted_inputs)
{
	uint64 namespace_id = UINT64_C(0x5a5a5a5a5a5a5a5a);
	uint64 root_id = UINT64_C(0x6b6b6b6b6b6b6b6b);
	uint32 file_slot = UINT32_C(0x7c7c7c7c);

	UT_ASSERT(!cluster_undo_root_namespace_id(0, 0, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(0x5a5a5a5a5a5a5a5a));
	UT_ASSERT(!cluster_undo_root_namespace_id(1, 129, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(0x5a5a5a5a5a5a5a5a));
	UT_ASSERT(cluster_undo_root_namespace_id(UINT64_C(4363953127297), 0, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(562949953421185));
	namespace_id = UINT64_C(0x5a5a5a5a5a5a5a5a);
	UT_ASSERT(!cluster_undo_root_namespace_id(UINT64_C(4363953127298), 0, &namespace_id));
	UT_ASSERT_EQ(namespace_id, UINT64_C(0x5a5a5a5a5a5a5a5a));

	UT_ASSERT(!cluster_undo_root_file_slot(0, 0, &file_slot));
	UT_ASSERT_EQ(file_slot, UINT32_C(0x7c7c7c7c));
	UT_ASSERT(!cluster_undo_root_file_slot(129, 0, &file_slot));
	UT_ASSERT_EQ(file_slot, UINT32_C(0x7c7c7c7c));
	UT_ASSERT(!cluster_undo_root_file_slot(1, 256, &file_slot));
	UT_ASSERT_EQ(file_slot, UINT32_C(0x7c7c7c7c));

	UT_ASSERT(!cluster_undo_root_id(0, 0, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(0x6b6b6b6b6b6b6b6b));
	UT_ASSERT(!cluster_undo_root_id(UINT64_C(562949953421312), 0, &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(0x6b6b6b6b6b6b6b6b));
	UT_ASSERT(!cluster_undo_root_id(1, UINT32_C(32768), &root_id));
	UT_ASSERT_EQ(root_id, UINT64_C(0x6b6b6b6b6b6b6b6b));
}


UT_TEST(test_pgrd_descriptor_resolves_exact_block0_root)
{
	ClusterUndoRootDescriptorV1 descriptor = valid_shared_descriptor();
	ClusterUndoBlock0ResolvedRoot shared;
	ClusterUndoBlock0ResolvedRoot authority;
	ClusterUndoBlock0ResolvedRoot local;
	ClusterUndoBlock0ResolvedRoot sentinel;

	UT_ASSERT(cluster_undo_root_descriptor_resolve(&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED, 1,
												   1, &shared));
	UT_ASSERT_EQ(shared.intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED);
	UT_ASSERT_EQ(shared.root_id, UINT64_C(32768));
	UT_ASSERT_EQ(shared.root_generation, UINT64_C(1));

	UT_ASSERT(cluster_undo_root_descriptor_resolve(
		&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0, 1, 1, &authority));
	UT_ASSERT_EQ(authority.intent, CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0);
	UT_ASSERT_EQ(authority.root_id, shared.root_id);
	UT_ASSERT_EQ(authority.root_generation, shared.root_generation);

	descriptor.root_kind = CLUSTER_UNDO_ROOT_KIND_LOCAL;
	descriptor.owner_node = 0;
	descriptor.root_ordinal = 1;
	descriptor.namespace_id = UINT64_C(2);
	UT_ASSERT(cluster_undo_root_descriptor_resolve(
		&descriptor, CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL, 2, 264, &local));
	UT_ASSERT_EQ(local.intent, CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL);
	UT_ASSERT_EQ(local.root_id, UINT64_C(65799));
	UT_ASSERT_EQ(local.root_generation, UINT64_C(1));

	memset(&sentinel, 0x5a, sizeof(sentinel));
	local = sentinel;
	UT_ASSERT(!cluster_undo_root_descriptor_resolve(&descriptor, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
													2, 264, &local));
	UT_ASSERT_EQ(memcmp(&local, &sentinel, sizeof(local)), 0);
}


int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_pgrd_v1_exact_layout_and_roundtrip);
	UT_RUN(test_pgrd_v1_zero_and_corruption_fail_closed_without_output);
	UT_RUN(test_pgrd_v1_identity_relations_fail_closed);
	UT_RUN(test_pgrd_file_ids_are_exact_and_disjoint);
	UT_RUN(test_pgrd_formula_rejects_invalid_and_exhausted_inputs);
	UT_RUN(test_pgrd_descriptor_resolves_exact_block0_root);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
