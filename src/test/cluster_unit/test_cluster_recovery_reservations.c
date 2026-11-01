/* Separate identity reservations never substitute for coverage receipts. */
int registry_v2_fixture_main(void);
#define main registry_v2_fixture_main
#include "test_cluster_recovery_registry.c"
#undef main
#include "common/cluster_recovery_reservations.h"
#include "../../common/sha2_int.h"
#include "port/pg_crc32c.h"

static ClusterRecoveryRegistryV3 registry3;
static ClusterRecoveryFileReservationV1 reserved[73];
static void fixture3(void)
{
	fixture();
	memset(&registry3, 0, sizeof(registry3));
	memset(reserved, 0, sizeof(reserved));
	registry3.available = reg;
	registry3.catalog_version = CATALOG_VERSION_NO;
	registry3.reservation_count = 1;
	registry3.reservations = reserved;
	reserved[0].expected = files[0].identity;
	reserved[0].expected.kind = CLUSTER_RECOVERY_FILE_CLOG_V2;
	reserved[0].expected.object_id = 7;
	memset(reserved[0].batch_uuid, 0x71, 16);
	reserved[0].creation_owner = sources[0].source.claim.identity;
	reserved[0].owner_generation = 19;
	reserved[0].state = CLUSTER_RECOVERY_FILE_RESERVED_V1;
}
static uint64 read_be(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	while (n--) v = (v << 8) | *p++;
	return v;
}
UT_TEST(test_complete_nested_coverage_and_identity_only_record)
{
	uint8 bytes[1856], ref[128], old[1344], oldref[128];
	Size n = 0;
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	const ClusterRecoveryRegistryV3 *v;
	fixture3();
	UT_ASSERT(cluster_recovery_registry_size_v3(&registry3, sizeof(bytes), &n));
	UT_ASSERT_EQ(n, sizeof(bytes));
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, n, ref));
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, old, sizeof(old), oldref));
	UT_ASSERT(memcmp(bytes + 256, old, sizeof(old)) == 0);
	UT_ASSERT(memcmp(bytes + 128, oldref, 128) == 0);
	UT_ASSERT(memcmp(bytes, "PRCREG3", 7) == 0);
	UT_ASSERT_EQ(read_be(bytes + 8, 4), UINT32_C(0x52535631));
	UT_ASSERT_EQ(read_be(bytes + 48, 8), 1600);
	UT_ASSERT_EQ(read_be(bytes + 56, 8), 1);
	UT_ASSERT_EQ(read_be(bytes + 1600 + 224, 8), 19);
	UT_ASSERT_EQ(read_be(bytes + 1600 + 232, 4), 1);
	UT_ASSERT(cluster_recovery_registry_decode_v3(bytes, n, ref, CATALOG_VERSION_NO, n, &d));
	v = cluster_recovery_registry_view_v3(d);
	UT_ASSERT(v && v->available.coverage.file_count == 2 && v->reservation_count == 1);
	UT_ASSERT_EQ(v->reservations[0].expected.object_id, 7);
	UT_ASSERT(memcmp(v->reservations[0].batch_uuid, reserved[0].batch_uuid, 16) == 0);
	UT_ASSERT(memcmp(&v->reservations[0].creation_owner, &reserved[0].creation_owner,
					 sizeof(ClusterControlRootIdentity)) == 0);
	cluster_recovery_registry_destroy_v3(&d);
	UT_ASSERT(d == NULL);
}
UT_TEST(test_old_reader_and_catalog_remain_closed)
{
	uint8 bytes[1856], ref[128], saved[1856];
	ClusterRecoveryRegistryDecodedV2 *old = NULL;
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
	UT_ASSERT(!cluster_recovery_registry_reference_valid_v2(ref));
	UT_ASSERT(!cluster_recovery_registry_decode_v2(bytes, sizeof(bytes), ref, sizeof(bytes), &old));
	UT_ASSERT(!cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
		CATALOG_VERSION_NO - 1, sizeof(bytes), &d));
	UT_ASSERT(d == NULL && old == NULL);
	memcpy(saved, bytes, sizeof(bytes));
	registry3.catalog_version--;
	UT_ASSERT(!cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
	UT_ASSERT(memcmp(saved, bytes, sizeof(bytes)) == 0);
}
UT_TEST(test_incomplete_coverage_never_becomes_reservation)
{
	Size n = 99;
	fixture3();
	files[0].synced_bytes = 0;
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	UT_ASSERT_EQ(n, 99);
	fixture3();
	registry3.available.coverage.file_count = 1;
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	fixture3();
	files[1].image_digest[0] = 0; /* nonzero whole digest still valid */
	UT_ASSERT(cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	memset(files[1].image_digest, 0, 32);
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
}
UT_TEST(test_missing_creation_custody_refuses)
{
	Size n;
	for (int i = 0; i < 12; i++) {
		fixture3();
		switch (i) {
		case 0: memset(reserved[0].batch_uuid, 0, 16); break;
		case 1: reserved[0].owner_generation = 0; break;
		case 2: reserved[0].owner_generation = UINT64_MAX; break;
		case 3: reserved[0].creation_owner.system_identifier++; break;
		case 4: reserved[0].creation_owner.origin_node_id = 128; break;
		case 5: reserved[0].creation_owner.origin_thread_id++; break;
		case 6: reserved[0].creation_owner.origin_owner_incarnation = 0; break;
		case 7: reserved[0].creation_owner.root_lineage_seq = 0; break;
		case 8: reserved[0].creation_owner.reserved42 = 1; break;
		case 9: memset(reserved[0].creation_owner.authority_uuid, 0, 16); break;
		case 10: reserved[0].state = 2; break;
		case 11: reserved[0].expected.registration_generation = 16; break;
		}
		UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	}
}
UT_TEST(test_duplicate_or_available_key_refuses_even_with_different_birth)
{
	Size n;
	fixture3();
	reserved[1] = reserved[0];
	registry3.reservation_count = 2;
	reserved[1].expected.creation_generation++;
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	reserved[1].expected.origin++;
	UT_ASSERT(cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
	fixture3();
	reserved[0].expected = files[0].identity;
	reserved[0].expected.creation_generation++;
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
}
UT_TEST(test_budget_both_vectors_and_more_than_sixty_four)
{
	Size n = 99;
	uint8 *bytes, ref[128];
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	for (int i = 1; i < 73; i++) {
		reserved[i] = reserved[0];
		reserved[i].expected.object_id += i;
	}
	registry3.reservation_count = 73;
	UT_ASSERT(cluster_recovery_registry_size_v3(&registry3, 20288, &n));
	UT_ASSERT_EQ(n, 20288);
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, n - 1, &n));
	bytes = malloc(n);
	UT_ASSERT(bytes != NULL);
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, n, ref));
	UT_ASSERT(cluster_recovery_registry_decode_v3(bytes, n, ref, CATALOG_VERSION_NO, n, &d));
	UT_ASSERT_EQ(cluster_recovery_registry_view_v3(d)->reservation_count, 73);
	cluster_recovery_registry_destroy_v3(&d);
	free(bytes);
	registry3.reservation_count = UINT64_MAX;
	UT_ASSERT(!cluster_recovery_registry_size_v3(&registry3, SIZE_MAX, &n));
}
UT_TEST(test_empty_reservation_vector_keeps_original_receipts)
{
	uint8 bytes[1600], ref[128];
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	registry3.reservation_count = 0;
	registry3.reservations = NULL;
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
	UT_ASSERT(cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
		CATALOG_VERSION_NO, sizeof(bytes), &d));
	UT_ASSERT_EQ(cluster_recovery_registry_view_v3(d)->available.coverage.file_count, 2);
	UT_ASSERT_EQ(cluster_recovery_registry_view_v3(d)->reservation_count, 0);
	cluster_recovery_registry_destroy_v3(&d);
}
UT_TEST(test_all_bytes_and_reference_are_bound)
{
	uint8 bytes[1856], ref[128];
	fixture3();
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
	for (Size i = 0; i < sizeof(bytes); i++) {
		bytes[i] ^= 1;
		UT_ASSERT(!cluster_recovery_registry_matches_v3(bytes, sizeof(bytes), &registry3, ref));
		bytes[i] ^= 1;
	}
	for (Size i = 0; i < sizeof(ref); i++) {
		ref[i] ^= 1;
		UT_ASSERT(!cluster_recovery_registry_matches_v3(bytes, sizeof(bytes), &registry3, ref));
		ref[i] ^= 1;
	}
}
UT_TEST(test_full_alias_and_fresh_decode_preserve_inputs)
{
	uint8 bytes[1856], ref[128], saved[sizeof(reserved)];
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
	memcpy(saved, reserved, sizeof(reserved));
	UT_ASSERT(!cluster_recovery_registry_encode_v3(&registry3, (uint8 *)reserved, sizeof(bytes), ref));
	UT_ASSERT(memcmp(saved, reserved, sizeof(reserved)) == 0);
	UT_ASSERT(!cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), bytes + 7));
	UT_ASSERT(!cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
		CATALOG_VERSION_NO, sizeof(bytes), (ClusterRecoveryRegistryDecodedV3 **)(void *)bytes));
	UT_ASSERT(cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
		CATALOG_VERSION_NO, sizeof(bytes), &d));
	UT_ASSERT(!cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
		CATALOG_VERSION_NO, sizeof(bytes), &d));
	cluster_recovery_registry_destroy_v3(&d);
}
static void reservation_drift(void) { reserved[0].creation_owner.root_lineage_seq++; }
static const char *registry_name3 = "pgrac_recovery_registry/000000000000000f-0000000000000010-0000000000000011.v3";
static void cleanup3(int dir, const char *path)
{
	(void)unlinkat(dir, registry_name3, 0);
	(void)unlinkat(dir, "pgrac_recovery_registry", AT_REMOVEDIR);
	(void)close(dir);
	(void)rmdir(path);
}
UT_TEST(test_real_file_selected_load_and_immutable_creation)
{
	char path[] = "/tmp/pgrac-reservation-XXXXXX";
	int dir;
	uint8 ref[128], saved[128];
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	UT_ASSERT(cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	memcpy(saved, ref, 128);
	UT_ASSERT(cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 4096, &d));
	UT_ASSERT_EQ(cluster_recovery_registry_view_v3(d)->reservation_count, 1);
	cluster_recovery_registry_destroy_v3(&d);
	UT_ASSERT(!cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	UT_ASSERT(memcmp(saved, ref, 128) == 0);
	UT_ASSERT(!cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 1855, &d));
	cleanup3(dir, path);
}
UT_TEST(test_partial_or_damaged_file_never_loads)
{
	char path[] = "/tmp/pgrac-reservation-damage-XXXXXX";
	int dir, fd;
	uint8 ref[128], byte = 0x7f;
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	UT_ASSERT(cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	fd = openat(dir, registry_name3, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, &byte, 1, 1600 + 240), 1);
	UT_ASSERT(!cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 4096, &d));
	UT_ASSERT(d == NULL);
	UT_ASSERT_EQ(ftruncate(fd, 500), 0);
	UT_ASSERT(!cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 4096, &d));
	UT_ASSERT_EQ(close(fd), 0);
	cleanup3(dir, path);
}
UT_TEST(test_sync_and_late_owner_failure_preserve_reference_and_file)
{
	char path[] = "/tmp/pgrac-reservation-fault-XXXXXX";
	uint8 ref[128], saved[128];
	int dir;
	fixture3();
	memset(ref, 0xa5, 128); memcpy(saved, ref, 128);
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	test_registry_io_fault(1);
	UT_ASSERT(!cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	UT_ASSERT(memcmp(saved, ref, 128) == 0);
	UT_ASSERT(!cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	UT_ASSERT_EQ(unlinkat(dir, registry_name3, 0), 0);
	test_registry_io_read_hook(reservation_drift);
	UT_ASSERT(!cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	UT_ASSERT(memcmp(saved, ref, 128) == 0);
	cleanup3(dir, path);
}

static void reservation_put(uint8 *p, uint64 v, unsigned n)
{
	for (unsigned i = 0; i < n; i++) p[i] = (uint8)(v >> (8 * (n - i - 1)));
}
static uint32 reservation_crc(const uint8 *p, Size n)
{
	pg_crc32c c;
	INIT_CRC32C(c); COMP_CRC32C(c, p, n); FIN_CRC32C(c);
	return c;
}
/* Deliberately repair transport checksums: canonical/semantic validation must
 * still reject malformed records; checksums alone cannot authorize them. */
static void reservation_reseal(uint8 *p, Size n, uint8 ref[128])
{
	pg_sha256_ctx c;
	reservation_put(p + 72, reservation_crc(p + 256, n - 256), 4);
	memset(p + 76, 0, 4);
	reservation_put(p + 76, reservation_crc(p, 256), 4);
	pg_sha256_init(&c); pg_sha256_update(&c, p, n); pg_sha256_final(&c, ref + 72);
	memset(ref + 104, 0, 4);
	reservation_put(ref + 104, reservation_crc(ref, 128), 4);
}
UT_TEST(test_resealed_malformed_record_and_counts_still_refuse)
{
	uint8 bytes[1856], ref[128];
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	for (int i = 0; i < 8; i++) {
		fixture3();
		UT_ASSERT(cluster_recovery_registry_encode_v3(&registry3, bytes, sizeof(bytes), ref));
		switch (i) {
		case 0: bytes[1600 + 240] = 1; break;
		case 1: bytes[1600 + 239] = 2; break;
		case 2: bytes[1600 + 235] = 2; break;
		case 3: memset(bytes + 1600 + 128, 0, 16); break;
		case 4: bytes[1600 + 144 + 42] = 1; break;
		case 5: bytes[80] = 1; break;
		case 6: memset(bytes + 56, 0xff, 8); break;
		case 7: memset(bytes + 40, 0xff, 8); break;
		}
		reservation_reseal(bytes, sizeof(bytes), ref);
		UT_ASSERT(!cluster_recovery_registry_decode_v3(bytes, sizeof(bytes), ref,
			CATALOG_VERSION_NO, sizeof(bytes), &d));
		UT_ASSERT(d == NULL);
	}
}
UT_TEST(test_named_file_alias_and_permissions_refuse)
{
	char path[] = "/tmp/pgrac-reservation-name-XXXXXX";
	uint8 ref[128];
	int dir, fd;
	ClusterRecoveryRegistryDecodedV3 *d = NULL;
	fixture3();
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	UT_ASSERT(cluster_recovery_registry_create_v3(dir, &registry3, 4096, ref));
	UT_ASSERT_EQ(linkat(dir, registry_name3, dir, "hardlink", 0), 0);
	UT_ASSERT(!cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 4096, &d));
	UT_ASSERT_EQ(unlinkat(dir, "hardlink", 0), 0);
	fd = openat(dir, registry_name3, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(fchmod(fd, 0666), 0);
	UT_ASSERT(!cluster_recovery_registry_load_selected_v3(dir, ref, CATALOG_VERSION_NO, 4096, &d));
	UT_ASSERT_EQ(close(fd), 0);
	cleanup3(dir, path);
}

int main(void)
{
	UT_PLAN(14);
	UT_RUN(test_complete_nested_coverage_and_identity_only_record);
	UT_RUN(test_old_reader_and_catalog_remain_closed);
	UT_RUN(test_incomplete_coverage_never_becomes_reservation);
	UT_RUN(test_missing_creation_custody_refuses);
	UT_RUN(test_duplicate_or_available_key_refuses_even_with_different_birth);
	UT_RUN(test_budget_both_vectors_and_more_than_sixty_four);
	UT_RUN(test_empty_reservation_vector_keeps_original_receipts);
	UT_RUN(test_all_bytes_and_reference_are_bound);
	UT_RUN(test_full_alias_and_fresh_decode_preserve_inputs);
	UT_RUN(test_real_file_selected_load_and_immutable_creation);
	UT_RUN(test_partial_or_damaged_file_never_loads);
	UT_RUN(test_sync_and_late_owner_failure_preserve_reference_and_file);
	UT_RUN(test_resealed_malformed_record_and_counts_still_refuse);
	UT_RUN(test_named_file_alias_and_permissions_refuse);
	UT_DONE();
	return ut_failed_count != 0;
}
