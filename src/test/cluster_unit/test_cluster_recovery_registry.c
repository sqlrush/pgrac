#ifdef TEST_CONTROL_REGISTRY
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include "common/cluster_recovery_registry.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();
static ClusterRecoveryRegistryV2 reg;
static ClusterRecoveryCheckpointSourceV2 sources[2];
static ClusterRecoveryCheckpointFileV2 files[2];
static ClusterRecoveryCheckpointFileSourceV2 intervals[2][2];
static void fixture(void)
{
	memset(&reg, 0, sizeof(reg));
	memset(sources, 0, sizeof(sources));
	memset(files, 0, sizeof(files));
	memset(intervals, 0, sizeof(intervals));
	reg.system_identifier = 11;
	reg.database_incarnation = 12;
	memset(reg.storage_uuid, 0x21, 16);
	memset(reg.redo_branch_uuid, 0x31, 16);
	reg.redo_branch_generation = 13;
	reg.coverage.control_generation = 14;
	reg.coverage.registration_generation = 15;
	reg.coverage.checkpoint_generation = 16;
	reg.coverage.commit_generation = 17;
	reg.coverage.sources = sources;
	reg.coverage.source_count = 2;
	reg.coverage.files = files;
	reg.coverage.file_count = 2;
	for (int i = 0; i < 2; i++)
	{
		ClusterControlRootIdentity *id = &sources[i].source.claim.identity;
		id->system_identifier = 11;
		memcpy(id->storage_uuid, reg.storage_uuid, 16);
		memset(id->authority_uuid, 0x41, 16);
		id->origin_node_id = i;
		id->origin_thread_id = i + 1;
		id->thread_claim_created_at = 20 + i;
		id->thread_claim_crc32c = 30 + i;
		id->origin_owner_incarnation = 40 + i;
		id->root_lineage_seq = 50 + i;
		sources[i].source.claim.database_incarnation = 12;
		sources[i].source.claim.max_config_generation = 60 + i;
		memset(sources[i].source.claim.claim_sha256, 0x51 + i, 32);
		sources[i].source.timeline = 1;
		sources[i].checkpoint.redo = 100;
		sources[i].checkpoint.ThisTimeLineID = 1;
		sources[i].checkpoint.PrevTimeLineID = 1;
		sources[i].checkpoint.fullPageWrites = true;
		sources[i].checkpoint.nextXid = FullTransactionIdFromU64(0x200000032);
		sources[i].checkpoint.time = -123;
		sources[i].checkpoint.nextOid = 101;
		sources[i].checkpoint.nextMulti = 102;
		sources[i].checkpoint.nextMultiOffset = 103;
		sources[i].checkpoint.oldestXid = 104;
		sources[i].checkpoint.oldestXidDB = 105;
		sources[i].checkpoint.oldestMulti = 106;
		sources[i].checkpoint.oldestMultiDB = 107;
		sources[i].checkpoint.oldestCommitTsXid = 108;
		sources[i].checkpoint.newestCommitTsXid = 109;
		sources[i].checkpoint.oldestActiveXid = 110;
		if (i == 1)
		{
			sources[i].local_bounds_observed = true;
			sources[i].next_full_xid = FullTransactionIdFromU64(0x200000033);
			sources[i].oldest_clog_xid = 3;
		}
		sources[i].checkpoint_start = 100;
		sources[i].checkpoint_end = 120;
		sources[i].redo_start = 80;
		sources[i].replay_end = 180;
		sources[i].native_floor = FullTransactionIdFromU64(0x200000003);
		sources[i].post_flush_issued = FullTransactionIdFromU64(0x200000030);
		files[i].identity.kind = CLUSTER_RECOVERY_FILE_RELMAP_V2;
		files[i].identity.format_id = CLUSTER_RECOVERY_FORMAT_ID_V2;
		files[i].identity.format_version = 2;
		files[i].identity.system_identifier = 11;
		files[i].identity.database_incarnation = 12;
		memcpy(files[i].identity.storage_uuid, reg.storage_uuid, 16);
		memcpy(files[i].identity.redo_branch_uuid, reg.redo_branch_uuid, 16);
		files[i].identity.redo_branch_generation = 13;
		files[i].identity.redo_timeline = 1;
		files[i].identity.creation_generation = 18;
		files[i].identity.registration_generation = 15;
		files[i].identity.object_id = 0;
		files[i].copy_count = 2;
		files[i].copy_index = i;
		files[i].synced_bytes = 2304;
		files[i].page_count = 1;
		memset(files[i].image_digest, 0x61 + i, 32);
		files[i].source_count = 2;
		files[i].sources = intervals[i];
		for (int j = 0; j < 2; j++)
		{
			intervals[i][j].source_index = j;
			intervals[i][j].redo_start = 80;
			intervals[i][j].replay_end = 180;
		}
	}
}
UT_TEST(test_complete_vectors_and_explicit_offsets)
{
	Size n = 0;
	uint8 bytes[1344], ref[128];
	fixture();
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &n));
	UT_ASSERT_EQ(n, 1344);
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, n, ref));
	UT_ASSERT(memcmp(bytes, "PRCREG2\0", 8) == 0);
	UT_ASSERT_EQ(bytes[143], 0);
	UT_ASSERT_EQ(bytes[142], 1);                     /* source offset256 */
	UT_ASSERT_EQ(bytes[288 + 256 + 44 + 3], 1);      /* second original source node */
	UT_ASSERT_EQ(bytes[256 + 200 + 7], 0x85);        /* signed checkpoint time -123 */
	UT_ASSERT_EQ(bytes[256 + 2 * 288 + 128 + 7], 0); /* synced_bytes2304 = 0x900 */
	{
		static const uint8 golden_sha[32] = {0x72, 0xa1, 0x18, 0x2e, 0xda, 0x98, 0xdc, 0xd8,
											 0x25, 0xf6, 0xbf, 0x7b, 0xe5, 0x57, 0xc3, 0x46,
											 0xd1, 0xfd, 0x5e, 0x63, 0xce, 0x7b, 0xd6, 0x12,
											 0x9a, 0x1b, 0x98, 0x17, 0x56, 0xc1, 0x38, 0x01};
		UT_ASSERT(memcmp(ref + 72, golden_sha, 32) == 0);
	}
	UT_ASSERT(cluster_recovery_registry_matches_v2(bytes, n, &reg, ref));
}
UT_TEST(test_complete_cohort_and_creation_binding)
{
	uint8 bytes[1344], ref[128];
	fixture();
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	sources[1].source.claim.identity.root_lineage_seq++;
	UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, sizeof(bytes), &reg, ref));
	sources[1].source.claim.identity.root_lineage_seq--;
	files[1].identity.creation_generation++;
	UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, sizeof(bytes), &reg, ref));
}
UT_TEST(test_damage_trailing_and_reference_refuse)
{
	uint8 bytes[1345], ref[128];
	fixture();
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, 1344, ref));
	for (Size i = 0; i < 1344; i++)
	{
		bytes[i] ^= 1;
		UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, 1344, &reg, ref));
		bytes[i] ^= 1;
	}
	UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, 1345, &reg, ref));
	for (int i = 0; i < 128; i++)
	{
		ref[i] ^= 1;
		UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, 1344, &reg, ref));
		ref[i] ^= 1;
	}
}
UT_TEST(test_geometry_duplicates_and_unknown_bounds)
{
	Size n = 99;
	uint8 bytes[1344], ref[128], before[1344];
	fixture();
	memset(bytes, 0xa5, sizeof(bytes));
	memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, 1343, &n));
	UT_ASSERT_EQ(n, 99);
	reg.coverage.source_count = UINT64_MAX;
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, SIZE_MAX, &n));
	fixture();
	sources[1] = sources[0];
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
	fixture();
	intervals[1][1] = intervals[1][0];
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	fixture();
	files[1] = files[0];
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	fixture();
	sources[0].next_full_xid = FullTransactionIdFromU64(3);
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	fixture();
	intervals[0][0].replay_end = 181;
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	fixture();
	sources[0].source.claim.identity.reserved42 = 1;
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	fixture();
	sources[0].source.claim.identity.origin_node_id = -1;
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
}
UT_TEST(test_output_alias_preserves_original)
{
	uint8 bytes[1344], ref[128];
	fixture();
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, (uint8 *)&sources[0], 1344, ref));
	UT_ASSERT_EQ(sources[0].source.claim.identity.system_identifier, 11);
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), bytes + 10));
}
UT_TEST(test_real_immutable_file_and_selected_read)
{
	char path[] = "/tmp/pgrac-registry-XXXXXX", name[160];
	uint8 ref[128], prior[128];
	int dir, fd;
	fixture();
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	{
		pid_t child = fork();
		int status = 0;
		UT_ASSERT(child >= 0);
		if (child == 0)
		{
			reg.coverage.file_count = SIZE_MAX / sizeof(*reg.coverage.files) + 1;
			_exit(cluster_recovery_registry_create_v2(dir, &reg, 4096, ref) ? 1 : 0);
		}
		UT_ASSERT(waitpid(child, &status, 0) == child);
		UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
	}
	UT_ASSERT(cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	UT_ASSERT(cluster_recovery_registry_read_v2(dir, &reg, ref, 4096));
	memcpy(prior, ref, 128);
	UT_ASSERT(!cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	UT_ASSERT(memcmp(prior, ref, 128) == 0);
	snprintf(name, sizeof(name), "pgrac_recovery_registry/%016llx-%016llx-%016llx.v2", 15LL, 16LL,
			 17LL);
	fd = openat(dir, name, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pwrite(fd, "x", 1, 300), 1);
	UT_ASSERT_EQ(close(fd), 0);
	UT_ASSERT(!cluster_recovery_registry_read_v2(dir, &reg, ref, 4096));
	UT_ASSERT_EQ(unlinkat(dir, name, 0), 0);
	UT_ASSERT_EQ(unlinkat(dir, "pgrac_recovery_registry", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(dir), 0);
	UT_ASSERT_EQ(rmdir(path), 0);
}
void test_registry_io_fault(int at);
void test_registry_io_read_hook(void (*hook)(void));
static void drift(void)
{
	sources[1].source.claim.identity.root_lineage_seq++;
}
UT_TEST(test_sync_failure_and_late_cohort_change_preserve_reference)
{
	char path[] = "/tmp/pgrac-registry-fault-XXXXXX", name[160];
	uint8 ref[128], saved[128];
	int dir;
	fixture();
	memset(ref, 0xa5, 128);
	memcpy(saved, ref, 128);
	UT_ASSERT(mkdtemp(path) != NULL);
	dir = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(dir >= 0);
	test_registry_io_fault(1);
	UT_ASSERT(!cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	UT_ASSERT(memcmp(ref, saved, 128) == 0);
	UT_ASSERT(!cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	snprintf(name, sizeof(name), "pgrac_recovery_registry/%016llx-%016llx-%016llx.v2", 15LL, 16LL,
			 17LL);
	UT_ASSERT_EQ(unlinkat(dir, name, 0), 0);
	test_registry_io_read_hook(drift);
	UT_ASSERT(!cluster_recovery_registry_create_v2(dir, &reg, 4096, ref));
	UT_ASSERT(memcmp(ref, saved, 128) == 0);
	UT_ASSERT_EQ(unlinkat(dir, name, 0), 0);
	UT_ASSERT_EQ(unlinkat(dir, "pgrac_recovery_registry", AT_REMOVEDIR), 0);
	UT_ASSERT_EQ(close(dir), 0);
	UT_ASSERT_EQ(rmdir(path), 0);
}
UT_TEST(test_complete_registry_exceeds_active_pin_capacity_and_keeps_birth_registration)
{
	ClusterRecoveryCheckpointFileV2 many[73];
	uint8 *bytes, ref[128];
	Size n = 0;
	fixture();
	for (int j = 0; j < 73; j++)
	{
		many[j] = files[0];
		many[j].identity.object_id = j;
		many[j].identity.registration_generation = 14;
		many[j].copy_count = 1;
	}
	reg.coverage.files = many;
	reg.coverage.file_count = 73;
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, 1024 * 1024, &n));
	bytes = malloc(19520);
	UT_ASSERT(bytes != NULL);
	UT_ASSERT_EQ(n, 19520);
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, 19520, ref));
	UT_ASSERT(cluster_recovery_registry_matches_v2(bytes, 19520, &reg, ref));
	many[72].identity.registration_generation = 16;
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, 19520, ref));
	free(bytes);
}
UT_TEST(test_creation_batch_roundtrip_preserves_old_record_prefix)
{
	ClusterRecoveryRegistryDecodedV2 *decoded = NULL;
	const ClusterRecoveryRegistryV2 *view;
	Size old_size = 0, size = 0;
	uint8 old[4096], bytes[4096], ref[128], oldref[128], batch[16];
	fixture();
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(old), &old_size));
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, old, old_size, oldref));
	memset(batch, 0x71, sizeof(batch));
	for (unsigned i = 0; i < 2; i++) memcpy(files[i].creation_batch_uuid, batch, 16);
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &size));
	UT_ASSERT_EQ(size, old_size + 32); /* two exact 224B records */
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, size, ref));
	if (size == old_size + 32) {
		UT_ASSERT_EQ(memcmp(old + 832, bytes + 832, 208), 0);
		UT_ASSERT_EQ(memcmp(old + 1040, bytes + 1056, 208), 0);
	}
	UT_ASSERT(cluster_recovery_registry_decode_v2(bytes, size, ref, sizeof(bytes), &decoded));
	view = cluster_recovery_registry_view_v2(decoded);
	UT_ASSERT(view != NULL);
	if (view != NULL) for (unsigned i = 0; i < 2; i++)
		UT_ASSERT_EQ(memcmp(view->coverage.files[i].creation_batch_uuid, batch, 16), 0);
	cluster_recovery_registry_destroy_v2(&decoded);
	UT_ASSERT(cluster_recovery_registry_decode_v2(old, old_size, oldref, sizeof(old), &decoded));
	view = cluster_recovery_registry_view_v2(decoded);
	memset(batch, 0, sizeof(batch));
	if (view != NULL) for (unsigned i = 0; i < 2; i++)
		UT_ASSERT_EQ(memcmp(view->coverage.files[i].creation_batch_uuid, batch, 16), 0);
	cluster_recovery_registry_destroy_v2(&decoded);
}
UT_TEST(test_creation_batch_partial_pair_and_unknown_extent_refuse)
{
	Size n = 0;
	uint8 bytes[4096], before[4096], ref[128];
	fixture();
	memset(files[0].creation_batch_uuid, 0x72, 16);
	memset(bytes, 0xa5, sizeof(bytes)); memcpy(before, bytes, sizeof(bytes));
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &n));
	UT_ASSERT(!cluster_recovery_registry_encode_v2(&reg, bytes, sizeof(bytes), ref));
	UT_ASSERT_EQ(memcmp(bytes, before, sizeof(bytes)), 0);
	memset(files[1].creation_batch_uuid, 0x73, 16);
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &n));
	memcpy(files[1].creation_batch_uuid, files[0].creation_batch_uuid, 16);
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &n));
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, n, ref));
	UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, n - 16, &reg, ref));
	bytes[832 + 208] ^= 1;
	UT_ASSERT(!cluster_recovery_registry_matches_v2(bytes, n, &reg, ref));
}
UT_TEST(test_explicit_coverage_family_is_not_guessed_or_downgraded)
{
	ClusterRecoveryRegistryDecodedV2 *d = NULL;
	const ClusterRecoveryRegistryV2 *v;
	uint8 old[4096], bytes[4096], ref[128], oldref[128], again[4096], againref[128];
	Size oldn = 0, n = 0, nextn = 0;
	fixture();
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(old), &oldn));
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, old, oldn, oldref));
	reg.coverage_format_version = 4;
	UT_ASSERT(cluster_recovery_registry_size_v2(&reg, sizeof(bytes), &n));
	UT_ASSERT_EQ(n, oldn + 32);
	UT_ASSERT(cluster_recovery_registry_encode_v2(&reg, bytes, n, ref));
	UT_ASSERT_EQ(memcmp(bytes, "PRCREG4", 8), 0);
	UT_ASSERT_EQ(memcmp(ref, "PRCREF4", 8), 0);
	UT_ASSERT(cluster_recovery_registry_decode_v2(bytes, n, ref, sizeof(bytes), &d));
	v = cluster_recovery_registry_view_v2(d);
	UT_ASSERT(v != NULL);
	if (v) {
		UT_ASSERT_EQ(v->coverage_format_version, 4);
		UT_ASSERT(cluster_recovery_registry_size_v2(v, sizeof(again), &nextn));
		UT_ASSERT_EQ(n, nextn);
		UT_ASSERT(cluster_recovery_registry_encode_v2(v, again, nextn, againref));
		UT_ASSERT_EQ(memcmp(bytes, again, n), 0);
		UT_ASSERT_EQ(memcmp(ref, againref, 128), 0);
	}
	cluster_recovery_registry_destroy_v2(&d);
	UT_ASSERT(!cluster_recovery_registry_decode_v2(old, oldn, ref, sizeof(old), &d));
	UT_ASSERT(!cluster_recovery_registry_decode_v2(bytes, n, oldref, sizeof(bytes), &d));
	UT_ASSERT(!cluster_recovery_registry_decode_v2(bytes, n + 1, ref, sizeof(bytes), &d));
	reg.coverage_format_version = 2;
	memset(files[0].creation_batch_uuid, 0x51, 16);
	memset(files[1].creation_batch_uuid, 0x51, 16);
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, sizeof(old), &oldn));
	reg.coverage_format_version = 3;
	UT_ASSERT(!cluster_recovery_registry_size_v2(&reg, sizeof(old), &oldn));
}
int main(void)
{
	UT_PLAN(11);
	UT_RUN(test_complete_vectors_and_explicit_offsets);
	UT_RUN(test_complete_cohort_and_creation_binding);
	UT_RUN(test_damage_trailing_and_reference_refuse);
	UT_RUN(test_geometry_duplicates_and_unknown_bounds);
	UT_RUN(test_output_alias_preserves_original);
	UT_RUN(test_real_immutable_file_and_selected_read);
	UT_RUN(test_sync_failure_and_late_cohort_change_preserve_reference);
	UT_RUN(test_complete_registry_exceeds_active_pin_capacity_and_keeps_birth_registration);
	UT_RUN(test_creation_batch_roundtrip_preserves_old_record_prefix);
	UT_RUN(test_creation_batch_partial_pair_and_unknown_extent_refuse);
	UT_RUN(test_explicit_coverage_family_is_not_guessed_or_downgraded);
	UT_DONE();
	return ut_failed_count != 0;
}
