/*-------------------------------------------------------------------------
 *
 * test_cluster_write_fence_durable.c
 *	  RF-ROOT P2 tests for STOP-02 \u00a717.5 direct durable authority.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_write_fence.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

int cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
char *cluster_voting_disks = NULL;
static uint32 test_wait_event_info = 0;
uint32 *my_wait_event_info = &test_wait_event_info;

static ClusterFenceMarker mock_slots[CLUSTER_MAX_VOTING_DISKS][CLUSTER_MAX_NODES];
static bool mock_slot_present[CLUSTER_MAX_VOTING_DISKS][CLUSTER_MAX_NODES];
static bool mock_disk_read_failed[CLUSTER_MAX_VOTING_DISKS];
static ClusterJoinCommitMarker mock_join_slots[CLUSTER_MAX_VOTING_DISKS];
static uint64 mock_owner_incarnations[CLUSTER_MAX_VOTING_DISKS];
static int mock_open_fds[CLUSTER_MAX_VOTING_DISKS];
static int mock_open_fd_count;
static int cache_invalidation_count;
static int owner_selector_total;

static void three_disk_config(char config[MAXPGPATH * 3 + 3], char paths[3][MAXPGPATH]);
static void remove_three_disks(char paths[3][MAXPGPATH]);

void
cluster_write_fence_authority_cache_invalidate(void)
{
	cache_invalidation_count++;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
cluster_voting_disk_open(const char *path, bool create_if_missing)
{
	int fd;

	(void)create_if_missing;
	fd = open(path, O_RDWR);
	if (fd >= 0 && mock_open_fd_count < CLUSTER_MAX_VOTING_DISKS)
		mock_open_fds[mock_open_fd_count++] = fd;
	return fd;
}

void
cluster_voting_disk_close(int fd)
{
	if (fd >= 0)
		(void)close(fd);
}

ClusterVotingDiskIoState
cluster_voting_disk_read_slot(int fd, int expected_disk_index, uint32 node_id,
							  ClusterVotingSlot *out)
{
	(void)fd;
	if (expected_disk_index < 0 || expected_disk_index >= CLUSTER_MAX_VOTING_DISKS
		|| node_id >= CLUSTER_MAX_NODES || mock_disk_read_failed[expected_disk_index])
		return CLUSTER_VOTING_DISK_IO_FAILED;
	memset(out, 0, sizeof(*out));
	out->node_id = node_id;
	out->incarnation = mock_owner_incarnations[expected_disk_index];
	if (mock_slot_present[expected_disk_index][node_id])
		cluster_fence_marker_pack(out->_reserved1, &mock_slots[expected_disk_index][node_id]);
	return CLUSTER_VOTING_DISK_IO_OK;
}

ClusterVotingDiskIoState
cluster_voting_disk_read_join_slot(int fd, uint32 node_id, void *out_slot512)
{
	int disk_index = -1;
	int i;

	(void)node_id;
	for (i = 0; i < mock_open_fd_count; i++)
		if (mock_open_fds[i] == fd) {
			disk_index = i;
			break;
		}
	if (disk_index < 0 || disk_index >= CLUSTER_MAX_VOTING_DISKS
		|| mock_disk_read_failed[disk_index])
		return CLUSTER_VOTING_DISK_IO_FAILED;
	memset(out_slot512, 0, CLUSTER_VOTING_SLOT_BYTES);
	memcpy(out_slot512, &mock_join_slots[disk_index], sizeof(mock_join_slots[disk_index]));
	return CLUSTER_VOTING_DISK_IO_OK;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_select_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
										uint64 frozen_admitted_bitmap_low,
										uint64 frozen_admitted_bitmap_high,
										const ClusterRecoveryOwnerDiskSampleV1 *samples,
										int total_disk_count, uint64 *out_incarnation)
{
	UT_ASSERT_EQ(node_id, 0);
	UT_ASSERT(immutable_claim != NULL);
	UT_ASSERT_EQ(frozen_admitted_bitmap_low, 1);
	UT_ASSERT_EQ(frozen_admitted_bitmap_high, 0);
	UT_ASSERT(samples != NULL);
	owner_selector_total = total_disk_count;
	UT_ASSERT_EQ(samples[0].join_marker.admitted_incarnation, 77);
	UT_ASSERT_EQ(samples[1].slot.incarnation, 70);
	*out_incarnation = 77;
	return CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
}

static ClusterFenceMarker
valid_marker(uint64 epoch, uint64 generation, uint64 event_id)
{
	ClusterFenceMarker m;

	memset(&m, 0, sizeof(m));
	m.magic = CLUSTER_FENCE_MARKER_MAGIC;
	m.version = CLUSTER_FENCE_MARKER_VERSION;
	m.fence_epoch = epoch;
	m.fence_generation = generation;
	m.fence_event_id = event_id;
	m.issuer_node_id = 1;
	m.fenced_dead_bitmap[0] = 0x04;
	return m;
}

static void
reset_mock(void)
{
	memset(mock_slots, 0, sizeof(mock_slots));
	memset(mock_slot_present, 0, sizeof(mock_slot_present));
	memset(mock_disk_read_failed, 0, sizeof(mock_disk_read_failed));
	memset(mock_join_slots, 0, sizeof(mock_join_slots));
	memset(mock_owner_incarnations, 0, sizeof(mock_owner_incarnations));
	memset(mock_open_fds, -1, sizeof(mock_open_fds));
	mock_open_fd_count = 0;
	cache_invalidation_count = 0;
	owner_selector_total = 0;
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
}

UT_TEST(test_owner_import_runtime_reads_every_distinct_disk)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterWalThreadClaim claim;
	uint64 incarnation = 0;

	reset_mock();
	three_disk_config(config, paths);
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(12345));
	mock_join_slots[0].admitted_incarnation = 77;
	mock_owner_incarnations[1] = 70;
	UT_ASSERT_EQ(cluster_recovery_owner_import_read_v1(0, &claim, 1, 0, &incarnation),
				 CLUSTER_RECOVERY_OWNER_IMPORT_JCMK);
	UT_ASSERT_EQ(owner_selector_total, 3);
	UT_ASSERT_EQ(incarnation, 77);
	remove_three_disks(paths);
}

static void
make_temp_disk(char path[MAXPGPATH])
{
	char templ[] = "/tmp/pgrac-fence-authority-XXXXXX";
	int fd = mkstemp(templ);

	if (fd < 0)
		abort();
	(void)close(fd);
	strlcpy(path, templ, MAXPGPATH);
}

static void
three_disk_config(char config[MAXPGPATH * 3 + 3], char paths[3][MAXPGPATH])
{
	make_temp_disk(paths[0]);
	make_temp_disk(paths[1]);
	make_temp_disk(paths[2]);
	snprintf(config, MAXPGPATH * 3 + 3, "%s,%s,%s", paths[0], paths[1], paths[2]);
	cluster_voting_disks = config;
}

static void
remove_three_disks(char paths[3][MAXPGPATH])
{
	int i;

	for (i = 0; i < 3; i++)
		(void)unlink(paths[i]);
}

UT_TEST(test_typed_preconditions_preserve_output)
{
	ClusterFenceAuthorityProof proof;
	ClusterFenceAuthorityProof before;

	reset_mock();
	memset(&proof, 0xA5, sizeof(proof));
	before = proof;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(NULL),
				 CLUSTER_FENCE_AUTHORITY_BAD_ARGUMENT);
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_OFF;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_ENFORCEMENT_OFF);
	UT_ASSERT_EQ(cache_invalidation_count, 1);
	UT_ASSERT(memcmp(&proof, &before, sizeof(proof)) == 0);
	cluster_write_fence_enforcement = CLUSTER_WRITE_FENCE_ENFORCE_ON;
	cluster_voting_disks = NULL;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_NO_CONFIG);
	UT_ASSERT_EQ(cache_invalidation_count, 2);
	UT_ASSERT(memcmp(&proof, &before, sizeof(proof)) == 0);
}

UT_TEST(test_strict_config_rejects_empty_and_physical_duplicate)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterFenceAuthorityProof proof;

	reset_mock();
	make_temp_disk(paths[0]);
	make_temp_disk(paths[1]);
	snprintf(config, sizeof(config), "%s,,%s", paths[0], paths[1]);
	cluster_voting_disks = config;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_BAD_CONFIG);

	make_temp_disk(paths[2]);
	(void)unlink(paths[2]);
	if (link(paths[0], paths[2]) != 0)
		abort();
	snprintf(config, sizeof(config), "%s,%s", paths[0], paths[2]);
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_BAD_CONFIG);
	(void)unlink(paths[0]);
	(void)unlink(paths[1]);
	(void)unlink(paths[2]);
}

UT_TEST(test_durable_majority_and_total_counts)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterFenceAuthorityProof proof;

	reset_mock();
	three_disk_config(config, paths);
	mock_slots[0][0] = valid_marker(8, 2, 0xAA);
	mock_slots[1][0] = mock_slots[0][0];
	mock_slots[2][0] = valid_marker(9, 1, 0xBB);
	mock_slot_present[0][0] = true;
	mock_slot_present[1][0] = true;
	mock_slot_present[2][0] = true;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof), CLUSTER_FENCE_AUTHORITY_OK);
	UT_ASSERT_EQ(proof.marker.fence_epoch, 8);
	UT_ASSERT_EQ(proof.agree_disk_count, 2);
	UT_ASSERT_EQ(proof.total_disk_count, 3);
	remove_three_disks(paths);
}

UT_TEST(test_join_baseline_majority_replaces_prior_failstop_tuple)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterFenceAuthorityProof proof;
	ClusterFenceMarker prior;
	ClusterFenceMarker joined;

	reset_mock();
	three_disk_config(config, paths);
	prior = valid_marker(8, 8, UINT64_C(0xAA));
	prior.issuer_node_id = 0;
	prior.fenced_dead_bitmap[0] = UINT8_C(0x06); /* owner 1 + unrelated 2 */
	joined = valid_marker(9, 9, UINT64_C(0xBB));
	joined.issuer_node_id = 0;
	joined.fenced_dead_bitmap[0] = UINT8_C(0x04); /* admit only owner 1 */
	joined.marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;

	/* Two distinct disks carry the prospective JOIN tuple while one still
	 * carries the prior FAIL_STOP tuple: the direct reader must choose the new
	 * majority without losing the unrelated excluded origin. */
	mock_slots[0][0] = joined;
	mock_slots[1][0] = joined;
	mock_slots[2][0] = prior;
	mock_slot_present[0][0] = true;
	mock_slot_present[1][0] = true;
	mock_slot_present[2][0] = true;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof), CLUSTER_FENCE_AUTHORITY_OK);
	UT_ASSERT_EQ(proof.marker.fence_epoch, UINT64_C(9));
	UT_ASSERT_EQ(proof.marker.fence_generation, UINT64_C(9));
	UT_ASSERT_EQ(proof.marker.fence_event_id, UINT64_C(0xBB));
	UT_ASSERT_EQ(proof.marker.fenced_dead_bitmap[0], UINT8_C(0x04));
	UT_ASSERT_EQ((int)proof.marker.marker_kind, (int)CLUSTER_FENCE_MARKER_KIND_BASELINE);
	UT_ASSERT_EQ(proof.agree_disk_count, 2);
	UT_ASSERT_EQ(proof.total_disk_count, 3);
	remove_three_disks(paths);
}

UT_TEST(test_unreadable_disk_stays_in_denominator)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterFenceAuthorityProof proof;

	reset_mock();
	three_disk_config(config, paths);
	mock_slots[0][0] = valid_marker(8, 2, 0xAA);
	mock_slots[1][0] = mock_slots[0][0];
	mock_slot_present[0][0] = true;
	mock_slot_present[1][0] = true;
	(void)unlink(paths[2]);
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof), CLUSTER_FENCE_AUTHORITY_OK);
	UT_ASSERT_EQ(proof.total_disk_count, 3);

	mock_disk_read_failed[1] = true;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_IO_UNAVAILABLE);
	(void)unlink(paths[0]);
	(void)unlink(paths[1]);
}

UT_TEST(test_split_mixed_and_equal_order_divergence)
{
	char paths[3][MAXPGPATH];
	char config[MAXPGPATH * 3 + 3];
	ClusterFenceAuthorityProof proof;

	reset_mock();
	three_disk_config(config, paths);
	mock_slots[0][0] = valid_marker(8, 2, 0xAA);
	mock_slots[1][0] = valid_marker(8, 2, 0xBB);
	mock_slots[2][0] = valid_marker(9, 1, 0xCC);
	mock_slot_present[0][0] = true;
	mock_slot_present[1][0] = true;
	mock_slot_present[2][0] = true;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_NO_MAJORITY);

	mock_slots[1][0].version++;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_MIXED_VERSION);
	UT_ASSERT_EQ(cache_invalidation_count, 2);
	mock_slots[1][0].version = CLUSTER_FENCE_MARKER_VERSION;
	mock_slots[0][1] = mock_slots[0][0];
	mock_slots[0][1].fence_event_id++;
	mock_slot_present[0][1] = true;
	UT_ASSERT_EQ(cluster_write_fence_read_durable_authority(&proof),
				 CLUSTER_FENCE_AUTHORITY_CORRUPT);
	UT_ASSERT_EQ(cache_invalidation_count, 3);
	remove_three_disks(paths);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_typed_preconditions_preserve_output);
	UT_RUN(test_strict_config_rejects_empty_and_physical_duplicate);
	UT_RUN(test_durable_majority_and_total_counts);
	UT_RUN(test_join_baseline_majority_replaces_prior_failstop_tuple);
	UT_RUN(test_unreadable_disk_stays_in_denominator);
	UT_RUN(test_split_mixed_and_equal_order_divergence);
	UT_RUN(test_owner_import_runtime_reads_every_distinct_disk);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
