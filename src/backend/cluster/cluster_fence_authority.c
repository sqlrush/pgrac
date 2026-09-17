/*-------------------------------------------------------------------------
 *
 * cluster_fence_authority.c
 *	  STOP-02 direct durable formation-authority proof (RF-ROOT P2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <sys/stat.h>

#include "utils/wait_event.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_recovery_duty.h"
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_write_fence.h"

typedef struct ClusterFenceDiskTarget {
	char path[MAXPGPATH];
	int fd;
	bool stat_valid;
	struct stat st;
} ClusterFenceDiskTarget;

static ClusterFenceAuthorityReadResult
fence_authority_fail(ClusterFenceAuthorityReadResult result)
{
	cluster_write_fence_authority_cache_invalidate();
	return result;
}

static ClusterFenceAuthorityReadResult
fence_parse_disk_config(ClusterFenceDiskTarget targets[CLUSTER_MAX_VOTING_DISKS], int *n_out)
{
	const char *p = cluster_voting_disks;
	int n = 0;
	int i;

	if (p == NULL || p[0] == '\0')
		return CLUSTER_FENCE_AUTHORITY_NO_CONFIG;
	for (;;) {
		const char *start = p;
		const char *end;
		size_t len;

		while (*p != '\0' && *p != ',')
			p++;
		end = p;
		while (start < end && (*start == ' ' || *start == '\t'))
			start++;
		while (end > start && (end[-1] == ' ' || end[-1] == '\t'))
			end--;
		len = (size_t)(end - start);
		if (len == 0 || len >= MAXPGPATH || n >= CLUSTER_MAX_VOTING_DISKS)
			return CLUSTER_FENCE_AUTHORITY_BAD_CONFIG;
		memcpy(targets[n].path, start, len);
		targets[n].path[len] = '\0';
		targets[n].fd = -1;
		targets[n].stat_valid = false;
		for (i = 0; i < n; i++)
			if (strcmp(targets[i].path, targets[n].path) == 0)
				return CLUSTER_FENCE_AUTHORITY_BAD_CONFIG;
		n++;
		if (*p == '\0')
			break;
		p++;
	}
	*n_out = n;
	return CLUSTER_FENCE_AUTHORITY_OK;
}

static bool
fence_targets_same_physical(const struct stat *a, const struct stat *b)
{
	bool a_device = S_ISBLK(a->st_mode) || S_ISCHR(a->st_mode);
	bool b_device = S_ISBLK(b->st_mode) || S_ISCHR(b->st_mode);

	if (S_ISREG(a->st_mode) && S_ISREG(b->st_mode))
		return a->st_dev == b->st_dev && a->st_ino == b->st_ino;
	if (a_device && b_device)
		return a->st_rdev == b->st_rdev;
	return false;
}

static void
fence_close_targets(ClusterFenceDiskTarget targets[CLUSTER_MAX_VOTING_DISKS], int n)
{
	int i;

	for (i = 0; i < n; i++)
		if (targets[i].fd >= 0) {
			cluster_voting_disk_close(targets[i].fd);
			targets[i].fd = -1;
		}
}

ClusterFenceAuthorityReadResult
cluster_write_fence_read_durable_authority(ClusterFenceAuthorityProof *out)
{
	ClusterFenceDiskTarget targets[CLUSTER_MAX_VOTING_DISKS];
	ClusterFenceMarker disk_markers[CLUSTER_MAX_VOTING_DISKS];
	ClusterFenceDiskVoteState disk_states[CLUSTER_MAX_VOTING_DISKS];
	ClusterFenceAuthorityReadResult result;
	int n_total = 0;
	int i;

	if (out == NULL)
		return CLUSTER_FENCE_AUTHORITY_BAD_ARGUMENT;
	if (cluster_write_fence_enforcement != CLUSTER_WRITE_FENCE_ENFORCE_ON)
		return fence_authority_fail(CLUSTER_FENCE_AUTHORITY_ENFORCEMENT_OFF);
	memset(targets, 0, sizeof(targets));
	memset(disk_markers, 0, sizeof(disk_markers));
	for (i = 0; i < CLUSTER_MAX_VOTING_DISKS; i++)
		targets[i].fd = -1;
	result = fence_parse_disk_config(targets, &n_total);
	if (result != CLUSTER_FENCE_AUTHORITY_OK)
		return fence_authority_fail(result);

	for (i = 0; i < n_total; i++) {
		int j;

		disk_states[i] = CLUSTER_FENCE_DISK_VOTE_UNREADABLE;
		targets[i].fd = cluster_voting_disk_open(targets[i].path, false);
		if (targets[i].fd < 0 || fstat(targets[i].fd, &targets[i].st) != 0)
			continue;
		if (!S_ISREG(targets[i].st.st_mode) && !S_ISBLK(targets[i].st.st_mode)
			&& !S_ISCHR(targets[i].st.st_mode)) {
			fence_close_targets(targets, n_total);
			return fence_authority_fail(CLUSTER_FENCE_AUTHORITY_BAD_CONFIG);
		}
		targets[i].stat_valid = true;
		for (j = 0; j < i; j++)
			if (targets[j].stat_valid
				&& fence_targets_same_physical(&targets[i].st, &targets[j].st)) {
				fence_close_targets(targets, n_total);
				return fence_authority_fail(CLUSTER_FENCE_AUTHORITY_BAD_CONFIG);
			}
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WRITE_FENCE_VERIFY);
	for (i = 0; i < n_total; i++) {
		ClusterFenceMarker slot_markers[CLUSTER_MAX_NODES];
		bool outer_crc_valid[CLUSTER_MAX_NODES];
		bool disk_failed = false;
		uint32 node;

		memset(slot_markers, 0, sizeof(slot_markers));
		memset(outer_crc_valid, 0, sizeof(outer_crc_valid));
		if (targets[i].fd < 0 || !targets[i].stat_valid)
			continue;
		for (node = 0; node < CLUSTER_MAX_NODES; node++) {
			ClusterVotingSlot slot;
			ClusterVotingDiskIoState io_state;

			io_state = cluster_voting_disk_read_slot(targets[i].fd, i, node, &slot);
			if (io_state == CLUSTER_VOTING_DISK_IO_OK) {
				memcpy(&slot_markers[node], slot._reserved1, sizeof(ClusterFenceMarker));
				outer_crc_valid[node] = true;
			} else if (io_state != CLUSTER_VOTING_DISK_IO_TORN) {
				disk_failed = true;
				break;
			}
		}
		if (!disk_failed)
			disk_states[i] = cluster_fence_disk_vote_select_v1(slot_markers, outer_crc_valid,
															   CLUSTER_MAX_NODES, &disk_markers[i]);
	}
	pgstat_report_wait_end();
	fence_close_targets(targets, n_total);

	result = cluster_fence_authority_prove_v1(disk_markers, disk_states, n_total, out);
	return result == CLUSTER_FENCE_AUTHORITY_OK ? result : fence_authority_fail(result);
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
									  uint64 frozen_admitted_bitmap_low,
									  uint64 frozen_admitted_bitmap_high, uint64 *out_incarnation)
{
	ClusterFenceDiskTarget targets[CLUSTER_MAX_VOTING_DISKS];
	ClusterRecoveryOwnerDiskSampleV1 samples[CLUSTER_MAX_VOTING_DISKS];
	ClusterFenceAuthorityReadResult parse_result;
	int n_total = 0;
	int i;

	if (out_incarnation != NULL)
		*out_incarnation = 0;
	if (out_incarnation == NULL || immutable_claim == NULL || node_id < 0
		|| node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_ARGUMENT;
	if (cluster_write_fence_enforcement != CLUSTER_WRITE_FENCE_ENFORCE_ON)
		return CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE;
	memset(targets, 0, sizeof(targets));
	memset(samples, 0, sizeof(samples));
	for (i = 0; i < CLUSTER_MAX_VOTING_DISKS; i++) {
		targets[i].fd = -1;
		samples[i].join_io_state = CLUSTER_VOTING_DISK_IO_FAILED;
		samples[i].slot_io_state = CLUSTER_VOTING_DISK_IO_FAILED;
	}
	parse_result = fence_parse_disk_config(targets, &n_total);
	if (parse_result == CLUSTER_FENCE_AUTHORITY_NO_CONFIG)
		return CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE;
	if (parse_result != CLUSTER_FENCE_AUTHORITY_OK)
		return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_CONFIG;
	for (i = 0; i < n_total; i++) {
		int j;

		targets[i].fd = cluster_voting_disk_open(targets[i].path, false);
		if (targets[i].fd < 0 || fstat(targets[i].fd, &targets[i].st) != 0)
			continue;
		if (!S_ISREG(targets[i].st.st_mode) && !S_ISBLK(targets[i].st.st_mode)
			&& !S_ISCHR(targets[i].st.st_mode)) {
			fence_close_targets(targets, n_total);
			return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_CONFIG;
		}
		targets[i].stat_valid = true;
		for (j = 0; j < i; j++)
			if (targets[j].stat_valid
				&& fence_targets_same_physical(&targets[i].st, &targets[j].st)) {
				fence_close_targets(targets, n_total);
				return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_CONFIG;
			}
	}
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_WRITE_FENCE_VERIFY);
	for (i = 0; i < n_total; i++) {
		union {
			ClusterJoinCommitMarker marker;
			uint8 bytes[CLUSTER_VOTING_SLOT_BYTES];
		} join_slot;

		if (targets[i].fd < 0 || !targets[i].stat_valid)
			continue;
		memset(&join_slot, 0, sizeof(join_slot));
		samples[i].join_io_state
			= cluster_voting_disk_read_join_slot(targets[i].fd, (uint32)node_id, join_slot.bytes);
		if (samples[i].join_io_state == CLUSTER_VOTING_DISK_IO_OK)
			samples[i].join_marker = join_slot.marker;
		samples[i].slot_io_state
			= cluster_voting_disk_read_slot(targets[i].fd, i, (uint32)node_id, &samples[i].slot);
	}
	pgstat_report_wait_end();
	fence_close_targets(targets, n_total);
	return cluster_recovery_owner_import_select_v1(
		node_id, immutable_claim, frozen_admitted_bitmap_low, frozen_admitted_bitmap_high, samples,
		n_total, out_incarnation);
}

#endif /* USE_PGRAC_CLUSTER */
