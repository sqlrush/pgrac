/*-------------------------------------------------------------------------
 *
 * cluster_recovery_duty.h
 *	  No-generation failed-origin recovery-duty identity (RF-ROOT P2).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_DUTY_H
#define CLUSTER_RECOVERY_DUTY_H

#include "cluster/cluster_control_root.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"

#define CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES 74
#define CLUSTER_RECOVERY_DUTY_DIGEST_BYTES 32

typedef struct ClusterRecoveryDutyDigest {
	uint8 bytes[CLUSTER_RECOVERY_DUTY_DIGEST_BYTES];
} ClusterRecoveryDutyDigest;

typedef enum ClusterRecoveryDutyCompare {
	CLUSTER_RECOVERY_DUTY_COMPARE_EXACT = 0,
	CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT = 1,
	CLUSTER_RECOVERY_DUTY_COMPARE_INVALID = 2
} ClusterRecoveryDutyCompare;

typedef enum ClusterRecoveryOwnerImportResult {
	CLUSTER_RECOVERY_OWNER_IMPORT_JCMK = 0,
	CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT = 1,
	CLUSTER_RECOVERY_OWNER_IMPORT_BAD_ARGUMENT = 2,
	CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH = 3,
	CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN = 4,
	CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN = 5,
	CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED = 6,
	CLUSTER_RECOVERY_OWNER_IMPORT_CAPABILITY_UNAVAILABLE = 7,
	CLUSTER_RECOVERY_OWNER_IMPORT_BAD_CONFIG = 8
} ClusterRecoveryOwnerImportResult;

typedef struct ClusterRecoveryOwnerDiskSampleV1 {
	ClusterVotingDiskIoState join_io_state;
	ClusterJoinCommitMarker join_marker;
	ClusterVotingDiskIoState slot_io_state;
	ClusterVotingSlot slot;
} ClusterRecoveryOwnerDiskSampleV1;

typedef struct ClusterFormationWitnessV1 ClusterFormationWitnessV1;

typedef enum ClusterFormationWitnessResult {
	CLUSTER_FORMATION_WITNESS_READY = 0,
	CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT = 1,
	CLUSTER_FORMATION_WITNESS_UNSTABLE = 2,
	CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN = 3,
	CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED = 4,
	CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH = 5,
	CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED = 6,
	CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE = 7,
	CLUSTER_FORMATION_WITNESS_IO_FAILED = 8,
	CLUSTER_FORMATION_WITNESS_CORRUPT = 9
} ClusterFormationWitnessResult;

/* Internal canonical snapshot captured under the reconfig lock. */
typedef struct ClusterFormationSnapshotV1 {
	ReconfigEvent applied;
	ClusterMembershipTable membership;
	uint8 pending_join_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 clean_departed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 removed_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 excluded_bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint64 local_epoch;
	uint64 victim_incarnation;
	uint32 prebump_sync_active;
	uint8 self_join_admitted;
	uint8 self_join_failed;
	uint8 reserved[2];
} ClusterFormationSnapshotV1;

/* AD-023 recovery-control classification tag.  Captured runtime snapshots
 * always keep reserved[] zero; only the opaque phase-3 witness copy may carry
 * this local tag.  It authorizes no serving or write operation. */
#define CLUSTER_FORMATION_SNAPSHOT_RECOVERY_CONTROL UINT8_C(1)

StaticAssertDecl(sizeof(ClusterRecoveryDutyDigest) == 32, "ClusterRecoveryDutyDigest ABI");

/* Shared pure validity predicate for every consumer of the exact duty key.
 * Keeping this beside the canonical encoder prevents compact resource ids
 * from accepting an identity that the durable/root layer would reject. */
static inline bool
cluster_recovery_duty_key_valid_v1(const ClusterRecoveryDutyKey *key)
{
	ClusterWalThreadClaim claim;
	bool storage_nonzero = false;
	bool authority_nonzero = false;
	int i;

	if (key == NULL || key->system_identifier == 0)
		return false;
	for (i = 0; i < 16; i++) {
		storage_nonzero = storage_nonzero || key->storage_uuid[i] != 0;
		authority_nonzero = authority_nonzero || key->authority_uuid[i] != 0;
	}
	if (!storage_nonzero || !authority_nonzero
		|| (key->authority_uuid[6] & UINT8_C(0xf0)) != UINT8_C(0x40)
		|| (key->authority_uuid[8] & UINT8_C(0xc0)) != UINT8_C(0x80) || key->origin_thread_id == 0
		|| key->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT || key->origin_node_id < 0
		|| key->origin_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| key->origin_thread_id != (uint16)(key->origin_node_id + 1) || key->reserved42 != 0
		|| key->thread_claim_created_at == 0 || key->thread_claim_crc32c == 0
		|| key->reserved60 != 0 || key->origin_owner_incarnation == 0 || key->root_lineage_seq == 0)
		return false;
	cluster_wal_thread_claim_fill(&claim, key->origin_thread_id, key->origin_node_id,
								  key->thread_claim_created_at);
	return key->thread_claim_crc32c == claim.crc;
}

extern bool cluster_recovery_duty_key_encode_v1(const ClusterRecoveryDutyKey *key,
												uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES]);
extern ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed);
extern bool cluster_recovery_duty_digest_v1(const ClusterRecoveryDutyKey *key,
											ClusterRecoveryDutyDigest *out);
extern ClusterRecoveryOwnerImportResult cluster_recovery_owner_import_select_v1(
	int32 node_id, const ClusterWalThreadClaim *immutable_claim, uint64 frozen_admitted_bitmap_low,
	uint64 frozen_admitted_bitmap_high, const ClusterRecoveryOwnerDiskSampleV1 *samples,
	int total_disk_count, uint64 *out_incarnation);
extern ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_read_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
									  uint64 frozen_admitted_bitmap_low,
									  uint64 frozen_admitted_bitmap_high, uint64 *out_incarnation);
extern bool cluster_recovery_owner_rejoin_v1(int32 node_id, uint64 admitted_incarnation);
extern bool cluster_control_root_thread_clean_close_publish(void); /* RF-ROOT P6 */
extern bool
cluster_control_root_thread_clean_close_publish_retry(void); /* RF-ROOT P7 recovery path */
extern bool cluster_control_root_thread_open_publish(uint64 boot_incarnation); /* RF-ROOT P6 */
extern bool cluster_control_root_checkpoint_advance_publish(
	XLogRecPtr redo, TimeLineID tli, XLogRecPtr ckpt_record_start, XLogRecPtr ckpt_record_end,
	uint32 record_crc32c);								   /* RF-ROOT P7 G1a */
extern bool cluster_control_root_fpw_sticky_publish(void); /* RF-ROOT P7 G1a-2 */
extern ClusterFormationWitnessResult cluster_formation_witness_decide_v1(
	const ClusterFormationSnapshotV1 *f1, const ClusterFenceAuthorityProof *authority,
	const ClusterFormationSnapshotV1 *f2, uint16 origin_thread, bool opening_new_duty);
extern ClusterFormationWitnessResult
cluster_formation_witness_build_wait(uint16 origin_thread, bool opening_new_duty, int timeout_ms,
									 ClusterFormationWitnessV1 **out);
extern ClusterFormationWitnessResult
cluster_formation_witness_build_live_wait(uint16 origin_thread, int timeout_ms,
										  ClusterFormationWitnessV1 **out);
extern ClusterFormationWitnessResult
cluster_formation_witness_build_recovery_control_wait(uint16 origin_thread, int timeout_ms,
													  ClusterFormationWitnessV1 **out);
extern ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness);
extern ClusterFormationWitnessResult
cluster_formation_classification_revalidate_nowait(uint16 origin_thread,
												   const ClusterFenceAuthorityProof *authority,
												   const ClusterFormationSnapshotV1 *snapshot);
extern bool cluster_formation_snapshot_matches_v1(const ClusterFormationSnapshotV1 *expected,
												  const ClusterFormationSnapshotV1 *observed);
/* Copy the immutable classification already owned by an admitted witness.
 * This performs no current-state read; callers must separately revalidate the
 * same opaque witness before consuming the copy. */
extern bool cluster_formation_witness_copy_classification_v1(
	const ClusterFormationWitnessV1 *witness, uint16 *origin_thread,
	ClusterFenceAuthorityProof *authority, ClusterFormationSnapshotV1 *snapshot);
extern const ClusterFenceAuthorityProof *
cluster_formation_witness_authority(const ClusterFormationWitnessV1 *witness);
extern void cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness);

#endif /* CLUSTER_RECOVERY_DUTY_H */
