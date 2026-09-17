/*-------------------------------------------------------------------------
 *
 * cluster_control_root.h
 *	  Survivor-readable failed-origin control-root carrier (RF-ROOT P1).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_ROOT_H
#define CLUSTER_CONTROL_ROOT_H

#include "c.h"
#include "common/sha2.h" /* PG_SHA256_DIGEST_LENGTH (contract) */

#define CLUSTER_CONTROL_ROOT_REL_PATH "global/pgrac_control_root"
#define CLUSTER_CONTROL_ROOT_BAK_REL_PATH "global/pgrac_control_root.bak"
#define CLUSTER_CONTROL_ROOT_FILE_BYTES UINT32_C(66048)
#define CLUSTER_CONTROL_ROOT_HEADER_BYTES UINT16_C(512)
#define CLUSTER_CONTROL_ROOT_RECORD_BYTES UINT16_C(512)
#define CLUSTER_CONTROL_ROOT_RECORD_COUNT UINT16_C(128)

#define CLUSTER_CONTROL_ROOT_FORMAT_ROOT_V1 UINT64_C(0x01)
#define CLUSTER_CONTROL_ROOT_FORMAT_R14_BOUND_V1 UINT64_C(0x04)
#define CLUSTER_CONTROL_ROOT_FORMAT_MIGRATION_BINDING_V1 UINT64_C(0x08)
#define CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1 UINT64_C(0x0d)

#define CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID UINT32_C(0x00000001)
#define CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID UINT32_C(0x00000004)
#define CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID UINT32_C(0x00000008)
#define CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID UINT32_C(0x00000010)
#define CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF UINT32_C(0x00000020)
#define CLUSTER_CONTROL_ROOT_FLAG_CONSERVATIVE_SCN_VALID UINT32_C(0x00000040)
#define CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID UINT32_C(0x00000080)
#define CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID UINT32_C(0x00000100)
#define CLUSTER_CONTROL_ROOT_FLAGS_V1 UINT32_C(0x000001fd)

#define CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE UINT64_C(0x01)
#define CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE UINT64_C(0x02)
#define CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT UINT64_C(0x08)
#define CLUSTER_CONTROL_ROOT_PATCH_TAIL UINT64_C(0x10)
#define CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS UINT64_C(0x20)
#define CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY UINT64_C(0x40)
#define CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND UINT64_C(0x80)
#define CLUSTER_CONTROL_ROOT_PATCH_ALL_V1 UINT64_C(0xfb)

#define PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1 (UINT64_C(1) << 17)
#define PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1 (UINT64_C(1) << 18)
#define PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1 (UINT64_C(1) << 19)
#define PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1 (UINT64_C(1) << 21)
#define PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1 (UINT64_C(1) << 22)
#define PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1 (UINT64_C(1) << 23)
#define PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1 (UINT64_C(1) << 24)
/* Bit 0 is the already-frozen R4 synchronous-CR semantic feature.  Keep the
 * complete root-v1 known set public so every reader rejects the same unknown
 * bits; inclusion here is understanding, not activation. */
#define PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1                                                   \
	((UINT64_C(1) << 0) | PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1                                  \
	 | PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1                                              \
	 | PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1                                                \
	 | PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1                                       \
	 | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1                                        \
	 | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1                                               \
	 | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1)

typedef enum ClusterControlRootLifecycle {
	CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED = 0,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN = 1,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED = 2,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE = 3,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED = 4,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED = 5
} ClusterControlRootLifecycle;

typedef enum ClusterControlRootActivationState {
	CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED = 1,
	CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE = 2
} ClusterControlRootActivationState;

typedef enum ClusterControlRootCheckpointSource {
	CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1 = 1,
	CLUSTER_CONTROL_ROOT_CHECKPOINT_RECOVERY_ANCHOR_V1 = 2
} ClusterControlRootCheckpointSource;

typedef enum ClusterControlRootTailValidationKind {
	CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1 = 1
} ClusterControlRootTailValidationKind;

typedef enum ClusterControlRootConservativeBoundKind {
	CLUSTER_CONTROL_ROOT_BOUND_NONE = 0,
	CLUSTER_CONTROL_ROOT_BOUND_R14_M1_PARTITION_S_V1 = 1
} ClusterControlRootConservativeBoundKind;

typedef struct ClusterControlRootIdentity {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint16 origin_thread_id;
	uint16 reserved42;
	int32 origin_node_id;
	int64 thread_claim_created_at;
	uint32 thread_claim_crc32c;
	uint32 reserved60;
	uint64 origin_owner_incarnation;
	uint64 root_lineage_seq;
} ClusterControlRootIdentity;

typedef ClusterControlRootIdentity ClusterRecoveryDutyKey;

typedef struct ClusterControlRootSnapshot {
	ClusterControlRootIdentity identity;
	uint32 lifecycle;
	uint32 root_flags;
	uint64 root_publish_seq;
	uint64 reserved96;
	uint32 checkpoint_tli;
	uint32 tail_tli;
	uint32 recovered_tli;
	uint16 checkpoint_source_kind;
	uint16 tail_validation_kind;
	uint16 conservative_bound_kind;
	uint16 reserved122;
	uint32 reserved124;
	uint64 checkpoint_lower_lsn;
	uint64 validated_tail_lsn_exclusive;
	uint64 recovered_through_lsn_exclusive;
	uint64 conservative_commit_scn;
	uint64 reserved160;
	uint64 tail_last_record_lsn;
	uint64 recovered_last_record_lsn;
	int64 published_at_usec;
	uint32 tail_last_record_crc32c;
	uint32 checkpoint_record_crc32c;
	uint32 recovered_last_record_crc32c;
	uint32 lifecycle_reason;
	uint64 reserved208;
} ClusterControlRootSnapshot;

typedef struct ClusterControlRootReadToken {
	uint8 authority_uuid[16];
	uint16 origin_thread_id;
	uint8 source;
	uint8 lifecycle;
	uint32 reserved20;
	uint64 root_lineage_seq;
	uint64 reserved32;
	uint64 file_txn_seq;
	uint64 root_publish_seq;
	uint32 record_crc32c;
	uint32 root_flags;
} ClusterControlRootReadToken;

typedef struct ClusterControlRootPatch {
	uint64 mask;
	uint32 expected_lifecycle;
	uint32 expected_flags_mask;
	uint32 expected_flags_value;
	uint32 reserved20;
	uint64 reserved24;
	ClusterControlRootSnapshot desired;
} ClusterControlRootPatch;

typedef enum ClusterControlRootReadMode {
	CLUSTER_CONTROL_ROOT_READ_STRONG = 1,
	CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE = 2
} ClusterControlRootReadMode;

typedef enum ClusterControlRootPublishReason {
	CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT = 1,
	CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN = 2,
	CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE = 3,
	CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_DUTY_OPEN = 4,
	CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED = 5,
	CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_PROGRESS = 6,
	CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE = 7,
	CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN = 8,
	CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE = 9,
	CLUSTER_CONTROL_ROOT_PUBLISH_CONSERVATIVE_BOUND = 10,
	CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE = 11,
	CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY = 12,
	CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR = 13
} ClusterControlRootPublishReason;

typedef enum ClusterControlRootResult {
	CLUSTER_CONTROL_ROOT_OK_PRIMARY = 0,
	CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED = 1,
	CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED = 2,
	CLUSTER_CONTROL_ROOT_ABSENT = 3,
	CLUSTER_CONTROL_ROOT_BAD_SIZE = 4,
	CLUSTER_CONTROL_ROOT_BAD_MAGIC = 5,
	CLUSTER_CONTROL_ROOT_BAD_VERSION = 6,
	CLUSTER_CONTROL_ROOT_BAD_ENDIAN = 7,
	CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC = 8,
	CLUSTER_CONTROL_ROOT_BAD_BODY_CRC = 9,
	CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC = 10,
	CLUSTER_CONTROL_ROOT_BAD_RESERVED = 11,
	CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH = 12,
	CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID = 13,
	CLUSTER_CONTROL_ROOT_RANGE_INVALID = 14,
	CLUSTER_CONTROL_ROOT_COPY_DIVERGENT = 15,
	CLUSTER_CONTROL_ROOT_MIXED_VERSION = 16,
	CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE = 17,
	CLUSTER_CONTROL_ROOT_IO_ERROR = 18,
	CLUSTER_CONTROL_ROOT_POSTREAD_FAILED = 19,
	CLUSTER_CONTROL_ROOT_STALE_TOKEN = 20,
	CLUSTER_CONTROL_ROOT_CAS_CONFLICT = 21,
	CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED = 22,
	CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT = 23,
	CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED = 24,
	CLUSTER_CONTROL_ROOT_HASH_MISMATCH = 25,
	CLUSTER_CONTROL_ROOT_MIGRATION_ROUND_MISMATCH = 26,
	CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN = 27
} ClusterControlRootResult;

typedef struct ClusterControlRootMigrationImage {
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	int64 created_at_usec;
	uint32 assigned_record_count;
	uint32 reserved52;
	ClusterControlRootSnapshot records[128];
} ClusterControlRootMigrationImage;

typedef struct ClusterControlRootMigrationRoundV1 {
	uint8 magic[4];
	uint16 version;
	uint16 bytes;
	uint64 prepare_generation;
	uint64 transition_epoch;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint64 admitted_bitmap_low;
	uint64 admitted_bitmap_high;
	uint64 capability_sample_digest;
	uint64 coordinator_incarnation;
	uint32 coordinator_node_id;
	uint32 reserved76;
} ClusterControlRootMigrationRoundV1;

typedef struct ClusterControlRootFileToken {
	uint8 authority_uuid[16];
	uint64 file_txn_seq;
	uint32 body_crc32c;
	uint32 header_crc32c;
	uint32 activation_state;
	uint16 format_version;
	uint16 record_count;
	uint64 system_identifier;
	uint8 image_sha256[32];
} ClusterControlRootFileToken;

StaticAssertDecl(sizeof(ClusterControlRootIdentity) == 80, "ClusterControlRootIdentity ABI");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, system_identifier) == 0,
				 "control-root system identifier offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, storage_uuid) == 8,
				 "control-root storage UUID offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, authority_uuid) == 24,
				 "control-root authority UUID offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, origin_thread_id) == 40,
				 "control-root thread offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, reserved42) == 42,
				 "control-root reserved42 offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, origin_node_id) == 44,
				 "control-root node offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, thread_claim_created_at) == 48,
				 "control-root claim time offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, thread_claim_crc32c) == 56,
				 "control-root claim CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, reserved60) == 60,
				 "control-root reserved60 offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, origin_owner_incarnation) == 64,
				 "control-root owner offset");
StaticAssertDecl(offsetof(ClusterControlRootIdentity, root_lineage_seq) == 72,
				 "control-root lineage offset");
StaticAssertDecl(sizeof(ClusterControlRootSnapshot) == 216, "ClusterControlRootSnapshot ABI");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, lifecycle) == 80,
				 "control-root lifecycle offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, root_flags) == 84,
				 "control-root flags offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, root_publish_seq) == 88,
				 "control-root publish sequence offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, reserved96) == 96,
				 "control-root reserved96 offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, checkpoint_tli) == 104,
				 "control-root checkpoint TLI offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, tail_tli) == 108,
				 "control-root tail TLI offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, recovered_tli) == 112,
				 "control-root recovered TLI offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, checkpoint_source_kind) == 116,
				 "control-root checkpoint source offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, tail_validation_kind) == 118,
				 "control-root tail validation offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, conservative_bound_kind) == 120,
				 "control-root bound kind offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, reserved122) == 122,
				 "control-root reserved122 offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, reserved124) == 124,
				 "control-root reserved124 offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, checkpoint_lower_lsn) == 128,
				 "control-root checkpoint offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, validated_tail_lsn_exclusive) == 136,
				 "control-root tail offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, recovered_through_lsn_exclusive) == 144,
				 "control-root recovered offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, conservative_commit_scn) == 152,
				 "control-root conservative SCN offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, reserved160) == 160,
				 "control-root reserved160 offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, tail_last_record_lsn) == 168,
				 "control-root tail witness offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, recovered_last_record_lsn) == 176,
				 "control-root recovered witness offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, published_at_usec) == 184,
				 "control-root publish time offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, tail_last_record_crc32c) == 192,
				 "control-root tail CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, checkpoint_record_crc32c) == 196,
				 "control-root checkpoint CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, recovered_last_record_crc32c) == 200,
				 "control-root recovered CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, lifecycle_reason) == 204,
				 "control-root lifecycle reason offset");
StaticAssertDecl(offsetof(ClusterControlRootSnapshot, reserved208) == 208,
				 "control-root reserved208 offset");
StaticAssertDecl(sizeof(ClusterControlRootReadToken) == 64, "ClusterControlRootReadToken ABI");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, authority_uuid) == 0,
				 "control-root token authority offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, origin_thread_id) == 16,
				 "control-root token thread offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, source) == 18,
				 "control-root token source offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, lifecycle) == 19,
				 "control-root token lifecycle offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, reserved20) == 20,
				 "control-root token reserved20 offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, root_lineage_seq) == 24,
				 "control-root token lineage offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, reserved32) == 32,
				 "control-root token reserved32 offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, file_txn_seq) == 40,
				 "control-root token txn offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, root_publish_seq) == 48,
				 "control-root token publish offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, record_crc32c) == 56,
				 "control-root token CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootReadToken, root_flags) == 60,
				 "control-root token flags offset");
StaticAssertDecl(sizeof(ClusterControlRootPatch) == 248, "ClusterControlRootPatch ABI");
StaticAssertDecl(offsetof(ClusterControlRootPatch, mask) == 0, "control-root patch mask offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, expected_lifecycle) == 8,
				 "control-root patch lifecycle offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, expected_flags_mask) == 12,
				 "control-root patch flags mask offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, expected_flags_value) == 16,
				 "control-root patch flags value offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, reserved20) == 20,
				 "control-root patch reserved20 offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, reserved24) == 24,
				 "control-root patch reserved24 offset");
StaticAssertDecl(offsetof(ClusterControlRootPatch, desired) == 32,
				 "control-root patch desired offset");
StaticAssertDecl(sizeof(ClusterControlRootMigrationImage) == 27704,
				 "ClusterControlRootMigrationImage ABI");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, system_identifier) == 0,
				 "control-root image system identifier offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, storage_uuid) == 8,
				 "control-root image storage UUID offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, authority_uuid) == 24,
				 "control-root image authority UUID offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, created_at_usec) == 40,
				 "control-root image creation time offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, assigned_record_count) == 48,
				 "control-root image count offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, reserved52) == 52,
				 "control-root image reserved offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationImage, records) == 56,
				 "control-root image records offset");
StaticAssertDecl(sizeof(ClusterControlRootMigrationRoundV1) == 80,
				 "ClusterControlRootMigrationRoundV1 ABI");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, prepare_generation) == 8,
				 "control-root round generation offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, transition_epoch) == 16,
				 "control-root round epoch offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, source_feature_bitmap) == 24,
				 "control-root round source offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, target_feature_bitmap) == 32,
				 "control-root round target offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, admitted_bitmap_low) == 40,
				 "control-root round admitted-low offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, admitted_bitmap_high) == 48,
				 "control-root round admitted-high offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, capability_sample_digest) == 56,
				 "control-root round capability offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, coordinator_incarnation) == 64,
				 "control-root round coordinator offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, coordinator_node_id) == 72,
				 "control-root round coordinator node offset");
StaticAssertDecl(offsetof(ClusterControlRootMigrationRoundV1, reserved76) == 76,
				 "control-root round reserved offset");
StaticAssertDecl(sizeof(ClusterControlRootFileToken) == 80, "ClusterControlRootFileToken ABI");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, authority_uuid) == 0,
				 "control-root file token authority offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, file_txn_seq) == 16,
				 "control-root file token sequence offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, body_crc32c) == 24,
				 "control-root file token body CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, header_crc32c) == 28,
				 "control-root file token header CRC offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, activation_state) == 32,
				 "control-root file token activation offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, format_version) == 36,
				 "control-root file token version offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, record_count) == 38,
				 "control-root file token record count offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, system_identifier) == 40,
				 "control-root file token system identifier offset");
StaticAssertDecl(offsetof(ClusterControlRootFileToken, image_sha256) == 48,
				 "control-root file token hash offset");

extern ClusterControlRootResult cluster_control_root_read_canonical(
	uint16 origin_thread_id, const ClusterControlRootIdentity *expected_identity,
	ClusterControlRootReadMode mode, ClusterControlRootSnapshot *out_snapshot,
	ClusterControlRootReadToken *out_token);
/* contract §A: BOOTSTRAP identity discovery + STRONG bound read (token
 * minted by the STRONG step only). */
/* RF-ROOT P9 verification (implementation): verify the canonical root is ACTIVE and
 * bound to exactly this cutover round (full canonical validation, read-only
 * proof; returns a file token for the caller's freshness binding). */
extern ClusterControlRootResult cluster_control_root_bootstrap_validate_active_round(
	const ClusterControlRootMigrationRoundV1 *round, ClusterControlRootFileToken *token);

/* RF-ROOT P9 verification (cold-formation): member-side field binding —
 * ACTIVE root bound to the round identity a member holds (epoch /
 * prepare-generation / source+target bitmaps) + non-zero round sha.  The
 * full round + its sha are coordinator-local (seam shmem), so members
 * cannot recompute round_sha256; the identity fields + the root's own
 * CRC/dual-copy validation bind the round. */
extern ClusterControlRootResult cluster_control_root_bootstrap_validate_active_round_fields(
	uint64 transition_epoch, uint64 prepare_generation, uint64 source_feature_bitmap,
	uint64 target_feature_bitmap);

/* RF-ROOT P9 verification (contract): re-arm the bit22 cutover latch from a
 * durable ACTIVE root across a postmaster restart.  Returns whether the
 * dual-path gate reads as post-bit22 afterwards. */
extern bool cluster_control_root_restore_bit22_latch_if_active(void);

extern ClusterControlRootResult
cluster_control_root_read_canonical_discovered(uint16 origin_thread_id,
											   ClusterControlRootSnapshot *out_snapshot,
											   ClusterControlRootReadToken *out_token);
/* Stage 8 contract (verified implementation): lock-free canonical read for a DEAD
 * origin's thread — no live writer exists (the only publisher was the dead
 * postmaster) and the GRD recovery adopts each shard to exactly one
 * survivor, so the STRONG read's CF-share-lock race guard is unnecessary.
 * Used by the post-bit22 hw-remaster path while the GRD recovery holds the
 * CF shard FROZEN (CF(S) itself unavailable), breaking the remaster ->
 * unfreeze -> CF-shard-NORMAL deadlock. */
extern ClusterControlRootResult
cluster_control_root_read_canonical_dead_origin(uint16 origin_thread_id,
												ClusterControlRootSnapshot *out_snapshot);
extern ClusterControlRootResult cluster_control_root_lookup_owner_by_node_runtime(
	int32 old_node_id, ClusterControlRootIdentity *out_identity,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token);
extern ClusterControlRootResult cluster_control_root_compare_and_publish(
	const ClusterControlRootReadToken *expected_token, const ClusterControlRootPatch *patch,
	ClusterControlRootPublishReason reason, ClusterControlRootSnapshot *out_snapshot,
	ClusterControlRootReadToken *out_token);
extern ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootSnapshot *out_snapshot);
extern bool cluster_control_root_identity_equal(const ClusterControlRootIdentity *left,
												const ClusterControlRootIdentity *right);
extern bool cluster_control_root_feature_bitmap_is_known(uint64 active_feature_bitmap);
extern ClusterControlRootResult
cluster_control_root_create_prepared(const ClusterControlRootMigrationImage *image,
									 const ClusterControlRootMigrationRoundV1 *round,
									 ClusterControlRootFileToken *out_token);
extern ClusterControlRootResult cluster_control_root_activate_prepared(
	const ClusterControlRootFileToken *expected_token, const uint8 expected_round_sha256[32],
	const ClusterControlRootMigrationRoundV1 *round, ClusterControlRootFileToken *out_token);
extern bool cluster_control_root_round_sha256(const ClusterControlRootMigrationRoundV1 *round,
											  uint8 out_sha[PG_SHA256_DIGEST_LENGTH]);
/* contract step ④d: construct the create_prepared migration image from the
 * live registry + claims + membership (coordinator side). */
extern ClusterControlRootResult
cluster_control_root_build_migration_image(const ClusterControlRootMigrationRoundV1 *round,
										   ClusterControlRootMigrationImage *out);
extern ClusterControlRootResult
cluster_control_root_discard_inactive(const ClusterControlRootFileToken *expected_token,
									  const uint8 expected_round_sha256[32]);

#endif /* CLUSTER_CONTROL_ROOT_H */
