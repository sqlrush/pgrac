/*-------------------------------------------------------------------------
 *
 * cluster_control_root_private.h
 *	  Backend-private publication authority seam (RF-ROOT P5).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_ROOT_PRIVATE_H
#define CLUSTER_CONTROL_ROOT_PRIVATE_H

#include "catalog/pg_control.h"
#include "cluster/cluster_control_root.h"

/* PGRAC: backend-private decoded carrier, never a disk struct or permission.
 * Keep the public identity/snapshot/token ABI unchanged.  Both v1 and v2
 * use the same field validators, but each I/O consumer selects its version.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ControlRootCommonV2 {
	uint32 database_state;
	uint32 reserved4;
	uint64 database_incarnation;
	uint64 formation_seq;
	uint64 configured[2];
	uint64 serving[2];
	uint64 config_generation;
	uint8 config_sha256[PG_SHA256_DIGEST_LENGTH];
	uint64 control_image_generation;
	uint8 control_image_sha256[PG_SHA256_DIGEST_LENGTH];
	uint64 catalog_manifest_generation;
	uint64 global_scn_high_water;
	uint8 catalog_manifest_sha256[PG_SHA256_DIGEST_LENGTH];
} ControlRootCommonV2;

typedef struct ControlRootRecordRefsV2 {
	uint64 history_generation;
	uint8 history_sha256[PG_SHA256_DIGEST_LENGTH];
	uint64 anchor_generation;
	uint8 anchor_sha256[PG_SHA256_DIGEST_LENGTH];
	uint8 claim_sha256[PG_SHA256_DIGEST_LENGTH];
} ControlRootRecordRefsV2;

typedef struct ControlRootHeader {
	uint64 file_txn_seq;
	uint64 system_identifier;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint32 activation_state;
	int64 created_at_usec;
	int64 published_at_usec;
	uint32 body_crc32c;
	uint8 migration_round_sha256[PG_SHA256_DIGEST_LENGTH];
	uint8 source_wal_state_sha256[PG_SHA256_DIGEST_LENGTH];
	uint64 migration_prepare_generation;
	uint64 migration_transition_epoch;
	uint64 source_feature_bitmap;
	uint64 target_feature_bitmap;
	uint32 header_crc32c;
	uint16 format_version;
	ControlRootCommonV2 v2;
} ControlRootHeader;

typedef struct ControlRootImage {
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	ControlRootHeader header;
	ClusterControlRootSnapshot records[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	ControlRootRecordRefsV2 refs[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint64 publisher_incarnation[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint32 publisher_node[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint32 record_crc32c[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	bool present[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
} ControlRootImage;

StaticAssertDecl(sizeof(ControlRootCommonV2) == 184, "root-v2 logical common carrier");
StaticAssertDecl(sizeof(ControlRootRecordRefsV2) == 112, "root-v2 logical record references");

/* Memory-only codec.  No file I/O, migration, publication or startup authority.
 * Decode refuses v1 and clears the entire output on failure.  Encode emits
 * canonical v2 bytes from the decoded fields, or clears bytes on failure.
 * Input bytes may be out->bytes; the result must not alias the expected UUID.
 */
extern ClusterControlRootResult cluster_control_root_v2_decode(const uint8 *bytes, size_t len,
															   const uint8 storage_uuid[16],
															   uint64 system_identifier,
															   ControlRootImage *out);
extern ClusterControlRootResult cluster_control_root_v2_encode(ControlRootImage *image);

/* PGRAC: root-selected COMMON image only, not a per-thread startup projection.
 * Requires an already held clusterwide CF-S/X and verified storage identity.
 * No publication, migration, repair or serving admission. All three outputs
 * are required and cleared on refusal; inputs/outputs must not overlap.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_control_root_v2_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token);

extern bool
cluster_control_root_create_authority_current_v1(const ClusterControlRootMigrationImage *image,
												 const ClusterControlRootMigrationRoundV1 *round);
extern bool cluster_control_root_activate_authority_current_v1(
	const ClusterControlRootFileToken *expected_token, const uint8 expected_round_sha256[32],
	const ClusterControlRootMigrationRoundV1 *round);
extern bool
cluster_control_root_publish_authority_current_v1(const ClusterControlRootReadToken *expected_token,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootPublishReason reason);
extern ClusterControlRootResult cluster_control_root_recovery_complete_publish_v1(
	const ClusterControlRootReadToken *expected_token, const ClusterControlRootPatch *patch,
	ClusterControlRootSnapshot *out_snapshot, ClusterControlRootReadToken *out_token);

#endif /* CLUSTER_CONTROL_ROOT_PRIVATE_H */
