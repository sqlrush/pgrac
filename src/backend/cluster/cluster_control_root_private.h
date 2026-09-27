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
#include "cluster/cluster_wal_durable_prefix.h"

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

/* PGRAC: bounded retained-writer input, never an authority/retirement proof.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define CLUSTER_WAL_HISTORY_MAX_RECORDS 128
#define CLUSTER_WAL_HISTORY_HEADER_BYTES 64
#define CLUSTER_WAL_HISTORY_MAX_BYTES (64 + 128 * 512 + 4)
typedef struct ClusterWalHistoryRecord {
	ClusterControlRootSnapshot snapshot;
	ControlRootRecordRefsV2 refs;
	uint64 publisher_incarnation;
	uint32 publisher_node;
	uint32 record_crc32c;
} ClusterWalHistoryRecord;

typedef struct ClusterWalHistoryImage {
	uint32 count;
	ClusterWalHistoryRecord records[CLUSTER_WAL_HISTORY_MAX_RECORDS];
} ClusterWalHistoryImage;

/* Root is already decoded v2; its exact origin record selects the object.
 * No I/O, sorting, repair or admission. Caller owns hash resources. Inputs must
 * not overlap out. Every refusal clears out, including rejected aliases.
 */
extern ClusterControlRootResult
cluster_control_root_v2_history_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalHistoryImage *out);

/* PGRAC: canonical retained set production, not set-completeness or JOIN
 * authority. Inputs must not overlap outputs. Refusal clears both outputs.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_control_root_v2_history_encode(const ControlRootImage *root, uint32 origin_node,
									   const ClusterWalHistoryImage *history,
									   uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], size_t *length);

/* Process-owned immutable file installation. The enclosing root publisher
 * must retain the same clusterwide CF-X through root CAS. No formal object
 * is ever removed by discard, including after an ambiguous install failure.
 */
typedef struct ClusterWalHistoryStage {
	uint64 generation;
	uint64 system_identifier;
	uint64 current_owner_incarnation;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint8 sha256[32];
	uint8 operation_uuid[16];
	uint64 object_dir_dev;
	uint64 object_dir_ino;
	uint64 staging_dir_dev;
	uint64 staging_dir_ino;
	uint64 file_dev;
	uint64 file_ino;
	uint32 origin_node;
	uint32 length;
	uint32 owner_pid;
	uint32 state;
} ClusterWalHistoryStage;

StaticAssertDecl(sizeof(ClusterWalHistoryStage) == 168, "history staging descriptor size");

extern ClusterControlRootResult
cluster_wal_history_prepare(const ControlRootImage *root, uint32 origin_node,
							const ClusterWalHistoryImage *history, uint64 generation,
							const uint8 operation_uuid[16], ClusterWalHistoryStage *out);
extern ClusterControlRootResult cluster_wal_history_install(ClusterWalHistoryStage *stage);
extern ClusterControlRootResult cluster_wal_history_discard(ClusterWalHistoryStage *stage);

/* Exact read-only consumption under existing clusterwide CF-S/X. No staging
 * directory or write permission required; refusal clears the whole output. */
extern ClusterControlRootResult cluster_wal_history_read_locked(const ControlRootImage *root,
																uint32 origin_node,
																ClusterWalHistoryImage *out);

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

/* PGRAC: root-selected COMMON image plus exact configuration-object binding,
 * not a per-thread startup projection or GUC-application acknowledgement.
 * Requires an already held clusterwide CF-S/X and verified storage identity.
 * No publication, migration, repair or serving admission. All three outputs
 * are required and cleared on refusal; inputs/outputs must not overlap.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_control_root_v2_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token);

/* Exact thread projection, with the same held-lock/output contract. This
 * still does not perform startup, serving or recovery-completion admission.
 */
extern ClusterControlRootResult
cluster_control_root_v2_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token);

/* Runtime local owner only. Borrow the caller's CF-S/X; not early startup. */
extern ClusterControlRootResult
cluster_control_root_v2_read_runtime_local_locked(ControlFileData *out);

/* PGRAC: normal current-writer checkpoint retention only. Owns CF-S and
 * revalidates actual local runtime identity; does not read/retire historical
 * writers or grant recovery/serving permission. Outputs clear on refusal;
 * an unconfirmed owned CF release is FATAL, never a retain-and-continue result.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_control_root_v2_read_retention_current(const ClusterControlRootIdentity *self,
											   ClusterControlRootSnapshot *out,
											   ClusterControlRootReadToken *token);

/* Backend-private compatibility output only. Caller just read the selected
 * thread from current root under its held CF-X. Never an authority writer.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_cf_control_projection_write_locked(const ControlFileData *selected);

/* PGRAC: normal online checkpoint only. Actual checkpointer/membership/fence
 * and local WAL flush facts authorize this operation, never caller booleans.
 * Owns CF-S read, outside-X staging, WALR-before-CF-X, and exact file CAS.
 * The checkpoint end comes from native XLogInsert/Flush; the publisher derives
 * its CRC from actual exact-generation WAL, including a valid zero checksum.
 * Outputs clear on refusal; a failed post-publication check does not undo root.
 * Startup, shutdown, migration and native compatibility projection are separate
 * adapter responsibilities. Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult cluster_control_root_v2_checkpoint_publish(
	const ClusterControlRootIdentity *self, const ControlFileData *thread_control,
	XLogRecPtr checkpoint_end, ClusterControlRootSnapshot *out,
	ClusterControlRootFileToken *out_token, ControlFileData *out_control);

/* PGRAC: publishes WAL-verified shutdown checkpoint evidence only. Does not
 * close a thread/database or change membership; returned native view still
 * obeys the selected root lifecycle. Normal-stop/close admission is separate.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult cluster_control_root_v2_shutdown_checkpoint_publish(
	const ClusterControlRootIdentity *self, const ControlFileData *thread_control,
	XLogRecPtr checkpoint_end, ClusterControlRootSnapshot *out,
	ClusterControlRootFileToken *out_token, ControlFileData *out_control);

/* PGRAC: own checkpointer only; reads the exact root-selected shutdown WAL
 * and durable prefix. No lifecycle/serving/registry write. Not clean-close.
 * Author: SqlRush <sqlrush@gmail.com> */
extern ClusterControlRootResult
cluster_control_root_v2_shutdown_observe(const ClusterWalDurablePrefixRef *expected,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *out_token);

struct ClusterPhase1FullStopPlan;
/* Only the actual post-checkpoint normal-stop controller may own this call.
 * Success includes exact CF/WALR retirement, not merely durable root bytes. */
extern ClusterControlRootResult
cluster_control_root_v2_normal_stop_close(const struct ClusterPhase1FullStopPlan *plan,
										  bool *all_closed);

/* PGRAC: normal-stop observation only, not thread close or PI-retirement
 * permission. Authenticate the raw selected anchor and exact durable prefix
 * under CF-S. LMON/LMS poll the original release; unavailable clears output.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef enum ClusterControlRootStopPhase {
	CLUSTER_CONTROL_ROOT_STOP_UNKNOWN = 0,
	CLUSTER_CONTROL_ROOT_STOP_ACTIVE,
	CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT,
	CLUSTER_CONTROL_ROOT_STOP_CLOSED
} ClusterControlRootStopPhase;

/* A durable close is a successor of the checkpoint observation, never a
 * new writer grant. Pre-checkpoint consumers still require ACTIVE. */
static inline bool
cluster_control_root_stop_after_checkpoint(ClusterControlRootStopPhase phase)
{
	return phase == CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT
		   || phase == CLUSTER_CONTROL_ROOT_STOP_CLOSED;
}

typedef struct ClusterControlRootStopObservation {
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootStopPhase phase;
	/* One CF interval; zero entries are not serving observations. Not cached
	 * authority and not a replacement for the consumer's formation check. */
	struct {
		uint64 incarnation;
		int64 claim_created_at;
		ClusterControlRootStopPhase phase;
	} members[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
} ClusterControlRootStopObservation;

extern ClusterControlRootResult
cluster_control_root_v2_stop_phase_read(uint16 thread, uint64 admitted_incarnation,
										ClusterControlRootStopObservation *out);

/* PGRAC: exact failed-writer control publishers. The request is a carrier,
 * not authority: each operation authenticates its opaque formation/fence
 * owners and the current physical v2 root. OPEN clears the checkpoint's old
 * tail; SEAL owns WALR-S and input-only IR-X, scans physical WAL/PGWP and
 * publishes the final input. Neither operation replays data or opens serving.
 * All owned releases must be confirmed before outputs become usable.
 * Author: SqlRush <sqlrush@gmail.com> */
struct ClusterRecoverySerialRequest;
extern ClusterControlRootResult
cluster_control_root_v2_failure_open_publish(const struct ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token);
extern ClusterControlRootResult
cluster_control_root_v2_failure_tail_publish(const struct ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token);

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
