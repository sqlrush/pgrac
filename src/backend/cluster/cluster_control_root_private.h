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
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_startup_exit.h"

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

/* PGRAC: explicit v3 extension, separate from the unchanged v2 carrier.
 * A reference is pending work, never startup or serving permission.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct ControlRootStartupRefV3 {
	uint64 generation;
	uint8 sha256[PG_SHA256_DIGEST_LENGTH];
} ControlRootStartupRefV3;

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
	ControlRootStartupRefV3 startup[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint64 publisher_incarnation[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint32 publisher_node[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	uint32 record_crc32c[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
	bool present[CLUSTER_CONTROL_ROOT_RECORD_COUNT];
} ControlRootImage;

StaticAssertDecl(sizeof(ControlRootCommonV2) == 184, "root-v2 logical common carrier");
StaticAssertDecl(sizeof(ControlRootRecordRefsV2) == 112, "root-v2 logical record references");
StaticAssertDecl(sizeof(ControlRootStartupRefV3) == 40, "root-v3 logical startup reference");

/* Derive the complete clean-restart observation cut from literal root-v3
 * bytes and an exact captured formation. No I/O, peer substitution, serving
 * or reservation. Caller must use a CF-held primary and revalidate the same
 * root/formation/provider under the actual publication owner before CAS.
 * Failure clears output; input/output aliasing is refused. */
struct ClusterFormationSnapshotV1;
extern ClusterControlRootResult cluster_control_root_v3_clean_exit_cut(
	const uint8 *bytes, Size length, const uint8 storage_uuid[16], uint64 system_identifier,
	const struct ClusterFormationSnapshotV1 *formation, ClusterStartupExitCut *out);

/* Coordinator instance's native StartupProcess: consume LMON's full-clean observation and
 * actual predecessor files, then reserve all required successors in one CAS.
 * No caller-provided evidence digest, writer installation or serving grant.
 * Owns CF-X; callers must not hold CF and must supply a CurrentResourceOwner
 * for the existing WALR lifetime. Refusal clears out, including an
 * uncertain durable publication; selected objects must then be reobserved. */
extern ClusterControlRootResult
cluster_control_root_v3_reserve_clean(const ClusterStartupExitCut *expected,
									  ClusterControlRootFileToken *out);

/* PGRAC: bounded retained-writer input, never an authority/retirement proof.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#define CLUSTER_WAL_HISTORY_MAX_RECORDS 128
#define CLUSTER_WAL_HISTORY_HEADER_BYTES 64
#define CLUSTER_WAL_HISTORY_V2_HEADER_BYTES 96
#define CLUSTER_WAL_TERMINAL_REF_BYTES 48
#define CLUSTER_WAL_HISTORY_MAX_BYTES (96 + 128 * 512 + 4)
typedef struct ClusterWalHistoryRecord {
	ClusterControlRootSnapshot snapshot;
	ControlRootRecordRefsV2 refs;
	uint64 publisher_incarnation;
	uint32 publisher_node;
	uint32 record_crc32c;
} ClusterWalHistoryRecord;

/* A terminal has no checkpoint; it cannot masquerade as an ordinary record. */
typedef struct ClusterWalTerminalRef {
	uint64 incarnation;
	uint64 generation;
	uint8 sha256[32];
} ClusterWalTerminalRef;

typedef struct ClusterWalHistoryImage {
	uint32 count;
	ClusterWalHistoryRecord records[CLUSTER_WAL_HISTORY_MAX_RECORDS];
	uint32 terminal_count;
	ClusterWalTerminalRef terminals[CLUSTER_WAL_HISTORY_MAX_RECORDS];
} ClusterWalHistoryImage;

/* PGRAC: decoded immutable pending initialization, never an authority token.
 * Author: SqlRush <sqlrush@gmail.com> */
#define CLUSTER_WAL_STARTUP_BYTES 1536
typedef enum ClusterWalStartupPhase {
	CLUSTER_WAL_STARTUP_RESERVED = 1,
	CLUSTER_WAL_STARTUP_INITIALIZING = 2,
	CLUSTER_WAL_STARTUP_DURABLE = 3
} ClusterWalStartupPhase;

typedef enum ClusterWalStartupInputKind {
	CLUSTER_WAL_STARTUP_CLEAN = 1,
	CLUSTER_WAL_STARTUP_RECOVERED = 2,
	CLUSTER_WAL_STARTUP_IMPORTED = 3
} ClusterWalStartupInputKind;

typedef struct ClusterWalStartupImage {
	uint32 phase;
	uint32 input_kind;
	uint8 operation_uuid[16];
	uint64 database_incarnation;
	uint64 config_generation;
	uint64 formation_epoch;
	uint64 predecessor_file_sequence;
	uint8 predecessor_file_sha256[32];
	uint64 generation;
	XLogRecPtr first_segment_lsn;
	TimeLineID timeline;
	uint32 segment_size;
	uint8 predecessor_evidence_sha256[32];
	XLogRecPtr input_record_start;
	XLogRecPtr input_record_end;
	pg_crc32c input_record_crc;
	TimeLineID input_timeline;
	XLogRecPtr sealed_input_end;
	ClusterWalHistoryRecord predecessor;
	ClusterWalHistoryRecord successor;
	ClusterWalThreadClaimV2 claim;
	ClusterWalDurablePrefix prefix;
	TimeLineID prefix_timeline;
} ClusterWalStartupImage;

/* Checkpoint-less initialization terminal. Logical carrier only: the embedded
 * original and physical census grant neither recovery nor serving permission.
 * Author: SqlRush <sqlrush@gmail.com> */
#define CLUSTER_WAL_TERMINAL_BYTES 2304
#define CLUSTER_WAL_INITIALIZATION_TERMINATED 4
typedef struct ClusterWalTerminalImage {
	uint32 closure_version;
	uint8 operation_uuid[16];
	uint64 generation;
	uint64 database_incarnation;
	uint64 sealing_sequence;
	uint8 sealing_sha256[32];
	uint64 formation_epoch;
	uint64 recoverer_incarnation;
	uint32 recoverer_node;
	uint64 ir_request_id;
	ControlRootStartupRefV3 original_ref;
	uint8 isolation_sha256[32];
	uint8 closure_sha256[32];
	uint8 original[CLUSTER_WAL_STARTUP_BYTES];
	/* Derived by decoding original[], not another authoritative copy. */
	ClusterWalStartupImage initialization;
	ClusterWalStartupObservation observation;
} ClusterWalTerminalImage;

/* Memory-only terminal codecs. The supplied flat union must contain the
 * original predecessor; its historical PGWH pointer is never traversed.
 * Hash/CRC/field validation is not proof of isolation or native closure.
 * Inputs must not overlap outputs; all outputs clear on refusal. */
extern ClusterControlRootResult
cluster_wal_terminal_decode(const uint8 *bytes, size_t length, const ControlRootImage *root,
							uint32 node, const ClusterWalHistoryImage *history,
							const ClusterWalTerminalRef *ref, ClusterWalTerminalImage *out);
extern ClusterControlRootResult cluster_wal_terminal_encode(const ControlRootImage *root,
															uint32 node,
															const ClusterWalHistoryImage *history,
															const ClusterWalTerminalImage *terminal,
															uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES],
															ClusterWalTerminalRef *ref);

/* PGRAC: one origin's complete selected metadata, not replay/retention
 * authority. Pending is not a synthetic current record or checkpoint.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct ClusterWalOriginInputs {
	ClusterWalHistoryRecord current;
	ClusterWalHistoryImage history;
	bool has_pending;
	ClusterWalStartupImage pending;
} ClusterWalOriginInputs;

/* PGRAC: a qualified physical observation, not native closure or a terminal.
 * Current W0 is kept separate from the interrupted initializer W1.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct ClusterWalInitializerInput {
	ClusterWalStartupImage startup;
	ClusterWalStartupObservation observation;
} ClusterWalInitializerInput;

struct ClusterRecoverySerialGuard;
struct ClusterWalRetentionPin;
extern ClusterControlRootResult
cluster_control_root_v3_initializer_observe(struct ClusterRecoverySerialGuard *serial,
											struct ClusterWalRetentionPin *pin,
											ClusterWalInitializerInput *out);

/* Caller supplies an authenticated immutable v3 root and already holds
 * clusterwide CF-S/X. Consume every selected history/PGWG object, including
 * RESERVED. This only reads metadata; physical input use still needs exact
 * WALR/IR/fence owners, claim/anchor/WAL validation and root revalidation.
 * Every refusal clears the entire output. Inputs and output cannot overlap. */
extern ClusterControlRootResult cluster_wal_origin_inputs_read_locked(const ControlRootImage *root,
																	  uint32 node,
																	  ClusterWalOriginInputs *out);

/* Native StartupProcess advances one exact clean-cohort observation. WAIT
 * means no native mutation permission; repeat only after releasing all holds.
 * OK returns this target's INITIALIZING operation, not serving permission.
 * An interrupted initialized writer needs recovery, never caller adoption. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_advance_clean(const ClusterWalDurablePrefixRef *restart,
											  ClusterWalStartupImage *out);

/* Exact target StartupProcess, RESERVED only. Creates/reobserves empty
 * physical metadata; owns CF-X and never grants native WAL/data permission.
 * Refusal clears out. Existing selected files are never overwritten. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_prepare_target(const ClusterControlRootIdentity *self,
											   const uint8 operation_uuid[16],
											   ClusterWalStartupImage *out);
/* Coordinator StartupProcess: actual all-target EMPTY readback followed by
 * exact root CAS to INITIALIZING. Still not an ordinary writer/serving grant. */
extern ClusterControlRootResult cluster_control_root_v3_startup_begin_clean(
	const ClusterControlRootIdentity *self, const uint8 operation_uuid[16],
	const ClusterControlRootFileToken *expected, ClusterControlRootFileToken *out);
/* Startup-only, root-selected INITIALIZING and actual empty namespace. Owns
 * CF-X, returns no ordinary writer/serving permission, creates no files. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_read_writer(const ClusterControlRootIdentity *self,
											const uint8 operation_uuid[16],
											ClusterWalStartupImage *out);
/* Exact INITIALIZING owner only. Atomically switch the native pg_wal symlink
 * from immutable restart input to the selected EMPTY generation and fsync its
 * parent. No writer admission; uncertain retry only accepts that exact route. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_route_writer(const ClusterControlRootIdentity *self,
											 const uint8 operation_uuid[16],
											 ClusterWalStartupImage *out);
/* Actual native EOR checkpoint of this process's selected new writer. Keeps
 * the predecessor current and target non-serving; INSTALL is separate. */
extern ClusterControlRootResult cluster_control_root_v3_startup_checkpoint(
	const ClusterControlRootIdentity *self, const uint8 operation_uuid[16],
	const ControlFileData *control, XLogRecPtr end, ClusterWalStartupImage *out);
/* DURABLE intent must match the selected operation, or the exact already
 * installed successor and full retained union on retry. Owns WALR/CF, never
 * admits SQL or publishes the runtime writer mirror. Refusal clears out. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_install_writer(const ClusterWalStartupImage *expected,
											   ClusterWalDurablePrefixRef *out);

/* Exact selected-object decoding. Does not prove the evidence digest,
 * inspect physical WAL or authorize a state transition. All output clears on
 * refusal, and no input may overlap it. Caller owns root/CF qualification. */
extern ClusterControlRootResult
cluster_control_root_v3_startup_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalStartupImage *out);

/* Encode for the prospective root, returning its exact immutable reference.
 * Does not publish the reference or validate physical exit/WAL evidence.
 * Inputs and outputs must not overlap; both outputs clear on refusal. */
extern ClusterControlRootResult cluster_control_root_v3_startup_encode(
	const ControlRootImage *root, uint32 origin_node, const ClusterWalStartupImage *startup,
	uint8 bytes[CLUSTER_WAL_STARTUP_BYTES], ControlRootStartupRefV3 *out_ref);

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

/* Same flat PGWH record-v2 representation selected by an explicit root-v3.
 * Pending startup is separate and must be included by the enclosing census. */
extern ClusterControlRootResult
cluster_control_root_v3_history_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalHistoryImage *out);
extern ClusterControlRootResult
cluster_control_root_v3_history_encode(const ControlRootImage *root, uint32 origin_node,
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

/* Same owned immutable-file discipline, but a distinct namespace and fixed
 * PGWG size. A stage of either family cannot be installed by the other API.
 * Installation is not root selection, native mutation or serving permission. */
typedef ClusterWalHistoryStage ClusterWalStartupStage;
extern ClusterControlRootResult cluster_wal_startup_prepare(const ControlRootImage *root,
															uint32 origin_node,
															const ClusterWalStartupImage *startup,
															ClusterWalStartupStage *out);
extern ClusterControlRootResult cluster_wal_startup_install(ClusterWalStartupStage *stage);
extern ClusterControlRootResult cluster_wal_startup_discard(ClusterWalStartupStage *stage);
extern ClusterControlRootResult cluster_wal_startup_read_locked(const ControlRootImage *root,
																uint32 origin_node,
																ClusterWalStartupImage *out);

/* Same immutable directory, distinct terminal-only length and API. Selection
 * is always through the supplied root's freshly read manifest; no standalone
 * filename scan or use as an active initialization operation. */
extern ClusterControlRootResult cluster_wal_terminal_read_locked(const ControlRootImage *root,
																 uint32 node, uint32 terminal_index,
																 ClusterWalTerminalImage *out);
extern ClusterControlRootResult
cluster_wal_terminal_prepare(const ControlRootImage *root, uint32 node,
							 const ClusterWalHistoryImage *history,
							 const ClusterWalTerminalImage *terminal, ClusterWalStartupStage *out);
extern ClusterControlRootResult cluster_wal_terminal_install(ClusterWalStartupStage *stage);
extern ClusterControlRootResult cluster_wal_terminal_discard(ClusterWalStartupStage *stage);

/* Physical metadata only, under qualified CF-X. The root adapter owns target
 * and provider admission. create=false refuses missing metadata, except that
 * a RESERVED generation not created yet returns RECONFIG_WAIT to the sync
 * barrier. A missing required parent or partial generation is never a wait.
 * sync=true
 * independently reestablishes physical durability, including when the target
 * failed after creation. Neither mode selects INITIALIZING or enables insertion. */
extern ClusterControlRootResult cluster_wal_startup_empty_locked(const ControlRootImage *root,
																 uint32 origin_node, bool create,
																 bool sync);
extern ClusterControlRootResult
cluster_wal_startup_route_locked(const ControlRootImage *root, uint32 origin_node,
								 const char *pgdata, const ClusterWalDurablePrefixRef *restart);

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

/* PGRAC: explicit startup-capable representation only. These APIs do not
 * change a v1/v2 file reader, publish objects or admit a native startup.
 * Decode clears all output on refusal; encode clears bytes on refusal.
 * Author: SqlRush <sqlrush@gmail.com> */
extern ClusterControlRootResult cluster_control_root_v3_decode(const uint8 *bytes, size_t len,
															   const uint8 storage_uuid[16],
															   uint64 system_identifier,
															   ControlRootImage *out);
extern ClusterControlRootResult cluster_control_root_v3_encode(ControlRootImage *image);

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

/* Explicit startup-capable readers. Preserve pending references; a current
 * thread projection is not a complete recovery/retention input census. */
extern ClusterControlRootResult
cluster_control_root_v3_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token);
extern ClusterControlRootResult
cluster_control_root_v3_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token);

/* Runtime local owner only. Borrow the caller's CF-S/X; not early startup. */
extern ClusterControlRootResult
cluster_control_root_v2_read_runtime_local_locked(ControlFileData *out);
extern ClusterControlRootResult
cluster_control_root_v3_read_runtime_local_locked(ControlFileData *out);
/* Explicit old-format tools/tests only; runtime dispatch must use v3. */
extern ClusterControlRootResult
cluster_control_root_v2_read_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
									   ClusterControlRootSnapshot *out,
									   ClusterControlRootReadToken *token);
extern ClusterControlRootResult
cluster_control_root_v3_read_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
									   ClusterControlRootSnapshot *out,
									   ClusterControlRootReadToken *token);

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
extern ClusterControlRootResult
cluster_control_root_v3_read_retention_current(const ClusterControlRootIdentity *self,
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
extern ClusterControlRootResult cluster_control_root_v3_checkpoint_publish(
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
extern ClusterControlRootResult cluster_control_root_v3_shutdown_checkpoint_publish(
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
extern ClusterControlRootResult
cluster_control_root_v3_shutdown_observe(const ClusterWalDurablePrefixRef *expected,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *out_token);

struct ClusterPhase1FullStopPlan;
/* Only the actual post-checkpoint normal-stop controller may own this call.
 * Success includes exact CF/WALR retirement, not merely durable root bytes. */
extern ClusterControlRootResult
cluster_control_root_v2_normal_stop_close(const struct ClusterPhase1FullStopPlan *plan,
										  bool *all_closed);
extern ClusterControlRootResult
cluster_control_root_v3_normal_stop_close(const struct ClusterPhase1FullStopPlan *plan,
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
extern ClusterControlRootResult
cluster_control_root_v3_stop_phase_read(uint16 thread, uint64 admitted_incarnation,
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
extern ClusterControlRootResult
cluster_control_root_v3_failure_open_publish(const struct ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token);
extern ClusterControlRootResult
cluster_control_root_v3_failure_tail_publish(const struct ClusterRecoverySerialRequest *request,
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
