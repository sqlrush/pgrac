/*-------------------------------------------------------------------------
 * PGRAC: root-selected immutable configuration objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHARED_CONFIG_H
#define CLUSTER_SHARED_CONFIG_H

#include "cluster/cluster_control_root.h"

#define CLUSTER_SHARED_CONFIG_MAX_BYTES (1024 * 1024)
#define CLUSTER_SHARED_CONFIG_MAX_ENTRIES 8192
#define CLUSTER_SHARED_CONFIG_MAX_NAME 127
#define CLUSTER_SHARED_CONFIG_MAX_VALUE 8192
#define CLUSTER_SHARED_CONFIG_COMMON (-1)

/* Logical in-memory carriers, not disk structs or startup permissions. */
typedef struct ClusterSharedConfigIdentity {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint64 generation;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint64 configured[2];
} ClusterSharedConfigIdentity;

typedef struct ClusterSharedConfigRef {
	ClusterSharedConfigIdentity identity;
	uint8 sha256[32];
} ClusterSharedConfigRef;

typedef struct ClusterSharedConfigEntry {
	int node_id; /* -1 common, otherwise an explicitly configured node */
	const char *name;
	const char *value;
} ClusterSharedConfigEntry;

typedef struct ClusterSharedConfigImage {
	char *bytes; /* palloc-owned exact bytes plus a convenience NUL, not hashed */
	size_t len;
} ClusterSharedConfigImage;

StaticAssertDecl(sizeof(ClusterSharedConfigIdentity) == 72, "shared config identity carrier");
StaticAssertDecl(sizeof(ClusterSharedConfigRef) == 104, "shared config reference carrier");

/* Entries must already be in canonical key order; no implicit sorting or
 * duplicate resolution. Inputs/outputs must not alias. Refusal clears outputs.
 * These APIs validate representation and binding ONLY. Actual GUC name/context,
 * value, sensitive-reference and application policy is a separate mandatory
 * consumer gate; neither a decoded object nor lookup grants serving permission.
 */
extern ClusterControlRootResult
cluster_shared_config_encode(const ClusterSharedConfigIdentity *id,
							 const ClusterSharedConfigEntry *entries, size_t count, char *bytes,
							 size_t capacity, size_t *len, uint8 sha256[32]);
extern ClusterControlRootResult cluster_shared_config_validate(const char *bytes, size_t len,
															   const ClusterSharedConfigRef *ref,
															   uint32 *count);
extern ClusterControlRootResult cluster_shared_config_lookup(const char *bytes, size_t len,
															 const ClusterSharedConfigRef *ref,
															 int node_id, const char *name,
															 char *value, size_t capacity);

/* Borrow CF-S/X. Exact fixed path only; no directory search/projection/.bak.
 * Caller owns root selection/revalidation. No directories or objects created.
 * shared_root must be an absolute deployment path. The image is cleared on
 * failure; release successful output with cluster_shared_config_free().
 */
extern ClusterControlRootResult cluster_shared_config_read_locked(const char *shared_root,
																  const ClusterSharedConfigRef *ref,
																  ClusterSharedConfigImage *out);
extern void cluster_shared_config_free(ClusterSharedConfigImage *image);

/* Read-only inspection. The entire object is validated before the first
 * callback; callback entries are borrowed only for that invocation. A callback
 * refusal propagates immediately. This is not an atomic application API.
 */
typedef ClusterControlRootResult (*ClusterSharedConfigVisitor)(
	const ClusterSharedConfigEntry *entry, void *arg);
extern ClusterControlRootResult cluster_shared_config_visit(const char *bytes, size_t len,
															const ClusterSharedConfigRef *ref,
															ClusterSharedConfigVisitor visitor,
															void *arg);

typedef enum ClusterSharedConfigPolicyReason {
	CLUSTER_CONFIG_POLICY_OK = 0,
	CLUSTER_CONFIG_POLICY_FORMAT,
	CLUSTER_CONFIG_POLICY_UNKNOWN,
	CLUSTER_CONFIG_POLICY_CONTEXT,
	CLUSTER_CONFIG_POLICY_UNSUPPORTED,
	CLUSTER_CONFIG_POLICY_SCOPE,
	CLUSTER_CONFIG_POLICY_COLD_ONLY,
	CLUSTER_CONFIG_POLICY_REFERENCE,
	CLUSTER_CONFIG_POLICY_VALUE,
	CLUSTER_CONFIG_POLICY_MISSING,
	CLUSTER_CONFIG_POLICY_RECOVERY_CAPACITY
} ClusterSharedConfigPolicyReason;

/* Diagnostic counts, never an application ACK or a complete-profile permit.
 * No values (potential secrets) are copied into diagnostics. In-memory only.
 */
typedef struct ClusterSharedConfigPolicyReport {
	ClusterSharedConfigPolicyReason reason;
	int node_id;
	char name[CLUSTER_SHARED_CONFIG_MAX_NAME + 1];
	uint32 checked_entries;
	uint32 restart_entries;
	uint32 cold_entries;
} ClusterSharedConfigPolicyReport;

/* Root-selected historical minima, not current settings or recovery proof. */
typedef struct ClusterControlRecoveryCapacity {
	uint32 current_sources;
	uint32 history_sources;
	uint32 max_connections;
	uint32 max_worker_processes;
	uint32 max_wal_senders;
	uint32 max_prepared_xacts;
	uint32 max_locks_per_xact;
} ClusterControlRecoveryCapacity;

/* Read already applied native values only. No assignment or bound retirement.
 * Inputs must not alias report; callers still prove full recovery-input coverage.
 */
extern ClusterControlRootResult
cluster_shared_config_check_recovery_capacity(const ClusterControlRecoveryCapacity *required,
											  ClusterSharedConfigPolicyReport *report);

/* Requires the registered native GUC engine. Checks do not assign settings.
 * online_change applies to ONE changed entry, not unchanged cold entries in a
 * full image. SQL permission checks and exact old/new diff belong to publisher.
 */
extern ClusterControlRootResult
cluster_shared_config_check_entry(const ClusterSharedConfigEntry *entry, bool online_change,
								  ClusterSharedConfigPolicyReport *report);
extern ClusterControlRootResult
cluster_shared_config_check_gucs(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
								 ClusterSharedConfigPolicyReport *report);

/* Mandatory PRE2 bootstrap bindings only, not application/admission. Requires
 * native parameter registration. Expected paths/node come from the independent
 * bootstrap inputs, not from this object. Applies existing static name/scope
 * policy without testing coupled native hooks against old process values.
 * Native apply/postchecks, recovery minima and physical qualification remain
 * mandatory. No I/O/assignment/default filling; inputs must not alias report.
 */
extern ClusterControlRootResult cluster_shared_config_check_bootstrap(
	const char *bytes, size_t len, const ClusterSharedConfigRef *ref, int node_id,
	const char *shared_root, const char *wal_root, const char *undo_root,
	ClusterSharedConfigPolicyReport *report);

/* PGRAC: process-local receipt, not a shared ACK or serving permission.
 * Caller must supply the root-selected reference and independently bound node,
 * then revalidate root/profile/recovery minima before admission. Startup only;
 * any refusal is FATAL because native assign hooks are not reversible. The
 * receipt stays zero until the whole application succeeds. Inputs/output must
 * not alias. Does not authorize an online reload or a backend SQL apply.
 */
typedef struct ClusterSharedConfigApplied {
	ClusterSharedConfigRef ref;
	uint32 node_id;
	uint32 applied_entries;
} ClusterSharedConfigApplied;
StaticAssertDecl(sizeof(ClusterSharedConfigApplied) == 112, "config application receipt");
extern void cluster_shared_config_apply_startup(const char *bytes, size_t len,
												const ClusterSharedConfigRef *ref, int node_id,
												ClusterSharedConfigApplied *out);

/* PGRAC: publisher-owned memory, not persistent/wire/shared-memory state.
 * Prepare does not publish. Install borrows CF-X, held by the same caller
 * through root CAS. Discard never removes formal objects. No GUC policy or
 * application ACK is implied by any of these low-level operations.
 * Inputs and the prepare output must not overlap. I/O-uncertain orphan cleanup
 * requires the publisher's exact retired-owner proof, never a wall-clock age.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigStage {
	ClusterSharedConfigRef ref;
	uint8 operation_uuid[16];
	uint64 bytes;
	uint64 object_dir_dev;
	uint64 object_dir_ino;
	uint64 staging_dir_dev;
	uint64 staging_dir_ino;
	uint64 file_dev;
	uint64 file_ino;
	uint32 owner_pid;
	uint32 state;
} ClusterSharedConfigStage;
StaticAssertDecl(sizeof(ClusterSharedConfigStage) == 184, "config staging descriptor");

extern ClusterControlRootResult cluster_shared_config_prepare(const char *shared_root,
															  const char *bytes, size_t len,
															  const ClusterSharedConfigRef *ref,
															  const uint8 operation_uuid[16],
															  ClusterSharedConfigStage *out);
extern ClusterControlRootResult cluster_shared_config_install(const char *shared_root,
															  ClusterSharedConfigStage *stage);
extern ClusterControlRootResult cluster_shared_config_discard(const char *shared_root,
															  ClusterSharedConfigStage *stage);

/* Native policy before any staging I/O. Not a root publish or application. */
extern ClusterControlRootResult
cluster_shared_config_prepare_gucs(const char *shared_root, const char *bytes, size_t len,
								   const ClusterSharedConfigRef *ref,
								   const uint8 operation_uuid[16], ClusterSharedConfigStage *out,
								   ClusterSharedConfigPolicyReport *report);

#endif
