/*-------------------------------------------------------------------------
 * PGRAC: root-selected immutable configuration objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHARED_CONFIG_H
#define CLUSTER_SHARED_CONFIG_H

#include "cluster/cluster_control_root.h"
#include "port/atomics.h"

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

/* Owned input selected under the caller's cluster-wide CF-S/X, not a node
 * application or data permit. prior is the consumer's actual last reference.
 * Refusal clears outputs; aliases refuse without touching either carrier.
 * ERROR does not release the borrowed CF. Caller frees a successful image.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigSelected {
	ClusterSharedConfigRef ref;
	ClusterControlRootFileToken root;
} ClusterSharedConfigSelected;
StaticAssertDecl(sizeof(ClusterSharedConfigSelected) == 184, "selected configuration input");
extern ClusterControlRootResult
cluster_control_root_config_read_locked(const ClusterSharedConfigRef *prior,
										ClusterSharedConfigSelected *out,
										ClusterSharedConfigImage *image);

/* LMON-owned cooperative read/retirement. A result is delivered only after
 * exact CF release at the same control cut/prior input. LOCK_UNAVAILABLE
 * retains the attempt; ERROR/cancel retains only existing CF cleanup duty.
 * No native hooks, node ACK or data permission. Caller frees successful bytes.
 * Alias/refusal behavior is the same as read_locked. Cancel must be called
 * before the background duty stops polling; it never touches another task.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_control_root_config_poll(const ClusterSharedConfigRef *prior,
								 ClusterSharedConfigSelected *out, ClusterSharedConfigImage *image);
extern void cluster_control_root_config_cancel(void);

/* Ephemeral native-family delivery, not configuration authority or an ACK.
 * One real process writes a slot; sequence protects a bounded value copy.
 * Owner replacement/reset requires the native postmaster lifecycle barrier.
 * No pointers, paths, native hooks or disk/wire representation in the slot.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigDeliverySlot {
	pg_atomic_uint64 sequence;
	uint64 local_generation;
	int32 writer_pid;
	uint32 length;
	ClusterSharedConfigRef ref;
	char bytes[CLUSTER_SHARED_CONFIG_MAX_BYTES + 1];
} ClusterSharedConfigDeliverySlot;

extern void cluster_shared_config_delivery_slot_init(ClusterSharedConfigDeliverySlot *slot);
/* Only after native waitpid proves this exact writer dead; not a timeout. */
extern bool cluster_shared_config_delivery_slot_retire(ClusterSharedConfigDeliverySlot *slot,
													   int32 writer_pid);
extern bool cluster_shared_config_delivery_slot_write(ClusterSharedConfigDeliverySlot *slot,
													  uint64 local_generation, int32 writer_pid,
													  const ClusterSharedConfigRef *ref,
													  const char *bytes, size_t len);
extern bool cluster_shared_config_delivery_slot_read(ClusterSharedConfigDeliverySlot *slot,
													 uint64 local_generation,
													 ClusterSharedConfigRef *ref,
													 ClusterSharedConfigImage *image);

/* PGRAC: amend one explicit scope/key in an exact selected object. NULL
 * change.value means RESET. Preserve all other entries/identity; changed
 * requests advance generation once, without wrap. No-op returns an owned copy
 * of the original. No GUC policy, I/O, root publication or admission is implied.
 * Refusal clears outputs. Inputs/outputs must not overlap; free out normally.
 * Author: SqlRush <sqlrush@gmail.com>
 */
extern ClusterControlRootResult
cluster_shared_config_amend(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
							const ClusterSharedConfigEntry *change, ClusterSharedConfigImage *out,
							ClusterSharedConfigRef *next_ref, bool *changed);

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
	uint32 pending_sources;
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

/* One online SET/RESET, with native old/changed/new policy before staging.
 * No-op leaves stage zero and reports changed=false. No assignment, SQL
 * privilege check, root CAS or application ACK; the publisher owns those.
 * Inputs/outputs must not alias. Refusal leaves stage/changed clear. */
extern ClusterControlRootResult cluster_shared_config_prepare_change(
	const char *shared_root, const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
	const ClusterSharedConfigEntry *change, const uint8 operation_uuid[16],
	ClusterSharedConfigStage *out, bool *changed, ClusterSharedConfigPolicyReport *report);

/* PGRAC: one internal publication attempt, not an application ACK. Native
 * SQL permissions/audit must precede this call. Owns CF-S then CF-X, stages
 * outside CF, and preserves all thread/recovery inputs. CAS/initializer
 * competition is retryable by the caller; never loop while holding CF.
 * Refusal clears out, even when a root write may already have occurred.
 * Published work must subsequently be applied by the background owner;
 * caller cancellation cannot revoke it. Inputs/outputs must not overlap.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigPublication {
	ClusterSharedConfigRef ref;
	ClusterControlRootFileToken root;
	bool changed;
} ClusterSharedConfigPublication;

extern ClusterControlRootResult
cluster_control_root_config_change(const ClusterSharedConfigEntry *change,
								   ClusterSharedConfigPublication *out,
								   ClusterSharedConfigPolicyReport *report);

/* PGRAC: process-local reload result, NOT a node/cluster application ACK.
 * A selected newer generation may skip intermediates; equal generation must
 * have equal bytes/hash. Native FILE source precedence and SET/SET LOCAL are
 * preserved. Counts describe this old->new operation, NOT cumulative readiness.
 * Pending/deferred entries are NOT active new values. A later diff reporting
 * zero cannot retire a prior restart/deferred obligation; the owner must keep
 * that obligation until actual process restart/application. The caller
 * owns root revalidation and admission; a failure may follow native assignments
 * and clears out without pretending to undo them. Inputs/outputs must not alias.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigReload {
	ClusterSharedConfigRef old_ref;
	ClusterSharedConfigRef ref;
	uint32 node_id;
	uint32 applied_entries;
	uint32 removed_entries;
	uint32 pending_restart_entries;
	uint32 deferred_entries;
	/* Cumulative native gauges, including removed settings from older refs.
	 * These are not node ACKs or proof that pending values are active. */
	uint32 pending_restart_total;
	uint32 deferred_total;
} ClusterSharedConfigReload;
StaticAssertDecl(sizeof(ClusterSharedConfigReload) == 240, "native config reload outcome");

extern ClusterControlRootResult cluster_shared_config_apply_reload(
	const char *old_bytes, size_t old_len, const ClusterSharedConfigRef *old_ref,
	const char *new_bytes, size_t new_len, const ClusterSharedConfigRef *new_ref, int node_id,
	ClusterSharedConfigReload *out, ClusterSharedConfigPolicyReport *report);

/* Actual process-local defaults, inherited with native values at fork. A
 * generation/barrier number supplied by another process is never a substitute.
 * ref is the last fully consumed target, NOT proof that pending values are
 * active. failed is sticky after potentially partial native application.
 * Neither an observation nor an applier PID is a node/application ACK.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigProcess {
	ClusterSharedConfigRef ref;
	uint32 node_id;
	uint32 pending_restart_total;
	uint32 deferred_total;
	int32 applier_pid;
	bool failed;
	/* Native parallel restoration replaces inherited defaults with leader
	 * values. This classified query-owned state has NO ref/application proof. */
	bool parallel_snapshot;
} ClusterSharedConfigProcess;
StaticAssertDecl(sizeof(ClusterSharedConfigProcess) == 128, "native config process outcome");

/* Refusal clears out. Caller owns target selection, identity revalidation,
 * registration and admission. No lock/I/O/shared state or implicit restart.
 * Inputs/outputs must not overlap. Observe returns whether state exists, not
 * whether this process may serve. Unseeded or failed processes cannot reload.
 */
extern bool cluster_shared_config_process_observe(ClusterSharedConfigProcess *out);
/* Copy only actual fully consumed defaults. Not a selected-root read or ACK;
 * failed/parallel/unseeded states refuse. Successful image is caller-owned. */
extern bool cluster_shared_config_process_copy(ClusterSharedConfigProcess *out,
											   ClusterSharedConfigImage *image);
extern void cluster_shared_config_process_parallel_restore(void);
extern ClusterControlRootResult cluster_shared_config_process_reload(
	const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
	ClusterSharedConfigProcess *out, ClusterSharedConfigPolicyReport *report);

/* Actual native process-lifetime observations, not a node census/ACK. A slot
 * is single-writer; readers never wait for its writer. Slot index plus the
 * registration serial, not PID alone, identifies a lifetime. Native shmem
 * initialization is the only sequence reset. No disk/wire representation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ClusterSharedConfigRegistration {
	uint64 registration;
	int32 pid;
	int32 role;
	ClusterSharedConfigProcess process;
	bool observed;
} ClusterSharedConfigRegistration;
StaticAssertDecl(sizeof(ClusterSharedConfigRegistration) == 152, "config process registration");

typedef struct ClusterSharedConfigSlot {
	pg_atomic_uint64 sequence;
	ClusterSharedConfigRegistration value;
} ClusterSharedConfigSlot;
StaticAssertDecl(sizeof(ClusterSharedConfigSlot) == 160, "config process observation slot");
extern void cluster_shared_config_registration_init(ClusterSharedConfigSlot *slot);
extern void cluster_shared_config_process_new_shmem(void);
extern bool cluster_shared_config_process_attach(ClusterSharedConfigSlot *slot);
extern void cluster_shared_config_process_detach(void);
/* Clear output on a busy/refused read. Input/output must not overlap; an
 * alias refuses without touching either carrier. An empty slot is observable,
 * but pid=0 / observed=false is never an active-process proof. */
extern bool cluster_shared_config_registration_read(ClusterSharedConfigSlot *slot,
													ClusterSharedConfigRegistration *out);

/* Native family transport. Creation/reset are real postmaster-only, before
 * children start / after DATA children exit. No disk state or CF acquisition.
 * logger_started/reaped are called only at the actual native PID boundaries.
 */
extern void cluster_shared_config_delivery_start(void);
extern void cluster_shared_config_delivery_new_shmem(void);
extern void cluster_shared_config_delivery_lmon_started(int32 pid);
extern void cluster_shared_config_delivery_lmon_reaped(int32 pid);
extern void cluster_shared_config_delivery_logger_started(int32 pid);
extern void cluster_shared_config_delivery_logger_reaped(int32 pid);
extern void cluster_shared_config_delivery_logger_attach(void);
extern bool cluster_shared_config_delivery_logger_observe(ClusterSharedConfigRegistration *out);
extern bool cluster_shared_config_delivery_publish(const ClusterSharedConfigRef *ref,
												   const ClusterSharedConfigImage *image);
extern bool cluster_shared_config_delivery_reload(void);
extern bool cluster_shared_config_delivery_parent_publish(void);
extern void cluster_shared_config_delivery_lmon_tick(void);
extern void cluster_shared_config_delivery_lmon_cancel(void);

#endif
