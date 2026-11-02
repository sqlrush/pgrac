/* Native by-value identities selected by the original control/creation owner. */
#ifndef CLUSTER_CONTROL_NATIVE_FILES_H
#define CLUSTER_CONTROL_NATIVE_FILES_H
#include "cluster/cluster_control_file_identity.h"
#include "common/cluster_recovery_reservations.h"
#include "common/pgrac_initdb_recovery.h"

#define CLUSTER_NATIVE_FILES_REGISTRY_BUDGET_V2 ((Size)1048576)
/* Registry file entries occupy at least 208 canonical bytes. Full inventories
 * above this startup reservation refuse; they are never truncated to 64 pins. */
#define CLUSTER_NATIVE_FILES_CAPACITY_V2 ((1048576 - 256) / 208)
#define CLUSTER_NATIVE_RESERVATIONS_CAPACITY_V2 ((1048576 - 256) / 256)

typedef enum ClusterControlNativeModeV2 {
	CLUSTER_NATIVE_FILES_UNKNOWN_V2 = 0,
	CLUSTER_NATIVE_FILES_CREATION_V2,
	CLUSTER_NATIVE_FILES_MANAGED_V2
} ClusterControlNativeModeV2;
typedef enum ClusterControlNativeResultV2 {
	CLUSTER_NATIVE_FILES_OK_V2 = 0,
	CLUSTER_NATIVE_FILES_REFUSED_V2,
	CLUSTER_NATIVE_FILES_NEEDS_REGISTRATION_V2,
	CLUSTER_NATIVE_FILES_CAPACITY_EXCEEDED_V2,
	CLUSTER_NATIVE_FILES_BUSY_V2,
	CLUSTER_NATIVE_FILES_NOT_FOUND_V2,
	CLUSTER_NATIVE_FILES_AMBIGUOUS_V2,
	CLUSTER_NATIVE_FILES_NOT_AVAILABLE_V2,
	CLUSTER_NATIVE_FILES_REGISTRATION_DEBT_V2
} ClusterControlNativeResultV2;
/* Local, address-bound borrow. Zero-initialize; never copy a live borrow.
 * expected is immutable by-value. Keep it with the ORIGINAL slot/FD/I/O duty
 * across unlock, completion and ERROR cleanup, then release exactly once.
 * A borrow adds no FD/pin/coverage or file permission. Caller retains original
 * native family locks, actual page birth, old-writer and file-lifetime duties. */
typedef struct ClusterControlNativeFileBorrowV2 {
	ClusterRecoveryFileIdentityV2 expected;
	uint64 table_generation;
	uint64 startup_generation;
	uintptr_t private_self;
	int private_pid;
	uint32 private_index;
} ClusterControlNativeFileBorrowV2;
typedef struct ClusterControlNativeTableInfoV2 {
	ClusterControlNativeModeV2 mode;
	uint64 table_generation, startup_generation, count, capacity, borrowed;
	uint64 reservation_count;
	Size registry_byte_budget, shared_bytes;
	ClusterControlRelmapReferenceV2 control;
	/* Only CREATION: the authenticated original batch, not CF/ROOT authority.
	 * Descriptor integers are observations, never independently borrowable. */
	PgracInitdbRecoveryContext creation;
} ClusterControlNativeTableInfoV2;
extern void cluster_control_native_files_shmem_register(void);
/* Unknown is refusal, never permission to select a legacy file. No CF, I/O,
 * allocation or family-lock acquisition on the following local read path. */
extern ClusterControlNativeModeV2 cluster_control_native_files_mode_v2(void);
extern bool cluster_control_native_files_info_v2(ClusterControlNativeTableInfoV2 *out);
extern bool cluster_control_native_file_acquire_v2(const ClusterControlFileKeyV2 *,
												   ClusterControlNativeFileBorrowV2 *fresh);
/* Resolve an address without guessing its creation origin. Exactly one
 * AVAILABLE identity in the complete selected table is required. Missing and
 * multiple origins have distinct results; refusal leaves fresh unchanged and
 * takes no borrow. The current MANAGED producer installs only full coverage
 * identities; UNKNOWN and the unpublished CREATION batch are NOT_AVAILABLE.
 * RESERVED identities are retained in a separate complete vector, never in
 * that available vector. An address with only RESERVED returns NOT_AVAILABLE.
 * Same address/process-bound borrow, current and release as acquire_v2; no CF,
 * I/O, allocation or family-lock acquisition. Caller retains original locks
 * and file-lifetime duty through cleanup. Zero-initialize fresh. */
extern ClusterControlNativeResultV2 cluster_control_native_file_borrow_by_address_v2(
	ClusterRecoveryFileKindV2 kind, Oid database_oid, Oid tablespace_oid, uint64 object_id,
	ClusterControlNativeFileBorrowV2 *fresh);
extern bool cluster_control_native_file_current_v2(const ClusterControlNativeFileBorrowV2 *);
/* Release remains possible after a failed current check; clears only a valid
 * local borrow. False leaves the original duty intact. No CF/IO/reselection. */
extern bool cluster_control_native_file_release_v2(ClusterControlNativeFileBorrowV2 *);
/* Exact creator-only RESERVED borrow, BEFORE family locks, under the SAME
 * original CF-X. Select/verify original ROOT/claim/anchor/creator and complete
 * expected/batch against the installed full table. No CF acquisition, new
 * owner/scope/FD/pin or file permission. Keep the original creator/file duty;
 * current/release then use the existing local borrow counter without CF/I/O.
 * COMPLETE/cancel must not discard that original duty. Fresh zero out only;
 * false/ERROR takes no borrow and leaves out unchanged. Never falls back to an
 * AVAILABLE row. Ordinary acquire/address APIs still refuse RESERVED. */
extern ClusterControlNativeResultV2 cluster_control_native_file_reserved_acquire_locked_v2(
	const ClusterSharedConfigRef *, Size registry_byte_budget, uint64 startup_generation,
	const uint8 batch_uuid[16], const ClusterRecoveryFileIdentityV2 *expected,
	ClusterControlNativeFileBorrowV2 *fresh);
/* Complete gap check before lease/WAL/allocator side effects. Inclusive range;
 * includes the caller's two successor segments. Presence includes persistent
 * RESERVED, without granting creator authority or I/O. No CF or registration here. */
extern ClusterControlNativeResultV2 cluster_control_native_files_clog_range_v2(uint16 origin,
																			   uint64 first_segment,
																			   uint64 last_segment);
/* Explicit control entry, SAME original CF-X throughout. Select complete ROOT
 * and registry, then recheck before atomic local publication. startup_generation
 * comes from the original OPEN/Start, not a page/counter. Only identical rows or
 * distinct additions are supported: removal/reidentity refuses until the
 * original cross-instance stop/drain transition is connected. Live borrows
 * prevent a changed table. RESERVED removal/reidentity still refuses until the
 * original stop/drain is connected; same-identity completion is permitted.
 * No waiting under family locks. Refusal preserves
 * old table, original CF/FD/pins/debt. Does not publish ROOT or activate v2. */
extern ClusterControlNativeResultV2 cluster_control_native_files_publish_locked_v2(
	const ClusterSharedConfigRef *, Size registry_byte_budget, uint64 startup_generation);
/* Validate the requested complete range against independently PERSISTENT
 * registration (AVAILABLE or RESERVED), then publish the complete selected
 * inventory. OK requires complete presence. Missing identity
 * returns NEEDS_REGISTRATION before any table mutation; it does not invent
 * synced bytes/page birth/coverage or infer identity from a file. Actual control
 * reservation publication must precede this entry. */
extern ClusterControlNativeResultV2 cluster_control_native_files_preregister_locked_v2(
	const ClusterSharedConfigRef *, Size registry_byte_budget, uint64 startup_generation,
	uint16 origin, uint64 first_segment, uint64 last_segment);
/* Original OPEN control producer; SAME original CF-X, before family locks.
 * Whole inclusive predicted gap plus successors, never a truncated prefix.
 * Missing keys receive control-issued identities in one persistent RESERVE,
 * followed by the original complete table publication. Existing identities and
 * creator batches are preserved. ERROR or REGISTRATION_DEBT after entering the
 * one-shot publication retains the original creation duty: no rollback/retry.
 * Other refusals precede registration. No file I/O permission or activation.
 * Fresh *out must be NULL. Local metadata only; destroy never retires debt. */
typedef struct ClusterControlNativeClogEntryV2 {
	ClusterRecoveryFileIdentityV2 expected;
	uint8 batch_uuid[16];
	bool reserved;
} ClusterControlNativeClogEntryV2;
typedef struct ClusterControlNativeClogRangeV2 {
	ClusterSharedConfigRef config;
	uint64 startup_generation, table_generation;
	uint64 first_segment, last_segment, count;
	ClusterControlNativeClogEntryV2 entries[FLEXIBLE_ARRAY_MEMBER];
} ClusterControlNativeClogRangeV2;
extern ClusterControlNativeResultV2
cluster_control_native_files_reserve_clog_range_locked_v2(uint16 origin, uint64 first_segment,
														  uint64 last_segment,
														  ClusterControlNativeClogRangeV2 **fresh);
extern void cluster_control_native_clog_range_destroy_v2(ClusterControlNativeClogRangeV2 **);
/* Pure storage predicate for the control producer's output; no table read,
 * borrow, I/O or authority. */
extern bool cluster_control_native_files_output_distinct_v2(const void *, Size);
#endif
