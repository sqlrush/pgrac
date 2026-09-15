/*-------------------------------------------------------------------------
 *
 * cluster_undo_block0.h
 *	  Local identity and resident-state core for undo block zero.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/storage/cluster_undo_block0.h
 *
 * NOTES
 *	  This is a pgrac-original file.  All exported symbols use the
 *	  cluster_undo_block0_ namespace.
 *	  Spec: spec-8.4a-undo-block0-authority-prerequisite.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_BLOCK0_H
#define CLUSTER_UNDO_BLOCK0_H

#include "c.h"

#include "access/xlogdefs.h"
#include "cluster/cluster_undo_segment.h"
#include "cluster/storage/cluster_undo_alloc.h"

#define CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER CLUSTER_UNDO_SEGS_PER_INSTANCE
#define CLUSTER_UNDO_BLOCK0_SLOT_COUNT                                                             \
	((uint32)UNDO_OWNER_INSTANCE_MAX * CLUSTER_UNDO_BLOCK0_SLOTS_PER_OWNER)

StaticAssertDecl(CLUSTER_UNDO_BLOCK0_SLOT_COUNT == 32768,
				 "undo block-zero logical slot count must remain 32768");

/* One logical segment-header identity. */
typedef struct ClusterUndoBlock0LogicalKey {
	uint32 segment_id;
	uint8 owner_instance;
} ClusterUndoBlock0LogicalKey;

/* Resolved storage target retained inside one logical slot. */
typedef struct ClusterUndoBlock0ResolvedRoot {
	ClusterUndoPathIntent intent;
	uint64 root_id;
	uint64 root_generation;
} ClusterUndoBlock0ResolvedRoot;

/* Presence is independent of value because generation zero is valid. */
typedef struct ClusterUndoBlock0Generation {
	bool known;
	uint32 value;
} ClusterUndoBlock0Generation;

typedef enum ClusterUndoBlock0Result {
	CLUSTER_UNDO_BLOCK0_OK,
	CLUSTER_UNDO_BLOCK0_NOT_FOUND,
	CLUSTER_UNDO_BLOCK0_NOT_PUBLISHED,
	CLUSTER_UNDO_BLOCK0_GENERATION_MISMATCH,
	CLUSTER_UNDO_BLOCK0_IDENTITY_MISMATCH,
	CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED,
	CLUSTER_UNDO_BLOCK0_CAPACITY_UNAVAILABLE,
	CLUSTER_UNDO_BLOCK0_IO_ERROR
} ClusterUndoBlock0Result;

/* Resident payload states.  Recovery-private images never enter this FSM. */
typedef enum ClusterUndoBlock0SlotState {
	CLUSTER_UNDO_BLOCK0_SLOT_EMPTY,
	CLUSTER_UNDO_BLOCK0_SLOT_FILLING,
	CLUSTER_UNDO_BLOCK0_SLOT_VALID_CLEAN,
	CLUSTER_UNDO_BLOCK0_SLOT_VALID_DIRTY,
	CLUSTER_UNDO_BLOCK0_SLOT_RETIRING
} ClusterUndoBlock0SlotState;

typedef enum ClusterUndoBlock0Mode {
	CLUSTER_UNDO_BLOCK0_SHARED,
	CLUSTER_UNDO_BLOCK0_EXCLUSIVE
} ClusterUndoBlock0Mode;

typedef enum ClusterUndoBlock0AuthorityKind {
	CLUSTER_UNDO_BLOCK0_LIVE_OWNER,
	CLUSTER_UNDO_BLOCK0_RECOVERY_OWNER,
	CLUSTER_UNDO_BLOCK0_STARTUP_REDO
} ClusterUndoBlock0AuthorityKind;

typedef struct ClusterUndoBlock0AuthorityProof {
	ClusterUndoBlock0AuthorityKind kind;
	uint8 owner_instance;
	bool cluster_epoch_present;
	uint64 cluster_epoch;
	uint64 recovery_generation;
} ClusterUndoBlock0AuthorityProof;

typedef struct ClusterUndoBlock0Pin {
	int slot;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot resolved_root;
	ClusterUndoBlock0Generation observed_generation;
	ClusterUndoBlock0Mode mode;
	ClusterUndoBlock0AuthorityProof proof;
} ClusterUndoBlock0Pin;

typedef struct ClusterUndoBlock0RecoveryGuard {
	int slot;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot resolved_root;
	ClusterUndoBlock0AuthorityProof proof;
	bool content_x_held;
} ClusterUndoBlock0RecoveryGuard;

typedef struct ClusterUndoBlock0FrameToken {
	uint32 frame_index;
	bool owned;
} ClusterUndoBlock0FrameToken;

/* One caller-owned item from the complete canonical post-replay census. */
typedef struct ClusterUndoBlock0ResidentCensusItem {
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot resolved_root;
	ClusterUndoBlock0Generation generation;
	ClusterUndoBlock0AuthorityProof proof;
} ClusterUndoBlock0ResidentCensusItem;

typedef enum ClusterR4PrerequisiteStatus {
	CLUSTER_R4_PREREQUISITE_RF_DEFERRED = 0,
	CLUSTER_R4_PREREQUISITE_R4A_READY = 1
} ClusterR4PrerequisiteStatus;

typedef struct ClusterR4PrerequisiteSnapshot {
	ClusterR4PrerequisiteStatus status;
	bool ready;
	uint8 reserved0[3];
	int32 target_node_id;
	uint32 episode_state_generation;
	uint64 jcmk_generation;
	uint64 request_nonce;
	uint64 old_admitted_incarnation;
	uint64 fresh_incarnation;
	uint64 committed_epoch;
	uint64 grammar_fingerprint;
} ClusterR4PrerequisiteSnapshot;

StaticAssertDecl(sizeof(ClusterR4PrerequisiteSnapshot) == 64,
				 "R4 prerequisite snapshot must remain 64 bytes");
StaticAssertDecl(offsetof(ClusterR4PrerequisiteSnapshot, jcmk_generation) == 16,
				 "R4 prerequisite JCMK generation offset must remain 16");
StaticAssertDecl(offsetof(ClusterR4PrerequisiteSnapshot, grammar_fingerprint) == 56,
				 "R4 prerequisite fingerprint offset must remain 56");

extern ClusterUndoBlock0Result
cluster_undo_block0_logical_slot(const ClusterUndoBlock0LogicalKey *logical, uint32 *slot);
extern bool cluster_undo_block0_root_valid(const ClusterUndoBlock0ResolvedRoot *root);
extern bool cluster_undo_block0_backend_has_resources(void);
extern bool cluster_undo_block0_root_matches(const ClusterUndoBlock0ResolvedRoot *observed,
											 const ClusterUndoBlock0ResolvedRoot *expected);
extern bool cluster_undo_block0_generation_matches(const ClusterUndoBlock0Generation *observed,
												   const ClusterUndoBlock0Generation *expected);
extern bool cluster_undo_block0_generation_advance(const ClusterUndoBlock0Generation *current,
												   ClusterUndoBlock0Generation *next);
extern bool cluster_undo_block0_state_transition_allowed(ClusterUndoBlock0SlotState from,
												 ClusterUndoBlock0SlotState to);

/* Separate block0 subregion embedded after the ordinary undo DATA bank. */
extern Size cluster_undo_block0_shmem_size(uint32 frame_count);
extern bool cluster_undo_block0_shmem_init_region(void *address, Size size, uint32 frame_count,
												  bool found);
extern void cluster_undo_block0_shmem_detach(void);

extern ClusterUndoBlock0Result
cluster_undo_block0_frame_reserve_batch(uint32 count, ClusterUndoBlock0FrameToken *tokens);
extern void cluster_undo_block0_frame_release(ClusterUndoBlock0FrameToken *token);

extern ClusterUndoBlock0Result
cluster_undo_block0_admit_runtime(const ClusterUndoBlock0LogicalKey *logical,
								  const ClusterUndoBlock0ResolvedRoot *root,
								  const ClusterUndoBlock0AuthorityProof *proof,
								  ClusterUndoBlock0FrameToken *token, ClusterUndoBlock0Pin *pin,
								  char **page);
extern ClusterUndoBlock0Result
cluster_undo_block0_reserve(const ClusterUndoBlock0LogicalKey *logical,
							const ClusterUndoBlock0ResolvedRoot *expected_root,
							const ClusterUndoBlock0AuthorityProof *proof,
							ClusterUndoBlock0Pin *pin);
extern ClusterUndoBlock0Result
cluster_undo_block0_lock_content(ClusterUndoBlock0Pin *pin,
								 const ClusterUndoBlock0Generation *expected,
								 ClusterUndoBlock0Mode mode, char **page);
extern ClusterUndoBlock0Result
cluster_undo_block0_pin(const ClusterUndoBlock0LogicalKey *logical,
						const ClusterUndoBlock0ResolvedRoot *expected_root,
						const ClusterUndoBlock0Generation *expected, ClusterUndoBlock0Mode mode,
						const ClusterUndoBlock0AuthorityProof *proof, ClusterUndoBlock0Pin *pin,
						char **page);
extern ClusterUndoBlock0Result
cluster_undo_block0_copy_resident(const ClusterUndoBlock0LogicalKey *logical,
								  const ClusterUndoBlock0ResolvedRoot *expected_root,
								  const ClusterUndoBlock0Generation *expected,
								  const ClusterUndoBlock0AuthorityProof *proof,
								  char private_page[BLCKSZ],
								  ClusterUndoBlock0Generation *observed_generation);
extern ClusterUndoBlock0Result
cluster_undo_block0_copy_readonly(const ClusterUndoBlock0LogicalKey *logical,
								  const ClusterUndoBlock0ResolvedRoot *read_root,
								  const ClusterUndoBlock0Generation *expected,
								  const ClusterUndoBlock0AuthorityProof *proof,
								  char private_page[BLCKSZ]);
extern ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *expected_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	ClusterUndoBlock0Generation *observed_generation);
/* Same exact sample, but never queues behind the resident content lock and
 * never fills a missing slot.  CAPACITY_UNAVAILABLE means the caller must
 * drop its later-ranked lock and retry from its original deadline. */
extern ClusterUndoBlock0Result
cluster_undo_block0_sample_resident_generation_conditional(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *expected_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	ClusterUndoBlock0Generation *observed_generation);
extern ClusterUndoBlock0Result
cluster_undo_block0_prove_strict_empty(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0AuthorityProof *proof);
extern ClusterUndoBlock0Result
cluster_undo_block0_recovery_private_begin(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *redo_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	bool allow_absent,
	ClusterUndoBlock0RecoveryGuard *guard,
	char private_page[BLCKSZ],
	bool *exists);
extern void
cluster_undo_block0_recovery_private_finish(ClusterUndoBlock0RecoveryGuard *guard,
											const char *successor_page,
											XLogRecPtr replay_lsn,
											bool write_image,
											bool fsync_parent);
extern void
cluster_undo_block0_recovery_private_abort(ClusterUndoBlock0RecoveryGuard *guard);
extern ClusterUndoBlock0Result cluster_undo_block0_provision_begin(
	const ClusterUndoBlock0LogicalKey *logical,
	const ClusterUndoBlock0ResolvedRoot *target_root,
	const ClusterUndoBlock0AuthorityProof *proof,
	ClusterUndoBlock0FrameToken *token,
	ClusterUndoBlock0Pin *pin,
	char **unpublished_page,
	bool *creator);
extern void cluster_undo_block0_provision_publish(ClusterUndoBlock0Pin *pin,
	XLogRecPtr init_lsn);
extern void cluster_undo_block0_provision_abort(ClusterUndoBlock0Pin *pin);
extern bool cluster_undo_block0_verify_clean_census(
	const ClusterUndoBlock0ResidentCensusItem *items, uint32 count);
/* Normal Startup's closed pass only: never an online eviction interface. */
extern bool cluster_undo_block0_normal_start_empty(void);
extern bool
cluster_undo_block0_normal_start_discard(const ClusterUndoBlock0ResidentCensusItem *item,
										 uint32 frame_index);
extern void cluster_undo_block0_mark_wal_dirty(ClusterUndoBlock0Pin *pin,
											  XLogRecPtr wal_lsn);
extern void cluster_undo_block0_flush_sync(ClusterUndoBlock0Pin *pin,
									 const char *successor_page,
									 XLogRecPtr required_wal_lsn,
									 bool fsync_parent);
extern void cluster_undo_block0_unpin(ClusterUndoBlock0Pin *pin);
extern ClusterR4PrerequisiteSnapshot cluster_undo_block0_r4_prerequisite_snapshot(void);
extern bool
cluster_undo_block0_r4_publish_ready(const ClusterR4PrerequisiteSnapshot *expected);

typedef struct ClusterR4StartupCompletionContextV1
	ClusterR4StartupCompletionContextV1;
typedef struct RfRootResourceAdmissionV1 RfRootResourceAdmissionV1;
typedef struct RfRecordClosureProofV1 RfRecordClosureProofV1;
typedef struct RfPageResourceProofV1 RfPageResourceProofV1;
typedef struct RfSideResourceProofSetV1 RfSideResourceProofSetV1;

typedef enum ClusterR4StartupCompletionResultV1
{
	CLUSTER_R4_STARTUP_COMPLETION_OK = 0,
	CLUSTER_R4_STARTUP_COMPLETION_RETRY = 1,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_LINEAGE = 2,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_ROOT = 3,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_RECORD = 4,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_PAGE = 5,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_SIDE = 6,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_CENSUS = 7,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_IO = 8,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_DIRTY = 9,
	CLUSTER_R4_STARTUP_COMPLETION_BLOCKED_DEPENDENCY = 10,
	CLUSTER_R4_STARTUP_COMPLETION_INVALID = 11
} ClusterR4StartupCompletionResultV1;

extern ClusterR4StartupCompletionResultV1
cluster_undo_block0_r4_startup_begin(
	int timeout_ms, ClusterR4StartupCompletionContextV1 **out);
extern ClusterR4StartupCompletionResultV1
cluster_undo_block0_r4_startup_close_next(
	ClusterR4StartupCompletionContextV1 *context,
	const RfRootResourceAdmissionV1 *root,
	const RfRecordClosureProofV1 *record,
	const RfPageResourceProofV1 *page,
	const RfSideResourceProofSetV1 *side);
extern ClusterR4StartupCompletionResultV1
cluster_undo_block0_r4_startup_finalize(
	ClusterR4StartupCompletionContextV1 **context);
extern void cluster_undo_block0_r4_startup_abort(
	ClusterR4StartupCompletionContextV1 **context);

#endif /* CLUSTER_UNDO_BLOCK0_H */
