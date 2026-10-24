/* PGRAC: exact recovery stamp/checkpoint interlock, not write authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#ifndef CLUSTER_RECOVERY_STAMP_H
#define CLUSTER_RECOVERY_STAMP_H

#include "cluster/cluster_wal_writer.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_recovery_tt_record.h"
#include "cluster/cluster_recovery_ctrc_record.h"
#include "cluster/cluster_page_authority.h"
#include "access/generic_xlog.h"
#include "storage/spin.h"

#define CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES 320
#define CLUSTER_RECOVERY_STAMP_SLOTS 128
#define CLUSTER_RECOVERY_STAMP_EXIT_V1 1

typedef enum ClusterRecoveryStampKindV1 {
	CLUSTER_RECOVERY_STAMP_C0 = 1,
	CLUSTER_RECOVERY_STAMP_FPI = 2,
	CLUSTER_RECOVERY_STAMP_D0 = 3
} ClusterRecoveryStampKindV1;

typedef enum ClusterRecoveryStampPhaseV1 {
	CLUSTER_RECOVERY_STAMP_EMPTY = 0,
	CLUSTER_RECOVERY_STAMP_PRE_INSERT,
	CLUSTER_RECOVERY_STAMP_INSERTING,
	CLUSTER_RECOVERY_STAMP_INSERTED,
	CLUSTER_RECOVERY_STAMP_DURABLE,
	CLUSTER_RECOVERY_STAMP_ADOPT_PENDING
} ClusterRecoveryStampPhaseV1;

typedef enum ClusterRecoveryStampPollV1 {
	CLUSTER_RECOVERY_STAMP_INVALID = 0,
	CLUSTER_RECOVERY_STAMP_PENDING,
	CLUSTER_RECOVERY_STAMP_COMPLETE
} ClusterRecoveryStampPollV1;

typedef struct ClusterRecoveryStampOwnerV1 {
	ClusterWalWriterToken writer;
	ClusterWalThreadClaimRefV2 target;
	uint64 executor_boot;
	uint64 recovery_serial;
	int32 executor_pid;
} ClusterRecoveryStampOwnerV1;

typedef struct ClusterRecoveryStampKeyV1 {
	uint64 slot_generation;
	uint64 window_seq;
	uint16 target_thread;
} ClusterRecoveryStampKeyV1;

/* Returned by prepare, copied unchanged by the one original executor.
 * The complete operation is value-owned and immutable until retirement. */
typedef struct ClusterRecoveryStampHandleV1 {
	ClusterRecoveryStampKeyV1 key;
	ClusterRecoveryStampOwnerV1 owner;
	ClusterRecoveryStampKindV1 kind;
	uint32 payload_length;
	uint8 payload[CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES];
} ClusterRecoveryStampHandleV1;

/* Local image comparison plus references to the original held protection.
 * The locator is the typed SPACE decoder's expected key; the namespace is
 * the immutable operation owner's. No image bytes or object pointers. */
typedef struct ClusterRecoveryStampFpiV1 {
	GenericXLogRecoveryImageDescriptorV1 image;
	RelFileLocator locator;
	uint32 reserved_zero;
	RfPageRecoveryReferenceV1 protection;
} ClusterRecoveryStampFpiV1;
StaticAssertDecl(sizeof(ClusterRecoveryStampFpiV1) <= CLUSTER_RECOVERY_STAMP_PAYLOAD_BYTES,
	"recovery FPI envelope must fit the existing stamp payload");

/* Process-local WAL-leg result, never embedded in shared memory. The exact
 * image copy remains available for subsequent typed installation. The caller
 * retains the original preflight/serial/pin and this promoted guard on success,
 * refusal after promotion, or ERROR. WAL flush does not retire the window. */
typedef struct ClusterRecoveryFpiInsertV1 {
	GenericXLogRecoveryImageV1 image;
	RfPageAuthorityGuardV1 *guard;
	ClusterRecoveryStampHandleV1 window;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	bool wal_flushed;
} ClusterRecoveryFpiInsertV1;

/* Embedded in the original per-thread replay slot. No pointers or callbacks. */
typedef struct ClusterRecoveryStampSlotV1 {
	slock_t lock;
	ClusterRecoveryStampHandleV1 handle;
	ClusterRecoveryStampPhaseV1 phase;
	ClusterRecoveryStampPhaseV1 adopt_from;
	uint64 executor_attempt_stamp;
	uint64 bound_worker_attempt;
	int32 bound_worker_pid;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	bool failed;
} ClusterRecoveryStampSlotV1;

typedef struct ClusterRecoveryStampCaptureV1 {
	bool captured;
	bool caller_owned;
	uint32 count;
	ClusterRecoveryStampKeyV1 keys[CLUSTER_RECOVERY_STAMP_SLOTS];
} ClusterRecoveryStampCaptureV1;

/* Original validated BGW entry binds its scheduler attempt. Only the
 * original LMON reports exact native-handle BGWH_STOPPED. Unfinished
 * C0/D0 windows remain pending; this grants no adoption/write authority.
 * STOPPED validates a full snapshot outside slot locks, then rechecks the
 * exact operation/binding before recording debt. Refusal never retires it. */
extern bool cluster_recovery_stamp_worker_begin_v1(uint16 thread, uint64 attempt);
extern bool cluster_recovery_stamp_worker_stopped_v1(uint16 thread, uint64 attempt);

/* Call only after original authority, reservation and held-FD preparation.
 * Admits exact C0/D0 bound to the original writer/target/boot/serial;
 * Raw FPI and unknown shapes refuse. Complete codec validation is outside every
 * slot lock; transitions compare the prepared operation without decoding.
 * No WAL or IO and no authority, graph-completion or release qualification.
 * Inputs/output must not alias. Output is zero on refusal. */
extern bool cluster_recovery_stamp_prepare_v1(const ClusterRecoveryStampOwnerV1 *owner,
											  ClusterRecoveryStampKindV1 kind, const uint8 *payload,
											  Size length, ClusterRecoveryStampHandleV1 *out);
/* The sole FPI admission path. Describe the local image before promotion;
 * consume its exact typed target and original promoted PAGE/serial/pin here.
 * This only records PRE_INSERT, never insertion, installation or completion.
 * Retain all borrowed objects and the local image through the actual work.
 * Inputs/output must not alias. Output is zero on ordinary refusal. */
extern bool cluster_recovery_stamp_prepare_fpi_v1(const ClusterRecoveryStampOwnerV1 *owner,
	const RfPageIdentityV1 *target, const GenericXLogRecoveryImageDescriptorV1 *image,
	const RfPageAuthorityGuardV1 *guard, ClusterRecoverySerialGuard *serial_guard,
	ClusterWalRetentionPin *retention_pin, ClusterRecoveryStampHandleV1 *out);
/* Both insertion transitions require the original executor's critical section.
 * INSERTED records the actual XLogInsert start/end; no insertion lock is held.
 * All transitions match the unchanged prepared operation, without allocation,
 * hashing or codec calls inside the critical section or slot spinlock. */
extern bool cluster_recovery_stamp_inserting_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_inserted_v1(const ClusterRecoveryStampHandleV1 *handle,
											   XLogRecPtr start, XLogRecPtr end);
/* Dormant D0 adapter: immediately after the original executor's XLogInsert,
 * bind its returned end and original last-insert header/actual start to this
 * exact INSERTING window. Refusal leaves the window unchanged and cannot
 * cancel post-insert duty. The header checks do not prove payload identity,
 * full raw-record SHA/CRC, owner/graph authority, flush or target durability;
 * the original inserter and the remaining independent proofs are required. */
extern bool cluster_recovery_stamp_ctrc_inserted_v2(const ClusterRecoveryStampHandleV1 *handle,
													XLogRecPtr end);
/* Dormant Startup preparation before any critical section. Prepare the
 * original held-FD D0 target first, then the original exact PRE_INSERT window.
 * If window preparation refuses or raises ERROR, close only this call's
 * newly prepared target; propagate ERROR. Both private outputs publish only
 * together on success and remain unchanged on refusal. Inputs/outputs must
 * be disjoint from each other and the live replay region. The immutable
 * owner/payload and original authority, graph, pins and target reservation
 * remain caller prerequisites. Does not insert WAL or authorize a live path. */
extern bool cluster_recovery_stamp_ctrc_prepare_v2(const ClusterRecoveryStampOwnerV1 *owner,
	const uint8 *payload, Size length, ClusterRecoveryStampHandleV1 *out,
	ClusterUndoSmgrRecoveryStampV1 *target);
/* The original executor's dormant D0 emitter. The caller must have completed
 * all readiness/authority/capacity checks and hold the immutable prepared
 * payload while in its critical section. This performs exactly one native
 * XLogBeginInsert/RegisterData/XLogInsert and immediately binds the original
 * native header and positions. A live native XID, disallowed insertion, any
 * pre-insert mismatch, native error, or post-insert mismatch refuses; after
 * insertion the window is sticky failed and cannot be cancelled or completed.
 * It proves neither WAL flush nor target IO durability, graph/owner authority,
 * full raw-record identity, nor any live production caller. */
extern XLogRecPtr
cluster_recovery_stamp_ctrc_insert_v2(const ClusterRecoveryStampHandleV1 *handle);
/* Dormant combined emitter and immediate complete native-record snapshot.
 * Caller prepares exactly 357B of private scratch before the original critical
 * section, retaining the immutable handle. One native insertion, bounded copy,
 * exact payload/positions/window recheck; no crypto, flush or target IO here.
 * Refusal leaves outputs unchanged, but matched postinsert duty is sticky.
 * Use record_hash_v2 only after the original finish and critical exit. Neither
 * this copy nor its later hash supplies source/graph/owner qualification. */
extern XLogRecPtr cluster_recovery_stamp_ctrc_insert_record_v2(
	const ClusterRecoveryStampHandleV1 *handle, void *raw, Size raw_length, XLogRecPtr *start);
/* Dormant original-startup D0 flush step, in the executor's critical section.
 * Flushes the exact INSERTED window's actual end with its original writer,
 * then independently checks native startup flush coverage and revalidates the
 * writer/window. No slot lock is held across those calls. A matched failure
 * is sticky; native errors retain the original post-insert responsibility.
 * Success leaves INSERTED unchanged: target sync/postread and all other
 * readiness/authority proofs are still required before DURABLE or clear.
 * This is not a generic worker flush receipt or a live production caller. */
extern bool cluster_recovery_stamp_ctrc_flush_v2(const ClusterRecoveryStampHandleV1 *handle);
/* Dormant startup-only finish, inside the original insertion critical section.
 * Flushes this exact INSERTED native end, writes only the held-FD stage bound
 * to all 320B of this payload, directly syncs/reads back, and rechecks the
 * original writer/flush/window before DURABLE and clear. A matched refusal
 * stays failed; native errors retain post-insert duty. The caller retains
 * graph/source/terminal/ROOT/IR/WALR/target reservation throughout. This does
 * not establish those proofs, close the ResourceOwner token or add a live caller. */
extern bool cluster_recovery_stamp_ctrc_finish_v2(const ClusterRecoveryStampHandleV1 *handle,
												  ClusterUndoSmgrRecoveryStampV1 stamp);
/* Dormant single Startup operation in the caller's original critical section:
 * read-only match the caller-owned held D0 stage and complete payload before
 * the native insertion, then snapshot the record and finish that exact window.
 * Pre-insert mismatch leaves the window cancelable. Publish the complete raw
 * record and actual start only after target
 * sync/readback and clear succeed; refusal leaves both outputs unchanged.
 * raw_length must equal the native header, long data header and 320B payload.
 * Outputs must be private and disjoint from each other, handle and live slot.
 * The immutable handle, prepared held-FD token and all original proofs/pins
 * remain the caller's responsibility. Native errors propagate without cancel
 * or critical-section changes. Hash only after the original critical exit.
 * This adds neither a live caller nor qualification or release authority. */
extern XLogRecPtr cluster_recovery_stamp_ctrc_apply_v2(
	const ClusterRecoveryStampHandleV1 *handle, ClusterUndoSmgrRecoveryStampV1 stamp,
	void *raw, Size raw_length, XLogRecPtr *start);
/* Dormant Startup executor, entered outside any critical section after the
 * caller has established and retained all original qualification/proofs/pins.
 * Prepare the held target and window, own exactly one critical section around
 * the original apply, then close the held target and publish private outputs.
 * Pre-insert refusal cancels that exact window; refusal after INSERTING is
 * PANIC with duty retained. Native errors propagate in their original section.
 * Output contract is the same 357B raw/actual-start pair as apply_v2, disjoint
 * from each other, immutable inputs and live replay region. Refusal preserves
 * both outputs. A close refusal may follow already-durable completion; it
 * does not undo or reclassify that completed operation. Hash after return.
 * No live caller, producer qualification, graph or release authority is added. */
extern XLogRecPtr cluster_recovery_stamp_ctrc_execute_v2(
	const ClusterRecoveryStampOwnerV1 *owner, const uint8 *payload, Size length,
	void *raw, Size raw_length, XLogRecPtr *start);
/* Complete the original dormant executor, then hash its owned full native
 * record outside the critical section and publish raw bytes + exact R together.
 * The caller retains the ROOT-selected source index/identity, original writer,
 * cut/pins/qualification and immutable inputs; this does not establish them.
 * The 357B raw buffer and reference output must be private and disjoint from
 * each other, all inputs and the live replay region. Both remain unchanged
 * on refusal/ERROR. A refused hash after completion does not undo/redo or mark
 * the completed operation failed. Native postinsert PANIC remains unchanged.
 * Never backfill this external reference into its D0 payload or same graph;
 * no live caller, semantic completion, floor/issued or release authority. */
extern bool cluster_recovery_stamp_ctrc_execute_reference_v2(
	const ClusterRecoveryStampOwnerV1 *owner, const uint8 *payload, Size length,
	uint32 source_index, const uint8 source_identity_digest[32], void *raw,
	Size raw_length, ClusterRecoveryCTRCReferenceV2 *out);
/* Only the original executor calls durable after real WAL flush and exact
 * target sync/postread. These APIs record completion, never prove it. */
extern bool cluster_recovery_stamp_durable_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_clear_v1(const ClusterRecoveryStampHandleV1 *handle);
/* Cancel only before INSERTING, when no WAL insertion/target mutation began. */
extern bool cluster_recovery_stamp_cancel_v1(const ClusterRecoveryStampHandleV1 *handle);
extern bool cluster_recovery_stamp_fail_v1(const ClusterRecoveryStampHandleV1 *handle);
/* Capture after fixing redo and releasing native insertion locks. No caller
 * lock, IO or wait is retained. Poll never adds later windows or clears one. */
extern bool cluster_recovery_stamp_capture_v1(ClusterRecoveryStampCaptureV1 *out);
extern ClusterRecoveryStampPollV1
cluster_recovery_stamp_poll_v1(const ClusterRecoveryStampCaptureV1 *capture);
#endif
