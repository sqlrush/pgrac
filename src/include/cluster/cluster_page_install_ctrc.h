/*-------------------------------------------------------------------------
 *
 * cluster_page_install_ctrc.h
 *     Protected storage input under original native CTRC replay owners.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *     src/include/cluster/cluster_page_install_ctrc.h
 *
 * NOTES
 *     No ordinary RR guard/serial/pin conversion or borrowed-owner release.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_INSTALL_CTRC_H
#define CLUSTER_PAGE_INSTALL_CTRC_H

#include "access/generic_xlog.h"
#include "cluster/cluster_page_authority_ctrc.h"
#include "cluster/cluster_space_identity.h"
#include "storage/bufpage.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_recovery_stamp.h"
struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;
struct ClusterRecoveryUndoEntry;

typedef struct RfPageCtrcReadV1 {
	bool ready;
	PGAlignedBlock image;
} RfPageCtrcReadV1;

/* Original post-DONE Startup, its complete CTRC observation and a promoted
 * native replay-set PAGE object for this exact MAIN version. Five native
 * reads validate SPACE0/SPACE1/MAIN and then reread both SPACE pages. Every
 * storage observation is bracketed by original owner/scope/PAGE checks.
 * Keep Start, work, inputs and all PAGE backing storage alive until return.
 * Store the by-value result in separate caller-owned storage. It is an input
 * image, not authority, completion, FPI preparation or an installation proof.
 * Refusal returns ready=false and a zero image; ERROR propagates. Neither path
 * releases PAGE/IR/WALR, retires original debt or writes any caller input. */
extern RfPageCtrcReadV1 rf_page_recovery_ctrc_replays_read_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before);

typedef struct RfPageCtrcFpiV1 {
	bool ready;
	bool rewritten;
	GenericXLogRecoveryImageV1 frame;
} RfPageCtrcFpiV1;

/* Same original native custody as the reader above. Freeze the complete work
 * and same-order physical TT observations in one budgeted private allocation;
 * only the original accepted TX result/canonical C0 factories may supply each
 * terminal. Project all eight ITLs, then prepare a private Generic frame and
 * revalidate all original inputs, results and custody after token allocation.
 * Keep the complete original work/physical array and backing storage alive.
 * Zero subjects accepts only an originally accepted empty result and a page
 * with no outstanding ITL. ready && !rewritten returns a zero frame without
 * minting a token. Refusal returns an entirely zero value; ERROR propagates
 * after freeing only private memory. No WAL, installation, discharge, new
 * authority or service completion is produced. Store the result separately
 * from all borrowed inputs; memory_budget bounds only the private snapshot. */
extern RfPageCtrcFpiV1 rf_page_recovery_ctrc_replays_itl_fpi_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

typedef struct RfPageCtrcFpiInsertV1 {
	GenericXLogRecoveryImageV1 image;
	ClusterRecoveryStampHandleV1 window;
	XLogRecPtr record_start;
	XLogRecPtr record_end;
	bool wal_flushed;
} RfPageCtrcFpiInsertV1;

/* Original accepted post-DONE Start/work and all native replay owners.
 * Supply an unpromoted native PAGE object for exactly this one image target,
 * with no caller-held LWLocks. Copy/hash and native record capacity preparation
 * precede the exact single-target PAGE promotion. All borrowed inputs and
 * backing storage must remain alive and immutable through return and later
 * installation/discharge; store this by-value result separately from them.
 * memory_budget covers both simultaneous full-work snapshots, including the
 * original F4 producer's snapshot. False/ERROR retain original PAGE/IR/WALR;
 * the caller destroys PAGE even if promotion succeeded but insertion failed.
 * Only exact PRE_INSERT cancellation is allowed before insertion. Any later
 * failure preserves failed window debt. wal_flushed proves this WAL leg only,
 * not target installation, discharge, DURABLE, COMPLETE or OPEN. Never reuse
 * an outstanding operation to begin a second record. */
extern RfPageCtrcFpiInsertV1 rf_page_recovery_ctrc_replays_fpi_insert_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const GenericXLogRecoveryImageV1 *image,
	Size subject_count, Size memory_budget);

/* Consume the same original post-DONE Start/work/all-native custody and its
 * immutable INSERTED, flushed FPI. Only its exact promoted PAGE may be held;
 * no other caller LWLocks. Full raw work is frozen in one private allocation
 * bounded by memory_budget, and every borrowed input stays alive/immutable.
 * Validate actual flush/timeline, installed writer and full SPACE identity;
 * install only the valid before or byte-exact canonical result, then sync and
 * full postread. The canonical LSN/origin identify the actual recovery writer.
 * A true value means only process-local target_synced, never discharge,
 * DURABLE, COMPLETE or OPEN. False/ERROR retains PAGE/IR/WALR and marks only
 * the exact still-owned window failed; ERROR propagates after private cleanup.
 * This API writes no caller input, does not extend storage and releases no
 * original owner, receipt or window. */
extern bool rf_page_recovery_ctrc_replays_fpi_install_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	const RfPageCtrcAuthorityV1 *authority, const RfPageCtrcFpiInsertV1 *inserted,
	Size subject_count, Size memory_budget);

typedef enum RfPageCtrcStepPhaseV1 {
	RF_PAGE_CTRC_STEP_REFUSED = 0,
	RF_PAGE_CTRC_STEP_WAIT,
	RF_PAGE_CTRC_STEP_UNCHANGED,
	RF_PAGE_CTRC_STEP_TARGET_SYNCED,
	RF_PAGE_CTRC_STEP_FPI_DURABLE,
	RF_PAGE_CTRC_STEP_UNCHANGED_SYNCED
} RfPageCtrcStepPhaseV1;

typedef struct RfPageCtrcStepResultV1 {
	RfPageCtrcStepPhaseV1 phase;
	RfPageCtrcFpiInsertV1 inserted;
} RfPageCtrcStepResultV1;

/* Execute one supplied original target from canonical ITL preparation through
 * the original Generic/F4 WAL leg and strict sync/postread. Enter with its
 * unpromoted single-target PAGE object and zero caller-held LWLocks. Full work,
 * physical observations, source, SPACE/before and accepted TX remain fixed.
 * Preparation's PAGE is dropped only for hash/WAL capacity preparation; after
 * exact re-promotion, reread and compare every original MAIN/SPACE byte before
 * PRE_INSERT. Keep every borrowed input and backing object alive/immutable.
 * Before the first PAGE, pin existing cluster_fs MAIN/SPACE raw descriptors,
 * namespace/inode/extent and current process/ResourceOwner. All five reads,
 * WAL-gap reread, write/sync/postread use those same descriptors; no VFD is
 * opened in that window. Other storage backends refuse. Close the private
 * descriptors on every return/ERROR without releasing any original grant.
 * memory_budget covers simultaneous private snapshots, original F4 and the
 * complete bounded storage preparation allocation.
 * WAIT in these older entries is only an actual conditional PAGE collision.
 * UNCHANGED is a protected
 * observation, TARGET_SYNCED only local installation; neither clears debt nor
 * publishes DURABLE/COMPLETE/OPEN. PAGE remains held after successful promotion
 * except during the explicit pre-WAL gap; caller destroys it on every exit.
 * False/ERROR never releases IR/WALR or receipts, and retains exact failed F4
 * debt. ERROR propagates after private cleanup. Store the by-value result apart
 * from borrowed inputs; do not reinsert an outstanding operation. */
extern RfPageCtrcStepResultV1 rf_page_recovery_ctrc_replays_itl_step_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before,
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

/* Same native step, additionally requiring the first protected MAIN image
 * to equal the complete fixed graph terminal bytes. Only the independently
 * verified storage checksum is normalized. Keep canonical and all backing
 * storage immutable/disjoint through return; refusal never authorizes redo
 * over a predecessor, discharge or a second pending insertion. */
extern RfPageCtrcStepResultV1 rf_page_recovery_ctrc_replays_itl_graph_step_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const struct ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

/* Opt in to recording the original FPI DURABLE before releasing the same raw
 * descriptors and private snapshots used for sync/full postread. Revalidate
 * full original custody, installed writer, flush/range and target identity on
 * both sides of that transition. FPI_DURABLE retains the window and every
 * first/pin/companion/IR/WALR obligation; it never clears a window or supplies
 * service completion. Close failure or late loss refuses and marks the exact
 * window failed even after DURABLE. A saved result is not a held-FD proof for
 * later discharge. The two older step entries retain TARGET_SYNCED semantics.
 * All original budget, immutable-input, PAGE and ERROR contracts still apply. */
extern RfPageCtrcStepResultV1 rf_page_recovery_ctrc_replays_itl_graph_durable_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const struct ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

/* Also sync an unchanged terminal MAIN page under the original PAGE and raw
 * descriptors, then repeat the complete five-read MAIN/SPACE observation and
 * compare every byte with preparation. Only UNCHANGED_SYNCED reports this
 * local completion; the older entries still return observation-only UNCHANGED.
 * No FPI, WAL or write is produced for an unchanged page. Rewritten pages use
 * the original FPI_DURABLE branch. Both unchanged and rewritten pages check
 * original native cache responsibilities under preparation PAGE. Retire only
 * an exact clean, unpinned native CURRENT/N predecessor via
 * the original bufmgr invalidation; dirty/first/PCM/PI/CR/CF owners are retained.
 * Native cache contention also returns WAIT before insertion. Once absent,
 * check that local observation at the existing current gates, including the
 * off-PAGE WAL gap. Unexpected residency refuses with original window debt;
 * an absence observation never grants future PAGE or mutation authority.
 * False/ERROR/close failure retains all debt;
 * neither phase grants post-return FD custody, discharge or service completion.
 * All original immutable-input, budget and cleanup contracts apply. */
extern RfPageCtrcStepResultV1 rf_page_recovery_ctrc_replays_itl_graph_sync_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const struct ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

struct ClusterSharedFsPageV1;
typedef struct RfPageCtrcHeldStepV1 {
	RfPageCtrcStepResultV1 result;
	struct ClusterSharedFsPageV1 *storage;
} RfPageCtrcHeldStepV1;

/* Opt in to retaining the SAME original raw MAIN/SPACE descriptors only on
 * FPI_DURABLE, after all original sync/postread, F4 and currentness checks.
 * The returned non-NULL storage belongs to this process/ResourceOwner and is
 * allocated in TopMemoryContext. Keep the original Start and borrowed inputs
 * alive; close storage before retiring that owner. The caller must save both
 * values before callbacks/ERROR and close on every eventual local cleanup.
 * All other phases and ERROR close locally as before. No descriptor is
 * reopened or added, and the original budget still covers the preparation.
 * This transfers only private FD cleanup, never PAGE/IR/WALR, window debt,
 * discharge or COMPLETE/OPEN authority. The four older entries still close
 * their descriptors on every return. */
extern RfPageCtrcHeldStepV1 rf_page_recovery_ctrc_replays_itl_graph_hold_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const struct ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

typedef struct RfPageCtrcUnchangedV1 {
	bool ready;
	ClusterSpaceIdentity space;
	RfPageVersionV1 before;
	BlockNumber blockno;
	PGAlignedBlock page, space0, space1;
} RfPageCtrcUnchangedV1;

typedef struct RfPageCtrcHeldTargetV1 {
	RfPageCtrcHeldStepV1 step;
	RfPageCtrcUnchangedV1 unchanged;
} RfPageCtrcHeldTargetV1;

/* Separate opt-in: also retain the same descriptors on UNCHANGED_SYNCED,
 * together with all three original raw pages. The original held entry above
 * still retains only FPI_DURABLE. Save the entire value and FD before any
 * callback; the caller's fixed budget must cover the retained baseline as
 * well as every raw-handle preparation peak. No new FPI/window or mutation
 * is produced for an unchanged target. The baseline is an observation only,
 * never a first/pin/history/companion discharge or completion grant. */
extern RfPageCtrcHeldTargetV1 rf_page_recovery_ctrc_replays_itl_graph_hold_target_smgr_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source,
	RfPageCtrcAuthorityV1 *authority, const struct ClusterSpaceIdentity *space,
	BlockNumber blockno, const RfPageVersionV1 *expected_before, const char canonical[BLCKSZ],
	const struct ClusterRecoveryUndoEntry *physical, Size physical_count, Size memory_budget);

/* Final read of an original Start-owned unchanged baseline and SAME raw FD.
 * Zero caller LWLocks, full original work/source/accepted TX and exact PAGE
 * are mandatory. Freeze inputs before promotion, five-read and compare every
 * MAIN/SPACE byte, including header/checksum bytes. No sync/write/WAL/reopen
 * or window access. WOULD_BLOCK keeps original custody; refusal/ERROR leaves
 * caller cleanup and every original obligation intact. Original Start must
 * revalidate the complete saved value after PAGE/proof destruction. */
extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_unchanged_readback_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const RfPageCtrcUnchangedV1 *unchanged, struct ClusterSharedFsPageV1 *storage,
	Size subject_count, Size memory_budget);

/* Read back an original pending DURABLE operation using its SAME held raw
 * MAIN/SPACE descriptors. Enter with its unpromoted one-target PAGE object
 * and zero caller LWLocks. Freeze the full work/source/operation and describe
 * the frame before PAGE promotion; memory_budget bounds the work snapshot.
 * Recheck original accepted TX, installed writer, complete F4/protection and
 * actual flush/timeline, then five-read SPACE/MAIN and require every canonical
 * result byte, including actual writer LSN/origin/checksum. Before/third
 * versions refuse; no WAL, write, sync, reopen or buffer invalidation occurs.
 * PAGE/cache collision returns WOULD_BLOCK without failing the window. Late
 * refusal/ERROR retains original storage and owners; an entered read failure
 * marks only the exact still-owned window failed. Caller destroys PAGE on
 * every exit, and retains original storage cleanup on normal/ERROR paths.
 * OK is a protected readback only, never first/pin/companion discharge,
 * window clear, retention release or independent/joint COMPLETE/OPEN. */
extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_fpi_readback_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const RfPageCtrcFpiInsertV1 *inserted, struct ClusterSharedFsPageV1 *storage,
	Size subject_count, Size memory_budget);

/* Final readback of an original Start-owned completed operation after its
 * exact F4 was cleared. Require the ORIGINAL saved result address and SAME
 * retained raw FD on every current check; an equivalent caller copy cannot
 * enter. All pending-reader PAGE/source/accepted-TX/descriptor/installed-writer/
 * actual-flush/cache/five-read/canonical-byte checks remain mandatory. The
 * pending API above still requires a nonfailed DURABLE F4. No WAL/write/sync,
 * reopen, window mutation, cleanup transfer, lifetime, discharge or service
 * grant. False/ERROR leaves original Start cleanup and all shared debt intact;
 * caller destroys PAGE and revokes its original episode on late failure. */
extern RfPageProofDetailV1 rf_page_recovery_ctrc_replays_completed_readback_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const RfPageCtrcFpiInsertV1 *inserted, struct ClusterSharedFsPageV1 *storage,
	Size subject_count, Size memory_budget);

/* A private by-value full image from the original protected readback. The
 * detail is OK only for the entire verified native page; every refusal returns
 * a zero image. Store the returned value apart from every borrowed input and
 * keep all inputs immutable through return. memory_budget includes one extra
 * BLCKSZ image. This is observation, not first/pin/companion or completion
 * authority. */
typedef struct RfPageCtrcReadbackImageV1 {
	RfPageProofDetailV1 detail;
	PGAlignedBlock image;
} RfPageCtrcReadbackImageV1;

/* Original Start's PAGE-owner result, produced only after its completed
 * operation and SAME descriptors pass the protected whole-page readback.
 * These are the frozen family/outcome/rule values, not caller-selectable
 * policies. replay_source_index is the original complete ROOT/input index
 * of the replay tail, not the new FPI's writer or record source. The
 * target indexes describe this immutable PAGE plan, not persistent IDs.
 * original operation supplies a SEPARATE full writer and actual FPI
 * coordinates (all zero for unchanged pages). Coordinates alone are not a
 * canonical WAL reference without raw bytes. No FD, pointer, PID or local
 * clock is part of this value.
 *
 * This proves this PAGE owner's exact synced after-image only. Complete
 * G_ref/G_owner, native/base/lifetime qualification, first/pin/history and
 * companion discharge, and the original joint COMPLETE-to-OPEN remain due.
 * It is not an independent completion certificate. Keep the original Start,
 * work, full source set and custody live; revalidate them before graph use.
 * Refusal clears every field except detail. */
typedef struct RfPageCtrcOwnerProofV1 {
	RfPageProofDetailV1 detail;
	uint16 owner_family; /* frozen row 10: PAGE/VM/FSM/retained WAL */
	uint16 outcome;      /* 1: APPLIED_AND_DURABLE for this exact PAGE */
	uint16 rule;         /* 1: TYPED_EFFECT_DURABLE; 6: EXACT_ALREADY_APPLIED */
	uint32 target_index, target_count;
	uint32 replay_source_index, replay_source_count;
	ClusterSpaceIdentity space;
	BlockNumber blockno;
	RfPageVersionV1 before;
	ClusterWalWriterToken writer; /* actual recovery FPI writer; not replay source */
	XLogRecPtr record_start, record_end;
	PGAlignedBlock image;
} RfPageCtrcOwnerProofV1;

/* One PAGE owner's canonical atom3 bytes, coupled by value to its actual
 * protected after-image. This is not a graph node or release certificate.
 * The graph producer still binds the exact transaction/complete effect scope
 * and all other required witnesses, owners, base and lifetime qualifications. */
typedef struct RfPageCtrcOwnerCompletionV1 {
	RfPageCtrcOwnerProofV1 owner;
	Size length;
	uint8 completion[MAXPGPATH + 96];
} RfPageCtrcOwnerCompletionV1;

/* Complete original PAGE target set, not a completed G_owner or release.
 * Each entry owns exact canonical atom2/atom3 bytes from its actual reader. */
typedef struct RfPageCtrcOwnerEvidenceV1 {
	RfPageCtrcOwnerCompletionV1 completed;
	Size evidence_length;
	uint8 evidence[BLCKSZ + MAXPGPATH + 96];
} RfPageCtrcOwnerEvidenceV1;

typedef struct RfPageCtrcOwnersV1 {
	RfPageProofDetailV1 detail;
	uint32 count;
	RfPageCtrcOwnerEvidenceV1 *entries;
} RfPageCtrcOwnersV1;

/* Every original reference remains present, in original order. An owner
 * index names only the actual current PAGE containing a non-FREE ITL;
 * UINT32_MAX retains an unmatched, historical, FREE or tuple obligation.
 * This adjacency input is not transaction-effect coverage or discharge. */
typedef struct RfPageCtrcOwnerReferenceV1 {
	uint32 reference_index;
	uint32 owner_index;
} RfPageCtrcOwnerReferenceV1;

struct ClusterRecoveryCensusReferencesV1;
typedef struct RfPageCtrcOwnerReferencesV1 {
	RfPageProofDetailV1 detail;
	RfPageCtrcOwnersV1 owners;
	const struct ClusterRecoveryCensusReferencesV1 *references;
	uint32 count;
	RfPageCtrcOwnerReferenceV1 *entries;
} RfPageCtrcOwnerReferencesV1;

#define CLUSTER_PAGE_CTRC_OWNER_GRAPH_V1 1
typedef struct RfPageCtrcOwnerGraphEntryV1 {
	uint32 reference_index, owner_index;
	Size key_length, evidence_length;
	const uint8 *key, *evidence;
	/* Optional current ITL kind1 key and complete actual after-image atom2.
	 * NULL/zero retains an unresolved field, never a zero-reference proof. */
	Size reference_key_length, reference_evidence_length;
	const uint8 *reference_key, *reference_evidence;
} RfPageCtrcOwnerGraphEntryV1;

/* Complete reference order, with canonical PAGE owner contributions only for
 * matched current non-FREE ITLs. Unmatched entries have no key/evidence.
 * A byte-exact current ITL also exposes its canonical DATA field key, with
 * full T from the twice-checked original forest member and the SAME owner's
 * complete after-image witness. Evidence borrows related.owners.entries;
 * keys belong to entries. Keep both original result allocations alive.
 * Changed/moved/invalid fields and historical/FREE/tuple references retain
 * NULL reference fields, including when a PAGE owner key is available.
 * This creates no discharged_by edge, complete graph or discharge verdict. */
typedef struct RfPageCtrcOwnerGraphV1 {
	RfPageProofDetailV1 detail;
	RfPageCtrcOwnerReferencesV1 related;
	uint32 count;
	RfPageCtrcOwnerGraphEntryV1 *entries;
} RfPageCtrcOwnerGraphV1;

extern RfPageCtrcReadbackImageV1 rf_page_recovery_ctrc_replays_completed_image_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const RfPageCtrcFpiInsertV1 *inserted, struct ClusterSharedFsPageV1 *storage,
	Size subject_count, Size memory_budget);
extern RfPageCtrcReadbackImageV1 rf_page_recovery_ctrc_replays_unchanged_image_v1(
	const struct ClusterRecoveryStartV1 *start, const struct ClusterColdCtrcWorkV1 *work,
	const struct ClusterRecoveryCtrcReplaySetSourceV1 *source, RfPageCtrcAuthorityV1 *authority,
	const RfPageCtrcUnchangedV1 *unchanged, struct ClusterSharedFsPageV1 *storage,
	Size subject_count, Size memory_budget);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PAGE_INSTALL_CTRC_H */
