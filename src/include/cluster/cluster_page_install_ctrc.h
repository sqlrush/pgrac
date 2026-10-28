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
	RF_PAGE_CTRC_STEP_TARGET_SYNCED
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
 * WAIT is only an actual conditional PAGE collision. UNCHANGED is a protected
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

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PAGE_INSTALL_CTRC_H */
