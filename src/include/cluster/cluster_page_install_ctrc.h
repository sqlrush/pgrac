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
 *     No ordinary RR guard/serial/pin conversion or storage mutation.
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

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PAGE_INSTALL_CTRC_H */
