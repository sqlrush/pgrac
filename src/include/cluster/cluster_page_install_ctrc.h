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

#include "cluster/cluster_page_authority_ctrc.h"
#include "cluster/cluster_space_identity.h"
#include "storage/bufpage.h"

#ifdef USE_PGRAC_CLUSTER
struct ClusterRecoveryStartV1;
struct ClusterColdCtrcWorkV1;

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

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_PAGE_INSTALL_CTRC_H */
