/*-------------------------------------------------------------------------
 *
 * cluster_side_route.c
 *	  RF-SIDE D-SIDE-01 — total route registry + §2.1 verdicts
 *	  (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-01 / §2.1 / §3.2 / §5.1 U-SIDE-01..03.
 *
 *	  The registry mirrors the §3.2 routing matrix: opcode-granular rows
 *	  for the matrix-named opcodes, rmgr-granular rows otherwise, and no
 *	  row at all for unknown rmgrs (lookup fails -> the caller treats it
 *	  as BLOCKED).  The verdict function is pure, so cold and online
 *	  wrappers always agree on route + verdict for the same record.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_route.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/rmgr.h" /* RM_* IDs */
#include "access/xact.h"
#include "catalog/pg_control.h" /* XLOG_* opcodes */
#include "cluster/cluster_rf_route.h"
#include "cluster/cluster_side_route.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "storage/standbydefs.h" /* XLOG_STANDBY_* */

/*
 * D-SIDE-01 consumes the same exhaustive generated opcode manifest as
 * RF-PAGE.  This file owns only the SIDE disposition of a manifest row; it
 * never carries a second opcode list.  Therefore a source/manifest change
 * makes the shared 137-row census RED once, instead of letting PAGE and SIDE
 * silently drift apart.
 */
static bool
side_manifest_find(uint8 rmid, uint16 opcode, RfOpcodeRouteV1 *route, bool *active)
{
	uint8 normalized;
	size_t i;

	if (opcode > UINT8_MAX || route == NULL || active == NULL)
		return false;
	if (rmid == RM_XACT_ID)
		normalized = (uint8)opcode & XLOG_XACT_OPMASK;
	else
		normalized = (uint8)opcode & ~XLR_INFO_MASK;
	for (i = 0; i < rf_opcode_route_manifest_count_v1(); i++) {
		RfOpcodeRouteV1 candidate;
		bool candidate_active;

		if (!rf_opcode_route_manifest_entry_v1(i, &candidate, &candidate_active))
			return false;
		if (candidate.rmid == rmid && candidate.normalized_info == normalized) {
			if (rmid == RM_XACT_ID
				&& (((uint8)opcode & XLOG_XACT_HAS_INFO) & ~candidate.legal_info_flags) != 0)
				return false;
			*route = candidate;
			*active = candidate_active;
			return true;
		}
	}
	return false;
}

static void
side_manifest_disposition(const RfOpcodeRouteV1 *route, bool active, ClusterSideRouteRow *out)
{
	out->rmid = route->rmid;
	out->opcode = route->normalized_info;
	out->opcode_mask = route->legal_info_flags;
	out->kind = CLUSTER_SIDE_ROUTE_BLOCKED;
	if (!active)
		return;
	if (route->record_owner == RF_ROUTE_OWNER_PAGE_CODEC) {
		out->kind = CLUSTER_SIDE_ROUTE_PAGE;
		return;
	}
	if (route->record_owner == RF_ROUTE_OWNER_LOGICAL_NOOP) {
		out->kind = CLUSTER_SIDE_ROUTE_PROVED_NOOP;
		out->noop_reason = "LOGICAL_MESSAGE_ONLY";
		return;
	}
	if (route->record_owner != RF_ROUTE_OWNER_SIDE_TYPED)
		return;

	if (route->codec_id == RF_ROUTE_CODEC_SIDE_CLUSTER_UNDO) {
		if (route->normalized_info != XLOG_HW_RESERVE)
			out->kind = CLUSTER_SIDE_ROUTE_TT_UNDO;
		return;
	}
	if (route->codec_id != RF_ROUTE_CODEC_SIDE_STANDARD)
		return;

	switch (route->rmid) {
	case RM_XLOG_ID:
		if (route->normalized_info == XLOG_NOOP || route->normalized_info == XLOG_SWITCH
			|| route->normalized_info == XLOG_RESTORE_POINT
			|| route->normalized_info == XLOG_BACKUP_END) {
			out->kind = CLUSTER_SIDE_ROUTE_PROVED_NOOP;
			out->noop_reason = "CONTROL_ONLY";
		}
		break;
	case RM_XACT_ID:
		if (route->normalized_info != XLOG_XACT_INVALIDATIONS)
			out->kind = CLUSTER_SIDE_ROUTE_TT_UNDO;
		break;
	case RM_SMGR_ID:
		out->kind = CLUSTER_SIDE_ROUTE_STORAGE;
		break;
	case RM_CLOG_ID:
	case RM_MULTIXACT_ID:
	case RM_COMMIT_TS_ID:
		out->kind = CLUSTER_SIDE_ROUTE_PROJECTION;
		break;
	case RM_STANDBY_ID:
		if (route->normalized_info == XLOG_STANDBY_LOCK
			|| route->normalized_info == XLOG_RUNNING_XACTS) {
			out->kind = CLUSTER_SIDE_ROUTE_PROVED_NOOP;
			out->noop_reason = "PRIMARY_NO_STANDBY_CONSUMER";
		}
		break;
	case RM_HEAP2_ID:
		/* XLOG_HEAP2_NEW_CID: physical recovery has no logical
			 * decoding consumer. REWRITE remains authority-blocked. */
		if (route->normalized_info == 0x70) {
			out->kind = CLUSTER_SIDE_ROUTE_PROVED_NOOP;
			out->noop_reason = "LOGICAL_ONLY";
		}
		break;
	case RM_GIST_ID:
		/* XLOG_GIST_ASSIGN_LSN is the rmgr-declared no-op. */
		if (route->normalized_info == 0x70) {
			out->kind = CLUSTER_SIDE_ROUTE_PROVED_NOOP;
			out->noop_reason = "DECLARED_RMGR_NOOP";
		}
		break;
	default:
		break;
	}
}

bool
cluster_side_route_lookup(uint8 rmid, uint16 opcode, ClusterSideRouteRow *out)
{
	RfOpcodeRouteV1 route;
	bool active;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!side_manifest_find(rmid, opcode, &route, &active))
		return false;
	side_manifest_disposition(&route, active, out);
	return true;
}

ClusterSideRouteVerdict
cluster_side_route_verdict(const ClusterSideRouteRow *row)
{
	/* Pure: cold and online wrappers always agree (U-SIDE-02).  A
	 * PROVED_NOOP row carries its proof kind; every other row either
	 * applies under its owning primitive or is BLOCKED. */
	if (row == NULL)
		return CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED;
	switch (row->kind) {
	case CLUSTER_SIDE_ROUTE_PROVED_NOOP:
		return CLUSTER_SIDE_ROUTE_VERDICT_PROVED_NOOP;
	case CLUSTER_SIDE_ROUTE_PAGE:
	case CLUSTER_SIDE_ROUTE_TT_UNDO:
	case CLUSTER_SIDE_ROUTE_PROJECTION:
	case CLUSTER_SIDE_ROUTE_STORAGE:
		return CLUSTER_SIDE_ROUTE_VERDICT_APPLY;
	case CLUSTER_SIDE_ROUTE_BLOCKED:
	default:
		return CLUSTER_SIDE_ROUTE_VERDICT_BLOCKED;
	}
}

#else /* !USE_PGRAC_CLUSTER */

/* Disable-cluster build: this file compiles to nothing. */

#endif /* USE_PGRAC_CLUSTER */
