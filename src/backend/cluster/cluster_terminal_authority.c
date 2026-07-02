/*-------------------------------------------------------------------------
 *
 * cluster_terminal_authority.c
 *	  Smart Fusion terminal authority substrate (spec-6.2).
 *
 *	  The decision here is deliberately conservative.  It is a pure function of
 *	  caller-supplied evidence, so unit tests can pin the fail-closed matrix and
 *	  later data-plane work can reuse it without inventing local exceptions.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_terminal_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_terminal_authority.h"

ClusterTerminalAuthorityReason
cluster_terminal_authority_decide(const ClusterTerminalAuthorityEvidence *ev)
{
	if (ev == NULL)
		return CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN;
	if (!ev->enabled)
		return CLUSTER_TERMINAL_AUTH_DISABLED;
	if (ev->origin_node < 0 || ev->origin_node >= CLUSTER_TERMINAL_AUTH_MAX_NODES)
		return CLUSTER_TERMINAL_AUTH_INVALID_ORIGIN;
	if (!TransactionIdIsNormal(ev->xid))
		return CLUSTER_TERMINAL_AUTH_INVALID_XID;

	if (!ev->epoch_known)
		return CLUSTER_TERMINAL_AUTH_EPOCH_UNKNOWN;
	if (ev->observed_epoch != ev->current_epoch)
		return CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED;

	if (!ev->owner_known)
		return CLUSTER_TERMINAL_AUTH_OWNER_UNKNOWN;
	if (ev->owner_node != ev->origin_node)
		return CLUSTER_TERMINAL_AUTH_OWNER_MISMATCH;

	switch (ev->terminal_state) {
	case CLUSTER_TERMINAL_AUTH_COMMITTED:
		if (!SCN_VALID(ev->terminal_scn))
			return CLUSTER_TERMINAL_AUTH_COMMIT_SCN_MISSING;
		if (ev->durable_commit_required) {
			if (!ev->durable_commit_resolved)
				return CLUSTER_TERMINAL_AUTH_DURABLE_MISSING;
			if (ev->durable_commit_scn != ev->terminal_scn)
				return CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH;
		}
		break;
	case CLUSTER_TERMINAL_AUTH_ABORTED:
		break;
	case CLUSTER_TERMINAL_AUTH_IN_PROGRESS:
		return CLUSTER_TERMINAL_AUTH_NONTERMINAL;
	case CLUSTER_TERMINAL_AUTH_UNKNOWN:
	default:
		return CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN;
	}

	if (ev->retention_required && !ev->retention_proven)
		return CLUSTER_TERMINAL_AUTH_RETENTION_UNPROVEN;

	return CLUSTER_TERMINAL_AUTH_OK;
}

bool
cluster_terminal_authority_allows(const ClusterTerminalAuthorityEvidence *ev)
{
	return cluster_terminal_authority_decide(ev) == CLUSTER_TERMINAL_AUTH_OK;
}

const char *
cluster_terminal_authority_reason_name(ClusterTerminalAuthorityReason reason)
{
	switch (reason) {
	case CLUSTER_TERMINAL_AUTH_OK:
		return "ok";
	case CLUSTER_TERMINAL_AUTH_DISABLED:
		return "disabled";
	case CLUSTER_TERMINAL_AUTH_INVALID_ORIGIN:
		return "invalid_origin";
	case CLUSTER_TERMINAL_AUTH_INVALID_XID:
		return "invalid_xid";
	case CLUSTER_TERMINAL_AUTH_EPOCH_UNKNOWN:
		return "epoch_unknown";
	case CLUSTER_TERMINAL_AUTH_EPOCH_CHANGED:
		return "epoch_changed";
	case CLUSTER_TERMINAL_AUTH_OWNER_UNKNOWN:
		return "owner_unknown";
	case CLUSTER_TERMINAL_AUTH_OWNER_MISMATCH:
		return "owner_mismatch";
	case CLUSTER_TERMINAL_AUTH_TERMINAL_UNKNOWN:
		return "terminal_unknown";
	case CLUSTER_TERMINAL_AUTH_NONTERMINAL:
		return "nonterminal";
	case CLUSTER_TERMINAL_AUTH_COMMIT_SCN_MISSING:
		return "commit_scn_missing";
	case CLUSTER_TERMINAL_AUTH_DURABLE_MISSING:
		return "durable_missing";
	case CLUSTER_TERMINAL_AUTH_DURABLE_MISMATCH:
		return "durable_mismatch";
	case CLUSTER_TERMINAL_AUTH_RETENTION_UNPROVEN:
		return "retention_unproven";
	default:
		return "invalid_reason";
	}
}
