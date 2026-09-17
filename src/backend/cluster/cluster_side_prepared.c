/*-------------------------------------------------------------------------
 *
 * cluster_side_prepared.c
 *	  RF-SIDE D-SIDE-03 — PREPARED/in-doubt binding + resolution
 *	  judgements (implementation).
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-03 / §2.3 / §4 / §5.1 U-SIDE-06/07.
 *
 *	  Pure judgement over the four typed facts; no persistent state, no
 *	  mutation, no locks.  The production 2PC/TT wiring supplies the
 *	  facts and owns the durable pending store + RECO resolution.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_side_prepared.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_side_prepared.h"

ClusterSidePreparedVerdict
cluster_side_prepared_verdict(const ClusterSidePreparedInput *in)
{
	/*
	 * §2.3 PREPARED: all four facts are required.  In particular the
	 * pending entry must be database-scoped durable — an origin-local
	 * file or recoverer-local cache is never weight (the caller's
	 * pending_durable_ok fact carries that proof).  Any false keeps the
	 * transaction IN-DOUBT-polarity-blocked: no commit/abort projection
	 * may be published from a partial prepare.
	 */
	if (in == NULL)
		return CLUSTER_SIDE_PREPARED_BLOCKED;
	if (!in->prepare_redo_ok || !in->pending_durable_ok || !in->tt_undo_match
		|| !in->gid_identity_match)
		return CLUSTER_SIDE_PREPARED_BLOCKED;
	return CLUSTER_SIDE_PREPARED_IN_DOUBT;
}

bool
cluster_side_prepared_resolve_ready(const ClusterSidePreparedResolveInput *in)
{
	/*
	 * §2.3 resolution: the terminal (COMMIT_PREPARED / ABORT_PREPARED)
	 * redo must exist, the pending/prepare record must match EXACTLY
	 * (U-SIDE-07: identity conflicts are symmetrically BLOCKED — a
	 * terminal naming a different GID/identity than the pending record
	 * can never resolve it), and the side completion must hold:
	 * COMMIT needs the matching TT truth, ROLLBACK needs verified undo
	 * completion.  A premature abort (undo not complete) or a terminal
	 * without the matching pending (terminal-before-prepare) is BLOCKED
	 * (§4 rows: restart never auto-aborts; locks/resources release only
	 * after the matching resolution is durable verified).
	 */
	if (in == NULL)
		return false;
	return in->terminal_redo_ok && in->pending_match && in->tt_undo_complete;
}

bool
cluster_side_prepared_gid_identity_ok(const char *gid, uint16 gidlen, uint32 available_bytes)
{
	/*
	 * GID identity leg (implementation): a well-formed GID is
	 * non-empty, bounded by MAXPGPATH (the same cap PG's 2PC path uses
	 * for gid — twophase.c rejects longer GIDs at prepare), and fully
	 * inside the record payload the caller extracted it from.  An empty,
	 * oversized or truncated GID can never name a matching pending
	 * entry — identity is BLOCKED, never guessed.
	 */
	if (gid == NULL || gidlen == 0 || gidlen > MAXPGPATH || available_bytes < (uint32)gidlen)
		return false;
	return gid[0] != '\0';
}
