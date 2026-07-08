/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs.c
 *	  Shared-undo block GCS integration -- owner-as-master routing +
 *	  coherent grant / PI data plane (spec-5.22b D2).
 *
 *	  D2-1 (this increment) ships owner-as-master routing: the two routing
 *	  predicates that keep undo resources off the GRD/GCS hash-master path
 *	  (their authority lives at the owning instance).  The grant / PI /
 *	  serve-gate / physical-migration legs (D2-2 .. D2-5) land here as they
 *	  are implemented.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_gcs.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_mode.h" /* cluster_node_id */
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_resid.h"

/*
 * cluster_undo_block_lookup_master -- owner-as-master routing entry.
 *
 *	The master of an undo resource is the encoded owner_node (D1's
 *	cluster_undo_resid_master), never a shard hash: the undo authority lives
 *	at the owning instance.  This is the single legal master-lookup entry for
 *	the undo class; cluster_grd_lookup_master / cluster_gcs_lookup_master fail
 *	closed on it (D1-5 guard, 53R9Q).
 */
int32
cluster_undo_block_lookup_master(const ClusterResId *undo_resid)
{
	Assert(undo_resid != NULL);
	Assert(cluster_undo_resid_is_undo(undo_resid));

	return cluster_undo_resid_master(undo_resid);
}

/*
 * cluster_undo_block_master_is_self -- local fast-path routing gate.
 *
 *	true iff this instance owns the undo resource (owner_node ==
 *	cluster_node_id), so the owner can read/write its own undo without a
 *	network grant.  The owner-incarnation epoch self-check that L364 requires
 *	before actually serving from the local fast path is applied on the
 *	grant/acquire path (D2-3), where the co-sampled live-authority triple is
 *	available; this predicate is the pure node-id half.
 */
bool
cluster_undo_block_master_is_self(const ClusterResId *undo_resid)
{
	Assert(undo_resid != NULL);
	Assert(cluster_undo_resid_is_undo(undo_resid));

	return cluster_undo_resid_master(undo_resid) == cluster_node_id;
}

/*
 * cluster_undo_path_uses_shared_root -- pure physical-root decision (D2-2).
 *
 *	The single source of truth that both undo path builders consult:
 *	cluster_undo_path_resolve (runtime segment I/O) and the redo write
 *	surface in cluster_undo_xlog.c.  Keeping the decision here (not
 *	duplicated in each builder) is what prevents own-instance undo
 *	split-brain -- runtime reading the shared copy while redo stamps the
 *	local copy (Hardening v1.0.1 裁决 A).
 */
bool
cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent, bool peer_mode, bool coherence_on)
{
	/*
	 * P1-3 hard contract: a dead-origin materialized copy (recovery rebuilt
	 * it in the local DataDir) NEVER migrates to the shared root, in any
	 * mode.  Redirecting it would send dead-origin resolve / CR reads to an
	 * empty shared path (spec-5.22b §3.6, R1).
	 */
	if (intent == CLUSTER_UNDO_PATH_MATERIALIZED_LOCAL)
		return false;

	Assert(intent == CLUSTER_UNDO_PATH_RUNTIME_SHARED);

	/*
	 * Own live runtime undo migrates to the shared cluster_fs root only when
	 * the whole physical migration is armed: a declared multi-node
	 * deployment (peer_mode) AND cluster.undo_gcs_coherence on.  Either off
	 * => local DataDir, so D2 lands inert at the default (裁决 A).
	 */
	return peer_mode && coherence_on;
}

/*
 * cluster_undo_grant_armed -- pure coherence gate (D2-3).
 *
 *	See cluster_undo_gcs.h for the contract.  Mirrors the physical-migration
 *	gate above (peer_mode && coherence_on): the grant/PI data plane and the
 *	physical migration arm together, so a peer never grants against undo bytes
 *	that were never migrated to the shared root.
 */
bool
cluster_undo_grant_armed(bool coherence_on, bool peer_mode)
{
	return coherence_on && peer_mode;
}

/*
 * cluster_undo_grant_admissible -- pure reader S-grant admissibility (D2-3).
 *
 *	See cluster_undo_gcs.h for the two-dimension fail-closed contract.  The two
 *	admit conditions are ANDed; any single failure returns false so the caller
 *	keeps the pre-existing 53R97 fail-closed boundary (Rule 8.A -- this only
 *	widens "admit when provable", never "admit when unprovable").
 */
bool
cluster_undo_grant_admissible(const ClusterResId *undo_resid, uint32 expected_generation,
							  ClusterLiveAuthority auth, uint64 local_epoch, XLogRecPtr anchor_lsn)
{
	if (undo_resid == NULL)
		return false;

	/*
	 * (1) Segment-generation anti-ABA (D1 §3.3 dim 1).  A recycled segment
	 * reuses (undo_segment, block_no) for different content, so a generation
	 * mismatch means the reference is stale -- fail closed, never a match.
	 */
	if (!cluster_undo_resid_generation_matches(undo_resid, expected_generation))
		return false;

	/*
	 * (2) Owner-incarnation epoch + durable coverage (D-i2/D-i3).  Reuse the
	 * pure live-authority window gate: authority sampled under a different
	 * membership epoch cannot be trusted, a reply with no live authority
	 * (invalid hwm) is never guessed, and the durable high-water must cover the
	 * version anchor (Invalid at the block level -> epoch + presence only).
	 */
	if (!cluster_vis_live_authority_covers_policy(anchor_lsn, auth, local_epoch))
		return false;

	return true;
}

/*
 * cluster_undo_grant_reader_pcm_mode / _transition -- reader lock contract
 * (D2-3).  The reader takes the undo block in PCM S mode via the read-first
 * N->S transition (the legal read-first pair; cluster_pcm_lock owns and tests
 * the transition-legality table).  The writer/cleaner X path is D2-4.
 */
PcmState
cluster_undo_grant_reader_pcm_mode(void)
{
	return PCM_LOCK_MODE_S;
}

PcmLockTransition
cluster_undo_grant_reader_pcm_transition(void)
{
	return PCM_TRANS_N_TO_S;
}
