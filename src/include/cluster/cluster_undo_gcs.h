/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs.h
 *	  Shared-undo block GCS integration -- owner-as-master routing +
 *	  coherent grant / PI data plane (spec-5.22b D2).
 *
 *	  D1 (cluster_undo_resid.h) named an undo block as a first-class cluster
 *	  resource whose master IS the owning instance.  D2 wires that identity
 *	  into the real data plane: owner-as-master routing (this file's D2-1
 *	  layer), reader S-grant / writer X-grant with PI invalidation, a
 *	  remaster serve-gate, and the physical migration of undo segments onto
 *	  shared storage.
 *
 *	  D2-1 (owner-as-master routing, this increment) declares the two
 *	  routing predicates.  An undo resource NEVER hash-routes: its master is
 *	  the encoded owner_node (cluster_undo_resid_master), and the GRD/GCS
 *	  hash-master lookups fail closed on the undo class (D1-5 guard, 53R9Q).
 *	  master==self selects the local fast path (no network grant); the
 *	  owner-incarnation epoch self-check that L364 requires before serving
 *	  from that fast path is applied on the grant/acquire path (D2-3), where
 *	  the co-sampled live-authority triple is available -- this pure routing
 *	  predicate is the node-id half only, so cluster_unit links it
 *	  standalone.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_gcs.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_GCS_H
#define CLUSTER_UNDO_GCS_H

#include "cluster/cluster_grd.h"				/* ClusterResId */
#include "cluster/cluster_pcm_lock.h"			/* PcmState / PcmLockTransition */
#include "cluster/cluster_runtime_visibility.h" /* ClusterLiveAuthority */
#include "cluster/storage/cluster_undo_alloc.h" /* ClusterUndoPathIntent */

/*
 * cluster_undo_block_lookup_master -- owner-as-master routing entry.
 *
 *	Returns the encoded owner_node (via cluster_undo_resid_master); an undo
 *	resource is NEVER hash-routed.  cluster_grd_lookup_master /
 *	cluster_gcs_lookup_master fail closed on the undo class (D1-5 guard,
 *	ERRCODE_CLUSTER_UNDO_RESID_HASH_ROUTED), so this is the single legal
 *	master-lookup entry for undo resources.
 */
extern int32 cluster_undo_block_lookup_master(const ClusterResId *undo_resid);

/*
 * cluster_undo_block_master_is_self -- local fast-path routing gate.
 *
 *	true iff this instance is the owning master of the undo resource
 *	(owner_node == cluster_node_id), i.e. the owner reading/writing its own
 *	undo can take the local path without a network grant.  This is the pure
 *	routing half; the owner-incarnation epoch self-check that guards actually
 *	serving from the local fast path (L364) lands on the grant/acquire path
 *	(D2-3), not here.
 */
extern bool cluster_undo_block_master_is_self(const ClusterResId *undo_resid);

/*
 * cluster_undo_path_uses_shared_root -- pure physical-root decision (D2-2).
 *
 *	Returns true iff an undo segment with the given path intent resolves to
 *	the shared cluster_fs root (cluster.shared_data_dir) rather than the
 *	local DataDir.  Pure: takes the mode inputs explicitly (no globals), so
 *	cluster_unit drives every mode combination standalone.
 *
 *	  RUNTIME_SHARED     -> shared iff (peer_mode && coherence_on)
 *	  MATERIALIZED_LOCAL -> always local (P1-3 hard contract, spec-5.22b §3.6)
 *
 *	cluster_undo_path_resolve (cluster_undo_alloc.c) and the redo write
 *	surface (cluster_undo_xlog.c) read the live peer_mode / coherence globals
 *	and delegate the decision here so the two path builders never diverge
 *	(own-instance undo split-brain, Hardening v1.0.1 裁决 A).
 */
extern bool cluster_undo_path_uses_shared_root(ClusterUndoPathIntent intent, bool peer_mode,
											   bool coherence_on);

/*
 * cluster_undo_intent_for_owner (the per-call intent derivation every undo
 * smgr / path call site uses) is a static inline in cluster_undo_alloc.h so
 * the ~30 call sites take no link dependency on this routing object.
 */

/*
 * ClusterUndoGrantResult -- reader S-grant outcome (spec-5.22b §2.2, D2-3).
 *
 *	The authority triple co-sampled with the granted block image (never
 *	max-merged: it must stay the authority the bytes were shipped under), plus
 *	the semantic reply status.  Under owner-as-master the owner is both master
 *	and holder, so a successful grant maps to GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER
 *	(Q3★A: 2-way holder ship; the 6.12i undo-TT-fetch wire is reused verbatim,
 *	no new reply enum).
 */
typedef struct ClusterUndoGrantResult {
	uint64 origin_epoch;	 /* owner incarnation epoch co-sampled with bytes */
	XLogRecPtr live_hwm_lsn; /* owner durable AND TT-applied high-water */
	uint64 tt_generation;	 /* owner TT-slot generation (anti-alias; D3 uses) */
	uint8 status;			 /* GCS_BLOCK_REPLY_* semantic status (Q3★A) */
} ClusterUndoGrantResult;

/*
 * cluster_undo_grant_armed -- pure coherence gate (D2-3, §3.5/Q8).
 *
 *	The mastered reader S-grant path is armed iff cluster.undo_gcs_coherence is
 *	on AND the deployment is peer-mode.  Off in either dimension => the caller
 *	keeps its pre-D2 authority-less fetch path (D2 lands inert at the default).
 *	Pure (mode inputs explicit, no globals) so cluster_unit drives it standalone.
 */
extern bool cluster_undo_grant_armed(bool coherence_on, bool peer_mode);

/*
 * cluster_undo_grant_admissible -- pure reader S-grant admissibility (D2-3,
 * §3.2/§3.3).
 *
 *	Decide whether an owner-shipped undo-block image + co-sampled authority may
 *	be admitted as a coherent S view of undo_resid.  Two ANDed fail-closed
 *	dimensions (Rule 8.A -- any doubt returns false, never admits):
 *	  (1) SEGMENT-generation anti-ABA: the ref's encoded generation (segment
 *	      wrap_count) must still match the reader's expected generation (D1
 *	      cluster_undo_resid_generation_matches), else the segment was recycled
 *	      and the ref is stale.
 *	  (2) owner-incarnation epoch + durable coverage: reuses the pure D-i2 gate
 *	      cluster_vis_live_authority_covers_policy (epoch match + hwm present +
 *	      hwm >= anchor_lsn).  anchor_lsn is InvalidXLogRecPtr for a block-level
 *	      grant with no specific page version (the version-coverage gate is the
 *	      D3/D6 verdict consumer's, which owns the tuple's anchor).
 *
 *	The per-slot TT generation (auth.tt_generation) is a separate finer ABA
 *	dimension resolved at D3 (§3.3 dim 3), not here.
 */
extern bool cluster_undo_grant_admissible(const ClusterResId *undo_resid,
										  uint32 expected_generation, ClusterLiveAuthority auth,
										  uint64 local_epoch, XLogRecPtr anchor_lsn);

/*
 * cluster_undo_grant_reader_pcm_mode / _transition -- reader lock contract
 * (D2-3, §2.2).  The reader takes the undo block in PCM S mode via the
 * read-first N->S transition; the writer/cleaner path (X via N->X) is D2-4.
 * The transition-legality table itself is owned + tested by cluster_pcm_lock.
 */
extern PcmState cluster_undo_grant_reader_pcm_mode(void);
extern PcmLockTransition cluster_undo_grant_reader_pcm_transition(void);

/*
 * cluster_undo_block_acquire_shared -- reader S-grant primitive (D2-3, §2.2).
 *
 *	A peer reads owner N's undo block (block0 TT header is D3's first consumer)
 *	through owner-as-master routing:
 *	  - not armed (coherence off / non-peer) -> false: caller keeps its old
 *	    authority-less path (fresh refs stay 53R97 fail-closed).
 *	  - master==self -> the owner reads its own undo; the owner-incarnation
 *	    self-check + local fast-path land in D6 (see cluster_undo_gcs_grant.c),
 *	    so this increment fail-closes self rather than serve an unproven local
 *	    read (Rule 8.A).
 *	  - master!=self (live owner) -> the owner (authority + holder) ships the
 *	    current image over the reused 6.12i wire; the requesting peer consumes
 *	    the shipped image and NEVER opens the foreign undo file (invariant #8).
 *	    Admitted only through cluster_undo_grant_admissible.
 *
 *	Returns true with *res populated and *dst_block holding the coherent image;
 *	false (any miss / DENIED / doubt) -> the caller MUST keep 53R97 (Rule 8.A,
 *	never false-visible).  Runtime object (heavy deps): not in the standalone
 *	cluster_unit link; forward-covered by the D2-7/D6 TAP legs.
 */
extern bool cluster_undo_block_acquire_shared(const ClusterResId *undo_resid,
											  uint32 expected_generation, char *dst_block,
											  ClusterUndoGrantResult *res);

#endif /* CLUSTER_UNDO_GCS_H */
