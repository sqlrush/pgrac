/*-------------------------------------------------------------------------
 *
 * cluster_xnode_lever.h
 *	  pgrac spec-6.12 cross-node performance lever runtime -- shared
 *	  per-wave counters + the wave-c visibility resolver memo.
 *
 *	  Owns the "pgrac cluster xnode lever" shmem region: a small block
 *	  of monotonic counters that make each 6.12 wave's behaviour
 *	  observable through the cluster_dump_state 'xnode_lever' category
 *	  (no new pg_proc, no catversion).  Counters tick only while the
 *	  owning wave GUC or cluster.xnode_profile is on; with both off the
 *	  hot paths stay byte-identical.
 *
 *	  Wave c (cluster.page_scn_shortcut): a backend-local, per-top-level-
 *	  transaction memo of TERMINAL remote transaction outcomes keyed by
 *	  the exact ClusterTTStatusKey.  COMMITTED(+scn) / ABORTED are
 *	  immutable facts for an exact key, so replaying them inside one
 *	  transaction skips the repeated TT overlay lookups that spec-5.59
 *	  measured at ~2.7us per tuple with zero amortization.  Non-terminal
 *	  statuses (IN_PROGRESS / SUBCOMMITTED / CLEANED_OUT / UNKNOWN) are
 *	  NEVER memoized (rule 8.A: nothing uncertain is replayed).  Entries
 *	  are scoped to the installing top-level transaction (lxid-stamped),
 *	  which keeps the memo inside the snapshot-horizon guarantees the
 *	  exact-key TT lookup itself relies on.
 *
 *	  Backend-only header (pg_atomic + PGPROC dependencies).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_xnode_lever.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_XNODE_LEVER_H
#define CLUSTER_XNODE_LEVER_H

#include "c.h"
#include "port/atomics.h"
#include "cluster/cluster_scn.h"	   /* SCN */
#include "cluster/cluster_tt_status.h" /* ClusterTTStatusKey / ClusterTTStatus */

/*
 * Shared counters, one region for all 6.12 waves (fields append per wave;
 * the dump emits every field, so the key surface is version-stable within
 * a wave and append-only across waves).
 */
typedef struct ClusterXnodeLeverShared {
	/* ---- wave c: page-SCN shortcut / resolver terminal memo ---- */
	pg_atomic_uint64 c_resolve_count;	   /* remote-ref resolves entered */
	pg_atomic_uint64 c_tt_lookup_count;	   /* TT exact lookups performed */
	pg_atomic_uint64 c_memo_hit_count;	   /* resolves answered by memo */
	pg_atomic_uint64 c_memo_install_count; /* terminal outcomes installed */

	/*
	 * D0 measure legs (tick under cluster.xnode_profile too): how often
	 * the ITL slot already carried a cached COMMITTED stamp at TT-lookup
	 * time (what the naive page-gate would have trusted), and how often
	 * that stamp disagreed with the TT terminal verdict (C1b: ITL stamps
	 * carry the commit SCN value, committed-ness authority stays with
	 * CLOG/TT -- a disagreement here is the direct evidence that trusting
	 * stamps alone would be a false-visible).
	 */
	pg_atomic_uint64 c_stamp_cached_seen_count;
	pg_atomic_uint64 c_stamp_contradicted_count;

	/* ---- wave a: quiescent S-cache via X->S downgrade ---- */
	pg_atomic_uint64 a_downgrade_count;			/* X->S self-downgrades served */
	pg_atomic_uint64 a_downgrade_refused_count; /* candidates that stayed one-shot */
	pg_atomic_uint64 a_fwd_oneshot_count;		/* forwarded X-held reads that stayed
												 * one-shot (read_scache off, or the
												 * pre-㉕ MVP ceiling counter) */

	/* ---- wave a ㉕: remote-holder downgrade (holder != master) ---- */
	pg_atomic_uint64 a_remote_downgrade_count;		   /* holder-side: accepted +
														* durable S grant shipped */
	pg_atomic_uint64 a_remote_downgrade_refused_count; /* holder-side: refused
														* (active ITL / raced /
														* flush or notify failed)
														* -> one-shot fallback */
	pg_atomic_uint64 a_remote_ack_degraded_count;	   /* requester-side: durable
														* grant received but the S
														* registration was denied
														* (notify raced/lost) ->
														* degraded to one-shot
														* (Rule 8.A fail-closed) */

	/* ---- wave e1: GES release-side deterministic handoff ---- */
	pg_atomic_uint64 e1_drain_count;			   /* release drains verified */
	pg_atomic_uint64 e1_grant_count;			   /* identities granted by drains */
	pg_atomic_uint64 e1_invariant_violation_count; /* 8.A-dual: MUST stay 0 */

	/* ---- wave g: block self-containment (active-ITL migration) ---- */
	pg_atomic_uint64 g_active_itl_transfer_count;	/* X-transfers shipped WITH an
													* uncommitted ITL slot (D11
													* deferral lifted) */
	pg_atomic_uint64 g_stamp_skipped_count;			/* commit cleanouts that skipped
													* the stamp (block not resident;
													* drifted ITL -> TT authority) */
	pg_atomic_uint64 g_drift_resolved_via_tt_count; /* reader resolutions of a
													 * stamp-skipped ACTIVE slot
													 * that the TT authority
													 * decided (observability) */

	/* ---- wave e2: master->holder BAST nudge (㉔ form) ---- */
	pg_atomic_uint64 e2_bast_nudge_sent_count;	  /* master: nudges sent alongside
												   * a live-X-holder deny */
	pg_atomic_uint64 e2_bast_nudge_yield_count;	  /* holder: nudges that yielded
												   * (quiescent X->S succeeded) */
	pg_atomic_uint64 e2_bast_nudge_refused_count; /* holder: nudges refused
												   * (active ITL / raced / off) */

	/* ---- wave h: Past Image retention (AD-002 PI orthogonal state) ---- */
	pg_atomic_uint64 h_pi_kept_count;		/* transfers/invalidates that kept
										   * a PI instead of dropping */
	pg_atomic_uint64 h_pi_ineligible_count; /* PI wanted but fell back to the
											 * plain drop (pinned buffer) */

	/* ---- wave h D-h2: PI-holder discard protocol (Q25-A dual trigger) ---- */
	pg_atomic_uint64 h_pi_write_note_count;		/* FlushBuffer noted a tracked-
												 * block write into the ring */
	pg_atomic_uint64 h_pi_note_overflow_count;	/* ring full: note dropped
												 * (fail-safe, PI lingers) */
	pg_atomic_uint64 h_pi_discard_notify_count; /* master issued a PI_DISCARD
												 * directive (wire or local) */
	pg_atomic_uint64 h_pi_discarded_count;		/* holder truly invalidated a
												 * BUF_TYPE_PI buffer */
	pg_atomic_uint64 h_pi_discard_miss_count;	/* directive found no droppable
												 * PI (already gone / pinned /
												 * live-S over-approximation) */
} ClusterXnodeLeverShared;

/* Set once by shmem init; NULL until the region is attached. */
extern PGDLLIMPORT ClusterXnodeLeverShared *ClusterXnodeLeverCtl;

extern Size cluster_xnode_lever_shmem_size(void);
extern void cluster_xnode_lever_shmem_init(void);
extern void cluster_xnode_lever_shmem_register(void);

/*
 * Wave-c resolver memo (backend-local storage, shared counters).
 *
 * cluster_vis_memo_probe: true + fills status/scn only for a terminal
 * outcome installed by THIS top-level transaction under the exact key.
 * cluster_vis_memo_install: no-op unless status is terminal (COMMITTED
 * with a valid SCN, or ABORTED).  Both are no-ops while
 * cluster.page_scn_shortcut is off.
 */
extern bool cluster_vis_memo_probe(const ClusterTTStatusKey *key, uint8 *status_out, SCN *scn_out);
extern void cluster_vis_memo_install(const ClusterTTStatusKey *key, uint8 status, SCN commit_scn);

/*
 * D0 measure hooks for the resolver (cheap; gated on wave GUC or
 * cluster.xnode_profile inside).
 */
extern void cluster_lever_c_note_resolve(void);
extern void cluster_lever_c_note_tt_lookup(bool stamp_cached_present, bool stamp_contradicted);

/*
 * Wave-a counters (ticked from the GCS serve path; cheap, gated on the
 * wave GUC or cluster.xnode_profile inside).
 */
extern void cluster_lever_a_note_downgrade(bool downgraded);
extern void cluster_lever_a_note_fwd_oneshot(void);
extern void cluster_lever_a_note_remote_downgrade(bool downgraded);
extern void cluster_lever_a_note_remote_ack_degraded(void);

/*
 * Wave-g counters (block self-containment; gated on cluster.block_self_contained
 * or cluster.xnode_profile inside).
 */
extern void cluster_lever_g_note_active_itl_transfer(void);
extern void cluster_lever_g_note_stamp_skipped(void);
extern void cluster_lever_g_note_drift_resolved_via_tt(void);

/*
 * Wave-e2 counters (BAST nudge; ticked from the master deny path and the
 * holder-side nudge handler).
 */
extern void cluster_lever_e2_note_nudge_sent(void);
extern void cluster_lever_e2_note_nudge_result(bool yielded);

/*
 * Wave-h counters (Past Image retention; ticked from the bufmgr transfer /
 * invalidate drop sites).
 */
extern void cluster_lever_h_note_pi_kept(void);
extern void cluster_lever_h_note_pi_ineligible(void);

/*
 * Wave-h D-h2 counters (PI-holder discard protocol; ticked from FlushBuffer's
 * write-note, the master's discard fan-out, and the holder's PI drop).
 */
extern void cluster_lever_h_note_write_note(bool overflowed);
extern void cluster_lever_h_note_discard_notify(void);
extern void cluster_lever_h_note_discard_result(bool discarded);

#endif /* CLUSTER_XNODE_LEVER_H */
