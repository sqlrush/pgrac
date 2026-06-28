/*-------------------------------------------------------------------------
 *
 * cluster_resolver_cache.h
 *	  pgrac shared (per-instance) resolver cache -- a SEARCH-shortcut memo for
 *	  the own-instance CR Source 3 WRAP_ANY by-xid durable scan (spec-5.55).
 *
 *	  The expensive resolver leg is the O(local_segments * TT_SLOTS_PER_SEGMENT)
 *	  WRAP_ANY by-xid scan that CR Source 3 runs when a committed deleter's ITL
 *	  slot was recycled (cluster_cr.c, cluster_tt_durable.c).  Every backend that
 *	  reconstructs a CR image of the same recently-deleted row re-runs that whole
 *	  scan independently.  This memo caches the position the LAST scan matched --
 *	  (xid_epoch, raw_xid, origin) -> {seg, slot, wrap} -- so a peer backend can
 *	  RE-VALIDATE that one slot in O(1) (cluster_tt_slot_durable_lookup) instead
 *	  of re-searching every segment.
 *
 *	  The cached {seg,slot,wrap} is a POSITION HINT, never an authoritative value
 *	  (spec-5.55 §3.1, all 8.A):
 *	   (1) memo non-authoritative: the commit_scn is ALWAYS a fresh durable
 *	       re-read (cluster_tt_slot_durable_lookup); commit_scn_debug is
 *	       observability only, never returned.
 *	   (2) exact-(xid,wrap) re-validation: a recycled / wrap-bumped slot fails the
 *	       lookup -> probe miss -> the caller re-scans (a pure search shortcut).
 *	   (3) acceptance same-segment rerun: a memo hit runs the SAME wrap_suspect
 *	       acceptance gate (cluster_cr_accept_resolved_scn: WRAP_ANY + the CURRENT
 *	       retention horizon + the sticky retention reliability) as a fresh scan
 *	       -> verdict-equivalent by construction; the memo NEVER short-circuits
 *	       acceptance.
 *	   (4) xid_epoch fence: a single monotonic own-instance FullTransactionId
 *	       epoch (cluster_resolver_xid_epoch) makes a hint from epoch E a key MISS
 *	       in E+1, before raw_xid can be reused -- the cross-incarnation ambiguity
 *	       belt.  (pgrac segments the xid space by (origin_node_id, xid) compound
 *	       key, NOT by value-range, so own-instance raw-xid reuse period == one
 *	       own-node FullTransactionId epoch == this counter's granularity.)
 *	   (5) cross-instance fail-closed: own-instance scans only (CR Source 3).
 *	   (6) COMMITTED-only: the by-xid scan only matches TT_SLOT_COMMITTED.
 *
 *	  Two modes, both default OFF (entries == 0 -> true zero memory, byte-
 *	  identical to the spec-3.22 path):
 *	   - cluster.resolver_cache_enabled (TRUST): a re-validated + accepted hit
 *	     resolves WITHOUT the O(segments) fresh scan.
 *	   - cluster.resolver_cache_measure (DIAGNOSTIC, §0.6 value gate): the
 *	     authoritative fresh scan ALWAYS runs and the hint is never trusted; only
 *	     the counters (would-the-memo-hit, would-re-validation+acceptance-pass)
 *	     are recorded -- this is how the value gate is (re-)measured.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.55-shared-resolver-cache.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_resolver_cache.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RESOLVER_CACHE_H
#define CLUSTER_RESOLVER_CACHE_H

#ifndef FRONTEND

#include "postgres.h"

#include "cluster/cluster_scn.h" /* SCN */

/*
 * GUCs (registered in cluster_guc.c, spec-5.55 D7).  All PGC_POSTMASTER: the
 * entry count + mode switches are fixed at shmem-reservation time.  entries == 0
 * (the default) is true zero memory; the region is still registered (so the
 * shmem-region-count baseline is deterministic) but its size_fn returns 0.
 */
extern int cluster_shared_resolver_cache_entries;
extern bool cluster_resolver_cache_enabled; /* TRUST mode */
extern bool cluster_resolver_cache_measure; /* DIAGNOSTIC mode (value gate) */

/* ---- shmem region lifecycle (cluster_shmem.c framework) ---- */
extern Size cluster_resolver_cache_shmem_size(void);
extern void cluster_resolver_cache_shmem_init(void);
extern void cluster_resolver_cache_shmem_register(void);
extern void cluster_resolver_cache_request_lwlocks(void);

/* ---- mode predicates ---- */

/* true when the trust path may skip the fresh scan on an accepted hit. */
extern bool cluster_resolver_cache_trust(void);

/* ---- spec-5.55 D2: own-instance xid-wraparound fence ---- */

/*
 * cluster_resolver_xid_epoch -- the single monotonic own-instance xid epoch.
 *	A hint cached at epoch E is a key MISS once the own node's xid counter wraps
 *	to E+1 (before raw_xid can be reused), so a memo hit can never span two
 *	incarnations of the same 32-bit raw xid (gate (4)).  O(1).
 */
extern uint64 cluster_resolver_xid_epoch(void);

/* ---- spec-5.55 D4/D5: probe + install (trust path) ---- */

/*
 * spec-5.57 D5 (cross-instance identity contract, frozen; no data plane here):
 *	the memo's (origin_node, xid_epoch) is OWN-INSTANCE-ONLY at 5.55 -- the caller
 *	passes its own node id and the by-xid scan is local.  spec-5.57 freezes the
 *	cross-instance forward semantics: origin_node != cluster_node_id is the
 *	class③ runtime-warm-remote ROUTING dimension (where to fetch undo from), and
 *	xid_epoch is the fence that, extended to be GES-remaster-aware, invalidates a
 *	cross-instance hint on remaster.  These are ENABLED only when the Stage 6
 *	(#119) cross-instance undo data plane lands; until then the CR walker
 *	fail-closes a runtime-warm remote origin (53R9G) before any resolve, so the
 *	memo is never consulted cross-instance.  See Spec: spec-5.57 §2.4 / §3.5.
 */

/*
 * cluster_resolver_cache_probe -- look up the memo for (current-epoch, raw_xid,
 *	origin) and, on a key match, LOCK-FREE re-validate the hint slot via
 *	cluster_tt_slot_durable_lookup (gate (1)+(2)+(4)).  Returns true and a
 *	CANDIDATE *out_scn (a fresh durable re-read) iff the slot STILL binds
 *	(raw_xid, hint.wrap) COMMITTED.  The CALLER MUST still run the SAME acceptance
 *	gate (cluster_cr_accept_resolved_scn, gate (3)) it runs on a fresh scan --
 *	probe does NO acceptance.  Returns false (no count) when the region is off.
 */
extern bool cluster_resolver_cache_probe(uint16 origin_node, TransactionId raw_xid, SCN *out_scn);

/*
 * cluster_resolver_cache_install -- cache the matched {seg,slot,wrap} position
 *	hint for (current-epoch, raw_xid, origin).  The caller MUST pass an own-node
 *	(gate (5)) COMMITTED (gate (6)) RESOLVED result that PASSED acceptance
 *	(gate (3)).  No-op when the region is off.
 */
extern void cluster_resolver_cache_install(uint16 origin_node, TransactionId raw_xid, uint16 seg,
										   uint16 slot, uint16 wrap, SCN commit_scn);

/* count an acceptance outcome on a memo hit (run by the caller, gate (3)). */
extern void cluster_resolver_cache_count_acceptance(bool passed);

/* ---- counters (spec-5.55 D8; read lock-free by dump) ---- */
extern uint64 cluster_resolver_cache_lookup_count(void);
extern uint64 cluster_resolver_cache_key_present_count(void);
extern uint64 cluster_resolver_cache_epoch_miss_count(void);
extern uint64 cluster_resolver_cache_hit_count(void);
extern uint64 cluster_resolver_cache_revalidate_miss_count(void);
extern uint64 cluster_resolver_cache_acceptance_pass_count(void);
extern uint64 cluster_resolver_cache_acceptance_failclosed_count(void);
extern uint64 cluster_resolver_cache_install_count(void);
extern uint64 cluster_resolver_cache_evict_count(void);
extern uint64 cluster_resolver_cache_nonown_skip_count(void);
extern uint64 cluster_resolver_cache_nonterminal_skip_count(void);

/* ---- test/diagnostic ---- */
extern int cluster_resolver_cache_live_entries(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_RESOLVER_CACHE_H */
