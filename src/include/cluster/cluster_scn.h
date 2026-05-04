/*-------------------------------------------------------------------------
 *
 * cluster_scn.h
 *	  pgrac cluster SCN (System Change Number) typedef + invariants.
 *
 *	  Stage 1.4 ships only the typedef + InvalidScn constant + SCN_VALID
 *	  / SCN_FORMAT macros.  The full encoding layer (scn_encode /
 *	  scn_decode / SCN_NODE_ID_BITS / SCN_LOCAL_BITS) and the comparison
 *	  contract (scn_time_cmp / scn_total_cmp / scn_recovery_cmp + grep
 *	  gate) land at spec-1.15 when local_scn maintenance starts (refer
 *	  to docs/scn-protocol-design.md §3.2 + §3.2.1 v1.1).
 *
 *	  On disk: pd_block_scn (PageHeaderData, stage 1.4), commit_scn
 *	  (ITL slot, stage 1.5), write_scn (ITL slot, stage 1.5), xl_scn
 *	  (WAL record, stage 1.18), and TT slot fields (undo segment,
 *	  stage 1.20) all use this typedef.
 *
 *	  InvalidScn locked to 0 (spec-1.4 §8 Q2 = A) -- matches PG's
 *	  InvalidTransactionId convention; lets MemSet zero-init occupy the
 *	  "not yet set" semantics naturally.  SCN protocol guarantees all
 *	  real values are >= 1 (monotonically advancing per local node), so
 *	  0 is permanently reserved as the "absent" sentinel.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_scn.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.4-block-format-pageheader-scn.md
 *	  Design: docs/scn-protocol-design.md v1.1 §3.2 + §3.2.1
 *	  AD-008 (SCN protocol; Lamport-style distributed counters).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SCN_H
#define CLUSTER_SCN_H

#include "c.h" /* uint64 */


/*
 * SCN -- System Change Number.
 *
 *	Stage 1.4 stub: 8-byte unsigned integer.  Stage 1.15 will overlay
 *	the encoding layer (8b node_id || 56b local_scn) on top of this
 *	typedef; the stub already reserves the byte width so that no on-
 *	disk format change is needed when the encoding layer lands.
 */
typedef uint64 SCN;


/*
 * InvalidScn -- the "absent" / "not yet set" sentinel.
 *
 *	Locked to 0 by spec-1.4 §8 Q2 = A.  Real SCN values always >= 1
 *	(monotonically advancing per local node, per AD-008).
 */
#define InvalidScn ((SCN)0)


/*
 * SCN_VALID -- hot-path validity check.
 *
 *	Prefer this macro over direct comparison so future encoding-layer
 *	changes (spec-1.15+) can centralize semantics here.  In particular,
 *	spec-1.15 will introduce scn_time_cmp / scn_total_cmp /
 *	scn_recovery_cmp comparison functions; business code MUST NOT use
 *	`a < b` / `a == b` / `a > b` on SCN values (CI grep gate enforces;
 *	exception: `scn != InvalidScn` via this macro).
 *
 *	See docs/scn-protocol-design.md §3.2.1.
 */
#define SCN_VALID(scn) ((scn) != InvalidScn)


/*
 * SCN_FORMAT / SCN_FORMAT_ARG -- printf-style format helpers.
 *
 *	Use the (unsigned long) cast in SCN_FORMAT_ARG for portability:
 *	uint64 prints as %lu on 64-bit Linux/macOS and as %llu on 32-bit
 *	platforms; (unsigned long) defers the choice to the caller's ABI
 *	while guaranteeing %lu is always correct.
 */
#define SCN_FORMAT "%lu"
#define SCN_FORMAT_ARG(scn) ((unsigned long)(scn))


/* ============================================================
 * Spec-1.15: SCN encoding layer (8 bit node_id + 56 bit local_scn)
 * ============================================================
 *
 *	docs/scn-protocol-design.md v1.1 §3.1 / §3.2 + AD-008 distributed
 *	Lamport SCN.  Stage 1.4 stub above is preserved unchanged; the
 *	encoding layer below extends the typedef without changing on-disk
 *	width or alignment.
 *
 *	Layout:
 *	    bit 63..56 : node_id  (8 bits;  0..127 valid, 128..255 reserved)
 *	    bit 55..0  : local_scn (56 bits; monotonically advancing)
 *
 *	Stage 1.15 single-node mode: only the local node advance() path
 *	produces real SCN values.  observe(remote_scn) updates a
 *	max_observed_remote statistic but does NOT bump local_scn (that
 *	lands at spec-1.16 Lamport observe).
 *
 *	Frontend visibility: bufpage.h pulls cluster_scn.h into frontend
 *	tools (pg_checksums / pg_resetwal / ...) via the PageHeaderData
 *	layout.  The encoding constants + inline helpers below are pure
 *	bit math (no LWLock / TimestampTz / palloc) and remain available
 *	to frontend code.  Backend-only APIs (LWLock-protected advance /
 *	observe / TimestampTz accessors / Size shmem hooks) are gated
 *	behind #ifndef FRONTEND below.
 */


/*
 * NodeId — pgrac cluster node identifier (spec-0.10).
 *
 *	cluster.node_id GUC range: -1 (unset / single-node fallback) and
 *	0..127 (valid).  SCN encoding only uses 8 bits; valid range is
 *	0..127, 128..255 reserved.  -1 is INVALID for SCN advance —
 *	cluster_scn_advance() ereport(ERROR) when node_id == -1.
 */
typedef int32 NodeId;


#define SCN_NODE_ID_BITS 8
#define SCN_LOCAL_BITS 56
#define SCN_NODE_ID_SHIFT 56
#define SCN_LOCAL_MASK ((uint64)((1ULL << SCN_LOCAL_BITS) - 1))
/* Spec-1.15 L3: node_id 三段 valid/reserved/invalid. */
#define SCN_MAX_VALID_NODE_ID 127
#define SCN_MAX_NODE_ID ((1 << SCN_NODE_ID_BITS) - 1)
#define SCN_MAX_LOCAL SCN_LOCAL_MASK
#define SCN_NODE_ID_VALID(n) ((n) >= 0 && (n) <= SCN_MAX_VALID_NODE_ID)


/*
 * Wraparound watermark thresholds (spec-1.15 D7 stub).
 *
 *	Spec-1.15 L6 reality: at 100us per advance,
 *	  2^50 advances ≈ 3568 years (WARNING throttled 1/min)
 *	  2^55 advances ≈ 228000 years (PANIC)
 *	Theoretical sentinels for monitoring discipline; full wrap
 *	protection (freeze table + reset protocol) lands spec-1.16+.
 */
#define SCN_WRAP_WARNING_THRESHOLD ((uint64)1ULL << 50)
#define SCN_WRAP_PANIC_THRESHOLD ((uint64)1ULL << 55)


/* StaticAssertDecl invariants (cluster_scn.c) */
#define SCN_INVARIANT_BITS_SUM (SCN_NODE_ID_BITS + SCN_LOCAL_BITS)
#define SCN_INVARIANT_SIZE 8


/*
 * Encoding inline helpers.
 */
static inline SCN
scn_encode(NodeId node, uint64 local)
{
	Assert(SCN_NODE_ID_VALID(node));
	Assert(local <= SCN_MAX_LOCAL);
	return ((SCN)((uint8)node) << SCN_NODE_ID_SHIFT) | (local & SCN_LOCAL_MASK);
}

static inline NodeId
scn_node_id(SCN scn)
{
	return (NodeId)(uint8)(scn >> SCN_NODE_ID_SHIFT);
}

static inline uint64
scn_local(SCN scn)
{
	return scn & SCN_LOCAL_MASK;
}


#ifndef FRONTEND

#include "datatype/timestamp.h" /* TimestampTz (backend only) */
#include "access/xlogdefs.h"	/* XLogRecPtr */


/*
 * Comparison contract (spec-1.15 §3.2.1; Q2 + L4):
 *
 *	scn_time_cmp     -- visibility / MVCC; only local_scn matters.
 *	scn_total_cmp    -- ITL / global ordering; local_scn → node_id
 *	                    tie-break (NOT raw uint64 — high node_id bits
 *	                    would corrupt time-order on cross-node ties).
 *	scn_recovery_cmp -- WAL k-way merge / standby apply; local_scn →
 *	                    LSN → node_id three-level tie-break.
 *
 *	CI gate (scripts/ci/check-scn-cmp-gate.sh): outside cluster_scn.{c,h},
 *	business code MUST NOT use `<` / `==` / `>` on SCN-typed values.
 */
extern int scn_time_cmp(SCN a, SCN b);
extern int scn_total_cmp(SCN a, SCN b);
extern int scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn,
							NodeId b_node);


/*
 * Public advance / observe API (single-node Stage 1.15 only).
 */
extern SCN cluster_scn_advance(void);
extern void cluster_scn_observe(SCN remote_scn);
extern SCN cluster_scn_current(void);
extern uint64 cluster_scn_advance_count(void);
extern uint64 cluster_scn_max_observed_remote(void);
extern NodeId cluster_scn_node_id(void);
extern TimestampTz cluster_scn_initialized_at(void);
extern TimestampTz cluster_scn_last_advance_at(void);


/*
 * Shmem hookup.
 */
extern Size cluster_scn_shmem_size(void);
extern void cluster_scn_shmem_init(void);
extern void cluster_scn_shmem_register(void);

#endif /* !FRONTEND */


#endif /* CLUSTER_SCN_H */
