/*-------------------------------------------------------------------------
 *
 * cluster_page_set.h
 *	  RF-PAGE PGDEL-05 — in-memory per-block recovery set builder and
 *	  the §6.3 contributor closure.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-05 ("in-memory per-block recovery-set builder 与
 *	  contributor closure；不是 persistent artifact"), §3.1
 *	  RedoPageChange, §3.3 shape-2 skip, §3.4 exact expected-before
 *	  adjacency, §6 per-block recovery set and conditional merge.
 *
 *	  DESIGN CONTRACT:
 *	    - BlockRecoverySet is a REBUILDABLE in-memory plan: it is not a
 *	      persistent artifact, not a successor authority, not a WAL
 *	      retirement certificate.  A recoverer crash discards it and the
 *	      successor rebuilds from canonical inputs (D3′, spec §6.1).
 *	    - The closure is a PURE judgement over the ordered contributor
 *	      list: same inputs always produce the same verdict (rerun
 *	      determinism, §6.3-9).
 *	    - Raw cross-thread LSN ordering is rejected: adjacency is exact
 *	      PageVersion equality only, and equal SCN with different
 *	      incarnation/identity is NOT adjacency (§3.4, §6.3, PU-27).
 *
 *	  NOT DELIVERED HERE (stays RED, later PGDEL items): the mutation/
 *	  durability/post-read chain (PGDEL-06) and the source census that
 *	  fills the recovery set from live CURRENT/PI/STORAGE (the caller
 *	  uses the PGDEL-04 validators + PGDEL-02 decode to build the
 *	  contributor list; the wiring into the production orchestrator is
 *	  PGDEL-06/07).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_set.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_SET_H
#define CLUSTER_PAGE_SET_H

#include "cluster/cluster_page_source.h"

/*
 * §3.1 RedoPageChange — one page-affecting redo record projected onto
 * the PageVersion contract.  `expected_before`/`result_version` must be
 * VALID versions for the change to participate in a chain (invalid =
 * unknown/gap, never a join); `change_identity` is an opaque
 * exact-equality record identity; `failed_origin_thread` names the
 * failed-origin duty this change belongs to.
 */
typedef struct ClusterPageRedoChange {
	ClusterPageIdentity identity;
	ClusterPageClass page_class;
	ClusterPageVersion expected_before;
	ClusterPageVersion result_version;
	uint64 change_identity; /* opaque exact-equality */
	uint16 failed_origin_thread;
} ClusterPageRedoChange;

/*
 * §6.1 BlockRecoverySet — the in-memory rebuildable plan for ONE block.
 */
typedef struct ClusterBlockRecoverySet {
	uint16 failed_origin_thread;
	ClusterPageIdentity identity;
	ClusterPageClass page_class;
	ClusterPageSourceKind source_kind;		   /* selected source (PGDEL-04) */
	ClusterPageVersion source_version;		   /* selected source's version */
	ClusterPageVersion terminal_version;	   /* required terminal version */
	const ClusterPageRedoChange *contributors; /* ordered same-block chain */
	int n_contributors;
} ClusterBlockRecoverySet;

/*
 * §6.3 contributor-closure result.  OK means the chain is closed:
 *   - every contributor is same-block, classified (not UNKNOWN), with
 *     valid versions;
 *   - c[i].result_version == c[i+1].expected_before for every adjacent
 *     pair (exact equality incl. incarnation; equal SCN with different
 *     incarnation/identity is NOT adjacency, §3.4);
 *   - the first expected_before == source_version (or, for an empty
 *     chain, source_version == terminal_version);
 *   - the last result_version == terminal_version (terminal unique by
 *     construction — the closure FAILS on any mismatch instead of
 *     guessing which terminal is right).
 */
typedef enum ClusterPageClosureResult {
	CLUSTER_PAGE_CLOSURE_OK = 0,
	CLUSTER_PAGE_CLOSURE_GAP,				/* missing/unknown/invalid version join */
	CLUSTER_PAGE_CLOSURE_UNKNOWN_CLASS,		/* a contributor class is UNKNOWN */
	CLUSTER_PAGE_CLOSURE_INCARNATION_CROSS, /* incarnation boundary crossed */
	CLUSTER_PAGE_CLOSURE_THREAD_MISMATCH,	/* contributor of another origin */
	CLUSTER_PAGE_CLOSURE_TERMINAL_MISMATCH, /* chain end != terminal */
	CLUSTER_PAGE_CLOSURE_INVALID_INPUT
} ClusterPageClosureResult;

/*
 * §6.3 closure: validate the recovery set's contributor chain.  Pure and
 * deterministic: the same canonical inputs always produce the same
 * result (rerun determinism).  No mutation, no I/O.
 */
extern ClusterPageClosureResult
cluster_page_contributor_closure(const ClusterBlockRecoverySet *set);

/*
 * §3.3 shape-2 trusted skip: true when a contributor chain is closed
 * FROM `from_version` TO the set's terminal_version — i.e. the change
 * whose result_version equals `from_version` (or the source itself, for
 * an empty chain) is already covered by the closed chain.  Only a
 * closed chain proves coverage: a numeric high-water never does (§3.3).
 */
extern bool cluster_page_contributor_chain_covers(const ClusterBlockRecoverySet *set,
												  const ClusterPageVersion *from_version);

#endif /* CLUSTER_PAGE_SET_H */
