/*-------------------------------------------------------------------------
 *
 * cluster_side_prepared.h
 *	  RF-SIDE D-SIDE-03 — database-scoped PREPARED/in-doubt binding and
 *	  RECO-style resolution judgements.
 *
 *	  Spec: specs/spec-rf-side-typed-recovery-and-shared-terminal.md
 *	  §1.2 D-SIDE-03 ("database-scoped PREPARED pending state、RECO-style
 *	  recovery ownership、exact prepare/terminal binding"), §2.3
 *	  PREPARED/in-doubt contract, §4 crash-matrix PREPARED rows,
 *	  §5.1 U-SIDE-06/07.
 *
 *	  CONTRACT (spec §2.3 PREPARED):
 *	    - prepare 不等于 commit/abort：任何 status projection 必须保留
 *	      in-doubt polarity;
 *	    - pending entry 必须是 database-scoped durable state；
 *	      origin-local file 或 recoverer-local cache 不独立承重;
 *	    - pending missing、同 transaction 不同 GID/identity、
 *	      terminal-before-prepare without verified base、或 TT/undo
 *	      mismatch 一律 BLOCKED;
 *	    - prepared transaction 的 locks/resources 只在 matching
 *	      resolution durable verified 后释放；不因 recoverer 或 node
 *	      重启自动 abort。
 *
 *	  DELIVERED HERE: the judgement layer — `verdict` decides whether a
 *	  prepare is genuinely IN_DOUBT (all four facts hold) and `resolve`
 *	  decides whether a terminal (COMMIT_PREPARED / ABORT_PREPARED) may
 *	  resolve it.  The actual durable pending store and the RECO
 *	  resolution ownership remain the production 2PC/TT wiring (RED).
 *	  No ABI change.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_side_prepared.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SIDE_PREPARED_H
#define CLUSTER_SIDE_PREPARED_H

typedef enum ClusterSidePreparedVerdict {
	CLUSTER_SIDE_PREPARED_IN_DOUBT = 0, /* prepare + pending + TT/undo all match */
	CLUSTER_SIDE_PREPARED_BLOCKED		/* any fact missing/conflicting */
} ClusterSidePreparedVerdict;

/*
 * The four typed facts of a PREPARED state.  All are caller-declared
 * (the production 2PC/TT wiring supplies them); this layer judges the
 * conjunction only.
 */
typedef struct ClusterSidePreparedInput {
	bool prepare_redo_ok;	 /* exact prepare terminal redo seen */
	bool pending_durable_ok; /* database-scoped durable pending entry */
	bool tt_undo_match;		 /* matching shared TT/undo identity */
	bool gid_identity_match; /* same transaction, exact GID/identity */
} ClusterSidePreparedInput;

/*
 * §2.3: PREPARED is IN_DOUBT only when EVERY fact holds.  A missing or
 * conflicting fact is BLOCKED — never a guessed commit/abort, never an
 * origin-local file or recoverer-local cache standing in for the
 * database-scoped pending entry.
 */
extern ClusterSidePreparedVerdict cluster_side_prepared_verdict(const ClusterSidePreparedInput *in);

/*
 * RECO-style resolution (COMMIT_PREPARED / ABORT_PREPARED terminal):
 * ready only when the terminal redo is present, the pending/prepare
 * record matches EXACTLY, and the side completion holds — for COMMIT the
 * matching TT truth, for ROLLBACK the verified undo completion.  A
 * terminal without the matching pending (terminal-before-prepare) or a
 * premature abort (undo not complete) is BLOCKED: locks/resources are
 * released only after the matching resolution is durable verified
 * (§2.3 last bullet; §4 PREPARED rows).
 */
typedef struct ClusterSidePreparedResolveInput {
	bool terminal_redo_ok; /* commit-prepared / rollback-prepared redo */
	bool pending_match;	   /* exact pending/prepare identity match */
	bool tt_undo_complete; /* COMMIT: TT match; ROLLBACK: undo done */
} ClusterSidePreparedResolveInput;

extern bool cluster_side_prepared_resolve_ready(const ClusterSidePreparedResolveInput *in);

/*
 * cluster_side_prepared_gid_identity_ok -- the GID identity leg of the
 * PREPARED binding, extracted for direct unit coverage.  The GID must be
 * non-empty, bounded by the same cap the
 * wire/2PC path uses, and fully inside the available payload bytes.
 * Pure predicate; no state, no locks.
 */
extern bool cluster_side_prepared_gid_identity_ok(const char *gid, uint16 gidlen,
												  uint32 available_bytes);

#endif /* CLUSTER_SIDE_PREPARED_H */
