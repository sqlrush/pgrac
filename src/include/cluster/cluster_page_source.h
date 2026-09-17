/*-------------------------------------------------------------------------
 *
 * cluster_page_source.h
 *	  RF-PAGE PGDEL-04 — CURRENT/PI/STORAGE provenance validators and
 *	  the §5.5 source-selection rule.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-04 ("CURRENT/PI/STORAGE provenance validators；必须
 *	  有 production source owners"), §5 source provenance, §6.2 build
 *	  algorithm, §11.1 G1/G1′/G3.
 *
 *	  DESIGN CONTRACT (G1′): these validators are PURE judgement over
 *	  caller-declared facts.  The production source owners supply the
 *	  facts — GCS holder/stability witness (CURRENT), the past-image
 *	  subsystem (PI: cluster_bufmgr_block_is_pi /
 *	  cluster_bufmgr_snapshot_pi_block), shared-storage smgr reads
 *	  (STORAGE: smgrread + cluster_fs) — and the caller performs the
 *	  actual copy/read under its own serialization.  A proof can never
 *	  be produced from a pathname, mtime, counter, digest-only, target
 *	  LSN or recoverer-local cache (spec §5.1 last paragraph): every
 *	  input below is a typed fact.
 *
 *	  DELIVERED HERE:
 *	    - ClusterPageSourceKind + ClusterPageSourceValidateInput (the
 *	      typed fact set shared by all three validators);
 *	    - cluster_page_source_validate_current / _pi / _storage: the
 *	      §5.2 / §5.3 / §5.4 conjunctions;
 *	    - cluster_page_source_select: §5.5 conflict/absence rule +
 *	      the §6.2 CURRENT > PI > STORAGE preference for equal-version
 *	      sources.
 *
 *	  NOT DELIVERED HERE (stays RED, later PGDEL items): the covered
 *	  contributor boundary (PGDEL-05) — while contributors_closed is
 *	  false, STORAGE validation FAILS (spec §5.4-5), so the STORAGE
 *	  branch is honestly closed until that proof exists; the mutation/
 *	  durability/post-read chain (PGDEL-06).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_source.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_SOURCE_H
#define CLUSTER_PAGE_SOURCE_H

#include "cluster/cluster_page_version.h"

typedef enum ClusterPageSourceKind {
	CLUSTER_PAGE_SOURCE_CURRENT = 0, /* §5.2 survivor GCS holder */
	CLUSTER_PAGE_SOURCE_PI,			 /* §5.3 past image */
	CLUSTER_PAGE_SOURCE_STORAGE		 /* §5.4 checkpointed shared storage */
} ClusterPageSourceKind;

/*
 * Typed fact set for one source candidate.  `source_version` is the
 * source's PageVersion (must be VALID to carry weight — an invalid
 * version makes every validator fail); `identity` is the exact expected
 * resource.  The boolean facts are exactly what the named production
 * owner declares; this layer never infers them.
 */
typedef struct ClusterPageSourceValidateInput {
	const ClusterPageIdentity *identity;	  /* exact expected resource */
	const ClusterPageVersion *source_version; /* the source's PageVersion */
	bool integrity_ok;						  /* page-class integrity verifier passed */
	bool stability_ok;		  /* CURRENT: GCS stability witness; PI: holder stable */
	bool lineage_ok;		  /* control-root / failure-generation lineage current */
	bool owner_ok;			  /* source owner present & authoritative */
	bool ship_boundary_ok;	  /* PI: ship/boundary SCN proof (spec §5.3) */
	bool anchored_ok;		  /* STORAGE: version anchored to durable checkpoint/root */
	bool coverage_ok;		  /* STORAGE: FPI/init/FPW + rmgr exceptions handled */
	bool fresh_ok;			  /* STORAGE: re-verified before mutation (§5.4-7) */
	bool contributors_closed; /* STORAGE: §5.4-5, owned by PGDEL-05 */
} ClusterPageSourceValidateInput;

/*
 * §5.2 CURRENT conjunction: exact identity + valid version + integrity +
 * GCS stability witness + lineage + owner.  An empty-but-witnessed
 * CURRENT set (already at the terminal result) is a valid source; the
 * "empty replay set" conclusion needs the §5.2-6 per-block chain, which
 * is PGDEL-05's — validation here only admits the source.
 */
extern bool cluster_page_source_validate_current(const ClusterPageSourceValidateInput *in);

/*
 * §5.3 PI conjunction: exact identity + valid version + integrity +
 * ship/boundary SCN proof + holder-stability + lineage + owner.  On any
 * failure the PI is discarded: its bytes must never be smuggled into the
 * STORAGE branch, and a PI miss is never written as "page recovered".
 */
extern bool cluster_page_source_validate_pi(const ClusterPageSourceValidateInput *in);

/*
 * §5.4 STORAGE conjunction: exact identity + valid version + integrity +
 * lineage + checkpoint/root anchor + FPI/init/FPW coverage + pre-mutation
 * freshness + contributor closure.  While contributors_closed is false
 * (PGDEL-05 not landed) this validator FAILS — the STORAGE base is
 * honestly closed, and retained failed-origin redo is never a base
 * (spec §5.4 last paragraph).
 */
extern bool cluster_page_source_validate_storage(const ClusterPageSourceValidateInput *in);

/*
 * §5.5/§6.2 selection: returns the chosen input index, or -1 (BLOCKED).
 *   - no valid source            -> -1 (mutation=0, resource BLOCKED);
 *   - >=2 valid sources          -> -1 when their versions conflict
 *     (corruption/BLOCKED; never max-SCN/max-LSN/majority);
 *   - >=2 valid, same version    -> the §6.2 preference order
 *     CURRENT > PI > STORAGE.
 * `inputs` is an array of `n` candidates; kind is read from a parallel
 * `kinds` array (inputs do not carry their own kind).
 */
extern int cluster_page_source_select(const ClusterPageSourceKind *kinds,
									  const ClusterPageSourceValidateInput *inputs, int n);

#endif /* CLUSTER_PAGE_SOURCE_H */
