/*-------------------------------------------------------------------------
 *
 * cluster_page_apply.h
 *	  RF-PAGE PGDEL-06 — native apply admission, the §7.2 mutation
 *	  sequence gates, the §7.3 typed page proof, and the §7.6/§7.7
 *	  stable-base STOP + crash-matrix judgements.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-06 ("native buffer/GCS apply + durability + canonical
 *	  post-read；carrier STOP 不绕过"), §7 Apply, durability, post-read
 *	  and D3′, §11.1 G1/G1″/G3/G9.
 *
 *	  HARD CONTRACT (G1″/§7.6 — do not erase): this layer implements the
 *	  JUDGEMENTS of the §7 sequence.  The actual native buffer/GCS
 *	  authority, page write path, durability barrier and canonical
 *	  post-read are executed by the production caller under its own
 *	  serialization (the orchestrator/apply worker); nothing here mutates
 *	  a page, writes storage or touches authority.  The repeated-
 *	  recoverer mid-write cut stays RED: cluster_page_apply_midwrite_cut
 *	  always reports STABLE_BASE_UNRESOLVED (PL-03 / §7.6) — no approved
 *	  stable-base strategy exists, so a target-write crash can never be
 *	  papered over with SKIP, an expected-failure green or a mock carrier.
 *
 *	  NOT DELIVERED HERE (stays RED, later PGDEL items): the production
 *	  caller chain that fires every gate in the real orchestrator
 *	  (PGDEL-06 production wiring + PGDEL-07 proof handoff to RF-ROOT/
 *	  SIDE; §10.3 production-caller test), observability (PGDEL-08) and
 *	  the fault/acceptance TAP legs (PGDEL-09).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_apply.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_APPLY_H
#define CLUSTER_PAGE_APPLY_H

#include "cluster/cluster_page_recovery.h"
#include "cluster/cluster_page_set.h"

/*
 * §7.1 mutation admission — one typed fact per gate.  All seven facts are
 * declared by the production caller (duty/root authority, recovery
 * actor, fence, resource serialization, source proof freshness, working
 * version, retention pin); this layer judges the conjunction only.  On
 * any false the caller performs NO page mutation and never converts a
 * temporary failure into a skip (spec §7.1).
 */
typedef struct ClusterPageMutationAdmission {
	bool duty_root_ok;			/* exact duty/control root current */
	bool recoverer_active_ok;	/* active recoverer + failure generation */
	bool fence_ok;				/* four-layer fence armed */
	bool serialization_ok;		/* exact resource serialization held */
	bool source_proof_fresh;	/* selected §5 proof re-validated */
	bool working_version_exact; /* current == expected-before (exact) */
	bool retention_covers;		/* retention pin covers ALL contributors */
} ClusterPageMutationAdmission;

/* true iff every §7.1 fact holds. */
extern bool cluster_page_mutation_admission(const ClusterPageMutationAdmission *admission);

/*
 * §7.2 sequence step judgement.  The caller executes each native step
 * and then asks this layer to (a) advance the §3.5 state machine and
 * (b) report the §8.1 outcome.  The steps between MUTATED_IN_MEMORY and
 * RESOURCE_RELEASED carry their own §7.2 fact sets:
 *
 *   durable_ok    the durability barrier (fdatasync/durable ordering)
 *                 for the exact relation/page completed;
 *   post_read_ok  the canonical storage re-read verified identity/class/
 *                 incarnation/PageVersion/integrity == expected result;
 *   authority_ok  the fresh RF-ROOT authority revalidation passed.
 *
 *  A failed step stops the sequence with the outcome owning the proof
 *  that failed (BLOCKED_* / CORRUPTION_VERSION / STALE_AUTHORITY) — the
 *  caller must not release the resource, and write-return/dirty-bit/
 *  relation-fsync-counter/logical-DONE/pre-write-checksum are never
 *  substitutes (spec §7.2 last paragraph).
 */
typedef struct ClusterPageSequenceResult {
	ClusterPageRecoveryState state; /* advanced state (unchanged on failure) */
	ClusterPageRecoveryOutcome outcome;
} ClusterPageSequenceResult;

extern ClusterPageSequenceResult cluster_page_apply_step_durability(ClusterPageRecoveryState state,
																	bool durable_ok);
extern ClusterPageSequenceResult cluster_page_apply_step_post_read(ClusterPageRecoveryState state,
																   bool post_read_ok);
extern ClusterPageSequenceResult cluster_page_apply_step_authority(ClusterPageRecoveryState state,
																   bool authority_ok);

/*
 * §7.3 typed page proof exported to RF-ROOT/SIDE (logical export only —
 * no ABI frozen).  RF-ROOT consumes it for resource release + the
 * FND-10 conjunction; RF-SIDE consumes only page dependencies; it is
 * never an all-domain barrier (spec §7.3).
 */
typedef struct ClusterPageProof {
	uint16 failed_origin_thread;
	ClusterPageIdentity identity;
	ClusterPageClass page_class;
	ClusterPageVersion post_read_version; /* canonical post-read result */
	bool contributor_coverage;			  /* source-to-terminal chain closed */
	bool durability_barrier_ok;			  /* durable ordering completed */
	bool post_read_ok;					  /* canonical bytes verified */
	bool authority_revalidated;			  /* RF-ROOT revalidation passed */
} ClusterPageProof;

/*
 * §7.6 STOP-RF-PAGE-STABLE-BASE: the repeated-recoverer mid-write cut.
 * The public corpus provides no stable preimage/atomicity/carrier for a
 * recoverer crashing during the target write, and no stable-base
 * strategy is approved.  This judgement ALWAYS reports
 * STABLE_BASE_UNRESOLVED (RED/STOP): no false DONE, no skip, no mock
 * carrier (PL-03 / §7.6 / §7.7 "during target write" row).
 */
extern ClusterPageRecoveryOutcome cluster_page_apply_midwrite_cut(void);

/*
 * §7.7 crash-matrix judgements.  `cut` names the crash point of the
 * matrix; the returned outcome is what the successor MUST observe (a
 * rebuild, a fresh post-read or the stable-base STOP).  Every row maps
 * to exactly one outcome; unknown cuts fail closed.
 */
typedef enum ClusterPageCrashCut {
	CLUSTER_PAGE_CUT_BEFORE_SOURCE_PROOF = 0,
	CLUSTER_PAGE_CUT_AFTER_SOURCE_PROOF,
	CLUSTER_PAGE_CUT_DURING_TARGET_WRITE,
	CLUSTER_PAGE_CUT_AFTER_WRITE_BEFORE_DURABILITY,
	CLUSTER_PAGE_CUT_AFTER_DURABILITY_BEFORE_POST_READ,
	CLUSTER_PAGE_CUT_AFTER_POST_READ_BEFORE_RELEASE,
	CLUSTER_PAGE_CUT_AFTER_RELEASE
} ClusterPageCrashCut;

extern ClusterPageRecoveryOutcome cluster_page_crash_matrix_verdict(ClusterPageCrashCut cut);

#endif /* CLUSTER_PAGE_APPLY_H */
