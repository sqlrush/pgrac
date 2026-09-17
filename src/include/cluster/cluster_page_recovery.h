/*-------------------------------------------------------------------------
 *
 * cluster_page_recovery.h
 *	  RF-PAGE PGDEL-03 — exhaustive page/record class dispatcher, the
 *	  §3.5 per-block recovery state machine, and the §8.1 outcome
 *	  surface.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-03 ("exhaustive page/record class dispatcher；unknown
 *	  default 必须 BLOCKED"), §3.5 state machine, §4.1 recovery-action
 *	  column, §8.1 outcomes, §11.1 G9 (every promise lands as a RED-able
 *	  input/action/outcome).
 *
 *	  DELIVERED HERE:
 *	    - cluster_page_class_recovery_action: the §4.1 recovery-action
 *	      column as a closed class -> action table (every class has
 *	      exactly one action; UNKNOWN/WILLINIT-without-rule/NONLOGGED-
 *	      without-owner default BLOCKED);
 *	    - the §3.5 per-block state machine: strictly adjacent advance
 *	      only; a jump, repeat or terminal re-advance is rejected;
 *	      crash/restart always re-enters UNCLASSIFIED (D3′ — the
 *	      successor never reads a predecessor-private phase);
 *	    - cluster_page_dispatcher_verdict: classify + §3.2 decide ->
 *	      one §8.1 outcome; the class layer can only emit BLOCKED_CLASS
 *	      for the blocked branch — the SOURCE/CONTRIBUTOR/VERSION/AUTHORITY
 *	      subdivisions belong to the PGDEL-04..06 layers that own those
 *	      proofs.
 *
 *	  NOT DELIVERED HERE (stays RED, later PGDEL items): source
 *	  provenance (PGDEL-04), contributor closure (PGDEL-05), the
 *	  mutation/durability/post-read chain (PGDEL-06), observability
 *	  (PGDEL-08).  This layer performs no mutation, no I/O and no
 *	  authority interaction.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_recovery.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_RECOVERY_H
#define CLUSTER_PAGE_RECOVERY_H

#include "cluster/cluster_page_version.h"

/*
 * §4.1 recovery-action column (closed mapping).  Every class has exactly
 * one row; the "route"/"rebuild" rows carry their own owner/lifecycle
 * proof requirements which the apply layer (PGDEL-06) must satisfy before
 * acting — the action table itself never guesses.
 */
typedef enum ClusterPageRecoveryAction {
	CLUSTER_PAGE_ACTION_APPLY = 0, /* NORMAL/CLEANOUT: versioned delta apply */
	CLUSTER_PAGE_ACTION_INIT,	   /* NEW: init only under a full-init rule */
	CLUSTER_PAGE_ACTION_INCARNATE, /* INCARNATION: close old set, open new */
	CLUSTER_PAGE_ACTION_DISCARD,   /* TEMP: discard/recreate with owner proof */
	CLUSTER_PAGE_ACTION_REBUILD,   /* REBUILDABLE/NONLOGGED: rebuild/route */
	CLUSTER_PAGE_ACTION_ROUTE,	   /* HEADER: route to the typed owner */
	CLUSTER_PAGE_ACTION_IMAGE,	   /* FULLIMAGE: image payload under provenance */
	CLUSTER_PAGE_ACTION_BLOCKED	   /* WILLINIT (no rule) / UNKNOWN: mutation=0 */
} ClusterPageRecoveryAction;

/*
 * §8.1 outcome surface.  The class layer emits APPLY / SKIP /
 * BLOCKED_CLASS only; the finer BLOCKED_SOURCE / BLOCKED_CONTRIBUTOR /
 * CORRUPTION_VERSION / STALE_AUTHORITY / STABLE_BASE_UNRESOLVED are
 * produced by the PGDEL-04..06 layers that own those proofs (the enum is
 * defined here once so every layer shares one outcome vocabulary).
 */
typedef enum ClusterPageRecoveryOutcome {
	CLUSTER_PAGE_OUTCOME_APPLY = 0,
	CLUSTER_PAGE_OUTCOME_SKIP,
	CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE,
	CLUSTER_PAGE_OUTCOME_BLOCKED_CONTRIBUTOR,
	CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS,
	CLUSTER_PAGE_OUTCOME_CORRUPTION_VERSION,
	CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY,
	CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED
} ClusterPageRecoveryOutcome;

/*
 * §3.5 per-block recovery state machine.  One attempt advances strictly
 * one adjacent step at a time; a crash discards the attempt and the
 * successor re-enters UNCLASSIFIED (D3′ — predecessor-private progress is
 * never read as correctness).
 */
typedef enum ClusterPageRecoveryState {
	CLUSTER_PAGE_STATE_UNCLASSIFIED = 0,
	CLUSTER_PAGE_STATE_CLASSIFIED,
	CLUSTER_PAGE_STATE_SOURCE_PROVEN,
	CLUSTER_PAGE_STATE_CONTRIBUTORS_CLOSED,
	CLUSTER_PAGE_STATE_VERSION_CHAIN_VERIFIED,
	CLUSTER_PAGE_STATE_AUTHORITY_REVALIDATED,
	CLUSTER_PAGE_STATE_MUTATED_IN_MEMORY,
	CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED,
	CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE,
	CLUSTER_PAGE_STATE_POST_READ_VERIFIED,
	CLUSTER_PAGE_STATE_RESOURCE_RELEASED
} ClusterPageRecoveryState;

/*
 * §4.1 recovery-action column: the closed class -> action table.
 * UNKNOWN -> BLOCKED (spec: "unknown default 必须 BLOCKED").
 */
extern ClusterPageRecoveryAction cluster_page_class_recovery_action(ClusterPageClass page_class);

/*
 * §3.5 state machine: advance exactly one adjacent step.  Returns true
 * and updates *state on success; false (no change) on a jump, repeat,
 * terminal re-advance or NULL.  A fresh attempt starts at UNCLASSIFIED.
 */
extern bool cluster_page_state_advance(ClusterPageRecoveryState *state,
									   ClusterPageRecoveryState expected_next);

/*
 * §8.1 verdict combination: classify + §3.2 decide -> one outcome.
 * The class layer's blocked branch is BLOCKED_CLASS; the finer
 * subdivisions are the PGDEL-04..06 layers' to emit.
 */
extern ClusterPageRecoveryOutcome cluster_page_dispatcher_verdict(ClusterPageClass page_class,
																  ClusterPageApplyVerdict verdict);

#endif /* CLUSTER_PAGE_RECOVERY_H */
