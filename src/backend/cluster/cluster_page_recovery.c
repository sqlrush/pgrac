/*-------------------------------------------------------------------------
 *
 * cluster_page_recovery.c
 *	  RF-PAGE PGDEL-03 — exhaustive dispatcher + §3.5 state machine +
 *	  §8.1 outcome surface (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-03 / §3.5 / §4.1 / §8.1 / §11.1 G9.
 *
 *	  The action table is the §4.1 recovery-action column verbatim:
 *	  every class has exactly one action; WILLINIT (no full-init rule),
 *	  NONLOGGED-without-owner and UNKNOWN land on BLOCKED (mutation=0,
 *	  resource never released).  The state machine admits only adjacent
 *	  advances, so a caller cannot skip a proof step (spec §3.5: "任一
 *	  阶段只能前进到相邻阶段").  Both are side-effect free.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_recovery.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_recovery.h"

ClusterPageRecoveryAction
cluster_page_class_recovery_action(ClusterPageClass page_class)
{
	switch (page_class) {
	case CLUSTER_PAGE_CLASS_NORMAL:
	case CLUSTER_PAGE_CLASS_CLEANOUT:
		/*
			 * PC-NORMAL / PC-CLEANOUT: versioned deterministic delta
			 * apply (the §3.2 gate decides per record; the cleanout codec
			 * census is the apply layer's, spec §4.7 keeps the action
			 * BLOCKED until producer + codec exist — this table only
			 * assigns the row's action).
			 */
		return CLUSTER_PAGE_ACTION_APPLY;

	case CLUSTER_PAGE_CLASS_NEW:
		/* PC-NEW: initialize only from a declared full-init rule. */
		return CLUSTER_PAGE_ACTION_INIT;

	case CLUSTER_PAGE_CLASS_INCARNATION:
		/* PC-INCARNATION: close the old contributor set, open the
			 * new; never merge incarnations. */
		return CLUSTER_PAGE_ACTION_INCARNATE;

	case CLUSTER_PAGE_CLASS_TEMP:
		/* PC-TEMP: discard/recreate with the owning-session proof. */
		return CLUSTER_PAGE_ACTION_DISCARD;

	case CLUSTER_PAGE_CLASS_REBUILDABLE:
		/* PC-REBUILDABLE (FSM only, approved deviation): invalidate
			 * and rebuild from heap truth + relation size. */
		return CLUSTER_PAGE_ACTION_REBUILD;

	case CLUSTER_PAGE_CLASS_HEADER:
		/* PC-HEADER: route to the exact typed owner; generic page
			 * replay never touches it (§4.5). */
		return CLUSTER_PAGE_ACTION_ROUTE;

	case CLUSTER_PAGE_CLASS_FULLIMAGE:
		/* PC-FULLIMAGE: an image payload under provenance proof. */
		return CLUSTER_PAGE_ACTION_IMAGE;

	case CLUSTER_PAGE_CLASS_WILLINIT:
		/* PC-WILLINIT: the attribute alone never authorizes init;
			 * without the exact rmgr full-init rule the action is
			 * BLOCKED (§4.6 / PU-17). */
		return CLUSTER_PAGE_ACTION_BLOCKED;

	case CLUSTER_PAGE_CLASS_NONLOGGED:
		/* PC-NONLOGGED: rebuild/route under a declared rebuild owner;
			 * without one the apply layer must BLOCK (never treat as
			 * WAL-covered). */
		return CLUSTER_PAGE_ACTION_REBUILD;

	case CLUSTER_PAGE_CLASS_UNCLASSIFIED:
	case CLUSTER_PAGE_CLASS_UNKNOWN:
	default:
		/* PC-UNKNOWN (and the state-machine start value): mutation=0,
			 * never released (spec §4.1 / PGDEL-03 "unknown default 必须
			 * BLOCKED"). */
		return CLUSTER_PAGE_ACTION_BLOCKED;
	}
}

bool
cluster_page_state_advance(ClusterPageRecoveryState *state, ClusterPageRecoveryState expected_next)
{
	ClusterPageRecoveryState cur;

	if (state == NULL)
		return false;
	cur = *state;
	/* Only the exact adjacent successor is admitted (spec §3.5).  The
	 * terminal state re-advance is rejected because RESOURCE_RELEASED is
	 * not a step into anything. */
	if (expected_next != cur + 1)
		return false;
	if (expected_next > CLUSTER_PAGE_STATE_RESOURCE_RELEASED)
		return false;
	*state = expected_next;
	return true;
}

ClusterPageRecoveryOutcome
cluster_page_dispatcher_verdict(ClusterPageClass page_class, ClusterPageApplyVerdict verdict)
{
	/* The class layer's blocked branch is BLOCKED_CLASS.  The finer
	 * subdivisions (BLOCKED_SOURCE, BLOCKED_CONTRIBUTOR,
	 * CORRUPTION_VERSION, STALE_AUTHORITY, STABLE_BASE_UNRESOLVED) are
	 * emitted by the PGDEL-04..06 layers that own those proofs — never
	 * guessed here (G9: an outcome must come from its owning proof). */
	if (verdict == CLUSTER_PAGE_APPLY_APPLY)
		return CLUSTER_PAGE_OUTCOME_APPLY;
	if (verdict == CLUSTER_PAGE_APPLY_SKIP)
		return CLUSTER_PAGE_OUTCOME_SKIP;
	return CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS;
}
