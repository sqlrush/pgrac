/*-------------------------------------------------------------------------
 *
 * cluster_page_apply.c
 *	  RF-PAGE PGDEL-06 — mutation admission, §7.2 sequence step
 *	  judgements, §7.3 typed page proof, §7.6 stable-base STOP and
 *	  §7.7 crash-matrix verdicts (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-06 / §7 / §11.1 G1, G1″, G3, G9.
 *
 *	  Judgement-only layer: every native action (buffer/GCS authority,
 *	  page write, durability barrier, canonical post-read) is executed by
 *	  the production caller; this file decides the gates and advances the
 *	  §3.5 state machine.  The mid-write cut ALWAYS fails closed with
 *	  STABLE_BASE_UNRESOLVED (§7.6 / PL-03) — nothing here can turn that
 *	  RED into a green.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_apply.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_apply.h"

bool
cluster_page_mutation_admission(const ClusterPageMutationAdmission *admission)
{
	/* §7.1: every gate is a typed fact; any false means no mutation and
	 * never a temporary-failure-to-skip conversion. */
	if (admission == NULL)
		return false;
	return admission->duty_root_ok && admission->recoverer_active_ok && admission->fence_ok
		   && admission->serialization_ok && admission->source_proof_fresh
		   && admission->working_version_exact && admission->retention_covers;
}

static ClusterPageSequenceResult
cluster_page_sequence_fail(ClusterPageRecoveryState state, ClusterPageRecoveryOutcome outcome)
{
	ClusterPageSequenceResult r;

	r.state = state; /* unchanged: no advance past a failed gate */
	r.outcome = outcome;
	return r;
}

ClusterPageSequenceResult
cluster_page_apply_step_durability(ClusterPageRecoveryState state, bool durable_ok)
{
	ClusterPageSequenceResult r;

	/* §7.2-6: the durability barrier for the exact relation/page must
	 * complete before PAGE_WRITE_DURABLE.  A write return, dirty bit or
	 * relation fsync counter is never a substitute. */
	if (state != CLUSTER_PAGE_STATE_WAL_BEFORE_DATA_SATISFIED)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CONTRIBUTOR);
	if (!durable_ok)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE);
	r.outcome = CLUSTER_PAGE_OUTCOME_APPLY;
	r.state = state;
	if (!cluster_page_state_advance(&r.state, CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE))
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
	return r;
}

ClusterPageSequenceResult
cluster_page_apply_step_post_read(ClusterPageRecoveryState state, bool post_read_ok)
{
	ClusterPageSequenceResult r;

	/* §7.2-7/8: canonical storage re-read must verify identity/class/
	 * incarnation/PageVersion/integrity == expected result (PU-30). */
	if (state != CLUSTER_PAGE_STATE_PAGE_WRITE_DURABLE)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CONTRIBUTOR);
	if (!post_read_ok)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_CORRUPTION_VERSION);
	r.outcome = CLUSTER_PAGE_OUTCOME_APPLY;
	r.state = state;
	if (!cluster_page_state_advance(&r.state, CLUSTER_PAGE_STATE_POST_READ_VERIFIED))
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
	return r;
}

ClusterPageSequenceResult
cluster_page_apply_step_authority(ClusterPageRecoveryState state, bool authority_ok)
{
	ClusterPageSequenceResult r;

	/* §7.2-9: fresh RF-ROOT revalidation before release (PU-29: stale
	 * authority before release -> no release). */
	if (state != CLUSTER_PAGE_STATE_POST_READ_VERIFIED)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CONTRIBUTOR);
	if (!authority_ok)
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY);
	r.outcome = CLUSTER_PAGE_OUTCOME_APPLY;
	r.state = state;
	if (!cluster_page_state_advance(&r.state, CLUSTER_PAGE_STATE_RESOURCE_RELEASED))
		return cluster_page_sequence_fail(state, CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS);
	return r;
}

ClusterPageRecoveryOutcome
cluster_page_apply_midwrite_cut(void)
{
	/*
	 * §7.6 / §7.7 "during target write" row / PL-03: the target may be
	 * torn and no stable-base strategy is approved.  This is a permanent
	 * RED/STOP until a user-approved stable-base strategy revises the
	 * spec — never SKIP, never an expected-failure green, never a mock
	 * carrier.
	 */
	return CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED;
}

ClusterPageRecoveryOutcome
cluster_page_crash_matrix_verdict(ClusterPageCrashCut cut)
{
	switch (cut) {
	case CLUSTER_PAGE_CUT_BEFORE_SOURCE_PROOF:
		/* §7.7: no trusted state -> rebuild the source census;
			 * mutation=0. */
		return CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE;

	case CLUSTER_PAGE_CUT_AFTER_SOURCE_PROOF:
		/* Ephemeral proof only: the successor re-censuses and never
			 * adopts the predecessor-local plan (D3′). */
		return CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE;

	case CLUSTER_PAGE_CUT_DURING_TARGET_WRITE:
		/* Target may be torn: STOP-RF-PAGE-STABLE-BASE. */
		return CLUSTER_PAGE_OUTCOME_STABLE_BASE_UNRESOLVED;

	case CLUSTER_PAGE_CUT_AFTER_WRITE_BEFORE_DURABILITY:
		/* Non-durable/untrusted target: rebuild or BLOCKED. */
		return CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE;

	case CLUSTER_PAGE_CUT_AFTER_DURABILITY_BEFORE_POST_READ:
		/* Durable but unverified bytes: fresh post-read; failure =>
			 * rebuild/BLOCKED. */
		return CLUSTER_PAGE_OUTCOME_BLOCKED_SOURCE;

	case CLUSTER_PAGE_CUT_AFTER_POST_READ_BEFORE_RELEASE:
		/* Durable result re-verifiable: authority revalidate then
			 * exact release (PU-29). */
		return CLUSTER_PAGE_OUTCOME_STALE_AUTHORITY;

	case CLUSTER_PAGE_CUT_AFTER_RELEASE:
		/* Resource ready only; redo retirement stays RF-ROOT
			 * FND-10's decision — never implied by the release. */
		return CLUSTER_PAGE_OUTCOME_APPLY;

	default:
		/* Unknown cut: fail closed. */
		return CLUSTER_PAGE_OUTCOME_BLOCKED_CLASS;
	}
}
