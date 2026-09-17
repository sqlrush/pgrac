/*-------------------------------------------------------------------------
 *
 * cluster_page_source.c
 *	  RF-PAGE PGDEL-04 — CURRENT/PI/STORAGE provenance validators and
 *	  the §5.5 source-selection rule (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-04 / §5 / §6.2 / §11.1 G1, G1′, G3, G9.
 *
 *	  Pure judgement over caller-declared facts: every input is a typed
 *	  fact from a named production owner (GCS holder, past-image
 *	  subsystem, shared-storage smgr).  Nothing here reads storage,
 *	  takes locks or copies bytes; the caller does that under its own
 *	  serialization (G1′: the validators must be able to access the real
 *	  types, which they do — the inputs ARE the real types).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_source.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_source.h"

/* Shared base: exact identity + valid source version.  A source whose
 * version does not describe the exact expected resource, or whose version
 * is invalid (zero/torn/unknown), can never carry weight (spec §5.1:
 * exact PageIdentity/PageVersion). */
static bool
cluster_page_source_base_ok(const ClusterPageSourceValidateInput *in)
{
	if (in == NULL || in->identity == NULL || in->source_version == NULL)
		return false;
	if (!cluster_page_version_valid(in->source_version))
		return false;
	if (!cluster_page_identity_equal(&in->source_version->identity, in->identity))
		return false;
	return true;
}

bool
cluster_page_source_validate_current(const ClusterPageSourceValidateInput *in)
{
	/*
	 * §5.2 CURRENT: the survivor GCS holder proves current authority over
	 * the exact resource; identity/version/stability witness are coherent
	 * before AND after the copy (the caller re-checks and re-passes the
	 * witness); the source is not an unverified buffer of a failed/stale
	 * incarnation (lineage_ok); integrity + page-class verifier pass; the
	 * control root / failure generation is still current (lineage_ok).
	 * The empty-set-at-terminal case needs the §5.2-6 chain proof (PGDEL-
	 * 05); this validator only admits the source.
	 */
	if (!cluster_page_source_base_ok(in))
		return false;
	return in->integrity_ok && in->stability_ok && in->lineage_ok && in->owner_ok;
}

bool
cluster_page_source_validate_pi(const ClusterPageSourceValidateInput *in)
{
	/*
	 * §5.3 PI: exact resource/tag + past-image version + ship/boundary
	 * SCN proof + source holder + integrity + failure-generation lineage.
	 * PI is NOT a freely discardable local copy until the page write is
	 * complete and the resource authority is notified — the apply layer
	 * (PGDEL-06) owns that ordering; validation here only admits or
	 * rejects the proof.  On rejection the PI is discarded and its bytes
	 * must never reach the STORAGE branch.
	 */
	if (!cluster_page_source_base_ok(in))
		return false;
	return in->integrity_ok && in->ship_boundary_ok && in->stability_ok && in->lineage_ok
		   && in->owner_ok;
}

bool
cluster_page_source_validate_storage(const ClusterPageSourceValidateInput *in)
{
	/*
	 * §5.4 STORAGE: the full conjunction —
	 *   1. exact PageIdentity/class/incarnation verifiable (base_ok);
	 *   2. physical integrity verifier passed (integrity_ok);
	 *   3. PageVersion valid (base_ok);
	 *   4. version anchored to durable checkpoint/control-root lineage
	 *      (anchored_ok + lineage_ok);
	 *   5. contributor set complete from source to required end
	 *      (contributors_closed — PGDEL-05 owns this proof; while it is
	 *      false, STORAGE is honestly CLOSED);
	 *   6. FPI/init/FPW + rmgr exceptions disposition explicit
	 *      (coverage_ok);
	 *   7. source still verified bytes before this mutation (fresh_ok).
	 * Retained failed-origin redo is never a STORAGE base; a torn target,
	 * invalid version or unprovable anchor fails the validator.
	 */
	if (!cluster_page_source_base_ok(in))
		return false;
	return in->integrity_ok && in->lineage_ok && in->anchored_ok && in->coverage_ok && in->fresh_ok
		   && in->contributors_closed;
}

static int
cluster_page_source_kind_rank(ClusterPageSourceKind kind)
{
	switch (kind) {
	case CLUSTER_PAGE_SOURCE_CURRENT:
		return 0; /* §6.2: prefer CURRENT */
	case CLUSTER_PAGE_SOURCE_PI:
		return 1;
	case CLUSTER_PAGE_SOURCE_STORAGE:
		return 2; /* last resort: checkpointed storage */
	}
	return 3;
}

int
cluster_page_source_select(const ClusterPageSourceKind *kinds,
						   const ClusterPageSourceValidateInput *inputs, int n)
{
	bool valid[8];
	int nvalid = 0;
	int first = -1;
	int i;

	if (kinds == NULL || inputs == NULL || n < 0)
		return -1;
	if (n > (int)lengthof(valid))
		return -1; /* bounded: fail closed beyond the table */

	for (i = 0; i < n; i++) {
		switch (kinds[i]) {
		case CLUSTER_PAGE_SOURCE_CURRENT:
			valid[i] = cluster_page_source_validate_current(&inputs[i]);
			break;
		case CLUSTER_PAGE_SOURCE_PI:
			valid[i] = cluster_page_source_validate_pi(&inputs[i]);
			break;
		case CLUSTER_PAGE_SOURCE_STORAGE:
			valid[i] = cluster_page_source_validate_storage(&inputs[i]);
			break;
		default:
			valid[i] = false;
			break;
		}
		if (valid[i]) {
			nvalid++;
			if (first < 0)
				first = i;
		}
	}

	/* §5.5: no valid source -> mutation=0, resource BLOCKED. */
	if (nvalid == 0)
		return -1;

	/* §5.5: multiple valid sources whose PageVersion conflicts are
	 * corruption/BLOCKED — never max-SCN, max-LSN or majority. */
	if (nvalid > 1) {
		for (i = 0; i < n; i++) {
			int j;

			if (!valid[i])
				continue;
			for (j = i + 1; j < n; j++) {
				if (!valid[j])
					continue;
				if (!cluster_page_version_equal(inputs[i].source_version, inputs[j].source_version))
					return -1; /* conflicting versions: BLOCKED */
			}
		}
		/* Equal versions: §6.2 preference CURRENT > PI > STORAGE. */
		first = -1;
		for (i = 0; i < n; i++) {
			if (!valid[i])
				continue;
			if (first < 0
				|| cluster_page_source_kind_rank(kinds[i])
					   < cluster_page_source_kind_rank(kinds[first]))
				first = i;
		}
	}
	return first;
}
