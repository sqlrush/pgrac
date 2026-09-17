/*-------------------------------------------------------------------------
 *
 * cluster_page_version.c
 *	  RF-PAGE PGDEL-01 — PageVersion semantic type, exact comparison and
 *	  the closed page/record classifier (implementation).
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  §2.1 PGDEL-01 / §3 PageVersion 与 redo contract / §4.1 Closed
 *	  classifier / §10.1 PU-01..PU-08, PU-13, PU-14, PU-17.
 *
 *	  The whole layer is in-memory and side-effect free (no mutation, no
 *	  I/O, no authority, no release) — the production caller chain of the
 *	  later PGDEL items consumes it; nothing here is wired into the
 *	  existing thread-recovery replay path yet (G1/G3: that path stays
 *	  un-versioned and RED until PGDEL-02..06 land).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_page_version.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_version.h"

/* ---------------------------------------------------------------------
 * Identity / version helpers
 * --------------------------------------------------------------------- */

bool
cluster_page_identity_valid(const ClusterPageIdentity *identity)
{
	if (identity == NULL)
		return false;
	/* A zeroed RelFileLocator is the PG "never-set" identity; a fork/block
	 * must be a real address.  RelFileNumber is an Oid; InvalidOid is its
	 * never-set value. */
	if (identity->rlocator.spcOid == InvalidOid || identity->rlocator.dbOid == InvalidOid
		|| identity->rlocator.relNumber == InvalidOid)
		return false;
	if (identity->forknum < 0 || identity->forknum > MAX_FORKNUM)
		return false;
	if (identity->blocknum == InvalidBlockNumber)
		return false;
	return true;
}

bool
cluster_page_identity_equal(const ClusterPageIdentity *a, const ClusterPageIdentity *b)
{
	if (a == NULL || b == NULL)
		return false;
	return RelFileLocatorEquals(a->rlocator, b->rlocator) && a->forknum == b->forknum
		   && a->blocknum == b->blocknum;
}

bool
cluster_page_version_valid(const ClusterPageVersion *version)
{
	/*
	 * §3.1: InvalidScn, zero, missing field, decode failure and unknown
	 * version are ALL invalid — never mapped to oldest/newest/unformatted.
	 * A valid version needs a valid identity AND a non-zero incarnation AND
	 * a non-zero token.
	 */
	if (version == NULL)
		return false;
	if (!cluster_page_identity_valid(&version->identity))
		return false;
	if (version->incarnation == 0)
		return false;
	if (version->token == 0)
		return false;
	return true;
}

bool
cluster_page_version_equal(const ClusterPageVersion *a, const ClusterPageVersion *b)
{
	/*
	 * Exact equality only, and only between VALID versions.  An invalid
	 * version equals nothing (fail-closed): two invalid versions are not
	 * "equal" either, because invalid has no usable semantics (spec §3.1).
	 */
	if (!cluster_page_version_valid(a) || !cluster_page_version_valid(b))
		return false;
	return cluster_page_identity_equal(&a->identity, &b->identity)
		   && a->incarnation == b->incarnation && a->token == b->token;
}

bool
cluster_page_before_state_valid(const ClusterPageBeforeState *state)
{
	if (state == NULL)
		return false;
	if (!cluster_page_identity_valid(&state->identity))
		return false;
	if (state->new_incarnation == 0)
		return false;
	return true;
}

bool
cluster_page_before_state_equal(const ClusterPageBeforeState *a, const ClusterPageBeforeState *b)
{
	/* Same strict rule as versions: valid-only exact equality. */
	if (!cluster_page_before_state_valid(a) || !cluster_page_before_state_valid(b))
		return false;
	return cluster_page_identity_equal(&a->identity, &b->identity)
		   && a->new_incarnation == b->new_incarnation;
}

/* ---------------------------------------------------------------------
 * §3.2 admission decision
 * --------------------------------------------------------------------- */

ClusterPageApplyVerdict
cluster_page_version_decide(const ClusterPageVersion *current_working,
							const ClusterPageVersion *expected_before,
							const ClusterPageVersion *result_version,
							const ClusterPageVersion *trusted_source_version)
{
	/*
	 * §3.2 order:
	 *   if trusted_source_version proves c.result_version is already
	 *       covered:  SKIP(c)
	 *   else if current_working_version == c.expected_before:
	 *       APPLY(c);  require resulting_version == c.result_version
	 *   else:          BLOCKED_OR_CORRUPTION(c)
	 *
	 * §3.3 shape 1 is the only skip proof implemented here: source exact
	 * PageVersion equals the change result-version.  (Shape 2 — per-block
	 * contributor chain closure — belongs to PGDEL-05.)  The skip requires
	 * a VALID trusted source AND a VALID result: an invalid result cannot
	 * be proven covered by anything (PU-01 containment: BLOCKED).
	 *
	 * APPLY additionally requires a VALID result_version: the post-apply
	 * `require resulting_version == result_version` would otherwise be
	 * unverifiable, so admitting an invalid result would be a false-green
	 * door.  current_working may be NULL (no working image yet): then
	 * APPLY is impossible and only the trusted skip can pass.
	 *
	 * `>=`, raw LSN size, checksum match, already-scanned, header-looks-
	 * newer and recoverer-local bitmaps are all deliberately absent
	 * (spec §3.2 last paragraph).
	 */
	if (result_version != NULL && cluster_page_version_valid(result_version)
		&& trusted_source_version != NULL
		&& cluster_page_version_equal(trusted_source_version, result_version))
		return CLUSTER_PAGE_APPLY_SKIP;

	if (current_working == NULL || expected_before == NULL || result_version == NULL
		|| !cluster_page_version_valid(result_version)
		|| !cluster_page_version_valid(expected_before))
		return CLUSTER_PAGE_APPLY_BLOCKED;

	if (cluster_page_version_equal(current_working, expected_before))
		return CLUSTER_PAGE_APPLY_APPLY;

	return CLUSTER_PAGE_APPLY_BLOCKED;
}

/* ---------------------------------------------------------------------
 * §4.1 closed classifier
 * --------------------------------------------------------------------- */

/*
 * The (rmid, opcode) known-set registry.  Compiled EMPTY: PGDEL-02 owns
 * the rmgr census.  Fixed capacity, fail-closed on overflow (a full
 * registry never silently grows — an unrecognized record must stay
 * UNKNOWN rather than be admitted by a reallocated table).
 */
#define CLUSTER_PAGE_KNOWN_OPCODE_MAX 64

typedef struct ClusterPageKnownOpcode {
	uint8 rmid;
	uint16 opcode;
} ClusterPageKnownOpcode;

static ClusterPageKnownOpcode cluster_page_known_opcodes[CLUSTER_PAGE_KNOWN_OPCODE_MAX];
static int cluster_page_known_opcode_count = 0;

bool
cluster_page_class_register_known_opcode(uint8 rmid, uint16 opcode)
{
	if (cluster_page_class_is_known_opcode(rmid, opcode))
		return true; /* idempotent */
	if (cluster_page_known_opcode_count >= CLUSTER_PAGE_KNOWN_OPCODE_MAX)
		return false; /* fail-closed: registry full */
	cluster_page_known_opcodes[cluster_page_known_opcode_count].rmid = rmid;
	cluster_page_known_opcodes[cluster_page_known_opcode_count].opcode = opcode;
	cluster_page_known_opcode_count++;
	return true;
}

bool
cluster_page_class_is_known_opcode(uint8 rmid, uint16 opcode)
{
	int i;

	for (i = 0; i < cluster_page_known_opcode_count; i++)
		if (cluster_page_known_opcodes[i].rmid == rmid
			&& cluster_page_known_opcodes[i].opcode == opcode)
			return true;
	return false;
}

/*
 * §4.1 exhaustive dispatcher: exactly one row or UNKNOWN.  The decision
 * tree is closed over the declared inputs; every "no-match", "multi-match"
 * or unknown rmid/opcode/fork/owner path lands on UNKNOWN, which the
 * caller must treat as BLOCKED (mutation=0, no release).  Classification
 * assigns the row only — init/lifecycle/provenance proofs and recovery
 * actions belong to the later PGDEL items.
 */
ClusterPageClass
cluster_page_classify(const ClusterPageClassifyInput *input)
{
	bool new_page;
	bool full_image;
	bool will_init;

	if (input == NULL)
		return CLUSTER_PAGE_CLASS_UNKNOWN;

	/*
	 * Multi-match guards first: any combination of two row-owners is
	 * ambiguous and therefore UNKNOWN (spec §4.1 "multi-match ... BLOCKED").
	 * A temp/full-image/will-init/cleanout/nonlogged record that is also a
	 * declared header or a rebuildable FSM page is ambiguous.
	 */
	full_image = input->has_full_page_image;
	will_init = input->has_will_init;
	new_page = input->page_absent || input->page_is_new;

	if (input->header_owner != CLUSTER_PAGE_HEADER_OWNER_NONE
		&& (input->forknum == INIT_FORKNUM || input->forknum == FSM_FORKNUM
			|| input->relation_is_temp || full_image || will_init || new_page || input->is_cleanout
			|| input->relation_is_unlogged))
		return CLUSTER_PAGE_CLASS_UNKNOWN;
	if (input->forknum == FSM_FORKNUM
		&& (full_image || will_init || input->is_cleanout || input->relation_is_unlogged))
		return CLUSTER_PAGE_CLASS_UNKNOWN;
	if (full_image && will_init)
		return CLUSTER_PAGE_CLASS_UNKNOWN;
	if ((input->is_cleanout || input->relation_is_unlogged) && (full_image || will_init))
		return CLUSTER_PAGE_CLASS_UNKNOWN;

	/*
	 * PC-TEMP: temp/session-local page — no shared persistent obligation,
	 * discard/recreate only (never failed-origin replay).  Recognized by
	 * the temp fork or an explicitly temp-scoped relation.
	 */
	if (input->forknum == INIT_FORKNUM || input->relation_is_temp)
		return CLUSTER_PAGE_CLASS_TEMP;

	/*
	 * PC-REBUILDABLE: FSM only, under the user-approved deviation
	 * (spec §4.4, 8/8 zero-correctness-consumer census).  FSM is a
	 * rebuildable hint cache — invalidate/rebuild, never generic redo
	 * apply.  The multi-match guards above already rejected FSM +
	 * image/init/cleanout combinations.
	 */
	if (input->forknum == FSM_FORKNUM)
		return CLUSTER_PAGE_CLASS_REBUILDABLE;

	/* PC-HEADER: route to the exact typed owner (RF-ROOT/RF-SIDE/PG core);
	 * generic page replay must never touch it (spec §4.5). */
	if (input->header_owner != CLUSTER_PAGE_HEADER_OWNER_NONE)
		return CLUSTER_PAGE_CLASS_HEADER;

	/*
	 * PC-WILLINIT: the attribute alone never authorizes initialization.
	 * Without an exact rmgr-specific full-init rule the class cannot be
	 * proven (spec §4.6 / PU-17): no rule is registered yet, so every
	 * WILL_INIT record is UNKNOWN (BLOCKED).  Do not mistake "will init"
	 * for PC-NEW — the init proof is absent.
	 */
	if (will_init)
		return CLUSTER_PAGE_CLASS_UNKNOWN;

	/*
	 * PC-FULLIMAGE: an FPI is a possible image payload, never authority.
	 * Classification only; lineage/provenance/result-version admission
	 * belongs to the apply layer (PGDEL-06, PU-15/16).
	 */
	if (full_image)
		return CLUSTER_PAGE_CLASS_FULLIMAGE;

	/* PC-NEW: absent or uninitialized page.  The §4.3 UNFORMATTED
	 * before-state + full-init lifecycle proof are the apply layer's
	 * (PU-09/10); classification here only assigns the row. */
	if (new_page)
		return CLUSTER_PAGE_CLASS_NEW;

	/*
	 * Main-fork persistent pages.  PC-CLEANOUT / PC-NONLOGGED are declared
	 * by the caller (exact codec census is PGDEL-02/03; spec §4.7 keeps
	 * their recovery action BLOCKED until producer+codec exist — the class
	 * itself is identifiable from the declaration).
	 */
	if (input->is_cleanout)
		return CLUSTER_PAGE_CLASS_CLEANOUT;
	if (input->relation_is_unlogged)
		return CLUSTER_PAGE_CLASS_NONLOGGED;

	/*
	 * PC-NORMAL is the only general delta-replay class and requires the
	 * full PageVersion before/result chain.  A main-fork record is NORMAL
	 * ONLY when its (rmid, opcode) is in the known-set registry; anything
	 * else is an unknown rmid/opcode and must fail closed (spec §4.1 /
	 * §4.2 / PU-07).  The registry is compiled EMPTY (PGDEL-02 census).
	 */
	if (cluster_page_class_is_known_opcode(input->rmid, input->opcode))
		return CLUSTER_PAGE_CLASS_NORMAL;

	return CLUSTER_PAGE_CLASS_UNKNOWN;
}
