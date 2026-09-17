/*-------------------------------------------------------------------------
 *
 * cluster_page_version.h
 *	  RF-PAGE PGDEL-01 — PageVersion semantic type, exact comparison and
 *	  the closed page/record classifier.
 *
 *	  Spec: specs/spec-rf-page-crash-safe-page-replay-journal.md
 *	  (§2.1 PGDEL-01, §3 PageVersion 与 redo contract, §4.1 Closed
 *	  classifier).  This is the FIRST RF-PAGE deliverable: the semantic
 *	  schema from §3.1 (PageIdentity / PageIncarnation / VersionToken),
 *	  the §3.2 expected-before -> result-version admission decision with
 *	  the §3.3 trusted result-version skip (shape 1: source exact
 *	  equality), and the §4.1 exhaustive page/record class dispatcher
 *	  whose unknown/ambiguous default is BLOCKED.
 *
 *	  DELIBERATE BOUNDARIES (spec §2.2 / §11.1 G1/G3 — do not erase):
 *	    - This is an in-memory semantic layer.  NO struct bytes, wire
 *	      opcode, persistent layout or actor is frozen (spec §2.1).
 *	    - The rmgr/opcode known-set is a REGISTRY, compiled EMPTY
 *	      (PGDEL-02 owns the rmgr census).  An unregistered main-fork
 *	      record classifies UNKNOWN -> BLOCKED, never NORMAL.
 *	    - No producer, source-proof, mutation, durability, post-read or
 *	      release chain exists yet (G3: all RED).  `decide` here is the
 *	      §3.2 admission gate only; it does not mutate, write or release.
 *	    - The existing thread-recovery replay path (exact-f076
 *	      PX-01..PX-06: smgrread -> LSN-gated apply -> smgrwrite) is
 *	      UNTOUCHED and stays un-versioned until the later PGDEL items
 *	      land.  Nothing here claims the existing path is PageVersion-
 *	      compliant (spec §8.2-1).
 *	    - InvalidScn / zero / missing / decode-failure / unknown are all
 *	      INVALID versions: never mapped to oldest/newest/unformatted
 *	      (spec §3.1).  Unformatted is an explicit class state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_page_version.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_VERSION_H
#define CLUSTER_PAGE_VERSION_H

#include "common/relpath.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

/*
 * §3.1 Logical identities.  VersionToken is an opaque exact-equality token:
 * its only order comes from the verified expected-before -> result-version
 * edge/dependency topology (verified in the later PGDEL items); this layer
 * deliberately provides NO numeric ordering, so "numeric-higher" can never
 * be used as a skip (spec §3.3 / PU-05).  Incarnation changes whenever the
 * same physical address can denote a newly created, truncated, dropped/
 * recreated or reused page.
 */
typedef uint64 ClusterPageIncarnation;
typedef uint64 ClusterVersionToken;

/* exact shared-storage database identity + physical relation/fork/block. */
typedef struct ClusterPageIdentity {
	RelFileLocator rlocator;
	ForkNumber forknum;
	BlockNumber blocknum;
} ClusterPageIdentity;

typedef struct ClusterPageVersion {
	ClusterPageIdentity identity;
	ClusterPageIncarnation incarnation; /* 0 = invalid */
	ClusterVersionToken token;			/* 0 = invalid */
} ClusterPageVersion;

/* §3.4: expected_before is "PageVersion-or-explicit-class-state".  The one
 * explicit class state defined here is UNFORMATTED (PC-NEW before-state):
 * typed, never a numeric sentinel (spec §3.1, §4.3). */
typedef struct ClusterPageBeforeState {
	ClusterPageIdentity identity;
	ClusterPageIncarnation new_incarnation; /* 0 = invalid */
} ClusterPageBeforeState;

/*
 * §3.2 admission decision.  The redo-change's full identity (expected_before,
 * result_version, class, failed-origin, record identity) is the caller's
 * (PGDEL-02/03 producer); this gate consumes only the version contract.
 * Any invalid input fails closed to BLOCKED — InvalidScn containment is
 * BLOCKED, never a version success (spec §10.1 PU-01).
 */
typedef enum ClusterPageApplyVerdict {
	CLUSTER_PAGE_APPLY_APPLY = 0, /* current == expected_before, result valid */
	CLUSTER_PAGE_APPLY_SKIP,	  /* trusted source == result (shape 1) */
	CLUSTER_PAGE_APPLY_BLOCKED	  /* anything else: fail closed */
} ClusterPageApplyVerdict;

/*
 * §4.1 closed page/record class table.  Every page-affecting record must
 * land in exactly one row; multi-match / no-match / unknown rmid-opcode /
 * unknown fork-header / ambiguous lifecycle classify UNKNOWN (BLOCKED).
 * Class names track the spec table rows (PC-NORMAL .. PC-UNKNOWN).
 */
typedef enum ClusterPageClass {
	CLUSTER_PAGE_CLASS_UNCLASSIFIED = 0, /* state-machine start (spec §3.5) */
	CLUSTER_PAGE_CLASS_NORMAL,			 /* PC-NORMAL */
	CLUSTER_PAGE_CLASS_NEW,				 /* PC-NEW */
	CLUSTER_PAGE_CLASS_INCARNATION,		 /* PC-INCARNATION */
	CLUSTER_PAGE_CLASS_TEMP,			 /* PC-TEMP */
	CLUSTER_PAGE_CLASS_REBUILDABLE,		 /* PC-REBUILDABLE: FSM only (approved deviation) */
	CLUSTER_PAGE_CLASS_HEADER,			 /* PC-HEADER */
	CLUSTER_PAGE_CLASS_FULLIMAGE,		 /* PC-FULLIMAGE */
	CLUSTER_PAGE_CLASS_WILLINIT,		 /* PC-WILLINIT */
	CLUSTER_PAGE_CLASS_CLEANOUT,		 /* PC-CLEANOUT */
	CLUSTER_PAGE_CLASS_NONLOGGED,		 /* PC-NONLOGGED */
	CLUSTER_PAGE_CLASS_UNKNOWN			 /* PC-UNKNOWN: mutation=0, never released */
} ClusterPageClass;

/* §4.5: header pages route to an exact typed owner.  NONE means the caller
 * declares no typed owner — a header-class page without an owner is never
 * auto-inferred (that would be a guess, and guesses are UNKNOWN). */
typedef enum ClusterPageHeaderOwner {
	CLUSTER_PAGE_HEADER_OWNER_NONE = 0,
	CLUSTER_PAGE_HEADER_OWNER_ROOT,	  /* RF-ROOT typed owner */
	CLUSTER_PAGE_HEADER_OWNER_SIDE,	  /* RF-SIDE typed owner */
	CLUSTER_PAGE_HEADER_OWNER_PG_CORE /* PG core typed owner */
} ClusterPageHeaderOwner;

/*
 * Closed classifier inputs.  rmid/opcode come from the redo record
 * (RmgrId / info-after-XLR_INFO_MASK); the boolean facts (temp scope, FSM
 * fork, absent/new page, cleanout/nonlogged declaration, header owner) are
 * exact caller-declared facts — the classifier never infers them from
 * names, paths, counters or digests (spec §5.1).
 */
typedef struct ClusterPageClassifyInput {
	uint8 rmid;
	uint16 opcode;
	bool has_full_page_image; /* XLR_FULL_PAGE_WRITE / FPI payload */
	bool has_will_init;		  /* WILL_INIT record attribute */
	ForkNumber forknum;
	bool relation_is_temp;				 /* temp/session-local relation scope */
	bool relation_is_unlogged;			 /* nonlogged relation (no WAL coverage) */
	bool page_absent;					 /* relation/file absent or block beyond EOF */
	bool page_is_new;					 /* PageIsNew: all-zero/uninitialized page */
	bool is_cleanout;					 /* delayed cleanout metadata normalization */
	ClusterPageHeaderOwner header_owner; /* typed owner declaration */
} ClusterPageClassifyInput;

/*
 * Identity / version helpers.
 */
extern bool cluster_page_identity_valid(const ClusterPageIdentity *identity);
extern bool cluster_page_identity_equal(const ClusterPageIdentity *a, const ClusterPageIdentity *b);
extern bool cluster_page_version_valid(const ClusterPageVersion *version);
extern bool cluster_page_version_equal(const ClusterPageVersion *a, const ClusterPageVersion *b);
extern bool cluster_page_before_state_valid(const ClusterPageBeforeState *state);
extern bool cluster_page_before_state_equal(const ClusterPageBeforeState *a,
											const ClusterPageBeforeState *b);

/*
 * §3.2 admission decision (shape-1 trusted skip, spec §3.3).  `decide`
 * consumes: current_working (may be NULL when the caller has no working
 * image yet — then only the trusted-source SKIP branch can pass, and
 * APPLY is impossible), expected_before, result_version, and
 * trusted_source_version (NULL when no trusted source is offered; the
 * §3.3 shape-2 contributor-chain skip belongs to PGDEL-05).
 */
extern ClusterPageApplyVerdict cluster_page_version_decide(
	const ClusterPageVersion *current_working, const ClusterPageVersion *expected_before,
	const ClusterPageVersion *result_version, const ClusterPageVersion *trusted_source_version);

/*
 * §4.1 closed classifier.  Returns exactly one class; UNKNOWN means the
 * caller must fail closed (mutation=0, resource never released).  The
 * UNFORMATTED before-state / lifecycle / init proofs are NOT evaluated
 * here (PGDEL-03/06): classification is the row assignment only.
 */
extern ClusterPageClass cluster_page_classify(const ClusterPageClassifyInput *input);

/*
 * rmgr/opcode known-set registry.  COMPILED EMPTY — PGDEL-02 owns the
 * rmgr census; until a (rmid, opcode) pair is registered, a main-fork
 * record classifies UNKNOWN (BLOCKED), which is the honest pre-census
 * default (spec §4.1 "unknown rmid/opcode ... all BLOCKED").  Registering
 * is idempotent; a full registry refuses (fail-closed) rather than
 * silently growing.
 */
extern bool cluster_page_class_register_known_opcode(uint8 rmid, uint16 opcode);
extern bool cluster_page_class_is_known_opcode(uint8 rmid, uint16 opcode);

#endif /* CLUSTER_PAGE_VERSION_H */
