/*-------------------------------------------------------------------------
 *
 * cluster_ges_mode.c
 *	  GES enqueue lock-mode encoding layer -- pure layer.
 *
 *	  The frozen 8x8 compatibility matrix is the single source of truth;
 *	  ges_modes_compatible / ges_mode_compat_set / ges_mode_convert_class
 *	  all derive from it.  All functions here are pure and FRONTEND-safe:
 *	  out-of-range input asserts (debug) and fails closed to the
 *	  conservative answer, never calling ereport.  The live cross-check
 *	  against PG's DoLockModesConflict and the GUC-driven self-check live
 *	  in the backend layer (cluster_ges_mode_backend.c).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_mode.c
 *
 * NOTES
 *	  Spec: spec-5.1a-ges-mode-encoding-compat-matrix.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ges_mode.h"
#include "port.h"				/* pg_strcasecmp */

/*
 * Frozen GES compatibility matrix (SSOT).
 *
 *	[held][wanted] == 1 when the two modes are compatible, 0 when they
 *	conflict.  Index 0 (NoLock) is reserved.  These values mirror PG's
 *	LockConflicts[] table; the backend layer asserts equivalence at
 *	startup against the live DoLockModesConflict().
 *
 *	          wanted: AS RS RE SUE S  SRE E  AE
 */
const uint8 ges_mode_compat_matrix[GES_MODE_COUNT + 1][GES_MODE_COUNT + 1] = {
	/* (unused 0) */ {0, 0, 0, 0, 0, 0, 0, 0, 0},
	/* AccessShareLock		 */ {0, 1, 1, 1, 1, 1, 1, 1, 0},
	/* RowShareLock			 */ {0, 1, 1, 1, 1, 1, 1, 0, 0},
	/* RowExclusiveLock		 */ {0, 1, 1, 1, 1, 0, 0, 0, 0},
	/* ShareUpdateExclusive	 */ {0, 1, 1, 1, 0, 0, 0, 0, 0},
	/* ShareLock			 */ {0, 1, 1, 0, 0, 1, 0, 0, 0},
	/* ShareRowExclusiveLock */ {0, 1, 1, 0, 0, 0, 0, 0, 0},
	/* ExclusiveLock		 */ {0, 1, 0, 0, 0, 0, 0, 0, 0},
	/* AccessExclusiveLock	 */ {0, 0, 0, 0, 0, 0, 0, 0, 0},
};

/* Canonical PG lock-mode names, indexed by mode (1..8); 0 reserved. */
static const char *const ges_mode_pg_names[GES_MODE_COUNT + 1] = {
	"",
	"AccessShareLock",
	"RowShareLock",
	"RowExclusiveLock",
	"ShareUpdateExclusiveLock",
	"ShareLock",
	"ShareRowExclusiveLock",
	"ExclusiveLock",
	"AccessExclusiveLock",
};

/* Oracle DLM alias name per ClusterGesDlmMode. */
static const char *const ges_dlm_names[] = {
	"NL", "CR", "CW", "PR", "PW", "EX",
};

/* PG mode -> approximate Oracle DLM alias (display only). */
static const ClusterGesDlmMode ges_mode_dlm_map[GES_MODE_COUNT + 1] = {
	GES_DLM_NL,					/* 0 reserved */
	GES_DLM_CR,					/* AccessShareLock */
	GES_DLM_CR,					/* RowShareLock */
	GES_DLM_CW,					/* RowExclusiveLock */
	GES_DLM_CW,					/* ShareUpdateExclusiveLock */
	GES_DLM_PR,					/* ShareLock */
	GES_DLM_PW,					/* ShareRowExclusiveLock */
	GES_DLM_PW,					/* ExclusiveLock */
	GES_DLM_EX,					/* AccessExclusiveLock */
};

/*
 * ges_modes_compatible -- can a holder of `held` coexist with `wanted`?
 *
 *	Pure matrix lookup.  Out-of-range input fails closed to "incompatible".
 */
bool
ges_modes_compatible(ClusterGesMode held, ClusterGesMode wanted)
{
	Assert(ges_mode_is_valid(held));
	Assert(ges_mode_is_valid(wanted));
	if (!ges_mode_is_valid(held) || !ges_mode_is_valid(wanted))
		return false;
	return ges_mode_compat_matrix[held][wanted] != 0;
}

/*
 * ges_mode_compat_set -- bitmask of modes compatible with `m`.
 *
 *	Derived from the frozen matrix.  Out-of-range fails closed to empty.
 */
LOCKMASK
ges_mode_compat_set(ClusterGesMode m)
{
	LOCKMASK	mask = 0;
	ClusterGesMode w;

	Assert(ges_mode_is_valid(m));
	if (!ges_mode_is_valid(m))
		return 0;

	for (w = GES_MODE_FIRST; w <= GES_MODE_LAST; w++)
		if (ges_mode_compat_matrix[m][w])
			mask |= GES_MODE_BIT(w);
	return mask;
}

/*
 * ges_mode_convert_class -- classify a `from`->`to` conversion.
 *
 *	Classification by compatibility-set containment (PG modes form a
 *	partial order).  Out-of-range fails closed to LATERAL.
 */
ClusterGesConvertClass
ges_mode_convert_class(ClusterGesMode from, ClusterGesMode to)
{
	LOCKMASK	sf;
	LOCKMASK	st;

	Assert(ges_mode_is_valid(from));
	Assert(ges_mode_is_valid(to));
	if (!ges_mode_is_valid(from) || !ges_mode_is_valid(to))
		return GES_CONVERT_LATERAL;

	if (from == to)
		return GES_CONVERT_SAME;

	sf = ges_mode_compat_set(from);
	st = ges_mode_compat_set(to);

	if (sf == st)
		return GES_CONVERT_LATERAL;	/* equal-strength distinct modes */
	if ((st & sf) == st)
		return GES_CONVERT_UPGRADE;	/* compat_set(to) subset of compat_set(from) */
	if ((sf & st) == sf)
		return GES_CONVERT_DOWNGRADE;	/* compat_set(from) subset of compat_set(to) */
	return GES_CONVERT_LATERAL;
}

/*
 * ges_mode_to_dlm -- approximate Oracle DLM alias for a PG mode (display).
 */
ClusterGesDlmMode
ges_mode_to_dlm(ClusterGesMode pgmode)
{
	if (!ges_mode_is_valid(pgmode))
		return GES_DLM_NL;
	return ges_mode_dlm_map[pgmode];
}

/*
 * ges_dlm_mode_name -- short name for a DLM alias.
 */
const char *
ges_dlm_mode_name(ClusterGesDlmMode dlm)
{
	if (dlm < GES_DLM_NL || dlm > GES_DLM_EX)
		return "?";
	return ges_dlm_names[dlm];
}

/*
 * ges_mode_pg_name -- canonical PG lock-mode name for a GES mode.
 */
const char *
ges_mode_pg_name(ClusterGesMode m)
{
	if (!ges_mode_is_valid(m))
		return "";
	return ges_mode_pg_names[m];
}

/*
 * ges_mode_from_pg_name -- parse a canonical PG lock-mode name.
 *
 *	Case-insensitive.  Returns the mode (1..8) or 0 when the name is not
 *	one of the 8 canonical PG names (DLM aliases and unknown strings map
 *	to 0; the UDF boundary turns 0 into a user-facing ERROR).
 */
ClusterGesMode
ges_mode_from_pg_name(const char *name)
{
	ClusterGesMode m;

	if (name == NULL)
		return 0;

	for (m = GES_MODE_FIRST; m <= GES_MODE_LAST; m++)
		if (pg_strcasecmp(name, ges_mode_pg_names[m]) == 0)
			return m;
	return 0;
}
