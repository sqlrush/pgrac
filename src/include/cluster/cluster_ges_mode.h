/*-------------------------------------------------------------------------
 *
 * cluster_ges_mode.h
 *	  GES enqueue lock-mode encoding layer + frozen 8x8 compatibility
 *	  matrix contract.
 *
 *	  The canonical GES enqueue mode space is the 8 PostgreSQL lock modes
 *	  (AccessShareLock .. AccessExclusiveLock).  This module freezes the
 *	  PG conflict table as a const 2D matrix (the single source of truth),
 *	  and derives the compatibility / convert-class / Oracle-DLM-alias
 *	  helpers from it.  The GES enqueue mode space is distinct from the
 *	  PCM block-lock states {N,S,X}.
 *
 *	  Two API layers:
 *	    - Pure layer (FRONTEND-safe, no elog/shmem): constants, the const
 *	      matrix, ges_mode_is_valid, ges_modes_compatible,
 *	      ges_mode_compat_set, ges_mode_convert_class, ges_mode_to_dlm,
 *	      ges_dlm_mode_name, ges_mode_pg_name, ges_mode_from_pg_name.
 *	      Out-of-range input asserts (debug) and fails closed to the
 *	      conservative answer; it never calls ereport.
 *	    - Backend layer (#ifndef FRONTEND): startup self-check against the
 *	      live PG conflict table, plus init.  These may ereport.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges_mode.h
 *
 * NOTES
 *	  Spec: spec-5.1a-ges-mode-encoding-compat-matrix.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_MODE_H
#define CLUSTER_GES_MODE_H

#include "storage/lockdefs.h"

/*
 * ClusterGesMode -- readability alias for a GES enqueue mode.
 *
 *	The mode space IS the PG LOCKMODE space; this typedef only documents
 *	intent at GES call sites.  Valid values are GES_MODE_FIRST..GES_MODE_LAST.
 */
typedef LOCKMODE ClusterGesMode;

#define GES_MODE_FIRST AccessShareLock	  /* 1 */
#define GES_MODE_LAST AccessExclusiveLock /* 8 */
#define GES_MODE_COUNT MaxLockMode		  /* 8 */

StaticAssertDecl(GES_MODE_COUNT == 8, "GES mode space is the 8 PG lock modes");
StaticAssertDecl(GES_MODE_FIRST == 1 && GES_MODE_LAST == 8,
				 "GES mode bounds track storage/lockdefs.h");

/*
 * GES_MODE_BIT -- single-mode bit for a LOCKMASK (same value as the
 * backend-only LOCKBIT_ON, redefined here so the pure layer needs only
 * storage/lockdefs.h and stays FRONTEND-safe).
 */
#define GES_MODE_BIT(m) ((LOCKMASK)1 << (m))

/*
 * ges_mode_is_valid -- range check for a GES enqueue mode (pure).
 */
static inline bool
ges_mode_is_valid(ClusterGesMode m)
{
	return m >= GES_MODE_FIRST && m <= GES_MODE_LAST;
}

/*
 * ClusterGesDlmMode -- Oracle DLM/enqueue lock-mode alias (6 modes).
 *
 *	These are display/alignment aliases only, NOT extra modes.  CR == SS
 *	(Row-S), CW == SX (Row-X), PR == S (Share), PW == SSX (Share-Row-X).
 *	The PG->DLM mapping is many-to-one and approximate (PG's
 *	ShareUpdateExclusiveLock / ExclusiveLock have no exact DLM peer), so
 *	a DLM alias is never a compatibility-decision input.
 */
typedef enum ClusterGesDlmMode {
	GES_DLM_NL = 0, /* Null */
	GES_DLM_CR,		/* Concurrent Read  (== SS) */
	GES_DLM_CW,		/* Concurrent Write (== SX) */
	GES_DLM_PR,		/* Protected Read   (== S) */
	GES_DLM_PW,		/* Protected Write  (== SSX) */
	GES_DLM_EX		/* Exclusive */
} ClusterGesDlmMode;

/*
 * ClusterGesConvertClass -- classification of a mode-to-mode conversion.
 *
 *	PG's 8 modes form a partial order (some pairs are incomparable), so a
 *	conversion is classified by compatibility-set containment rather than
 *	a total strength rank.
 */
typedef enum ClusterGesConvertClass {
	GES_CONVERT_SAME = 0,  /* from == to */
	GES_CONVERT_UPGRADE,   /* compat_set(to) strict subset of compat_set(from) */
	GES_CONVERT_DOWNGRADE, /* compat_set(to) strict superset of compat_set(from) */
	GES_CONVERT_LATERAL	   /* incomparable, or equal-strength distinct modes */
} ClusterGesConvertClass;

/*
 * ges_mode_compat_matrix -- frozen 8x8 compatibility matrix (SSOT).
 *
 *	ges_mode_compat_matrix[held][wanted] == 1 when the two modes are
 *	compatible, 0 when they conflict.  Index 0 is reserved (NoLock).
 *	ges_modes_compatible / ges_mode_compat_set derive from this table.
 */
extern const uint8 ges_mode_compat_matrix[GES_MODE_COUNT + 1][GES_MODE_COUNT + 1];

/* Pure layer (FRONTEND-safe; out-of-range fails closed, never ereports). */
extern bool ges_modes_compatible(ClusterGesMode held, ClusterGesMode wanted);
extern LOCKMASK ges_mode_compat_set(ClusterGesMode m);
extern ClusterGesConvertClass ges_mode_convert_class(ClusterGesMode from, ClusterGesMode to);
extern ClusterGesDlmMode ges_mode_to_dlm(ClusterGesMode pgmode);
extern const char *ges_dlm_mode_name(ClusterGesDlmMode dlm);
extern const char *ges_mode_pg_name(ClusterGesMode m);
extern ClusterGesMode ges_mode_from_pg_name(const char *name);

#ifndef FRONTEND

/* Backend layer: severity for the startup matrix-vs-PG self-check. */
typedef enum ClusterGesModeSelfcheck {
	GES_MODE_SELFCHECK_OFF = 0,
	GES_MODE_SELFCHECK_WARN,
	GES_MODE_SELFCHECK_FATAL
} ClusterGesModeSelfcheck;

extern int cluster_ges_mode_selfcheck; /* GUC: cluster.ges_mode_selfcheck */

extern void cluster_ges_mode_init(void);
extern bool ges_mode_compat_matches_pg(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_MODE_H */
