/*-------------------------------------------------------------------------
 *
 * cluster_version_macros.h
 *	  Centralized pgrac version macros (semver + stage suffix).
 *
 *	  Single source of truth for the pgrac version string.  Bumping
 *	  the project version means changing one #define here -- the rest
 *	  of the codebase composes the version string at compile time
 *	  via PGRAC_VERSION_STRING.
 *
 *	  This header is intentionally self-contained: it does NOT include
 *	  postgres.h or any PG internal header, so unit tests in
 *	  src/test/cluster_unit/ can include it standalone.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_version_macros.h
 *
 * NOTES
 *	  Bump policy (CLAUDE.md rule 19):
 *	    - functional point complete  -> bump PGRAC_STAGE_STEP
 *	    - Stage complete             -> bump PGRAC_VERSION_MINOR
 *	                                    + reset PGRAC_STAGE_NUM/STEP
 *	    - bug fix                    -> bump PGRAC_VERSION_PATCH
 *	    - production v1.0            -> bump PGRAC_VERSION_MAJOR
 *
 *	  After editing, also update the corresponding git tag:
 *	      v<MAJOR>.<MINOR>.<PATCH>-stage<NUM>.<STEP>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VERSION_MACROS_H
#define CLUSTER_VERSION_MACROS_H


/* ============================================================
 * Version components (single source of truth)
 * ============================================================ */
#define PGRAC_VERSION_MAJOR 0
#define PGRAC_VERSION_MINOR 1
#define PGRAC_VERSION_PATCH 0

#define PGRAC_STAGE_NUM 0
#define PGRAC_STAGE_STEP 12

/* PostgreSQL version this fork is based on. */
#define PGRAC_PG_BASE_VERSION "16.13"


/* ============================================================
 * Preprocessor stringification helpers
 * ============================================================ */
#define PGRAC_STR_HELPER(x) #x
#define PGRAC_STR(x) PGRAC_STR_HELPER(x)


/* ============================================================
 * Composed version string (compile-time constant)
 *
 *   "pgrac v<MAJOR>.<MINOR>.<PATCH>-stage<NUM>.<STEP>"
 *   " (based on PostgreSQL <PG_VERSION>)"
 *
 * Example:
 *   "pgrac v0.1.0-stage0.8 (based on PostgreSQL 16.13)"
 * ============================================================ */
#define PGRAC_VERSION_STRING                                                                                                                    \
	"pgrac v" PGRAC_STR(PGRAC_VERSION_MAJOR) "." PGRAC_STR(PGRAC_VERSION_MINOR) "." PGRAC_STR(                                                  \
		PGRAC_VERSION_PATCH) "-stage" PGRAC_STR(PGRAC_STAGE_NUM) "." PGRAC_STR(PGRAC_STAGE_STEP) " (based on PostgreSQL " PGRAC_PG_BASE_VERSION \
																								 ")"


#endif /* CLUSTER_VERSION_MACROS_H */
