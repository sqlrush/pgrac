/*-------------------------------------------------------------------------
 *
 * cluster_version.c
 *	  Implementation of pgrac_version_string (pure function, no PG deps).
 *
 *	  This file is intentionally minimal and free of PG internal headers
 *	  (no postgres.h / no elog / no palloc).  The goal is to allow unit
 *	  tests in src/test/cluster_unit/ to link this single .o file
 *	  without dragging in the entire PG backend.
 *
 *	  See spec-0.4-unit-test-framework.md §1.1 for the rationale behind
 *	  splitting this out of cluster.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_version.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Do NOT add #include "postgres.h" here.  If a future change needs
 *	  PG internals, add the new function to cluster.c instead.
 *
 *-------------------------------------------------------------------------
 */
#include "cluster/cluster_version.h"


/*
 * pgrac_version_string -- Return the pgrac version string.
 *
 *	Returns a static null-terminated string.  Callers must not free or
 *	modify the returned memory.
 *
 *	The string is composed at compile time by macros in
 *	cluster_version_macros.h (CLAUDE.md rule 19, single source of
 *	truth).  To bump the version, edit cluster_version_macros.h --
 *	this function's body does not need to change.
 *
 * Inputs:
 *	(none)
 *
 * Returns:
 *	Pointer to static literal in .rodata.
 *
 * Side Effects:
 *	None.
 *
 * NOTES
 *	The version string format is:
 *	  pgrac v<MAJOR>.<MINOR>.<PATCH>-stage<S>.<N> (based on PostgreSQL <X.Y>)
 *
 * Author: SqlRush <sqlrush@gmail.com>
 */
const char *
pgrac_version_string(void)
{
	return PGRAC_VERSION_STRING;
}
