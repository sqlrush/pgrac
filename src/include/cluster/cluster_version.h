/*-------------------------------------------------------------------------
 *
 * cluster_version.h
 *	  Pure version-string accessor for the pgrac cluster subsystem.
 *
 *	  This header is intentionally self-contained: it does NOT include
 *	  postgres.h or any PG internal header.  This allows unit tests in
 *	  src/test/cluster_unit/ to link against cluster_version.o without
 *	  pulling in the full PG backend.
 *
 *	  See spec-0.4-unit-test-framework.md §1.1 for the rationale behind
 *	  splitting this out of cluster.h / cluster.c.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_version.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Keep this header free of PG-internal includes so that downstream
 *	  unit tests can include it without dragging in the entire backend.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_VERSION_H
#define CLUSTER_VERSION_H

/*
 * Centralized version macros (PGRAC_VERSION_STRING etc.) live in a
 * separate header so unit tests can use them without pulling cluster
 * implementation symbols.
 */
#include "cluster/cluster_version_macros.h"

/*
 * pgrac_version_string -- Return the pgrac version string.
 *
 *	Returns a static null-terminated string identifying the pgrac
 *	build (e.g. "pgrac v0.1.0-stage0.8 (based on PostgreSQL 16.13)").
 *
 *	The returned pointer must not be freed or modified by the caller.
 *	The function is implemented in cluster_version.c and uses no PG
 *	internal APIs (no elog, no palloc, no shared memory).
 *
 *	Equivalent to the compile-time constant PGRAC_VERSION_STRING; the
 *	function form exists for callers that need a function pointer or
 *	want to avoid #include "cluster/cluster_version_macros.h".
 */
extern const char *pgrac_version_string(void);

#endif /* CLUSTER_VERSION_H */
