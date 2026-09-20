/*-------------------------------------------------------------------------
 * cluster_unit_port_stubs.c
 *   Standalone error boundary for the real backend portability library.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * ARM's runtime CRC selector logs its chosen implementation. Tests that
 * otherwise need no backend error machinery still link the real selector.
 * Existing per-fixture error handlers take precedence over these weak
 * defaults. Only the selector's DEBUG1 message is ignored; any unexpected
 * severity aborts the test, never becoming a successful operation.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_update_trace.h"
#include "cluster/cluster_xnode_profile.h"

/* Profiling-aware product objects can link without a diagnostic backend.
 * Dedicated trace tests override these defaults with the real collector. */
bool cluster_update_trace_enabled __attribute__((weak)) = false;
bool cluster_xnode_profile_enabled __attribute__((weak)) = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl __attribute__((weak)) = NULL;

void __attribute__((weak))
cluster_update_trace_event_at(const ClusterUpdateTraceEvent *event pg_attribute_unused(),
							  uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_interval_enter_at(ClusterUpdateTraceInterval *interval pg_attribute_unused(),
									   const ClusterUpdateTraceEvent *event pg_attribute_unused(),
									   uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_interval_leave_at(ClusterUpdateTraceInterval *interval pg_attribute_unused(),
									   bool finished pg_attribute_unused(),
									   uint64 now pg_attribute_unused())
{
	abort();
}

uint64 __attribute__((weak))
cluster_update_trace_phase_begin_at(int bucket pg_attribute_unused(),
									uint64 now pg_attribute_unused())
{
	abort();
}

void __attribute__((weak))
cluster_update_trace_phase_end_at(uint64 token pg_attribute_unused(),
								  uint64 now pg_attribute_unused())
{
	abort();
}

bool __attribute__((weak))
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel != DEBUG1)
		abort();
	return false;
}

bool __attribute__((weak))
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int __attribute__((weak))
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

void __attribute__((weak))
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	abort();
}
