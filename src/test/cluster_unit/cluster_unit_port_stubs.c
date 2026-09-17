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
