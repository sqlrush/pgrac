/*-------------------------------------------------------------------------
 * mx_range.c
 *    Read-only native MultiXact range probe for the maintenance fixture.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_tap/pre2/mx_range.c
 * NOTES
 *    PGRAC test module; exposes the existing locked snapshot, no setters.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/multixact.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(pre2_mx_range);

Datum
pre2_mx_range(PG_FUNCTION_ARGS)
{
	MultiXactId oldest, next;
	ReadMultiXactIdRange(&oldest, &next);
	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%u|%u", oldest, next)));
}
