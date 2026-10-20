/* Author: SqlRush <sqlrush@gmail.com> */
/*-------------------------------------------------------------------------
 * Real CR translation unit plus a test-only entry for the synchronous
 * retained-history adapter. No product branch or predicate is replaced.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CR_WALK_SOURCE
#define CR_WALK_SOURCE "../../backend/cluster/cluster_cr.c"
#endif
#include CR_WALK_SOURCE

bool cr_history_test_synchronous(char *page, SCN read_scn, const BufferTag *tag, bool server_mode,
								 bool *partial, uint32 *steps);

bool
cr_history_test_synchronous(char *page, SCN read_scn, const BufferTag *tag, bool server_mode,
							bool *partial, uint32 *steps)
{
	return cr_construct_retained_history(page, read_scn, tag, server_mode, partial, steps);
}
