/*-------------------------------------------------------------------------
 *
 * test_pgrac_aux_consumer.c
 *    Pin a VM page through its real relation-level consumer for a TAP probe.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_tap/aux_probe/test_pgrac_aux_consumer.c
 *
 * NOTES
 *    Test-only, superuser-only module. No direct buffer/PCM state mutation,
 *    synthetic grant, page initialization, timeout change or injected return.
 *    A holder can keep the real pin until the test creates its release marker.
 *    Normal resource-owner cleanup still owns errors and cancellation.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <unistd.h>

#include "access/table.h"
#include "access/visibilitymap.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(test_pgrac_aux_vm_pin);

Datum
test_pgrac_aux_vm_pin(PG_FUNCTION_ARGS)
{
	Relation relation;
	Buffer buffer = InvalidBuffer;
	Page page;
	PageHeader header;
	uint64 digest = UINT64CONST(14695981039346656037);
	bool hold = PG_GETARG_BOOL(1);
	char *release_path = NULL;
	char *result;
	int i;

	if (!superuser())
		ereport(ERROR, (errmsg("auxiliary consumer probe requires superuser")));
	if (hold) {
		release_path = psprintf("%s/aux-probe-release", DataDir);
		if (access(release_path, F_OK) == 0 || errno != ENOENT)
			ereport(ERROR, (errmsg("auxiliary consumer requires a fresh release marker path")));
	}
	relation = table_open(PG_GETARG_OID(0), AccessShareLock);
	if (relation->rd_rel->relkind != RELKIND_RELATION
		|| relation->rd_rel->relpersistence != RELPERSISTENCE_PERMANENT)
		ereport(ERROR, (errmsg("auxiliary consumer probe requires a permanent table")));

	/* The production map reader owns both initialization and its locked
	 * PageIsNew recheck. This fixture neither initializes nor marks VM bits. */
	visibilitymap_pin(relation, 0, &buffer);
	if (!BufferIsValid(buffer) || BufferIsLocal(buffer))
		ereport(ERROR, (errmsg("auxiliary consumer did not return a shared pin")));
	page = BufferGetPage(buffer);
	header = (PageHeader)page;
	if (PageIsNew(page))
		ereport(ERROR, (errmsg("auxiliary consumer returned an uninitialized VM page")));
	for (i = SizeOfPageHeaderData; i < BLCKSZ; i++) {
		digest ^= (unsigned char)page[i];
		digest *= UINT64CONST(1099511628211);
	}
	result = psprintf("pid=%d rel=%u page_new=0 lower=%u upper=%u special=%u bitmap=%llu",
					  MyProcPid, RelationGetRelid(relation), header->pd_lower, header->pd_upper,
					  header->pd_special, (unsigned long long)digest);
	ereport(NOTICE, (errmsg("aux probe pinned: %s hold=%s", result, hold ? "true" : "false")));

	while (hold && access(release_path, F_OK) != 0) {
		if (errno != ENOENT)
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read auxiliary probe release marker: %m")));
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1L,
						PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}
	ReleaseBuffer(buffer);
	table_close(relation, AccessShareLock);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}
