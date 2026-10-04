/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_retained_cut_records.c
 *	  Retention lower census (S07) over records in their real WAL formats.
 *
 *	  The product census source is compiled into this test and linked with
 *	  the actual page route and detached preflight, the typed SIDE planner
 *	  and transaction decoder, the SPACE codecs and xactdesc parsing.  Only
 *	  the retained input scope is a fixture: it feeds decoded records built
 *	  from real record layouts (xl_xact_commit/abort with their xinfo
 *	  sections, xl_smgr_create/truncate, typed SPACE drops) as if read from
 *	  each source, so every classification below comes from the real
 *	  decoders or from the census's native fallback.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_wal_retained_cut_records.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-s9p2-03-shared-wal-and-checkpoint.md
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stdlib.h>

#include "../../backend/cluster/cluster_wal_retained_cut.c"

#include "access/xact.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_block_apply.h"
#include "cluster/cluster_space_reservation.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#include "test_cluster_wal_retained_cut_records_boundary.h"

#include "test_cluster_wal_retained_cut_records_cases.h"

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_records_commit_tombstone_pins_its_source);
	UT_RUN(test_records_commit_with_invalidations_is_classified);
	UT_RUN(test_records_abort_with_relations_pins_its_source);
	UT_RUN(test_records_native_smgr_create_and_truncate);
	UT_RUN(test_records_foreign_source_pins_only_itself);
	UT_RUN(test_records_unknown_native_record_still_refuses);
	UT_RUN(test_records_short_abort_does_not_advance_retention);
	UT_DONE();
	return ut_failed_count != 0;
}
