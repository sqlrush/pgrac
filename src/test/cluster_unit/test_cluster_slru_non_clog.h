/*-------------------------------------------------------------------------
 * test_cluster_slru_non_clog.h
 *     CLOG is absent from these standalone SUBTRANS/MultiXact fixtures.
 *
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *     src/test/cluster_unit/test_cluster_slru_non_clog.h
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_SLRU_NON_CLOG_H
#define TEST_CLUSTER_SLRU_NON_CLOG_H
#ifdef USE_PGRAC_CLUSTER
#include "access/slru.h"
static int
non_clog_fixture_writeback_begin(SlruCtl ctl pg_attribute_unused())
{
	return -1;
}
static void
non_clog_fixture_writeback_end(int origin pg_attribute_unused())
{}
#define cluster_clog_relmap_writeback_begin non_clog_fixture_writeback_begin
#define cluster_clog_relmap_writeback_end non_clog_fixture_writeback_end
#endif
#endif
