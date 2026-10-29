/*-------------------------------------------------------------------------
 * test_cluster_startup_native_ctrc.h
 *     Private native directory fixture for the original Startup consumer.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_STARTUP_NATIVE_CTRC_H
#define TEST_CLUSTER_STARTUP_NATIVE_CTRC_H
#include "cluster/cluster_terminal_ref_census.h"
extern void test_startup_native_ctrc_begin(unsigned mode, unsigned at);
extern ClusterCtrcLocalEmptyV1 test_startup_native_ctrc_probe(void);
extern void test_startup_native_ctrc_unlocked(void);
extern void test_startup_native_ctrc_check(unsigned probes, unsigned attempts, unsigned releases);
extern void test_startup_native_ctrc_end(void);
/* Complete original native pure predicates, no directory or service grant. */
extern ClusterCtrcItlCleanoutApplyResult test_startup_native_ctrc_project(
	const struct ClusterColdTtSlotInputV1 *subjects, Size count,
	const char *before, char *after);
extern bool test_startup_native_ctrc_heap(const char *image);
extern bool test_startup_native_ctrc_hot(const char *image, BlockNumber block);
#endif
