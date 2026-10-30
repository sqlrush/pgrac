/*-------------------------------------------------------------------------
 *
 * test_cluster_owner_full_fixture.h
 *    Host-only owner FULL producer/consumer fixture bytes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_owner_full_fixture.h
 *
 * NOTES
 *    This is a pgrac-original test fixture. Origin I/O, native membership
 *    and backend publication remain explicit fixture boundaries.
 *
 *-------------------------------------------------------------------------
 */
#ifndef TEST_CLUSTER_OWNER_FULL_FIXTURE_H
#define TEST_CLUSTER_OWNER_FULL_FIXTURE_H
#include "cluster/cluster_savepoint.h"
#include "storage/buf_internals.h"
#include "storage/bufpage.h"
typedef struct TestOwnerFullFixture {
	uint64 magic;
	ClusterSavepointOperation expected;
	BufferTag tag;
	SCN read_scn;
	PGAlignedBlock full;
	PGAlignedBlock undo;
} TestOwnerFullFixture;
#define TEST_PEER_FULL_MAGIC UINT64CONST(0x4a50454546554c4c)
#define TEST_OWNER_FULL_MAGIC UINT64CONST(0x4a4f574e46554c4c)
#endif
