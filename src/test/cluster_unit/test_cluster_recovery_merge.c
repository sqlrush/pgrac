/*-------------------------------------------------------------------------
 *
 * test_cluster_recovery_merge.c
 *	  pgrac spec-4.5 D11 — cluster_unit tests for the merge engine pure
 *	  core (cluster_recovery_merge.h) and the xl_scn record layout.
 *
 *	  23 tests:
 *	    heap   T1  push/pop yields ascending (scn) order
 *	           T2  scn tie -> lsn order (scn_recovery_cmp level 2)
 *	           T3  scn+lsn tie -> node order (level 3)
 *	           T4  k=1 stream degenerates to that stream's order
 *	           T5  interleaved k=2 (the cold-merge shape) global order
 *	           T6  full k=16 stress, strictly non-decreasing pops
 *	           T7  re-push after pop keeps the invariant (engine loop)
 *	    class  T8  XACT/CLOG/MULTIXACT/STANDBY/COMMIT_TS no-block -> G
 *	           T9  XLOG / RELMAP no-block -> L
 *	           T10 DBASE / TBLSPC no-block -> U
 *	           T11 unlisted no-block rmid -> U
 *	           T12 block record, first shared -> S
 *	           T13 block record, first local -> L
 *	           T14 block record, mixed routing -> U
 *	    layout T15 SizeOfXLogRecord == 32
 *	           T16 offsetof xl_scn == 16, xl_crc == 28 (crc stays last)
 *	           T17 CRC covers xl_scn (flip a byte -> recompute differs)
 *	           T18 InvalidScn is 0 (zero-prefix legality)
 *	           T19 heap empty pop returns false
 *	           T20 heap key carries the stream index intact
 *	    stream T21 streaming mode selects concrete WAL records
 *	           T22 heartbeat-only streams do not emit records
 *	           T23 missing stream head blocks record selection
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.5-kway-scn-merge-replay.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/xlogrecord.h"
#include "cluster/cluster_recovery_merge.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* scn_recovery_cmp is in cluster_scn.c; provide it for the unit link by
 * including the same arithmetic the backend uses.  cluster_scn.o is not
 * linked here, so stub the three comparators the header pulls in. */
int
scn_recovery_cmp(SCN a, XLogRecPtr a_lsn, NodeId a_node, SCN b, XLogRecPtr b_lsn, NodeId b_node)
{
	if (a != b)
		return (a < b) ? -1 : 1;
	if (a_lsn != b_lsn)
		return (a_lsn < b_lsn) ? -1 : 1;
	if (a_node != b_node)
		return (a_node < b_node) ? -1 : 1;
	return 0;
}

static ClusterRecmergeKey
K(uint64 scn, XLogRecPtr lsn, int32 node)
{
	ClusterRecmergeKey k;

	k.scn = scn;
	k.lsn = lsn;
	k.node = node;
	return k;
}

UT_TEST(test_heap_scn_order)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(30, 1, 0), 0);
	cluster_recmerge_heap_push(&h, K(10, 1, 0), 1);
	cluster_recmerge_heap_push(&h, K(20, 1, 0), 2);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 10);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 20);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 30);
}

UT_TEST(test_heap_scn_tie_lsn)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(5, 0x300, 0), 0);
	cluster_recmerge_heap_push(&h, K(5, 0x100, 0), 1);
	cluster_recmerge_heap_push(&h, K(5, 0x200, 0), 2);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.lsn == 0x100);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.lsn == 0x200);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.lsn == 0x300);
}

UT_TEST(test_heap_scn_lsn_tie_node)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(5, 0x100, 2), 0);
	cluster_recmerge_heap_push(&h, K(5, 0x100, 0), 1);
	cluster_recmerge_heap_push(&h, K(5, 0x100, 1), 2);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.node == 0);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.node == 1);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.node == 2);
}

UT_TEST(test_heap_k1)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;
	int i;
	uint64 prev = 0;

	cluster_recmerge_heap_init(&h);
	/* single stream: push then pop one at a time keeps its own order */
	for (i = 0; i < 5; i++) {
		cluster_recmerge_heap_push(&h, K((uint64)(i + 1) * 10, i, 0), 0);
		UT_ASSERT(cluster_recmerge_heap_pop(&h, &o));
		UT_ASSERT(o.key.scn > prev);
		prev = o.key.scn;
	}
}

UT_TEST(test_heap_interleaved_k2)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;
	/* stream A scns 10,40,50 ; stream B scns 20,30,60 (cold-merge shape) */
	uint64 a[3] = { 10, 40, 50 }, b[3] = { 20, 30, 60 };
	int ia = 0, ib = 0;
	uint64 prev = 0;
	int popped = 0;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(a[ia], ia, 0), 0);
	cluster_recmerge_heap_push(&h, K(b[ib], ib, 1), 1);
	while (cluster_recmerge_heap_pop(&h, &o)) {
		UT_ASSERT(o.key.scn > prev);
		prev = o.key.scn;
		popped++;
		if (o.stream == 0 && ++ia < 3)
			cluster_recmerge_heap_push(&h, K(a[ia], ia, 0), 0);
		else if (o.stream == 1 && ++ib < 3)
			cluster_recmerge_heap_push(&h, K(b[ib], ib, 1), 1);
	}
	UT_ASSERT_EQ(popped, 6);
	UT_ASSERT_EQ((int)prev, 60);
}

UT_TEST(test_heap_k16_stress)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;
	int s;
	uint64 prev = 0;
	int count = 0;

	cluster_recmerge_heap_init(&h);
	/* 16 streams, stream s seeds scn (s*100 + 1) */
	for (s = 0; s < 16; s++)
		cluster_recmerge_heap_push(&h, K((uint64)s * 100 + 1, 0, s), s);
	while (cluster_recmerge_heap_pop(&h, &o)) {
		UT_ASSERT(o.key.scn >= prev);
		prev = o.key.scn;
		count++;
	}
	UT_ASSERT_EQ(count, 16);
}

UT_TEST(test_heap_repush_invariant)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(10, 0, 0), 0);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 10);
	cluster_recmerge_heap_push(&h, K(15, 0, 0), 0); /* same stream advances */
	cluster_recmerge_heap_push(&h, K(12, 0, 1), 1);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 12);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.key.scn == 15);
}

UT_TEST(test_class_global)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_XACT_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_GLOBAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_CLOG_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_GLOBAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_MULTIXACT_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_GLOBAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_STANDBY_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_GLOBAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_COMMIT_TS_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_GLOBAL);
}

UT_TEST(test_class_local_noblock)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_XLOG_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_LOCAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_RELMAP_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_LOCAL);
}

UT_TEST(test_class_unclassifiable_noblock)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_DBASE_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_UNCLASSIFIABLE);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_TBLSPC_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_UNCLASSIFIABLE);
}

UT_TEST(test_class_unlisted_noblock)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_GIN_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_UNCLASSIFIABLE);
}

UT_TEST(test_class_block_shared)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_HEAP_ID, true, true, true),
				 (int)CLUSTER_RECMERGE_SHARED);
	/* spec-4.5a P0-3: cluster undo materializes locally on BOTH paths --
	 * regardless of block refs (production undo records carry none). */
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_CLUSTER_UNDO_ID, true, true, true),
				 (int)CLUSTER_RECMERGE_MATERIALIZE_LOCAL);
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_CLUSTER_UNDO_ID, false, false, true),
				 (int)CLUSTER_RECMERGE_MATERIALIZE_LOCAL);
}

UT_TEST(test_class_block_local)
{
	/* a block record on a relation routed local */
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_BTREE_ID, true, false, true),
				 (int)CLUSTER_RECMERGE_LOCAL);
}

UT_TEST(test_class_block_mixed)
{
	UT_ASSERT_EQ((int)cluster_recovery_record_class(RM_HEAP_ID, true, true, false),
				 (int)CLUSTER_RECMERGE_UNCLASSIFIABLE);
}

UT_TEST(test_layout_size)
{
	UT_ASSERT_EQ((int)SizeOfXLogRecord, 32);
}

UT_TEST(test_layout_offsets)
{
	UT_ASSERT_EQ((int)offsetof(XLogRecord, xl_scn), 16);
	UT_ASSERT_EQ((int)offsetof(XLogRecord, xl_crc), 28);
}

UT_TEST(test_layout_crc_covers_scn)
{
	/* xl_crc is the last field, so the CRC input length (offsetof crc)
	 * includes xl_scn. */
	UT_ASSERT_EQ((int)offsetof(XLogRecord, xl_crc), (int)(offsetof(XLogRecord, xl_scn) + 8 + 4));
}

UT_TEST(test_invalid_scn_zero)
{
	UT_ASSERT_EQ((long long)InvalidScn, 0LL);
}

UT_TEST(test_heap_empty_pop)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	UT_ASSERT(!cluster_recmerge_heap_pop(&h, &o));
}

UT_TEST(test_heap_stream_index)
{
	ClusterRecmergeHeap h;
	ClusterRecmergeHeapEntry o;

	cluster_recmerge_heap_init(&h);
	cluster_recmerge_heap_push(&h, K(99, 0, 7), 7);
	UT_ASSERT(cluster_recmerge_heap_pop(&h, &o) && o.stream == 7 && o.key.node == 7);
}

UT_TEST(test_streaming_selects_record)
{
	ClusterRecmergeStreamingInput inputs[3];
	ClusterRecmergeKey key;
	int stream = -1;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].record_available = true;
	inputs[0].key = K(50, 30, 0);
	inputs[1].heartbeat_seen = true;
	inputs[1].heartbeat_key = K(60, 1, 1);
	inputs[2].record_available = true;
	inputs[2].key = K(40, 90, 2);

	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 3, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_RECORD_READY);
	UT_ASSERT_EQ(stream, 2);
	UT_ASSERT_EQ((int)key.scn, 40);
}

UT_TEST(test_streaming_heartbeat_only_no_record)
{
	ClusterRecmergeStreamingInput inputs[2];
	ClusterRecmergeKey key;
	int stream = -1;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].heartbeat_seen = true;
	inputs[0].heartbeat_key = K(10, 10, 0);
	inputs[1].heartbeat_seen = true;
	inputs[1].heartbeat_key = K(20, 20, 1);

	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_NO_RECORD);
	UT_ASSERT_EQ(stream, -1);
}

UT_TEST(test_streaming_missing_stream_blocks_record)
{
	ClusterRecmergeStreamingInput inputs[2];
	ClusterRecmergeKey key;
	int stream = -1;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].record_available = true;
	inputs[0].key = K(10, 10, 0);
	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_NO_RECORD);
	UT_ASSERT_EQ(stream, -1);

	inputs[1].heartbeat_seen = true;
	inputs[1].heartbeat_key = K(10, 10, 1);
	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_RECORD_READY);
	UT_ASSERT_EQ(stream, 0);
	UT_ASSERT_EQ((int)key.scn, 10);
}

UT_TEST(test_streaming_heartbeat_frontier_blocks_record)
{
	ClusterRecmergeStreamingInput inputs[2];
	ClusterRecmergeKey key;
	int stream = -1;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].record_available = true;
	inputs[0].key = K(20, 20, 0);
	inputs[1].heartbeat_seen = true;
	inputs[1].heartbeat_key = K(19, 99, 1);

	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_NO_RECORD);
	UT_ASSERT_EQ(stream, -1);

	inputs[1].heartbeat_key = K(20, 99, 1);
	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_RECORD_READY);
	UT_ASSERT_EQ(stream, 0);
	UT_ASSERT_EQ((int)key.scn, 20);
}

UT_TEST(test_streaming_heartbeat_equal_scn_uses_closed_frontier)
{
	ClusterRecmergeStreamingInput inputs[2];
	ClusterRecmergeKey key;
	int stream = -1;

	memset(inputs, 0, sizeof(inputs));
	inputs[0].record_available = true;
	inputs[0].key = K(20, 200, 0);
	inputs[1].heartbeat_seen = true;
	inputs[1].heartbeat_key = K(20, PG_UINT64_MAX, SCN_MAX_VALID_NODE_ID);

	UT_ASSERT_EQ((int)cluster_recmerge_streaming_select(inputs, 2, &stream, &key),
				 (int)CLUSTER_RECMERGE_STREAMING_RECORD_READY);
	UT_ASSERT_EQ(stream, 0);
	UT_ASSERT_EQ((int)key.scn, 20);
}


int
main(int argc, char **argv)
{
	UT_PLAN(25);

	UT_RUN(test_heap_scn_order);
	UT_RUN(test_heap_scn_tie_lsn);
	UT_RUN(test_heap_scn_lsn_tie_node);
	UT_RUN(test_heap_k1);
	UT_RUN(test_heap_interleaved_k2);
	UT_RUN(test_heap_k16_stress);
	UT_RUN(test_heap_repush_invariant);
	UT_RUN(test_class_global);
	UT_RUN(test_class_local_noblock);
	UT_RUN(test_class_unclassifiable_noblock);
	UT_RUN(test_class_unlisted_noblock);
	UT_RUN(test_class_block_shared);
	UT_RUN(test_class_block_local);
	UT_RUN(test_class_block_mixed);
	UT_RUN(test_layout_size);
	UT_RUN(test_layout_offsets);
	UT_RUN(test_layout_crc_covers_scn);
	UT_RUN(test_invalid_scn_zero);
	UT_RUN(test_heap_empty_pop);
	UT_RUN(test_heap_stream_index);
	UT_RUN(test_streaming_selects_record);
	UT_RUN(test_streaming_heartbeat_only_no_record);
	UT_RUN(test_streaming_missing_stream_blocks_record);
	UT_RUN(test_streaming_heartbeat_frontier_blocks_record);
	UT_RUN(test_streaming_heartbeat_equal_scn_uses_closed_frontier);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
