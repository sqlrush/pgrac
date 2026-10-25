/* Native publication boundaries needed by the relmap allocator interlock.
 * SLRU extension, stripe authority and locking remain controlled leaves.
 * Author: SqlRush <sqlrush@gmail.com> */

UT_TEST(native_subxid_publication_preserves_the_final_cache_slot)
{
	FullTransactionId top, last, overflow;

	reset_allocator();
	top = GetNewTransactionId(false);
	proc.subxidStatus.count = subxids[0].count = PGPROC_MAX_CACHED_SUBXIDS - 1;
	for (unsigned i = 0; i < PGPROC_MAX_CACHED_SUBXIDS - 1; i++)
		proc.subxids.xids[i] = 5000 + i;
	last = GetNewTransactionId(true);
	UT_ASSERT_EQ(proc.subxidStatus.count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT_EQ(subxids[0].count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT(!proc.subxidStatus.overflowed && !subxids[0].overflowed);
	UT_ASSERT_EQ(proc.subxids.xids[PGPROC_MAX_CACHED_SUBXIDS - 1], XidFromFullTransactionId(last));
	overflow = GetNewTransactionId(true);
	UT_ASSERT(proc.subxidStatus.overflowed && subxids[0].overflowed);
	UT_ASSERT_EQ(proc.subxidStatus.count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT_EQ(subxids[0].count, PGPROC_MAX_CACHED_SUBXIDS);
	UT_ASSERT_EQ(proc.subxids.xids[PGPROC_MAX_CACHED_SUBXIDS - 1], XidFromFullTransactionId(last));
	for (unsigned i = 0; i < PGPROC_MAX_CACHED_SUBXIDS - 1; i++)
		UT_ASSERT_EQ(proc.subxids.xids[i], 5000 + i);
	UT_ASSERT_EQ(proc.xid, XidFromFullTransactionId(top));
	UT_ASSERT_EQ(visible_xids[0], XidFromFullTransactionId(top));
	UT_ASSERT(FullTransactionIdPrecedes(last, overflow));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), U64FromFullTransactionId(overflow));
	UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), U64FromFullTransactionId(overflow) + 1);
	UT_ASSERT_EQ(native_publish_count, 3);
	UT_ASSERT_EQ(native_publish_order_errors + lock_errors + early_extensions, 0);
	UT_ASSERT(!held);
}

UT_TEST(native_publication_keeps_the_full_era_across_raw_rollover)
{
	for (unsigned which = 0; which < 4; which++)
	{
		bool striped = (which & 1) != 0;
		uint64 era = which >= 2 ? 7 : 0;
		uint64 expected, next;
		FullTransactionId issued;

		reset_allocator();
		cluster_enabled = cluster_shared_catalog = cluster_xid_striping = striped;
		variables.nextXid = FullTransactionIdFromU64((era << 32) | UINT32_MAX);
		reservation = true;
		floor_full = era << 32;
		limit_full = ((era + 1) << 32) + 512;
		expected = striped ? ((era + 1) << 32) + cluster_node_id : (era << 32) | UINT32_MAX;
		next = striped ? expected + 1 : ((era + 1) << 32) + FirstNormalTransactionId;
		issued = GetNewTransactionId(false);
		UT_ASSERT_EQ(U64FromFullTransactionId(issued), expected);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.nextXid), next);
		UT_ASSERT_EQ(proc.xid, XidFromFullTransactionId(issued));
		UT_ASSERT_EQ(visible_xids[0], XidFromFullTransactionId(issued));
		UT_ASSERT_EQ(variables.clusterIssuedState, striped ? 1 : 0);
		UT_ASSERT_EQ(U64FromFullTransactionId(variables.clusterLastIssuedXid), striped ? expected : 0);
		UT_ASSERT_EQ(native_publish_count, striped ? 1 : 0);
		UT_ASSERT_EQ(waits + lock_errors + native_publish_order_errors + early_extensions, 0);
		UT_ASSERT(!held);
	}
}
