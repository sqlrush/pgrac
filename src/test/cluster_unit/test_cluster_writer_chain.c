/*-------------------------------------------------------------------------
 *
 * test_cluster_writer_chain.c
 *	  cluster_unit U0 truth table for cross-node terminal-writer chaining
 *	  (spec-7.1a D0): outcome kind x next-version availability ->
 *	  {TM_Updated / TM_Deleted / TM_Ok / not-mappable(53R9H)} plus the
 *	  TM_FailureData field contract (ctid / xmax real values,
 *	  cmax == InvalidCommandId per the native other-transaction contract).
 *
 *	  cluster_writer_chain_decide is pure (no buffer / no page), so the
 *	  table is enumerated exhaustively here.  The resolver integration
 *	  (TT / live-IC verdict / cross-node next-version probe) is covered
 *	  by cluster_tap (2-node write-write legs).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_writer_chain.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-7.1a-cross-instance-write-write-mvcc-coordination.md §4 U0.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include "access/htup_details.h"
#include "cluster/cluster_writer_chain.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't link full libpgport in this test binary. */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ============================================================
 *	Stubs — this binary links only cluster_writer_chain.o + libpgport.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* backend itemptr.c is not linked; block+offset equality is the contract. */
bool
ItemPointerEquals(ItemPointer pointer1, ItemPointer pointer2)
{
	return ItemPointerGetBlockNumberNoCheck(pointer1) == ItemPointerGetBlockNumberNoCheck(pointer2)
		   && ItemPointerGetOffsetNumberNoCheck(pointer1)
				  == ItemPointerGetOffsetNumberNoCheck(pointer2);
}

#define OLD_BLK 7
#define OLD_OFF 3
#define NEW_BLK 9
#define NEW_OFF 5
#define UPDATE_XID ((TransactionId)4242)

/* Sentinels: assert decide() never writes res/tmfd on the false path. */
#define RES_SENTINEL ((TM_Result)0x7f)
#define XMAX_SENTINEL ((TransactionId)0xdeadbeef)
#define CMAX_SENTINEL ((CommandId)0xfeedface)

static HeapTupleData dummy_new_tuple;

static ClusterWriterOutcome
make_outcome(ClusterWriterOutcomeKind kind, bool with_new_ctid, bool with_new_tuple)
{
	ClusterWriterOutcome wo;

	memset(&wo, 0, sizeof(wo));
	wo.kind = kind;
	if (with_new_ctid)
		ItemPointerSet(&wo.new_ctid, NEW_BLK, NEW_OFF);
	else
		ItemPointerSetInvalid(&wo.new_ctid);
	wo.new_tuple = with_new_tuple ? &dummy_new_tuple : NULL;
	return wo;
}

static void
make_sentinels(TM_Result *res, TM_FailureData *tmfd, ItemPointerData *old_ctid, bool old_ctid_self)
{
	*res = RES_SENTINEL;
	memset(tmfd, 0, sizeof(*tmfd));
	ItemPointerSetInvalid(&tmfd->ctid);
	tmfd->xmax = XMAX_SENTINEL;
	tmfd->cmax = CMAX_SENTINEL;
	/* DELETE: old tuple's t_ctid self-points; UPDATE: it names the next version. */
	if (old_ctid_self)
		ItemPointerSet(old_ctid, OLD_BLK, OLD_OFF);
	else
		ItemPointerSet(old_ctid, NEW_BLK, NEW_OFF);
}

/* ---- U0.1 remote writer aborted -> TM_Ok, tmfd untouched ---- */
UT_TEST(test_u0_aborted_maps_tm_ok)
{
	ClusterWriterOutcome wo = make_outcome(CWO_ABORTED, false, false);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, true);
	UT_ASSERT(cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)TM_Ok);
	/* TM_Ok never writes failure data (native contract: tmfd only on failure). */
	UT_ASSERT_EQ((int)tmfd.xmax, (int)XMAX_SENTINEL);
	UT_ASSERT_EQ((int)tmfd.cmax, (int)CMAX_SENTINEL);
}

/* ---- U0.2 remote DELETE committed -> TM_Deleted + full tmfd ---- */
UT_TEST(test_u0_delete_committed_maps_tm_deleted)
{
	ClusterWriterOutcome wo = make_outcome(CWO_DELETED, false, false);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, true);
	UT_ASSERT(cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)TM_Deleted);
	UT_ASSERT(ItemPointerEquals(&tmfd.ctid, &old_ctid));
	UT_ASSERT_EQ((int)tmfd.xmax, (int)UPDATE_XID);
	UT_ASSERT_EQ((int)tmfd.cmax, (int)InvalidCommandId);
}

/* ---- U0.3 remote UPDATE committed + next version probed -> TM_Updated ---- */
UT_TEST(test_u0_update_committed_maps_tm_updated)
{
	ClusterWriterOutcome wo = make_outcome(CWO_UPDATED, true, true);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, false);
	UT_ASSERT(cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)TM_Updated);
	/* ctid = the NEXT version (what EPQ chases), never the old tid. */
	UT_ASSERT(ItemPointerEquals(&tmfd.ctid, &wo.new_ctid));
	UT_ASSERT_EQ((int)ItemPointerGetBlockNumber(&tmfd.ctid), NEW_BLK);
	UT_ASSERT_EQ((int)tmfd.xmax, (int)UPDATE_XID);
	UT_ASSERT_EQ((int)tmfd.cmax, (int)InvalidCommandId);
}

/* ---- U0.4 UPDATE committed but next version NOT fetched -> not mappable ---- */
UT_TEST(test_u0_update_without_new_tuple_fails_closed)
{
	ClusterWriterOutcome wo = make_outcome(CWO_UPDATED, true, false);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, false);
	UT_ASSERT(!cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	/* false return must leave res/tmfd unwritten (no partial failure data). */
	UT_ASSERT_EQ((int)res, (int)RES_SENTINEL);
	UT_ASSERT_EQ((int)tmfd.xmax, (int)XMAX_SENTINEL);
	UT_ASSERT_EQ((int)tmfd.cmax, (int)CMAX_SENTINEL);
}

/* ---- U0.5 UPDATE committed but next-version tid invalid -> not mappable ---- */
UT_TEST(test_u0_update_without_new_ctid_fails_closed)
{
	ClusterWriterOutcome wo = make_outcome(CWO_UPDATED, false, true);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, false);
	UT_ASSERT(!cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)RES_SENTINEL);
}

/* ---- U0.6 DELETE/UPDATE with invalid update xid -> not mappable ---- */
UT_TEST(test_u0_invalid_update_xid_fails_closed)
{
	ClusterWriterOutcome del = make_outcome(CWO_DELETED, false, false);
	ClusterWriterOutcome upd = make_outcome(CWO_UPDATED, true, true);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, true);
	UT_ASSERT(!cluster_writer_chain_decide(&del, &old_ctid, InvalidTransactionId, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)RES_SENTINEL);

	make_sentinels(&res, &tmfd, &old_ctid, false);
	UT_ASSERT(!cluster_writer_chain_decide(&upd, &old_ctid, InvalidTransactionId, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)RES_SENTINEL);
}

/* ---- U0.7 UNRESOLVABLE -> not mappable, res/tmfd untouched ---- */
UT_TEST(test_u0_unresolvable_fails_closed)
{
	ClusterWriterOutcome wo = make_outcome(CWO_UNRESOLVABLE, false, false);
	TM_Result res;
	TM_FailureData tmfd;
	ItemPointerData old_ctid;

	make_sentinels(&res, &tmfd, &old_ctid, true);
	UT_ASSERT(!cluster_writer_chain_decide(&wo, &old_ctid, UPDATE_XID, &res, &tmfd));
	UT_ASSERT_EQ((int)res, (int)RES_SENTINEL);
	UT_ASSERT_EQ((int)tmfd.xmax, (int)XMAX_SENTINEL);
	UT_ASSERT_EQ((int)tmfd.cmax, (int)CMAX_SENTINEL);
}

/* ---- U0.8 tmfd == NULL supported (heapam callers reuse native fill) ---- */
UT_TEST(test_u0_null_tmfd_supported)
{
	ClusterWriterOutcome del = make_outcome(CWO_DELETED, false, false);
	ClusterWriterOutcome upd = make_outcome(CWO_UPDATED, true, true);
	ClusterWriterOutcome abo = make_outcome(CWO_ABORTED, false, false);
	TM_Result res;
	ItemPointerData old_ctid;

	ItemPointerSet(&old_ctid, OLD_BLK, OLD_OFF);
	res = RES_SENTINEL;
	UT_ASSERT(cluster_writer_chain_decide(&del, &old_ctid, UPDATE_XID, &res, NULL));
	UT_ASSERT_EQ((int)res, (int)TM_Deleted);
	res = RES_SENTINEL;
	UT_ASSERT(cluster_writer_chain_decide(&upd, &old_ctid, UPDATE_XID, &res, NULL));
	UT_ASSERT_EQ((int)res, (int)TM_Updated);
	res = RES_SENTINEL;
	UT_ASSERT(cluster_writer_chain_decide(&abo, &old_ctid, UPDATE_XID, &res, NULL));
	UT_ASSERT_EQ((int)res, (int)TM_Ok);
}

/* ---- U0.9 enum ABI stability (test-session / TAP contract) ---- */
UT_TEST(test_u0_outcome_kind_enum_stable)
{
	UT_ASSERT_EQ((int)CWO_UPDATED, 0);
	UT_ASSERT_EQ((int)CWO_DELETED, 1);
	UT_ASSERT_EQ((int)CWO_ABORTED, 2);
	UT_ASSERT_EQ((int)CWO_UNRESOLVABLE, 3);
}

int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_u0_aborted_maps_tm_ok);
	UT_RUN(test_u0_delete_committed_maps_tm_deleted);
	UT_RUN(test_u0_update_committed_maps_tm_updated);
	UT_RUN(test_u0_update_without_new_tuple_fails_closed);
	UT_RUN(test_u0_update_without_new_ctid_fails_closed);
	UT_RUN(test_u0_invalid_update_xid_fails_closed);
	UT_RUN(test_u0_unresolvable_fails_closed);
	UT_RUN(test_u0_null_tmfd_supported);
	UT_RUN(test_u0_outcome_kind_enum_stable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
