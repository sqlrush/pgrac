/*-------------------------------------------------------------------------
 *
 * test_cluster_xnode_lever.c
 *	  Standalone unit tests for spec-6.12 lever runtime (U-c set): the
 *	  wave-c terminal-outcome memo truth table + counter surface.
 *
 *	  U-c coverage:
 *	    1) shmem init zeroes every counter.
 *	    2) GUC off -> probe misses / install is a no-op (byte-identical
 *	       inertness of the default path).
 *	    3) terminal COMMITTED(+scn) roundtrip: install then probe hit
 *	       returns the identical status/scn; hit/install counters tick.
 *	    4) non-terminal statuses (UNKNOWN / IN_PROGRESS / SUBCOMMITTED /
 *	       CLEANED_OUT / COMMITTED without a valid SCN) are NEVER
 *	       installed (rule 8.A: nothing uncertain replays).
 *	    5) transaction scope: an entry installed under lxid A is not
 *	       replayed under lxid B (stale-transaction fencing).
 *	    6) exact-key discipline: any single key-field difference
 *	       (local_xid / cluster_epoch / tt_slot_id) misses -- the
 *	       synthetic slot-reuse edge (same slot, different xid) can
 *	       never alias to a hit.
 *	    7) ABORTED roundtrip returns ABORTED with InvalidScn.
 *	    8) measure-mode counters (resolve / tt_lookup / stamp evidence)
 *	       tick under cluster.xnode_profile with the wave GUC off, and
 *	       stay silent with both GUCs off.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_xnode_lever.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Standalone binary linking
 *	  cluster_xnode_lever.o only; PG backend symbols stubbed locally.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdlib.h>
#include <string.h>

#include "cluster/cluster_xnode_lever.h"
#include "storage/proc.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ============================================================
 * Stubs needed to link cluster_xnode_lever.o standalone.
 * ============================================================ */

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/* GUC gate symbols (cluster_guc.o not linked). */
bool cluster_page_scn_shortcut = false;
bool cluster_xnode_profile_enabled = false;

/* Backend identity: the memo stamps entries with MyProc->lxid. */
static PGPROC ut_proc;
PGPROC *MyProc = &ut_proc;

/* L105 union force-align ShmemInitStruct stub. */
void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	static union {
		uint64 force_align;
		char data[256]; /* generous; lever region is 6 counters */
	} lever_buf;
	static bool lever_initialized = false;

	if (name != NULL && strcmp(name, "pgrac cluster xnode lever") == 0) {
		Assert(size <= sizeof(lever_buf.data));
		*foundPtr = lever_initialized;
		lever_initialized = true;
		return lever_buf.data;
	}

	*foundPtr = true;
	return NULL;
}

void cluster_shmem_register_region(const void *r);
void
cluster_shmem_register_region(const void *r pg_attribute_unused())
{}


/* ============================================================
 * Helpers.
 * ============================================================ */

static ClusterTTStatusKey
mk_key(uint16 origin, uint16 seg, uint32 slot, uint32 epoch, TransactionId xid)
{
	ClusterTTStatusKey k;

	memset(&k, 0, sizeof(k));
	k.origin_node_id = origin;
	k.undo_segment_id = seg;
	k.tt_slot_id = slot;
	k.cluster_epoch = epoch;
	k.local_xid = xid;
	return k;
}

static uint64
ctr(pg_atomic_uint64 *c)
{
	return pg_atomic_read_u64(c);
}


/* ============================================================
 * U-c tests.
 * ============================================================ */

UT_TEST(test_lever_shmem_init_zeroes)
{
	cluster_xnode_lever_shmem_init();
	UT_ASSERT_NOT_NULL(ClusterXnodeLeverCtl);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_resolve_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_tt_lookup_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_hit_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_install_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_stamp_contradicted_count), (uint64)0);
}

UT_TEST(test_lever_guc_off_inert)
{
	ClusterTTStatusKey k = mk_key(1, 2, 33, 0, 1000);
	uint8 status = 0;
	SCN scn = 0;

	cluster_page_scn_shortcut = false;
	cluster_xnode_profile_enabled = false;
	ut_proc.lxid = 7;

	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_COMMITTED, (SCN)500);
	UT_ASSERT(!cluster_vis_memo_probe(&k, &status, &scn));
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_install_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_hit_count), (uint64)0);

	/* measure hooks silent with both GUCs off */
	cluster_lever_c_note_resolve();
	cluster_lever_c_note_tt_lookup(true, true);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_resolve_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_tt_lookup_count), (uint64)0);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count), (uint64)0);
}

UT_TEST(test_lever_committed_roundtrip)
{
	ClusterTTStatusKey k = mk_key(1, 2, 33, 0, 1000);
	uint8 status = 0;
	SCN scn = 0;

	cluster_page_scn_shortcut = true;
	ut_proc.lxid = 7;

	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_COMMITTED, (SCN)500);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_install_count), (uint64)1);

	UT_ASSERT(cluster_vis_memo_probe(&k, &status, &scn));
	UT_ASSERT_EQ((int)status, (int)CLUSTER_TT_STATUS_COMMITTED);
	UT_ASSERT_EQ((uint64)scn, (uint64)500);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_hit_count), (uint64)1);
}

UT_TEST(test_lever_nonterminal_never_installed)
{
	ClusterTTStatusKey k = mk_key(3, 4, 55, 0, 2000);
	uint8 status = 0;
	SCN scn = 0;
	uint64 pre_install = ctr(&ClusterXnodeLeverCtl->c_memo_install_count);

	cluster_page_scn_shortcut = true;
	ut_proc.lxid = 7;

	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_UNKNOWN, (SCN)500);
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_IN_PROGRESS, (SCN)500);
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_SUBCOMMITTED, (SCN)500);
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_CLEANED_OUT, (SCN)500);
	/* COMMITTED without a valid SCN is not a usable terminal fact. */
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_COMMITTED, InvalidScn);

	UT_ASSERT(!cluster_vis_memo_probe(&k, &status, &scn));
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_memo_install_count), pre_install);
}

UT_TEST(test_lever_lxid_scope_fencing)
{
	ClusterTTStatusKey k = mk_key(1, 2, 34, 0, 1001);
	uint8 status = 0;
	SCN scn = 0;

	cluster_page_scn_shortcut = true;
	ut_proc.lxid = 7;
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_COMMITTED, (SCN)600);
	UT_ASSERT(cluster_vis_memo_probe(&k, &status, &scn));

	/* next top-level transaction: prior entries never replay */
	ut_proc.lxid = 8;
	UT_ASSERT(!cluster_vis_memo_probe(&k, &status, &scn));
}

UT_TEST(test_lever_exact_key_discipline)
{
	ClusterTTStatusKey k = mk_key(1, 2, 40, 5, 3000);
	ClusterTTStatusKey k_other_xid = mk_key(1, 2, 40, 5, 3001);
	ClusterTTStatusKey k_other_epoch = mk_key(1, 2, 40, 6, 3000);
	ClusterTTStatusKey k_other_slot = mk_key(1, 2, 41, 5, 3000);
	uint8 status = 0;
	SCN scn = 0;

	cluster_page_scn_shortcut = true;
	ut_proc.lxid = 9;
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_COMMITTED, (SCN)700);

	/* synthetic slot-reuse edge: same slot recycled to a different xid
	 * must NOT alias to the old owner's outcome */
	UT_ASSERT(!cluster_vis_memo_probe(&k_other_xid, &status, &scn));
	UT_ASSERT(!cluster_vis_memo_probe(&k_other_epoch, &status, &scn));
	UT_ASSERT(!cluster_vis_memo_probe(&k_other_slot, &status, &scn));
	UT_ASSERT(cluster_vis_memo_probe(&k, &status, &scn));
}

UT_TEST(test_lever_aborted_roundtrip)
{
	ClusterTTStatusKey k = mk_key(2, 1, 77, 0, 4000);
	uint8 status = 0;
	SCN scn = 12345; /* poisoned; must come back InvalidScn */

	cluster_page_scn_shortcut = true;
	ut_proc.lxid = 9;

	/* the resolver passes result.commit_scn through even for ABORTED;
	 * the memo must normalize to InvalidScn */
	cluster_vis_memo_install(&k, CLUSTER_TT_STATUS_ABORTED, (SCN)999);
	UT_ASSERT(cluster_vis_memo_probe(&k, &status, &scn));
	UT_ASSERT_EQ((int)status, (int)CLUSTER_TT_STATUS_ABORTED);
	UT_ASSERT_EQ((uint64)scn, (uint64)InvalidScn);
}

UT_TEST(test_lever_measure_mode_counters)
{
	uint64 pre_resolve;
	uint64 pre_lookup;
	uint64 pre_seen;
	uint64 pre_contra;

	/* profile on, wave off: D0 measure mode */
	cluster_page_scn_shortcut = false;
	cluster_xnode_profile_enabled = true;

	pre_resolve = ctr(&ClusterXnodeLeverCtl->c_resolve_count);
	pre_lookup = ctr(&ClusterXnodeLeverCtl->c_tt_lookup_count);
	pre_seen = ctr(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count);
	pre_contra = ctr(&ClusterXnodeLeverCtl->c_stamp_contradicted_count);

	cluster_lever_c_note_resolve();
	cluster_lever_c_note_tt_lookup(true, false);
	cluster_lever_c_note_tt_lookup(true, true);
	cluster_lever_c_note_tt_lookup(false, false);

	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_resolve_count), pre_resolve + 1);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_tt_lookup_count), pre_lookup + 3);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_stamp_cached_seen_count), pre_seen + 2);
	UT_ASSERT_EQ(ctr(&ClusterXnodeLeverCtl->c_stamp_contradicted_count), pre_contra + 1);

	cluster_xnode_profile_enabled = false;
}


UT_DEFINE_GLOBALS();

int
main(int argc pg_attribute_unused(), char *argv[] pg_attribute_unused())
{
	UT_PLAN(8);

	UT_RUN(test_lever_shmem_init_zeroes);
	UT_RUN(test_lever_guc_off_inert);
	UT_RUN(test_lever_committed_roundtrip);
	UT_RUN(test_lever_nonterminal_never_installed);
	UT_RUN(test_lever_lxid_scope_fencing);
	UT_RUN(test_lever_exact_key_discipline);
	UT_RUN(test_lever_aborted_roundtrip);
	UT_RUN(test_lever_measure_mode_counters);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
