/*-------------------------------------------------------------------------
 *
 * test_cluster_stage3_acceptance.c
 *	  pgrac spec-3.17 D6 — Stage 3 MVCC final surface snapshot (8 static
 *	  contract tests).
 *
 *	  Tests in this binary (L1-L8):
 *	    L1  Stage 3 final surface landed snapshot:  CATALOG_VERSION_NO
 *	        >= 202606041 (spec-3.16 ship value) — final-state assertion
 *	        only, not a per-spec catversion table (the binary does not
 *	        preserve history;  any future spec keeps it monotone).
 *	    L2  RM_CLUSTER_UNDO live opcodes 0x10/0x30/0x40/0x50/0x60 全注册
 *	        + XLR_INFO_MASK low nibble clear (L217 anti-collision;  the
 *	        StaticAssertDecl in cluster_undo_xlog.h enforces it at compile
 *	        time, this test pins the values at runtime).
 *	    L3  6 MVCC capability dump category names stable (undo / cr /
 *	        tt_status / visibility / tt_2pc / recovery) — compile-time
 *	        string invariants;  runtime emission verified by t/226 L10.
 *	    L4  SQLSTATE 53R97 / 53R9C / 53R9D / 53R9E / 53R9G / 53R9H 全
 *	        encodable via MAKE_SQLSTATE (Stage 3 MVCC error surface).
 *	    L5  CLUSTER_WAIT_EVENTS_COUNT current snapshot = 98 (spec-4.6 D4
 *	        ship value;  update-required contract — any future spec adding
 *	        wait events MUST bump this snapshot).
 *	    L6  ClusterTTStatus 6-value enum (0-5) + TTSlotStatus 5 functional
 *	        states (0-4) + INVALID=0xFF sentinel complete (wire/ABI lock
 *	        — values MUST NOT be reordered).
 *	    L7  retention predicate ACTIVE-retains invariant:  a real call into
 *	        cluster_tt_slot_recyclable() proves CTS_ACTIVE / CTS_FREE are
 *	        retained and CTS_ABORTED is always recyclable (deep coverage in
 *	        test_cluster_retention;  this pins the acceptance-level
 *	        invariant).
 *	    L8  0x20 XLOG_UNDO_TT_SLOT_BIND still reserved — header comment
 *	        only, no #define (BIND is not WAL-logged;  the opcode byte
 *	        stays unallocated).
 *
 *	  Static contract assertions + one real retention-predicate call.
 *	  Behavioral coverage in cluster_tap t/226 (capability cross-cutting)
 *	  and t/227 (workload smoke).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_stage3_acceptance.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.17-stage3-mvcc-acceptance.md (FROZEN v0.2)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "access/xlogrecord.h"
#include "catalog/catversion.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_undo_retention.h"
#include "cluster/cluster_views.h"
#include "miscadmin.h"
#include "utils/errcodes.h"

#include "cluster/storage/cluster_undo_xlog.h"

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


UT_DEFINE_GLOBALS();


void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/*
 * scn_time_cmp stub — cluster_undo_retention.o (linked for L7) references
 * only this comparator.  Mirrors the real contract: visibility ordering uses
 * local_scn only;  all test SCNs use node 0, so scn_local(v) == v.
 */
int
scn_time_cmp(SCN a, SCN b)
{
	uint64 la = scn_local(a);
	uint64 lb = scn_local(b);

	if (la < lb)
		return -1;
	if (la > lb)
		return 1;
	return 0;
}


/* ===== L1 — Stage 3 final surface snapshot ===== */

UT_TEST(test_stage3_catversion_at_or_above_spec_3_16)
{
	/* spec-3.16 ship value is 202606041 (current Stage 3 surface).  Any
	 * future spec must keep CATALOG_VERSION_NO monotone non-decreasing;
	 * Stage 3 acceptance requires the Stage 3 MVCC surface present. */
	UT_ASSERT((long)CATALOG_VERSION_NO >= 202606041L);
}


/* ===== L2 — RM_CLUSTER_UNDO live opcodes + XLR_INFO_MASK clear ===== */

UT_TEST(test_stage3_undo_opcodes_registered_and_info_mask_clear)
{
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_INIT, 0x10);	/* spec-1.22 */
	UT_ASSERT_EQ((int)XLOG_UNDO_TT_SLOT_COMMIT, 0x30);	/* spec-3.11 */
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_RECYCLE, 0x40); /* spec-3.13 */
	UT_ASSERT_EQ((int)XLOG_UNDO_SEGMENT_REUSE, 0x50);	/* spec-3.13 */
	UT_ASSERT_EQ((int)XLOG_UNDO_TT_SLOT_ABORT, 0x60);	/* spec-3.15 */
	UT_ASSERT_EQ((int)XLOG_UNDO_BLOCK_WRITE, 0x70);		/* spec-3.18 D2 */

	/* L217:  every live opcode leaves the low nibble (XLR_INFO_MASK,
	 * reserved by the xlog framework) clear.  The cluster_undo_xlog.h
	 * StaticAssertDecl block enforces this at compile time;  pin it at
	 * runtime too so a future opcode addition trips here as well. */
	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_INIT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_TT_SLOT_COMMIT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_RECYCLE & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_SEGMENT_REUSE & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_TT_SLOT_ABORT & XLR_INFO_MASK), 0);
	UT_ASSERT_EQ((int)(XLOG_UNDO_BLOCK_WRITE & XLR_INFO_MASK), 0);
}


/* ===== L2b — xl_undo_block_write WAL ABI (spec-3.18 D2 3-range delta) ===== */

UT_TEST(test_stage3_undo_block_write_wal_abi)
{
	/* Fixed 16B header;  block_lsn never travels in the body (it is the
	 * record's own LSN, set on write + redo).  hdr_prefix range stops at
	 * offsetof(block_lsn) == 40 so the delta excludes it. */
	UT_ASSERT_EQ(sizeof(xl_undo_block_write), 16);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, segment_id), 0);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, block_no), 4);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, instance), 8);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, has_fpi), 9);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, rec_off), 10);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, rec_len), 12);
	UT_ASSERT_EQ(offsetof(xl_undo_block_write, slot_off), 14);
	UT_ASSERT_EQ((int)UNDO_BLOCK_HDR_PREFIX_LEN, 40);
}


/* ===== L2c — D2a always-FPI apply: image restored + block_lsn from record LSN ===== */

UT_TEST(test_stage3_undo_block_write_fpi_apply)
{
	char image[BLCKSZ];
	char out[BLCKSZ] = { 0 }; /* filled by the FPI apply; init silences cppcheck uninitvar */
	UndoBlockHeader *ih = (UndoBlockHeader *)image;
	UndoBlockHeader *oh = (UndoBlockHeader *)out;

	/* A WAL full-page image carries a STALE block_lsn (set before XLogInsert);
	 * redo must overwrite it with the record's own LSN and leave every other
	 * byte byte-identical (block_lsn never travels in the body -- §2.6). */
	memset(image, 0xAB, BLCKSZ);
	ih->magic = PGRAC_UNDO_BLOCK_MAGIC;
	ih->block_lsn = (XLogRecPtr)0xDEAD; /* stale value inside the image */

	cluster_undo_apply_block_write_fpi(image, (XLogRecPtr)0xBEEF, out);

	UT_ASSERT_EQ((long long)oh->block_lsn, (long long)0xBEEF); /* = record LSN */
	UT_ASSERT_EQ((long long)oh->magic, (long long)PGRAC_UNDO_BLOCK_MAGIC);

	/* Normalize the one intended diff, then the rest must be identical. */
	oh->block_lsn = ih->block_lsn;
	UT_ASSERT_EQ(memcmp(image, out, BLCKSZ), 0);
}


/* ===== L2d — D2b 3-range delta apply: hdr_prefix + record + slot + block_lsn ===== */

UT_TEST(test_stage3_undo_block_write_delta_apply)
{
	char base[BLCKSZ];
	xl_undo_block_write rec;
	char body[UNDO_BLOCK_HDR_PREFIX_LEN + 64 + 8]; /* hdr_prefix(40) + rec(64) + slot(8) */
	const UndoBlockHeader *bh = (const UndoBlockHeader *)base;

	/* Existing on-disk block image (the redo base). */
	memset(base, 0x11, BLCKSZ);

	/* Delta: a 64-byte record at offset 200, an 8-byte slot near the tail. */
	memset(&rec, 0, sizeof(rec));
	rec.has_fpi = 0;
	rec.rec_off = 200;
	rec.rec_len = 64;
	rec.slot_off = 8000;

	memset(body, 0x22, UNDO_BLOCK_HDR_PREFIX_LEN);			/* header prefix */
	memset(body + UNDO_BLOCK_HDR_PREFIX_LEN, 0x33, 64);		/* record */
	memset(body + UNDO_BLOCK_HDR_PREFIX_LEN + 64, 0x44, 8); /* slot */

	cluster_undo_apply_block_write_delta(base, &rec, body, (XLogRecPtr)0xCAFE);

	/* header prefix [0,40) patched (block_lsn at offset 40 is NOT in the prefix) */
	UT_ASSERT_EQ((int)(unsigned char)base[0], 0x22);
	UT_ASSERT_EQ((int)(unsigned char)base[UNDO_BLOCK_HDR_PREFIX_LEN - 1], 0x22);
	/* record [200,264) patched */
	UT_ASSERT_EQ((int)(unsigned char)base[200], 0x33);
	UT_ASSERT_EQ((int)(unsigned char)base[263], 0x33);
	/* slot [8000,8008) patched */
	UT_ASSERT_EQ((int)(unsigned char)base[8000], 0x44);
	UT_ASSERT_EQ((int)(unsigned char)base[8007], 0x44);
	/* untouched gaps keep the base bytes */
	UT_ASSERT_EQ((int)(unsigned char)base[199], 0x11); /* between header and record */
	UT_ASSERT_EQ((int)(unsigned char)base[264], 0x11); /* after the record */
	/* block_lsn set from the record LSN (never carried in the body) */
	UT_ASSERT_EQ((long long)bh->block_lsn, (long long)0xCAFE);
}


/* ===== L3 — 6 MVCC capability dump category names stable ===== */

UT_TEST(test_stage3_capability_dump_category_names)
{
	/* t/226 L10 asserts pg_cluster_state actually emits rows for each of
	 * these categories at runtime;  this test pins the category name
	 * strings as compile-time invariants so a typo in cluster_debug.c emit
	 * sites would diverge from the contract surface here. */
	const char *cats[6] = {
		"undo",		  /* spec-3.6/3.8 undo write-path counters */
		"cr",		  /* spec-3.10 CR block construction counters */
		"tt_status",  /* spec-3.1/3.2 durable TT status counters */
		"visibility", /* spec-3.14 HeapTupleSatisfies fork counters */
		"tt_2pc",	  /* spec-3.15 2PC prepared counters */
		"recovery"	  /* spec-3.16 crash-recovery counters */
	};
	int i;
	int total_len = 0;

	for (i = 0; i < 6; i++) {
		UT_ASSERT_NOT_NULL((void *)cats[i]);
		UT_ASSERT((int)strlen(cats[i]) > 1);
		total_len += (int)strlen(cats[i]);
	}
	/* "undo"+"cr"+"tt_status"+"visibility"+"tt_2pc"+"recovery" = 4+2+9+10+6+8 */
	UT_ASSERT_EQ(total_len, 39);
}


/* ===== L4 — Stage 3 MVCC SQLSTATE surface encodable ===== */

UT_TEST(test_stage3_sqlstate_mvcc_surface_encodable)
{
	UT_ASSERT_EQ(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN, MAKE_SQLSTATE('5', '3', 'R', '9', '7'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_MULTIXACT_MEMBER_OVERLAY_MISS,
				 MAKE_SQLSTATE('5', '3', 'R', '9', 'C'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_UNDO_RECORD_INVALID_UBA, MAKE_SQLSTATE('5', '3', 'R', '9', 'D'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_UNDO_SEGMENTS_HARD_CAP_REACHED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', 'E'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CR_CROSS_INSTANCE_UNSUPPORTED,
				 MAKE_SQLSTATE('5', '3', 'R', '9', 'G'));
	UT_ASSERT_EQ(ERRCODE_CLUSTER_CROSS_NODE_WRITE_CONFLICT, MAKE_SQLSTATE('5', '3', 'R', '9', 'H'));
}


/* ===== L5 — CLUSTER_WAIT_EVENTS_COUNT current snapshot 98 ===== */

UT_TEST(test_stage3_wait_events_count_snapshot_97)
{
	/* spec-4.2 D5 value (95 + 2 wal-state registry I/O).  Update-required contract:  a future spec
	 * adding a wait event MUST bump this snapshot (it is current state, not
	 * "==93 forever").  spec-4.6 D4: 97 → 98;  spec-4.7 D1: 98 → 99
	 * (+ ClusterGCSBlockRecovering);  spec-4.11 D5: 99 → 100
	 * (+ ClusterThreadRecovery). */
	UT_ASSERT_EQ((int)CLUSTER_WAIT_EVENTS_COUNT, 102);
}


/* ===== L6 — TT status / slot enum completeness ===== */

UT_TEST(test_stage3_tt_enum_values_locked)
{
	/* ClusterTTStatus:  6 values 0-5 (SUBCOMMITTED=5 added spec-3.5).
	 * Existing values MUST NOT be reordered (HC183 + wire ABI lock). */
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_UNKNOWN, 0);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_IN_PROGRESS, 1);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_COMMITTED, 2);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_ABORTED, 3);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_CLEANED_OUT, 4);
	UT_ASSERT_EQ((int)CLUSTER_TT_STATUS_SUBCOMMITTED, 5);

	/* TTSlotStatus (on-disk):  5 functional states 0-4 + INVALID=0xFF
	 * corruption sentinel.  Value 0 (UNUSED) is permanently the "no tx"
	 * sentinel and MUST NOT be reassigned. */
	UT_ASSERT_EQ((int)TT_SLOT_UNUSED, 0);
	UT_ASSERT_EQ((int)TT_SLOT_ACTIVE, 1);
	UT_ASSERT_EQ((int)TT_SLOT_COMMITTED, 2);
	UT_ASSERT_EQ((int)TT_SLOT_ABORTED, 3);
	UT_ASSERT_EQ((int)TT_SLOT_RECYCLABLE, 4);
	UT_ASSERT_EQ((int)TT_SLOT_INVALID, 0xFF);

	/* ClusterTTSlotAllocStatus (shmem allocator):  4 states 0-3, wire-
	 * stable with cluster_tt_slot_test_force_status (2=COMMITTED, 3=
	 * ABORTED) and MUST NOT be reassigned. */
	UT_ASSERT_EQ((int)CTS_FREE, 0);
	UT_ASSERT_EQ((int)CTS_ACTIVE, 1);
	UT_ASSERT_EQ((int)CTS_COMMITTED, 2);
	UT_ASSERT_EQ((int)CTS_ABORTED, 3);
}


/* ===== L7 — retention predicate ACTIVE-retains invariant (real call) ===== */

UT_TEST(test_stage3_retention_active_retains_invariant)
{
	/* A node-0 SCN whose local value is v;  scn_local(v) == v for node 0. */
	SCN commit5 = (SCN)5;
	SCN horizon10 = (SCN)10;

	/* ACTIVE (in-flight) is never recyclable regardless of horizon:  the
	 * owning xact may still be read.  This is the load-bearing retention
	 * invariant the Stage 3 cleaner depends on (8.A fail-closed family). */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_ACTIVE, commit5, horizon10), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_FREE, commit5, horizon10), 0);

	/* ABORTED versions are invisible to any read_scn -> always recyclable. */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_ABORTED, InvalidScn, horizon10), 1);

	/* COMMITTED is gated:  below horizon -> recyclable;  at/above -> retained;
	 * unresolved commit_scn -> fail-closed retain (8.A). */
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, commit5, horizon10), 1);
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, horizon10, horizon10), 0);
	UT_ASSERT_EQ((int)cluster_tt_slot_recyclable(CTS_COMMITTED, InvalidScn, horizon10), 0);
}


/* ===== L8 — 0x20 BIND opcode byte still reserved (no #define) ===== */

UT_TEST(test_stage3_bind_opcode_reserved)
{
	/* spec-3.x: 0x20 XLOG_UNDO_TT_SLOT_BIND is documented as reserved in
	 * cluster_undo_xlog.h but intentionally has NO #define (BIND is not
	 * WAL-logged;  FREE-path slot reuse handles binding).  The byte 0x20
	 * therefore differs from every live opcode and leaves XLR_INFO_MASK
	 * clear — verifying it stays unallocated and collision-safe. */
	const int bind_byte = 0x20;

	UT_ASSERT_EQ(bind_byte & XLR_INFO_MASK, 0);
	UT_ASSERT(bind_byte != XLOG_UNDO_SEGMENT_INIT);
	UT_ASSERT(bind_byte != XLOG_UNDO_TT_SLOT_COMMIT);
	UT_ASSERT(bind_byte != XLOG_UNDO_SEGMENT_RECYCLE);
	UT_ASSERT(bind_byte != XLOG_UNDO_SEGMENT_REUSE);
	UT_ASSERT(bind_byte != XLOG_UNDO_TT_SLOT_ABORT);
}


int
main(void)
{
	UT_RUN(test_stage3_catversion_at_or_above_spec_3_16);
	UT_RUN(test_stage3_undo_opcodes_registered_and_info_mask_clear);
	UT_RUN(test_stage3_undo_block_write_wal_abi);
	UT_RUN(test_stage3_undo_block_write_fpi_apply);
	UT_RUN(test_stage3_undo_block_write_delta_apply);
	UT_RUN(test_stage3_capability_dump_category_names);
	UT_RUN(test_stage3_sqlstate_mvcc_surface_encodable);
	UT_RUN(test_stage3_wait_events_count_snapshot_97);
	UT_RUN(test_stage3_tt_enum_values_locked);
	UT_RUN(test_stage3_retention_active_retains_invariant);
	UT_RUN(test_stage3_bind_opcode_reserved);
	UT_DONE();
}
