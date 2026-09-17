/*-------------------------------------------------------------------------
 *
 * test_cluster_wal_state.c
 *	  pgrac spec-4.2 D6 — cluster_unit tests for the ClusterWalState
 *	  registry pure helpers (cluster_wal_state.h, header-only inline).
 *
 *	  21 tests covering:
 *	    T1   header sizeof == 512 + offsetof locks (incl. explicit
 *	         _pad_12 at 12..15 -- v0.2 P2)
 *	    T2   slot sizeof == 512 + offsetof locks
 *	    T3   SLOT_OFFSET single-source macro: thread 1 -> 512,
 *	         thread 128 -> 65536, last slot end == FILE_SIZE (v0.2 P0)
 *	    T4   header fill/validate round-trip
 *	    T5   header corruption rejected: crc flip / magic / version /
 *	         slot_count
 *	    T6   slot fill/validate round-trip (owner mode, OK)
 *	    T7   all-zero slot classifies EMPTY
 *	    T8   slot corruption -> CORRUPT: magic / version / crc
 *	    T9   slot self-description mismatch -> CORRUPT (mis-addressed)
 *	    T10  invalid state value -> CORRUPT (0 and 3+; L3 three-band)
 *	    T11  foreign node identity -> FOREIGN (owner mode)
 *	    T12  reader mode (expect_node = -1) accepts any node
 *	    T13  state enum on-disk values locked (ACTIVE=1 / STOPPED=2)
 *	    T14  EMPTY requires the full 512B zero: zeroed fields + body
 *	         garbage -> CORRUPT (round-2 P1, absence-as-proof)
 *	    T15  crc covers tli/lsn/scn fields (flip each -> bad crc)
 *	    T16  A1 W2 ACTIVE exact mask, extension preservation and W6 clear
 *	    T17  A1 W2 EMPTY initialization
 *	    T18  A1 W4 telemetry exact mask
 *	    T19  A1 W5a checkpoint and W5b sticky exact masks
 *	    T20  A1 W3 STOPPED exact mask + idempotent no-write result
 *	    T21  A1 typed rejection and distinct post-read mismatch detection
 *
 *	  Linkage mirrors test_cluster_wal_thread: header-only inclusion +
 *	  libpgcommon/libpgport for pg_crc32c -- no module .o, no stubs.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-4.2-wal-thread-metadata-catalog.md (FROZEN v1.0)
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>
#include <string.h>

#include "cluster/cluster_wal_state.h"

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


/* cassert builds pull libpgport objects that reference this. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


UT_TEST(test_header_layout_locks)
{
	UT_ASSERT_EQ((int)sizeof(ClusterWalStateHeader), 512);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, slot_count), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, _pad_12), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, created_at), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, _reserved), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateHeader, crc), 504);
}

UT_TEST(test_slot_layout_locks)
{
	UT_ASSERT_EQ((int)sizeof(ClusterWalStateSlot), 512);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, thread_id), 6);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, node_id), 8);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, state), 12);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, tli), 16);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, started_at), 24);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, last_updated), 32);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, highest_lsn), 40);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, highest_scn), 48);
	UT_ASSERT_EQ((int)offsetof(ClusterWalStateSlot, crc), 504);
}

/* ---- T3: the single-source offset macro (spec-4.2 v0.2 P0 lock) ---- */
UT_TEST(test_slot_offset_macro_locks)
{
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(1), 512L);
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(2), 1024L);
	UT_ASSERT_EQ((long)CLUSTER_WAL_STATE_SLOT_OFFSET(128), 65536L);
	/* last slot ends exactly at the fixed file size: a write through
	 * the macro can never extend the file */
	UT_ASSERT_EQ((long)(CLUSTER_WAL_STATE_SLOT_OFFSET(128) + CLUSTER_WAL_STATE_SLOT_SIZE),
				 (long)CLUSTER_WAL_STATE_FILE_SIZE);
	UT_ASSERT_EQ((int)CLUSTER_WAL_STATE_FILE_SIZE, 66048);
}

UT_TEST(test_header_roundtrip)
{
	ClusterWalStateHeader h;
	const char *reason = (const char *)0x1;

	cluster_wal_state_header_fill(&h, 1234567890LL);
	UT_ASSERT_EQ(h.magic == CLUSTER_WAL_STATE_HEADER_MAGIC, true);
	UT_ASSERT_EQ((int)h.slot_count, 128);
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), true);
	UT_ASSERT_EQ(reason == NULL, true);
}

UT_TEST(test_header_corruption_rejected)
{
	ClusterWalStateHeader h;
	const char *reason = NULL;

	cluster_wal_state_header_fill(&h, 42);
	h.created_at ^= 1;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad crc"), 0);

	cluster_wal_state_header_fill(&h, 42);
	h.magic = 0xDEADBEEF;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad magic"), 0);

	cluster_wal_state_header_fill(&h, 42);
	h.version = 99;
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad version"), 0);

	/* slot_count is covered by crc; emulate a v2-style mismatch by
	 * refilling crc over a tampered count */
	cluster_wal_state_header_fill(&h, 42);
	h.slot_count = 64;
	h.crc = cluster_wal_state_block_crc(&h);
	UT_ASSERT_EQ(cluster_wal_state_header_validate(&h, &reason), false);
	UT_ASSERT_EQ(strcmp(reason, "bad slot_count"), 0);
}

UT_TEST(test_slot_roundtrip_owner_ok)
{
	ClusterWalStateSlot s;
	const char *reason = (const char *)0x1;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 0x1234,
								0x5678);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason), (int)CLUSTER_WAL_SLOT_OK);
	UT_ASSERT_EQ(reason == NULL, true);
}

UT_TEST(test_slot_empty)
{
	ClusterWalStateSlot s;

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 7, -1, NULL),
				 (int)CLUSTER_WAL_SLOT_EMPTY);
}

UT_TEST(test_slot_corruption_rejected)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.last_updated ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad crc"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.magic = 0xDEADBEEF;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad magic"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.version = 9;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "bad version"), 0);
}

UT_TEST(test_slot_self_description_mismatch)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	/* slot says thread 5 but was read from slot 4: mis-addressed write */
	cluster_wal_state_slot_fill(&s, 5, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "slot self-description mismatch"), 0);
}

UT_TEST(test_slot_invalid_state_rejected)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, 0, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "invalid state"), 0);

	cluster_wal_state_slot_fill(&s, 4, 3, 3, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
	UT_ASSERT_EQ(strcmp(reason, "invalid state"), 0);
}

UT_TEST(test_slot_foreign_identity)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 9, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_FOREIGN);
	UT_ASSERT_EQ(strcmp(reason, "node_id mismatch"), 0);
}

UT_TEST(test_slot_reader_mode_any_node)
{
	ClusterWalStateSlot s;

	cluster_wal_state_slot_fill(&s, 4, 9, CLUSTER_WAL_SLOT_STATE_STOPPED, 1, 100, 200, 1, 2);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, NULL), (int)CLUSTER_WAL_SLOT_OK);
}

UT_TEST(test_state_enum_on_disk_values)
{
	UT_ASSERT_EQ((int)CLUSTER_WAL_SLOT_STATE_ACTIVE, 1);
	UT_ASSERT_EQ((int)CLUSTER_WAL_SLOT_STATE_STOPPED, 2);
}

/*
 * EMPTY demands the full 512B be zero (spec-4.2 round-2 P1): zeroed
 * magic/version/state/crc glued to body garbage is CORRUPT, not EMPTY.
 */
UT_TEST(test_slot_zero_fields_nonzero_body_is_corrupt)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	memset(&s, 0, sizeof(s));
	s.highest_lsn = 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	memset(&s, 0, sizeof(s));
	s._reserved[100] = 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	memset(&s, 0, sizeof(s));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, -1, &reason),
				 (int)CLUSTER_WAL_SLOT_EMPTY);
}

UT_TEST(test_crc_covers_watermark_fields)
{
	ClusterWalStateSlot s;
	const char *reason = NULL;

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.tli ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.highest_lsn ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);

	cluster_wal_state_slot_fill(&s, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1, 100, 200, 1, 2);
	s.highest_scn ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&s, 4, 3, &reason),
				 (int)CLUSTER_WAL_SLOT_CORRUPT);
}


/* ---- A1 common fresh-image/mask/post-read contract (RF REV1 R2/R6) ---- */

static void
fill_a1_active_slot(ClusterWalStateSlot *slot)
{
	cluster_wal_state_slot_fill(slot, 4, 3, CLUSTER_WAL_SLOT_STATE_ACTIVE, 7, 100, 200, 300, 400);
	slot->checkpoint_redo_lsn = 500;
	slot->refresh_interval_ms = 600;
	slot->fpw_was_off = 1;
	slot->merge_recovered_lsn = 700;
	slot->_reserved[17] = 0x5a;
	slot->crc = cluster_wal_state_block_crc(slot);
}

UT_TEST(test_a1_w2_active_exact_mask_preserves_extensions_and_clears_w6)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot after;
	ClusterWalStateUpdate update;
	ClusterWalStateUpdateResult result;

	fill_a1_active_slot(&before);
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_ACTIVE;
	update.tli = 8;
	update.started_at = 1000;
	update.last_updated = 1100;
	update.highest_lsn = 1200;
	update.highest_scn = 1300;
	update.refresh_interval_ms = 1400;

	result = cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &after);
	UT_ASSERT_EQ((int)result, (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ((int)after.thread_id, 4);
	UT_ASSERT_EQ((int)after.node_id, 3);
	UT_ASSERT_EQ((int)after.state, (int)CLUSTER_WAL_SLOT_STATE_ACTIVE);
	UT_ASSERT_EQ((int)after.tli, 8);
	UT_ASSERT_EQ(after.started_at, 1000);
	UT_ASSERT_EQ(after.last_updated, 1100);
	UT_ASSERT_EQ(after.highest_lsn, 1200);
	UT_ASSERT_EQ(after.highest_scn, 1300);
	UT_ASSERT_EQ(after.refresh_interval_ms, 1400);
	UT_ASSERT_EQ(after.merge_recovered_lsn, 0);
	UT_ASSERT_EQ(after.checkpoint_redo_lsn, before.checkpoint_redo_lsn);
	UT_ASSERT_EQ(after.fpw_was_off, before.fpw_was_off);
	UT_ASSERT_EQ(after._reserved[17], before._reserved[17]);
	UT_ASSERT_EQ(after.crc, cluster_wal_state_block_crc(&after));
}

UT_TEST(test_a1_w2_empty_initializes_exact_active_slot)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot after;
	ClusterWalStateUpdate update;

	memset(&before, 0, sizeof(before));
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_ACTIVE;
	update.tli = 8;
	update.started_at = 1000;
	update.last_updated = 1100;
	update.highest_lsn = 1200;
	update.highest_scn = 1300;
	update.refresh_interval_ms = 1400;

	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &after),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_classify(&after, 4, 3, NULL),
				 (int)CLUSTER_WAL_SLOT_OK);
	UT_ASSERT_EQ((int)after.state, (int)CLUSTER_WAL_SLOT_STATE_ACTIVE);
	UT_ASSERT_EQ(after.checkpoint_redo_lsn, 0);
	UT_ASSERT_EQ(after.fpw_was_off, 0);
	UT_ASSERT_EQ(after.merge_recovered_lsn, 0);
	UT_ASSERT_EQ(after._reserved[17], 0);
}

UT_TEST(test_a1_w4_telemetry_exact_mask)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot after;
	ClusterWalStateUpdate update;

	fill_a1_active_slot(&before);
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_TELEMETRY;
	update.tli = 9;
	update.last_updated = 2100;
	update.highest_lsn = 2200;
	update.highest_scn = 2300;
	update.refresh_interval_ms = 2400;

	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &after),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ((int)after.tli, 9);
	UT_ASSERT_EQ(after.last_updated, 2100);
	UT_ASSERT_EQ(after.highest_lsn, 2200);
	UT_ASSERT_EQ(after.highest_scn, 2300);
	UT_ASSERT_EQ(after.refresh_interval_ms, 2400);
	UT_ASSERT_EQ(after.state, before.state);
	UT_ASSERT_EQ(after.started_at, before.started_at);
	UT_ASSERT_EQ(after.checkpoint_redo_lsn, before.checkpoint_redo_lsn);
	UT_ASSERT_EQ(after.fpw_was_off, before.fpw_was_off);
	UT_ASSERT_EQ(after.merge_recovered_lsn, before.merge_recovered_lsn);
	UT_ASSERT_EQ(after._reserved[17], before._reserved[17]);
}

UT_TEST(test_a1_w5_checkpoint_and_fpw_exact_masks)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot checkpoint;
	ClusterWalStateSlot sticky;
	ClusterWalStateUpdate update;

	fill_a1_active_slot(&before);
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_CHECKPOINT;
	update.checkpoint_redo_lsn = 9000;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &checkpoint),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ(checkpoint.checkpoint_redo_lsn, 9000);
	UT_ASSERT_EQ(checkpoint.fpw_was_off, before.fpw_was_off);
	UT_ASSERT_EQ(checkpoint.merge_recovered_lsn, before.merge_recovered_lsn);

	before.fpw_was_off = 0;
	before.crc = cluster_wal_state_block_crc(&before);
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_FPW_STICKY;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &sticky),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ(sticky.fpw_was_off, 1);
	UT_ASSERT_EQ(sticky.checkpoint_redo_lsn, before.checkpoint_redo_lsn);
	UT_ASSERT_EQ(sticky.merge_recovered_lsn, before.merge_recovered_lsn);
	UT_ASSERT_EQ(sticky._reserved[17], before._reserved[17]);
}

UT_TEST(test_a1_w3_stopped_exact_mask_and_idempotence)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot stopped;
	ClusterWalStateUpdate update;

	fill_a1_active_slot(&before);
	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_STOPPED;
	update.tli = 10;
	update.last_updated = 3100;
	update.highest_lsn = 3200;
	update.highest_scn = 3300;

	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &stopped),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	UT_ASSERT_EQ((int)stopped.state, (int)CLUSTER_WAL_SLOT_STATE_STOPPED);
	UT_ASSERT_EQ((int)stopped.tli, 10);
	UT_ASSERT_EQ(stopped.last_updated, 3100);
	UT_ASSERT_EQ(stopped.highest_lsn, 3200);
	UT_ASSERT_EQ(stopped.highest_scn, 3300);
	UT_ASSERT_EQ(stopped.started_at, before.started_at);
	UT_ASSERT_EQ(stopped.checkpoint_redo_lsn, before.checkpoint_redo_lsn);
	UT_ASSERT_EQ(stopped.fpw_was_off, before.fpw_was_off);
	UT_ASSERT_EQ(stopped.merge_recovered_lsn, before.merge_recovered_lsn);
	UT_ASSERT_EQ(stopped._reserved[17], before._reserved[17]);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&stopped, 4, 3, &update, &before),
				 (int)CLUSTER_WAL_STATE_UPDATE_NOOP);
}

UT_TEST(test_a1_update_typed_rejection_and_postread_mismatch)
{
	ClusterWalStateSlot before;
	ClusterWalStateSlot expected;
	ClusterWalStateSlot observed;
	ClusterWalStateUpdate update;

	memset(&update, 0, sizeof(update));
	update.kind = CLUSTER_WAL_STATE_UPDATE_TELEMETRY;
	memset(&before, 0, sizeof(before));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &expected),
				 (int)CLUSTER_WAL_STATE_UPDATE_EMPTY);

	fill_a1_active_slot(&before);
	before.crc ^= 1;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &expected),
				 (int)CLUSTER_WAL_STATE_UPDATE_CORRUPT);

	fill_a1_active_slot(&before);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 9, &update, &expected),
				 (int)CLUSTER_WAL_STATE_UPDATE_FOREIGN);

	fill_a1_active_slot(&before);
	before.state = CLUSTER_WAL_SLOT_STATE_STOPPED;
	before.crc = cluster_wal_state_block_crc(&before);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &expected),
				 (int)CLUSTER_WAL_STATE_UPDATE_WRONG_STATE);

	fill_a1_active_slot(&before);
	update.tli = 11;
	UT_ASSERT_EQ((int)cluster_wal_state_slot_prepare_update(&before, 4, 3, &update, &expected),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	memcpy(&observed, &expected, sizeof(observed));
	UT_ASSERT_EQ((int)cluster_wal_state_slot_verify_postread(&expected, &observed, 4, 3),
				 (int)CLUSTER_WAL_STATE_UPDATE_OK);
	observed._reserved[17] ^= 1;
	observed.crc = cluster_wal_state_block_crc(&observed);
	UT_ASSERT_EQ((int)cluster_wal_state_slot_verify_postread(&expected, &observed, 4, 3),
				 (int)CLUSTER_WAL_STATE_UPDATE_POSTREAD_MISMATCH);
}


/* RF-ROOT P9 verification (implementation): source-close writer gate stubs — the
 * unit harness never freezes the source. */
bool
cluster_r4_bit22_source_writer_enter(void)
{
	return true;
}

void
cluster_r4_bit22_source_writer_leave(void)
{}

bool
cluster_r4_bit22_source_close_begin(uint64 transition_epoch pg_attribute_unused(),
									uint64 prepare_generation pg_attribute_unused())
{
	return true;
}

bool
cluster_r4_bit22_source_close_current(uint64 transition_epoch pg_attribute_unused(),
									  uint64 prepare_generation pg_attribute_unused())
{
	return false;
}


int
main(int argc, char **argv)
{
	UT_PLAN(21);


	UT_RUN(test_header_layout_locks);
	UT_RUN(test_slot_layout_locks);
	UT_RUN(test_slot_offset_macro_locks);
	UT_RUN(test_header_roundtrip);
	UT_RUN(test_header_corruption_rejected);
	UT_RUN(test_slot_roundtrip_owner_ok);
	UT_RUN(test_slot_empty);
	UT_RUN(test_slot_corruption_rejected);
	UT_RUN(test_slot_self_description_mismatch);
	UT_RUN(test_slot_invalid_state_rejected);
	UT_RUN(test_slot_foreign_identity);
	UT_RUN(test_slot_reader_mode_any_node);
	UT_RUN(test_state_enum_on_disk_values);
	UT_RUN(test_slot_zero_fields_nonzero_body_is_corrupt);
	UT_RUN(test_crc_covers_watermark_fields);
	UT_RUN(test_a1_w2_active_exact_mask_preserves_extensions_and_clears_w6);
	UT_RUN(test_a1_w2_empty_initializes_exact_active_slot);
	UT_RUN(test_a1_w4_telemetry_exact_mask);
	UT_RUN(test_a1_w5_checkpoint_and_fpw_exact_masks);
	UT_RUN(test_a1_w3_stopped_exact_mask_and_idempotence);
	UT_RUN(test_a1_update_typed_rejection_and_postread_mismatch);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
