/* PGRAC: actual bounded relmap retention state core, no authority mocks.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres.h"

#include "cluster/cluster_relmap_retention.h"
#include "cluster/cluster_tt_local.h"
#include "cluster/cluster_tt_slot.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();

#define BASE UINT64CONST(0x200000000)
#define HALF UINT64CONST(0x80000000)
#define OK CLUSTER_RELMAP_RETENTION_OK

static ClusterRelmapRetentionKey
key_for(Oid dbid, uint64 full)
{
	ClusterRelmapRetentionKey key = { 0 };

	key.identity.system_identifier = 1234;
	key.identity.database_incarnation = 5678;
	key.identity.dbid = dbid;
	key.identity.shared = dbid == InvalidOid;
	key.generation = 8;
	key.origin = 3;
	key.top_full_xid = full;
	key.membership_epoch = 91;
	key.admitted_incarnation = 101;
	key.boot_incarnation = 202;
	return key;
}

static void
building(ClusterRelmapRetentionDirectory *d)
{
	memset(d, 0, sizeof(*d));
	UT_ASSERT_EQ(cluster_relmap_retention_build_start(d, 1234, 5678, 3, 7, BASE + 10,
													   BASE + 1000), OK);
}

static void
ready(ClusterRelmapRetentionDirectory *d)
{
	building(d);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(d), OK);
}

static bool
has_pin(const ClusterRelmapRetentionDirectory *d, Oid dbid, uint64 full)
{
	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
		if (d->pins[i].occupied && d->pins[i].key.identity.dbid == dbid
			&& d->pins[i].key.top_full_xid == full)
			return true;
	return false;
}

UT_TEST(test_unknown_and_building_cannot_authorize_consumers)
{
	ClusterRelmapRetentionDirectory d = { 0 };
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionTruncate cut;

	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&d, BASE + 1001), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 200, &cut), OK);
	building(&d);
	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&d, BASE + 1001), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 200, &cut), OK);
}

UT_TEST(test_complete_empty_census_allows_normal_progress)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + 1001), OK);
	UT_ASSERT_EQ(d.last_issued, BASE + 1001);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 200, &cut), OK);
	UT_ASSERT_EQ(cut.cutoff, BASE + 200);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
	UT_ASSERT_EQ(d.native_floor, BASE + 200);
}

UT_TEST(test_begin_first_clamps_native_truncate_to_pin)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 300, &cut), OK);
	UT_ASSERT_EQ(cut.cutoff, BASE + 100);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
	UT_ASSERT(has_pin(&d, 42, BASE + 100));
}

UT_TEST(test_truncate_first_refuses_late_pin_below_inflight_cut)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey late = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey safe = key_for(43, BASE + 300);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 300, &cut), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &late), CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
	UT_ASSERT(!has_pin(&d, 42, BASE + 100));
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &safe), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &late), CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
}

UT_TEST(test_uncertain_truncate_retains_reservation_and_blocks_consumers)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 300);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 300, &cut), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_uncertain(&d, &cut), OK);
	UT_ASSERT_EQ(d.state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	UT_ASSERT_EQ(d.inflight.serial, cut.serial);
	UT_ASSERT(has_pin(&d, 42, BASE + 300));
	UT_ASSERT_EQ(d.native_floor, BASE + 10);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_observe(&d, BASE + 1001), OK);
	UT_ASSERT_NE(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
}

UT_TEST(test_stale_truncate_tokens_cannot_clear_new_reservation)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionTruncate first, next, wrong;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 100, &first), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 200, &next),
				 CLUSTER_RELMAP_RETENTION_BUSY);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &first), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 200, &next), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_complete(&d, &first), OK);
	wrong = next;
	wrong.generation++;
	UT_ASSERT_NE(cluster_relmap_retention_truncate_complete(&d, &wrong), OK);
	wrong = next;
	wrong.cutoff++;
	UT_ASSERT_NE(cluster_relmap_retention_truncate_uncertain(&d, &wrong), OK);
	UT_ASSERT_EQ(d.inflight.serial, next.serial);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &next), OK);
}

UT_TEST(test_half_space_equality_rejected_without_observing_candidate)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&d, BASE + HALF + 99), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + HALF + 100),
				 CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	UT_ASSERT_EQ(d.last_issued, BASE + 1000);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + HALF + 99), OK);
}

UT_TEST(test_recomputed_striped_candidate_cannot_use_prior_check)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&d, BASE + 1024), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + HALF + 128),
				 CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	UT_ASSERT_EQ(d.last_issued, BASE + 1000);
}

UT_TEST(test_allocation_first_rejects_pin_that_arrives_too_late)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + HALF + 100), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	UT_ASSERT(!has_pin(&d, 42, BASE + 100));
}

UT_TEST(test_new_pin_between_check_and_observe_is_rechecked)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&d, BASE + HALF + 100), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_observe(&d, BASE + HALF + 100),
				 CLUSTER_RELMAP_RETENTION_XID_WINDOW);
}

UT_TEST(test_begin_rejects_missing_native_or_unissued_or_other_epoch_identity)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 9);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
	key.top_full_xid = BASE + 1001;
	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &key), OK);
	key.top_full_xid = UINT64CONST(0x300000064); /* Same raw 100, different FullXid. */
	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &key), OK);
	key.top_full_xid = BASE + 1; /* Permanent raw XID. */
	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &key), OK);
}

UT_TEST(test_exact_owner_identity_required_to_cancel_or_start_stage)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100), wrong;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	for (int field = 0; field < 8; field++) {
		wrong = key;
		switch (field) {
		case 0: wrong.identity.system_identifier++; break;
		case 1: wrong.identity.database_incarnation++; break;
		case 2: wrong.origin++; break;
		case 3: wrong.top_full_xid += UINT64CONST(0x100000000); break;
		case 4: wrong.membership_epoch++; break;
		case 5: wrong.admitted_incarnation++; break;
		case 6: wrong.boot_incarnation++; break;
		case 7: wrong.generation++; break;
		}
		UT_ASSERT_NE(cluster_relmap_retention_cancel_prepare(&d, &wrong), OK);
		UT_ASSERT_NE(cluster_relmap_retention_stage_start(&d, &wrong, 0x1234), OK);
		UT_ASSERT(has_pin(&d, 42, BASE + 100));
	}
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	UT_ASSERT(!has_pin(&d, 42, BASE + 100));
}

UT_TEST(test_stage_attempt_is_retained_even_before_publisher_is_armed)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_NE(cluster_relmap_retention_stage_start(&d, &key, 0), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_NE(cluster_relmap_retention_stage_start(&d, &key, 0x2345), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 500, &cut), OK);
	UT_ASSERT_EQ(cut.cutoff, BASE + 100);
}

UT_TEST(test_capacity_refusal_preserves_existing_pins_and_cancel_restores_capacity)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key;

	ready(&d);
	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
		key = key_for(100 + i, BASE + 100 + i);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	}
	key = key_for(999, BASE + 200);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED);
	UT_ASSERT(!has_pin(&d, 999, BASE + 200));
	key = key_for(100, BASE + 100);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	key = key_for(999, BASE + 200);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT(has_pin(&d, 101, BASE + 101));
}

UT_TEST(test_namespace_conflict_does_not_replace_existing_owner)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100), other;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	other = key;
	other.generation++;
	other.top_full_xid++;
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &other), CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT(has_pin(&d, 42, BASE + 100));
	other = key_for(InvalidOid, BASE + 110);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &other), OK);
	other.identity.shared = false;
	UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &other), OK);
}

UT_TEST(test_restored_pending_obligation_remains_retained)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionTruncate cut;

	building(&d);
	/* Fixture supplies a qualified pending obligation. Actual pair reading,
	 * namespace enumeration and Startup authorization are separate callers. */
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 500, &cut), OK);
	UT_ASSERT_EQ(cut.cutoff, BASE + 100);
}

UT_TEST(test_failed_restore_cannot_be_ignored_to_publish_empty_directory)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	building(&d);
	UT_ASSERT_NE(cluster_relmap_retention_build_add(&d, &key, 0), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
	building(&d);
	key.origin = 4;
	UT_ASSERT_NE(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&d, BASE + 1001), OK);
}

UT_TEST(test_rebuild_capacity_failure_retains_pins_and_invalidates_completeness)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key;

	building(&d);
	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
		key = key_for(100 + i, BASE + 100 + i);
		UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	}
	key = key_for(999, BASE + 200);
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED);
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT(has_pin(&d, 100, BASE + 100));
}

UT_TEST(test_rebuild_conflicting_wal_end_invalidates_without_replacing_pin)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	building(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_add(&d, &key, 0x2345), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT_EQ(d.pins[0].wal_end, 0x1234);
}

UT_TEST(test_invalidation_cannot_reset_existing_pins_in_place)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	cluster_relmap_retention_invalidate(&d);
	UT_ASSERT_NE(cluster_relmap_retention_build_start(&d, 1234, 5678, 3, 8, BASE + 10,
													   BASE + 1000), OK);
	UT_ASSERT(has_pin(&d, 42, BASE + 100));
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
}

UT_TEST(test_overflow_reserved_raw_xid_and_backward_issue_refuse)
{
	ClusterRelmapRetentionDirectory d = { 0 };
	ClusterRelmapRetentionKey key = key_for(42, PG_UINT64_MAX - 100);

	UT_ASSERT_EQ(cluster_relmap_retention_build_start(&d, 1234, 5678, 3, 7,
													   PG_UINT64_MAX - 1000, PG_UINT64_MAX - 10), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), CLUSTER_RELMAP_RETENTION_XID_WINDOW);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_observe(&d, PG_UINT64_MAX), OK);
	ready(&d);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_observe(&d, BASE + 999), OK);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_observe(&d, BASE + 1000), OK);
	UT_ASSERT_NE(cluster_relmap_retention_allocation_observe(&d, UINT64CONST(0x300000001)), OK);
	UT_ASSERT_EQ(d.last_issued, BASE + 1000);
}

UT_TEST(test_multiple_namespace_pins_keep_the_earliest_full_xid)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey a = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey b = key_for(43, BASE + 200);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &b), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &a), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &a), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 500, &cut), OK);
	UT_ASSERT_EQ(cut.cutoff, BASE + 200);
	UT_ASSERT_EQ(cluster_relmap_retention_allocation_check(&d, BASE + HALF + 150), OK);
}

UT_TEST(test_output_alias_cannot_erase_live_truncate_reservation)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey late = key_for(42, BASE + 100);
	ClusterRelmapRetentionTruncate cut;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_begin(&d, BASE + 300, &cut), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 400, &d.inflight), OK);
	UT_ASSERT_EQ(d.inflight.serial, cut.serial);
	UT_ASSERT_EQ(d.inflight.cutoff, cut.cutoff);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &late), CLUSTER_RELMAP_RETENTION_NATIVE_GONE);
	UT_ASSERT_EQ(cluster_relmap_retention_truncate_complete(&d, &cut), OK);
}

UT_TEST(test_invalid_build_identity_key_and_cut_cannot_grant_permission)
{
	ClusterRelmapRetentionDirectory d = { 0 };
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100), wrong;
	ClusterRelmapRetentionTruncate cut;

	UT_ASSERT_NE(cluster_relmap_retention_build_start(&d, 0, 5678, 3, 7, BASE + 10,
													   BASE + 1000), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_start(&d, 1234, 5678, 128, 7, BASE + 10,
													   BASE + 1000), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_start(&d, 1234, 5678, 3, 7, BASE + 1001,
													   BASE + 1000), OK);
	UT_ASSERT_NE(cluster_relmap_retention_build_finish(&d), OK);
	ready(&d);
	for (int field = 0; field < 3; field++) {
		wrong = key;
		switch (field) {
		case 0: wrong.admitted_incarnation = 0; break;
		case 1: wrong.boot_incarnation = 0; break;
		case 2: wrong.generation = 1; break;
		}
		UT_ASSERT_NE(cluster_relmap_retention_begin(&d, &wrong), OK);
	}
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 9, &cut), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 1002, &cut), OK);
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 1, &cut), OK);
	d.truncate_serial = PG_UINT64_MAX;
	UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&d, BASE + 100, &cut), OK);
	UT_ASSERT_EQ(d.inflight.serial, 0);
	UT_ASSERT_EQ(d.native_floor, BASE + 10);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
}

UT_TEST(test_epoch_zero_is_exact_identity_not_missing_admission)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100), different;

	ready(&d);
	key.membership_epoch = 0;
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	different = key;
	different.membership_epoch = 1;
	UT_ASSERT_NE(cluster_relmap_retention_stage_start(&d, &different, 900), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 900), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
}

/* Controlled samples of the original owner's published receipt, not a TT
 * publisher, native/SIDE qualification or runtime-lock fixture. */
static ClusterCanonicalTxnBinding
binding_for(uint64 full)
{
	ClusterCanonicalTxnBinding binding = { 0 };

	binding.segment_id = 17;
	binding.segment_generation = 0; /* Zero is a legal original generation. */
	binding.xid = (TransactionId) full;
	binding.slot_offset = TT_SLOTS_PER_SEGMENT - 1;
	binding.slot_wrap = 0;
	binding.origin_instance = 4; /* Directory origins are zero-based. */
	binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHED;
	binding.active_lsn = 0x4567;
	return binding;
}

static int
bound_pins(const ClusterRelmapRetentionDirectory *d, uint64 full)
{
	int count = 0;

	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++)
		if (d->pins[i].occupied && d->pins[i].key.top_full_xid == full
			&& d->pins[i].has_tt_binding)
			count++;
	return count;
}

UT_TEST(test_tt_guards_require_complete_directory)
{
	ClusterRelmapRetentionDirectory d = { 0 };
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(NULL, &key, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(NULL, key.top_full_xid, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(NULL, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	for (int state = 0; state < 2; state++) {
		if (state == 1)
			building(&d);
		UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding),
					 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding),
					 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
					 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
		UT_ASSERT(!has_pin(&d, 42, key.top_full_xid));
	}
}

UT_TEST(test_published_binding_precedes_first_relmap_pin)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	ready(&d);
	memcpy(&before, &d, sizeof(d));
	/* The original ACTIVE hook adds no obligation when no relmap pin exists. */
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	UT_ASSERT_EQ(bound_pins(&d, key.top_full_xid), 1);
	UT_ASSERT_EQ(d.pins[0].binding.active_lsn, binding.active_lsn);
	UT_ASSERT_EQ(d.pins[0].binding.segment_generation, 0);
	UT_ASSERT_EQ(d.pins[0].binding.slot_wrap, 0);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
}

UT_TEST(test_later_active_enrollment_preserves_staged_obligation)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(bound_pins(&d, key.top_full_xid), 0);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT_EQ(bound_pins(&d, key.top_full_xid), 1);
	UT_ASSERT(d.pins[0].may_be_staged);
	UT_ASSERT_EQ(d.pins[0].wal_end, 0x1234);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
	memcpy(&before, &d, sizeof(d));
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
}

UT_TEST(test_late_binding_covers_all_namespaces_and_future_begin)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey keys[4] = { key_for(InvalidOid, BASE + 100),
		key_for(42, BASE + 100), key_for(43, BASE + 100), key_for(44, BASE + 100) };
	ClusterCanonicalTxnBinding binding = binding_for(BASE + 100);

	ready(&d);
	for (int i = 0; i < 3; i++)
		UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &keys[i]), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, BASE + 100, &binding), OK);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 100), 3);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &keys[3]), OK);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 100), 4);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &keys[0]), OK);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 100), 3);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
}

UT_TEST(test_bound_begin_enrolls_existing_same_full_xid_namespaces)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey first = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey next = key_for(43, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(first.top_full_xid);
	ClusterCanonicalTxnBinding unrelated = binding_for(BASE + 200);
	ClusterRelmapRetentionKey other = key_for(44, BASE + 200);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &first), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &first, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &other), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &next, &binding), OK);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 100), 2);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 200), 0);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &unrelated),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
}

UT_TEST(test_same_raw_other_full_xid_cannot_supply_missing_binding)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	memcpy(&before, &d, sizeof(d));
	/* A no-pin hook is a no-op, not reconstruction of this pin's era. */
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d,
		key.top_full_xid + UINT64CONST(0x100000000), &binding), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_NE(cluster_relmap_retention_bind(&d, BASE + 101, &binding), OK);
	UT_ASSERT_NE(cluster_relmap_retention_bind(&d, BASE + 2, &binding), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
}

UT_TEST(test_invalid_tt_receipts_cannot_create_or_release_pins)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey next = key_for(43, BASE + 100);
	ClusterCanonicalTxnBinding binding;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	memcpy(&before, &d, sizeof(d));
	UT_ASSERT_NE(cluster_relmap_retention_bind(&d, key.top_full_xid, NULL), OK);
	UT_ASSERT_NE(cluster_relmap_retention_tt_release_check(&d, NULL), OK);
	for (int field = 0; field < 13; field++) {
		binding = binding_for(key.top_full_xid);
		switch (field) {
		case 0: binding.publish_state = CLUSTER_CANONICAL_TXN_NONE; break;
		case 1: binding.publish_state = CLUSTER_CANONICAL_TXN_RESERVED; break;
		case 2: binding.publish_state = CLUSTER_CANONICAL_TXN_PUBLISHING; break;
		case 3: binding.publish_state = CLUSTER_CANONICAL_TXN_FAILED; break;
		case 4: binding.reserved16 = 1; break;
		case 5: binding.segment_id = 0; break;
		case 6: binding.segment_generation = UINT32_MAX; break;
		case 7: binding.slot_offset = TT_SLOTS_PER_SEGMENT; break;
		case 8: binding.slot_wrap = TT_WRAP_INVALID; break;
		case 9: binding.origin_instance = 0; break;
		case 10: binding.origin_instance++; break;
		case 11: binding.xid = FrozenTransactionId; break;
		case 12: binding.active_lsn = InvalidXLogRecPtr; break;
		}
		UT_ASSERT_NE(cluster_relmap_retention_begin_bound(&d, &next, &binding), OK);
		UT_ASSERT_NE(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
		UT_ASSERT_NE(cluster_relmap_retention_tt_release_check(&d, &binding), OK);
		UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	}
}

UT_TEST(test_conflicting_binding_refuses_atomically_including_active_lsn)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey next = key_for(43, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid), other;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	memcpy(&before, &d, sizeof(d));
	for (int field = 0; field < 5; field++) {
		other = binding;
		switch (field) {
		case 0: other.segment_id++; break;
		case 1: other.segment_generation++; break;
		case 2: other.slot_offset--; break;
		case 3: other.slot_wrap++; break;
		case 4: other.active_lsn++; break;
		}
		UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &next, &other),
					 CLUSTER_RELMAP_RETENTION_CONFLICT);
		UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &other),
					 CLUSTER_RELMAP_RETENTION_CONFLICT);
		UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	}
}

UT_TEST(test_bound_begin_refusal_cannot_partially_enroll_existing_pin)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey conflict = key_for(42, BASE + 200);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);
	ClusterCanonicalTxnBinding other = binding_for(conflict.top_full_xid);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 0x1234), OK);
	memcpy(&before, &d, sizeof(d));
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &conflict, &other),
				 CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
}

UT_TEST(test_tt_release_matches_exact_physical_key_not_active_lsn)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid), other;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	other = binding;
	other.active_lsn++;
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &other),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
	for (int field = 0; field < 5; field++) {
		other = binding;
		switch (field) {
		case 0: other.segment_id++; break;
		case 1: other.segment_generation++; break;
		case 2: other.slot_offset--; break;
		case 3: other.slot_wrap++; break;
		case 4: other.xid++; break;
		}
		/* Only this relmap conjunct is satisfied; original TT gates remain. */
		UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &other), OK);
	}
}

UT_TEST(test_cancel_clears_only_unstaged_relmap_binding_edge)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding), OK);
	/* No TT owner is called here: its published ACTIVE/BIND is untouched. */
	UT_ASSERT_EQ(binding.publish_state, CLUSTER_CANONICAL_TXN_PUBLISHED);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
	cluster_relmap_retention_invalidate(&d);
	UT_ASSERT_EQ(bound_pins(&d, key.top_full_xid), 1);
	UT_ASSERT_EQ(d.pins[0].binding.active_lsn, binding.active_lsn);
	UT_ASSERT(d.pins[0].may_be_staged);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_NE(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT_NE(cluster_relmap_retention_cancel_prepare(&d, &key), OK);
}

UT_TEST(test_restored_pending_needs_real_tt_enrollment)
{
	ClusterRelmapRetentionDirectory d;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);

	building(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_build_add(&d, &key, 0x1234), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_build_finish(&d), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, key.top_full_xid, &binding), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(cluster_relmap_retention_cancel_prepare(&d, &key), CLUSTER_RELMAP_RETENTION_RETAINED);
}

UT_TEST(test_capacity_failure_cannot_partially_enroll_published_binding)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key;
	ClusterCanonicalTxnBinding binding = binding_for(BASE + 100);

	ready(&d);
	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
		key = key_for(100 + i, BASE + 100);
		UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	}
	memcpy(&before, &d, sizeof(d));
	key = key_for(999, BASE + 100);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding),
				 CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	UT_ASSERT_EQ(cluster_relmap_retention_bind(&d, BASE + 100, &binding), OK);
	UT_ASSERT_EQ(bound_pins(&d, BASE + 100), CLUSTER_RELMAP_RETENTION_CAPACITY);
	UT_ASSERT_EQ(cluster_relmap_retention_tt_release_check(&d, &binding),
				 CLUSTER_RELMAP_RETENTION_RETAINED);
}

UT_TEST(test_prepare_snapshot_copies_actual_pin_and_allows_exact_stage_only)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
	ClusterRelmapRetentionKey other = key_for(43, BASE + 200);
	ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);
	ClusterRelmapRetentionSnapshot snapshot;

	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &other), OK);
	before = d;
	UT_ASSERT_EQ(cluster_relmap_retention_prepare_capture(&d, &key, &snapshot), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	UT_ASSERT(snapshot.pin.occupied && snapshot.pin.has_tt_binding);
	UT_ASSERT_EQ(snapshot.pin.binding.active_lsn, binding.active_lsn);
	UT_ASSERT_EQ(snapshot.generation, d.generation);
	UT_ASSERT_EQ(snapshot.native_floor, d.native_floor);
	UT_ASSERT_EQ(snapshot.last_issued, d.last_issued);
	UT_ASSERT_EQ(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, false, 0), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &other, &snapshot, false, 0), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, true, 0x888), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &other, 0x777), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, false, 0), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&d, &key, 0x888), OK);
	UT_ASSERT_EQ(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, true, 0x888), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, true, 0x889), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, false, 0), OK);
	UT_ASSERT(!snapshot.pin.may_be_staged && snapshot.pin.wal_end == 0);
}

UT_TEST(test_prepare_snapshot_refuses_every_original_binding_field_change)
{
	for (unsigned fault = 0; fault < 10; ++fault) {
		ClusterRelmapRetentionDirectory d, before;
		ClusterRelmapRetentionKey key = key_for(42, BASE + 100);
		ClusterCanonicalTxnBinding binding = binding_for(key.top_full_xid);
		ClusterRelmapRetentionSnapshot snapshot;
		ready(&d);
		UT_ASSERT_EQ(cluster_relmap_retention_begin_bound(&d, &key, &binding), OK);
		UT_ASSERT_EQ(cluster_relmap_retention_prepare_capture(&d, &key, &snapshot), OK);
		switch (fault) {
		case 0: ++d.pins[0].binding.segment_id; break;
		case 1: ++d.pins[0].binding.segment_generation; break;
		case 2: ++d.pins[0].binding.xid; break;
		case 3: --d.pins[0].binding.slot_offset; break;
		case 4: ++d.pins[0].binding.slot_wrap; break;
		case 5: ++d.pins[0].binding.origin_instance; break;
		case 6: ++d.pins[0].binding.active_lsn; break;
		case 7: d.pins[0].binding.publish_state = 0; break;
		case 8: ++d.pins[0].binding.reserved16; break;
		case 9: d.pins[0].has_tt_binding = false; break;
		}
		before = d;
		UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, &snapshot, false, 0), OK);
		UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	}
}

UT_TEST(test_prepare_snapshot_alias_refusal_preserves_original_storage)
{
	ClusterRelmapRetentionDirectory d, before;
	ClusterRelmapRetentionKey key = key_for(42, BASE + 100), key_before;
	ClusterRelmapRetentionSnapshot snapshot, zero = {0};
	ready(&d);
	UT_ASSERT_EQ(cluster_relmap_retention_begin(&d, &key), OK);
	before = d;
	key_before = key;
	UT_ASSERT_NE(cluster_relmap_retention_prepare_capture(&d, &key,
		(ClusterRelmapRetentionSnapshot *)&d.pins[0]), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_capture(&d, &key,
		(ClusterRelmapRetentionSnapshot *)&key), OK);
	UT_ASSERT(memcmp(&d, &before, sizeof(d)) == 0);
	UT_ASSERT(memcmp(&key, &key_before, sizeof(key)) == 0);
	memset(&snapshot, 0xa5, sizeof(snapshot));
	UT_ASSERT_NE(cluster_relmap_retention_prepare_capture(NULL, &key, &snapshot), OK);
	UT_ASSERT(memcmp(&snapshot, &zero, sizeof(snapshot)) == 0);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, NULL, false, 0), OK);
	UT_ASSERT_NE(cluster_relmap_retention_prepare_matches(&d, &key, &zero, false, 0), OK);
}

int
main(void)
{
	UT_PLAN(41);
	UT_RUN(test_unknown_and_building_cannot_authorize_consumers);
	UT_RUN(test_complete_empty_census_allows_normal_progress);
	UT_RUN(test_begin_first_clamps_native_truncate_to_pin);
	UT_RUN(test_truncate_first_refuses_late_pin_below_inflight_cut);
	UT_RUN(test_uncertain_truncate_retains_reservation_and_blocks_consumers);
	UT_RUN(test_stale_truncate_tokens_cannot_clear_new_reservation);
	UT_RUN(test_half_space_equality_rejected_without_observing_candidate);
	UT_RUN(test_recomputed_striped_candidate_cannot_use_prior_check);
	UT_RUN(test_allocation_first_rejects_pin_that_arrives_too_late);
	UT_RUN(test_new_pin_between_check_and_observe_is_rechecked);
	UT_RUN(test_begin_rejects_missing_native_or_unissued_or_other_epoch_identity);
	UT_RUN(test_exact_owner_identity_required_to_cancel_or_start_stage);
	UT_RUN(test_stage_attempt_is_retained_even_before_publisher_is_armed);
	UT_RUN(test_capacity_refusal_preserves_existing_pins_and_cancel_restores_capacity);
	UT_RUN(test_namespace_conflict_does_not_replace_existing_owner);
	UT_RUN(test_restored_pending_obligation_remains_retained);
	UT_RUN(test_failed_restore_cannot_be_ignored_to_publish_empty_directory);
	UT_RUN(test_rebuild_capacity_failure_retains_pins_and_invalidates_completeness);
	UT_RUN(test_rebuild_conflicting_wal_end_invalidates_without_replacing_pin);
	UT_RUN(test_invalidation_cannot_reset_existing_pins_in_place);
	UT_RUN(test_overflow_reserved_raw_xid_and_backward_issue_refuse);
	UT_RUN(test_multiple_namespace_pins_keep_the_earliest_full_xid);
	UT_RUN(test_output_alias_cannot_erase_live_truncate_reservation);
	UT_RUN(test_invalid_build_identity_key_and_cut_cannot_grant_permission);
	UT_RUN(test_epoch_zero_is_exact_identity_not_missing_admission);
	UT_RUN(test_tt_guards_require_complete_directory);
	UT_RUN(test_published_binding_precedes_first_relmap_pin);
	UT_RUN(test_later_active_enrollment_preserves_staged_obligation);
	UT_RUN(test_late_binding_covers_all_namespaces_and_future_begin);
	UT_RUN(test_bound_begin_enrolls_existing_same_full_xid_namespaces);
	UT_RUN(test_same_raw_other_full_xid_cannot_supply_missing_binding);
	UT_RUN(test_invalid_tt_receipts_cannot_create_or_release_pins);
	UT_RUN(test_conflicting_binding_refuses_atomically_including_active_lsn);
	UT_RUN(test_bound_begin_refusal_cannot_partially_enroll_existing_pin);
	UT_RUN(test_tt_release_matches_exact_physical_key_not_active_lsn);
	UT_RUN(test_cancel_clears_only_unstaged_relmap_binding_edge);
	UT_RUN(test_restored_pending_needs_real_tt_enrollment);
	UT_RUN(test_capacity_failure_cannot_partially_enroll_published_binding);
	UT_RUN(test_prepare_snapshot_copies_actual_pin_and_allows_exact_stage_only);
	UT_RUN(test_prepare_snapshot_refuses_every_original_binding_field_change);
	UT_RUN(test_prepare_snapshot_alias_refusal_preserves_original_storage);
	UT_DONE();
	return ut_failed_count != 0;
}
