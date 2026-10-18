/* PGRAC: actual bounded relmap retention state core, no authority mocks.
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md */
#include "postgres_fe.h"

#include "cluster/cluster_relmap_retention.h"
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
	for (int field = 0; field < 4; field++) {
		wrong = key;
		switch (field) {
		case 0: wrong.membership_epoch = 0; break;
		case 1: wrong.admitted_incarnation = 0; break;
		case 2: wrong.boot_incarnation = 0; break;
		case 3: wrong.generation = 1; break;
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

int
main(void)
{
	UT_PLAN(24);
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
	UT_DONE();
	return ut_failed_count != 0;
}
