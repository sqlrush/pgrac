/* Original retention .o with the confirmation owner's explicit pure leaves. */
#include "cluster/cluster_control_relmap_delta.h"

static char pin_confirm_owner, pin_confirm_typed, pin_confirm_plan;
static ClusterControlRootReadToken pin_confirm_before[2], pin_confirm_after[2];
static bool pin_confirm_complete, pin_confirm_valid, pin_confirm_throw;

bool
cluster_control_relmap_recovery_create_confirmed_root_delta_v2(
	const struct ClusterControlRelmapRecoveryCreateV2 *c, const struct ClusterColdTypedV1 *typed,
	const struct ClusterRecoveryFencePlan *plan, uint64 episode,
	const ClusterControlRootReadToken *before, ClusterControlRootReadToken *effective)
{
	uint16 i;
	if (c != (const void *)&pin_confirm_owner || typed != (const void *)&pin_confirm_typed
		|| plan != (const void *)&pin_confirm_plan || episode != 9
		|| !pin_confirm_complete || !pin_confirm_valid
		|| before == NULL || effective == NULL || before->origin_thread_id == 0
		|| (i = before->origin_thread_id - 1) >= 2
		|| memcmp(before, &pin_confirm_before[i], sizeof(*before)))
		return false;
	if (pin_confirm_throw && i == 1)
		pg_re_throw(); /* original owner ERROR while checking the second origin */
	*effective = pin_confirm_after[i];
	return true;
}

UT_TEST(test_confirmed_pin_resumes_same_binding_with_exact_new_tokens)
{
	for (uint16 nthreads = 1; nthreads <= 2; nthreads++) {
		ClusterWalRetentionInterval intervals[2]
			= { { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 },
				{ 2, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 } };
		ClusterWalRetentionPinThreadRequest requests[2]
			= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
		ClusterRecoverySerialGuardSet set = {0};
		ClusterWalRetentionPin *pin = NULL;

		reset_pin_fakes();
		cluster_shared_config = true;
		set.count = nthreads;
		for (uint16 i = 0; i < nthreads; i++) {
			set.guards[i] = make_serial_guard(&requests[i]);
			set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
			pin_confirm_before[i] = requests[i].root_read;
			pin_confirm_after[i] = requests[i].root_read;
			pin_confirm_after[i].file_txn_seq = 777;
		}
		pin_confirm_valid = pin_confirm_complete = true;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, nthreads, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(nthreads == 1
			? cluster_wal_retention_pin_bind_one(pin, &set.guards[0])
			: cluster_wal_retention_pin_bind_set(pin, &set), CLUSTER_WAL_PIN_OK);
		memset(&set, 0, sizeof(set));
		UT_ASSERT_EQ(cluster_wal_retention_pin_confirm_delta_v1(pin, &set,
			(const void *)&pin_confirm_typed, (const void *)&pin_confirm_plan, 9,
			(const void *)&pin_confirm_owner), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_confirm_delta_v1(pin, &set,
			(const void *)&pin_confirm_typed, (const void *)&pin_confirm_plan, 9,
			(const void *)&pin_confirm_owner), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(fake_acquire_call_count, nthreads);
		UT_ASSERT_EQ(fake_release_call_count, 0);
		set.count = nthreads;
		for (uint16 i = 0; i < nthreads; i++) {
			set.guards[i] = make_serial_guard(&requests[i]);
			set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
			set.guards[i].root_read_token = pin_confirm_after[i];
		}
		UT_ASSERT_EQ(cluster_wal_retention_pin_revalidate(pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(pin, &set), CLUSTER_WAL_PIN_INVALID);
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
}

UT_TEST(test_pin_confirm_refuses_incomplete_proof_and_partial_or_foreign_delta)
{
	for (unsigned fault = 0; fault < 6; fault++) {
		ClusterWalRetentionInterval intervals[2]
			= { { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 },
				{ 2, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 } };
		ClusterWalRetentionPinThreadRequest requests[2]
			= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
		ClusterRecoverySerialGuardSet set = {0};
		ClusterWalRetentionPin *pin = NULL;

		reset_pin_fakes();
		cluster_shared_config = true;
		set.count = 2;
		for (uint16 i = 0; i < 2; i++) {
			set.guards[i] = make_serial_guard(&requests[i]);
			set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
			pin_confirm_before[i] = requests[i].root_read;
			pin_confirm_after[i] = requests[i].root_read;
			pin_confirm_after[i].file_txn_seq = 777;
		}
		pin_confirm_valid = pin_confirm_complete = true;
		UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin), CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(pin, &set), CLUSTER_WAL_PIN_OK);
		memset(&set, 0, sizeof(set));
		if (fault == 0) pin_confirm_complete = false;
		if (fault == 1) pin_confirm_valid = false;
		if (fault == 2) pin_confirm_before[1].file_txn_seq++;
		if (fault == 3) pin_confirm_after[1].root_publish_seq++;
		if (fault == 4) pin_confirm_after[1].file_txn_seq = 0;
		if (fault == 5) set.guards[1].release_uncertain = true;
		UT_ASSERT(cluster_wal_retention_pin_confirm_delta_v1(pin, &set,
			(const void *)&pin_confirm_typed, (const void *)&pin_confirm_plan, 9,
			(const void *)&pin_confirm_owner) != CLUSTER_WAL_PIN_OK);
		UT_ASSERT_EQ(fake_acquire_call_count, 2);
		UT_ASSERT_EQ(fake_release_call_count, 0);
		UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
	}
}

UT_TEST(test_pin_confirmation_error_keeps_original_debt_cleanup_only)
{
	ClusterWalRetentionInterval intervals[2]
		= { { 1, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 },
			{ 2, 0, 1, TEST_WAL_SEG_SIZE, TEST_WAL_SEG_SIZE * 2 } };
	ClusterWalRetentionPinThreadRequest requests[2]
		= { make_pin_request(1, &intervals[0], 1), make_pin_request(2, &intervals[1], 1) };
	ClusterRecoverySerialGuardSet set = {0};
	ClusterWalRetentionPin *pin = NULL;
	volatile bool caught = false;

	reset_pin_fakes();
	cluster_shared_config = true;
	set.count = 2;
	for (uint16 i = 0; i < 2; i++) {
		set.guards[i] = make_serial_guard(&requests[i]);
		set.guards[i].mode = CLUSTER_RECOVERY_SERIAL_COLD_FORMED;
		pin_confirm_before[i] = requests[i].root_read;
		pin_confirm_after[i] = requests[i].root_read;
		pin_confirm_after[i].file_txn_seq = 777;
	}
	pin_confirm_valid = pin_confirm_complete = true;
	UT_ASSERT_EQ(cluster_wal_retention_pin_acquire(requests, 2, &pin), CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(cluster_wal_retention_pin_bind_set(pin, &set), CLUSTER_WAL_PIN_OK);
	memset(&set, 0, sizeof(set));
	pin_confirm_throw = true;
	PG_TRY();
	{
		(void)cluster_wal_retention_pin_confirm_delta_v1(pin, &set,
			(const void *)&pin_confirm_typed, (const void *)&pin_confirm_plan, 9,
			(const void *)&pin_confirm_owner);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	pin_confirm_throw = false;
	UT_ASSERT(caught);
	UT_ASSERT(cluster_wal_retention_pin_serial_suspended_current_v1(pin, &set)
		!= CLUSTER_WAL_PIN_OK);
	UT_ASSERT(cluster_wal_retention_pin_confirm_delta_v1(pin, &set,
		(const void *)&pin_confirm_typed, (const void *)&pin_confirm_plan, 9,
		(const void *)&pin_confirm_owner) != CLUSTER_WAL_PIN_OK);
	UT_ASSERT_EQ(fake_acquire_call_count, 2);
	UT_ASSERT_EQ(fake_release_call_count, 0);
	UT_ASSERT_EQ(cluster_wal_retention_pin_release(&pin), CLUSTER_WALR_RELEASE_CONFIRMED);
}
