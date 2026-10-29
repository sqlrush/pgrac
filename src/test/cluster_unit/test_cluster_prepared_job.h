/* Prepared publication work, joined to the actual CF owner/registry.
 * ROOT I/O and scheduling remain the leaves in test_cluster_prepared_cf.h.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_control_root_prepared.h"

static ClusterControlRootPreparedWorkV1 prepared_job;
static uint64 prepared_job_generation;

static void
prepared_job_fixture(bool activate)
{
	uint8 encoded[80];

	prepared_fixture(activate);
	/* This fixture's boot leaf shares the real registry creator. Initialize
	 * it before encoding the round, including when this group runs first. */
	UT_ASSERT(lock_owner_creator());
	memset(&prepared_job, 0, sizeof(prepared_job));
	prepared_job.release.job_generation = ++prepared_job_generation;
	prepared_round.coordinator_incarnation = cluster_qvotec_get_self_incarnation();
	prepared_ack_round = prepared_round;
	UT_ASSERT(prepared_encode_round(&prepared_round, encoded));
	UT_ASSERT(prepared_sha256(encoded, sizeof(encoded), prepared_round_hash));
	memcpy(prepared_disk.header.migration_round_sha256, prepared_round_hash, 32);
	prepared_encode_header(&prepared_disk);
	prepared_make_token(&prepared_disk, &prepared_expected);
	CurrentResourceOwner = (ResourceOwner)1;
	entry_latch_sets = 0;
}

static ClusterControlRootResult
prepared_job_call(ClusterControlRootFileToken *out)
{
	return prepared_activate
			   ? cluster_control_root_activate_prepared_job_v1(
					 &prepared_expected, prepared_round_hash, &prepared_round, &prepared_job, out)
			   : cluster_control_root_create_prepared_job_v1(&prepared_image, &prepared_round,
															 &prepared_job, out);
}

static void
prepared_job_closed(void)
{
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT(control_locked == NULL && PG_exception_stack == NULL);
	UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_CONFIRMED);
	UT_ASSERT_EQ(prepared_job.phase, CONTROL_ROOT_PREPARED_WORK_CONSUMED);
}

UT_TEST(prepared_job_delivers_publication_once_after_exact_release)
{
	for (int activate = 0; activate < 2; activate++)
		for (int prior = 0; prior < 2; prior++) {
			ClusterControlRootFileToken out;
			int requests;
			unsigned reads;

			prepared_job_fixture(activate);
			if (prior) {
				prepared_timeout();
				UT_ASSERT(acknowledge_retirements());
				cluster_cf_retirement_poll();
			}
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
			prepared_no_output(&out);
			UT_ASSERT_EQ(prepared_job.result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(prepared_job.token.file_txn_seq, activate ? 2 : 1);
			UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_RETIRING);
			UT_ASSERT_EQ(prepared_job.release.original.cookie,
						 cluster_cf_owner_cookie(ExclusiveLock));
			UT_ASSERT_EQ(prepared_writes, 1);
			requests = g_seven_count;
			reads = prepared_reads;
			for (int poll = 0; poll < 3; poll++) {
				UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
							 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
				prepared_no_output(&out);
				UT_ASSERT_EQ(g_seven_count, requests);
				UT_ASSERT_EQ(prepared_reads, reads);
				UT_ASSERT_EQ(prepared_writes, 1);
				UT_ASSERT_EQ(entry_waits, 0);
			}
			UT_ASSERT(acknowledge_retirements());
			UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
						 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(out.file_txn_seq, activate ? 2 : 1);
			UT_ASSERT_EQ(out.activation_state, activate ? CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
														: CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
			UT_ASSERT_EQ(g_seven_count, requests);
			UT_ASSERT_EQ(prepared_reads, reads);
			UT_ASSERT_EQ(prepared_writes, 1);
			UT_ASSERT(entry_latch_sets != 0);
			UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
						 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			prepared_no_output(&out);
			prepared_job_closed();
		}
}

UT_TEST(prepared_job_wrong_ack_keeps_published_result_private)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterControlRootFileToken out;
		ClusterControlRequestCut cut;
		ClusterControlRetireMessage ack, wrong;

		prepared_job_fixture(activate);
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		control_cut(&cut);
		UT_ASSERT(cluster_control_request_claim(&prepared_job.release.original.registry.handle,
												retire_driver, &cut, &ack));
		ack.verb = CLUSTER_CONTROL_RETIRED;
		for (unsigned field = 0; field < 7; field++) {
			wrong = ack;
			switch (field) {
			case 0:
				wrong.key.holder.request_id++;
				break;
			case 1:
				wrong.key.holder.cluster_epoch++;
				break;
			case 2:
				wrong.key.resid.field1++;
				break;
			case 3:
				wrong.key.holder.procno++;
				break;
			case 4:
				wrong.key.holder.node_id++;
				break;
			case 5:
				wrong.exchange_id++;
				break;
			case 6:
				wrong.cleanup_epoch++;
				break;
			}
			UT_ASSERT(!cluster_control_request_ack(&wrong, 0, retire_driver, &cut));
			UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
						 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
			prepared_no_output(&out);
			UT_ASSERT_EQ(prepared_writes, 1);
		}
		UT_ASSERT(cluster_control_request_ack(&ack, 0, retire_driver, &cut));
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
					 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		prepared_job_closed();
	}
}

UT_TEST(prepared_job_census_may_consume_original_ack_first)
{
	ClusterControlRootFileToken out;

	prepared_job_fixture(true);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	UT_ASSERT(acknowledge_retirements());
	cluster_lock_owners_service_poll();
	UT_ASSERT_EQ(cf_hold_x.owner.attempt.generation, 0);
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(prepared_writes, 1);
	prepared_job_closed();
}

UT_TEST(prepared_job_receipt_survives_a_later_cf_cookie)
{
	ClusterControlRootFileToken out;
	uint64 original, later;

	prepared_job_fixture(false);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	original = prepared_job.release.original.cookie;
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_CONFIRMED);
	++g_next_request_id;
	UT_ASSERT(cluster_cf_lock(ExclusiveLock));
	later = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT(later != original);
	UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock), CLUSTER_CF_RELEASE_UNCONFIRMED);
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, later));
	UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, original));
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.file_txn_seq, 1);
	UT_ASSERT_EQ(prepared_writes, 1);
	prepared_job_closed();
}

UT_TEST(prepared_job_preserves_the_original_root_failure)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterControlRootFileToken out;
		ClusterControlRootResult expected = field == 0 ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
											: field == 1
												? CLUSTER_CONTROL_ROOT_MIGRATION_ROUND_MISMATCH
												: CLUSTER_CONTROL_ROOT_IO_ERROR;

		prepared_job_fixture(true);
		if (field == 0)
			prepared_expected.file_txn_seq++;
		else if (field == 1)
			prepared_round_hash[0] ^= 1;
		else
			prepared_read_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		prepared_no_output(&out);
		UT_ASSERT_EQ(prepared_job.result, expected);
		UT_ASSERT_EQ(prepared_writes, 0);
		UT_ASSERT(acknowledge_retirements());
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out), expected);
		prepared_no_output(&out);
		prepared_job_closed();
	}
}

UT_TEST(prepared_job_cannot_be_copied_or_restarted_while_live)
{
	ClusterControlRootFileToken out;
	ClusterControlRootPreparedWorkV1 copy;

	prepared_job_fixture(true);
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	copy = prepared_job;
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&copy, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(prepared_writes, 1);
	UT_ASSERT(acknowledge_retirements());
	cluster_cf_retirement_poll();
	copy = prepared_job;
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&copy, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	prepared_no_output(&out);
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	prepared_job_closed();
}

UT_TEST(prepared_job_identity_loss_rejects_delivery_but_keeps_cleanup)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterControlRootFileToken out;
		uint64 epoch, generation;
		ResourceOwner owner;

		prepared_job_fixture(false);
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		epoch = owner_epoch;
		generation = owner_generation;
		owner = CurrentResourceOwner;
		if (field == 0)
			++owner_epoch;
		else if (field == 1)
			++owner_generation;
		else
			CurrentResourceOwner = (ResourceOwner)2;
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
					 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		UT_ASSERT(prepared_job.rejected);
		prepared_no_output(&out);
		/* Restore only the fixture cut for exact cleanup. Refusal is sticky. */
		owner_epoch = epoch;
		owner_generation = generation;
		CurrentResourceOwner = owner;
		UT_ASSERT(acknowledge_retirements());
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
					 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		prepared_no_output(&out);
		UT_ASSERT_EQ(prepared_writes, 1);
		prepared_job_closed();
	}
}

static void
prepared_job_release_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("prepared job release cancelled")));
}

UT_TEST(prepared_job_release_error_retains_the_original_receipt_address)
{
	ClusterControlRootFileToken out;
	volatile bool caught = false;

	prepared_job_fixture(true);
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	cold_consumer = true;
	cold_release_waits = 0;
	entry_on_release_wait = prepared_job_release_error;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	PG_TRY();
	{
		(void)prepared_job_call(&out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(entry_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(cold_release_waits, 1);
	UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_RETIRING);
	UT_ASSERT(cf_hold_x.job_receipt == &prepared_job.release);
	UT_ASSERT_EQ(prepared_job.token.file_txn_seq, 2);
	entry_on_release_wait = NULL;
	cold_consumer = false;
	UT_ASSERT(acknowledge_retirements());
	UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(prepared_writes, 1);
	UT_ASSERT_EQ(cold_release_waits, 1);
	prepared_job_closed();
}

UT_TEST(prepared_job_first_release_keeps_original_bounded_confirmation)
{
	ClusterControlRootFileToken out;

	prepared_job_fixture(false);
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
	entry_ack_on_reset = true;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	UT_ASSERT_EQ(prepared_job_call(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.file_txn_seq, 1);
	UT_ASSERT_EQ(g_last_timeout_ms, 1000);
	UT_ASSERT(entry_resets != 0);
	UT_ASSERT_EQ(prepared_writes, 1);
	prepared_job_closed();
}

static void
prepared_job_postread_error(void)
{
	ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED), errmsg("prepared job readback cancelled")));
}

static void
prepared_job_unbound_error(bool activate)
{
	ClusterControlRootFileToken out;
	volatile bool caught = false;

	prepared_job_fixture(activate);
	prepared_on_published_read = prepared_job_postread_error;
	g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	PG_TRY();
	{
		(void)prepared_job_call(&out);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(entry_code, ERRCODE_QUERY_CANCELED);
	UT_ASSERT_EQ(prepared_writes, 1);
	UT_ASSERT_EQ(prepared_job.result, CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(prepared_job.phase, CONTROL_ROOT_PREPARED_WORK_RETIRING);
	UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_UNBOUND);
	UT_ASSERT(cf_hold_x.job_receipt == NULL);
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	prepared_no_output(&out);
	prepared_on_published_read = NULL;
}

UT_TEST(prepared_job_error_before_release_cleans_only_the_original_hold)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterControlRootFileToken out;
		uint64 cookie;

		prepared_job_unbound_error(activate);
		cookie = cluster_cf_owner_cookie(ExclusiveLock);
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
					 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		UT_ASSERT_EQ(prepared_job.release.original.cookie, cookie);
		UT_ASSERT(cf_hold_x.job_receipt == &prepared_job.release);
		prepared_no_output(&out);
		UT_ASSERT(acknowledge_retirements());
		UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
					 CLUSTER_CONTROL_ROOT_IO_ERROR);
		prepared_no_output(&out);
		UT_ASSERT_EQ(prepared_writes, 1);
		prepared_job_closed();
	}
}

UT_TEST(prepared_job_unbound_error_never_adopts_a_later_cf_request)
{
	for (int activate = 0; activate < 2; activate++)
		for (int later = 0; later < 2; later++) {
			ClusterControlRootFileToken out;
			uint64 original, cookie = 0;

			prepared_job_unbound_error(activate);
			original = cluster_cf_owner_cookie(ExclusiveLock);
			UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock),
						 CLUSTER_CF_RELEASE_UNCONFIRMED);
			UT_ASSERT(acknowledge_retirements());
			cluster_cf_retirement_poll();
			UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, original));
			if (later) {
				++g_next_request_id;
				UT_ASSERT(cluster_cf_lock(ExclusiveLock));
				cookie = cluster_cf_owner_cookie(ExclusiveLock);
				UT_ASSERT(cookie != original);
			}
			UT_ASSERT_EQ(cluster_control_root_prepared_job_poll_v1(&prepared_job, &out),
						 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			prepared_no_output(&out);
			UT_ASSERT_EQ(prepared_job.release.state, CF_JOB_RECEIPT_UNBOUND);
			UT_ASSERT(cf_hold_x.job_receipt == NULL);
			UT_ASSERT_EQ(prepared_writes, 1);
			if (later) {
				UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
				UT_ASSERT_EQ(cluster_cf_owner_cookie(ExclusiveLock), cookie);
				UT_ASSERT_EQ(cluster_cf_unlock_confirmed(ExclusiveLock),
							 CLUSTER_CF_RELEASE_UNCONFIRMED);
				UT_ASSERT(acknowledge_retirements());
				cluster_cf_retirement_poll();
				UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, cookie));
			}
			UT_ASSERT(cluster_control_request_empty());
			UT_ASSERT(!cluster_cf_held(ExclusiveLock));
		}
}
