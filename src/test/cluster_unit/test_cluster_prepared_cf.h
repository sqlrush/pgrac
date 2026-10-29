/* Prepared ROOT consumers joined to the actual CF/owner/CONTROL fixture.
 * Physical image validation/I/O, WAL census and all-member ACK observation
 * are leaves. This does not assert a native LMON or storage proof.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "cluster/cluster_semantic_activation.h"
#include "common/cryptohash.h"

static ControlRootImage prepared_disk;
static ClusterControlRootMigrationImage prepared_image;
static ClusterControlRootMigrationRoundV1 prepared_round, prepared_ack_round;
static ClusterControlRootFileToken prepared_expected;
static uint8 prepared_round_hash[32];
static bool prepared_activate, prepared_ack_complete;
static unsigned prepared_acquires, prepared_reads, prepared_wal_reads, prepared_writes;
static uint64 prepared_cookie;
static ClusterControlRequestView prepared_request;
static ClusterControlRootResult prepared_storage_result, prepared_read_result;

static bool
prepared_image_valid(const ClusterControlRootMigrationImage *image,
					 const ClusterControlRootMigrationRoundV1 *round)
{
	/* The physical validators have their own ROOT tests. Keep this leaf
	 * explicit; a valid byte image cannot replace the real authority check. */
	return image != NULL && round != NULL && image->system_identifier == 1234
		   && image->storage_uuid[0] == 31 && image->assigned_record_count == 1;
}

static ClusterControlRootResult
prepared_storage(const uint8 *uuid, bool probe)
{
	UT_ASSERT(probe && (uuid == NULL || uuid[0] == 31));
	return prepared_storage_result;
}

static void
prepared_encode_record(uint8 *dst, const ClusterControlRootSnapshot *record, uint64 incarnation,
					   uint32 node, ClusterControlRootPublishReason reason)
{
	UT_ASSERT_EQ(incarnation, 17);
	UT_ASSERT_EQ(node, 0);
	UT_ASSERT_EQ(reason, CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT);
	memcpy(dst, record, Min(sizeof(*record), CLUSTER_CONTROL_ROOT_RECORD_BYTES));
}

static void
prepared_encode_header(ControlRootImage *root)
{
	root->header.format_version = 1;
	memcpy(root->bytes, &root->header, sizeof(root->header));
}

static ClusterControlRootResult
prepared_wal(const ClusterControlRootSnapshot *records, const uint8 *expected, uint8 out[32],
			 const ClusterControlRootMigrationRoundV1 *round)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(records[0].identity.system_identifier, 1234);
	UT_ASSERT_EQ(round->coordinator_incarnation, 17);
	++prepared_wal_reads;
	memset(out, 0x55, 32);
	if (expected != NULL && memcmp(expected, out, 32) != 0)
		return CLUSTER_CONTROL_ROOT_MIGRATION_ROUND_MISMATCH;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
prepared_initial(const ControlRootImage *root)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(root->header.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
	UT_ASSERT_EQ(root->header.file_txn_seq, 1);
	++prepared_writes;
	prepared_disk = *root;
	return true;
}

static bool
prepared_updated(const ControlRootImage *old, const ControlRootImage *next)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(old->header.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
	UT_ASSERT_EQ(next->header.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(next->header.file_txn_seq, old->header.file_txn_seq + 1);
	++prepared_writes;
	prepared_disk = *next;
	return true;
}

static ClusterControlRootResult
prepared_read_pair(ControlRootImage *primary, ControlRootImage *bak)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	++prepared_reads;
	*primary = *bak = prepared_disk;
	return prepared_read_result;
}

static bool
prepared_path(char *path, size_t size, const char *relative)
{
	UT_ASSERT_EQ(strcmp(relative, CLUSTER_CONTROL_ROOT_REL_PATH), 0);
	return snprintf(path, size, "prepared-root-fixture") > 0;
}

static ClusterControlRootResult
prepared_read_one(const char *path, const uint8 uuid[16], uint64 sysid, ControlRootImage *out)
{
	UT_ASSERT(cluster_cf_held_is_clusterwide(ExclusiveLock));
	UT_ASSERT_EQ(strcmp(path, "prepared-root-fixture"), 0);
	UT_ASSERT_EQ(uuid[0], 31);
	UT_ASSERT_EQ(sysid, 1234);
	++prepared_reads;
	*out = prepared_disk;
	return prepared_read_result;
}

bool
cluster_semantic_activation_ack_complete_matches(uint64 epoch, uint64 generation, uint64 members_lo,
												 uint64 members_hi, uint64 source, uint64 target,
												 uint64 digest,
												 ClusterSemanticActivationAckStage stage)
{
	/* Scheduling/ACK-table observation is a leaf. The complete source-owned
	 * create/activate authority predicates consume these exact arguments. */
	return prepared_ack_complete && epoch == prepared_ack_round.transition_epoch
		   && generation == prepared_ack_round.prepare_generation
		   && members_lo == prepared_ack_round.admitted_bitmap_low
		   && members_hi == prepared_ack_round.admitted_bitmap_high
		   && source == prepared_ack_round.source_feature_bitmap
		   && target == prepared_ack_round.target_feature_bitmap
		   && digest == prepared_ack_round.capability_sample_digest
		   && stage == CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED;
}

static bool
prepared_actual_cf_lock(LOCKMODE mode)
{
	bool acquired;

	UT_ASSERT_EQ(mode, ExclusiveLock);
	++prepared_acquires;
	++g_next_request_id;
	acquired = cluster_cf_lock(mode);
	if (acquired && prepared_cookie != 0) {
		ClusterControlRequestView old;

		/* This is checked after the original acquire's stale-slot drain and
		 * before any physical ROOT observation or publication. */
		UT_ASSERT(cluster_cf_release_completed(mode, prepared_cookie));
		UT_ASSERT(!cluster_control_request_snapshot(&prepared_request.handle, &old));
	}
	return acquired;
}

#define CONTROL_ROOT_FORMAT_VERSION UINT16_C(1)
#define write_u16_le prepared_write_u16
#define write_u32_le prepared_write_u32
#define write_u64_le prepared_write_u64
#define bytes_are_zero prepared_bytes_zero
#define control_root_sha256 prepared_sha256
#define cluster_control_root_feature_bitmap_is_known prepared_features_known
#define encode_round prepared_encode_round
#define file_token_equal prepared_token_equal
#define make_file_token prepared_make_token
#define acquire_clusterwide_cf prepared_acquire
#define release_cf prepared_release
#define cluster_cf_lock prepared_actual_cf_lock
#define migration_image_validate prepared_image_valid
#define storage_contract_check prepared_storage
#define read_source_wal_state prepared_wal
#define encode_record prepared_encode_record
#define encode_header prepared_encode_header
#define publish_initial_image prepared_initial
#define publish_updated_image prepared_updated
#define read_canonical_pair prepared_read_pair
#define build_control_path prepared_path
#define read_one_image prepared_read_one
#include "test_cluster_prepared_cf.inc"
#undef read_one_image
#undef build_control_path
#undef read_canonical_pair
#undef publish_updated_image
#undef publish_initial_image
#undef encode_header
#undef encode_record
#undef read_source_wal_state
#undef storage_contract_check
#undef migration_image_validate
#undef cluster_cf_lock
#undef release_cf
#undef acquire_clusterwide_cf
#undef make_file_token
#undef file_token_equal
#undef encode_round
#undef cluster_control_root_feature_bitmap_is_known
#undef control_root_sha256
#undef bytes_are_zero
#undef write_u64_le
#undef write_u32_le
#undef write_u16_le
#undef CONTROL_ROOT_FORMAT_VERSION

static ClusterControlRootResult
prepared_call(ClusterControlRootFileToken *out)
{
	return prepared_activate
			   ? cluster_control_root_activate_prepared(&prepared_expected, prepared_round_hash,
														&prepared_round, out)
			   : cluster_control_root_create_prepared(&prepared_image, &prepared_round, out);
}

static void
prepared_fixture(bool activate)
{
	uint8 encoded[80];

	entry_fixture(false);
	MyBackendType = B_LMON;
	MyAuxProcType = LmonProcess;
	prepared_activate = activate;
	prepared_cookie = 0;
	prepared_ack_complete = true;
	prepared_acquires = prepared_reads = prepared_wal_reads = prepared_writes = 0;
	g_s5_result = g_s6_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
	prepared_storage_result = prepared_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	memset(&prepared_round, 0, sizeof(prepared_round));
	memcpy(prepared_round.magic, "PCRM", 4);
	prepared_round.version = 1;
	prepared_round.bytes = sizeof(prepared_round);
	prepared_round.prepare_generation = 8;
	prepared_round.transition_epoch = owner_epoch;
	prepared_round.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	prepared_round.admitted_bitmap_low = 15;
	prepared_round.capability_sample_digest = 7;
	prepared_round.coordinator_incarnation = 17;
	prepared_ack_round = prepared_round;
	UT_ASSERT(prepared_encode_round(&prepared_round, encoded));
	UT_ASSERT(prepared_sha256(encoded, sizeof(encoded), prepared_round_hash));
	memset(&prepared_image, 0, sizeof(prepared_image));
	prepared_image.system_identifier = 1234;
	prepared_image.storage_uuid[0] = 31;
	prepared_image.authority_uuid[0] = 41;
	prepared_image.assigned_record_count = 1;
	prepared_image.records[0].identity = entry_selected.claim.identity;
	memset(&prepared_disk, 0, sizeof(prepared_disk));
	prepared_disk.header.file_txn_seq = 1;
	prepared_disk.header.system_identifier = 1234;
	prepared_disk.header.storage_uuid[0] = 31;
	prepared_disk.header.authority_uuid[0] = 41;
	prepared_disk.header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED;
	memcpy(prepared_disk.header.migration_round_sha256, prepared_round_hash, 32);
	memset(prepared_disk.header.source_wal_state_sha256, 0x55, 32);
	prepared_disk.records[0] = prepared_image.records[0];
	prepared_encode_header(&prepared_disk);
	prepared_make_token(&prepared_disk, &prepared_expected);
}

static void
prepared_no_output(const ClusterControlRootFileToken *out)
{
	ClusterControlRootFileToken zero = { 0 };

	UT_ASSERT_EQ(memcmp(out, &zero, sizeof(zero)), 0);
}

static void
prepared_timeout(void)
{
	ClusterControlRootFileToken out;
	ClusterCfAttemptObservation observed;
	uint64 before = cluster_cf_attempt_sequence();

	memset(&out, 0xa5, sizeof(out));
	UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT(cluster_cf_last_attempt_since(before, &observed));
	UT_ASSERT(cluster_cf_attempt_wait_slice(&observed));
	UT_ASSERT_EQ(g_last_timeout_ms, 1000);
	UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cf_hold_x.owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	prepared_cookie = cluster_cf_owner_cookie(ExclusiveLock);
	UT_ASSERT(prepared_cookie != 0);
	UT_ASSERT(cluster_control_request_snapshot(&cf_hold_x.owner.attempt, &prepared_request));
	UT_ASSERT_EQ(prepared_request.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT_EQ(prepared_request.owner.pid, MyProcPid);
	UT_ASSERT_EQ(prepared_request.owner.incarnation, control_creator.incarnation);
	UT_ASSERT_EQ(prepared_request.message.key.holder.node_id, 0);
	UT_ASSERT_EQ(prepared_request.message.key.holder.procno, 42);
	UT_ASSERT_EQ(prepared_request.message.key.holder.cluster_epoch, owner_epoch);
	UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes, 0);
	prepared_no_output(&out);
}

static void
prepared_finish(void)
{
	if (cluster_cf_held(ExclusiveLock))
		entry_cleanup(ExclusiveLock, cluster_cf_owner_cookie(ExclusiveLock));
	UT_ASSERT(!cluster_cf_held(ShareLock) && !cluster_cf_held(ExclusiveLock));
	UT_ASSERT(cluster_control_request_empty());
	UT_ASSERT_EQ(pg_atomic_read_u32(&MyProc->cluster_grd_registered_count), 0);
	UT_ASSERT_EQ(entry_waits, 0);
	UT_ASSERT(control_locked == NULL && !entry_native_lock && PG_exception_stack == NULL);
	MyBackendType = B_STARTUP;
	MyAuxProcType = StartupProcess;
}

static void
prepared_release_debt(const ClusterControlRootFileToken *out)
{
	ClusterControlRequestView view;
	uint64 cookie = cluster_cf_owner_cookie(ExclusiveLock);

	UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
	UT_ASSERT_EQ(cf_hold_x.owner.state, CLUSTER_LOCK_OWNER_RETIRING);
	UT_ASSERT(cookie != 0 && cookie != prepared_cookie);
	UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, cookie));
	UT_ASSERT(cluster_control_request_snapshot(&cf_hold_x.owner.attempt, &view));
	UT_ASSERT_EQ(view.state, CLUSTER_CONTROL_REQUEST_ABANDONED);
	UT_ASSERT_EQ(view.message.key.holder.request_id, g_next_request_id);
	UT_ASSERT_EQ(view.owner.pid, MyProcPid);
	prepared_no_output(out);
}

/* Break caught: a new ROOT request/read/publication escapes the old cleanup,
 * or the already-owned original request cannot finish after its exact ACK. */
UT_TEST(lmon_prepared_cf_requires_exact_retirement_before_reapply)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterControlRootFileToken out;
		int seven;

		prepared_fixture(activate);
		prepared_timeout();
		seven = g_seven_count;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		for (int tick = 0; tick < 3; tick++) {
			cluster_cf_retirement_poll();
			cluster_lock_owners_service_poll();
			UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(g_seven_count, seven);
			UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
			UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes, 0);
			prepared_no_output(&out);
		}
		UT_ASSERT(acknowledge_retirements());
		cluster_cf_retirement_poll();
		UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
		UT_ASSERT(!cluster_cf_held(ExclusiveLock) && cluster_control_request_empty());
		/* The original timeout is closed and ROOT is reapplied, but the
		 * distinct new release is asynchronous. Never export its token. */
		UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		UT_ASSERT_EQ(g_seven_count, seven + 1);
		UT_ASSERT_EQ(prepared_disk.header.file_txn_seq, activate ? 2 : 1);
		UT_ASSERT_EQ(prepared_disk.header.activation_state,
					 activate ? CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
							  : CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		UT_ASSERT_EQ(prepared_writes, 1);
		UT_ASSERT_EQ(prepared_reads, 1 + activate);
		prepared_release_debt(&out);
		prepared_finish();
	}
}

/* Break caught: retirement matches only a resource or PID, adopting a
 * different full key or exchange instead of the original request. */
UT_TEST(lmon_prepared_cf_wrong_ack_cannot_release_the_original_request)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterControlRequestCut cut;
		ClusterControlRetireMessage ack, wrong;
		ClusterControlRootFileToken out;
		int seven;

		prepared_fixture(activate);
		prepared_timeout();
		seven = g_seven_count;
		control_cut(&cut);
		UT_ASSERT(
			cluster_control_request_claim(&prepared_request.handle, retire_driver, &cut, &ack));
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
			cluster_cf_retirement_poll();
			UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
			UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			UT_ASSERT_EQ(g_seven_count, seven);
			UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes, 0);
		}
		UT_ASSERT(cluster_control_request_ack(&ack, 0, retire_driver, &cut));
		cluster_cf_retirement_poll();
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		prepared_release_debt(&out);
		prepared_finish();
	}
}

/* Break caught: owner service consumes the ACK first, leaving the CF slot
 * stuck forever or permitting a reapply before the CF receipt is recorded. */
UT_TEST(lmon_prepared_cf_service_consumed_ack_still_finishes_own_receipt)
{
	for (int activate = 0; activate < 2; activate++) {
		ClusterControlRootFileToken out;

		prepared_fixture(activate);
		prepared_timeout();
		UT_ASSERT(acknowledge_retirements());
		cluster_lock_owners_service_poll();
		UT_ASSERT(cluster_cf_held(ExclusiveLock) && !cluster_cf_held_is_usable(ExclusiveLock));
		UT_ASSERT_EQ(cf_hold_x.owner.state, CLUSTER_LOCK_OWNER_RETIRING);
		UT_ASSERT_EQ(cf_hold_x.owner.attempt.generation, 0);
		UT_ASSERT(!cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, prepared_cookie));
		UT_ASSERT_EQ(prepared_writes, 1);
		prepared_release_debt(&out);
		prepared_finish();
	}
}

/* Expected-no-error RED: the old timeout is retired, the new grant and
 * physical publication succeed, but the original LMON release cannot wait
 * for its own CONTROL work. This remains opt-in until its fix is frozen. */
UT_TEST(lmon_prepared_cf_successful_publication_must_finish_original_round)
{
	for (int activate = 0; activate < 2; activate++)
		for (int prior_timeout = 0; prior_timeout < 2; prior_timeout++) {
			ClusterControlRootFileToken out;
			ClusterControlRootResult result;
			uint64 original;

			prepared_fixture(activate);
			if (prior_timeout)
				prepared_timeout();
			original = prepared_cookie;
			if (prior_timeout) {
				UT_ASSERT(acknowledge_retirements());
				cluster_cf_retirement_poll();
				UT_ASSERT(cluster_cf_release_completed(ExclusiveLock, original));
			}
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			result = prepared_call(&out);
			UT_ASSERT_EQ(prepared_writes, 1);
			UT_ASSERT_EQ(prepared_disk.header.file_txn_seq, activate ? 2 : 1);
			printf(
				"# prepared-cf activate=%d prior_timeout=%d result=%d writes=%u seq=" UINT64_FORMAT
				" old_cookie=" UINT64_FORMAT " release_cookie=" UINT64_FORMAT
				" owner_state=%d registry_generation=" UINT64_FORMAT " role=%d\n",
				activate, prior_timeout, (int)result, prepared_writes,
				prepared_disk.header.file_txn_seq, original, cluster_cf_owner_cookie(ExclusiveLock),
				(int)cf_hold_x.owner.state, cf_hold_x.owner.attempt.generation, (int)MyBackendType);
			if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN) {
				ClusterControlRequestView view;

				prepared_release_debt(&out);
				UT_ASSERT(cluster_control_request_snapshot(&cf_hold_x.owner.attempt, &view));
				printf("# prepared-cf release-key type=%u method=%u fields=%u/%u/%u/%u mode=%d"
					   " holder=%u/%u/" UINT64_FORMAT "/" UINT64_FORMAT
					   " registry=%u/" UINT64_FORMAT " creator=%d/%u/" UINT64_FORMAT
					   " state=%d exchange=" UINT64_FORMAT " driver=" UINT64_FORMAT "\n",
					   view.message.key.resid.type, view.message.key.resid.lockmethodid,
					   view.message.key.resid.field1, view.message.key.resid.field2,
					   view.message.key.resid.field3, view.message.key.resid.field4, (int)view.mode,
					   view.message.key.holder.node_id, view.message.key.holder.procno,
					   view.message.key.holder.cluster_epoch, view.message.key.holder.request_id,
					   view.handle.slot, view.handle.generation, view.owner.pid, view.owner.procno,
					   view.owner.incarnation, (int)view.state, view.message.exchange_id,
					   view.driver);
			}
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			UT_ASSERT_EQ(out.file_txn_seq, activate ? 2 : 1);
			prepared_finish();
		}
}

/* Break caught: byte validity or a retired request substitutes for the
 * coordinator identity or the original PREPARED all-member ACK binding. */
UT_TEST(lmon_prepared_cf_retry_rechecks_coordinator_and_prepared_ack)
{
	for (int activate = 0; activate < 2; activate++)
		for (unsigned field = 0; field < (activate ? 8 : 2); field++) {
			ClusterControlRootFileToken out;
			int seven;

			prepared_fixture(activate);
			prepared_timeout();
			UT_ASSERT(acknowledge_retirements());
			cluster_cf_retirement_poll();
			seven = g_seven_count;
			switch (field) {
			case 0:
				prepared_round.coordinator_node_id++;
				break;
			case 1:
				prepared_round.target_feature_bitmap = 0;
				break;
			case 2:
				prepared_ack_complete = false;
				break;
			case 3:
				prepared_ack_round.transition_epoch++;
				break;
			case 4:
				prepared_ack_round.prepare_generation++;
				break;
			case 5:
				prepared_ack_round.admitted_bitmap_low ^= 2;
				break;
			case 6:
				prepared_ack_round.capability_sample_digest++;
				break;
			case 7:
				prepared_ack_round.source_feature_bitmap ^= 1;
				break;
			}
			g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
			UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			UT_ASSERT_EQ(g_seven_count, seven);
			UT_ASSERT_EQ(prepared_reads | prepared_writes, 0);
			prepared_no_output(&out);
			prepared_finish();
		}
}

/* Break caught: real canonical failure grants output or mutates ACTIVE.
 * The existing uncertain release masks the more specific ROOT result; the
 * opt-in liveness RED deliberately does not treat that as a successful round. */
UT_TEST(lmon_prepared_cf_real_token_and_round_failure_still_refuse)
{
	for (unsigned field = 0; field < 3; field++) {
		ClusterControlRootFileToken out;
		int seven;

		prepared_fixture(true);
		prepared_timeout();
		UT_ASSERT(acknowledge_retirements());
		cluster_cf_retirement_poll();
		seven = g_seven_count;
		if (field == 0)
			prepared_expected.file_txn_seq++;
		else if (field == 1)
			prepared_round_hash[0] ^= 1;
		else
			prepared_read_result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		g_seven_result = CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		UT_ASSERT_EQ(prepared_call(&out), CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
		UT_ASSERT_EQ(g_seven_count, seven + 1);
		UT_ASSERT_EQ(prepared_reads, 1);
		UT_ASSERT_EQ(prepared_wal_reads | prepared_writes, 0);
		UT_ASSERT_EQ(prepared_disk.header.activation_state,
					 CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		prepared_no_output(&out);
		prepared_release_debt(&out);
		prepared_finish();
	}
}

/* Break caught: a hard transport refusal or storage-contract error becomes
 * usable CF/ROOT authority merely because another iteration is possible. */
UT_TEST(lmon_prepared_cf_hard_failure_never_publishes_root)
{
	const ClusterLockAcquireResult hard[]
		= { CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL, CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK };

	for (int activate = 0; activate < 2; activate++)
		for (unsigned failure = 0; failure < lengthof(hard) + 1; failure++) {
			ClusterControlRootFileToken out;
			ClusterCfAttemptObservation observation;
			uint64 before;

			prepared_fixture(activate);
			before = cluster_cf_attempt_sequence();
			if (failure == lengthof(hard))
				prepared_storage_result = CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
			else
				g_seven_result = hard[failure];
			UT_ASSERT_EQ(prepared_call(&out), failure == lengthof(hard)
												  ? CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED
												  : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
			if (failure != lengthof(hard)) {
				UT_ASSERT(cluster_cf_last_attempt_since(before, &observation));
				UT_ASSERT(!cluster_cf_attempt_wait_slice(&observation));
			} else
				UT_ASSERT_EQ(prepared_acquires, 0);
			UT_ASSERT_EQ(prepared_reads | prepared_wal_reads | prepared_writes, 0);
			prepared_no_output(&out);
			prepared_finish();
		}
}
