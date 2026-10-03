/*-------------------------------------------------------------------------
 *
 * test_cluster_storage_corosync.c
 *    Check the storage adapter at the external Corosync C API.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_storage_corosync.c
 *
 * NOTES
 *    PGRAC-original unit tests; product symbols retain the cluster_ prefix.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/guc_tables.h"
#include "../../backend/cluster/cluster_storage_quorum_corosync.c"
#include "../../backend/cluster/cluster_storage_quorum.c"
#undef printf
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_shared_config = true;
int cluster_node_id = 0;
char *cluster_storage_quorum_nodes = "0:11,1:12";
char *cluster_storage_quorum_cluster = "quorum-fixture";
static ClusterConf conf;
ClusterConf *ClusterConfShmem = &conf;
static ClusterNodeInfo nodes[2];
static StorageCorosyncModel callbacks;
static int quorate, notified_quorate, finalize_count, track_count;
static bool notify_enabled, api_failed, changed_after_notify;
static bool tracking_enabled;
static unsigned notification_mismatch;
static const char *bad_key;
static struct config_generic shared_settings[2];

struct config_generic *
find_option(const char *name, bool create_placeholders, bool skip_errors, int elevel)
{
	Size i;

	Assert(!create_placeholders && skip_errors);
	for (i = 0; i < lengthof(shared_settings); i++)
		if (strcmp(name, shared_settings[i].name) == 0)
			return &shared_settings[i];
	return NULL;
}

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	return node_id >= 0 && node_id < 2 ? &nodes[node_id] : NULL;
}

static int
fixture_initialize(uint64 *handle, int model, const void *data, uint32 *type, void *context)
{
	Assert(model == 1 && context == NULL);
	callbacks = *(const StorageCorosyncModel *)data;
	Assert(callbacks.model == model);
	*handle = 123;
	*type = 1;
	return STORAGE_CS_OK;
}

static int
fixture_cmap_initialize(uint64 *handle)
{
	*handle = 456;
	return STORAGE_CS_OK;
}

static int
fixture_finalize(uint64 handle)
{
	Assert(handle == 123 || handle == 456);
	if (handle == 123)
		tracking_enabled = false;
	finalize_count++;
	return STORAGE_CS_OK;
}

static int
fixture_track(uint64 handle, unsigned int flags)
{
	Assert(handle == 123 && (flags & 1) != 0);
	track_count++;
	if (api_failed)
		return 2;
	/* The upstream service rejects a second trackstart after CHANGES. */
	if (tracking_enabled)
		return 14; /* CS_ERR_EXIST */
	tracking_enabled = (flags & 2) != 0;
	return STORAGE_CS_OK;
}

static int
fixture_quorate(uint64 handle, int *out)
{
	Assert(handle == 123);
	*out = quorate;
	return STORAGE_CS_OK;
}

static int
fixture_dispatch(uint64 handle, int flags)
{
	StorageCorosyncRing ring = { 11, 8 };
	uint32 members[] = { 11, 12 };

	Assert(handle == 123 && flags == STORAGE_CS_DISPATCH_ALL);
	if (notify_enabled) {
		StorageCorosyncRing member_ring = ring;
		uint32 member_count = 2;

		if (notification_mismatch & 1)
			member_ring.sequence++;
		if (notification_mismatch & 2)
			member_count = 1;
		callbacks.members_notify(handle, member_ring, member_count, members, 0, NULL, 0, NULL);
		callbacks.quorum_notify(handle, notified_quorate, ring, 2, members);
	}
	if (changed_after_notify) {
		ring.sequence++;
		callbacks.members_notify(handle, ring, 1, members, 0, NULL, 1, members + 1);
	}
	return STORAGE_CS_OK;
}

static int
fixture_cmap_get(uint64 handle, const char *key, void *out, size_t *size, int *type)
{
	static const char *strings[][2] = {
		{ "totem.cluster_name", "quorum-fixture" }, { "quorum.provider", "corosync_votequorum" },
		{ "quorum.device.model", "net" },			{ "quorum.device.net.algorithm", "lms" },
		{ "quorum.device.net.tls", "required" },
	};
	uint32 number = 0;
	Size i;

	Assert(handle == 456);
	if (bad_key != NULL && strcmp(key, bad_key) == 0)
		return 2;
	for (i = 0; i < lengthof(strings); i++) {
		if (strcmp(key, strings[i][0]) == 0) {
			Size length = strlen(strings[i][1]) + 1;

			Assert(*size >= length);
			memcpy(out, strings[i][1], length);
			*size = length;
			*type = STORAGE_CMAP_STRING;
			return STORAGE_CS_OK;
		}
	}
	if (strcmp(key, "runtime.votequorum.this_node_id") == 0
		|| strcmp(key, "nodelist.node.0.nodeid") == 0)
		number = 11;
	else if (strcmp(key, "nodelist.node.1.nodeid") == 0)
		number = 12;
	else
		return STORAGE_CS_NOT_EXIST;
	Assert(*size >= sizeof(number));
	memcpy(out, &number, sizeof(number));
	*size = sizeof(number);
	*type = STORAGE_CMAP_UINT32;
	return STORAGE_CS_OK;
}

static void
reset_fixture(void)
{
	memset(&storage_api, 0, sizeof(storage_api));
	storage_api.initialize = fixture_initialize;
	storage_api.finalize = fixture_finalize;
	storage_api.trackstart = fixture_track;
	storage_api.dispatch = fixture_dispatch;
	storage_api.getquorate = fixture_quorate;
	storage_api.cmap_initialize = fixture_cmap_initialize;
	storage_api.cmap_finalize = fixture_finalize;
	storage_api.cmap_get = fixture_cmap_get;
	storage_connected = false;
	storage_quorum_handle = storage_cmap_handle = 0;
	memset(&storage_notified, 0, sizeof(storage_notified));
	cluster_storage_quorum_nodes = "0:11,1:12";
	cluster_storage_quorum_cluster = "quorum-fixture";
	quorate = notified_quorate = 1;
	notify_enabled = true;
	tracking_enabled = false;
	api_failed = changed_after_notify = false;
	notification_mismatch = 0;
	bad_key = NULL;
	finalize_count = track_count = 0;
	memset(shared_settings, 0, sizeof(shared_settings));
	shared_settings[0].name = "cluster.storage_quorum_nodes";
	shared_settings[1].name = "cluster.storage_quorum_cluster";
	shared_settings[0].vartype = shared_settings[1].vartype = PGC_STRING;
	shared_settings[0].status = shared_settings[1].status = GUC_SHARED_FILE;
}

UT_TEST(test_exact_profile_and_current_component)
{
	ClusterStorageQuorumView view;

	reset_fixture();
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
	UT_ASSERT_EQ(view.ring_sequence, 8);
	UT_ASSERT_EQ(view.members[0], 3);
	UT_ASSERT_EQ(track_count, 1);
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
	UT_ASSERT_EQ(track_count, 2);
}

UT_TEST(test_cached_callback_cannot_renew_current_permission)
{
	ClusterStorageQuorumView view;

	reset_fixture();
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
	notify_enabled = false;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_UNAVAILABLE);
	UT_ASSERT_EQ(view.members[0], 0);
}

UT_TEST(test_loss_disconnects_without_retaining_success)
{
	ClusterStorageQuorumView view;

	reset_fixture();
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
	api_failed = true;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_UNAVAILABLE);
	UT_ASSERT_EQ(finalize_count, 2);
	UT_ASSERT(!storage_connected);
	api_failed = false;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
}

UT_TEST(test_both_quorum_observations_and_matching_ring_are_required)
{
	ClusterStorageQuorumView view;

	reset_fixture();
	quorate = 0;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_NOT_QUORATE);
	quorate = 1;
	notified_quorate = 0;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_NOT_QUORATE);
	notified_quorate = 1;
	changed_after_notify = true;
	cluster_storage_corosync_sample(&view);
	UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_UNAVAILABLE);
	UT_ASSERT_EQ(view.members[0], 0);
}

UT_TEST(test_configuration_failure_revokes_previously_valid_observation)
{
	const char *keys[] = { "totem.cluster_name",
						   "quorum.provider",
						   "quorum.device.model",
						   "quorum.device.net.algorithm",
						   "quorum.device.net.tls",
						   "quorum.device.votes",
						   "quorum.expected_votes",
						   "quorum.two_node",
						   "quorum.auto_tie_breaker",
						   "quorum.last_man_standing",
						   "runtime.votequorum.this_node_id",
						   "nodelist.node.1.nodeid",
						   "nodelist.node.0.quorum_votes" };
	Size i;

	for (i = 0; i < lengthof(keys); i++) {
		ClusterStorageQuorumView view;

		reset_fixture();
		cluster_storage_corosync_sample(&view);
		UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
		bad_key = keys[i];
		cluster_storage_corosync_sample(&view);
		UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_CONFIGURATION);
		UT_ASSERT_EQ(view.members[0], 0);
		UT_ASSERT(!storage_connected);
	}
}

UT_TEST(test_local_settings_cannot_supply_storage_authority)
{
	Size i;

	for (i = 0; i < lengthof(shared_settings); i++) {
		ClusterStorageQuorumView view;

		reset_fixture();
		cluster_storage_corosync_sample(&view);
		UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_READY);
		shared_settings[i].status = 0;
		cluster_storage_corosync_sample(&view);
		UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_CONFIGURATION);
		UT_ASSERT_EQ(view.members[0], 0);
		UT_ASSERT(!storage_connected);
	}
}

UT_TEST(test_old_quorum_after_new_nodelist_cannot_renew_permission)
{
	ClusterStorageQuorumView view;
	ClusterStorageQuorumState state;
	unsigned mismatch;

	reset_fixture();
	cluster_storage_quorum_attach(&state, true);
	for (mismatch = 1; mismatch <= 3; mismatch++) {
		notification_mismatch = 0;
		cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), 1000000);
		UT_ASSERT(cluster_storage_quorum_allows_node(1));
		notification_mismatch = mismatch;
		cluster_storage_corosync_sample(&view);
		UT_ASSERT_EQ(view.reason, CLUSTER_STORAGE_QUORUM_UNAVAILABLE);
		UT_ASSERT_EQ(view.members[0], 0);
		cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), 1000000);
		UT_ASSERT(!cluster_storage_quorum_allows_node(0));
		UT_ASSERT(!cluster_storage_quorum_allows_node(1));
	}
	notification_mismatch = 0;
	cluster_storage_quorum_refresh(cluster_storage_quorum_now_us(), 1000000);
	UT_ASSERT(cluster_storage_quorum_allows_members(3, 0));
	cluster_storage_quorum_attach(NULL, false);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(test_exact_profile_and_current_component);
	UT_RUN(test_cached_callback_cannot_renew_current_permission);
	UT_RUN(test_loss_disconnects_without_retaining_success);
	UT_RUN(test_both_quorum_observations_and_matching_ring_are_required);
	UT_RUN(test_configuration_failure_revokes_previously_valid_observation);
	UT_RUN(test_local_settings_cannot_supply_storage_authority);
	UT_RUN(test_old_quorum_after_new_nodelist_cannot_renew_permission);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
