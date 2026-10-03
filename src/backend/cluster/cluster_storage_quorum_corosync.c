/*-------------------------------------------------------------------------
 *
 * cluster_storage_quorum_corosync.c
 *    Read the local Corosync quorum service without invoking a shell.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/backend/cluster/cluster_storage_quorum_corosync.c
 *
 * NOTES
 *    PGRAC-original adapter; all exported symbols use the cluster_ prefix.
 *    Uses the public libquorum model-v1 and libcmap ABIs. A missing library,
 *    unsupported profile or disconnected service never supplies eligibility.
 *    Spec: spec-s9p2-06-online-membership.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dlfcn.h>

#include "cluster/cluster_guc.h"
#include "cluster/cluster_storage_quorum.h"
#include "utils/guc_tables.h"

/* Public Corosync ABI types, kept private to this optional runtime adapter. */
typedef struct StorageCorosyncRing {
	uint32 node;
	uint64 sequence;
} StorageCorosyncRing;

typedef struct StorageCorosyncModel {
	int model;
	void (*quorum_notify)(uint64, uint32, StorageCorosyncRing, uint32, const uint32 *);
	void (*members_notify)(uint64, StorageCorosyncRing, uint32, const uint32 *, uint32,
						   const uint32 *, uint32, const uint32 *);
} StorageCorosyncModel;

typedef struct StorageCorosyncApi {
	void *quorum_lib;
	void *cmap_lib;
	int (*initialize)(uint64 *, int, const void *, uint32 *, void *);
	int (*finalize)(uint64);
	int (*trackstart)(uint64, unsigned int);
	int (*dispatch)(uint64, int);
	int (*getquorate)(uint64, int *);
	int (*cmap_initialize)(uint64 *);
	int (*cmap_finalize)(uint64);
	int (*cmap_get)(uint64, const char *, void *, size_t *, int *);
} StorageCorosyncApi;

#define STORAGE_CS_OK 1
#define STORAGE_CS_NOT_EXIST 12
#define STORAGE_CS_TRACK_CURRENT 1
#define STORAGE_CS_DISPATCH_ALL 2
#define STORAGE_CMAP_UINT8 2
#define STORAGE_CMAP_UINT32 6
#define STORAGE_CMAP_STRING 11

static StorageCorosyncApi storage_api;
static uint64 storage_quorum_handle;
static uint64 storage_cmap_handle;
static bool storage_connected;
static uint32 storage_local_id;
static uint32 storage_node_map[CLUSTER_MAX_NODES];
static ClusterStorageQuorumView storage_notified;
static ClusterStorageQuorumView storage_members_notified;

static void
storage_quorum_notify(uint64 handle, uint32 quorate, StorageCorosyncRing ring, uint32 count,
					  const uint32 *members)
{
	if (handle != storage_quorum_handle)
		return;
	(void)cluster_storage_quorum_decode_component(&storage_notified, ring.node, ring.sequence,
												  quorate, members, count, storage_local_id,
												  cluster_node_id, storage_node_map);
}

/* Keep both notifications: during a provider sync the membership view can
 * advance before the quorum callback. Neither callback can authorize alone. */
static void
storage_members_notify(uint64 handle, StorageCorosyncRing ring, uint32 count, const uint32 *members,
					   uint32 joined_count pg_attribute_unused(),
					   const uint32 *joined pg_attribute_unused(),
					   uint32 left_count pg_attribute_unused(),
					   const uint32 *left pg_attribute_unused())
{
	if (handle != storage_quorum_handle)
		return;
	(void)cluster_storage_quorum_decode_component(
		&storage_members_notified, ring.node, ring.sequence, 1, members, count, storage_local_id,
		cluster_node_id, storage_node_map);
}

static bool
storage_load_api(void)
{
	if (storage_api.initialize != NULL)
		return true;
	memset(&storage_api, 0, sizeof(storage_api));
	storage_api.quorum_lib = dlopen("libquorum.so.5", RTLD_NOW | RTLD_LOCAL);
	storage_api.cmap_lib = dlopen("libcmap.so.4", RTLD_NOW | RTLD_LOCAL);
	if (storage_api.quorum_lib == NULL || storage_api.cmap_lib == NULL)
		goto failed;
#define STORAGE_LOAD(field, library, symbol)                                                       \
	do {                                                                                           \
		*(void **)(&storage_api.field) = dlsym(storage_api.library, symbol);                       \
		if (storage_api.field == NULL)                                                             \
			goto failed;                                                                           \
	} while (0)
	STORAGE_LOAD(initialize, quorum_lib, "quorum_model_initialize");
	STORAGE_LOAD(finalize, quorum_lib, "quorum_finalize");
	STORAGE_LOAD(trackstart, quorum_lib, "quorum_trackstart");
	STORAGE_LOAD(dispatch, quorum_lib, "quorum_dispatch");
	STORAGE_LOAD(getquorate, quorum_lib, "quorum_getquorate");
	STORAGE_LOAD(cmap_initialize, cmap_lib, "cmap_initialize");
	STORAGE_LOAD(cmap_finalize, cmap_lib, "cmap_finalize");
	STORAGE_LOAD(cmap_get, cmap_lib, "cmap_get");
#undef STORAGE_LOAD
	return true;
failed:
	if (storage_api.quorum_lib != NULL)
		dlclose(storage_api.quorum_lib);
	if (storage_api.cmap_lib != NULL)
		dlclose(storage_api.cmap_lib);
	memset(&storage_api, 0, sizeof(storage_api));
	return false;
}

static bool
storage_string(const char *key, const char *expected)
{
	char value[256];
	size_t size = sizeof(value);
	int type = 0;

	return expected != NULL && *expected != '\0'
		   && storage_api.cmap_get(storage_cmap_handle, key, value, &size, &type) == STORAGE_CS_OK
		   && type == STORAGE_CMAP_STRING && size == strlen(expected) + 1 && size <= sizeof(value)
		   && memcmp(value, expected, size) == 0;
}

static bool
storage_uint32(const char *key, uint32 *out)
{
	size_t size = sizeof(*out);
	int type = 0;

	return storage_api.cmap_get(storage_cmap_handle, key, out, &size, &type) == STORAGE_CS_OK
		   && type == STORAGE_CMAP_UINT32 && size == sizeof(*out);
}

static bool
storage_absent_or_number(const char *key, uint32 expected, bool allow_value)
{
	uint32 value = 0;
	size_t size = sizeof(value);
	int type = 0;
	int result = storage_api.cmap_get(storage_cmap_handle, key, &value, &size, &type);

	if (result == STORAGE_CS_NOT_EXIST)
		return true;
	if (!allow_value || result != STORAGE_CS_OK)
		return false;
	if (type == STORAGE_CMAP_UINT8 && size == 1)
		return *(uint8 *)&value == expected;
	return type == STORAGE_CMAP_UINT32 && size == sizeof(value) && value == expected;
}

static bool
storage_setting_is_shared(const char *name)
{
	struct config_generic *record = find_option(name, false, true, DEBUG1);

	/* Necessary source provenance, not a configuration-generation receipt.
	 * Existing shared-config admission still owns the applied image proof. */
	return record != NULL && record->vartype == PGC_STRING
		   && (record->status & GUC_SHARED_FILE) != 0;
}

static bool
storage_profile_current(void)
{
	uint64 configured[2] = { 0, 0 };
	uint64 seen[2] = { 0, 0 };
	int node;
	int index;

	if (ClusterConfShmem == NULL || !storage_setting_is_shared("cluster.storage_quorum_nodes")
		|| !storage_setting_is_shared("cluster.storage_quorum_cluster"))
		return false;
	for (node = 0; node < CLUSTER_MAX_NODES; node++)
		if (cluster_conf_lookup_node(node) != NULL)
			configured[node / 64] |= UINT64_C(1) << (node % 64);
	if (!cluster_storage_quorum_parse_nodes(cluster_storage_quorum_nodes, configured,
											storage_node_map)
		|| !storage_string("totem.cluster_name", cluster_storage_quorum_cluster)
		|| !storage_string("quorum.provider", "corosync_votequorum")
		|| !storage_string("quorum.device.model", "net")
		|| !storage_string("quorum.device.net.algorithm", "lms")
		|| !storage_string("quorum.device.net.tls", "required")
		|| !storage_absent_or_number("quorum.device.votes", 0, false)
		|| !storage_absent_or_number("quorum.expected_votes", 0, false)
		|| !storage_absent_or_number("quorum.two_node", 0, true)
		|| !storage_absent_or_number("quorum.auto_tie_breaker", 0, true)
		|| !storage_absent_or_number("quorum.last_man_standing", 0, true)
		|| !storage_uint32("runtime.votequorum.this_node_id", &storage_local_id)
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| storage_local_id != storage_node_map[cluster_node_id])
		return false;
	for (index = 0; index <= CLUSTER_MAX_NODES; index++) {
		char key[80];
		uint32 id = 0;
		size_t size = sizeof(id);
		int type = 0;
		int result;

		snprintf(key, sizeof(key), "nodelist.node.%d.nodeid", index);
		result = storage_api.cmap_get(storage_cmap_handle, key, &id, &size, &type);
		if (result == STORAGE_CS_NOT_EXIST)
			return seen[0] == configured[0] && seen[1] == configured[1];
		if (index == CLUSTER_MAX_NODES || result != STORAGE_CS_OK || type != STORAGE_CMAP_UINT32
			|| size != sizeof(id) || id == 0)
			return false;
		for (node = 0; node < CLUSTER_MAX_NODES; node++)
			if (storage_node_map[node] == id)
				break;
		if (node == CLUSTER_MAX_NODES || (seen[node / 64] & (UINT64_C(1) << (node % 64))))
			return false;
		seen[node / 64] |= UINT64_C(1) << (node % 64);
		snprintf(key, sizeof(key), "nodelist.node.%d.quorum_votes", index);
		if (!storage_absent_or_number(key, 1, true))
			return false;
	}
	return false;
}

static void
storage_disconnect(void)
{
	if (storage_quorum_handle != 0)
		(void)storage_api.finalize(storage_quorum_handle);
	if (storage_cmap_handle != 0)
		(void)storage_api.cmap_finalize(storage_cmap_handle);
	storage_quorum_handle = storage_cmap_handle = 0;
	storage_connected = false;
	memset(&storage_notified, 0, sizeof(storage_notified));
	memset(&storage_members_notified, 0, sizeof(storage_members_notified));
}

/*
 * cluster_storage_corosync_sample -- Read current storage eligibility.
 * Inputs: local, immutable database profile and Corosync service state.
 * Returns: a complete observation, or an explicit ineligible reason in out.
 * Side Effects: QVOTEC-only local library IPC; never changes votes or services.
 * Author: SqlRush <sqlrush@gmail.com>
 */
void
cluster_storage_corosync_sample(ClusterStorageQuorumView *out)
{
	int quorate = 0;

	memset(out, 0, sizeof(*out));
	if (!storage_load_api())
		return;
	if (!storage_connected) {
		StorageCorosyncModel model = { 1, storage_quorum_notify, storage_members_notify };
		uint32 quorum_type = 0;

		if (storage_api.cmap_initialize(&storage_cmap_handle) != STORAGE_CS_OK
			|| storage_api.initialize(&storage_quorum_handle, 1, &model, &quorum_type, NULL)
				   != STORAGE_CS_OK
			|| quorum_type != 1)
			goto failed;
		storage_connected = true;
	}
	if (!storage_profile_current()) {
		out->reason = CLUSTER_STORAGE_QUORUM_CONFIGURATION;
		goto failed;
	}
	/* CURRENT queues a complete nodelist/quorum pair before the reply.
	 * Do not retain a CHANGES subscription: a subsequent trackstart would
	 * return CS_ERR_EXIST. Every poll must obtain its own complete sample. */
	memset(&storage_notified, 0, sizeof(storage_notified));
	memset(&storage_members_notified, 0, sizeof(storage_members_notified));
	if (storage_api.trackstart(storage_quorum_handle, STORAGE_CS_TRACK_CURRENT) != STORAGE_CS_OK
		|| storage_api.getquorate(storage_quorum_handle, &quorate) != STORAGE_CS_OK
		|| storage_api.dispatch(storage_quorum_handle, STORAGE_CS_DISPATCH_ALL) != STORAGE_CS_OK)
		goto failed;
	if (quorate != 1 || storage_notified.reason == CLUSTER_STORAGE_QUORUM_NOT_QUORATE) {
		out->reason = CLUSTER_STORAGE_QUORUM_NOT_QUORATE;
		return;
	}
	if (storage_notified.reason != CLUSTER_STORAGE_QUORUM_READY
		|| storage_members_notified.reason != CLUSTER_STORAGE_QUORUM_READY
		|| storage_notified.ring_node != storage_members_notified.ring_node
		|| storage_notified.ring_sequence != storage_members_notified.ring_sequence
		|| memcmp(storage_notified.members, storage_members_notified.members,
				  sizeof(storage_notified.members))
			   != 0)
		return;
	*out = storage_notified;
	return;
failed:
	storage_disconnect();
}
