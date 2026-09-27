/*-------------------------------------------------------------------------
 * PGRAC: early native preparation from exact root-selected objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "cluster/cluster_guc.h"
#include "miscadmin.h"
#include "storage/bufpage.h"
#include "utils/resowner.h"

#include "cluster_control_bootstrap_private.h"

typedef struct BootstrapPreparation {
	char *paths[4];
	ClusterControlBootstrapObservation before;
	ClusterControlBootstrapObservation after;
	ClusterSharedConfigApplied applied;
} BootstrapPreparation;

/* Fixed-size postmaster-local observation. Not inherited writer authority. */
static bool bootstrap_prepared_valid;
static ClusterControlBootstrapPrepared bootstrap_prepared;
static char bootstrap_paths[4][MAXPGPATH];

void
cluster_control_bootstrap_native_inputs_require(const char *pgdata)
{
	ClusterControlRootResult result = cluster_control_bootstrap_native_inputs(pgdata);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		ereport(FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("native startup inputs require unsupported recovery"),
				 errdetail("Native input qualification result=%d. No input was removed.", result),
				 errhint("Preserve this directory. Qualify backup, replication or prepared state "
						 "before startup.")));
}

static void
bootstrap_policy_refuse(const char *message, const ClusterSharedConfigPolicyReport *report)
{
	ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg_internal("%s", message),
					errdetail("Configuration reason=%d node=%d parameter=%s.", report->reason,
							  report->node_id, report->name)));
}

static void
bootstrap_preparation_release(ResourceOwner owner, ResourceOwner saved_owner, bool success)
{
	bool top_level = saved_owner == NULL;
	/* Early startup has no transaction owner or database locks to transfer. */
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_BEFORE_LOCKS, success, top_level);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_LOCKS, success, top_level);
	ResourceOwnerRelease(owner, RESOURCE_RELEASE_AFTER_LOCKS, success, top_level);
	CurrentResourceOwner = saved_owner;
	ResourceOwnerDelete(owner);
}

void
cluster_control_bootstrap_prepare(const char *pgdata, const char *shared_root, const char *wal_root,
								  const char *undo_root, uint32 node_id, bool reset,
								  ClusterControlBootstrapPrepared *out)
{
	const char *inputs[] = { pgdata, shared_root, wal_root, undo_root };
	ResourceOwner saved_owner = CurrentResourceOwner;
	MemoryContext saved_context = CurrentMemoryContext;
	ResourceOwner owner;
	BootstrapPreparation *state;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (IsUnderPostmaster || IsBootstrapProcessingMode() || process_shared_preload_libraries_done)
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native bootstrap preparation requires early startup")));
	bootstrap_prepared_valid = false;
	memset(&bootstrap_prepared, 0, sizeof(bootstrap_prepared));
	if (out == NULL || node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("invalid native bootstrap preparation input")));
	for (size_t i = 0; i < lengthof(inputs); i++) {
		size_t len = inputs[i] != NULL ? strnlen(inputs[i], MAXPGPATH) : 0;
		if (len < 2 || len >= MAXPGPATH || inputs[i][0] != '/')
			ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("invalid native bootstrap preparation path")));
	}

	/* Native string assignment can free the independently selected inputs.
	 * Keep private copies, and never derive a new path from the object applied.
	 */
	state = palloc0(sizeof(*state));
	for (size_t i = 0; i < lengthof(inputs); i++)
		state->paths[i] = pstrdup(inputs[i]);
	owner = ResourceOwnerCreate(saved_owner, "native control bootstrap preparation");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		ClusterControlRootResult result;
		ClusterSharedConfigPolicyReport report;

		process_cluster_gucs();
		result = cluster_control_bootstrap_read(state->paths[0], state->paths[1], state->paths[2],
												node_id, &state->before);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native bootstrap observation failed"),
							errdetail("Root observation result=%d.", result)));
		if (cluster_shared_config_check_bootstrap(
				state->before.config_bytes, state->before.config_len,
				&state->before.snapshot.config, node_id, state->paths[1], state->paths[2],
				state->paths[3], &report)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			bootstrap_policy_refuse("native bootstrap profile is not applicable", &report);

		/* Native hooks must observe selected WAL geometry, not a compatibility
		 * image/default. This installs process-local state only, never admission. */
		XLogInstallBootstrapControlFile(&state->before.snapshot.control, reset);
		cluster_shared_config_apply_startup(state->before.config_bytes, state->before.config_len,
											&state->before.snapshot.config, node_id,
											&state->applied);
		XLogCompleteBootstrapControlFile();
		if (cluster_shared_config_check_recovery_capacity(&state->before.required, &report)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			bootstrap_policy_refuse("native bootstrap recovery capacity is insufficient", &report);

		/* Application hooks need not be reversible. A changed/unknown observation
		 * now exits this process; never loop and apply a different generation.
		 */
		result = cluster_control_bootstrap_read(state->paths[0], state->paths[1], state->paths[2],
												node_id, &state->after);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native bootstrap observation could not be revalidated"),
							errdetail("Root observation result=%d.", result)));
		if (state->before.snapshot.root_sequence != state->after.snapshot.root_sequence
			|| memcmp(state->before.snapshot.root_sha256, state->after.snapshot.root_sha256, 32)
				   != 0
			|| memcmp(&state->before.snapshot.binding, &state->after.snapshot.binding,
					  sizeof(state->before.snapshot.binding))
				   != 0)
			ereport(
				FATAL,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("native bootstrap observation changed during configuration application")));
		if (cluster_shared_config_check_bootstrap(
				state->after.config_bytes, state->after.config_len, &state->after.snapshot.config,
				node_id, state->paths[1], state->paths[2], state->paths[3], &report)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			bootstrap_policy_refuse("native bootstrap profile is not applicable", &report);
		if (cluster_shared_config_check_recovery_capacity(&state->after.required, &report)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			bootstrap_policy_refuse("native bootstrap recovery capacity is insufficient", &report);
		result = cluster_control_bootstrap_wal_startup_route(state->paths[0], state->paths[2],
															 &state->after.snapshot);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native bootstrap WAL routing is not exact"),
							errdetail("WAL routing result=%d.", result)));
		result = cluster_control_bootstrap_side_route(state->paths[0], state->paths[1], node_id);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native bootstrap side routing is not exact"),
							errdetail("Native side routing result=%d.", result)));
		cluster_control_bootstrap_native_inputs_require(state->paths[0]);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(saved_context);
		FlushErrorState();
		bootstrap_preparation_release(owner, saved_owner, false);
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native bootstrap preparation failed")));
	}
	PG_END_TRY();
	bootstrap_preparation_release(owner, saved_owner, true);
	out->snapshot = state->after.snapshot;
	out->required = state->after.required;
	out->applied = state->applied;
	bootstrap_prepared = *out;
	for (size_t i = 0; i < lengthof(state->paths); i++)
		strlcpy(bootstrap_paths[i], state->paths[i], MAXPGPATH);
	bootstrap_prepared_valid = true;
	pfree(state->before.config_bytes);
	pfree(state->after.config_bytes);
	for (size_t i = 0; i < lengthof(state->paths); i++)
		pfree(state->paths[i]);
	pfree(state);
}

void
cluster_control_bootstrap_wal_recheck(const char *pgdata, ClusterWalDurablePrefixRef *out)
{
	ClusterControlBootstrapObservation *fresh;
	ClusterControlRootResult result;
	ResourceOwner saved_owner = CurrentResourceOwner;
	MemoryContext saved_context = CurrentMemoryContext;
	ResourceOwner owner;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || pgdata == NULL || IsUnderPostmaster || !bootstrap_prepared_valid
		|| !cluster_shared_config || !cluster_enabled
		|| cluster_node_id != (int)bootstrap_prepared.applied.node_id
		|| cluster_shared_data_dir == NULL || cluster_wal_threads_dir == NULL
		|| strcmp(cluster_shared_data_dir, bootstrap_paths[1]) != 0
		|| strcmp(cluster_wal_threads_dir, bootstrap_paths[2]) != 0
		|| GetSystemIdentifier() != bootstrap_prepared.snapshot.thread.system_identifier
		|| wal_segment_size != (int)bootstrap_prepared.snapshot.control.xlog_seg_size)
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native WAL initialization has no exact bootstrap preparation")));
	fresh = palloc0(sizeof(*fresh));
	owner = ResourceOwnerCreate(saved_owner, "native WAL bootstrap recheck");
	CurrentResourceOwner = owner;
	PG_TRY();
	{
		result = cluster_control_bootstrap_read(pgdata, bootstrap_paths[1], bootstrap_paths[2],
												cluster_node_id, fresh);
		if (result != 0
			|| fresh->snapshot.root_sequence != bootstrap_prepared.snapshot.root_sequence
			|| memcmp(fresh->snapshot.root_sha256, bootstrap_prepared.snapshot.root_sha256, 32) != 0
			|| memcmp(&fresh->snapshot.binding, &bootstrap_prepared.snapshot.binding,
					  sizeof(fresh->snapshot.binding))
				   != 0)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native WAL bootstrap preparation changed"),
							errdetail("Root recheck result=%d.", result)));
		result = cluster_control_bootstrap_wal_startup_route(pgdata, bootstrap_paths[2],
															 &fresh->snapshot);
		if (result != 0)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native WAL bootstrap route changed"),
							errdetail("WAL routing result=%d.", result)));
		result = cluster_control_bootstrap_side_route(pgdata, bootstrap_paths[1], cluster_node_id);
		if (result != 0)
			ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("native side bootstrap route changed"),
							errdetail("Native side routing result=%d.", result)));
		cluster_control_bootstrap_native_inputs_require(pgdata);
	}
	PG_CATCH();
	{
		bootstrap_prepared_valid = false;
		MemoryContextSwitchTo(saved_context);
		FlushErrorState();
		bootstrap_preparation_release(owner, saved_owner, false);
		ereport(FATAL, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("native WAL bootstrap recheck failed")));
	}
	PG_END_TRY();
	bootstrap_preparation_release(owner, saved_owner, true);
	*out = fresh->snapshot.wal;
	pfree(fresh->config_bytes);
	pfree(fresh);
}
