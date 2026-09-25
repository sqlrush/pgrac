/*-------------------------------------------------------------------------
 * PGRAC: early native preparation from exact root-selected objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
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
	pfree(state->before.config_bytes);
	pfree(state->after.config_bytes);
	for (size_t i = 0; i < lengthof(state->paths); i++)
		pfree(state->paths[i]);
	pfree(state);
}
