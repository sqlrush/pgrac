/* Minimal native lifecycle observations, not startup/serving permission.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include "cluster/cluster_control_observe.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster_control_bootstrap_private.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/resowner.h"

static void
observe_writer(StringInfo out, const ClusterControlRootIdentity *writer)
{
	appendStringInfo(out,
					 "{\"node\":%d,\"thread\":%u,\"incarnation\":" UINT64_FORMAT
					 ",\"wal_generation\":" UINT64_FORMAT ",\"boot\":\"%016" INT64_MODIFIER "x\"}",
					 writer->origin_node_id, writer->origin_thread_id,
					 writer->origin_owner_incarnation, writer->root_lineage_seq,
					 writer->origin_owner_incarnation);
}

static char *
observe_json(const ClusterControlBootstrapSnapshot *snapshot, const char *phase)
{
	StringInfoData out;
	bool comma = false;

	initStringInfo(&out);
	appendStringInfo(&out, "{\"system_identifier\":\"" UINT64_FORMAT "\",\"root_digest\":\"",
					 snapshot->binding.system_identifier);
	for (unsigned i = 0; i < sizeof(snapshot->root_sha256); i++)
		appendStringInfo(&out, "%02x", snapshot->root_sha256[i]);
	appendStringInfo(&out,
					 "\",\"generation\":" UINT64_FORMAT ",\"root_format\":%u,\"database_state\":%u,"
					 "\"database_incarnation\":" UINT64_FORMAT
					 ",\"root_phase\":\"%s\",\"members\":[",
					 snapshot->root_sequence, snapshot->root_format_version,
					 snapshot->database_state, snapshot->binding.database_incarnation, phase);
	for (unsigned node = 0; node < PGRAC_CONTROL_BINDING_MAX_NODES; node++) {
		if ((snapshot->config.identity.configured[node / 64] & (UINT64_C(1) << (node % 64))) == 0)
			continue;
		appendStringInfo(&out, "%s%u", comma ? "," : "", node);
		comma = true;
	}
	appendStringInfoString(&out, "],\"writer\":");
	observe_writer(&out, &snapshot->thread);
	appendStringInfo(&out,
					 ",\"writer_lifecycle\":%u,\"checkpoint_lsn\":\"%X/%X\","
					 "\"final_checkpoint\":%s}",
					 snapshot->writer_lifecycle, LSN_FORMAT_ARGS(snapshot->control.checkPoint),
					 strcmp(phase, "CLOSED") == 0 ? "true" : "false");
	return out.data;
}

char *
cluster_control_observe_writer_json(void)
{
	ClusterWalSourceRef current, predecessor, after, restart_after;
	ResourceXGateSnapshot gate, gate_after;
	uint64 epoch, generation, generation_after;
	StringInfoData out;

	/* This path is also used by the ordinary debug dump: memory-only, no
	 * authority-changing readiness accessor, ROOT I/O or retained-WAL scan. */
	if (!cluster_enabled || !cluster_shared_config || !IsUnderPostmaster
		|| MyBackendType != B_BACKEND || CritSectionCount != 0
		|| !cluster_wal_thread_current_v2_ref(&current)
		|| !cluster_wal_thread_restart_v2_ref(&predecessor)
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate) || gate.phase != RESOURCE_X_GATE_OPEN
		|| cluster_resource_x_writer_path_snapshot(&generation) != RESOURCE_X_WRITER_TARGET
		|| generation == 0 || (epoch = cluster_epoch_get_current()) == 0
		|| !cluster_wal_thread_current_v2_ref(&after)
		|| memcmp(&after, &current, sizeof(after)) != 0
		|| !cluster_wal_thread_restart_v2_ref(&restart_after)
		|| memcmp(&restart_after, &predecessor, sizeof(predecessor)) != 0
		|| !cluster_pcm_lock_resource_x_gate_snapshot(&gate_after)
		|| memcmp(&gate, &gate_after, sizeof(gate)) != 0
		|| cluster_resource_x_writer_path_snapshot(&generation_after) != RESOURCE_X_WRITER_TARGET
		|| generation != generation_after || epoch != cluster_epoch_get_current())
		return NULL;
	initStringInfo(&out);
	appendStringInfo(&out,
					 "{\"epoch\":" UINT64_FORMAT ",\"resource_x_formation\":" UINT64_FORMAT
					 ",\"r4_generation\":" UINT64_FORMAT ",\"writer\":",
					 epoch, gate.formation, generation);
	observe_writer(&out, &current.claim.identity);
	out.data[--out.len] = '\0';
	appendStringInfoString(&out, ",\"predecessor\":");
	observe_writer(&out, &predecessor.claim.identity);
	appendStringInfoString(&out, "}}");
	return out.data;
}

void
ClusterControlObserveMain(int argc, char **argv)
{
	ClusterControlBootstrapObservation observation = { 0 };
	ClusterControlRootResult result;
	char *end, *json;
	unsigned long node;
	bool closed, open;

	if (argc != 6 || argv[5][0] < '0' || argv[5][0] > '9')
		goto usage;
	errno = 0;
	node = strtoul(argv[5], &end, 10);
	if (errno != 0 || *end != '\0' || node >= PGRAC_CONTROL_BINDING_MAX_NODES)
		goto usage;
	/* Like the original standalone creator, establish the process-local
	 * hash/error resource owner. No config file, shared memory or startup. */
	InitializeGUCOptions();
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "control metadata observation");
	result = cluster_control_bootstrap_read(argv[2], argv[3], argv[4], (uint32)node, &observation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "could not observe selected writer: result=%d\n", (int)result);
		exit(1);
	}
	/* Full normal stop preserves the declared participants for its final
	 * receipt exchange. CLOSED is its durable state, not an empty bitmap. */
	closed
		= observation.snapshot.root_format_version == 3
		  && observation.snapshot.activation_state == CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		  && observation.snapshot.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED
		  && observation.snapshot.writer_lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
		  && observation.snapshot.control.state == DB_SHUTDOWNED
		  && observation.snapshot.control.checkPoint
				 == observation.snapshot.control.checkPointCopy.redo
		  && observation.required.pending_sources == 0
		  && memcmp(observation.snapshot.serving, observation.snapshot.config.identity.configured,
					sizeof(observation.snapshot.serving))
				 == 0;
	open = observation.snapshot.root_format_version == 3
		   && observation.snapshot.activation_state == CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		   && observation.snapshot.database_state == CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		   && observation.snapshot.writer_lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		   && observation.required.pending_sources == 0
		   && memcmp(observation.snapshot.serving, observation.snapshot.config.identity.configured,
					 sizeof(observation.snapshot.serving))
				  == 0;
	json = observe_json(&observation.snapshot, closed ? "CLOSED" : open ? "OPEN" : "OTHER");
	puts(json);
	pfree(json);
	pfree(observation.config_bytes);
	exit(0);
usage:
	fprintf(stderr, "usage: postgres --pgrac-observe-writer PGDATA SHARED_ROOT WAL_ROOT NODE\n");
	exit(1);
}
