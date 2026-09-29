/*-------------------------------------------------------------------------
 *
 * cluster_config_channels.c
 *    Configuration prefix exchanges owned by original LMON/LMS processes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/backend/cluster/cluster_config_channels.c
 * NOTES
 *    PGRAC-original bounded native-family coordination. All exported symbols
 *    use the cluster_ prefix. Reports do not grant DATA or configuration use.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "cluster/cluster_config_channels.h"
#include "cluster/cluster_cssd.h"
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_sf_dep.h"
#include "miscadmin.h"
#include "storage/proc.h"
#include "utils/memutils.h"

enum { CHANNEL_CANCEL = 0, CHANNEL_ARM = 1, CHANNEL_EXCHANGE = 2 };
enum { CHANNEL_PENDING = 1, CHANNEL_ARMED = 2, CHANNEL_COMPLETE = 3, CHANNEL_INVALID = 4 };

typedef struct ChannelLocal {
	ClusterConfigChannelsCommand command;
	ClusterConfigChannelOwner owner;
	ClusterConfigPrefixExchange peer[CLUSTER_MAX_NODES];
	ClusterConfigChannelReport report;
	int index;
	bool active;
} ChannelLocal;

static ChannelLocal channel_local;

static bool
channel_member(const uint64 set[2], unsigned node)
{
	return node < CLUSTER_MAX_NODES && (set[node / 64] & (UINT64CONST(1) << (node % 64))) != 0;
}

static int
channel_index(int role, int aux)
{
	if (role == B_LMON && aux == LmonProcess)
		return 0;
	if (role == B_LMS && aux == LmsProcess)
		return 1;
	if (role == B_LMS_WORKER && aux >= LmsWorker1Process && aux <= LmsWorker7Process)
		return 2 + aux - LmsWorker1Process;
	return -1;
}

static bool
channel_owner_read(int procno, ClusterConfigChannelOwner *out, int *index, uint64 *sequence)
{
	ClusterSharedConfigRegistration value;
	PGPROC *proc;
	if (ProcGlobal == NULL || procno < 0 || (uint32)procno >= ProcGlobal->allProcCount)
		return false;
	proc = &ProcGlobal->allProcs[procno];
	*sequence = pg_atomic_read_u64(&proc->cluster_config.sequence);
	if (proc->pid <= 0 || (*sequence & 1) != 0
		|| !cluster_shared_config_registration_read(&proc->cluster_config, &value)
		|| value.pid != proc->pid || value.registration == 0 || !value.observed
		|| *sequence != pg_atomic_read_u64(&proc->cluster_config.sequence))
		return false;
	*index = channel_index(value.role, value.aux_type);
	if (*index < 0)
		return false;
	out->registration = value.registration;
	out->pid = value.pid;
	out->procno = procno;
	return true;
}

static bool
channel_self(ClusterConfigChannelOwner *out, int *index)
{
	uint64 sequence;
	return IsUnderPostmaster && cluster_enabled && cluster_shared_config && MyProc != NULL
		   && channel_owner_read(MyProc->pgprocno, out, index, &sequence)
		   && MyProc == &ProcGlobal->allProcs[out->procno] && out->pid == MyProcPid
		   && *index == channel_index(MyBackendType, MyAuxProcType);
}

/* The original LMON is the only command writer. Readers neither acquire a
 * child-owned lock nor retry an interrupted copy as a successful command. */
static bool
channel_command_read(ClusterConfigChannelsBoard *board, int32 lmon_pid,
					 ClusterConfigChannelsCommand *out, uint64 *sequence)
{
	ClusterConfigChannelOwner actual;
	uint64 native_sequence;
	int index;
	if (board == NULL || lmon_pid <= 0)
		return false;
	*sequence = pg_atomic_read_u64(&board->command_sequence);
	if (*sequence == 0 || (*sequence & 1) != 0)
		return false;
	pg_read_barrier();
	*out = board->command;
	pg_read_barrier();
	return *sequence == pg_atomic_read_u64(&board->command_sequence) && out->serial != 0
		   && out->owner.pid == lmon_pid
		   && (out->phase == CHANNEL_ARM || out->phase == CHANNEL_EXCHANGE) && out->workers > 0
		   && out->workers < CLUSTER_CONFIG_CHANNEL_OWNERS
		   && channel_owner_read(out->owner.procno, &actual, &index, &native_sequence) && index == 0
		   && memcmp(&actual, &out->owner, sizeof(actual)) == 0;
}

static bool
channel_command_write(ClusterConfigChannelsBoard *board,
					  const ClusterConfigChannelsCommand *command, uint64 expected)
{
	if ((expected & 1) != 0 || expected > PG_UINT64_MAX - 2
		|| !pg_atomic_compare_exchange_u64(&board->command_sequence, &expected, expected + 1))
		return false;
	pg_write_barrier();
	board->command = *command;
	pg_write_barrier();
	pg_atomic_write_u64(&board->command_sequence, expected + 2);
	return true;
}

static bool
channel_members_current(const ClusterConfigChannelsCommand *command)
{
	ClusterR4MembershipSnapshot members;
	ClusterConfigMembersKey key;
	if (!cluster_reconfig_lmon_snapshot_r4_membership(&members)
		|| !cluster_config_members_make_key(&command->key.ref, &members, &key)
		|| memcmp(&key, &command->key, sizeof(key)) != 0)
		return false;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node)
		if (channel_member(key.required, node)
			&& members.admitted_incarnation[node] != command->incarnation[node])
			return false;
	return true;
}

/*
 * cluster_config_channels_arm -- Retain an exact, locally selected episode.
 *
 * Inputs: CF-selected reference and the coordinator's fresh global episode.
 * Returns: false on busy, changed native/member identity or missing substrate.
 * Side effects: one bounded family publication; no sends or service changes.
 * Author: SqlRush <sqlrush@gmail.com>
 */
bool
cluster_config_channels_arm(const ClusterSharedConfigRef *selected, const uint8 episode[16])
{
	ClusterConfigChannelsCommand command = { 0 }, prior;
	ClusterR4MembershipSnapshot members;
	ClusterConfigChannelsBoard *board;
	uint64 sequence, observed;
	int32 pid;
	int index;
	uint8 nonzero = 0;
	if (selected == NULL || episode == NULL || !cluster_lms_enabled || cluster_lms_workers < 1
		|| cluster_lms_workers >= CLUSTER_CONFIG_CHANNEL_OWNERS || cluster_normal_stop_requested()
		|| !channel_self(&command.owner, &index) || index != 0
		|| (board = cluster_shared_config_delivery_channels(&pid)) == NULL || pid != MyProcPid
		|| !cluster_reconfig_lmon_snapshot_r4_membership(&members)
		|| !cluster_config_members_make_key(selected, &members, &command.key))
		return false;
	for (unsigned i = 0; i < 16; ++i)
		nonzero |= episode[i];
	if (nonzero == 0)
		return false;
	memcpy(command.episode, episode, 16);
	command.workers = cluster_lms_workers;
	command.phase = CHANNEL_ARM;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node)
		if (channel_member(command.key.required, node))
			command.incarnation[node] = members.admitted_incarnation[node];
	sequence = pg_atomic_read_u64(&board->command_sequence);
	if (channel_command_read(board, pid, &prior, &observed)) {
		command.serial = prior.serial;
		command.phase = prior.phase;
		return memcmp(&command, &prior, sizeof(command)) == 0;
	}
	if ((sequence & 1) != 0 || sequence > PG_UINT64_MAX - 2)
		return false;
	command.serial = sequence + 2;
	return channel_command_write(board, &command, sequence);
}

void
cluster_config_channels_cancel(void)
{
	ClusterConfigChannelsCommand cancel = { 0 };
	ClusterConfigChannelsBoard *board;
	uint64 sequence;
	int32 pid;
	int index;
	if (!channel_self(&cancel.owner, &index) || index != 0
		|| (board = cluster_shared_config_delivery_channels(&pid)) == NULL || pid != MyProcPid)
		return;
	sequence = pg_atomic_read_u64(&board->command_sequence);
	if ((sequence & 1) != 0 || sequence > PG_UINT64_MAX - 2)
		return;
	cancel.serial = sequence + 2;
	(void)channel_command_write(board, &cancel, sequence);
}

static void
channel_report_write(ClusterConfigChannelsBoard *board)
{
	ClusterConfigChannelSlot *slot = &board->slots[channel_local.index];
	uint64 sequence = pg_atomic_read_u64(&slot->sequence);
	if ((sequence & 1) != 0)
		return;
	if (sequence > PG_UINT64_MAX - 2) {
		pg_atomic_write_u64(&slot->sequence, PG_UINT64_MAX);
		return;
	}
	pg_atomic_write_u64(&slot->sequence, sequence + 1);
	pg_write_barrier();
	slot->report = channel_local.report;
	pg_write_barrier();
	pg_atomic_write_u64(&slot->sequence, sequence + 2);
}

static void
channel_invalid(ClusterConfigChannelsBoard *board)
{
	if (!channel_local.active)
		return;
	channel_local.report.phase = CHANNEL_INVALID;
	memset(channel_local.report.complete, 0, sizeof(channel_local.report.complete));
	if (board != NULL)
		channel_report_write(board);
}

static bool
channel_same_episode(const ClusterConfigChannelsCommand *a, const ClusterConfigChannelsCommand *b)
{
	/* Only the monotonic ARM -> EXCHANGE stage may change in one serial. */
	return a->serial == b->serial && a->workers == b->workers
		   && memcmp(&a->owner, &b->owner, sizeof(a->owner)) == 0
		   && memcmp(&a->key, &b->key, sizeof(a->key)) == 0
		   && memcmp(a->episode, b->episode, 16) == 0
		   && memcmp(a->incarnation, b->incarnation, sizeof(a->incarnation)) == 0;
}

void
cluster_config_channels_stream_retiring(int32 peer)
{
	int32 pid;
	/* The native close/rebind owns this private exchange. Do not wait until
	 * the next tick: a different process can read our completed report now.
	 * Inherited parent state never writes the parent's report from a child. */
	if (!channel_local.active || channel_local.owner.pid != MyProcPid
		|| (peer != -1
			&& (peer < 0 || peer >= CLUSTER_MAX_NODES || !channel_local.peer[peer].active)))
		return;
	channel_invalid(cluster_shared_config_delivery_channels(&pid));
}

/* Exact maintenance traffic creates no ordinary module responsibility.
 * This is not a serving exemption: the original parser/handler/capability
 * checks still run. In particular, generic CONTROL and chunk wrappers are
 * never exempt, even when an inner payload resembles a configuration frame. */
static bool
channel_maintenance_frame(int32 peer, const ClusterICEnvelope *env, Size payload_length,
						  bool sending)
{
	if (env == NULL || peer < 0 || peer >= CLUSTER_MAX_NODES
		|| !channel_member(channel_local.command.key.required, peer)
		|| env->magic != PGRAC_IC_ENVELOPE_MAGIC || env->version != PGRAC_IC_ENVELOPE_VERSION_V1
		|| env->source_node_id != (uint32)(sending ? cluster_node_id : peer)
		|| env->dest_node_id != (uint32)(sending ? peer : cluster_node_id)
		|| env->epoch != channel_local.command.key.epoch || env->payload_length != payload_length)
		return false;
	if (env->msg_type == PGRAC_IC_MSG_HEARTBEAT)
		return payload_length == 0;
	if (channel_local.index != 0)
		return env->msg_type == PGRAC_IC_MSG_CONFIG_PREFIX_DATA
			   && payload_length == CLUSTER_CONFIG_PREFIX_BYTES;
	switch (env->msg_type) {
	case PGRAC_IC_MSG_CSSD_HEARTBEAT:
		return payload_length == sizeof(ClusterCssdHeartbeatPayload);
	case PGRAC_IC_MSG_PEER_CAPS_REPLY:
		return payload_length == PGRAC_IC_HELLO_BYTES;
	case PGRAC_IC_MSG_CONFIG_MEMBERS:
		return payload_length == CLUSTER_CONFIG_MEMBERS_BYTES;
	case PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL:
		return payload_length == CLUSTER_CONFIG_PREFIX_BYTES;
	default:
		return false;
	}
}

static bool
channel_activity_relevant(void)
{
	/* No shared-memory touch or parsing on the ordinary inactive fast path.
 * A fork must never publish over its parent's retained report. */
	return channel_local.active && channel_local.owner.pid == MyProcPid
		   && channel_local.report.phase != CHANNEL_INVALID;
}

/* Called BEFORE a new native send can write or enqueue bytes. A subsequent
 * NOT_ADMITTED may conservatively dirty this attempt: the original producer
 * still owns the work. Completion of an already queued tail is separate. */
void
cluster_config_channels_sending(int32 peer, const void *bytes, Size length)
{
	ClusterICEnvelope env;
	int32 pid;
	if (!channel_activity_relevant())
		return;
	if (bytes != NULL && length >= sizeof(env)) {
		memcpy(&env, bytes, sizeof(env));
		if (channel_maintenance_frame(peer, &env, length - sizeof(env), true))
			return;
	}
	channel_invalid(cluster_shared_config_delivery_channels(&pid));
}

/* Original native verification precedes this callback. Invalidate before
 * handing a whole frame to a handler or admitting its chunk responsibility,
 * not at the next owner tick. Never consume, refuse or mutate the frame. */
void
cluster_config_channels_received(int32 peer, const ClusterICEnvelope *env, Size payload_length)
{
	int32 pid;
	if (!channel_activity_relevant() || channel_maintenance_frame(peer, env, payload_length, false))
		return;
	channel_invalid(cluster_shared_config_delivery_channels(&pid));
}

static void
channel_refresh(ClusterConfigChannelsBoard *board)
{
	bool ready = true, complete = true;
	if (channel_local.report.phase == CHANNEL_INVALID) {
		channel_report_write(board);
		return;
	}
	memset(channel_local.report.armed, 0, sizeof(channel_local.report.armed));
	memset(channel_local.report.complete, 0, sizeof(channel_local.report.complete));
	for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer) {
		ClusterConfigPrefixExchange *exchange = &channel_local.peer[peer];
		uint64 bit = UINT64CONST(1) << (peer % 64);
		if (peer == (uint32)cluster_node_id
			|| !channel_member(channel_local.command.key.required, peer))
			continue;
		if (!exchange->active) {
			ready = complete = false;
			continue;
		}
		if (cluster_config_prefix_complete(exchange))
			channel_local.report.complete[peer / 64] |= bit;
		else
			complete = false;
		if (exchange->failed) {
			channel_invalid(board);
			return;
		}
		channel_local.report.armed[peer / 64] |= bit;
		channel_local.report.capability_generation[peer] = exchange->capability_generation;
	}
	channel_local.report.phase
		= !ready ? CHANNEL_PENDING
				 : (complete && channel_local.command.phase == CHANNEL_EXCHANGE ? CHANNEL_COMPLETE
																				: CHANNEL_ARMED);
	channel_report_write(board);
}

static bool
channel_local_current(const ClusterConfigChannelsCommand *command)
{
	ClusterConfigChannelOwner actual;
	int index;
	return channel_local.active && channel_self(&actual, &index) && index == channel_local.index
		   && memcmp(&actual, &channel_local.owner, sizeof(actual)) == 0
		   && channel_same_episode(command, &channel_local.command);
}

static void
channel_run(ClusterConfigChannelsBoard *board, const ClusterConfigChannelsCommand *command)
{
	if (channel_local.report.phase == CHANNEL_INVALID)
		return;
	channel_local.command.phase = command->phase;
	for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer) {
		ClusterConfigPrefixExchange *exchange = &channel_local.peer[peer];
		if (peer == (uint32)cluster_node_id || !channel_member(command->key.required, peer))
			continue;
		if (!exchange->active) {
			if (command->phase != CHANNEL_ARM) {
				channel_invalid(board);
				return;
			}
			(void)cluster_config_prefix_begin(exchange, &command->key, command->episode, peer,
											  command->incarnation[cluster_node_id],
											  command->incarnation[peer]);
		}
	}
	channel_refresh(board);
	if (channel_local.report.phase == CHANNEL_INVALID || command->phase != CHANNEL_EXCHANGE)
		return;
	for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer)
		if (channel_local.peer[peer].active)
			cluster_config_prefix_poll(&channel_local.peer[peer]);
	channel_refresh(board);
}

/* Called by original loops outside their retained module-work brackets.
 * No sleep, assignment hook, resource cancellation or global permission. */
void
cluster_config_channels_tick(void)
{
	ClusterConfigChannelsBoard *board;
	ClusterConfigChannelsCommand command;
	ClusterConfigChannelOwner actual;
	uint64 sequence;
	int32 pid;
	int index;
	board = cluster_shared_config_delivery_channels(&pid);
	if (!channel_self(&actual, &index) || !channel_command_read(board, pid, &command, &sequence)
		|| index > (int)command.workers || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES) {
		/* A forked process must not publish through an inherited owner's slot. */
		if (channel_local.active && channel_local.owner.pid == MyProcPid)
			channel_invalid(board);
		memset(&channel_local, 0, sizeof(channel_local));
		return;
	}
	if (!channel_local.active || channel_local.owner.pid != MyProcPid
		|| channel_local.command.serial != command.serial) {
		memset(&channel_local, 0, sizeof(channel_local));
		channel_local.command = command;
		channel_local.owner = actual;
		channel_local.index = index;
		channel_local.active = true;
		channel_local.report.owner = actual;
		channel_local.report.serial = command.serial;
		channel_local.report.phase = CHANNEL_PENDING;
		channel_report_write(board);
	}
	if (!channel_local_current(&command) || cluster_normal_stop_requested()
		|| (index == 0 && !channel_members_current(&command))) {
		channel_invalid(board);
		return;
	}
	PG_TRY();
	{
		channel_run(board, &command);
		if (sequence != pg_atomic_read_u64(&board->command_sequence))
			channel_invalid(board);
	}
	PG_CATCH();
	{
		channel_invalid(board);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

void
cluster_config_channels_ingress(const ClusterICEnvelope *env, const void *bytes)
{
	ClusterConfigChannelsBoard *board;
	ClusterConfigChannelsCommand command;
	uint64 sequence;
	int32 pid;
	if (env == NULL || env->source_node_id >= CLUSTER_MAX_NODES || cluster_normal_stop_requested()
		|| (board = cluster_shared_config_delivery_channels(&pid)) == NULL
		|| !channel_command_read(board, pid, &command, &sequence)
		|| !channel_local_current(&command) || channel_local.report.phase == CHANNEL_INVALID)
		return;
	PG_TRY();
	{
		(void)cluster_config_prefix_ingress(&channel_local.peer[env->source_node_id], env, bytes);
		channel_refresh(board);
		if (sequence != pg_atomic_read_u64(&board->command_sequence))
			channel_invalid(board);
	}
	PG_CATCH();
	{
		channel_invalid(board);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

static bool
channel_report_read(ClusterConfigChannelSlot *slot, ClusterConfigChannelReport *out,
					uint64 *sequence)
{
	*sequence = pg_atomic_read_u64(&slot->sequence);
	if (*sequence == 0 || (*sequence & 1) != 0)
		return false;
	pg_read_barrier();
	*out = slot->report;
	pg_read_barrier();
	return *sequence == pg_atomic_read_u64(&slot->sequence);
}

/* Collect actual native allocations, not just the last PID left in a slot.
 * All sequences are rechecked after the census. An observation has no future
 * lifetime: the retained coordinator must revalidate before each transition. */
static bool
channel_census(ClusterConfigChannelsBoard *board, const ClusterConfigChannelsCommand *command,
			   ClusterConfigChannelsCensus *out)
{
	int32 *pids, *after;
	uint64 report_sequence[CLUSTER_CONFIG_CHANNEL_OWNERS] = { 0 };
	uint64 native_sequence[CLUSTER_CONFIG_CHANNEL_OWNERS] = { 0 };
	int procno[CLUSTER_CONFIG_CHANNEL_OWNERS] = { 0 };
	uint32 seen = 0;
	uint32 capabilities[CLUSTER_MAX_NODES] = { 0 };
	bool capability_seen[CLUSTER_MAX_NODES] = { false };
	bool valid = false;
	Size count = ProcGlobal->allProcCount;
	if (count == 0 || count > MaxAllocSize / (2 * sizeof(*pids)))
		return false;
	pids = palloc(2 * count * sizeof(*pids));
	after = pids + count;
	if (!ProcConfigSnapshotPids(pids, count))
		goto done;
	out->required = (1u << (command->workers + 1)) - 1;
	out->serial = command->serial;
	for (uint32 p = 0; p < count; ++p) {
		ClusterSharedConfigRegistration reg;
		ClusterConfigChannelReport report;
		ClusterConfigChannelOwner actual;
		uint64 seq;
		int index;
		if (pids[p] == 0)
			continue;
		if (pids[p] < 0
			|| !cluster_shared_config_registration_read(&ProcGlobal->allProcs[p].cluster_config,
														&reg))
			goto done;
		if (reg.role != B_LMON && reg.role != B_LMS && reg.role != B_LMS_WORKER)
			continue;
		if (!channel_owner_read(p, &actual, &index, &seq) || index > (int)command->workers
			|| actual.pid != pids[p] || (seen & (1u << index)) != 0
			|| !channel_report_read(&board->slots[index], &report, &report_sequence[index])
			|| report.serial != command->serial
			|| memcmp(&report.owner, &actual, sizeof(actual)) != 0)
			goto done;
		seen |= 1u << index;
		native_sequence[index] = seq;
		procno[index] = p;
		/* CONTROL-owned HELLO generations can change independently of a DATA
		 * worker tick. Sample every armed peer, never accept an old report by
		 * PID/sequence alone. All current owners must name the same generation. */
		for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer) {
			uint32 generation = report.capability_generation[peer];
			if (!channel_member(report.armed, peer))
				continue;
			if ((capability_seen[peer] && capabilities[peer] != generation)
				|| !cluster_sf_peer_capability_generation_matches(
					peer, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1, generation))
				goto done;
			capabilities[peer] = generation;
			capability_seen[peer] = true;
		}
		if (report.phase == CHANNEL_ARMED || report.phase == CHANNEL_COMPLETE)
			out->armed |= 1u << index;
		if (report.phase == CHANNEL_COMPLETE)
			out->complete |= 1u << index;
		if (report.phase == CHANNEL_INVALID)
			out->invalid |= 1u << index;
	}
	if (seen != out->required || !ProcConfigSnapshotPids(after, count)
		|| memcmp(pids, after, count * sizeof(*pids)) != 0)
		goto done;
	for (unsigned i = 0; i <= command->workers; ++i)
		if (report_sequence[i] != pg_atomic_read_u64(&board->slots[i].sequence)
			|| native_sequence[i]
				   != pg_atomic_read_u64(&ProcGlobal->allProcs[procno[i]].cluster_config.sequence))
			goto done;
	for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; ++peer)
		if (capability_seen[peer]
			&& !cluster_sf_peer_capability_generation_matches(
				peer, PGRAC_IC_HELLO_CAP_CONFIG_PREFIX_V1, capabilities[peer]))
			goto done;
	valid = true;
done:
	pfree(pids);
	return valid;
}

bool
cluster_config_channels_observe(ClusterConfigChannelsCensus *out)
{
	ClusterConfigChannelsBoard *board;
	ClusterConfigChannelsCommand command;
	ClusterConfigChannelOwner self;
	ClusterConfigChannelsCensus census = { 0 };
	uint64 sequence;
	int32 pid;
	int index;
	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	if (!channel_self(&self, &index) || index != 0
		|| (board = cluster_shared_config_delivery_channels(&pid)) == NULL || pid != MyProcPid
		|| !channel_command_read(board, pid, &command, &sequence)
		|| !channel_members_current(&command) || !channel_census(board, &command, &census)
		|| !channel_members_current(&command)
		|| sequence != pg_atomic_read_u64(&board->command_sequence))
		return false;
	*out = census;
	return true;
}

bool
cluster_config_channels_exchange(void)
{
	ClusterConfigChannelsBoard *board;
	ClusterConfigChannelsCommand command;
	ClusterConfigChannelsCensus census;
	uint64 sequence;
	int32 pid;
	if (cluster_normal_stop_requested()
		|| (board = cluster_shared_config_delivery_channels(&pid)) == NULL || pid != MyProcPid
		|| !channel_command_read(board, pid, &command, &sequence)
		|| !cluster_config_channels_observe(&census) || census.serial != command.serial
		|| census.armed != census.required || census.invalid != 0)
		return false;
	if (command.phase == CHANNEL_EXCHANGE)
		return sequence == pg_atomic_read_u64(&board->command_sequence);
	command.phase = CHANNEL_EXCHANGE;
	return channel_command_write(board, &command, sequence);
}

void
cluster_config_channels_register(void)
{
	const ClusterICMsgTypeInfo control = {
		.msg_type = PGRAC_IC_MSG_CONFIG_PREFIX_CONTROL,
		.name = "config_prefix_control",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMON,
		.handler = cluster_config_channels_ingress,
		.plane = CLUSTER_IC_PLANE_CONTROL,
	};
	const ClusterICMsgTypeInfo data = {
		.msg_type = PGRAC_IC_MSG_CONFIG_PREFIX_DATA,
		.name = "config_prefix_data",
		.allowed_producer_mask = CLUSTER_IC_PRODUCER_LMS_DATA,
		.handler = cluster_config_channels_ingress,
		.plane = CLUSTER_IC_PLANE_DATA,
	};
	cluster_ic_register_msg_type(&control);
	cluster_ic_register_msg_type(&data);
}
