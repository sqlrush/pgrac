/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_provider.c
 *	  Production provider registry, worker isolation, and terminal verifier.
 *
 * The test-only id is deliberately absent from the production registry.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_ipmi.h"
#include "pgrac_fenced_config.h"
#include "pgrac_fenced_journal.h"
#include "pgrac_fenced_provider.h"

#define PGRAC_FENCED_WORKER_MAGIC UINT32_C(0x50465731)
#define PGRAC_FENCED_WORKER_ACTUATE UINT32_C(1)
#define PGRAC_FENCED_WORKER_READBACK UINT32_C(2)
#define PGRAC_FENCED_WORKER_RESOLVE UINT32_C(3)
#define PGRAC_FENCED_WORKER_TERM_GRACE_NS UINT64_C(1000000000)

typedef struct PgracFencedResolvedIdentity
{
	uint8 target_uuid[16];
	int32 victim_node_id;
	uint32 reserved0;
	uint64 mapping_generation;
} PgracFencedResolvedIdentity;

typedef union PgracFencedWorkerPayload
{
	PgracFencedReadbackV1 readback;
	PgracFencedResolvedIdentity resolved;
} PgracFencedWorkerPayload;

typedef struct PgracFencedWorkerMessage
{
	uint32 magic;
	uint32 call_type;
	uint32 provider_result;
	int32 native_status;
	PgracFencedWorkerPayload payload;
} PgracFencedWorkerMessage;

StaticAssertDecl(sizeof(PgracFencedResolvedIdentity) == 32,
				 "provider resolved identity size changed");
StaticAssertDecl(sizeof(PgracFencedWorkerMessage) == 48,
				 "provider worker message size changed");

static uint64_t callback_deadline_mono_ns;
static const PgracFencedJournalRecordV1 *callback_record;
static const PgracFencedConfigV1 *callback_config;

const struct PgracFencedConfigV1 *
pgrac_fenced_provider_callback_config(void)
{
	return callback_config;
}

const struct PgracFencedJournalRecordV1 *
pgrac_fenced_provider_callback_record(void)
{
	return callback_record;
}

uint64_t
pgrac_fenced_provider_callback_deadline_mono_ns(void)
{
	return callback_deadline_mono_ns;
}

const PgracFencedProviderOpsV1 *
pgrac_fenced_provider_lookup(uint16 provider_id)
{
	if (provider_id == PGRAC_FENCED_PROVIDER_ID_IPMI_LANPLUS_V1)
		return pgrac_fenced_ipmi_provider_ops();
	return NULL;
}

bool
pgrac_fenced_provider_ops_valid(const PgracFencedProviderOpsV1 *ops,
							bool allow_test_only)
{
	size_t name_len;
	size_t i;

	if (ops == NULL || ops->abi_version != PGRAC_FENCED_PROVIDER_ABI_V1 ||
		ops->struct_size != sizeof(*ops) || ops->reserved0 != 0 ||
		ops->provider_id == PGRAC_FENCED_PROVIDER_ID_UNAVAILABLE ||
		ops->provider_id == UINT16_MAX ||
		(ops->provider_id == PGRAC_FENCED_PROVIDER_ID_TEST_ONLY &&
		 !allow_test_only) ||
		ops->provider_name == NULL || ops->resolve == NULL ||
		ops->actuate_off == NULL || ops->readback == NULL ||
		ops->actuate_on == NULL || ops->shutdown == NULL)
		return false;
	name_len = strlen(ops->provider_name);
	if (name_len == 0 || name_len > 31)
		return false;
	for (i = 0; i < name_len; i++)
	{
		unsigned char ch = (unsigned char) ops->provider_name[i];

		if (ch < 0x21 || ch > 0x7e)
			return false;
	}
	return true;
}

static PgracFencedProviderTerminal
provider_result_terminal(PgracFencedProviderResult result)
{
	switch (result)
	{
		case PGRAC_FENCED_PROVIDER_OK:
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		case PGRAC_FENCED_PROVIDER_REJECTED:
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		case PGRAC_FENCED_PROVIDER_UNAVAILABLE:
		case PGRAC_FENCED_PROVIDER_CONFIG_ERROR:
			return PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE;
		case PGRAC_FENCED_PROVIDER_PENDING:
		case PGRAC_FENCED_PROVIDER_UNKNOWN:
		case PGRAC_FENCED_PROVIDER_IO_ERROR:
		default:
			return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
}

static PgracFencedProviderTerminal
classify_common(PgracFencedProviderResult result,
				const uint8 expected_target_uuid[16],
				const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal = provider_result_terminal(result);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (expected_target_uuid == NULL || readback == NULL)
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNAVAILABLE;
	if (readback->reserved0 != 0 ||
		readback->state < PGRAC_FENCED_TARGET_OFF ||
		readback->state > PGRAC_FENCED_TARGET_UNKNOWN ||
		readback->io_drain_state > PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	if (memcmp(readback->observed_target_uuid, expected_target_uuid, 16) != 0)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
}

PgracFencedProviderTerminal
pgrac_fenced_provider_classify_recovery(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal =
		classify_common(result, expected_target_uuid, readback);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (readback->state == PGRAC_FENCED_TARGET_OFF)
	{
		if (readback->io_drain_state == PGRAC_FENCED_IO_DRAIN_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		if (readback->io_drain_state ==
			PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
	if (readback->state == PGRAC_FENCED_TARGET_ON)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
}

PgracFencedProviderTerminal
pgrac_fenced_provider_classify_rejoin_on(
	PgracFencedProviderResult result, const uint8 expected_target_uuid[16],
	const PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderTerminal terminal =
		classify_common(result, expected_target_uuid, readback);

	if (terminal != PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN)
		return terminal;
	if (readback->state == PGRAC_FENCED_TARGET_ON)
	{
		if (readback->io_drain_state == PGRAC_FENCED_IO_DRAIN_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_PROVEN;
		if (readback->io_drain_state ==
			PGRAC_FENCED_IO_DRAIN_NOT_DRAINED)
			return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
		return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
	}
	if (readback->state == PGRAC_FENCED_TARGET_OFF)
		return PGRAC_FENCED_PROVIDER_TERMINAL_REJECTED;
	return PGRAC_FENCED_PROVIDER_TERMINAL_UNKNOWN;
}

static bool
monotonic_now_ns(uint64_t *out)
{
	struct timespec now;

	if (out == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 ||
		now.tv_sec < 0)
		return false;
	*out = (uint64_t) now.tv_sec * UINT64_C(1000000000) +
		(uint64_t) now.tv_nsec;
	return true;
}

static bool
target_valid(const PgracFencedTargetV1 *target)
{
	size_t i;
	bool nonzero = false;

	if (target == NULL || target->reserved0 != 0 ||
		target->victim_node_id < 0 || target->victim_node_id >= 128 ||
		target->mapping_generation == 0 || target->adapter_config_len > 4096 ||
		(target->adapter_config_len > 0 && target->adapter_config == NULL))
		return false;
	for (i = 0; i < sizeof(target->target_uuid); i++)
		nonzero = nonzero || target->target_uuid[i] != 0;
	return nonzero;
}

static bool
resolved_target_exact(const PgracFencedTargetV1 *configured,
				  const PgracFencedTargetV1 *resolved)
{
	return configured != NULL && resolved != NULL &&
		memcmp(resolved->target_uuid, configured->target_uuid,
			sizeof(configured->target_uuid)) == 0 &&
		resolved->victim_node_id == configured->victim_node_id &&
		resolved->reserved0 == 0 &&
		resolved->mapping_generation == configured->mapping_generation &&
		resolved->adapter_config == configured->adapter_config &&
		resolved->adapter_config_len == configured->adapter_config_len;
}

static bool
resolved_identity_exact(const PgracFencedTargetV1 *configured,
					const PgracFencedResolvedIdentity *resolved)
{
	return configured != NULL && resolved != NULL &&
		memcmp(resolved->target_uuid, configured->target_uuid,
			sizeof(configured->target_uuid)) == 0 &&
		resolved->victim_node_id == configured->victim_node_id &&
		resolved->reserved0 == 0 &&
		resolved->mapping_generation == configured->mapping_generation;
}

static int
deadline_poll_timeout_ms(uint64_t deadline_mono_ns)
{
	uint64_t now;
	uint64_t remaining;
	uint64_t milliseconds;

	if (!monotonic_now_ns(&now) || now >= deadline_mono_ns)
		return 0;
	remaining = deadline_mono_ns - now;
	milliseconds = (remaining + UINT64_C(999999)) / UINT64_C(1000000);
	if (milliseconds > INT_MAX)
		return INT_MAX;
	return (int) milliseconds;
}

static bool
process_group_gone(pid_t pgid)
{
	int rc;

	do
	{
		rc = kill(-pgid, 0);
	} while (rc != 0 && errno == EINTR);
	return rc != 0 && errno == ESRCH;
}

static bool
process_group_owner_prepare(void)
{
#ifdef __linux__
	return prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) == 0;
#else
	return true;
#endif
}

static void
reap_process_group_children(pid_t pgid, bool *leader_reaped)
{
	int status;
	pid_t waited;

	if (!*leader_reaped)
	{
		do
		{
			waited = waitpid(pgid, &status, WNOHANG);
		} while (waited < 0 && errno == EINTR);
		if (waited == pgid)
			*leader_reaped = true;
	}
	for (;;)
	{
		waited = waitpid(-pgid, &status, WNOHANG);
		if (waited > 0)
		{
			if (waited == pgid)
				*leader_reaped = true;
			continue;
		}
		if (waited < 0 && errno == EINTR)
			continue;
		return;
	}
}

static bool
wait_worker_exit(pid_t pid, bool leader_reaped)
{
	uint64_t stop_deadline;
	uint64_t now;
	struct timespec pause = {0, 10000000};

	if (!process_group_gone(pid))
		(void) kill(-pid, SIGTERM);
	else if (!leader_reaped)
		(void) kill(pid, SIGTERM);
	if (!monotonic_now_ns(&now) ||
		now > UINT64_MAX - PGRAC_FENCED_WORKER_TERM_GRACE_NS)
		stop_deadline = UINT64_MAX;
	else
		stop_deadline = now + PGRAC_FENCED_WORKER_TERM_GRACE_NS;
	for (;;)
	{
		reap_process_group_children(pid, &leader_reaped);
		if (leader_reaped && process_group_gone(pid))
			return true;
		if (!monotonic_now_ns(&now) || now >= stop_deadline)
			break;
		(void) nanosleep(&pause, NULL);
	}
	if (!process_group_gone(pid))
		(void) kill(-pid, SIGKILL);
	if (!leader_reaped && process_group_gone(pid))
		(void) kill(pid, SIGKILL);
	if (!monotonic_now_ns(&now) ||
		now > UINT64_MAX - PGRAC_FENCED_WORKER_TERM_GRACE_NS)
		stop_deadline = UINT64_MAX;
	else
		stop_deadline = now + PGRAC_FENCED_WORKER_TERM_GRACE_NS;
	for (;;)
	{
		(void) kill(-pid, SIGKILL);
		reap_process_group_children(pid, &leader_reaped);
		if (leader_reaped && process_group_gone(pid))
			return true;
		if (!monotonic_now_ns(&now) || now >= stop_deadline)
			return false;
		(void) nanosleep(&pause, NULL);
	}
}

/* PGRAC: owner must already have appended this record; encoding is not proof of fsync. */
static bool
execution_record_matches(const PgracFencedProviderOpsV1 *ops, uint32 call_type, bool turn_on,
						 const PgracFencedTargetV1 *target,
						 const PgracFencedJournalRecordV1 *record)
{
	uint8 frame[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	size_t length;
	int32 node;
	bool acquire;
	uint16 opcode = 0;

	if (call_type == PGRAC_FENCED_WORKER_RESOLVE)
		return record == NULL;
	if (record == NULL)
		return ops->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	if (record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE
		|| record->provider_id != ops->provider_id
		|| record->provider_abi_version != ops->abi_version
		|| record->mapping_generation != target->mapping_generation
		|| memcmp(record->intent.target_uuid, target->target_uuid, 16) != 0
		|| !pgrac_fenced_journal_frame_encode(record, frame, sizeof(frame), &length))
		return false;
	acquire = record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE;
	node = acquire ? record->intent.request.acquire.need.victim_node_id
				   : record->intent.request.rejoin.old_node_id;
	if (node != target->victim_node_id)
		return false;
	if (call_type != PGRAC_FENCED_WORKER_ACTUATE)
		return true;
	if (record->record_kind != PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED)
		return false;
	if (!acquire)
		opcode = record->intent.request.rejoin.opcode;
	if (turn_on)
		return !acquire && opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
			   && record->target_state == PGRAC_FENCED_JOURNAL_TARGET_NONE;
	if (acquire)
		return record->target_state == PGRAC_FENCED_JOURNAL_TARGET_NONE;
	/* Compensating OFF is recorded under the same owned ON/REFRESH need. */
	return (opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON
			|| opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON)
		   && record->target_state == PGRAC_FENCED_TARGET_OFF;
}

/* PGRAC: the owner supplies its accepted, already-validated snapshot. Never
 * reload it from disk here or let a record use another snapshot's target.
 */
static bool
execution_config_matches(const PgracFencedProviderOpsV1 *ops, const PgracFencedTargetV1 *target,
						 const PgracFencedJournalRecordV1 *record,
						 const PgracFencedConfigV1 *config, const uint8 digest[32])
{
	static const uint8 zero[32] = { 0 };
	const PgracFencedNodeConfigV1 *node;

	if (config == NULL)
		return digest == NULL && record == NULL
			   && ops->provider_id != PGRAC_FENCED_PROVIDER_ID_PACEMAKER_LIBVIRT_V1;
	if (digest == NULL || memcmp(digest, zero, 32) == 0
		|| (config->format_version != 1 && config->format_version != 2)
		|| config->provider_id != ops->provider_id || config->provider_abi != ops->abi_version
		|| config->system_identifier == 0
		|| config->mapping_generation != target->mapping_generation)
		return false;
	node = &config->nodes[target->victim_node_id];
	if (!node->present || memcmp(node->target_uuid, target->target_uuid, 16) != 0
		|| node->adapter_data_len > sizeof(node->adapter_data)
		|| node->adapter_data_len != target->adapter_config_len
		|| (node->adapter_data_len != 0
			&& memcmp(node->adapter_data, target->adapter_config, node->adapter_data_len) != 0))
		return false;
	if (record == NULL)
		return true; /* Resolve or a legacy format-1 callback; phase is checked separately. */
	return config->format_version == 2
		   && record->intent.system_identifier == config->system_identifier
		   && memcmp(record->semantic_config_digest, digest, 32) == 0
		   && memcmp(record->intent.protected_set_digest, node->protected_set_digest, 32) == 0;
}

static PgracFencedProviderWorkerResult
run_worker(const PgracFencedProviderOpsV1 *ops, bool allow_test_only, uint32 call_type,
		   bool turn_on, const PgracFencedTargetV1 *target,
		   const PgracFencedJournalRecordV1 *record, const PgracFencedConfigV1 *config,
		   const uint8 config_digest[32], uint64_t deadline_mono_ns,
		   PgracFencedWorkerMessage *message)
{
	struct pollfd poll_fd;
	uint64_t now;
	int pipe_fds[2] = {-1, -1};
	int status;
	int poll_rc;
	ssize_t got;
	bool group_clean;
	pid_t pid;
	pid_t waited;

	if (message == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	memset(message, 0, sizeof(*message));
	message->provider_result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	if (!pgrac_fenced_provider_ops_valid(ops, allow_test_only) || !target_valid(target)
		|| !execution_record_matches(ops, call_type, turn_on, target, record)
		|| !execution_config_matches(ops, target, record, config, config_digest)
		|| (call_type != PGRAC_FENCED_WORKER_ACTUATE && call_type != PGRAC_FENCED_WORKER_READBACK
			&& call_type != PGRAC_FENCED_WORKER_RESOLVE)
		|| !monotonic_now_ns(&now) || now >= deadline_mono_ns || !process_group_owner_prepare()
		|| pipe(pipe_fds) != 0)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	(void) fcntl(pipe_fds[0], F_SETFD, FD_CLOEXEC);
	(void) fcntl(pipe_fds[1], F_SETFD, FD_CLOEXEC);
	pid = fork();
	if (pid < 0)
	{
		(void) close(pipe_fds[0]);
		(void) close(pipe_fds[1]);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	if (pid == 0)
	{
		PgracFencedWorkerMessage child_message;
		PgracFencedProviderResult provider_result;
		PgracFencedTargetV1 child_resolved;
		PgracFencedJournalRecordV1 child_record;

		(void) close(pipe_fds[0]);
		if (setpgid(0, 0) != 0)
			_exit(125);
		memset(&child_message, 0, sizeof(child_message));
		memset(&child_resolved, 0, sizeof(child_resolved));
		child_message.magic = PGRAC_FENCED_WORKER_MAGIC;
		child_message.call_type = call_type;
		callback_deadline_mono_ns = deadline_mono_ns;
		/* PGRAC: fork-local only; legacy workers cannot inherit an owner. */
		callback_record = NULL;
		callback_config = config; /* Private fork snapshot; never stored in the parent. */
		if (record != NULL)
		{
			child_record = *record;
			callback_record = &child_record;
		}
		if (call_type == PGRAC_FENCED_WORKER_ACTUATE)
		{
			provider_result = turn_on ?
				ops->actuate_on(target, deadline_mono_ns,
							&child_message.native_status) :
				ops->actuate_off(target, deadline_mono_ns,
							 &child_message.native_status);
		}
		else if (call_type == PGRAC_FENCED_WORKER_READBACK)
			provider_result = ops->readback(target, deadline_mono_ns,
										&child_message.payload.readback);
		else
		{
			provider_result = ops->resolve(target, &child_resolved,
				&child_message.native_status);
			if (provider_result == PGRAC_FENCED_PROVIDER_OK &&
				!resolved_target_exact(target, &child_resolved))
				provider_result = PGRAC_FENCED_PROVIDER_REJECTED;
			if (provider_result == PGRAC_FENCED_PROVIDER_OK)
			{
				memcpy(child_message.payload.resolved.target_uuid,
					child_resolved.target_uuid,
					sizeof(child_message.payload.resolved.target_uuid));
				child_message.payload.resolved.victim_node_id =
					child_resolved.victim_node_id;
				child_message.payload.resolved.mapping_generation =
					child_resolved.mapping_generation;
			}
		}
		callback_deadline_mono_ns = 0;
		callback_record = NULL;
		callback_config = NULL;
		child_message.provider_result = (uint32) provider_result;
		got = write(pipe_fds[1], &child_message, sizeof(child_message));
		(void) close(pipe_fds[1]);
		_exit(got == sizeof(child_message) ? 0 : 125);
	}
	(void) close(pipe_fds[1]);
	pipe_fds[1] = -1;
	if (setpgid(pid, pid) != 0 && errno != EACCES && errno != ESRCH)
	{
		(void) close(pipe_fds[0]);
		(void) wait_worker_exit(pid, false);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	poll_fd.fd = pipe_fds[0];
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	for (;;)
	{
		poll_rc = poll(&poll_fd, 1, deadline_poll_timeout_ms(deadline_mono_ns));
		if (poll_rc >= 0 || errno != EINTR)
			break;
	}
	if (poll_rc == 0)
	{
		(void) close(pipe_fds[0]);
		(void) wait_worker_exit(pid, false);
		return PGRAC_FENCED_PROVIDER_WORKER_TIMEOUT;
	}
	if (poll_rc < 0)
	{
		(void) close(pipe_fds[0]);
		(void) wait_worker_exit(pid, false);
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	}
	do
	{
		got = read(pipe_fds[0], message, sizeof(*message));
	} while (got < 0 && errno == EINTR);
	(void) close(pipe_fds[0]);
	do
	{
		waited = waitpid(pid, &status, 0);
	} while (waited < 0 && errno == EINTR);
	group_clean = process_group_gone(pid);
	if (!group_clean)
		group_clean = wait_worker_exit(pid, waited == pid);
	if (got != sizeof(*message) || waited != pid || !WIFEXITED(status) ||
		WEXITSTATUS(status) != 0 || message->magic != PGRAC_FENCED_WORKER_MAGIC ||
		message->call_type != call_type ||
		message->provider_result > PGRAC_FENCED_PROVIDER_IO_ERROR ||
		!group_clean)
	{
		memset(message, 0, sizeof(*message));
		message->provider_result = PGRAC_FENCED_PROVIDER_UNKNOWN;
		return PGRAC_FENCED_PROVIDER_WORKER_CRASHED;
	}
	return PGRAC_FENCED_PROVIDER_WORKER_OK;
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_resolve(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *configured, uint64_t deadline_mono_ns,
	PgracFencedProviderResult *result, PgracFencedTargetV1 *resolved,
	int32 *native_status)
{
	return pgrac_fenced_provider_worker_resolve_configured(ops, allow_test_only, configured, NULL,
														   NULL, deadline_mono_ns, result, resolved,
														   native_status);
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_resolve_configured(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
	const PgracFencedTargetV1 *configured, const struct PgracFencedConfigV1 *config,
	const uint8 config_digest[32], uint64_t deadline_mono_ns, PgracFencedProviderResult *result,
	PgracFencedTargetV1 *resolved, int32 *native_status)
{
	PgracFencedWorkerMessage message;
	PgracFencedProviderWorkerResult worker_result;

	if (result == NULL || resolved == NULL || native_status == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	*result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	memset(resolved, 0, sizeof(*resolved));
	*native_status = 0;
	worker_result = run_worker(ops, allow_test_only, PGRAC_FENCED_WORKER_RESOLVE, false, configured,
							   NULL, config, config_digest, deadline_mono_ns, &message);
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK)
	{
		*result = (PgracFencedProviderResult) message.provider_result;
		*native_status = message.native_status;
		if (*result == PGRAC_FENCED_PROVIDER_OK &&
			resolved_identity_exact(configured, &message.payload.resolved))
			*resolved = *configured;
		else if (*result == PGRAC_FENCED_PROVIDER_OK)
			*result = PGRAC_FENCED_PROVIDER_REJECTED;
	}
	else if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE)
		*result = PGRAC_FENCED_PROVIDER_UNKNOWN;
	return worker_result;
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_actuate(const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
									 bool turn_on, const PgracFencedTargetV1 *target,
									 uint64_t deadline_mono_ns, PgracFencedProviderResult *result,
									 int32 *native_status)
{
	return pgrac_fenced_provider_worker_actuate_owned(ops, allow_test_only, turn_on, target, NULL,
													  NULL, NULL, deadline_mono_ns, result,
													  native_status);
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_actuate_owned(const PgracFencedProviderOpsV1 *ops,
										   bool allow_test_only, bool turn_on,
										   const PgracFencedTargetV1 *target,
										   const PgracFencedJournalRecordV1 *record,
										   const struct PgracFencedConfigV1 *config,
										   const uint8 config_digest[32], uint64_t deadline_mono_ns,
										   PgracFencedProviderResult *result, int32 *native_status)
{
	PgracFencedWorkerMessage message;
	PgracFencedProviderWorkerResult worker_result;

	if (result == NULL || native_status == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	*result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	*native_status = 0;
	worker_result = run_worker(ops, allow_test_only, PGRAC_FENCED_WORKER_ACTUATE, turn_on, target,
							   record, config, config_digest, deadline_mono_ns, &message);
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK) {
		*result = (PgracFencedProviderResult)message.provider_result;
		*native_status = message.native_status;
	} else if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE)
		*result = PGRAC_FENCED_PROVIDER_UNKNOWN;
	return worker_result;
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback(const PgracFencedProviderOpsV1 *ops, bool allow_test_only,
									  const PgracFencedTargetV1 *target, uint64_t deadline_mono_ns,
									  PgracFencedProviderResult *result,
									  PgracFencedReadbackV1 *readback)
{
	return pgrac_fenced_provider_worker_readback_owned(ops, allow_test_only, target, NULL, NULL,
													   NULL, deadline_mono_ns, result, readback);
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback_owned(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only, const PgracFencedTargetV1 *target,
	const PgracFencedJournalRecordV1 *record, const struct PgracFencedConfigV1 *config,
	const uint8 config_digest[32], uint64_t deadline_mono_ns, PgracFencedProviderResult *result,
	PgracFencedReadbackV1 *readback)
{
	PgracFencedWorkerMessage message;
	PgracFencedProviderWorkerResult worker_result;

	if (result == NULL || readback == NULL)
		return PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE;
	*result = PGRAC_FENCED_PROVIDER_UNAVAILABLE;
	memset(readback, 0, sizeof(*readback));
	worker_result = run_worker(ops, allow_test_only, PGRAC_FENCED_WORKER_READBACK, false, target,
							   record, config, config_digest, deadline_mono_ns, &message);
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_OK) {
		*result = (PgracFencedProviderResult)message.provider_result;
		*readback = message.payload.readback;
	} else if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_UNAVAILABLE)
		*result = PGRAC_FENCED_PROVIDER_UNKNOWN;
	return worker_result;
}

static bool
provider_readback_retryable(PgracFencedProviderWorkerResult worker_result,
							PgracFencedProviderResult result, const PgracFencedReadbackV1 *readback)
{
	if (worker_result == PGRAC_FENCED_PROVIDER_WORKER_CRASHED)
		return true;
	if (worker_result != PGRAC_FENCED_PROVIDER_WORKER_OK)
		return false;
	if (result == PGRAC_FENCED_PROVIDER_PENDING || result == PGRAC_FENCED_PROVIDER_UNKNOWN
		|| result == PGRAC_FENCED_PROVIDER_IO_ERROR)
		return true;
	return result == PGRAC_FENCED_PROVIDER_OK && readback != NULL
		   && (readback->state == PGRAC_FENCED_TARGET_TRANSITIONING
			   || readback->state == PGRAC_FENCED_TARGET_UNKNOWN);
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback_retry(const PgracFencedProviderOpsV1 *ops,
											bool allow_test_only, const PgracFencedTargetV1 *target,
											uint64_t deadline_mono_ns,
											PgracFencedProviderResult *result,
											PgracFencedReadbackV1 *readback)
{
	return pgrac_fenced_provider_worker_readback_retry_owned(
		ops, allow_test_only, target, NULL, NULL, NULL, deadline_mono_ns, result, readback);
}

PgracFencedProviderWorkerResult
pgrac_fenced_provider_worker_readback_retry_owned(
	const PgracFencedProviderOpsV1 *ops, bool allow_test_only, const PgracFencedTargetV1 *target,
	const PgracFencedJournalRecordV1 *record, const struct PgracFencedConfigV1 *config,
	const uint8 config_digest[32], uint64_t deadline_mono_ns, PgracFencedProviderResult *result,
	PgracFencedReadbackV1 *readback)
{
	PgracFencedProviderWorkerResult worker_result;
	struct timespec delay;
	uint64_t delay_ms = 100;
	uint64_t delay_ns;
	uint64_t remaining_ns;
	uint64_t now;

	if (result == NULL || readback == NULL)
		return pgrac_fenced_provider_worker_readback_owned(ops, allow_test_only, target, record,
														   config, config_digest, deadline_mono_ns,
														   result, readback);
	for (;;) {
		worker_result = pgrac_fenced_provider_worker_readback_owned(
			ops, allow_test_only, target, record, config, config_digest, deadline_mono_ns, result,
			readback);
		if (!provider_readback_retryable(worker_result, *result, readback)
			|| !monotonic_now_ns(&now) || now >= deadline_mono_ns)
			return worker_result;
		delay_ns = delay_ms * UINT64_C(1000000);
		remaining_ns = deadline_mono_ns - now;
		if (delay_ns > remaining_ns)
			delay_ns = remaining_ns;
		delay.tv_sec = (time_t)(delay_ns / UINT64_C(1000000000));
		delay.tv_nsec = (long)(delay_ns % UINT64_C(1000000000));
		while (nanosleep(&delay, &delay) != 0) {
			if (errno != EINTR)
				return worker_result;
		}
		if (delay_ns == remaining_ns)
			return worker_result;
		if (delay_ms < 800)
			delay_ms *= 2;
		else
			delay_ms = 1000;
	}
}
