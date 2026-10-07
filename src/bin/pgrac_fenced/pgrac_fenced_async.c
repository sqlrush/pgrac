/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_async.c
 *    Forked operation execution with parent-owned journal serialization.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/wait.h>
#ifdef __linux__
#include <sys/syscall.h>
#endif
#include <unistd.h>

#include "pgrac_fenced_async.h"

typedef enum PgracFencedAsyncMessageKind
{
	PGRAC_FENCED_ASYNC_MESSAGE_APPEND = 1,
	PGRAC_FENCED_ASYNC_MESSAGE_APPEND_RESULT = 2,
	PGRAC_FENCED_ASYNC_MESSAGE_COMPLETE = 3
} PgracFencedAsyncMessageKind;

typedef struct PgracFencedAsyncMessageV1
{
	uint32 kind;
	uint32 success;
	PgracFencedJournalRecordV1 record;
	PgracExternalFenceProtocolResponseV1 response;
} PgracFencedAsyncMessageV1;

static bool
send_message(int fd, const PgracFencedAsyncMessageV1 *message)
{
	int flags = 0;
	ssize_t written;

#ifdef MSG_NOSIGNAL
	flags |= MSG_NOSIGNAL;
#endif
	do
	{
		written = send(fd, message, sizeof(*message), flags);
	} while (written < 0 && errno == EINTR);
	return written == sizeof(*message);
}

static bool
receive_message(int fd, PgracFencedAsyncMessageV1 *message)
{
	ssize_t got;

	do
	{
		got = recv(fd, message, sizeof(*message), 0);
	} while (got < 0 && errno == EINTR);
	return got == sizeof(*message);
}

static bool
child_append(void *argument, PgracFencedJournalRecordV1 *record)
{
	PgracFencedAsyncMessageV1 message;
	int fd = *(int *) argument;

	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_ASYNC_MESSAGE_APPEND;
	message.record = *record;
	if (!send_message(fd, &message) || !receive_message(fd, &message) ||
		message.kind != PGRAC_FENCED_ASYNC_MESSAGE_APPEND_RESULT ||
		message.success != 1)
		return false;
	*record = message.record;
	return true;
}

static int
child_keep_only_ipc(int fd)
{
	struct rlimit limit;
	rlim_t upper;
	int keep = 3;
	int current;

	if (fd != keep)
	{
		if (dup2(fd, keep) < 0)
			return -1;
		(void) close(fd);
	}
	if (fcntl(keep, F_SETFD, FD_CLOEXEC) != 0 ||
		getrlimit(RLIMIT_NOFILE, &limit) != 0)
		return -1;
#if defined(__linux__) && defined(SYS_close_range)
	if (syscall(SYS_close_range, (unsigned int) (keep + 1), ~0U, 0) == 0)
		return keep;
	if (errno != ENOSYS && errno != EINVAL)
		return -1;
#endif
	upper = limit.rlim_cur;
	if (upper == RLIM_INFINITY || upper > INT_MAX)
		upper = INT_MAX;
	for (current = keep + 1; current < (int) upper; current++)
		(void) close(current);
	return keep;
}

static void
child_main(int fd, PgracFencedOperationContextV1 context,
		   const PgracExternalFenceProtocolRequestV1 *request,
		   const PgracFencedPreparedAcquireV1 *prepared,
		   uint64 deadline_mono_ns, uint64 proof_generation)
{
	PgracFencedAsyncMessageV1 message;
	PgracExternalFenceProtocolResponseV1 response;

	fd = child_keep_only_ipc(fd);
	if (fd < 0)
		_exit(120);
	context.journal_fd = -1;
	context.journal_directory_fd = -1;
	context.journal_append_hook = NULL;
	context.journal_append_argument = NULL;
	context.next_proof_generation = proof_generation;
	if (!pgrac_fenced_operation_set_journal_append_hook(&context,
			child_append, &fd) ||
		(prepared == NULL ?
		 !pgrac_fenced_operation_acquire(&context, request, deadline_mono_ns,
			&response) :
		 !pgrac_fenced_operation_execute_preaccepted(&context, request,
			prepared, deadline_mono_ns, &response)) || response.verdict == 0)
		_exit(121);
	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_ASYNC_MESSAGE_COMPLETE;
	message.success = 1;
	message.response = response;
	if (prepared != NULL)
		message.record = prepared->accepted_record;
	if (!send_message(fd, &message))
		_exit(122);
	(void) close(fd);
	_exit(0);
}

static bool
set_parent_fd_flags(int fd)
{
	int descriptor_flags = fcntl(fd, F_GETFD, 0);
	int status_flags = fcntl(fd, F_GETFL, 0);

	return descriptor_flags >= 0 && status_flags >= 0 &&
		fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) == 0 &&
		fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) == 0;
}

static bool
async_start_internal(PgracFencedOperationContextV1 *context,
				 const PgracExternalFenceProtocolRequestV1 *request,
				 const PgracFencedPreparedAcquireV1 *prepared,
				 uint64 deadline_mono_ns,
				 PgracFencedAsyncWorkerV1 *worker)
{
	PgracFencedPreparedAcquireV1 local_prepared;
	PgracExternalFenceProtocolResponseV1 local_response;
	uint64 proof_generation;
	int sockets[2];
	pid_t pid;

	if (worker == NULL)
		return false;
	memset(worker, 0, sizeof(*worker));
	worker->fd = -1;
	if (context == NULL || request == NULL || !context->available ||
		deadline_mono_ns == 0)
		return false;
	/* New-provider work is accepted durably by the parent, never in a child. */
	if (context->config->format_version == 2 && prepared == NULL)
	{
		if (pgrac_fenced_operation_accept(context, request, deadline_mono_ns,
				&local_prepared, &local_response) != PGRAC_FENCED_OPERATION_READY)
			return false;
		prepared = &local_prepared;
	}
	if ((prepared != NULL &&
		 !pgrac_fenced_operation_prepared_matches(context, request, prepared)) ||
		!pgrac_fenced_operation_reserve_proof_generation(context,
			&proof_generation) ||
		socketpair(AF_UNIX, SOCK_DGRAM, 0, sockets) != 0)
		return false;
	pid = fork();
	if (pid < 0)
	{
		(void) close(sockets[0]);
		(void) close(sockets[1]);
		return false;
	}
	if (pid == 0)
	{
		(void) close(sockets[0]);
		child_main(sockets[1], *context, request, prepared, deadline_mono_ns,
			proof_generation);
	}
	(void) close(sockets[1]);
	if (!set_parent_fd_flags(sockets[0]))
	{
		(void) close(sockets[0]);
		(void) kill(pid, SIGKILL);
		while (waitpid(pid, NULL, 0) < 0 && errno == EINTR)
			;
		return false;
	}
	worker->pid = pid;
	worker->fd = sockets[0];
	worker->active = true;
	worker->proof_generation = proof_generation;
	if (prepared != NULL)
		worker->accepted_record = prepared->accepted_record;
	return true;
}

bool
pgrac_fenced_async_start(PgracFencedOperationContextV1 *context,
					 const PgracExternalFenceProtocolRequestV1 *request,
					 uint64 deadline_mono_ns,
					 PgracFencedAsyncWorkerV1 *worker)
{
	return async_start_internal(context, request, NULL, deadline_mono_ns,
		worker);
}

bool
pgrac_fenced_async_start_preaccepted(
	PgracFencedOperationContextV1 *context,
	const PgracExternalFenceProtocolRequestV1 *request,
	const PgracFencedPreparedAcquireV1 *prepared,
	uint64 deadline_mono_ns,
	PgracFencedAsyncWorkerV1 *worker)
{
	if (prepared == NULL)
		return false;
	return async_start_internal(context, request, prepared, deadline_mono_ns,
		worker);
}

int
pgrac_fenced_async_fd(const PgracFencedAsyncWorkerV1 *worker)
{
	return worker == NULL || !worker->active ? -1 : worker->fd;
}

static bool
reap_exact_child(PgracFencedAsyncWorkerV1 *worker)
{
	pid_t waited;

	do
	{
		waited = waitpid(worker->pid, &worker->wait_status, 0);
	} while (waited < 0 && errno == EINTR);
	return waited == worker->pid && WIFEXITED(worker->wait_status) &&
		WEXITSTATUS(worker->wait_status) == 0;
}

/* Parent validation is before append/ACK, so a stale child cannot actuate. */
static bool
worker_record_matches(const PgracFencedOperationContextV1 *context,
					  const PgracFencedAsyncWorkerV1 *worker,
					  const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalRecordV1 *accepted = &worker->accepted_record;
	PgracFencedJournalRecordV1 candidate = *record;

	if (context->config->format_version != 2)
		return accepted->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE &&
			record->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE;
	if (accepted->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_ACQUIRE ||
		accepted->seq == UINT64_MAX ||
		record->intent.attempt != accepted->intent.attempt ||
		memcmp(record->daemon_boot_id, context->daemon_boot_id, 16) != 0 ||
		memcmp(accepted->daemon_boot_id, context->daemon_boot_id, 16) != 0 ||
		memcmp(record->semantic_config_digest, context->semantic_config_digest, 32) != 0 ||
		record->mapping_generation != context->config->mapping_generation ||
		record->intent.system_identifier != context->config->system_identifier ||
		record->provider_id != context->provider->provider_id ||
		(record->record_kind != PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED &&
		 record->record_kind != PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT &&
		 record->record_kind != PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT &&
		 record->record_kind != PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED) ||
		(record->record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED &&
		 record->proof_generation != worker->proof_generation))
		return false;
	/* The serial journal owner assigns the sequence after this semantic check. */
	candidate.seq = accepted->seq + 1;
	return pgrac_fenced_journal_intent_continues(accepted, &candidate);
}

static bool
worker_completion_matches(const PgracFencedOperationContextV1 *context,
						  const PgracFencedAsyncWorkerV1 *worker,
						  const PgracFencedAsyncMessageV1 *message)
{
	const PgracFencedJournalRecordV1 *accepted = &worker->accepted_record;
	const PgracFencedJournalRecordV1 *last = &worker->last_record;
	const PgracExternalFenceProtocolResponseV1 *response = &message->response;
	uint8 expected[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	uint8 actual[PGRAC_FENCED_JOURNAL_MAX_RECORD_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	uint8 digest[32];
	size_t expected_len, actual_len;

	if (context->config->format_version != 2)
		return accepted->intent.kind == PGRAC_FENCED_JOURNAL_INTENT_NONE;
	if (!pgrac_fenced_journal_frame_encode(accepted, expected, sizeof(expected), &expected_len) ||
		!pgrac_fenced_journal_frame_encode(&message->record, actual, sizeof(actual), &actual_len) ||
		expected_len != actual_len || memcmp(expected, actual, expected_len) != 0 ||
		memcmp(accepted->daemon_boot_id, context->daemon_boot_id, 16) != 0 ||
		memcmp(accepted->semantic_config_digest, context->semantic_config_digest, 32) != 0 ||
		accepted->mapping_generation != context->config->mapping_generation ||
		!pgrac_external_fence_response_v1_encode(response, response_frame) ||
		memcmp(response->request_nonce, accepted->intent.request.acquire.request_nonce, 16) != 0 ||
		memcmp(response->daemon_boot_id, context->daemon_boot_id, 16) != 0 ||
		response->provider_id != context->provider->provider_id ||
		response->provider_abi_version != context->provider->abi_version)
		return false;
	if (response->verdict != 1)
		return true;
	return last->record_kind == PGRAC_FENCED_JOURNAL_KIND_PROOF_SERVED &&
		worker_record_matches(context, worker, last) &&
		response->journal_seq == last->seq &&
		response->proof_generation == last->proof_generation &&
		response->verified_mono_ns == last->event_mono_ns &&
		response->fresh_until_mono_ns == last->fresh_until_mono_ns &&
		memcmp(response->target_state_digest, last->target_state_digest, 32) == 0 &&
		pgrac_external_fence_binding_digest_v1(&response->binding, digest) &&
		memcmp(digest, last->binding_digest, sizeof(digest)) == 0;
}

bool
pgrac_fenced_async_service(PgracFencedOperationContextV1 *context,
					   PgracFencedAsyncWorkerV1 *worker,
					   PgracFencedAsyncEvent *event,
					   PgracExternalFenceProtocolResponseV1 *response)
{
	PgracFencedAsyncMessageV1 message;
	bool appended;

	if (event != NULL)
		*event = PGRAC_FENCED_ASYNC_NONE;
	if (context == NULL || worker == NULL || event == NULL ||
		response == NULL || !worker->active || worker->fd < 0)
		return false;
	memset(&message, 0, sizeof(message));
	if (!receive_message(worker->fd, &message))
	{
		(void) close(worker->fd);
		worker->fd = -1;
		worker->active = false;
		(void) reap_exact_child(worker);
		return false;
	}
	if (message.kind == PGRAC_FENCED_ASYNC_MESSAGE_APPEND)
	{
		appended = worker_record_matches(context, worker, &message.record) &&
			pgrac_fenced_operation_append_journal(context,
			&message.record);
		if (appended)
		{
			worker->last_record_kind = message.record.record_kind;
			worker->last_record = message.record;
		}
		message.kind = PGRAC_FENCED_ASYNC_MESSAGE_APPEND_RESULT;
		message.success = appended ? 1 : 0;
		if (!send_message(worker->fd, &message))
			return false;
		*event = PGRAC_FENCED_ASYNC_JOURNAL;
		return appended;
	}
	if (message.kind != PGRAC_FENCED_ASYNC_MESSAGE_COMPLETE ||
		message.success != 1 || !worker_completion_matches(context, worker, &message))
		return false;
	*response = message.response;
	(void) close(worker->fd);
	worker->fd = -1;
	worker->active = false;
	if (!reap_exact_child(worker))
		return false;
	worker->pid = 0;
	*event = PGRAC_FENCED_ASYNC_COMPLETE;
	return true;
}
