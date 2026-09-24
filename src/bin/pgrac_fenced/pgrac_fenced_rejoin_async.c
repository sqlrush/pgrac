/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin_async.c
 *    Forked PFRJ phases with parent-owned state and durable sequencing.
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

#include "pgrac_fenced_rejoin_async.h"

typedef enum PgracFencedRejoinAsyncMessageKind
{
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND = 1,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT = 2,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF = 3,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT = 4,
	PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE = 5
} PgracFencedRejoinAsyncMessageKind;

typedef struct PgracFencedRejoinAsyncMessageV1
{
	uint32 kind;
	uint32 success;
	uint32 changed_slot;
	uint32 operation_count;
	uint64 proof_generation;
	/* PGRAC: APPEND and COMPLETE carry disjoint payloads, not two copies. */
	union
	{
		PgracFencedJournalRecordV1 record;
		PgracFencedRejoinOperationV1 operation;
	};
	PgracExternalFenceProtocolRejoinFrameV1 response;
} PgracFencedRejoinAsyncMessageV1;

StaticAssertDecl(sizeof(PgracFencedRejoinAsyncMessageV1) <= 2048,
	"rejoin worker message must fit the supported local datagram bound");

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	if (bytes == NULL)
		return false;
	for (i = 0; i < len; i++)
	{
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
send_message(int fd, const PgracFencedRejoinAsyncMessageV1 *message)
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
receive_message(int fd, PgracFencedRejoinAsyncMessageV1 *message)
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
	PgracFencedRejoinAsyncMessageV1 message;
	int fd = *(int *) argument;

	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND;
	message.record = *record;
	if (!send_message(fd, &message) || !receive_message(fd, &message) ||
		message.kind != PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT ||
		message.success != 1)
		return false;
	*record = message.record;
	return true;
}

static bool
child_reserve_proof(void *argument, uint64 *proof_generation)
{
	PgracFencedRejoinAsyncMessageV1 message;
	int fd = *(int *) argument;

	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF;
	if (!send_message(fd, &message) || !receive_message(fd, &message) ||
		message.kind !=
		PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT ||
		message.success != 1 || message.proof_generation == 0)
		return false;
	*proof_generation = message.proof_generation;
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

static bool
execute_action(PgracFencedRejoinContextV1 *context,
		   PgracFencedRejoinAsyncAction action,
		   const PgracExternalFenceProtocolRejoinFrameV1 *request,
		   const uint8 operation_id[16],
		   bool target_admissions_invalidated, uint64 deadline_mono_ns,
		   PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	switch (action)
	{
		case PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE:
			return pgrac_fenced_rejoin_admin_prepare(context, request,
				operation_id, deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT:
			return pgrac_fenced_rejoin_claim(context, request,
				deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON:
			return pgrac_fenced_rejoin_authorize_on(context, request,
				target_admissions_invalidated, deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON:
			return pgrac_fenced_rejoin_refresh_on(context, request,
				deadline_mono_ns, response);
		case PGRAC_FENCED_REJOIN_ASYNC_CLEANUP:
			return pgrac_fenced_rejoin_cleanup(context, request->operation_id,
				deadline_mono_ns);
	}
	return false;
}

static void
child_main(int fd, PgracFencedOperationContextV1 operation_context,
	   PgracFencedRejoinContextV1 rejoin_context,
	   PgracFencedRejoinAsyncAction action,
	   const PgracExternalFenceProtocolRejoinFrameV1 *request,
	   const uint8 operation_id[16], bool target_admissions_invalidated,
	   uint64 deadline_mono_ns)
{
	PgracFencedRejoinAsyncMessageV1 message;
	PgracFencedRejoinContextV1 before;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint32 changed_slot = UINT32_MAX;
	uint32 i;

	fd = child_keep_only_ipc(fd);
	if (fd < 0)
		_exit(120);
	operation_context.journal_fd = -1;
	operation_context.journal_directory_fd = -1;
	operation_context.journal_append_hook = NULL;
	operation_context.journal_append_argument = NULL;
	operation_context.proof_reserve_hook = NULL;
	operation_context.proof_reserve_argument = NULL;
	rejoin_context.operation_context = &operation_context;
	before = rejoin_context;
	memset(&response, 0, sizeof(response));
	if (!pgrac_fenced_operation_set_journal_append_hook(&operation_context,
			child_append, &fd) ||
		!pgrac_fenced_operation_set_proof_reserve_hook(&operation_context,
			child_reserve_proof, &fd) ||
		!execute_action(&rejoin_context, action, request, operation_id,
			target_admissions_invalidated, deadline_mono_ns, &response) ||
		!operation_context.available ||
		(action != PGRAC_FENCED_REJOIN_ASYNC_CLEANUP &&
		 !pgrac_external_fence_rejoin_v1_encode(&response, frame)))
		_exit(121);
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		if (memcmp(&before.operations[i], &rejoin_context.operations[i],
				sizeof(before.operations[i])) == 0)
			continue;
		if (changed_slot != UINT32_MAX)
			_exit(122);
		changed_slot = i;
	}
	memset(&message, 0, sizeof(message));
	message.kind = PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE;
	message.success = 1;
	message.changed_slot = changed_slot;
	message.operation_count = rejoin_context.operation_count;
	if (changed_slot != UINT32_MAX)
		message.operation = rejoin_context.operations[changed_slot];
	message.response = response;
	if (!send_message(fd, &message))
		_exit(123);
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
action_matches_request(PgracFencedRejoinAsyncAction action,
			   const PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	if (request == NULL)
		return false;
	return (action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON &&
			request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_CLEANUP &&
			(request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON ||
			 request->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON));
}

static bool
owned_worker_identity(PgracFencedOperationContextV1 *context,
					  const PgracFencedRejoinContextV1 *rejoin,
					  PgracFencedRejoinAsyncAction action,
					  const PgracExternalFenceProtocolRejoinFrameV1 *request,
					  const uint8 operation_id[16],
					  PgracFencedRejoinAsyncWorkerV1 *worker)
{
	const PgracFencedTargetV1 *claim = NULL;
	PgracFencedJournalRecordV1 *identity = &worker->identity;
	uint32 slot = UINT32_MAX;
	uint32 free_slot = UINT32_MAX;
	int32 node;

	if (context->config->format_version != 2)
		return action != PGRAC_FENCED_REJOIN_ASYNC_CLEANUP;
	if (action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT)
		claim = pgrac_fenced_rejoin_claim_target(rejoin);
	for (uint32 i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; ++i)
	{
		const PgracFencedRejoinOperationV1 *entry = &rejoin->operations[i];

		if (entry->state == PGRAC_FENCED_REJOIN_OPERATION_UNUSED)
		{
			if (free_slot == UINT32_MAX)
				free_slot = i;
			continue;
		}
		if (action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE ?
			entry->admin_request.old_node_id == request->old_node_id :
			action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT ?
			&entry->target == claim :
			memcmp(entry->operation_id, request->operation_id, 16) == 0)
		{
			slot = i;
			break;
		}
	}
	if (slot == UINT32_MAX && action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE)
		slot = free_slot;
	if (slot == UINT32_MAX)
		return false;
	worker->before = rejoin->operations[slot];
	worker->last_record = worker->before.last_record;
	if (action == PGRAC_FENCED_REJOIN_ASYNC_CLEANUP &&
		(worker->before.state != PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED ||
		 pgrac_fenced_journal_rejoin_terminal(&worker->last_record) ||
		 memcmp(request, &worker->last_record.intent.request.rejoin, sizeof(*request)) != 0))
		return false;
	worker->request = *request;
	worker->operation_slot = slot;
	worker->operation_count = rejoin->operation_count;
	worker->owned = true;
	identity->intent.kind = PGRAC_FENCED_JOURNAL_INTENT_REJOIN;
	identity->intent.attempt = worker->last_record.intent.attempt;
	if (action != PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT ||
		memcmp(worker->last_record.daemon_boot_id, context->daemon_boot_id, 16) != 0)
	{
		if (identity->intent.attempt == UINT64_MAX)
			return false;
		identity->intent.attempt++;
	}
	identity->intent.request.rejoin = action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT ?
		worker->last_record.intent.request.rejoin : *request;
	node = identity->intent.request.rejoin.old_node_id;
	if (node < 0 || node >= PGRAC_FENCED_MAX_NODES ||
		!context->config->nodes[node].present ||
		!pgrac_fenced_config_protected_set_digest(context->config, node,
			identity->intent.protected_set_digest))
		return false;
	memcpy(identity->intent.target_uuid, context->config->nodes[node].target_uuid, 16);
	identity->intent.system_identifier = context->config->system_identifier;
	memcpy(identity->operation_id,
		action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE &&
		worker->before.state == PGRAC_FENCED_REJOIN_OPERATION_UNUSED ? operation_id :
		worker->before.operation_id, 16);
	memcpy(identity->daemon_boot_id, context->daemon_boot_id, 16);
	memcpy(identity->semantic_config_digest, context->semantic_config_digest, 32);
	identity->provider_id = context->provider->provider_id;
	identity->provider_abi_version = PGRAC_FENCED_PROVIDER_ABI_V1;
	identity->mapping_generation = context->config->mapping_generation;
	return true;
}

static bool
owned_parent_unchanged(const PgracFencedRejoinContextV1 *context,
					   const PgracFencedRejoinAsyncWorkerV1 *worker)
{
	return !worker->owned ||
		(worker->operation_slot < PGRAC_FENCED_REJOIN_MAX_OPERATIONS &&
		 memcmp(&context->operations[worker->operation_slot], &worker->before,
			 sizeof(worker->before)) == 0);
}

static bool
owned_record_matches(const PgracFencedRejoinAsyncWorkerV1 *worker,
					 const PgracFencedJournalRecordV1 *record)
{
	const PgracFencedJournalRecordV1 *identity = &worker->identity;
	PgracFencedJournalRecordV1 check = *record;
	uint8 expected[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 actual[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 digest[32];
	bool kind_ok;

	if (!worker->owned)
		return true;
	if (record->intent.kind != PGRAC_FENCED_JOURNAL_INTENT_REJOIN ||
		record->intent.attempt != identity->intent.attempt ||
		record->intent.system_identifier != identity->intent.system_identifier ||
		memcmp(record->intent.target_uuid, identity->intent.target_uuid, 16) != 0 ||
		memcmp(record->intent.protected_set_digest, identity->intent.protected_set_digest, 32) != 0 ||
		memcmp(record->operation_id, identity->operation_id, 16) != 0 ||
		memcmp(record->daemon_boot_id, identity->daemon_boot_id, 16) != 0 ||
		memcmp(record->semantic_config_digest, identity->semantic_config_digest, 32) != 0 ||
		record->provider_id != identity->provider_id ||
		record->provider_abi_version != identity->provider_abi_version ||
		record->mapping_generation != identity->mapping_generation ||
		!pgrac_external_fence_rejoin_v1_encode(&identity->intent.request.rejoin, expected) ||
		!pgrac_external_fence_rejoin_v1_encode(&record->intent.request.rejoin, actual) ||
		memcmp(expected, actual, sizeof(expected)) != 0 ||
		worker->last_record.seq == UINT64_MAX)
		return false;
	/* The parent assigns the persistent sequence only after this validation. */
	check.seq = worker->last_record.seq + 1;
	kind_ok = record->record_kind == PGRAC_FENCED_JOURNAL_KIND_REQUEST_ACCEPTED;
	if (worker->action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT)
		kind_ok = record->record_kind == PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT ||
			(kind_ok && worker->identity.intent.attempt > worker->before.last_record.intent.attempt);
	else if (worker->action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON)
		kind_ok = kind_ok || record->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED ||
			record->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT ||
			record->record_kind == PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT;
	else if (worker->action == PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON)
		kind_ok = kind_ok || record->record_kind == PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT;
	else if (worker->action == PGRAC_FENCED_REJOIN_ASYNC_CLEANUP)
		kind_ok = kind_ok || record->record_kind == PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT ||
			((record->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_ISSUED ||
			  record->record_kind == PGRAC_FENCED_JOURNAL_KIND_ACTUATION_RESULT) &&
			 record->target_state == PGRAC_FENCED_TARGET_OFF) ||
			(record->record_kind == PGRAC_FENCED_JOURNAL_KIND_RECONCILED &&
			 pgrac_fenced_journal_rejoin_terminal(&check));
	if (!kind_ok)
		return false;
	if (record->proof_generation != 0 &&
		(record->proof_generation != worker->proof_generation ||
		 !pgrac_external_fence_target_state_digest_v1(record->intent.target_uuid,
			record->target_state, record->io_drain_state, record->mapping_generation,
			record->proof_generation, digest) ||
		 memcmp(digest, record->target_state_digest, sizeof(digest)) != 0))
		return false;
	return pgrac_fenced_journal_intent_continues(
		worker->last_record.seq == 0 ? NULL : &worker->last_record, &check);
}

static bool
owned_completion_matches(const PgracFencedOperationContextV1 *context,
						 const PgracFencedRejoinAsyncWorkerV1 *worker,
						 const PgracFencedRejoinAsyncMessageV1 *message)
{
	const PgracExternalFenceProtocolRejoinFrameV1 *response = &message->response;
	const PgracFencedJournalRecordV1 *identity = &worker->identity;
	const PgracFencedJournalRecordV1 *last = &worker->last_record;
	const PgracExternalFenceProtocolRejoinFrameV1 *need = &identity->intent.request.rejoin;
	PgracFencedRejoinOperationV1 expected = worker->before;
	uint32 count = worker->operation_count;
	uint32 positive_status;
	uint16 opcode;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	bool positive;

	if (!worker->owned)
		return true;
	if (worker->action == PGRAC_FENCED_REJOIN_ASYNC_CLEANUP)
	{
		/* PGRAC: internal completion cannot manufacture a client READY frame. */
		expected.last_record = *last;
		return !bytes_nonzero((const uint8 *) response, sizeof(*response)) &&
			count == message->operation_count && message->changed_slot == worker->operation_slot &&
			memcmp(&expected, &message->operation, sizeof(expected)) == 0;
	}
	switch (worker->action)
	{
		case PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE:
			opcode = PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT;
			positive_status = PGRAC_FENCED_REJOIN_STATUS_OFFERED;
			break;
		case PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT:
			opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER;
			positive_status = PGRAC_FENCED_REJOIN_STATUS_OFFERED;
			break;
		case PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON:
			opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT;
			positive_status = PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER;
			break;
		case PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON:
			opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT;
			positive_status = PGRAC_FENCED_REJOIN_STATUS_READY;
			break;
		default:
			return false;
	}
	positive = response->status == positive_status;
	if (!pgrac_external_fence_rejoin_v1_encode(response, frame) || response->opcode != opcode ||
		memcmp(response->transport_nonce, worker->request.transport_nonce, 16) != 0 ||
		memcmp(response->operation_id, identity->operation_id, 16) != 0 ||
		(!positive && response->status < PGRAC_FENCED_REJOIN_STATUS_REJECTED))
		return false;
	if (positive && worker->action != PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE)
	{
		bool claim = worker->action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT;

		if (last->record_kind != (claim ? PGRAC_FENCED_JOURNAL_KIND_READBACK_RESULT :
				PGRAC_FENCED_JOURNAL_KIND_REENABLE_RESULT) ||
			last->provider_result != PGRAC_FENCED_PROVIDER_OK || last->deny_reason != 0 ||
			last->target_state != (claim ? PGRAC_FENCED_TARGET_OFF : PGRAC_FENCED_TARGET_ON) ||
			last->io_drain_state != PGRAC_FENCED_IO_DRAIN_DRAINED ||
			last->proof_generation == 0 || last->proof_generation != worker->proof_generation ||
			response->system_identifier != identity->intent.system_identifier ||
			memcmp(response->protected_set_digest, identity->intent.protected_set_digest, 32) != 0 ||
			memcmp(response->rejoin_gate_digest, need->rejoin_gate_digest, 32) != 0 ||
			response->old_node_id != need->old_node_id ||
			response->old_incarnation != need->old_incarnation ||
			response->candidate_incarnation != need->candidate_incarnation ||
			response->provider_id != identity->provider_id ||
			response->provider_abi_version != identity->provider_abi_version ||
			response->target_mapping_generation != identity->mapping_generation ||
			memcmp(response->daemon_boot_id, identity->daemon_boot_id, 16) != 0 ||
			response->journal_seq != last->seq || response->verified_mono_ns != last->event_mono_ns ||
			response->fresh_until_mono_ns != last->fresh_until_mono_ns ||
			response->proof_generation != last->proof_generation ||
			memcmp(response->target_state_digest, last->target_state_digest, 32) != 0)
			return false;
	}
	if (positive)
	{
		switch (worker->action)
		{
			case PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE:
				if (expected.state == PGRAC_FENCED_REJOIN_OPERATION_UNUSED)
				{
					const PgracFencedNodeConfigV1 *node = &context->config->nodes[need->old_node_id];

					expected.state = PGRAC_FENCED_REJOIN_OPERATION_OFFERED;
					expected.admin_request = worker->request;
					memcpy(expected.operation_id, identity->operation_id, 16);
					memcpy(expected.target.target_uuid, identity->intent.target_uuid, 16);
					expected.target.victim_node_id = need->old_node_id;
					expected.target.mapping_generation = identity->mapping_generation;
					expected.target.adapter_config = node->adapter_data;
					expected.target.adapter_config_len = node->adapter_data_len;
					count++;
				}
				break;
			case PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT:
				expected.state = PGRAC_FENCED_REJOIN_OPERATION_CLAIMED;
				expected.offer_result = *response;
				break;
			case PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON:
				expected.state = PGRAC_FENCED_REJOIN_OPERATION_WAITING_JOINER;
				expected.on_result = *response;
				break;
			case PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON:
				expected.state = PGRAC_FENCED_REJOIN_OPERATION_READY;
				expected.ready_result = *response;
				break;
			case PGRAC_FENCED_REJOIN_ASYNC_CLEANUP:
				return false;
		}
	}
	expected.last_record = *last;
	if (count != message->operation_count)
		return false;
	if (message->changed_slot == UINT32_MAX)
		return memcmp(&expected, &worker->before, sizeof(expected)) == 0;
	return message->changed_slot == worker->operation_slot &&
		memcmp(&expected, &message->operation, sizeof(expected)) == 0;
}

bool
pgrac_fenced_rejoin_async_start(
	PgracFencedOperationContextV1 *operation_context,
	const PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncAction action,
	const PgracExternalFenceProtocolRejoinFrameV1 *request,
	const uint8 operation_id[16], bool target_admissions_invalidated,
	uint64 deadline_mono_ns, PgracFencedRejoinAsyncWorkerV1 *worker)
{
	int sockets[2];
	pid_t pid;

	if (worker == NULL)
		return false;
	memset(worker, 0, sizeof(*worker));
	worker->fd = -1;
	if (operation_context == NULL || rejoin_context == NULL ||
		rejoin_context->operation_context != operation_context ||
		!operation_context->available || deadline_mono_ns == 0 ||
		!action_matches_request(action, request) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE ?
			(operation_id == NULL || !bytes_nonzero(operation_id, 16)) :
			operation_id != NULL) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON ?
			!target_admissions_invalidated : target_admissions_invalidated) ||
		!owned_worker_identity(operation_context, rejoin_context, action, request,
			operation_id, worker) ||
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
		child_main(sockets[1], *operation_context, *rejoin_context, action,
			request, operation_id, target_admissions_invalidated,
			deadline_mono_ns);
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
	worker->action = action;
	worker->active = true;
	return true;
}

int
pgrac_fenced_rejoin_async_fd(
	const PgracFencedRejoinAsyncWorkerV1 *worker)
{
	return worker == NULL || !worker->active ? -1 : worker->fd;
}

static bool
reap_exact_child(PgracFencedRejoinAsyncWorkerV1 *worker)
{
	pid_t waited;

	do
	{
		waited = waitpid(worker->pid, &worker->wait_status, 0);
	} while (waited < 0 && errno == EINTR);
	return waited == worker->pid && WIFEXITED(worker->wait_status) &&
		WEXITSTATUS(worker->wait_status) == 0;
}

bool
pgrac_fenced_rejoin_async_service(
	PgracFencedOperationContextV1 *operation_context,
	PgracFencedRejoinContextV1 *rejoin_context,
	PgracFencedRejoinAsyncWorkerV1 *worker,
	PgracFencedRejoinAsyncEvent *event,
	PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	PgracFencedRejoinAsyncMessageV1 message;
	int64 completed_count;
	bool reserved;

	if (event != NULL)
		*event = PGRAC_FENCED_REJOIN_ASYNC_NONE;
	if (operation_context == NULL || rejoin_context == NULL ||
		worker == NULL || event == NULL || response == NULL ||
		!worker->active || worker->fd < 0)
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
	if (message.kind == PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND)
	{
		reserved = owned_parent_unchanged(rejoin_context, worker) &&
			owned_record_matches(worker, &message.record) &&
			pgrac_fenced_operation_append_journal(operation_context,
			&message.record);
		if (reserved && worker->owned)
			worker->last_record = message.record;
		message.kind =
			PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_APPEND_RESULT;
		message.success = reserved ? 1 : 0;
		if (!send_message(worker->fd, &message))
			return false;
		*event = PGRAC_FENCED_REJOIN_ASYNC_JOURNAL;
		return reserved;
	}
	if (message.kind ==
		PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF)
	{
		reserved = owned_parent_unchanged(rejoin_context, worker) &&
			pgrac_fenced_operation_reserve_proof_generation(
			operation_context, &message.proof_generation);
		if (reserved && worker->owned)
			worker->proof_generation = message.proof_generation;
		message.kind =
			PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_RESERVE_PROOF_RESULT;
		message.success = reserved ? 1 : 0;
		if (!send_message(worker->fd, &message))
			return false;
		*event = PGRAC_FENCED_REJOIN_ASYNC_PROOF;
		return reserved;
	}
	if (message.kind != PGRAC_FENCED_REJOIN_ASYNC_MESSAGE_COMPLETE ||
		message.success != 1 ||
		!owned_parent_unchanged(rejoin_context, worker) ||
		!owned_completion_matches(operation_context, worker, &message) ||
		message.operation_count > PGRAC_FENCED_REJOIN_MAX_OPERATIONS ||
		(message.changed_slot != UINT32_MAX &&
		 message.changed_slot >= PGRAC_FENCED_REJOIN_MAX_OPERATIONS))
		return false;
	/* PGRAC: another terminal target may retire while this exact slot is owned. */
	completed_count = worker->owned ?
		(int64) rejoin_context->operation_count + message.operation_count - worker->operation_count :
		message.operation_count;
	if (completed_count < 0 || completed_count > PGRAC_FENCED_REJOIN_MAX_OPERATIONS)
		return false;
	if (message.changed_slot != UINT32_MAX)
		rejoin_context->operations[message.changed_slot] = message.operation;
	rejoin_context->operation_count = (uint32) completed_count;
	*response = message.response;
	(void) close(worker->fd);
	worker->fd = -1;
	worker->active = false;
	if (!reap_exact_child(worker))
		return false;
	worker->pid = 0;
	*event = PGRAC_FENCED_REJOIN_ASYNC_COMPLETE;
	return true;
}
