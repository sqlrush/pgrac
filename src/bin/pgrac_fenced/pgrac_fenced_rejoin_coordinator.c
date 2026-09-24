/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_rejoin_coordinator.c
 *    Nonblocking PFRJ socket and exact-target reservation owner.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "pgrac_fenced_rejoin_coordinator.h"

#define PGRAC_FENCED_REJOIN_INVALIDATE_REASON UINT32_C(19)

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
configure_client_fd(int fd)
{
	int descriptor_flags;
	int status_flags;

	status_flags = fcntl(fd, F_GETFL, 0);
	descriptor_flags = fcntl(fd, F_GETFD, 0);
	if (status_flags < 0 || descriptor_flags < 0 ||
		fcntl(fd, F_SETFL, status_flags | O_NONBLOCK) != 0 ||
		fcntl(fd, F_SETFD, descriptor_flags | FD_CLOEXEC) != 0)
		return false;
#ifdef SO_NOSIGPIPE
	{
		int enabled = 1;

		if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled,
				sizeof(enabled)) != 0)
			return false;
	}
#endif
	return true;
}

static int
client_allocate(PgracFencedRejoinCoordinatorV1 *coordinator)
{
	uint32 i;

	if (coordinator->client_count >= PGRAC_FENCED_REJOIN_MAX_CLIENTS)
		return -1;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_CLIENTS; i++)
	{
		if (coordinator->clients[i].state ==
				PGRAC_FENCED_REJOIN_CLIENT_UNUSED)
			return (int) i;
	}
	return -1;
}

static void
client_clear(PgracFencedRejoinCoordinatorV1 *coordinator, uint32 slot)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];

	if (client->fd >= 0)
		(void) close(client->fd);
	if (client->state != PGRAC_FENCED_REJOIN_CLIENT_UNUSED)
		coordinator->client_count--;
	memset(client, 0, sizeof(*client));
	client->fd = -1;
}

static bool
client_release_target(PgracFencedRejoinCoordinatorV1 *coordinator,
			  PgracFencedRejoinClientV1 *client)
{
	bool owned = false;
	uint32 i;

	if (!client->target_reserved)
		return true;
	if (coordinator->operation_context->config->format_version == 2)
	{
		for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
		{
			const PgracFencedRejoinOperationV1 *operation = &coordinator->rejoin_context.operations[i];

			if (operation->state != PGRAC_FENCED_REJOIN_OPERATION_UNUSED &&
				operation->state != PGRAC_FENCED_REJOIN_OPERATION_OFFERED &&
				memcmp(operation->target.target_uuid, client->reserved_target, 16) == 0)
				owned = true;
		}
	}
	/* PGRAC: only detach the waiter while the operation still owns the target. */
	if (!owned && !pgrac_fenced_coordinator_rejoin_release_target(
			coordinator->scalar_coordinator, client->reserved_target))
		return false;
	client->target_reserved = false;
	memset(client->reserved_target, 0, sizeof(client->reserved_target));
	return true;
}

static bool
client_cancel_operation(PgracFencedRejoinCoordinatorV1 *coordinator,
			PgracFencedRejoinClientV1 *client)
{
	bool ok = true;

	if (!client->owns_operation ||
		!bytes_nonzero(client->operation_id, sizeof(client->operation_id)))
		return client_release_target(coordinator, client);
	if (pgrac_fenced_rejoin_target(&coordinator->rejoin_context,
			client->operation_id) != NULL)
	{
		if (!pgrac_fenced_rejoin_detach(&coordinator->rejoin_context,
				client->operation_id))
			ok = false;
	}
	if (!client_release_target(coordinator, client))
		ok = false;
	client->owns_operation = false;
	memset(client->operation_id, 0, sizeof(client->operation_id));
	return ok;
}

static bool
client_abandon(PgracFencedRejoinCoordinatorV1 *coordinator, uint32 slot)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];

	if (client->fd >= 0)
	{
		(void) close(client->fd);
		client->fd = -1;
	}
	if (client->state == PGRAC_FENCED_REJOIN_CLIENT_WORKER)
	{
		client->abandoned = true;
		return true;
	}
	if (!client_cancel_operation(coordinator, client))
		return false;
	client_clear(coordinator, slot);
	return true;
}

bool
pgrac_fenced_rejoin_coordinator_init(
	PgracFencedRejoinCoordinatorV1 *coordinator,
	PgracFencedOperationContextV1 *operation_context,
	PgracFencedCoordinatorV1 *scalar_coordinator)
{
	uint32 i;

	if (coordinator == NULL || operation_context == NULL ||
		scalar_coordinator == NULL ||
		scalar_coordinator->context != operation_context ||
		!operation_context->available)
		return false;
	memset(coordinator, 0, sizeof(*coordinator));
	coordinator->operation_context = operation_context;
	coordinator->scalar_coordinator = scalar_coordinator;
	coordinator->worker.fd = -1;
	coordinator->worker_owner = -1;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_CLIENTS; i++)
		coordinator->clients[i].fd = -1;
	return pgrac_fenced_rejoin_init(&coordinator->rejoin_context,
		operation_context);
}

bool
pgrac_fenced_rejoin_coordinator_restore(PgracFencedRejoinCoordinatorV1 *coordinator,
	PgracFencedJournalReconcileState *pending)
{
	uint32 i;

	if (coordinator == NULL || coordinator->quiescing || coordinator->client_count != 0 ||
		coordinator->worker.active || coordinator->worker_owner >= 0 ||
		!pgrac_fenced_rejoin_restore(&coordinator->rejoin_context, pending))
		return false;
	/* PGRAC: reserve all uncertain power work before listeners can admit callers. */
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		const PgracFencedRejoinOperationV1 *operation = &coordinator->rejoin_context.operations[i];

		if (operation->state == PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED &&
			pgrac_fenced_coordinator_rejoin_acquire_target(coordinator->scalar_coordinator,
				operation->target.target_uuid) == PGRAC_FENCED_REJOIN_TARGET_ERROR)
			return false;
	}
	return true;
}

bool
pgrac_fenced_rejoin_coordinator_accept_fd(
	PgracFencedRejoinCoordinatorV1 *coordinator, int client_fd,
	bool is_admin, uint64 transport_deadline_mono_ns)
{
	PgracFencedRejoinClientV1 *client;
	int slot;

	if (coordinator == NULL || coordinator->operation_context == NULL ||
		coordinator->quiescing || client_fd < 0 ||
		transport_deadline_mono_ns == 0)
	{
		if (client_fd >= 0)
			(void) close(client_fd);
		return false;
	}
	slot = client_allocate(coordinator);
	if (slot < 0 || !configure_client_fd(client_fd))
	{
		(void) close(client_fd);
		return slot < 0;
	}
	client = &coordinator->clients[slot];
	memset(client, 0, sizeof(*client));
	client->fd = client_fd;
	client->state = PGRAC_FENCED_REJOIN_CLIENT_INGRESS;
	client->is_admin = is_admin;
	client->transport_deadline_mono_ns = transport_deadline_mono_ns;
	coordinator->client_count++;
	return true;
}

static bool
frame_has_no_immediate_suffix(int fd)
{
	uint8 extra;
	ssize_t got;

	do
	{
		got = recv(fd, &extra, 1, MSG_DONTWAIT | MSG_PEEK);
	} while (got < 0 && errno == EINTR);
	if (got > 0 || got == 0)
		return false;
	return errno == EAGAIN || errno == EWOULDBLOCK;
}

static bool
client_receive_request(PgracFencedRejoinCoordinatorV1 *coordinator,
			   uint32 slot, uint64 now_mono_ns)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];
	uint64 timeout_ns;

	while (client->request_frame_used <
		PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES)
	{
		ssize_t got = recv(client->fd,
			client->request_frame + client->request_frame_used,
			PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES -
			client->request_frame_used, MSG_DONTWAIT);

		if (got > 0)
		{
			client->request_frame_used += (size_t) got;
			continue;
		}
		if (got == 0)
			return client_abandon(coordinator, slot);
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return true;
		return client_abandon(coordinator, slot);
	}
	if (!frame_has_no_immediate_suffix(client->fd) ||
		!pgrac_external_fence_rejoin_v1_decode(client->request_frame,
			PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES, &client->request) ||
		(client->is_admin ?
		 client->request.opcode !=
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE :
		 client->request.opcode ==
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE))
		return client_abandon(coordinator, slot);
	if (client->request.timeout_ms == 0)
		client->operation_deadline_mono_ns =
			client->transport_deadline_mono_ns;
	else
	{
		timeout_ns = (uint64) client->request.timeout_ms *
			UINT64_C(1000000);
		if (UINT64_MAX - now_mono_ns < timeout_ns)
			return client_abandon(coordinator, slot);
		client->operation_deadline_mono_ns = now_mono_ns + timeout_ns;
		if (client->operation_deadline_mono_ns >
			client->transport_deadline_mono_ns)
			client->operation_deadline_mono_ns =
				client->transport_deadline_mono_ns;
	}
	if (now_mono_ns >= client->operation_deadline_mono_ns)
		return client_abandon(coordinator, slot);
	client->state = PGRAC_FENCED_REJOIN_CLIENT_READY;
	return true;
}

static bool
client_has_event(int fd)
{
	struct pollfd descriptor;
	int rc;

	descriptor.fd = fd;
	descriptor.events = POLLIN | POLLHUP | POLLERR;
	descriptor.revents = 0;
	do
	{
		rc = poll(&descriptor, 1, 0);
	} while (rc < 0 && errno == EINTR);
	return rc != 0;
}

static bool
start_worker(PgracFencedRejoinCoordinatorV1 *coordinator, uint32 slot,
		 PgracFencedRejoinAsyncAction action,
		 const uint8 operation_id[16])
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];

	if (coordinator->worker_owner >= 0 || coordinator->worker.active ||
		!pgrac_fenced_rejoin_async_start(coordinator->operation_context,
			&coordinator->rejoin_context, action, &client->request,
			operation_id, action ==
				PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON,
			client->operation_deadline_mono_ns, &coordinator->worker))
		return false;
	client->action = action;
	client->state = PGRAC_FENCED_REJOIN_CLIENT_WORKER;
	coordinator->worker_owner = (int) slot;
	return true;
}

static PgracFencedCoordinatorRejoinTargetResult
reserve_client_target(PgracFencedRejoinCoordinatorV1 *coordinator,
			  PgracFencedRejoinClientV1 *client,
			  const uint8 target_uuid[16])
{
	PgracFencedCoordinatorRejoinTargetResult result;

	if (client->target_reserved &&
		memcmp(client->reserved_target, target_uuid,
			sizeof(client->reserved_target)) != 0)
		return PGRAC_FENCED_REJOIN_TARGET_ERROR;
	result = pgrac_fenced_coordinator_rejoin_acquire_target(
		coordinator->scalar_coordinator, target_uuid);
	if (result != PGRAC_FENCED_REJOIN_TARGET_ERROR &&
		!client->target_reserved)
	{
		memcpy(client->reserved_target, target_uuid,
			   sizeof(client->reserved_target));
		client->target_reserved = true;
	}
	return result;
}

static bool
start_ready_client(PgracFencedRejoinCoordinatorV1 *coordinator,
			   uint32 slot)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];
	const PgracFencedTargetV1 *target;
	const PgracFencedNodeConfigV1 *node;
	PgracFencedCoordinatorRejoinTargetResult reserved;
	uint8 operation_id[16];

	if (coordinator->worker_owner >= 0 || coordinator->worker.active)
		return true;
	switch (client->request.opcode)
	{
		case PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE:
			if (!client->is_admin ||
				!pg_strong_random(operation_id, sizeof(operation_id)) ||
				!bytes_nonzero(operation_id, sizeof(operation_id)))
				return false;
			memcpy(client->operation_id, operation_id,
				   sizeof(client->operation_id));
			client->action = PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE;
			if (client->request.old_node_id < 0 ||
				client->request.old_node_id >= PGRAC_FENCED_MAX_NODES ||
				!coordinator->operation_context->config->
					nodes[client->request.old_node_id].present)
				return start_worker(coordinator, slot, client->action,
					client->operation_id);
			node = &coordinator->operation_context->config->
				nodes[client->request.old_node_id];
			reserved = reserve_client_target(coordinator, client,
				node->target_uuid);
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_ERROR)
				return false;
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING)
			{
				client->state =
					PGRAC_FENCED_REJOIN_CLIENT_WAIT_RESERVATION;
				return true;
			}
			return start_worker(coordinator, slot, client->action,
				client->operation_id);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT:
			if (client->is_admin || client->owns_operation)
				return client_abandon(coordinator, slot);
			client->action = PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT;
			target = pgrac_fenced_rejoin_claim_target(
				&coordinator->rejoin_context);
			if (target == NULL)
				return start_worker(coordinator, slot, client->action, NULL);
			reserved = reserve_client_target(coordinator, client,
				target->target_uuid);
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_ERROR)
				return false;
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING)
			{
				client->state =
					PGRAC_FENCED_REJOIN_CLIENT_WAIT_RESERVATION;
				return true;
			}
			return start_worker(coordinator, slot, client->action, NULL);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON:
			target = pgrac_fenced_rejoin_request_target(
				&coordinator->rejoin_context, &client->request);
			if (!client->owns_operation || target == NULL ||
				memcmp(client->operation_id, client->request.operation_id,
					sizeof(client->operation_id)) != 0)
				return client_abandon(coordinator, slot);
			client->action = PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON;
			reserved = reserve_client_target(coordinator, client,
				target->target_uuid);
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_ERROR)
				return false;
			if (reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING)
			{
				client->state =
					PGRAC_FENCED_REJOIN_CLIENT_WAIT_RESERVATION;
				return true;
			}
			reserved = pgrac_fenced_coordinator_rejoin_invalidate_target(
				coordinator->scalar_coordinator, target->target_uuid,
				PGRAC_FENCED_REJOIN_INVALIDATE_REASON);
			if (reserved != PGRAC_FENCED_REJOIN_TARGET_READY)
				return reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING;
			return start_worker(coordinator, slot, client->action, NULL);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON:
			target = pgrac_fenced_rejoin_request_target(
				&coordinator->rejoin_context, &client->request);
			if (!client->owns_operation || !client->target_reserved ||
				target == NULL ||
				memcmp(client->operation_id, client->request.operation_id,
					sizeof(client->operation_id)) != 0 ||
				memcmp(client->reserved_target, target->target_uuid,
					sizeof(client->reserved_target)) != 0)
				return client_abandon(coordinator, slot);
			client->action = PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON;
			return start_worker(coordinator, slot, client->action, NULL);
		case PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CANCEL:
			if (!client->owns_operation ||
				memcmp(client->operation_id, client->request.operation_id,
					sizeof(client->operation_id)) != 0)
				return client_abandon(coordinator, slot);
			if (!pgrac_fenced_rejoin_cancel(&coordinator->rejoin_context,
					&client->request))
				return false;
			client->owns_operation = false;
			return client_abandon(coordinator, slot);
		default:
			return client_abandon(coordinator, slot);
	}
}

static bool
service_wait_reservation(PgracFencedRejoinCoordinatorV1 *coordinator,
			 uint32 slot)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];
	PgracFencedCoordinatorRejoinTargetResult reserved;

	if (!client->target_reserved)
		return false;
	reserved = pgrac_fenced_coordinator_rejoin_acquire_target(
		coordinator->scalar_coordinator, client->reserved_target);
	if (reserved == PGRAC_FENCED_REJOIN_TARGET_ERROR)
		return false;
	if (reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING ||
		coordinator->worker_owner >= 0 || coordinator->worker.active)
		return true;
	if (client->action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE)
		return start_worker(coordinator, slot, client->action,
			client->operation_id);
	if (client->action != PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT &&
		client->action != PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON)
		return false;
	if (client->action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON)
	{
		reserved = pgrac_fenced_coordinator_rejoin_invalidate_target(
			coordinator->scalar_coordinator, client->reserved_target,
			PGRAC_FENCED_REJOIN_INVALIDATE_REASON);
		if (reserved != PGRAC_FENCED_REJOIN_TARGET_READY)
			return reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING;
	}
	return start_worker(coordinator, slot, client->action, NULL);
}

static bool
response_positive(PgracFencedRejoinAsyncAction action,
			  const PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	return (action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE &&
		response->opcode ==
			PGRAC_EXTERNAL_FENCE_REJOIN_ADMIN_PREPARE_RESULT &&
		response->status == PGRAC_FENCED_REJOIN_STATUS_OFFERED) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT &&
		response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER &&
		response->status == PGRAC_FENCED_REJOIN_STATUS_OFFERED) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON &&
		response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT &&
		response->status == PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER) ||
		(action == PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON &&
		response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT &&
		response->status == PGRAC_FENCED_REJOIN_STATUS_READY);
}

static bool
service_worker(PgracFencedRejoinCoordinatorV1 *coordinator)
{
	PgracFencedRejoinAsyncEvent event;
	PgracFencedRejoinClientV1 *client;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	struct pollfd descriptor;
	bool positive;
	int owner;

	if (!coordinator->worker.active)
		return true;
	descriptor.fd = pgrac_fenced_rejoin_async_fd(&coordinator->worker);
	descriptor.events = POLLIN | POLLHUP | POLLERR;
	descriptor.revents = 0;
	if (poll(&descriptor, 1, 0) <= 0)
		return true;
	if (!pgrac_fenced_rejoin_async_service(coordinator->operation_context,
			&coordinator->rejoin_context, &coordinator->worker, &event,
			&response))
		return false;
	if (event != PGRAC_FENCED_REJOIN_ASYNC_COMPLETE)
		return true;
	owner = coordinator->worker_owner;
	coordinator->worker_owner = -1;
	if (coordinator->worker.action == PGRAC_FENCED_REJOIN_ASYNC_CLEANUP)
	{
		const PgracFencedRejoinAsyncWorkerV1 *worker = &coordinator->worker;

		if (owner != -1 || !worker->owned)
			return false;
		if (!pgrac_fenced_journal_rejoin_terminal(&worker->last_record))
			return true;
		return pgrac_fenced_rejoin_detach(&coordinator->rejoin_context,
			worker->identity.operation_id) &&
			pgrac_fenced_coordinator_rejoin_release_target(coordinator->scalar_coordinator,
				worker->identity.intent.target_uuid);
	}
	if (owner < 0 || owner >= PGRAC_FENCED_REJOIN_MAX_CLIENTS)
		return false;
	client = &coordinator->clients[owner];
	if (client->state != PGRAC_FENCED_REJOIN_CLIENT_WORKER)
		return false;
	positive = response_positive(client->action, &response);
	if (client->action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT && positive)
	{
		memcpy(client->operation_id, response.operation_id,
			   sizeof(client->operation_id));
		client->owns_operation = true;
	}
	if (client->action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE &&
		client->abandoned && positive)
	{
		memcpy(client->operation_id, response.operation_id,
			   sizeof(client->operation_id));
		client->owns_operation = true;
	}
	if ((client->action == PGRAC_FENCED_REJOIN_ASYNC_ADMIN_PREPARE ||
		 (client->action == PGRAC_FENCED_REJOIN_ASYNC_CLAIM_NEXT &&
		  !positive)) && !client_release_target(coordinator, client))
		return false;
	if ((client->action == PGRAC_FENCED_REJOIN_ASYNC_AUTHORIZE_ON ||
		 client->action == PGRAC_FENCED_REJOIN_ASYNC_REFRESH_ON) &&
		!positive && !client_cancel_operation(coordinator, client))
		return false;
	if (client->abandoned)
	{
		if (!client_cancel_operation(coordinator, client))
			return false;
		client_clear(coordinator, (uint32) owner);
		return true;
	}
	client->response = response;
	if (!pgrac_external_fence_rejoin_v1_encode(&client->response,
			client->response_frame))
		return false;
	client->response_frame_sent = 0;
	client->state = PGRAC_FENCED_REJOIN_CLIENT_EGRESS;
	return true;
}

static bool
service_egress(PgracFencedRejoinCoordinatorV1 *coordinator, uint32 slot)
{
	PgracFencedRejoinClientV1 *client = &coordinator->clients[slot];
	bool positive = response_positive(client->action, &client->response);
	int flags = MSG_DONTWAIT;
	ssize_t written;

#ifdef MSG_NOSIGNAL
	flags |= MSG_NOSIGNAL;
#endif
	while (client->response_frame_sent <
		PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES)
	{
		written = send(client->fd,
			client->response_frame + client->response_frame_sent,
			PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES -
			client->response_frame_sent, flags);
		if (written > 0)
		{
			client->response_frame_sent += (size_t) written;
			continue;
		}
		if (written < 0 && errno == EINTR)
			continue;
		if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			return true;
		return client_abandon(coordinator, slot);
	}
	if (client->is_admin)
	{
		client_clear(coordinator, slot);
		return true;
	}
	if (!positive)
		return client_abandon(coordinator, slot);
	memset(client->request_frame, 0, sizeof(client->request_frame));
	client->request_frame_used = 0;
	client->response_frame_sent = 0;
	memset(&client->request, 0, sizeof(client->request));
	memset(&client->response, 0, sizeof(client->response));
	client->action = 0;
	client->operation_deadline_mono_ns = 0;
	client->state = PGRAC_FENCED_REJOIN_CLIENT_INGRESS;
	return true;
}

static bool
service_clients(PgracFencedRejoinCoordinatorV1 *coordinator,
			uint64 now_mono_ns)
{
	PgracFencedRejoinClientV1 *client;
	uint32 i;

	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_CLIENTS; i++)
	{
		client = &coordinator->clients[i];
		if (client->state == PGRAC_FENCED_REJOIN_CLIENT_UNUSED)
			continue;
		if (client->fd < 0)
			continue;
		if (now_mono_ns >= client->transport_deadline_mono_ns ||
			(client->operation_deadline_mono_ns != 0 &&
			 now_mono_ns >= client->operation_deadline_mono_ns))
		{
			if (!client_abandon(coordinator, i))
				return false;
			continue;
		}
		if (client->state == PGRAC_FENCED_REJOIN_CLIENT_INGRESS)
		{
			if (!client_receive_request(coordinator, i, now_mono_ns))
				return false;
			continue;
		}
		if (client->state == PGRAC_FENCED_REJOIN_CLIENT_EGRESS)
		{
			if (!service_egress(coordinator, i))
				return false;
			continue;
		}
		if (client_has_event(client->fd))
		{
			if (!client_abandon(coordinator, i))
				return false;
			continue;
		}
		if (client->state ==
				PGRAC_FENCED_REJOIN_CLIENT_WAIT_RESERVATION)
		{
			if (!service_wait_reservation(coordinator, i))
				return false;
			continue;
		}
		if (client->state == PGRAC_FENCED_REJOIN_CLIENT_READY &&
			!start_ready_client(coordinator, i))
			return false;
	}
	return true;
}

/* PGRAC: one existing worker services detached operations without a client slot. */
static bool
service_owned_cleanup(PgracFencedRejoinCoordinatorV1 *coordinator, uint64 now)
{
	uint32 i;

	if (coordinator->operation_context->config->format_version != 2 ||
		coordinator->worker.active || coordinator->worker_owner >= 0)
		return true;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_OPERATIONS; i++)
	{
		uint32 slot = (coordinator->cleanup_cursor + i) % PGRAC_FENCED_REJOIN_MAX_OPERATIONS;
		PgracFencedRejoinOperationV1 *operation = &coordinator->rejoin_context.operations[slot];
		PgracFencedCoordinatorRejoinTargetResult reserved;
		uint64 duration;

		if (operation->state != PGRAC_FENCED_REJOIN_OPERATION_CLEANUP_REQUIRED)
			continue;
		reserved = pgrac_fenced_coordinator_rejoin_acquire_target(coordinator->scalar_coordinator,
			operation->target.target_uuid);
		if (reserved == PGRAC_FENCED_REJOIN_TARGET_ERROR)
			return false;
		if (reserved == PGRAC_FENCED_REJOIN_TARGET_WAITING)
			continue;
		duration = (uint64) operation->last_record.intent.request.rejoin.timeout_ms * UINT64_C(1000000);
		if (duration == 0 || UINT64_MAX - now < duration ||
			!pgrac_fenced_rejoin_async_start(coordinator->operation_context,
				&coordinator->rejoin_context, PGRAC_FENCED_REJOIN_ASYNC_CLEANUP,
				&operation->last_record.intent.request.rejoin, NULL, false,
				now + duration, &coordinator->worker))
			return false;
		coordinator->cleanup_cursor = (slot + 1) % PGRAC_FENCED_REJOIN_MAX_OPERATIONS;
		return true;
	}
	return true;
}

bool
pgrac_fenced_rejoin_coordinator_service(
	PgracFencedRejoinCoordinatorV1 *coordinator, uint64 now_mono_ns)
{
	if (coordinator == NULL || coordinator->operation_context == NULL ||
		!coordinator->operation_context->available || now_mono_ns == 0 ||
		!service_worker(coordinator))
		return false;
	return coordinator->quiescing ? true :
		service_clients(coordinator, now_mono_ns) && service_owned_cleanup(coordinator, now_mono_ns);
}

bool
pgrac_fenced_rejoin_coordinator_quiesce(
	PgracFencedRejoinCoordinatorV1 *coordinator)
{
	uint32 i;

	if (coordinator == NULL || coordinator->operation_context == NULL)
		return false;
	coordinator->quiescing = true;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_CLIENTS; i++)
	{
		if (coordinator->clients[i].state ==
				PGRAC_FENCED_REJOIN_CLIENT_UNUSED)
			continue;
		if (!client_abandon(coordinator, i))
			return false;
	}
	return true;
}

bool
pgrac_fenced_rejoin_coordinator_shutdown(
	PgracFencedRejoinCoordinatorV1 *coordinator)
{
	uint32 i;

	if (coordinator == NULL || coordinator->operation_context == NULL ||
		coordinator->worker.active || coordinator->worker_owner >= 0)
		return false;
	for (i = 0; i < PGRAC_FENCED_REJOIN_MAX_CLIENTS; i++)
	{
		if (coordinator->clients[i].state ==
				PGRAC_FENCED_REJOIN_CLIENT_UNUSED)
			continue;
		if (!client_cancel_operation(coordinator, &coordinator->clients[i]))
			return false;
		client_clear(coordinator, i);
	}
	memset(coordinator->rejoin_context.operations, 0,
		   sizeof(coordinator->rejoin_context.operations));
	coordinator->rejoin_context.operation_count = 0;
	return true;
}

uint32
pgrac_fenced_rejoin_coordinator_active_worker_count(
	const PgracFencedRejoinCoordinatorV1 *coordinator)
{
	return coordinator != NULL && coordinator->worker.active ? 1 : 0;
}

uint32
pgrac_fenced_rejoin_coordinator_operation_count(
	const PgracFencedRejoinCoordinatorV1 *coordinator)
{
	return coordinator == NULL ? 0 :
		coordinator->rejoin_context.operation_count;
}
