/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_live_rejoin_client.c
 *    Explicit root-harness client for the live PFRJ daemon lifecycle.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include "pgrac_fenced_config.h"
#include "pgrac_fenced_rejoin.h"

static bool
write_exact(int fd, const uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t written = write(fd, bytes + used, len - used);

		if (written > 0) {
			used += (size_t)written;
			continue;
		}
		if (written < 0 && errno == EINTR)
			continue;
		return false;
	}
	return true;
}

static bool
read_exact(int fd, uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t got = read(fd, bytes + used, len - used);

		if (got > 0) {
			used += (size_t)got;
			continue;
		}
		if (got < 0 && errno == EINTR)
			continue;
		return false;
	}
	return true;
}

static bool
exchange(int fd, const PgracExternalFenceProtocolRejoinFrameV1 *request,
		 PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];

	return pgrac_external_fence_rejoin_v1_encode(request, request_frame)
		   && write_exact(fd, request_frame, sizeof(request_frame))
		   && read_exact(fd, response_frame, sizeof(response_frame))
		   && pgrac_external_fence_rejoin_v1_decode(response_frame, sizeof(response_frame),
													response)
		   && memcmp(request->transport_nonce, response->transport_nonce,
					 sizeof(request->transport_nonce))
				  == 0;
}

static bool
proof_fresh_exact(const PgracExternalFenceProtocolRejoinFrameV1 *response)
{
	struct timespec now;
	uint64 now_ns;

	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0)
		return false;
	now_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	return response->verified_mono_ns < response->fresh_until_mono_ns
		   && response->fresh_until_mono_ns - response->verified_mono_ns == UINT64_C(5000000000)
		   && now_ns < response->fresh_until_mono_ns;
}

static void
bound_request(uint16 opcode, const PgracExternalFenceProtocolRejoinFrameV1 *source, uint8 nonce,
			  PgracExternalFenceProtocolRejoinFrameV1 *request)
{
	memset(request, 0, sizeof(*request));
	request->opcode = opcode;
	memset(request->transport_nonce, nonce, sizeof(request->transport_nonce));
	memcpy(request->operation_id, source->operation_id, sizeof(request->operation_id));
	request->system_identifier = source->system_identifier;
	memset(request->rejoin_gate_digest, 0x91, sizeof(request->rejoin_gate_digest));
	memcpy(request->protected_set_digest, source->protected_set_digest,
		   sizeof(request->protected_set_digest));
	request->old_node_id = source->old_node_id;
	request->old_incarnation = source->old_incarnation;
	request->candidate_incarnation = source->candidate_incarnation;
	request->timeout_ms = 5000;
}

int
main(void)
{
	PgracExternalFenceProtocolRejoinFrameV1 request;
	PgracExternalFenceProtocolRejoinFrameV1 offer;
	PgracExternalFenceProtocolRejoinFrameV1 on_result;
	PgracExternalFenceProtocolRejoinFrameV1 ready;
	struct sockaddr_un address;
	int fd;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0)
		return 1;
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	if (strlcpy(address.sun_path, PGRAC_FENCED_DB_SOCKET_PATH, sizeof(address.sun_path))
			>= sizeof(address.sun_path)
		|| connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0)
		return 2;
	memset(&request, 0, sizeof(request));
	request.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memset(request.transport_nonce, 0x61, sizeof(request.transport_nonce));
	if (!exchange(fd, &request, &offer)) {
		fprintf(stderr, "CLAIM exchange failed: %s\n", strerror(errno));
		return 3;
	}
	if (offer.opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER
		|| offer.status != PGRAC_FENCED_REJOIN_STATUS_OFFERED || !proof_fresh_exact(&offer)) {
		fprintf(stderr,
				"CLAIM response rejected: opcode=%u status=%u reason=%u "
				"provider=%u/%u verified=%llu fresh_until=%llu generation=%llu\n",
				(unsigned)offer.opcode, (unsigned)offer.status, (unsigned)offer.deny_reason,
				(unsigned)offer.provider_id, (unsigned)offer.provider_abi_version,
				(unsigned long long)offer.verified_mono_ns,
				(unsigned long long)offer.fresh_until_mono_ns,
				(unsigned long long)offer.proof_generation);
		return 3;
	}
	bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON, &offer, 0x62, &request);
	if (!exchange(fd, &request, &on_result)) {
		fprintf(stderr, "AUTHORIZE exchange failed: %s\n", strerror(errno));
		return 4;
	}
	if (on_result.opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT
		|| on_result.status != PGRAC_FENCED_REJOIN_STATUS_WAITING_JOINER
		|| !proof_fresh_exact(&on_result) || on_result.proof_generation <= offer.proof_generation) {
		fprintf(stderr, "AUTHORIZE response rejected: opcode=%u status=%u generation=%llu\n",
				(unsigned)on_result.opcode, (unsigned)on_result.status,
				(unsigned long long)on_result.proof_generation);
		return 4;
	}
	bound_request(PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON, &on_result, 0x63, &request);
	if (!exchange(fd, &request, &ready)) {
		fprintf(stderr, "REFRESH exchange failed: %s\n", strerror(errno));
		return 5;
	}
	if (ready.opcode != PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT
		|| ready.status != PGRAC_FENCED_REJOIN_STATUS_READY || !proof_fresh_exact(&ready)
		|| ready.proof_generation <= on_result.proof_generation) {
		fprintf(stderr, "REFRESH response rejected: opcode=%u status=%u generation=%llu\n",
				(unsigned)ready.opcode, (unsigned)ready.status,
				(unsigned long long)ready.proof_generation);
		return 5;
	}
	printf("offer=%llu on=%llu ready=%llu\n", (unsigned long long)offer.proof_generation,
		   (unsigned long long)on_result.proof_generation,
		   (unsigned long long)ready.proof_generation);
	(void)close(fd);
	return 0;
}
