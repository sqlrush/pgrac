/*-------------------------------------------------------------------------
 *
 * test_cluster_external_fence.c
 *	  RF-ROOT P4 provider-neutral external-fence ABI tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_guc.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/pgrac_external_fence_protocol.h"

#include <stdlib.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#undef printf
#undef fprintf
#undef snprintf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

static uint32 stub_local_capabilities;
static ClusterFormationWitnessResult stub_formation_result;
static uint16 stub_formation_origin_thread;
static ClusterFenceAuthorityProof stub_formation_authority;
static ClusterFormationSnapshotV1 stub_formation_snapshot;
static uint64 stub_external_admit_requested;
static uint64 stub_external_write_excluded;
static uint64 stub_external_unavailable;
static bool stub_runtime_active;
static bool stub_root_peer_authenticated;
static int stub_root_transport_fd = -1;
static int stub_connect_lease_ms = -1;
static int stub_root_transport_fds[CLUSTER_MAX_NODES];
static uint32 stub_root_transport_count;
static uint32 stub_root_transport_index;
static bool stub_rejoin_failure_current;
static bool stub_rejoin_grd_current;
static ClusterReconfigRejoinFailureSnapshotV1 stub_rejoin_failure;
static ClusterGrdRejoinClearSnapshotV1 stub_rejoin_grd;
static bool stub_rejoin_pending_current;
static bool stub_rejoin_pending_is_ready;
static ClusterReconfigRejoinPendingSnapshotV1 stub_rejoin_pending;
static ClusterControlRootResult stub_root_revalidate_result;
static ClusterControlRootSnapshot stub_root_revalidate_snapshot;
static ClusterMembershipState stub_membership_state;
static uint64 stub_admitted_floor;
static uint64 stub_cluster_epoch;
static bool stub_protected_set_identity_available;
static ClusterProtectedSetIdentityV1 stub_protected_set_identity;

extern bool cluster_external_fence_runtime_active(void);
extern bool
cluster_external_fence_rejoin_protected_set_digest(uint8 out[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES]);
extern bool cluster_external_fence_test_runtime_active(void);
extern int cluster_external_fence_test_connect_root_daemon(int timeout_ms);
extern bool cluster_external_fence_test_root_peer_authenticated(int fd);
extern int
cluster_external_fence_test_admission_lease_ms(const PgracExternalFenceAdmissionV1 *admission);
extern bool cluster_formation_witness_copy_classification_v1(
	const ClusterFormationWitnessV1 *witness, uint16 *origin_thread,
	ClusterFenceAuthorityProof *authority, ClusterFormationSnapshotV1 *snapshot);

char *cluster_external_fence_socket_path = "/var/run/pgrac/pgrac-fenced.sock";
int cluster_write_fence_lease_ms = 6000;

bool
cluster_external_fence_test_runtime_active(void)
{
	return stub_runtime_active;
}

int
cluster_external_fence_test_connect_root_daemon(int timeout_ms)
{
	int fd;

	(void)timeout_ms;
	if (stub_root_transport_index < stub_root_transport_count) {
		fd = stub_root_transport_fds[stub_root_transport_index];
		stub_root_transport_fds[stub_root_transport_index] = -1;
		stub_root_transport_index++;
		return fd;
	}
	fd = stub_root_transport_fd;
	stub_root_transport_fd = -1;
	if (stub_connect_lease_ms >= 0)
		cluster_write_fence_lease_ms = stub_connect_lease_ms;
	return fd;
}

bool
cluster_external_fence_test_root_peer_authenticated(int fd)
{
	(void)fd;
	return stub_root_peer_authenticated;
}

bool
cluster_shared_fs_get_protected_set_identity(ClusterProtectedSetIdentityV1 *out)
{
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (!stub_protected_set_identity_available || out == NULL)
		return false;
	*out = stub_protected_set_identity;
	return true;
}

void
cluster_write_fence_note_external_admit_requested(void)
{
	stub_external_admit_requested++;
}

void
cluster_write_fence_note_external_unavailable(void)
{
	stub_external_unavailable++;
}

void
cluster_write_fence_note_external_write_excluded(uint64 journal_seq, uint64 verified_mono_ns)
{
	(void)journal_seq;
	(void)verified_mono_ns;
	stub_external_write_excluded++;
}

void
cluster_write_fence_note_external_rejected(void)
{}
void
cluster_write_fence_note_external_unknown(void)
{}
void
cluster_write_fence_note_external_identity_mismatch(void)
{}
void
cluster_write_fence_note_external_expired(void)
{}
void
cluster_write_fence_note_external_daemon_disconnect(void)
{}

void
pfree(void *pointer)
{
	free(pointer);
}

void *
palloc0(Size size)
{
	return calloc(1, size);
}

void *
palloc(Size size)
{
	return malloc(size);
}

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	printf("# Assert failed: %s at %s:%d\n", condition_name, file_name, line_number);
	abort();
}

uint32
cluster_ic_local_capability_word(void)
{
	return stub_local_capabilities;
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness)
{
	(void)witness;
	return stub_formation_result;
}

bool
cluster_formation_witness_copy_classification_v1(const ClusterFormationWitnessV1 *witness,
												 uint16 *origin_thread,
												 ClusterFenceAuthorityProof *authority,
												 ClusterFormationSnapshotV1 *snapshot)
{
	if (witness == NULL || origin_thread == NULL || authority == NULL || snapshot == NULL)
		return false;
	*origin_thread = stub_formation_origin_thread;
	*authority = stub_formation_authority;
	*snapshot = stub_formation_snapshot;
	return true;
}

bool
cluster_recovery_duty_digest_v1(const ClusterRecoveryDutyKey *key, ClusterRecoveryDutyDigest *out)
{
	if (!cluster_recovery_duty_key_valid_v1(key) || out == NULL)
		return false;
	memset(out->bytes, 0xa7, sizeof(out->bytes));
	return true;
}

bool
cluster_reconfig_rejoin_failure_snapshot(int32 old_node_id, uint64 old_incarnation,
										 ClusterReconfigRejoinFailureSnapshotV1 *out_failure)
{
	if (out_failure != NULL)
		memset(out_failure, 0, sizeof(*out_failure));
	if (!stub_rejoin_failure_current || out_failure == NULL
		|| old_node_id != stub_rejoin_failure.old_node_id
		|| old_incarnation != stub_rejoin_failure.old_incarnation)
		return false;
	*out_failure = stub_rejoin_failure;
	return true;
}

bool
cluster_grd_rejoin_clear_snapshot(const ClusterReconfigRejoinFailureSnapshotV1 *failure,
								  ClusterGrdRejoinClearSnapshotV1 *out_clear)
{
	if (out_clear != NULL)
		memset(out_clear, 0, sizeof(*out_clear));
	if (!stub_rejoin_grd_current || failure == NULL || out_clear == NULL
		|| memcmp(failure, &stub_rejoin_failure, sizeof(*failure)) != 0)
		return false;
	*out_clear = stub_rejoin_grd;
	return true;
}

bool
cluster_reconfig_rejoin_pending_snapshot(const ClusterReconfigRejoinFailureSnapshotV1 *failure,
										 uint64 candidate_incarnation,
										 ClusterReconfigRejoinPendingSnapshotV1 *out_pending)
{
	if (out_pending != NULL)
		memset(out_pending, 0, sizeof(*out_pending));
	if (!stub_rejoin_pending_current || failure == NULL || out_pending == NULL
		|| memcmp(failure, &stub_rejoin_failure, sizeof(*failure)) != 0
		|| candidate_incarnation != stub_rejoin_pending.candidate_incarnation)
		return false;
	*out_pending = stub_rejoin_pending;
	return true;
}

bool
cluster_reconfig_rejoin_pending_ready(const ClusterReconfigRejoinPendingSnapshotV1 *pending)
{
	return stub_rejoin_pending_is_ready && pending != NULL
		   && memcmp(pending, &stub_rejoin_pending, sizeof(*pending)) == 0;
}

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootSnapshot *out_snapshot)
{
	(void)token;
	(void)expected_identity;
	if (out_snapshot != NULL)
		*out_snapshot = stub_root_revalidate_snapshot;
	return stub_root_revalidate_result;
}

void
cluster_join_marker_compute_crc(ClusterJoinCommitMarker *marker)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	marker->crc32c = (uint32)crc;
}

bool
cluster_join_marker_struct_valid(const ClusterJoinCommitMarker *marker, int32 expected_node)
{
	ClusterJoinCommitMarker copy;

	if (marker == NULL || marker->magic != CLUSTER_JCMK_MAGIC
		|| marker->version != CLUSTER_JCMK_VERSION || marker->node_id != expected_node)
		return false;
	copy = *marker;
	cluster_join_marker_compute_crc(&copy);
	return copy.crc32c == marker->crc32c;
}

ClusterMembershipState
cluster_membership_get_state(int32 node_id)
{
	(void)node_id;
	return stub_membership_state;
}

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	(void)node_id;
	return stub_admitted_floor;
}

uint64
cluster_epoch_get_current(void)
{
	return stub_cluster_epoch;
}

static void
fill_valid_duty(ClusterRecoveryDutyKey *duty)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(duty, 0, sizeof(*duty));
	duty->system_identifier = UINT64_C(0x0123456789abcdef);
	for (i = 0; i < 16; i++) {
		duty->storage_uuid[i] = (uint8)(i + 1);
		duty->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	duty->authority_uuid[6] = 0x4a;
	duty->authority_uuid[8] = 0x8b;
	duty->origin_thread_id = 1;
	duty->origin_node_id = 0;
	duty->thread_claim_created_at = 123456;
	cluster_wal_thread_claim_fill(&claim, duty->origin_thread_id, duty->origin_node_id,
								  duty->thread_claim_created_at);
	duty->thread_claim_crc32c = claim.crc;
	duty->origin_owner_incarnation = UINT64_C(0x100000001);
	duty->root_lineage_seq = 7;
}

static void
prepare_two_writer_formation(const ClusterRecoveryDutyKey *duty)
{
	memset(&stub_formation_authority, 0, sizeof(stub_formation_authority));
	memset(&stub_formation_snapshot, 0, sizeof(stub_formation_snapshot));
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;
	stub_formation_origin_thread = duty->origin_thread_id;
	stub_formation_authority.total_disk_count = 3;
	stub_formation_authority.agree_disk_count = 2;
	stub_formation_authority.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	stub_formation_authority.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	stub_formation_authority.marker.fence_epoch = UINT64_C(4);
	stub_formation_authority.marker.fence_event_id = UINT64_C(99);
	stub_formation_authority.marker.fence_generation = UINT64_C(7);
	stub_formation_authority.marker.issuer_node_id = 3;
	stub_formation_authority.marker.fenced_dead_bitmap[0] = UINT8_C(0x05);
	stub_formation_authority.marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_FENCE;
	stub_formation_snapshot.excluded_bitmap[0] = UINT8_C(0x05);
	stub_formation_snapshot.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
	stub_formation_snapshot.membership.membership_state[2] = CLUSTER_MEMBER_REMOVED;
	stub_formation_snapshot.membership.last_admitted_incarnation[0]
		= duty->origin_owner_incarnation;
	stub_formation_snapshot.membership.last_admitted_incarnation[2] = UINT64_C(0x300000003);
	stub_formation_snapshot.local_epoch = UINT64_C(4);
	stub_local_capabilities = PGRAC_IC_HELLO_CAP_CONTROL_ROOT_V1;
	stub_runtime_active = true;
	stub_protected_set_identity_available = true;
	stub_protected_set_identity.backend_id = CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE;
	memcpy(stub_protected_set_identity.storage_uuid, duty->storage_uuid,
		   sizeof(stub_protected_set_identity.storage_uuid));
}

static bool
test_read_all(int fd, uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t got = read(fd, bytes + used, len - used);

		if (got <= 0)
			return false;
		used += (size_t)got;
	}
	return true;
}

static bool
test_write_all(int fd, const uint8 *bytes, size_t len)
{
	size_t used = 0;

	while (used < len) {
		ssize_t written = write(fd, bytes + used, len - used);

		if (written <= 0)
			return false;
		used += (size_t)written;
	}
	return true;
}

static int
test_only_affirmative_provider_child_timed(int fd, uint64 mapping_generation, uint64 freshness_ns,
										   int delay_ms, int close_control_fd)
{
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	struct timespec now;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
	uint8 closed_byte;

	if (!test_read_all(fd, request_frame, sizeof(request_frame))
		|| !pgrac_external_fence_request_v1_decode(request_frame, sizeof(request_frame), &request))
		return 1;
	if (delay_ms > 0) {
		struct timespec delay;

		delay.tv_sec = delay_ms / 1000;
		delay.tv_nsec = (delay_ms % 1000) * 1000000L;
		if (nanosleep(&delay, NULL) != 0)
			return 6;
	}
	memset(&response, 0, sizeof(response));
	response.verdict = PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED;
	memcpy(response.request_nonce, request.request_nonce, sizeof(response.request_nonce));
	if (!pgrac_external_fence_binding_from_request_v1(&request.need, mapping_generation,
													  &response.binding))
		return 2;
	memset(response.daemon_boot_id, 0x91, sizeof(response.daemon_boot_id));
	response.journal_seq = UINT64_C(10);
	if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
		return 3;
	response.verified_mono_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	response.fresh_until_mono_ns = response.verified_mono_ns + freshness_ns;
	response.proof_generation = UINT64_C(11);
	response.provider_id = UINT16_C(1);
	response.provider_abi_version = UINT16_C(1);
	response.provider_result = UINT32_C(0);
	memset(response.target_state_digest, 0x92, sizeof(response.target_state_digest));
	response.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (!pgrac_external_fence_response_v1_encode(&response, response_frame)
		|| !test_write_all(fd, response_frame, sizeof(response_frame)))
		return 4;
	if (close_control_fd >= 0)
		return read(close_control_fd, &closed_byte, 1) == 1 ? 0 : 5;
	/* Keep the connection alive until the admission owner releases it. */
	return read(fd, &closed_byte, 1) == 0 ? 0 : 5;
}

static int
test_only_affirmative_provider_child(int fd)
{
	return test_only_affirmative_provider_child_timed(fd, UINT64_C(9), UINT64_C(1000000000), 0, -1);
}

static int
test_only_negative_provider_child(int fd)
{
	PgracExternalFenceProtocolRequestV1 request;
	PgracExternalFenceProtocolResponseV1 response;
	uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
	uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];

	if (!test_read_all(fd, request_frame, sizeof(request_frame))
		|| !pgrac_external_fence_request_v1_decode(request_frame, sizeof(request_frame), &request))
		return 1;
	memset(&response, 0, sizeof(response));
	response.verdict = PGRAC_EXTERNAL_FENCE_UNKNOWN;
	memcpy(response.request_nonce, request.request_nonce, sizeof(response.request_nonce));
	response.provider_id = UINT16_C(1);
	response.provider_abi_version = UINT16_C(1);
	response.provider_result = UINT32_C(3);
	response.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN;
	if (!pgrac_external_fence_response_v1_encode(&response, response_frame)
		|| !test_write_all(fd, response_frame, sizeof(response_frame)))
		return 2;
	return 0;
}

static void
fill_valid_scalar_need(PgracExternalFenceNeedV1 *need)
{
	memset(need, 0, sizeof(*need));
	need->system_identifier = UINT64_C(0x0123456789abcdef);
	memset(need->canonical_duty_digest.bytes, 0xa5, sizeof(need->canonical_duty_digest.bytes));
	need->victim_node_id = 1;
	need->victim_incarnation = UINT64_C(0x100000001);
	memset(need->protected_set_digest, 0x5a, sizeof(need->protected_set_digest));
	need->predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need->predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
}

UT_TEST(test_external_fence_literals)
{
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED, 1);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_PREDICATE_REJOIN_ON, 2);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1, 1);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_MS, 5000);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_DIGEST_BYTES, 32);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_NEED_V1_BYTES, 96);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_BINDING_V1_BYTES, 104);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_OFFER_V1_BYTES, 40);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_NEED_V1_BYTES, 104);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_BINDING_V1_BYTES, 112);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_FRAME_V1_BYTES, 256);
}

UT_TEST(test_external_fence_enum_ordinals)
{
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED, 1);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_UNAVAILABLE, 4);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_DENY_NONE, 0);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR, 31);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_PENDING, 0);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_REJOIN_CONSUMED, 9);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_NEED_SET_OK, 0);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_NEED_SET_DUTY_INVALID, 2);
	UT_ASSERT_EQ(PGRAC_EXTERNAL_FENCE_NEED_SET_STORAGE_UNAVAILABLE, 9);
}

UT_TEST(test_external_fence_recovery_layouts)
{
	UT_ASSERT_EQ(sizeof(PgracExternalFenceWriterV1), 16);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceWriterV1, incarnation), 8);
	UT_ASSERT_EQ(sizeof(PgracExternalFenceWriterSetDigest), 32);
	UT_ASSERT_EQ(sizeof(PgracExternalFenceNeedV1), 96);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceNeedV1, canonical_duty_digest), 8);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceNeedV1, victim_node_id), 40);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceNeedV1, victim_incarnation), 48);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceNeedV1, protected_set_digest), 56);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceNeedV1, predicate_version), 92);
	UT_ASSERT_EQ(sizeof(PgracExternalFenceBindingV1), 104);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceBindingV1, canonical_duty_digest), 8);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceBindingV1, target_mapping_generation), 56);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceBindingV1, protected_set_digest), 64);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceBindingV1, predicate_version), 100);
}

UT_TEST(test_external_fence_rejoin_layouts)
{
	UT_ASSERT_EQ(sizeof(PgracExternalFenceRejoinOfferV1), 40);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinOfferV1, old_node_id), 16);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinOfferV1, candidate_incarnation), 32);
	UT_ASSERT_EQ(sizeof(PgracExternalFenceRejoinNeedV1), 104);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinNeedV1, protected_set_digest), 64);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinNeedV1, predicate_id), 96);
	UT_ASSERT_EQ(sizeof(PgracExternalFenceRejoinBindingV1), 112);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinBindingV1, target_mapping_generation), 64);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinBindingV1, protected_set_digest), 72);
	UT_ASSERT_EQ(offsetof(PgracExternalFenceRejoinBindingV1, predicate_version), 108);
}

UT_TEST(test_external_fence_rejoin_snapshot_layouts)
{
	UT_ASSERT_EQ(sizeof(ClusterReconfigRejoinFailureSnapshotV1), 80);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, reconfig_kind), 0);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, event_id), 8);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, dead_bitmap), 32);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, survivor_bitmap), 48);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, old_node_id), 64);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinFailureSnapshotV1, old_incarnation), 72);
	UT_ASSERT_EQ(sizeof(ClusterGrdRejoinClearSnapshotV1), 32);
	UT_ASSERT_EQ(offsetof(ClusterGrdRejoinClearSnapshotV1, dead_bitmap_hash), 8);
	UT_ASSERT_EQ(offsetof(ClusterGrdRejoinClearSnapshotV1, survivor_bitmap), 16);
	UT_ASSERT_EQ(sizeof(ClusterReconfigRejoinPendingSnapshotV1), 96);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, old_epoch), 16);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, dead_bitmap), 40);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, join_bitmap), 56);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, node_id), 72);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, candidate_incarnation), 80);
	UT_ASSERT_EQ(offsetof(ClusterReconfigRejoinPendingSnapshotV1, observed_slot_generation), 88);
}

UT_TEST(test_external_fence_no_generation_set_api)
{
	typedef PgracExternalFenceNeedSetResult (*BuildFn)(const ClusterRecoveryDutyKey *,
													   const ClusterFormationWitnessV1 *,
													   PgracExternalFenceNeedSetV1 **);
	typedef bool (*NeedRevalidateFn)(const PgracExternalFenceNeedSetV1 *,
									 const ClusterFormationWitnessV1 *,
									 PgracExternalFenceDenyReason *);
	typedef PgracExternalFenceVerdict (*AdmitFn)(const PgracExternalFenceNeedSetV1 *,
												 const ClusterFormationWitnessV1 *, int,
												 PgracExternalFenceAdmissionSetV1 **);
	typedef bool (*AdmissionRevalidateFn)(
		const PgracExternalFenceAdmissionSetV1 *, const PgracExternalFenceNeedSetV1 *,
		const ClusterFormationWitnessV1 *, PgracExternalFenceDenyReason *);

	UT_ASSERT(
		__builtin_types_compatible_p(__typeof__(&cluster_external_fence_need_set_build), BuildFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_external_fence_need_set_revalidate_nowait), NeedRevalidateFn));
	UT_ASSERT(
		__builtin_types_compatible_p(__typeof__(&cluster_external_fence_admit_set_wait), AdmitFn));
	UT_ASSERT(__builtin_types_compatible_p(
		__typeof__(&cluster_external_fence_revalidate_set_nowait), AdmissionRevalidateFn));
}

UT_TEST(test_external_fence_inactive_capability_builds_no_need_set)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	fill_valid_duty(&duty);
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;
	stub_local_capabilities = 0;
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_CAPABILITY_UNAVAILABLE);
	UT_ASSERT(needs == NULL);
}

UT_TEST(test_external_fence_need_set_builds_complete_sorted_writers)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	const ClusterFormationWitnessV1 *replacement = (const ClusterFormationWitnessV1 *)(uintptr_t)2;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	const PgracExternalFenceNeedV1 *first;
	const PgracExternalFenceNeedV1 *second;

	fill_valid_duty(&duty);
	memset(&stub_formation_authority, 0, sizeof(stub_formation_authority));
	memset(&stub_formation_snapshot, 0, sizeof(stub_formation_snapshot));
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;
	stub_formation_origin_thread = duty.origin_thread_id;
	stub_formation_authority.total_disk_count = 3;
	stub_formation_authority.agree_disk_count = 2;
	stub_formation_authority.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	stub_formation_authority.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	stub_formation_authority.marker.fence_epoch = UINT64_C(4);
	stub_formation_authority.marker.fence_event_id = UINT64_C(99);
	stub_formation_authority.marker.fence_generation = UINT64_C(7);
	stub_formation_authority.marker.issuer_node_id = 3;
	stub_formation_authority.marker.fenced_dead_bitmap[0] = UINT8_C(0x05);
	stub_formation_authority.marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_FENCE;
	stub_formation_snapshot.excluded_bitmap[0] = UINT8_C(0x05);
	stub_formation_snapshot.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
	stub_formation_snapshot.membership.membership_state[2] = CLUSTER_MEMBER_REMOVED;
	stub_formation_snapshot.membership.last_admitted_incarnation[0] = duty.origin_owner_incarnation;
	stub_formation_snapshot.membership.last_admitted_incarnation[2] = UINT64_C(0x300000003);
	stub_formation_snapshot.local_epoch = UINT64_C(4);
	stub_local_capabilities = PGRAC_IC_HELLO_CAP_CONTROL_ROOT_V1;
	stub_runtime_active = true;
	stub_protected_set_identity_available = true;
	stub_protected_set_identity.backend_id = CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE;
	memcpy(stub_protected_set_identity.storage_uuid, duty.storage_uuid,
		   sizeof(stub_protected_set_identity.storage_uuid));

	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_OK);
	UT_ASSERT(needs != NULL);
	UT_ASSERT_EQ(cluster_external_fence_need_set_count(needs), 2);
	first = cluster_external_fence_need_set_at(needs, 0);
	second = cluster_external_fence_need_set_at(needs, 1);
	UT_ASSERT(first != NULL);
	UT_ASSERT(second != NULL);
	if (first != NULL) {
		UT_ASSERT_EQ(first->victim_node_id, 0);
		UT_ASSERT_EQ(first->victim_incarnation, duty.origin_owner_incarnation);
	}
	if (second != NULL) {
		UT_ASSERT_EQ(second->victim_node_id, 2);
		UT_ASSERT_EQ(second->victim_incarnation, UINT64_C(0x300000003));
	}
	UT_ASSERT(cluster_external_fence_need_set_digest(needs) != NULL);
	UT_ASSERT(cluster_external_fence_need_set_revalidate_nowait(needs, formation, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	UT_ASSERT(!cluster_external_fence_need_set_revalidate_nowait(needs, replacement, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE);
	cluster_external_fence_need_set_release(&needs);
	UT_ASSERT(needs == NULL);
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
}

UT_TEST(test_external_fence_need_set_rejects_incomplete_authority)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	PgracExternalFenceNeedSetV1 *needs = NULL;

	fill_valid_duty(&duty);
	prepare_two_writer_formation(&duty);
	memset(stub_formation_authority.marker.fenced_dead_bitmap, 0,
		   sizeof(stub_formation_authority.marker.fenced_dead_bitmap));
	stub_formation_authority.marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_BASELINE;
	stub_formation_authority.marker.issuer_node_id = CLUSTER_FENCE_BASELINE_INITIAL_ISSUER;
	stub_formation_authority.marker.fence_event_id = 0;
	stub_formation_authority.marker.fence_generation = 0;
	memset(stub_formation_snapshot.excluded_bitmap, 0,
		   sizeof(stub_formation_snapshot.excluded_bitmap));
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_COUNT_INVALID);
	UT_ASSERT(needs == NULL);

	prepare_two_writer_formation(&duty);
	stub_formation_authority.marker.fenced_dead_bitmap[0] = UINT8_C(0x04);
	stub_formation_snapshot.excluded_bitmap[0] = UINT8_C(0x04);
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_ORIGINAL_OWNER_MISSING);
	UT_ASSERT(needs == NULL);

	prepare_two_writer_formation(&duty);
	stub_formation_snapshot.membership.last_admitted_incarnation[2] = 0;
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_INCAR_UNPROVEN);
	UT_ASSERT(needs == NULL);

	prepare_two_writer_formation(&duty);
	stub_formation_snapshot.membership.membership_state[2] = CLUSTER_MEMBER_JOINING;
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE);
	UT_ASSERT(needs == NULL);

	prepare_two_writer_formation(&duty);
	stub_protected_set_identity.storage_uuid[0] ^= UINT8_C(0xff);
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_STORAGE_UNAVAILABLE);
	UT_ASSERT(needs == NULL);
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
}

UT_TEST(test_external_fence_provider_zero_is_unavailable_without_admission)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *admission = NULL;

	memset(&need, 0, sizeof(need));
	need.system_identifier = 1;
	memset(need.canonical_duty_digest.bytes, 0xa5, sizeof(need.canonical_duty_digest.bytes));
	need.victim_node_id = 1;
	need.victim_incarnation = 1;
	memset(need.protected_set_digest, 0x5a, sizeof(need.protected_set_digest));
	need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	stub_external_admit_requested = 0;
	stub_external_unavailable = 0;
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 1000, &admission),
				 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(admission == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(),
				 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE);
	UT_ASSERT_EQ(stub_external_admit_requested, 1);
	UT_ASSERT_EQ(stub_external_unavailable, 1);
}

UT_TEST(test_external_fence_nonproof_signals_never_admit)
{
	static const char *const standalone_observations[]
		= { "ping_loss", "membership_dead", "victim_process_exit" };
	PgracExternalFenceNeedV1 need;
	uint32 i;

	memset(&need, 0, sizeof(need));
	need.system_identifier = UINT64_C(0x0123456789abcdef);
	memset(need.canonical_duty_digest.bytes, 0xa5, sizeof(need.canonical_duty_digest.bytes));
	need.victim_node_id = 1;
	need.victim_incarnation = UINT64_C(0x100000001);
	memset(need.protected_set_digest, 0x5a, sizeof(need.protected_set_digest));
	need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	stub_runtime_active = false;
	stub_root_transport_fd = -1;

	/*
	 * These observations are intentionally not inputs to the external-fence
	 * API.  Exercise each one as an isolated trigger and prove that, without
	 * the authenticated daemon readback path, no opaque admission exists.
	 */
	for (i = 0; i < lengthof(standalone_observations); i++) {
		PgracExternalFenceAdmissionV1 *admission = NULL;

		UT_ASSERT_NOT_NULL(standalone_observations[i]);
		UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 1000, &admission),
					 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
		UT_ASSERT(admission == NULL);
		UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(),
					 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE);
	}
}

UT_TEST(test_external_fence_test_only_exact_response_creates_live_admission)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *admission = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	const PgracExternalFenceBindingV1 *binding;
	int sockets[2];
	int status;
	pid_t child;

	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		int child_rc;

		(void)close(sockets[0]);
		child_rc = test_only_affirmative_provider_child(sockets[1]);
		(void)close(sockets[1]);
		_exit(child_rc);
	}
	(void)close(sockets[1]);
	stub_root_transport_fd = sockets[0];
	stub_runtime_active = true;
	stub_external_write_excluded = 0;

	memset(&need, 0, sizeof(need));
	need.system_identifier = UINT64_C(0x0123456789abcdef);
	memset(need.canonical_duty_digest.bytes, 0xa5, sizeof(need.canonical_duty_digest.bytes));
	need.victim_node_id = 1;
	need.victim_incarnation = UINT64_C(0x100000001);
	memset(need.protected_set_digest, 0x5a, sizeof(need.protected_set_digest));
	need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &admission),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	UT_ASSERT(admission != NULL);
	binding = cluster_external_fence_admission_binding(admission);
	UT_ASSERT(binding != NULL);
	if (binding != NULL) {
		UT_ASSERT_EQ(binding->victim_incarnation, UINT64_C(0x100000001));
		UT_ASSERT_EQ(binding->target_mapping_generation, UINT64_C(9));
	}
	UT_ASSERT(cluster_external_fence_revalidate_nowait(admission, &need, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	UT_ASSERT_EQ(stub_external_write_excluded, 1);
	cluster_external_fence_admission_release(admission);
	admission = NULL;
	if (stub_root_transport_fd >= 0) {
		(void)close(stub_root_transport_fd);
		stub_root_transport_fd = -1;
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	stub_runtime_active = false;
}

UT_TEST(test_external_fence_admission_pins_call_entry_lease_snapshot)
{
	const int entry_values[] = { 1000, 6000, 600000 };
	PgracExternalFenceNeedV1 need;
	int i;

	memset(&need, 0, sizeof(need));
	need.system_identifier = UINT64_C(0x0123456789abcdef);
	memset(need.canonical_duty_digest.bytes, 0xa5, sizeof(need.canonical_duty_digest.bytes));
	need.victim_node_id = 1;
	need.victim_incarnation = UINT64_C(0x100000001);
	memset(need.protected_set_digest, 0x5a, sizeof(need.protected_set_digest));
	need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	stub_runtime_active = true;
	for (i = 0; i < (int)lengthof(entry_values); i++) {
		PgracExternalFenceAdmissionV1 *admission = NULL;
		int sockets[2];
		int status;
		pid_t child;

		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
		child = fork();
		UT_ASSERT(child >= 0);
		if (child == 0) {
			int child_rc;

			(void)close(sockets[0]);
			child_rc = test_only_affirmative_provider_child(sockets[1]);
			(void)close(sockets[1]);
			_exit(child_rc);
		}
		(void)close(sockets[1]);
		stub_root_transport_fd = sockets[0];
		cluster_write_fence_lease_ms = entry_values[i];
		stub_connect_lease_ms = entry_values[(i + 1) % lengthof(entry_values)];
		UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &admission),
					 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
		UT_ASSERT(admission != NULL);
		UT_ASSERT_EQ(cluster_external_fence_test_admission_lease_ms(admission), entry_values[i]);
		cluster_external_fence_admission_release(admission);
		UT_ASSERT_EQ(waitpid(child, &status, 0), child);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
	stub_connect_lease_ms = -1;
	cluster_write_fence_lease_ms = 6000;
	stub_runtime_active = false;
}

UT_TEST(test_external_fence_live_admission_detects_daemon_close)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *admission = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	int sockets[2];
	int control[2];
	int status;
	pid_t child;

	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
	UT_ASSERT_EQ(pipe(control), 0);
	child = fork();
	UT_ASSERT(child >= 0);
	if (child == 0) {
		int child_rc;

		(void)close(sockets[0]);
		(void)close(control[1]);
		child_rc = test_only_affirmative_provider_child_timed(sockets[1], UINT64_C(9),
															  UINT64_C(1000000000), 0, control[0]);
		(void)close(control[0]);
		(void)close(sockets[1]);
		_exit(child_rc);
	}
	(void)close(sockets[1]);
	(void)close(control[0]);
	stub_root_transport_fd = sockets[0];
	stub_runtime_active = true;
	memset(&need, 0, sizeof(need));
	need.system_identifier = UINT64_C(0x0123456789abcdef);
	memset(need.canonical_duty_digest.bytes, 0xa5, sizeof(need.canonical_duty_digest.bytes));
	need.victim_node_id = 1;
	need.victim_incarnation = UINT64_C(0x100000001);
	memset(need.protected_set_digest, 0x5a, sizeof(need.protected_set_digest));
	need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
	need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &admission),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	UT_ASSERT(admission != NULL);
	UT_ASSERT_EQ(write(control[1], "x", 1), 1);
	(void)close(control[1]);
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	UT_ASSERT(!cluster_external_fence_revalidate_nowait(admission, &need, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED);
	cluster_external_fence_admission_release(admission);
	stub_runtime_active = false;
}

UT_TEST(test_external_fence_e1_child_owns_independent_admission)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *parent_admission = NULL;
	int parent_sockets[2];
	int child_sockets[2];
	pid_t parent_provider;
	pid_t child_provider;
	pid_t e1_child;
	int status;

	fill_valid_scalar_need(&need);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, parent_sockets), 0);
	parent_provider = fork();
	UT_ASSERT(parent_provider >= 0);
	if (parent_provider == 0) {
		int child_rc;

		(void)close(parent_sockets[0]);
		child_rc = test_only_affirmative_provider_child(parent_sockets[1]);
		(void)close(parent_sockets[1]);
		_exit(child_rc);
	}
	(void)close(parent_sockets[1]);
	stub_root_transport_fd = parent_sockets[0];
	stub_runtime_active = true;
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &parent_admission),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	UT_ASSERT(parent_admission != NULL);

	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, child_sockets), 0);
	child_provider = fork();
	UT_ASSERT(child_provider >= 0);
	if (child_provider == 0) {
		int child_rc;

		(void)close(child_sockets[0]);
		child_rc = test_only_affirmative_provider_child(child_sockets[1]);
		(void)close(child_sockets[1]);
		_exit(child_rc);
	}
	(void)close(child_sockets[1]);
	e1_child = fork();
	UT_ASSERT(e1_child >= 0);
	if (e1_child == 0) {
		PgracExternalFenceAdmissionV1 *own_admission = NULL;
		PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		int child_rc = 0;

		/* A checkpointer/restartpointer must reject an inherited pointer. */
		if (cluster_external_fence_revalidate_nowait(parent_admission, &need, &reason)
			|| reason != PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT)
			child_rc = 1;
		stub_root_transport_fd = child_sockets[0];
		stub_root_transport_count = 0;
		stub_root_transport_index = 0;
		if (child_rc == 0
			&& cluster_external_fence_admit_wait(&need, 5000, &own_admission)
				   != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED)
			child_rc = 2;
		if (child_rc == 0
			&& (own_admission == NULL
				|| !cluster_external_fence_revalidate_nowait(own_admission, &need, &reason)))
			child_rc = 3;
		cluster_external_fence_admission_release(own_admission);
		_exit(child_rc);
	}
	(void)close(child_sockets[0]);
	UT_ASSERT_EQ(waitpid(e1_child, &status, 0), e1_child);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	UT_ASSERT_EQ(waitpid(child_provider, &status, 0), child_provider);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);

	cluster_external_fence_admission_release(parent_admission);
	UT_ASSERT_EQ(waitpid(parent_provider, &status, 0), parent_provider);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	stub_runtime_active = false;
}

UT_TEST(test_external_fence_mapping_reload_closes_old_and_fresh_admits)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *old_admission = NULL;
	PgracExternalFenceAdmissionV1 *new_admission = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	const PgracExternalFenceBindingV1 *binding;
	int old_sockets[2];
	int old_control[2];
	int new_sockets[2];
	pid_t old_provider;
	pid_t new_provider;
	int status;

	fill_valid_scalar_need(&need);
	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, old_sockets), 0);
	UT_ASSERT_EQ(pipe(old_control), 0);
	old_provider = fork();
	UT_ASSERT(old_provider >= 0);
	if (old_provider == 0) {
		int child_rc;

		(void)close(old_sockets[0]);
		(void)close(old_control[1]);
		child_rc = test_only_affirmative_provider_child_timed(
			old_sockets[1], UINT64_C(9), UINT64_C(1000000000), 0, old_control[0]);
		(void)close(old_control[0]);
		(void)close(old_sockets[1]);
		_exit(child_rc);
	}
	(void)close(old_sockets[1]);
	(void)close(old_control[0]);
	stub_root_transport_fd = old_sockets[0];
	stub_runtime_active = true;
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &old_admission),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	binding = cluster_external_fence_admission_binding(old_admission);
	UT_ASSERT(binding != NULL);
	if (binding != NULL)
		UT_ASSERT_EQ(binding->target_mapping_generation, UINT64_C(9));

	/* The daemon closes old live admissions before enabling generation 10. */
	UT_ASSERT_EQ(write(old_control[1], "x", 1), 1);
	(void)close(old_control[1]);
	UT_ASSERT_EQ(waitpid(old_provider, &status, 0), old_provider);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	UT_ASSERT(!cluster_external_fence_revalidate_nowait(old_admission, &need, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED);
	cluster_external_fence_admission_release(old_admission);

	UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, new_sockets), 0);
	new_provider = fork();
	UT_ASSERT(new_provider >= 0);
	if (new_provider == 0) {
		int child_rc;

		(void)close(new_sockets[0]);
		child_rc = test_only_affirmative_provider_child_timed(new_sockets[1], UINT64_C(10),
															  UINT64_C(1000000000), 0, -1);
		(void)close(new_sockets[1]);
		_exit(child_rc);
	}
	(void)close(new_sockets[1]);
	stub_root_transport_fd = new_sockets[0];
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 5000, &new_admission),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	binding = cluster_external_fence_admission_binding(new_admission);
	UT_ASSERT(binding != NULL);
	if (binding != NULL)
		UT_ASSERT_EQ(binding->target_mapping_generation, UINT64_C(10));
	UT_ASSERT(cluster_external_fence_revalidate_nowait(new_admission, &need, &reason));
	cluster_external_fence_admission_release(new_admission);
	UT_ASSERT_EQ(waitpid(new_provider, &status, 0), new_provider);
	UT_ASSERT(WIFEXITED(status));
	UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	stub_runtime_active = false;
}

UT_TEST(test_external_fence_admit_set_ands_all_sorted_writers)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	const ClusterFormationWitnessV1 *replacement = (const ClusterFormationWitnessV1 *)(uintptr_t)2;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceAdmissionSetV1 *admissions = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	const PgracExternalFenceBindingV1 *binding;
	int sockets[2][2];
	pid_t children[2];
	int status;
	int i;

	fill_valid_duty(&duty);
	memset(&stub_formation_authority, 0, sizeof(stub_formation_authority));
	memset(&stub_formation_snapshot, 0, sizeof(stub_formation_snapshot));
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;
	stub_formation_origin_thread = duty.origin_thread_id;
	stub_formation_authority.total_disk_count = 3;
	stub_formation_authority.agree_disk_count = 2;
	stub_formation_authority.marker.magic = CLUSTER_FENCE_MARKER_MAGIC;
	stub_formation_authority.marker.version = CLUSTER_FENCE_MARKER_VERSION;
	stub_formation_authority.marker.fence_epoch = UINT64_C(4);
	stub_formation_authority.marker.fence_event_id = UINT64_C(99);
	stub_formation_authority.marker.fence_generation = UINT64_C(7);
	stub_formation_authority.marker.issuer_node_id = 3;
	stub_formation_authority.marker.fenced_dead_bitmap[0] = UINT8_C(0x05);
	stub_formation_authority.marker.marker_kind = CLUSTER_FENCE_MARKER_KIND_FENCE;
	stub_formation_snapshot.excluded_bitmap[0] = UINT8_C(0x05);
	stub_formation_snapshot.membership.membership_state[0] = CLUSTER_MEMBER_DEAD;
	stub_formation_snapshot.membership.membership_state[2] = CLUSTER_MEMBER_REMOVED;
	stub_formation_snapshot.membership.last_admitted_incarnation[0] = duty.origin_owner_incarnation;
	stub_formation_snapshot.membership.last_admitted_incarnation[2] = UINT64_C(0x300000003);
	stub_formation_snapshot.local_epoch = UINT64_C(4);
	stub_local_capabilities = PGRAC_IC_HELLO_CAP_CONTROL_ROOT_V1;
	stub_runtime_active = true;
	stub_protected_set_identity_available = true;
	stub_protected_set_identity.backend_id = CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE;
	memcpy(stub_protected_set_identity.storage_uuid, duty.storage_uuid,
		   sizeof(stub_protected_set_identity.storage_uuid));
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_OK);
	UT_ASSERT(needs != NULL);

	stub_root_transport_count = 2;
	stub_root_transport_index = 0;
	stub_external_admit_requested = 0;
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		children[i] = fork();
		UT_ASSERT(children[i] >= 0);
		if (children[i] == 0) {
			int child_rc;
			int prior;

			for (prior = 0; prior < i; prior++)
				(void)close(sockets[prior][0]);
			(void)close(sockets[i][0]);
			child_rc = test_only_affirmative_provider_child(sockets[i][1]);
			(void)close(sockets[i][1]);
			_exit(child_rc);
		}
		(void)close(sockets[i][1]);
		stub_root_transport_fds[i] = sockets[i][0];
	}
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(needs, formation, 5000, &admissions),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	UT_ASSERT(admissions != NULL);
	UT_ASSERT_EQ(cluster_external_fence_admission_set_count(admissions), 2);
	binding = cluster_external_fence_admission_set_binding_at(admissions, 0);
	UT_ASSERT(binding != NULL);
	if (binding != NULL)
		UT_ASSERT_EQ(binding->victim_node_id, 0);
	binding = cluster_external_fence_admission_set_binding_at(admissions, 1);
	UT_ASSERT(binding != NULL);
	if (binding != NULL)
		UT_ASSERT_EQ(binding->victim_node_id, 2);
	UT_ASSERT(cluster_external_fence_revalidate_set_nowait(admissions, needs, formation, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	UT_ASSERT(
		!cluster_external_fence_revalidate_set_nowait(admissions, needs, replacement, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE);
	UT_ASSERT_EQ(stub_external_admit_requested, 2);
	cluster_external_fence_admission_set_release(&admissions);
	UT_ASSERT(admissions == NULL);
	for (i = 0; i < 2; i++) {
		if (stub_root_transport_fds[i] >= 0) {
			(void)close(stub_root_transport_fds[i]);
			stub_root_transport_fds[i] = -1;
		}
		UT_ASSERT_EQ(waitpid(children[i], &status, 0), children[i]);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
	cluster_external_fence_need_set_release(&needs);
	stub_root_transport_count = 0;
	stub_root_transport_index = 0;
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
}

UT_TEST(test_external_fence_admit_set_reverses_partial_success)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceAdmissionSetV1 *admissions = NULL;
	int sockets[2][2];
	pid_t children[2];
	int status;
	int i;

	fill_valid_duty(&duty);
	prepare_two_writer_formation(&duty);
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_OK);
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
	for (i = 0; i < 2; i++) {
		children[i] = fork();
		UT_ASSERT(children[i] >= 0);
		if (children[i] == 0) {
			int child_rc;
			int j;

			for (j = 0; j < 2; j++) {
				(void)close(sockets[j][0]);
				if (j != i)
					(void)close(sockets[j][1]);
			}
			child_rc = i == 0 ? test_only_affirmative_provider_child(sockets[i][1])
							  : test_only_negative_provider_child(sockets[i][1]);
			(void)close(sockets[i][1]);
			_exit(child_rc);
		}
	}
	stub_root_transport_count = 2;
	stub_root_transport_index = 0;
	stub_external_admit_requested = 0;
	for (i = 0; i < 2; i++) {
		(void)close(sockets[i][1]);
		stub_root_transport_fds[i] = sockets[i][0];
	}
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(needs, formation, 5000, &admissions),
				 PGRAC_EXTERNAL_FENCE_UNKNOWN);
	UT_ASSERT(admissions == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(),
				 PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN);
	UT_ASSERT_EQ(stub_external_admit_requested, 2);
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(waitpid(children[i], &status, 0), children[i]);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
	cluster_external_fence_need_set_release(&needs);
	stub_root_transport_count = 0;
	stub_root_transport_index = 0;
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
}

UT_TEST(test_external_fence_admit_set_final_expiry_clears_whole_output)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceAdmissionSetV1 *admissions = NULL;
	int sockets[2][2];
	pid_t children[2];
	int status;
	int i;

	fill_valid_duty(&duty);
	prepare_two_writer_formation(&duty);
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_OK);
	for (i = 0; i < 2; i++)
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
	for (i = 0; i < 2; i++) {
		children[i] = fork();
		UT_ASSERT(children[i] >= 0);
		if (children[i] == 0) {
			int child_rc;
			int j;

			for (j = 0; j < 2; j++) {
				(void)close(sockets[j][0]);
				if (j != i)
					(void)close(sockets[j][1]);
			}
			child_rc = test_only_affirmative_provider_child_timed(
				sockets[i][1], UINT64_C(9), i == 0 ? UINT64_C(30000000) : UINT64_C(1000000000),
				i == 0 ? 0 : 100, -1);
			(void)close(sockets[i][1]);
			_exit(child_rc);
		}
	}
	stub_root_transport_count = 2;
	stub_root_transport_index = 0;
	for (i = 0; i < 2; i++) {
		(void)close(sockets[i][1]);
		stub_root_transport_fds[i] = sockets[i][0];
	}
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(needs, formation, 5000, &admissions),
				 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(admissions == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(), PGRAC_EXTERNAL_FENCE_DENY_EXPIRED);
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(waitpid(children[i], &status, 0), children[i]);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
	cluster_external_fence_need_set_release(&needs);
	stub_root_transport_count = 0;
	stub_root_transport_index = 0;
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
}

UT_TEST(test_external_rejoin_races_invalidate_sets_and_held_guard)
{
	ClusterRecoveryDutyKey duty;
	const ClusterFormationWitnessV1 *formation = (const ClusterFormationWitnessV1 *)(uintptr_t)1;
	PgracExternalFenceNeedSetV1 *needs = NULL;
	PgracExternalFenceAdmissionSetV1 *admissions = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	int sockets[2][2];
	int controls[2][2];
	pid_t children[2];
	int status;
	int i;

	fill_valid_duty(&duty);
	prepare_two_writer_formation(&duty);
	UT_ASSERT_EQ(cluster_external_fence_need_set_build(&duty, formation, &needs),
				 PGRAC_EXTERNAL_FENCE_NEED_SET_OK);
	UT_ASSERT(needs != NULL);

	/* A rejoin/new-configuration cut before the first connect is zero-I/O. */
	stub_root_transport_count = 0;
	stub_root_transport_index = 0;
	stub_formation_result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(needs, formation, 5000, &admissions),
				 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(admissions == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(),
				 PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE);
	UT_ASSERT_EQ(stub_root_transport_index, 0);
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;

	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets[i]), 0);
		UT_ASSERT_EQ(pipe(controls[i]), 0);
	}
	for (i = 0; i < 2; i++) {
		children[i] = fork();
		UT_ASSERT(children[i] >= 0);
		if (children[i] == 0) {
			int child_rc;
			int j;

			for (j = 0; j < 2; j++) {
				(void)close(sockets[j][0]);
				(void)close(controls[j][1]);
				if (j != i) {
					(void)close(sockets[j][1]);
					(void)close(controls[j][0]);
				}
			}
			child_rc = test_only_affirmative_provider_child_timed(
				sockets[i][1], UINT64_C(9), UINT64_C(1000000000), 0, controls[i][0]);
			(void)close(controls[i][0]);
			(void)close(sockets[i][1]);
			_exit(child_rc);
		}
	}
	stub_root_transport_count = 2;
	stub_root_transport_index = 0;
	for (i = 0; i < 2; i++) {
		(void)close(sockets[i][1]);
		(void)close(controls[i][0]);
		stub_root_transport_fds[i] = sockets[i][0];
	}
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(needs, formation, 5000, &admissions),
				 PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED);
	UT_ASSERT(admissions != NULL);

	/* AUTHORIZE closes every old fd; P/new epoch makes the set stale. */
	stub_formation_snapshot.local_epoch++;
	stub_formation_result = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	for (i = 0; i < 2; i++) {
		UT_ASSERT_EQ(write(controls[i][1], "x", 1), 1);
		(void)close(controls[i][1]);
		UT_ASSERT_EQ(waitpid(children[i], &status, 0), children[i]);
		UT_ASSERT(WIFEXITED(status));
		UT_ASSERT_EQ(WEXITSTATUS(status), 0);
	}
	UT_ASSERT(!cluster_external_fence_revalidate_set_nowait(admissions, needs, formation, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE);

	cluster_external_fence_admission_set_release(&admissions);
	cluster_external_fence_need_set_release(&needs);
	stub_root_transport_count = 0;
	stub_root_transport_index = 0;
	stub_runtime_active = false;
	stub_protected_set_identity_available = false;
	stub_formation_result = CLUSTER_FORMATION_WITNESS_READY;
}

UT_TEST(test_external_fence_entry_and_nowait_fail_closed)
{
	PgracExternalFenceNeedV1 need;
	PgracExternalFenceAdmissionV1 *admission = (PgracExternalFenceAdmissionV1 *)(uintptr_t)1;
	PgracExternalFenceAdmissionSetV1 *admissions = (PgracExternalFenceAdmissionSetV1 *)(uintptr_t)1;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;

	memset(&need, 0, sizeof(need));
	UT_ASSERT_EQ(cluster_external_fence_admit_wait(&need, 0, &admission),
				 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(admission == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(), PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT(!cluster_external_fence_revalidate_nowait(NULL, &need, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT_EQ(cluster_external_fence_admit_set_wait(NULL, NULL, 0, &admissions),
				 PGRAC_EXTERNAL_FENCE_UNAVAILABLE);
	UT_ASSERT(admissions == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(), PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	cluster_external_fence_admission_release(NULL);
	cluster_external_fence_admission_set_release(NULL);
}

UT_TEST(test_external_fence_rejoin_api_provider_zero_fails_closed)
{
	PgracExternalFenceRejoinOpV1 *op = (PgracExternalFenceRejoinOpV1 *)(uintptr_t)1;
	PgracExternalFenceRejoinAuthorityClearV1 *clear
		= (PgracExternalFenceRejoinAuthorityClearV1 *)(uintptr_t)1;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	ClusterControlRootSnapshot snapshot;
	ClusterReconfigRejoinPendingSnapshotV1 pending;
	ClusterJoinCommitMarker marker;

	memset(&snapshot, 0xa5, sizeof(snapshot));
	memset(&pending, 0, sizeof(pending));
	memset(&marker, 0, sizeof(marker));
	UT_ASSERT_EQ(cluster_external_fence_rejoin_start_async(1000, &op),
				 PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT(op == NULL);
	UT_ASSERT_EQ(cluster_external_fence_last_deny_reason(),
				 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(NULL, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT(cluster_external_fence_rejoin_offer(NULL) == NULL);
	UT_ASSERT_EQ(
		cluster_external_fence_rejoin_authority_clear_build(NULL, NULL, NULL, &clear, &reason),
		PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT(clear == NULL);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_authorize_on_async(NULL, &clear, NULL, NULL, NULL,
																  NULL, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_refresh_on_async(NULL, &pending, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT(!cluster_external_fence_rejoin_revalidate_root(NULL, &snapshot, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT(memcmp(&snapshot, &(ClusterControlRootSnapshot){ 0 }, sizeof(snapshot)) == 0);
	UT_ASSERT(!cluster_external_fence_rejoin_consume_nowait(NULL, &pending, &marker, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT);
	UT_ASSERT(cluster_external_fence_rejoin_binding(NULL) == NULL);
	cluster_external_fence_rejoin_release(NULL);
	cluster_external_fence_rejoin_release(&op);
	UT_ASSERT(op == NULL);
	cluster_external_fence_rejoin_authority_clear_release(NULL);
	cluster_external_fence_rejoin_authority_clear_release(&clear);
	UT_ASSERT(clear == NULL);
}

UT_TEST(test_external_fence_current_package_forbids_activation_but_binds_storage)
{
	uint8 digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint8 expected[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];

	UT_ASSERT(!cluster_external_fence_runtime_active());
	stub_protected_set_identity_available = false;
	memset(digest, 0xa5, sizeof(digest));
	UT_ASSERT(!cluster_external_fence_rejoin_protected_set_digest(digest));
	UT_ASSERT(memcmp(digest, (uint8[32]){ 0 }, sizeof(digest)) == 0);

	memset(&stub_protected_set_identity, 0, sizeof(stub_protected_set_identity));
	stub_protected_set_identity.backend_id = 2;
	memset(stub_protected_set_identity.storage_uuid, 0x5a,
		   sizeof(stub_protected_set_identity.storage_uuid));
	stub_protected_set_identity_available = true;
	UT_ASSERT(pgrac_external_fence_protected_set_digest_v1(stub_protected_set_identity.backend_id,
														   stub_protected_set_identity.storage_uuid,
														   expected));
	UT_ASSERT(cluster_external_fence_rejoin_protected_set_digest(digest));
	UT_ASSERT(memcmp(digest, expected, sizeof(digest)) == 0);
}

UT_TEST(test_external_fence_rejoin_rejects_unauthenticated_root_peer)
{
	PgracExternalFenceRejoinOpV1 *op = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	struct sockaddr_un address;
	char socket_path[sizeof(address.sun_path)];
	int listen_fd;
	int peer_fd = -1;

	listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	UT_ASSERT(listen_fd >= 0);
	if (listen_fd < 0)
		return;
	snprintf(socket_path, sizeof(socket_path), "/tmp/pgrac-rejoin-auth-%ld.sock", (long)getpid());
	(void)unlink(socket_path);
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, socket_path, sizeof(address.sun_path));
	UT_ASSERT_EQ(bind(listen_fd, (struct sockaddr *)&address, sizeof(address)), 0);
	UT_ASSERT_EQ(listen(listen_fd, 1), 0);
	cluster_external_fence_socket_path = socket_path;
	stub_root_peer_authenticated = false;
	UT_ASSERT_EQ(cluster_external_fence_rejoin_start_async(5000, &op),
				 PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT(op != NULL);
	peer_fd = accept(listen_fd, NULL, NULL);
	UT_ASSERT(peer_fd >= 0);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(op, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH);
	cluster_external_fence_rejoin_release(&op);
	UT_ASSERT(op == NULL);
	if (peer_fd >= 0)
		(void)close(peer_fd);
	(void)close(listen_fd);
	(void)unlink(socket_path);
	cluster_external_fence_socket_path = "/var/run/pgrac/pgrac-fenced.sock";
}

UT_TEST(test_external_fence_rejoin_claim_receives_exact_offer)
{
	PgracExternalFenceRejoinOpV1 *op = NULL;
	PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	PgracExternalFenceProtocolRejoinFrameV1 claim;
	PgracExternalFenceProtocolRejoinFrameV1 response;
	const PgracExternalFenceRejoinOfferV1 *offer;
	const PgracExternalFenceRejoinBindingV1 *binding;
	PgracExternalFenceRejoinAuthorityClearV1 *authority_clear = NULL;
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	ClusterGrdRejoinClearSnapshotV1 grd_clear;
	ClusterReconfigRejoinPendingSnapshotV1 pending;
	ClusterControlRootIdentity old_identity;
	ClusterControlRootSnapshot complete_snapshot;
	ClusterControlRootSnapshot fresh_snapshot;
	ClusterControlRootReadToken complete_token;
	ClusterWalThreadClaim claim_record;
	ClusterJoinCommitMarker committed_candidate;
	struct sockaddr_un address;
	struct timespec now;
	uint8 frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	char socket_path[sizeof(address.sun_path)];
	int listen_fd;
	int peer_fd = -1;
	ssize_t io_count;
	PgracExternalFenceRejoinStatus start_status;
	PgracExternalFenceRejoinStatus authorize_status;
	PgracExternalFenceRejoinStatus refresh_status;

	listen_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	UT_ASSERT(listen_fd >= 0);
	if (listen_fd < 0)
		return;
	snprintf(socket_path, sizeof(socket_path), "/tmp/pgrac-rejoin-%ld.sock", (long)getpid());
	(void)unlink(socket_path);
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, socket_path, sizeof(address.sun_path));
	UT_ASSERT(bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) == 0);
	UT_ASSERT(listen(listen_fd, 1) == 0);
	cluster_external_fence_socket_path = socket_path;
	stub_root_peer_authenticated = true;

	start_status = cluster_external_fence_rejoin_start_async(5000, &op);
	UT_ASSERT_EQ(start_status, PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT(op != NULL);
	if (start_status != PGRAC_EXTERNAL_FENCE_REJOIN_PENDING || op == NULL)
		goto cleanup;
	peer_fd = accept(listen_fd, NULL, NULL);
	UT_ASSERT(peer_fd >= 0);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(op, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	io_count = read(peer_fd, frame, sizeof(frame));
	UT_ASSERT_EQ(io_count, sizeof(frame));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &claim));
	UT_ASSERT_EQ(claim.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT);

	memset(&response, 0, sizeof(response));
	response.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER;
	memcpy(response.transport_nonce, claim.transport_nonce, sizeof(response.transport_nonce));
	memset(response.operation_id, 0x11, sizeof(response.operation_id));
	response.system_identifier = UINT64_C(0x0123456789abcdef);
	memset(response.protected_set_digest, 0x22, sizeof(response.protected_set_digest));
	response.old_node_id = 1;
	response.old_incarnation = UINT64_C(70);
	response.candidate_incarnation = UINT64_C(77);
	response.provider_id = 1;
	response.provider_abi_version = 1;
	response.target_mapping_generation = UINT64_C(3);
	memset(response.daemon_boot_id, 0x33, sizeof(response.daemon_boot_id));
	response.journal_seq = UINT64_C(4);
	UT_ASSERT(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	response.verified_mono_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	response.fresh_until_mono_ns = response.verified_mono_ns + UINT64_C(5000000000);
	response.proof_generation = UINT64_C(5);
	memset(response.target_state_digest, 0x44, sizeof(response.target_state_digest));
	response.status = PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED;
	response.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	UT_ASSERT_EQ(write(peer_fd, frame, sizeof(frame)), sizeof(frame));
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(op, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	offer = cluster_external_fence_rejoin_offer(op);
	UT_ASSERT(offer != NULL);
	if (offer == NULL)
		goto cleanup;
	UT_ASSERT_EQ(offer->old_node_id, 1);
	UT_ASSERT_EQ(offer->old_incarnation, UINT64_C(70));
	UT_ASSERT_EQ(offer->candidate_incarnation, UINT64_C(77));
	UT_ASSERT(memcmp(offer->operation_id, response.operation_id, sizeof(offer->operation_id)) == 0);

	memset(&failure, 0, sizeof(failure));
	failure.reconfig_kind = RECONFIG_KIND_FAIL_STOP;
	failure.event_id = UINT64_C(8);
	failure.new_epoch = UINT64_C(9);
	failure.cssd_dead_generation = UINT64_C(10);
	failure.dead_bitmap[0] = UINT8_C(1) << 1;
	failure.survivor_bitmap[0] = UINT8_C(1);
	failure.old_node_id = 1;
	failure.old_incarnation = UINT64_C(70);
	memset(&grd_clear, 0, sizeof(grd_clear));
	grd_clear.episode_epoch = failure.new_epoch;
	grd_clear.dead_bitmap_hash = UINT64_C(11);
	memcpy(grd_clear.survivor_bitmap, failure.survivor_bitmap, sizeof(grd_clear.survivor_bitmap));
	stub_rejoin_failure_current = true;
	stub_rejoin_grd_current = true;
	stub_rejoin_failure = failure;
	stub_rejoin_grd = grd_clear;

	grd_clear.survivor_bitmap[0] = UINT8_C(4);
	UT_ASSERT_EQ(cluster_external_fence_rejoin_authority_clear_build(op, &failure, &grd_clear,
																	 &authority_clear, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT(authority_clear == NULL);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR);
	grd_clear = stub_rejoin_grd;
	UT_ASSERT_EQ(cluster_external_fence_rejoin_authority_clear_build(op, &failure, &grd_clear,
																	 &authority_clear, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT);
	UT_ASSERT(authority_clear != NULL);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);

	fill_valid_duty(&old_identity);
	old_identity.origin_thread_id = 2;
	old_identity.origin_node_id = 1;
	old_identity.origin_owner_incarnation = UINT64_C(70);
	cluster_wal_thread_claim_fill(&claim_record, old_identity.origin_thread_id,
								  old_identity.origin_node_id,
								  old_identity.thread_claim_created_at);
	old_identity.thread_claim_crc32c = claim_record.crc;
	memset(&complete_snapshot, 0, sizeof(complete_snapshot));
	complete_snapshot.identity = old_identity;
	complete_snapshot.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	complete_snapshot.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID;
	complete_snapshot.root_publish_seq = UINT64_C(12);
	memset(&complete_token, 0, sizeof(complete_token));
	memcpy(complete_token.authority_uuid, old_identity.authority_uuid,
		   sizeof(complete_token.authority_uuid));
	complete_token.origin_thread_id = old_identity.origin_thread_id;
	complete_token.source = UINT8_C(1);
	complete_token.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	complete_token.root_lineage_seq = old_identity.root_lineage_seq;
	complete_token.file_txn_seq = UINT64_C(13);
	complete_token.root_publish_seq = complete_snapshot.root_publish_seq;
	complete_token.record_crc32c = UINT32_C(14);
	complete_token.root_flags = complete_snapshot.root_flags;
	authorize_status = cluster_external_fence_rejoin_authorize_on_async(
		op, &authority_clear, &old_identity, &complete_snapshot, &complete_token,
		response.protected_set_digest, &reason);
	UT_ASSERT_EQ(authorize_status, PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT(authority_clear == NULL);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	if (authorize_status != PGRAC_EXTERNAL_FENCE_REJOIN_PENDING || authority_clear != NULL)
		goto cleanup;
	io_count = read(peer_fd, frame, sizeof(frame));
	UT_ASSERT_EQ(io_count, sizeof(frame));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &claim));
	UT_ASSERT_EQ(claim.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON);
	UT_ASSERT(memcmp(claim.operation_id, response.operation_id, sizeof(claim.operation_id)) == 0);
	UT_ASSERT_EQ(claim.system_identifier, old_identity.system_identifier);
	UT_ASSERT(memcmp(claim.rejoin_gate_digest, (uint8[32]){ 0 }, sizeof(claim.rejoin_gate_digest))
			  != 0);
	UT_ASSERT(memcmp(claim.protected_set_digest, response.protected_set_digest,
					 sizeof(claim.protected_set_digest))
			  == 0);
	UT_ASSERT_EQ(claim.old_node_id, 1);
	UT_ASSERT_EQ(claim.old_incarnation, UINT64_C(70));
	UT_ASSERT_EQ(claim.candidate_incarnation, UINT64_C(77));
	UT_ASSERT(claim.timeout_ms >= 1);

	memset(&response, 0, sizeof(response));
	response.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT;
	memcpy(response.transport_nonce, claim.transport_nonce, sizeof(response.transport_nonce));
	memcpy(response.operation_id, claim.operation_id, sizeof(response.operation_id));
	response.system_identifier = claim.system_identifier;
	memcpy(response.rejoin_gate_digest, claim.rejoin_gate_digest,
		   sizeof(response.rejoin_gate_digest));
	memcpy(response.protected_set_digest, claim.protected_set_digest,
		   sizeof(response.protected_set_digest));
	response.old_node_id = claim.old_node_id;
	response.old_incarnation = claim.old_incarnation;
	response.candidate_incarnation = claim.candidate_incarnation;
	response.provider_id = 1;
	response.provider_abi_version = 1;
	response.target_mapping_generation = UINT64_C(3);
	memset(response.daemon_boot_id, 0x33, sizeof(response.daemon_boot_id));
	response.journal_seq = UINT64_C(6);
	UT_ASSERT(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	response.verified_mono_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	response.fresh_until_mono_ns = response.verified_mono_ns + UINT64_C(5000000000);
	response.proof_generation = UINT64_C(6);
	memset(response.target_state_digest, 0x55, sizeof(response.target_state_digest));
	response.status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER;
	response.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	UT_ASSERT_EQ(write(peer_fd, frame, sizeof(frame)), sizeof(frame));
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(op, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);

	memset(&pending, 0, sizeof(pending));
	pending.reconfig_kind = RECONFIG_KIND_JOIN_PENDING;
	pending.event_id = UINT64_C(16);
	pending.old_epoch = failure.new_epoch;
	pending.new_epoch = failure.new_epoch + 1;
	pending.cssd_dead_generation = failure.cssd_dead_generation;
	pending.join_bitmap[0] = UINT8_C(1) << 1;
	pending.node_id = 1;
	pending.candidate_incarnation = UINT64_C(77);
	pending.observed_slot_generation = UINT64_C(17);
	stub_rejoin_pending = pending;
	stub_rejoin_pending_current = true;
	stub_rejoin_pending_is_ready = true;
	refresh_status = cluster_external_fence_rejoin_refresh_on_async(op, &pending, &reason);
	UT_ASSERT_EQ(refresh_status, PGRAC_EXTERNAL_FENCE_REJOIN_PENDING);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	if (refresh_status != PGRAC_EXTERNAL_FENCE_REJOIN_PENDING)
		goto cleanup;
	io_count = read(peer_fd, frame, sizeof(frame));
	UT_ASSERT_EQ(io_count, sizeof(frame));
	UT_ASSERT(pgrac_external_fence_rejoin_v1_decode(frame, sizeof(frame), &response));
	UT_ASSERT_EQ(response.opcode, PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON);
	UT_ASSERT(memcmp(response.operation_id, claim.operation_id, sizeof(response.operation_id))
			  == 0);
	UT_ASSERT(
		memcmp(response.transport_nonce, claim.transport_nonce, sizeof(response.transport_nonce))
		!= 0);
	UT_ASSERT(memcmp(response.rejoin_gate_digest, claim.rejoin_gate_digest,
					 sizeof(response.rejoin_gate_digest))
			  == 0);
	claim = response;
	memset(&response, 0, sizeof(response));
	response.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT;
	memcpy(response.transport_nonce, claim.transport_nonce, sizeof(response.transport_nonce));
	memcpy(response.operation_id, claim.operation_id, sizeof(response.operation_id));
	response.system_identifier = claim.system_identifier;
	memcpy(response.rejoin_gate_digest, claim.rejoin_gate_digest,
		   sizeof(response.rejoin_gate_digest));
	memcpy(response.protected_set_digest, claim.protected_set_digest,
		   sizeof(response.protected_set_digest));
	response.old_node_id = claim.old_node_id;
	response.old_incarnation = claim.old_incarnation;
	response.candidate_incarnation = claim.candidate_incarnation;
	response.provider_id = 1;
	response.provider_abi_version = 1;
	response.target_mapping_generation = UINT64_C(3);
	memset(response.daemon_boot_id, 0x33, sizeof(response.daemon_boot_id));
	response.journal_seq = UINT64_C(7);
	UT_ASSERT(clock_gettime(CLOCK_MONOTONIC, &now) == 0);
	response.verified_mono_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	response.fresh_until_mono_ns = response.verified_mono_ns + UINT64_C(5000000000);
	response.proof_generation = UINT64_C(7);
	memset(response.target_state_digest, 0x66, sizeof(response.target_state_digest));
	response.status = PGRAC_EXTERNAL_FENCE_REJOIN_READY;
	response.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	UT_ASSERT(pgrac_external_fence_rejoin_v1_encode(&response, frame));
	UT_ASSERT_EQ(write(peer_fd, frame, sizeof(frame)), sizeof(frame));
	UT_ASSERT_EQ(cluster_external_fence_rejoin_poll_nowait(op, &reason),
				 PGRAC_EXTERNAL_FENCE_REJOIN_READY);
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	binding = cluster_external_fence_rejoin_binding(op);
	UT_ASSERT(binding != NULL);
	if (binding != NULL) {
		UT_ASSERT_EQ(binding->old_node_id, 1);
		UT_ASSERT_EQ(binding->candidate_incarnation, UINT64_C(77));
	}
	stub_root_revalidate_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	stub_root_revalidate_snapshot = complete_snapshot;
	memset(&fresh_snapshot, 0xa5, sizeof(fresh_snapshot));
	UT_ASSERT(cluster_external_fence_rejoin_revalidate_root(op, &fresh_snapshot, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	UT_ASSERT(memcmp(&fresh_snapshot, &complete_snapshot, sizeof(fresh_snapshot)) == 0);
	memset(&committed_candidate, 0, sizeof(committed_candidate));
	committed_candidate.magic = CLUSTER_JCMK_MAGIC;
	committed_candidate.version = CLUSTER_JCMK_VERSION;
	committed_candidate.node_id = 1;
	committed_candidate.phase = CLUSTER_JCMK_PHASE_COMMITTED;
	committed_candidate.generation = UINT64_C(77);
	committed_candidate.admitted_incarnation = UINT64_C(77);
	committed_candidate.admitted_epoch = pending.new_epoch + 1;
	committed_candidate.commit_nonce = UINT64_C(18);
	cluster_join_marker_compute_crc(&committed_candidate);
	stub_membership_state = CLUSTER_MEMBER_JOINING;
	stub_admitted_floor = failure.old_incarnation;
	stub_cluster_epoch = pending.new_epoch;
	UT_ASSERT(
		cluster_external_fence_rejoin_consume_nowait(op, &pending, &committed_candidate, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_NONE);
	UT_ASSERT(
		!cluster_external_fence_rejoin_consume_nowait(op, &pending, &committed_candidate, &reason));
	UT_ASSERT_EQ(reason, PGRAC_EXTERNAL_FENCE_DENY_REJOIN_CONSUMED);

cleanup:
	cluster_external_fence_rejoin_authority_clear_release(&authority_clear);
	cluster_external_fence_rejoin_release(&op);
	UT_ASSERT(op == NULL);
	if (peer_fd >= 0)
		(void)close(peer_fd);
	(void)close(listen_fd);
	(void)unlink(socket_path);
	cluster_external_fence_socket_path = "/var/run/pgrac/pgrac-fenced.sock";
	stub_root_peer_authenticated = false;
}

int
main(void)
{
	UT_PLAN(25);
	UT_RUN(test_external_fence_literals);
	UT_RUN(test_external_fence_enum_ordinals);
	UT_RUN(test_external_fence_recovery_layouts);
	UT_RUN(test_external_fence_rejoin_layouts);
	UT_RUN(test_external_fence_rejoin_snapshot_layouts);
	UT_RUN(test_external_fence_no_generation_set_api);
	UT_RUN(test_external_fence_inactive_capability_builds_no_need_set);
	UT_RUN(test_external_fence_need_set_builds_complete_sorted_writers);
	UT_RUN(test_external_fence_need_set_rejects_incomplete_authority);
	UT_RUN(test_external_fence_provider_zero_is_unavailable_without_admission);
	UT_RUN(test_external_fence_nonproof_signals_never_admit);
	UT_RUN(test_external_fence_test_only_exact_response_creates_live_admission);
	UT_RUN(test_external_fence_admission_pins_call_entry_lease_snapshot);
	UT_RUN(test_external_fence_live_admission_detects_daemon_close);
	UT_RUN(test_external_fence_e1_child_owns_independent_admission);
	UT_RUN(test_external_fence_mapping_reload_closes_old_and_fresh_admits);
	UT_RUN(test_external_fence_admit_set_ands_all_sorted_writers);
	UT_RUN(test_external_fence_admit_set_reverses_partial_success);
	UT_RUN(test_external_fence_admit_set_final_expiry_clears_whole_output);
	UT_RUN(test_external_rejoin_races_invalidate_sets_and_held_guard);
	UT_RUN(test_external_fence_entry_and_nowait_fail_closed);
	UT_RUN(test_external_fence_rejoin_api_provider_zero_fails_closed);
	UT_RUN(test_external_fence_current_package_forbids_activation_but_binds_storage);
	UT_RUN(test_external_fence_rejoin_rejects_unauthenticated_root_peer);
	UT_RUN(test_external_fence_rejoin_claim_receives_exact_offer);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
