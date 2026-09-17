/*-------------------------------------------------------------------------
 *
 * cluster_external_fence.c
 *	  Provider-neutral external I/O fence backend boundary (RF-ROOT P4).
 *
 * The current approved package deliberately has no production provider:
 * provider id 0 is UNAVAILABLE and bit 24 is not advertised.  Consequently
 * this file keeps production fail-closed.  A compile-time unit-test seam may
 * supply an authenticated transport for the frozen protocol's positive-path
 * witnesses; it is absent from the production object and link map.
 *
 *-------------------------------------------------------------------------
 */
#ifdef __linux__
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include "postgres.h"

#ifndef WIN32
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>
#endif

#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "cluster/cluster_write_fence.h"
#include "common/cryptohash.h"
#include "common/pgrac_external_fence_protocol.h"

static PgracExternalFenceDenyReason external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;

#ifdef USE_CLUSTER_UNIT
extern bool cluster_external_fence_test_runtime_active(void);
extern int cluster_external_fence_test_connect_root_daemon(int timeout_ms);
extern bool cluster_external_fence_test_root_peer_authenticated(int fd);
extern int
cluster_external_fence_test_admission_lease_ms(const PgracExternalFenceAdmissionV1 *admission);
#endif

bool
cluster_external_fence_runtime_active(void)
{
#ifdef USE_CLUSTER_UNIT
	return cluster_external_fence_test_runtime_active();
#else
	/* STOP04 §11.7: provider 0 is the sole production registry outcome and
	 * no deployment certification exists, so bit24 positive activation is
	 * forbidden in this package. */
	return false;
#endif
}

bool
cluster_external_fence_rejoin_protected_set_digest(uint8 out[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES])
{
	ClusterProtectedSetIdentityV1 identity;

	if (out == NULL)
		return false;
	memset(out, 0, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
	memset(&identity, 0, sizeof(identity));
	if (!cluster_shared_fs_get_protected_set_identity(&identity)
		|| !pgrac_external_fence_protected_set_digest_v1(identity.backend_id, identity.storage_uuid,
														 out)) {
		memset(out, 0, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
		return false;
	}
	return true;
}

#define PGRAC_EXTERNAL_FENCE_ADMISSION_MAGIC UINT32_C(0x50464131)	  /* PFA1 */
#define PGRAC_EXTERNAL_FENCE_NEED_SET_MAGIC UINT32_C(0x50464e31)	  /* PFN1 */
#define PGRAC_EXTERNAL_FENCE_ADMISSION_SET_MAGIC UINT32_C(0x50465331) /* PFS1 */
#define PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC UINT32_C(0x50465231)	  /* PFR1 */
#define PGRAC_EXTERNAL_FENCE_REJOIN_CLEAR_MAGIC UINT32_C(0x50464331)  /* PFC1 */
#define PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS UINT64_C(5000000000)
#define PGRAC_EXTERNAL_FENCE_ROOT_SOURCE_PRIMARY UINT8_C(1)

#ifndef WIN32
static bool external_fence_monotonic_now(uint64 *out_now_ns);
#endif

static const uint8 external_fence_rejoin_clear_domain[] = "PGRAC-REJOIN-AUTHORITY-CLEAR-V1";
static const uint8 external_fence_rejoin_complete_domain[] = "PGRAC-REJOIN-COMPLETE-V1";
static const uint8 external_fence_rejoin_gate_domain[] = "PGRAC-REJOIN-GATE-V1";
static const uint8 external_fence_writer_set_domain[] = "PGRAC-PROTECTED-WRITERS-V1";

StaticAssertDecl(sizeof(external_fence_rejoin_clear_domain) == 32,
				 "rejoin authority-clear digest domain changed");
StaticAssertDecl(sizeof(external_fence_rejoin_complete_domain) == 25,
				 "rejoin complete digest domain changed");
StaticAssertDecl(sizeof(external_fence_rejoin_gate_domain) == 21,
				 "rejoin gate digest domain changed");
StaticAssertDecl(sizeof(external_fence_writer_set_domain) == 27,
				 "writer-set digest domain changed");

/* All three objects are backend-private and process-local.  No positive
 * instance can be created while provider id 0 is the sole registry outcome,
 * but keeping the complete closed representation here pins the no-wait and
 * lifetime boundary before later consumers are wired. */
struct PgracExternalFenceAdmissionV1 {
	uint32 magic;
	int32 owner_pid;
	uint32 released;
	PgracExternalFenceBindingV1 binding;
	uint8 daemon_boot_id[16];
	uint64 journal_seq;
	uint64 proof_generation;
	uint64 verified_mono_ns;
	uint64 fresh_until_mono_ns;
	uint64 cached_mapping_generation;
	int32 cooperative_lease_ms_snapshot;
	int32 socket_fd;
};

struct PgracExternalFenceNeedSetV1 {
	uint32 magic;
	int32 owner_pid;
	uint32 released;
	uint32 count;
	ClusterRecoveryDutyKey duty;
	ClusterRecoveryDutyDigest duty_digest;
	PgracExternalFenceWriterSetDigest writer_set_digest;
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 classification;
	const ClusterFormationWitnessV1 *formation;
	PgracExternalFenceNeedV1 needs[CLUSTER_MAX_NODES];
};

struct PgracExternalFenceAdmissionSetV1 {
	uint32 magic;
	int32 owner_pid;
	uint32 released;
	uint32 count;
	PgracExternalFenceWriterSetDigest writer_set_digest;
	const ClusterFormationWitnessV1 *formation;
	PgracExternalFenceAdmissionV1 *admissions[CLUSTER_MAX_NODES];
};

struct PgracExternalFenceRejoinOpV1 {
	uint32 magic;
	int32 owner_pid;
	uint32 status;
	uint32 deny_reason;
	int32 socket_fd;
	bool connect_pending;
	bool peer_authenticated;
	uint8 reserved22[2];
	uint64 deadline_mono_ns;
	uint8 transport_nonce[PGRAC_EXTERNAL_FENCE_NONCE_V1_BYTES];
	uint8 tx_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	size_t tx_sent;
	uint8 rx_frame[PGRAC_EXTERNAL_FENCE_REJOIN_V1_BYTES];
	size_t rx_used;
	PgracExternalFenceProtocolRejoinFrameV1 offer_frame;
	PgracExternalFenceProtocolRejoinFrameV1 on_frame;
	PgracExternalFenceProtocolRejoinFrameV1 ready_frame;
	PgracExternalFenceRejoinOfferV1 offer;
	PgracExternalFenceRejoinBindingV1 binding;
	PgracExternalFenceRejoinAuthorityClearV1 *moved_clear;
	ClusterControlRootIdentity saved_root_identity;
	ClusterControlRootSnapshot saved_root_snapshot;
	ClusterControlRootReadToken saved_root_token;
	PgracExternalFenceRejoinNeedV1 rejoin_need;
	uint8 root_completion_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	bool root_revalidated;
	bool consumed;
	bool authorize_enqueue_pending;
	PgracExternalFenceRejoinAuthorityClearV1 *authorize_clear_borrowed;
	bool refresh_enqueue_pending;
	bool refresh_sent;
	ClusterReconfigRejoinPendingSnapshotV1 refresh_pending;
};

struct PgracExternalFenceRejoinAuthorityClearV1 {
	uint32 magic;
	int32 owner_pid;
	uint64 operation_deadline_mono_ns;
	ClusterReconfigRejoinFailureSnapshotV1 failure;
	ClusterGrdRejoinClearSnapshotV1 grd_clear;
	PgracExternalFenceProtocolRejoinFrameV1 offer_frame;
	uint8 authority_clear_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
};

static int32
external_fence_owner_pid(void)
{
#ifndef WIN32
	return (int32)getpid();
#else
	return (int32)GetCurrentProcessId();
#endif
}

static bool
bytes_nonzero(const uint8 *bytes, size_t len)
{
	size_t i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static void
external_fence_put_u16_le(uint8 *out, uint16 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
}

static void
external_fence_put_u32_le(uint8 *out, uint32 value)
{
	out[0] = (uint8)value;
	out[1] = (uint8)(value >> 8);
	out[2] = (uint8)(value >> 16);
	out[3] = (uint8)(value >> 24);
}

static void
external_fence_put_u64_le(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> (i * 8));
}

static bool
external_fence_sha256(const uint8 *bytes, size_t len,
					  uint8 digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES])
{
	pg_cryptohash_ctx *ctx;
	bool ok;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		ok = false;
	else {
		ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
			 && pg_cryptohash_final(ctx, digest, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES) >= 0;
		pg_cryptohash_free(ctx);
	}
	if (!ok)
		memset(digest, 0, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
	return ok;
}

static bool
external_fence_rejoin_root_completion_digest(const ClusterControlRootIdentity *identity,
											 const ClusterControlRootReadToken *token,
											 uint8 digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES])
{
	uint8 preimage[167];
	size_t offset = 0;

#define ROOT_APPEND_BYTES(bytes_, len_)                                                            \
	do {                                                                                           \
		memcpy(preimage + offset, (bytes_), (len_));                                               \
		offset += (len_);                                                                          \
	} while (0)
#define ROOT_APPEND_U16(value_)                                                                    \
	do {                                                                                           \
		external_fence_put_u16_le(preimage + offset, (uint16)(value_));                            \
		offset += 2;                                                                               \
	} while (0)
#define ROOT_APPEND_U32(value_)                                                                    \
	do {                                                                                           \
		external_fence_put_u32_le(preimage + offset, (uint32)(value_));                            \
		offset += 4;                                                                               \
	} while (0)
#define ROOT_APPEND_U64(value_)                                                                    \
	do {                                                                                           \
		external_fence_put_u64_le(preimage + offset, (uint64)(value_));                            \
		offset += 8;                                                                               \
	} while (0)

	ROOT_APPEND_BYTES(external_fence_rejoin_complete_domain,
					  sizeof(external_fence_rejoin_complete_domain));
	ROOT_APPEND_U64(identity->system_identifier);
	ROOT_APPEND_BYTES(identity->storage_uuid, sizeof(identity->storage_uuid));
	ROOT_APPEND_BYTES(identity->authority_uuid, sizeof(identity->authority_uuid));
	ROOT_APPEND_U16(identity->origin_thread_id);
	ROOT_APPEND_U32(identity->origin_node_id);
	ROOT_APPEND_U64(identity->thread_claim_created_at);
	ROOT_APPEND_U32(identity->thread_claim_crc32c);
	ROOT_APPEND_U64(identity->origin_owner_incarnation);
	ROOT_APPEND_U64(identity->root_lineage_seq);
	ROOT_APPEND_BYTES(token->authority_uuid, sizeof(token->authority_uuid));
	ROOT_APPEND_U16(token->origin_thread_id);
	preimage[offset++] = token->source;
	preimage[offset++] = token->lifecycle;
	ROOT_APPEND_U32(0);
	ROOT_APPEND_U64(token->root_lineage_seq);
	ROOT_APPEND_U64(0);
	ROOT_APPEND_U64(token->file_txn_seq);
	ROOT_APPEND_U64(token->root_publish_seq);
	ROOT_APPEND_U32(token->record_crc32c);
	ROOT_APPEND_U32(token->root_flags);
	ROOT_APPEND_U32(CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE);
	Assert(offset == sizeof(preimage));

#undef ROOT_APPEND_U64
#undef ROOT_APPEND_U32
#undef ROOT_APPEND_U16
#undef ROOT_APPEND_BYTES

	return external_fence_sha256(preimage, sizeof(preimage), digest);
}

static bool
external_fence_rejoin_gate_digest(
	const uint8 root_completion_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES],
	const uint8 authority_clear_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES],
	uint8 digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES])
{
	uint8 preimage[85];

	memcpy(preimage, external_fence_rejoin_gate_domain, sizeof(external_fence_rejoin_gate_domain));
	memcpy(preimage + sizeof(external_fence_rejoin_gate_domain), root_completion_digest,
		   PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
	memcpy(preimage + sizeof(external_fence_rejoin_gate_domain) + PGRAC_EXTERNAL_FENCE_DIGEST_BYTES,
		   authority_clear_digest, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
	return external_fence_sha256(preimage, sizeof(preimage), digest);
}

static uint32
external_fence_bitmap_popcount(const uint8 bitmap[16])
{
	uint32 count = 0;
	int i;

	for (i = 0; i < 16; i++) {
		uint8 value = bitmap[i];

		while (value != 0) {
			count += value & 1U;
			value >>= 1;
		}
	}
	return count;
}

static bool
external_fence_bitmap_member(const uint8 bitmap[16], int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && (bitmap[node_id / 8] & (UINT8_C(1) << (node_id % 8))) != 0;
}

static bool
external_fence_rejoin_clear_digest(const PgracExternalFenceRejoinOpV1 *op,
								   const ClusterReconfigRejoinFailureSnapshotV1 *failure,
								   const ClusterGrdRejoinClearSnapshotV1 *grd_clear,
								   uint8 digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES])
{
	uint8 preimage[312];
	size_t offset = 0;
	bool ok;

#define APPEND_BYTES(bytes_, len_)                                                                 \
	do {                                                                                           \
		memcpy(preimage + offset, (bytes_), (len_));                                               \
		offset += (len_);                                                                          \
	} while (0)
#define APPEND_U16(value_)                                                                         \
	do {                                                                                           \
		external_fence_put_u16_le(preimage + offset, (uint16)(value_));                            \
		offset += 2;                                                                               \
	} while (0)
#define APPEND_U32(value_)                                                                         \
	do {                                                                                           \
		external_fence_put_u32_le(preimage + offset, (uint32)(value_));                            \
		offset += 4;                                                                               \
	} while (0)
#define APPEND_U64(value_)                                                                         \
	do {                                                                                           \
		external_fence_put_u64_le(preimage + offset, (uint64)(value_));                            \
		offset += 8;                                                                               \
	} while (0)

	APPEND_BYTES(external_fence_rejoin_clear_domain, sizeof(external_fence_rejoin_clear_domain));
	APPEND_BYTES(op->offer_frame.operation_id, sizeof(op->offer_frame.operation_id));
	APPEND_U64(op->offer_frame.system_identifier);
	APPEND_U32(op->offer_frame.old_node_id);
	APPEND_U32(0);
	APPEND_U64(op->offer_frame.old_incarnation);
	APPEND_U64(op->offer_frame.candidate_incarnation);
	APPEND_U32(RECONFIG_KIND_FAIL_STOP);
	APPEND_U32(0);
	APPEND_U64(failure->event_id);
	APPEND_U64(failure->new_epoch);
	APPEND_U64(failure->cssd_dead_generation);
	APPEND_BYTES(failure->dead_bitmap, sizeof(failure->dead_bitmap));
	APPEND_BYTES(failure->survivor_bitmap, sizeof(failure->survivor_bitmap));
	APPEND_U64(grd_clear->episode_epoch);
	APPEND_U64(grd_clear->dead_bitmap_hash);
	APPEND_BYTES(grd_clear->survivor_bitmap, sizeof(grd_clear->survivor_bitmap));
	APPEND_U64(op->offer_frame.target_mapping_generation);
	APPEND_U16(op->offer_frame.provider_id);
	APPEND_U16(op->offer_frame.provider_abi_version);
	APPEND_U32(0);
	APPEND_BYTES(op->offer_frame.daemon_boot_id, sizeof(op->offer_frame.daemon_boot_id));
	APPEND_U64(op->offer_frame.journal_seq);
	APPEND_U64(op->offer_frame.proof_generation);
	APPEND_U64(op->offer_frame.verified_mono_ns);
	APPEND_U64(op->offer_frame.fresh_until_mono_ns);
	APPEND_U64(op->deadline_mono_ns);
	APPEND_BYTES(op->offer_frame.target_state_digest, sizeof(op->offer_frame.target_state_digest));
	APPEND_BYTES(op->offer_frame.protected_set_digest,
				 sizeof(op->offer_frame.protected_set_digest));
	Assert(offset == sizeof(preimage));

	ok = external_fence_sha256(preimage, sizeof(preimage), digest);
	explicit_bzero(preimage, sizeof(preimage));

#undef APPEND_U64
#undef APPEND_U32
#undef APPEND_U16
#undef APPEND_BYTES

	return ok;
}

static bool
external_fence_need_valid(const PgracExternalFenceNeedV1 *need)
{
	return need != NULL && need->system_identifier != 0 && need->victim_node_id >= 0
		   && need->victim_node_id < CLUSTER_MAX_NODES && need->reserved0 == 0
		   && need->victim_incarnation != 0
		   && bytes_nonzero(need->canonical_duty_digest.bytes,
							sizeof(need->canonical_duty_digest.bytes))
		   && bytes_nonzero(need->protected_set_digest, sizeof(need->protected_set_digest))
		   && need->predicate_id == PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED
		   && need->predicate_version == PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
}

static void
external_fence_record_result(PgracExternalFenceVerdict verdict, PgracExternalFenceDenyReason reason,
							 uint64 journal_seq, uint64 verified_mono_ns)
{
	/* Verdict counters are mutually exclusive.  Reason-specific counters are
	 * additional diagnostics, never an alternate verdict. */
	switch (verdict) {
	case PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED:
		cluster_write_fence_note_external_write_excluded(journal_seq, verified_mono_ns);
		break;
	case PGRAC_EXTERNAL_FENCE_REJECTED:
		cluster_write_fence_note_external_rejected();
		break;
	case PGRAC_EXTERNAL_FENCE_UNKNOWN:
		cluster_write_fence_note_external_unknown();
		break;
	case PGRAC_EXTERNAL_FENCE_UNAVAILABLE:
		cluster_write_fence_note_external_unavailable();
		break;
	}

	switch (reason) {
	case PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH:
		cluster_write_fence_note_external_identity_mismatch();
		break;
	case PGRAC_EXTERNAL_FENCE_DENY_EXPIRED:
		cluster_write_fence_note_external_expired();
		break;
	case PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED:
		cluster_write_fence_note_external_daemon_disconnect();
		break;
	case PGRAC_EXTERNAL_FENCE_DENY_NONE:
	case PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT:
	case PGRAC_EXTERNAL_FENCE_DENY_EPISODE_NOT_CURRENT:
	case PGRAC_EXTERNAL_FENCE_DENY_VICTIM_NOT_AUTHORIZED:
	case PGRAC_EXTERNAL_FENCE_DENY_STORAGE_IDENTITY:
	case PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG:
	case PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH:
	case PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL:
	case PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_REJECTED:
	case PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN:
	case PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE:
	case PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT:
	case PGRAC_EXTERNAL_FENCE_DENY_JOURNAL:
	case PGRAC_EXTERNAL_FENCE_DENY_DAEMON_RESTARTED:
	case PGRAC_EXTERNAL_FENCE_DENY_MAPPING_CHANGED:
	case PGRAC_EXTERNAL_FENCE_DENY_MIXED_VERSION:
	case PGRAC_EXTERNAL_FENCE_DENY_REJOIN_INVALIDATED:
	case PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE:
	case PGRAC_EXTERNAL_FENCE_DENY_ROOT_NOT_COMPLETE:
	case PGRAC_EXTERNAL_FENCE_DENY_ROOT_STALE:
	case PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH:
	case PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH:
	case PGRAC_EXTERNAL_FENCE_DENY_JOIN_NOT_READY:
	case PGRAC_EXTERNAL_FENCE_DENY_REJOIN_CONSUMED:
	case PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING:
	case PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_STALE:
	case PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH:
	case PGRAC_EXTERNAL_FENCE_DENY_IO_NOT_DRAINED:
	case PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR:
		break;
	}
}

static bool
external_fence_binding_matches_need(const PgracExternalFenceBindingV1 *binding,
									const PgracExternalFenceNeedV1 *need)
{
	return binding->system_identifier == need->system_identifier
		   && memcmp(&binding->canonical_duty_digest, &need->canonical_duty_digest,
					 sizeof(binding->canonical_duty_digest))
				  == 0
		   && binding->victim_node_id == need->victim_node_id && binding->reserved0 == 0
		   && binding->victim_incarnation == need->victim_incarnation
		   && memcmp(binding->protected_set_digest, need->protected_set_digest,
					 sizeof(binding->protected_set_digest))
				  == 0
		   && binding->predicate_id == need->predicate_id
		   && binding->predicate_version == need->predicate_version;
}

static bool
external_fence_admission_valid(const PgracExternalFenceAdmissionV1 *admission)
{
	return admission != NULL && admission->magic == PGRAC_EXTERNAL_FENCE_ADMISSION_MAGIC
		   && admission->owner_pid == external_fence_owner_pid() && admission->released == 0
		   && admission->socket_fd >= 0;
}

static bool
external_fence_need_set_valid(const PgracExternalFenceNeedSetV1 *set)
{
	return set != NULL && set->magic == PGRAC_EXTERNAL_FENCE_NEED_SET_MAGIC
		   && set->owner_pid == external_fence_owner_pid() && set->released == 0 && set->count >= 1
		   && set->count <= CLUSTER_MAX_NODES
		   && bytes_nonzero(set->writer_set_digest.bytes, sizeof(set->writer_set_digest.bytes))
		   && set->formation != NULL;
}

static bool
external_fence_admission_set_valid(const PgracExternalFenceAdmissionSetV1 *set)
{
	return set != NULL && set->magic == PGRAC_EXTERNAL_FENCE_ADMISSION_SET_MAGIC
		   && set->owner_pid == external_fence_owner_pid() && set->released == 0 && set->count >= 1
		   && set->count <= CLUSTER_MAX_NODES
		   && bytes_nonzero(set->writer_set_digest.bytes, sizeof(set->writer_set_digest.bytes))
		   && set->formation != NULL;
}

static bool
external_fence_writer_set_digest_v1(
	const ClusterRecoveryDutyDigest *duty_digest,
	const uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES],
	const PgracExternalFenceNeedV1 *needs, uint32 count, PgracExternalFenceWriterSetDigest *out)
{
	uint8 preimage[27 + PGRAC_EXTERNAL_FENCE_DIGEST_BYTES * 2 + 4
				   + CLUSTER_MAX_NODES * sizeof(PgracExternalFenceWriterV1)];
	size_t offset = 0;
	uint32 i;

	if (duty_digest == NULL || protected_set_digest == NULL || needs == NULL || out == NULL
		|| count < 1 || count > CLUSTER_MAX_NODES)
		return false;
	memcpy(preimage + offset, external_fence_writer_set_domain,
		   sizeof(external_fence_writer_set_domain));
	offset += sizeof(external_fence_writer_set_domain);
	memcpy(preimage + offset, duty_digest->bytes, sizeof(duty_digest->bytes));
	offset += sizeof(duty_digest->bytes);
	memcpy(preimage + offset, protected_set_digest, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES);
	offset += PGRAC_EXTERNAL_FENCE_DIGEST_BYTES;
	external_fence_put_u32_le(preimage + offset, count);
	offset += 4;
	for (i = 0; i < count; i++) {
		external_fence_put_u32_le(preimage + offset, (uint32)needs[i].victim_node_id);
		offset += 4;
		external_fence_put_u32_le(preimage + offset, 0);
		offset += 4;
		external_fence_put_u64_le(preimage + offset, needs[i].victim_incarnation);
		offset += 8;
	}
	if (!external_fence_sha256(preimage, offset, out->bytes)) {
		explicit_bzero(preimage, sizeof(preimage));
		return false;
	}
	explicit_bzero(preimage, sizeof(preimage));
	return true;
}

PgracExternalFenceNeedSetResult
cluster_external_fence_need_set_build(const ClusterRecoveryDutyKey *duty,
									  const ClusterFormationWitnessV1 *formation,
									  PgracExternalFenceNeedSetV1 **out)
{
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 classification;
	ClusterProtectedSetIdentityV1 protected_identity;
	ClusterRecoveryDutyDigest duty_digest;
	ClusterFormationWitnessResult formation_result;
	PgracExternalFenceNeedSetV1 *set;
	uint8 protected_set_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint16 formation_origin_thread;
	uint32 count = 0;
	int32 node_id;

	if (out == NULL || *out != NULL || duty == NULL || formation == NULL)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_BAD_ARGUMENT;
	if (!cluster_recovery_duty_key_valid_v1(duty))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_DUTY_INVALID;
	formation_result = cluster_formation_witness_revalidate_nowait(formation);
	if (formation_result == CLUSTER_FORMATION_WITNESS_UNSTABLE)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE;
	if (formation_result != CLUSTER_FORMATION_WITNESS_READY)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_FENCE_AUTHORITY_UNAVAILABLE;

	if ((cluster_ic_local_capability_word() & PGRAC_IC_HELLO_CAP_CONTROL_ROOT_V1) == 0
		|| !cluster_external_fence_runtime_active())
		return PGRAC_EXTERNAL_FENCE_NEED_SET_CAPABILITY_UNAVAILABLE;
	if (!cluster_formation_witness_copy_classification_v1(formation, &formation_origin_thread,
														  &authority, &classification))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_FENCE_AUTHORITY_UNAVAILABLE;
	if (formation_origin_thread != duty->origin_thread_id)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_DUTY_INVALID;
	if (authority.total_disk_count == 0
		|| authority.agree_disk_count <= authority.total_disk_count / 2
		|| !cluster_fence_marker_valid_v1(&authority.marker))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_FENCE_AUTHORITY_UNAVAILABLE;
	if (memcmp(authority.marker.fenced_dead_bitmap, classification.excluded_bitmap,
			   sizeof(classification.excluded_bitmap))
			!= 0
		|| authority.marker.fence_epoch != classification.local_epoch)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE;

	for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
		if (!external_fence_bitmap_member(classification.excluded_bitmap, node_id))
			continue;
		count++;
		if (classification.membership.membership_state[node_id] != CLUSTER_MEMBER_DEAD
			&& classification.membership.membership_state[node_id] != CLUSTER_MEMBER_REMOVED)
			return PGRAC_EXTERNAL_FENCE_NEED_SET_MEMBERSHIP_UNSTABLE;
		if (classification.membership.last_admitted_incarnation[node_id] == 0)
			return PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_INCAR_UNPROVEN;
	}
	if (count < 1 || count > CLUSTER_MAX_NODES)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_COUNT_INVALID;
	if (!external_fence_bitmap_member(classification.excluded_bitmap, duty->origin_node_id))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_ORIGINAL_OWNER_MISSING;
	if (classification.membership.last_admitted_incarnation[duty->origin_node_id]
		!= duty->origin_owner_incarnation)
		return PGRAC_EXTERNAL_FENCE_NEED_SET_WRITER_INCAR_UNPROVEN;
	memset(&protected_identity, 0, sizeof(protected_identity));
	if (!cluster_shared_fs_get_protected_set_identity(&protected_identity)
		|| (protected_identity.backend_id != CLUSTER_SHARED_FS_BACKEND_BLOCK_DEVICE
			&& protected_identity.backend_id != CLUSTER_SHARED_FS_BACKEND_CLUSTER_FS)
		|| memcmp(protected_identity.storage_uuid, duty->storage_uuid,
				  sizeof(protected_identity.storage_uuid))
			   != 0
		|| !pgrac_external_fence_protected_set_digest_v1(
			protected_identity.backend_id, protected_identity.storage_uuid, protected_set_digest))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_STORAGE_UNAVAILABLE;
	if (!cluster_recovery_duty_digest_v1(duty, &duty_digest))
		return PGRAC_EXTERNAL_FENCE_NEED_SET_DUTY_INVALID;

	set = palloc0(sizeof(*set));
	set->magic = PGRAC_EXTERNAL_FENCE_NEED_SET_MAGIC;
	set->owner_pid = external_fence_owner_pid();
	set->count = count;
	set->duty = *duty;
	set->duty_digest = duty_digest;
	set->authority = authority;
	set->classification = classification;
	set->formation = formation;
	count = 0;
	for (node_id = 0; node_id < CLUSTER_MAX_NODES; node_id++) {
		PgracExternalFenceNeedV1 *need;

		if (!external_fence_bitmap_member(classification.excluded_bitmap, node_id))
			continue;
		need = &set->needs[count++];
		need->system_identifier = duty->system_identifier;
		need->canonical_duty_digest = duty_digest;
		need->victim_node_id = node_id;
		need->victim_incarnation = classification.membership.last_admitted_incarnation[node_id];
		memcpy(need->protected_set_digest, protected_set_digest,
			   sizeof(need->protected_set_digest));
		need->predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_WRITE_EXCLUDED;
		need->predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	}
	if (!external_fence_writer_set_digest_v1(&duty_digest, protected_set_digest, set->needs,
											 set->count, &set->writer_set_digest)) {
		explicit_bzero(set, sizeof(*set));
		pfree(set);
		return PGRAC_EXTERNAL_FENCE_NEED_SET_STORAGE_UNAVAILABLE;
	}
	*out = set;
	return PGRAC_EXTERNAL_FENCE_NEED_SET_OK;
}

bool
cluster_external_fence_need_set_revalidate_nowait(const PgracExternalFenceNeedSetV1 *needs,
												  const ClusterFormationWitnessV1 *formation,
												  PgracExternalFenceDenyReason *reason)
{
	uint32 i;

	if (reason == NULL)
		return false;
	if (!external_fence_need_set_valid(needs) || formation == NULL) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
		return false;
	}
	if (formation != needs->formation
		|| cluster_formation_witness_revalidate_nowait(formation)
			   != CLUSTER_FORMATION_WITNESS_READY) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE;
		return false;
	}
	for (i = 0; i < needs->count; i++) {
		if (!external_fence_need_valid(&needs->needs[i])) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE;
			return false;
		}
	}
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

static PgracExternalFenceVerdict
external_fence_admit_result(PgracExternalFenceVerdict verdict, PgracExternalFenceDenyReason reason,
							uint64 journal_seq, uint64 verified_mono_ns)
{
	external_fence_last_deny = reason;
	external_fence_record_result(verdict, reason, journal_seq, verified_mono_ns);
	return verdict;
}

static bool
external_fence_response_verdict_valid(const PgracExternalFenceProtocolResponseV1 *response)
{
	switch ((PgracExternalFenceVerdict)response->verdict) {
	case PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED:
		return response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_NONE;
	case PGRAC_EXTERNAL_FENCE_REJECTED:
		return response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_REJECTED
			   || response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_IO_NOT_DRAINED;
	case PGRAC_EXTERNAL_FENCE_UNKNOWN:
		return response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN;
	case PGRAC_EXTERNAL_FENCE_UNAVAILABLE:
		return response->deny_reason != PGRAC_EXTERNAL_FENCE_DENY_NONE
			   && response->deny_reason != PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_REJECTED
			   && response->deny_reason != PGRAC_EXTERNAL_FENCE_DENY_PROVIDER_UNKNOWN
			   && response->deny_reason != PGRAC_EXTERNAL_FENCE_DENY_IO_NOT_DRAINED;
	}
	return false;
}

#ifndef WIN32
static bool
external_fence_wait_ready(int fd, short events, uint64 deadline_ns,
						  PgracExternalFenceDenyReason *reason)
{
	for (;;) {
		struct pollfd poll_fd;
		uint64 now_ns;
		uint64 remaining_ns;
		uint64 remaining_ms;
		int poll_result;

		if (!external_fence_monotonic_now(&now_ns) || now_ns >= deadline_ns) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
			return false;
		}
		remaining_ns = deadline_ns - now_ns;
		remaining_ms = (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
		if (remaining_ms > (uint64)INT_MAX)
			remaining_ms = (uint64)INT_MAX;
		poll_fd.fd = fd;
		poll_fd.events = events;
		poll_fd.revents = 0;
		poll_result = poll(&poll_fd, 1, (int)remaining_ms);
		if (poll_result < 0) {
			if (errno == EINTR)
				continue;
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
		if (poll_result == 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
			return false;
		}
		if ((poll_fd.revents & events) != 0)
			return true;
		if ((poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
	}
}

static bool
external_fence_write_exact(int fd, const uint8 *bytes, size_t len, uint64 deadline_ns,
						   PgracExternalFenceDenyReason *reason)
{
	size_t sent = 0;
	int send_flags = MSG_DONTWAIT;

#ifdef MSG_NOSIGNAL
	send_flags |= MSG_NOSIGNAL;
#endif
	while (sent < len) {
		ssize_t count = send(fd, bytes + sent, len - sent, send_flags);

		if (count > 0) {
			sent += (size_t)count;
			continue;
		}
		if (count == 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
		if (errno == EINTR)
			continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
		if (!external_fence_wait_ready(fd, POLLOUT, deadline_ns, reason))
			return false;
	}
	return true;
}

static bool
external_fence_read_exact(int fd, uint8 *bytes, size_t len, uint64 deadline_ns,
						  PgracExternalFenceDenyReason *reason)
{
	size_t received = 0;

	while (received < len) {
		ssize_t count = recv(fd, bytes + received, len - received, MSG_DONTWAIT);

		if (count > 0) {
			received += (size_t)count;
			continue;
		}
		if (count == 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
		if (errno == EINTR)
			continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
		if (!external_fence_wait_ready(fd, POLLIN, deadline_ns, reason))
			return false;
	}
	return true;
}

static int
external_fence_connect_root_daemon(int timeout_ms, uint64 deadline_ns,
								   PgracExternalFenceDenyReason *reason)
{
	int fd = -1;
	int flags;

#ifdef USE_CLUSTER_UNIT
	fd = cluster_external_fence_test_connect_root_daemon(timeout_ms);
	if (fd < 0) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
		return -1;
	}
#else
	struct sockaddr_un address;
	struct stat socket_stat;
	int connect_result;

	if (cluster_external_fence_socket_path == NULL || cluster_external_fence_socket_path[0] != '/'
		|| strlen(cluster_external_fence_socket_path) >= sizeof(address.sun_path)
		|| lstat(cluster_external_fence_socket_path, &socket_stat) != 0
		|| !S_ISSOCK(socket_stat.st_mode) || socket_stat.st_uid != 0
		|| (socket_stat.st_mode & 0777) != 0660) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG;
		return -1;
	}
	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd < 0) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG;
		return -1;
	}
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, cluster_external_fence_socket_path, sizeof(address.sun_path));
#endif

	flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0
		|| fcntl(fd, F_SETFD, FD_CLOEXEC) < 0) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG;
		(void)close(fd);
		return -1;
	}
#ifdef SO_NOSIGPIPE
	{
		int no_sigpipe = 1;

		if (setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe)) != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG;
			(void)close(fd);
			return -1;
		}
	}
#endif

#ifndef USE_CLUSTER_UNIT
	connect_result = connect(fd, (struct sockaddr *)&address, sizeof(address));
	if (connect_result != 0) {
		int socket_error = 0;
		socklen_t socket_error_len = sizeof(socket_error);

		if (errno != EINPROGRESS && errno != EAGAIN && errno != EWOULDBLOCK) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
			(void)close(fd);
			return -1;
		}
		if (!external_fence_wait_ready(fd, POLLOUT, deadline_ns, reason)) {
			if (*reason == PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED)
				*reason = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
			(void)close(fd);
			return -1;
		}
		if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) != 0
			|| socket_error != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
			(void)close(fd);
			return -1;
		}
	}
#ifdef __linux__
	{
		struct ucred credential;
		socklen_t credential_len = sizeof(credential);

		if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credential, &credential_len) != 0
			|| credential_len != sizeof(credential) || credential.uid != 0 || credential.pid <= 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH;
			(void)close(fd);
			return -1;
		}
	}
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
	{
		uid_t uid;
		gid_t gid;

		if (getpeereid(fd, &uid, &gid) != 0 || uid != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH;
			(void)close(fd);
			return -1;
		}
	}
#else
	*reason = PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH;
	(void)close(fd);
	return -1;
#endif
#endif
	return fd;
}

static bool
external_fence_live_connection_exact(int fd, PgracExternalFenceDenyReason *reason)
{
	struct pollfd poll_fd;
	int poll_result;

	for (;;) {
		poll_fd.fd = fd;
		poll_fd.events = POLLIN | POLLHUP | POLLERR;
		poll_fd.revents = 0;
		poll_result = poll(&poll_fd, 1, 0);
		if (poll_result < 0 && errno == EINTR)
			continue;
		break;
	}
	if (poll_result == 0)
		return true;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
	return false;
}
#endif

PgracExternalFenceVerdict
cluster_external_fence_admit_wait(const PgracExternalFenceNeedV1 *need, int timeout_ms,
								  PgracExternalFenceAdmissionV1 **out)
{
	int32 cooperative_lease_ms_snapshot = cluster_write_fence_lease_ms;

	if (out != NULL)
		*out = NULL;
	if (out == NULL || !external_fence_need_valid(need) || timeout_ms < 1
		|| timeout_ms > (int)PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS) {
		external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}

	cluster_write_fence_note_external_admit_requested();
	if (!cluster_external_fence_runtime_active())
		return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
										   PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, 0, 0);

#ifdef WIN32
	return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
									   PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, 0, 0);
#else
	{
		PgracExternalFenceProtocolRequestV1 request;
		PgracExternalFenceProtocolResponseV1 response;
		PgracExternalFenceAdmissionV1 *admission;
		PgracExternalFenceDenyReason reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		uint8 request_frame[PGRAC_EXTERNAL_FENCE_REQUEST_V1_BYTES];
		uint8 response_frame[PGRAC_EXTERNAL_FENCE_RESPONSE_V1_BYTES];
		uint64 start_ns;
		uint64 deadline_ns;
		uint64 timeout_ns = (uint64)timeout_ms * UINT64_C(1000000);
		uint64 now_ns;
		int fd = -1;

		if (!external_fence_monotonic_now(&start_ns) || UINT64_MAX - start_ns < timeout_ns)
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, 0, 0);
		deadline_ns = start_ns + timeout_ns;
		memset(&request, 0, sizeof(request));
		if (!pg_strong_random(request.request_nonce, sizeof(request.request_nonce))
			|| !bytes_nonzero(request.request_nonce, sizeof(request.request_nonce)))
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, 0, 0);
		request.need.system_identifier = need->system_identifier;
		memcpy(request.need.canonical_duty_digest, need->canonical_duty_digest.bytes,
			   sizeof(request.need.canonical_duty_digest));
		request.need.victim_node_id = need->victim_node_id;
		request.need.victim_incarnation = need->victim_incarnation;
		memcpy(request.need.protected_set_digest, need->protected_set_digest,
			   sizeof(request.need.protected_set_digest));
		request.need.predicate_id = need->predicate_id;
		request.need.predicate_version = need->predicate_version;
		request.timeout_ms = (uint32)timeout_ms;
		if (!pgrac_external_fence_request_v1_encode(&request, request_frame))
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, 0, 0);

		fd = external_fence_connect_root_daemon(timeout_ms, deadline_ns, &reason);
		if (fd < 0)
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE, reason, 0, 0);
		if (!external_fence_write_exact(fd, request_frame, sizeof(request_frame), deadline_ns,
										&reason)
			|| !external_fence_read_exact(fd, response_frame, sizeof(response_frame), deadline_ns,
										  &reason)) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE, reason, 0, 0);
		}
		if (!pgrac_external_fence_response_v1_decode(response_frame, sizeof(response_frame),
													 &response)
			|| memcmp(request.request_nonce, response.request_nonce, sizeof(request.request_nonce))
				   != 0
			|| !external_fence_response_verdict_valid(&response)) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, 0, 0);
		}
		if (response.verdict != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED) {
			PgracExternalFenceVerdict verdict = (PgracExternalFenceVerdict)response.verdict;
			PgracExternalFenceDenyReason deny = (PgracExternalFenceDenyReason)response.deny_reason;

			(void)close(fd);
			return external_fence_admit_result(verdict, deny, response.journal_seq,
											   response.verified_mono_ns);
		}
		if (!pgrac_external_fence_affirmative_response_matches_request_v1(&request, &response)) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH, 0, 0);
		}
		if (!external_fence_monotonic_now(&now_ns) || now_ns >= deadline_ns) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, 0, 0);
		}
		if (now_ns < response.verified_mono_ns || now_ns >= response.fresh_until_mono_ns
			|| response.fresh_until_mono_ns <= response.verified_mono_ns
			|| response.fresh_until_mono_ns - response.verified_mono_ns
				   > PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
											   PGRAC_EXTERNAL_FENCE_DENY_EXPIRED, 0, 0);
		}
		if (!external_fence_live_connection_exact(fd, &reason)) {
			(void)close(fd);
			return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE, reason, 0, 0);
		}

		admission = palloc0(sizeof(*admission));
		admission->magic = PGRAC_EXTERNAL_FENCE_ADMISSION_MAGIC;
		admission->owner_pid = external_fence_owner_pid();
		admission->binding.system_identifier = response.binding.system_identifier;
		memcpy(admission->binding.canonical_duty_digest.bytes,
			   response.binding.canonical_duty_digest,
			   sizeof(response.binding.canonical_duty_digest));
		admission->binding.victim_node_id = response.binding.victim_node_id;
		admission->binding.victim_incarnation = response.binding.victim_incarnation;
		admission->binding.target_mapping_generation = response.binding.target_mapping_generation;
		memcpy(admission->binding.protected_set_digest, response.binding.protected_set_digest,
			   sizeof(response.binding.protected_set_digest));
		admission->binding.predicate_id = response.binding.predicate_id;
		admission->binding.predicate_version = response.binding.predicate_version;
		memcpy(admission->daemon_boot_id, response.daemon_boot_id,
			   sizeof(admission->daemon_boot_id));
		admission->journal_seq = response.journal_seq;
		admission->proof_generation = response.proof_generation;
		admission->verified_mono_ns = response.verified_mono_ns;
		admission->fresh_until_mono_ns = response.fresh_until_mono_ns;
		admission->cached_mapping_generation = response.binding.target_mapping_generation;
		admission->cooperative_lease_ms_snapshot = cooperative_lease_ms_snapshot;
		admission->socket_fd = fd;
		*out = admission;
		return external_fence_admit_result(PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED,
										   PGRAC_EXTERNAL_FENCE_DENY_NONE, response.journal_seq,
										   response.verified_mono_ns);
	}
#endif
}

bool
cluster_external_fence_revalidate_nowait(const PgracExternalFenceAdmissionV1 *admission,
										 const PgracExternalFenceNeedV1 *current,
										 PgracExternalFenceDenyReason *reason)
{
	uint64 now_ns;

	if (reason == NULL)
		return false;
	if (!external_fence_admission_valid(admission) || !external_fence_need_valid(current)) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
		return false;
	}
	if (!external_fence_binding_matches_need(&admission->binding, current)) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH;
		return false;
	}
	if (admission->binding.target_mapping_generation == 0
		|| admission->cached_mapping_generation != admission->binding.target_mapping_generation) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_MAPPING_CHANGED;
		return false;
	}
	if (!bytes_nonzero(admission->daemon_boot_id, sizeof(admission->daemon_boot_id))
		|| admission->journal_seq == 0 || admission->proof_generation == 0) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL;
		return false;
	}
#ifndef WIN32
	{
		struct timespec now;
		struct pollfd poll_fd;

		if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
			return false;
		}
		now_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
		if (now_ns < admission->verified_mono_ns || now_ns >= admission->fresh_until_mono_ns
			|| admission->fresh_until_mono_ns <= admission->verified_mono_ns
			|| admission->fresh_until_mono_ns - admission->verified_mono_ns
				   > PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_EXPIRED;
			return false;
		}
		poll_fd.fd = admission->socket_fd;
		poll_fd.events = POLLIN | POLLHUP | POLLERR;
		poll_fd.revents = 0;
		if (poll(&poll_fd, 1, 0) != 0) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED;
			return false;
		}
	}
#else
	(void)now_ns;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
	return false;
#endif
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

PgracExternalFenceVerdict
cluster_external_fence_admit_set_wait(const PgracExternalFenceNeedSetV1 *needs,
									  const ClusterFormationWitnessV1 *formation, int timeout_ms,
									  PgracExternalFenceAdmissionSetV1 **out)
{
	PgracExternalFenceAdmissionSetV1 *set;
	PgracExternalFenceDenyReason reason;
#ifndef WIN32
	int32 cooperative_lease_ms_snapshot = cluster_write_fence_lease_ms;
	uint64 deadline_ns;
	uint64 now_ns;
	uint64 timeout_ns;
	uint32 i;
#endif

	if (out != NULL)
		*out = NULL;
	if (out == NULL || timeout_ms < 1 || timeout_ms > 600000
		|| !external_fence_need_set_valid(needs) || formation == NULL) {
		external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}
	if (!cluster_external_fence_need_set_revalidate_nowait(needs, formation, &reason)) {
		external_fence_last_deny = reason;
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}

#ifdef WIN32
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE;
	return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
#else
	if (!external_fence_monotonic_now(&now_ns)) {
		external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}
	timeout_ns = (uint64)timeout_ms * UINT64_C(1000000);
	if (UINT64_MAX - now_ns < timeout_ns) {
		external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}
	deadline_ns = now_ns + timeout_ns;
	set = palloc0(sizeof(*set));
	set->magic = PGRAC_EXTERNAL_FENCE_ADMISSION_SET_MAGIC;
	set->owner_pid = external_fence_owner_pid();
	set->writer_set_digest = needs->writer_set_digest;
	set->formation = formation;
	for (i = 0; i < needs->count; i++) {
		PgracExternalFenceVerdict verdict;
		uint64 remaining_ns;
		uint64 remaining_ms;

		if (!external_fence_monotonic_now(&now_ns) || now_ns >= deadline_ns) {
			external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
			cluster_external_fence_admission_set_release(&set);
			return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
		}
		remaining_ns = deadline_ns - now_ns;
		remaining_ms = (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
		if (remaining_ms > PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS)
			remaining_ms = PGRAC_EXTERNAL_FENCE_TIMEOUT_MAX_MS;
		verdict = cluster_external_fence_admit_wait(&needs->needs[i], (int)remaining_ms,
													&set->admissions[i]);
		if (verdict != PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED || set->admissions[i] == NULL) {
			cluster_external_fence_admission_set_release(&set);
			return verdict;
		}
		set->admissions[i]->cooperative_lease_ms_snapshot = cooperative_lease_ms_snapshot;
		set->count++;
	}
	if (!external_fence_monotonic_now(&now_ns) || now_ns >= deadline_ns) {
		external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT;
		cluster_external_fence_admission_set_release(&set);
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}
	if (!cluster_external_fence_revalidate_set_nowait(set, needs, formation, &reason)) {
		external_fence_last_deny = reason;
		cluster_external_fence_admission_set_release(&set);
		return PGRAC_EXTERNAL_FENCE_UNAVAILABLE;
	}
	*out = set;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return PGRAC_EXTERNAL_FENCE_WRITE_EXCLUDED;
#endif
}

bool
cluster_external_fence_revalidate_set_nowait(const PgracExternalFenceAdmissionSetV1 *admissions,
											 const PgracExternalFenceNeedSetV1 *needs,
											 const ClusterFormationWitnessV1 *formation,
											 PgracExternalFenceDenyReason *reason)
{
	uint32 i;

	if (reason == NULL)
		return false;
	if (!external_fence_admission_set_valid(admissions) || !external_fence_need_set_valid(needs)
		|| formation == NULL) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT;
		return false;
	}
	if (admissions->formation != formation || needs->formation != formation
		|| admissions->count != needs->count
		|| memcmp(&admissions->writer_set_digest, &needs->writer_set_digest,
				  sizeof(admissions->writer_set_digest))
			   != 0) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_WRITER_SET_STALE;
		return false;
	}
	if (!cluster_external_fence_need_set_revalidate_nowait(needs, formation, reason))
		return false;
	for (i = 0; i < admissions->count; i++) {
		if (!cluster_external_fence_revalidate_nowait(admissions->admissions[i], &needs->needs[i],
													  reason))
			return false;
	}
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

uint32
cluster_external_fence_need_set_count(const PgracExternalFenceNeedSetV1 *set)
{
	return external_fence_need_set_valid(set) ? set->count : 0;
}

const PgracExternalFenceNeedV1 *
cluster_external_fence_need_set_at(const PgracExternalFenceNeedSetV1 *set, uint32 index)
{
	return external_fence_need_set_valid(set) && index < set->count ? &set->needs[index] : NULL;
}

const PgracExternalFenceWriterSetDigest *
cluster_external_fence_need_set_digest(const PgracExternalFenceNeedSetV1 *set)
{
	return external_fence_need_set_valid(set) ? &set->writer_set_digest : NULL;
}

void
cluster_external_fence_need_set_release(PgracExternalFenceNeedSetV1 **set)
{
	if (set != NULL) {
		if (*set != NULL && (*set)->magic == PGRAC_EXTERNAL_FENCE_NEED_SET_MAGIC
			&& (*set)->owner_pid == external_fence_owner_pid()) {
			(*set)->released = 1;
			explicit_bzero(*set, sizeof(**set));
			pfree(*set);
		}
		*set = NULL;
	}
}

const PgracExternalFenceBindingV1 *
cluster_external_fence_admission_binding(const PgracExternalFenceAdmissionV1 *admission)
{
	return external_fence_admission_valid(admission) ? &admission->binding : NULL;
}

#ifdef USE_CLUSTER_UNIT
int
cluster_external_fence_test_admission_lease_ms(const PgracExternalFenceAdmissionV1 *admission)
{
	return external_fence_admission_valid(admission) ? admission->cooperative_lease_ms_snapshot
													 : -1;
}
#endif

void
cluster_external_fence_admission_release(PgracExternalFenceAdmissionV1 *admission)
{
	if (admission == NULL || admission->magic != PGRAC_EXTERNAL_FENCE_ADMISSION_MAGIC
		|| admission->owner_pid != external_fence_owner_pid())
		return;
#ifndef WIN32
	if (admission->socket_fd >= 0)
		(void)close(admission->socket_fd);
#endif
	admission->released = 1;
	explicit_bzero(admission, sizeof(*admission));
	pfree(admission);
}

uint32
cluster_external_fence_admission_set_count(const PgracExternalFenceAdmissionSetV1 *set)
{
	return external_fence_admission_set_valid(set) ? set->count : 0;
}

const PgracExternalFenceBindingV1 *
cluster_external_fence_admission_set_binding_at(const PgracExternalFenceAdmissionSetV1 *set,
												uint32 index)
{
	if (!external_fence_admission_set_valid(set) || index >= set->count)
		return NULL;
	return cluster_external_fence_admission_binding(set->admissions[index]);
}

const PgracExternalFenceWriterSetDigest *
cluster_external_fence_admission_set_digest(const PgracExternalFenceAdmissionSetV1 *set)
{
	return external_fence_admission_set_valid(set) ? &set->writer_set_digest : NULL;
}

void
cluster_external_fence_admission_set_release(PgracExternalFenceAdmissionSetV1 **set)
{
	if (set != NULL) {
		if (*set != NULL && (*set)->magic == PGRAC_EXTERNAL_FENCE_ADMISSION_SET_MAGIC
			&& (*set)->owner_pid == external_fence_owner_pid()) {
			uint32 i = (*set)->count;

			while (i > 0) {
				i--;
				cluster_external_fence_admission_release((*set)->admissions[i]);
				(*set)->admissions[i] = NULL;
			}
			(*set)->released = 1;
			explicit_bzero(*set, sizeof(**set));
			pfree(*set);
		}
		*set = NULL;
	}
}

static PgracExternalFenceRejoinStatus
external_fence_rejoin_fail(PgracExternalFenceRejoinStatus status, PgracExternalFenceDenyReason deny,
						   PgracExternalFenceDenyReason *reason)
{
	external_fence_last_deny = deny;
	if (reason != NULL)
		*reason = deny;
	return status;
}

#ifndef WIN32
static bool
external_fence_monotonic_now(uint64 *out_now_ns)
{
	struct timespec now;

	if (out_now_ns == NULL || clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0
		|| now.tv_nsec < 0)
		return false;
	*out_now_ns = (uint64)now.tv_sec * UINT64_C(1000000000) + (uint64)now.tv_nsec;
	return true;
}

static bool
external_fence_root_peer_authenticated(int fd)
{
#ifdef USE_CLUSTER_UNIT
	return cluster_external_fence_test_root_peer_authenticated(fd);
#elif defined(__linux__)
	struct ucred credential;
	socklen_t credential_len = sizeof(credential);

	return getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credential, &credential_len) == 0
		   && credential_len == sizeof(credential) && credential.uid == 0 && credential.pid > 0;
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
	uid_t uid;
	gid_t gid;

	return getpeereid(fd, &uid, &gid) == 0 && uid == 0;
#else
	return false;
#endif
}

static void
external_fence_rejoin_close(PgracExternalFenceRejoinOpV1 *op)
{
	if (op->socket_fd >= 0) {
		(void)close(op->socket_fd);
		op->socket_fd = -1;
	}
}

static PgracExternalFenceRejoinStatus
external_fence_rejoin_terminal(PgracExternalFenceRejoinOpV1 *op,
							   PgracExternalFenceRejoinStatus status,
							   PgracExternalFenceDenyReason deny,
							   PgracExternalFenceDenyReason *reason)
{
	op->status = (uint32)status;
	op->deny_reason = (uint32)deny;
	external_fence_rejoin_close(op);
	return external_fence_rejoin_fail(status, deny, reason);
}

static bool
external_fence_rejoin_offer_valid(const PgracExternalFenceRejoinOpV1 *op,
								  const PgracExternalFenceProtocolRejoinFrameV1 *response,
								  uint64 now_ns)
{
	return response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER
		   && response->status == PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED
		   && response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_NONE
		   && memcmp(response->transport_nonce, op->transport_nonce, sizeof(op->transport_nonce))
				  == 0
		   && bytes_nonzero(response->operation_id, sizeof(response->operation_id))
		   && !bytes_nonzero(response->rejoin_gate_digest, sizeof(response->rejoin_gate_digest))
		   && response->system_identifier != 0
		   && bytes_nonzero(response->protected_set_digest, sizeof(response->protected_set_digest))
		   && response->old_node_id >= 0 && response->old_node_id < CLUSTER_MAX_NODES
		   && response->old_incarnation != 0
		   && response->candidate_incarnation > response->old_incarnation
		   && response->provider_id != 0 && response->provider_abi_version == 1
		   && response->target_mapping_generation != 0
		   && bytes_nonzero(response->daemon_boot_id, sizeof(response->daemon_boot_id))
		   && response->journal_seq != 0 && response->verified_mono_ns != 0
		   && response->fresh_until_mono_ns > response->verified_mono_ns
		   && response->fresh_until_mono_ns - response->verified_mono_ns
				  <= PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS
		   && response->verified_mono_ns <= now_ns && now_ns < response->fresh_until_mono_ns
		   && now_ns < op->deadline_mono_ns && response->proof_generation != 0
		   && bytes_nonzero(response->target_state_digest, sizeof(response->target_state_digest));
}

static bool
external_fence_rejoin_on_result_valid(const PgracExternalFenceRejoinOpV1 *op,
									  const PgracExternalFenceProtocolRejoinFrameV1 *response,
									  uint64 now_ns)
{
	return response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT
		   && response->status == PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER
		   && response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_NONE
		   && memcmp(response->transport_nonce, op->transport_nonce, sizeof(op->transport_nonce))
				  == 0
		   && memcmp(response->operation_id, op->offer_frame.operation_id,
					 sizeof(response->operation_id))
				  == 0
		   && response->system_identifier == op->rejoin_need.system_identifier
		   && memcmp(response->rejoin_gate_digest, op->rejoin_need.rejoin_gate_digest,
					 sizeof(response->rejoin_gate_digest))
				  == 0
		   && memcmp(response->protected_set_digest, op->rejoin_need.protected_set_digest,
					 sizeof(response->protected_set_digest))
				  == 0
		   && response->old_node_id == op->rejoin_need.old_node_id
		   && response->old_incarnation == op->rejoin_need.old_incarnation
		   && response->candidate_incarnation == op->rejoin_need.candidate_incarnation
		   && response->provider_id == op->offer_frame.provider_id
		   && response->provider_abi_version == op->offer_frame.provider_abi_version
		   && response->target_mapping_generation == op->offer_frame.target_mapping_generation
		   && memcmp(response->daemon_boot_id, op->offer_frame.daemon_boot_id,
					 sizeof(response->daemon_boot_id))
				  == 0
		   && response->journal_seq > op->offer_frame.journal_seq
		   && response->proof_generation > op->offer_frame.proof_generation
		   && response->verified_mono_ns != 0
		   && response->fresh_until_mono_ns > response->verified_mono_ns
		   && response->fresh_until_mono_ns - response->verified_mono_ns
				  <= PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS
		   && response->verified_mono_ns <= now_ns && now_ns < response->fresh_until_mono_ns
		   && now_ns < op->deadline_mono_ns
		   && bytes_nonzero(response->target_state_digest, sizeof(response->target_state_digest))
		   && memcmp(response->target_state_digest, op->offer_frame.target_state_digest,
					 sizeof(response->target_state_digest))
				  != 0;
}

static bool
external_fence_rejoin_refresh_result_valid(const PgracExternalFenceRejoinOpV1 *op,
										   const PgracExternalFenceProtocolRejoinFrameV1 *response,
										   uint64 now_ns)
{
	return response->opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT
		   && response->status == PGRAC_EXTERNAL_FENCE_REJOIN_READY
		   && response->deny_reason == PGRAC_EXTERNAL_FENCE_DENY_NONE
		   && memcmp(response->transport_nonce, op->transport_nonce, sizeof(op->transport_nonce))
				  == 0
		   && memcmp(response->operation_id, op->offer_frame.operation_id,
					 sizeof(response->operation_id))
				  == 0
		   && response->system_identifier == op->rejoin_need.system_identifier
		   && memcmp(response->rejoin_gate_digest, op->rejoin_need.rejoin_gate_digest,
					 sizeof(response->rejoin_gate_digest))
				  == 0
		   && memcmp(response->protected_set_digest, op->rejoin_need.protected_set_digest,
					 sizeof(response->protected_set_digest))
				  == 0
		   && response->old_node_id == op->rejoin_need.old_node_id
		   && response->old_incarnation == op->rejoin_need.old_incarnation
		   && response->candidate_incarnation == op->rejoin_need.candidate_incarnation
		   && response->provider_id == op->offer_frame.provider_id
		   && response->provider_abi_version == op->offer_frame.provider_abi_version
		   && response->target_mapping_generation == op->offer_frame.target_mapping_generation
		   && memcmp(response->daemon_boot_id, op->offer_frame.daemon_boot_id,
					 sizeof(response->daemon_boot_id))
				  == 0
		   && response->journal_seq > op->on_frame.journal_seq
		   && response->proof_generation > op->on_frame.proof_generation
		   && response->verified_mono_ns != 0
		   && response->fresh_until_mono_ns > response->verified_mono_ns
		   && response->fresh_until_mono_ns - response->verified_mono_ns
				  <= PGRAC_EXTERNAL_FENCE_MAX_FRESHNESS_NS
		   && response->verified_mono_ns <= now_ns && now_ns < response->fresh_until_mono_ns
		   && now_ns < op->deadline_mono_ns
		   && bytes_nonzero(response->target_state_digest, sizeof(response->target_state_digest))
		   && memcmp(response->target_state_digest, op->on_frame.target_state_digest,
					 sizeof(response->target_state_digest))
				  != 0;
}
#endif

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_start_async(int timeout_ms, PgracExternalFenceRejoinOpV1 **out_op)
{
	PgracExternalFenceRejoinOpV1 *op;
#ifndef WIN32
	PgracExternalFenceProtocolRejoinFrameV1 claim;
	struct sockaddr_un address;
	uint64 now_ns;
	uint64 timeout_ns;
	int flags;
	int connect_result;
#ifdef SO_NOSIGPIPE
	int no_sigpipe = 1;
#endif
#endif

	if (out_op != NULL)
		*out_op = NULL;
	if (out_op == NULL || timeout_ms < 1 || timeout_ms > 600000)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, NULL);

#ifdef WIN32
	external_fence_record_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
								 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, 0, 0);
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, NULL);
#else
	if (cluster_external_fence_socket_path == NULL || cluster_external_fence_socket_path[0] != '/'
		|| strlen(cluster_external_fence_socket_path) >= sizeof(address.sun_path))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG, NULL);
	if (!external_fence_monotonic_now(&now_ns))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, NULL);
	timeout_ns = (uint64)timeout_ms * UINT64_C(1000000);
	if (UINT64_MAX - now_ns < timeout_ns)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, NULL);

	op = palloc0(sizeof(*op));
	op->magic = PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC;
	op->owner_pid = external_fence_owner_pid();
	op->status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	op->socket_fd = -1;
	op->deadline_mono_ns = now_ns + timeout_ns;
	if (!pg_strong_random(op->transport_nonce, sizeof(op->transport_nonce))
		|| !bytes_nonzero(op->transport_nonce, sizeof(op->transport_nonce)))
		goto protocol_failure;

	memset(&claim, 0, sizeof(claim));
	claim.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_CLAIM_NEXT;
	memcpy(claim.transport_nonce, op->transport_nonce, sizeof(claim.transport_nonce));
	claim.status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	claim.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	if (!pgrac_external_fence_rejoin_v1_encode(&claim, op->tx_frame))
		goto protocol_failure;

	op->socket_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (op->socket_fd < 0)
		goto daemon_unavailable;
	flags = fcntl(op->socket_fd, F_GETFL, 0);
	if (flags < 0 || fcntl(op->socket_fd, F_SETFL, flags | O_NONBLOCK) < 0)
		goto socket_failure;
#ifdef SO_NOSIGPIPE
	if (setsockopt(op->socket_fd, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe)) != 0)
		goto socket_failure;
#endif
	memset(&address, 0, sizeof(address));
	address.sun_family = AF_UNIX;
	strlcpy(address.sun_path, cluster_external_fence_socket_path, sizeof(address.sun_path));
	connect_result = connect(op->socket_fd, (struct sockaddr *)&address, sizeof(address));
	if (connect_result != 0) {
		if (errno != EINPROGRESS && errno != EAGAIN && errno != EWOULDBLOCK)
			goto daemon_unavailable;
		op->connect_pending = true;
	}
	*out_op = op;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;

socket_failure:
	external_fence_rejoin_close(op);
	explicit_bzero(op, sizeof(*op));
	pfree(op);
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_SOCKET_CONFIG, NULL);
daemon_unavailable:
	external_fence_rejoin_close(op);
	explicit_bzero(op, sizeof(*op));
	pfree(op);
	external_fence_record_result(PGRAC_EXTERNAL_FENCE_UNAVAILABLE,
								 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, 0, 0);
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, NULL);
protocol_failure:
	explicit_bzero(op, sizeof(*op));
	pfree(op);
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, NULL);
#endif
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_poll_nowait(PgracExternalFenceRejoinOpV1 *op,
										  PgracExternalFenceDenyReason *reason)
{
	PgracExternalFenceProtocolRejoinFrameV1 response;
	uint16 expected_opcode;
#ifndef WIN32
	struct pollfd poll_fd;
	uint64 now_ns;
	ssize_t io_count;
	int socket_error;
	socklen_t socket_error_len;
	int send_flags = MSG_DONTWAIT;
#ifdef MSG_NOSIGNAL
	send_flags |= MSG_NOSIGNAL;
#endif
#endif

	if (reason == NULL || op == NULL || op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid() || op->socket_fd < 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (op->status != PGRAC_EXTERNAL_FENCE_REJOIN_PENDING) {
		*reason = (PgracExternalFenceDenyReason)op->deny_reason;
		return (PgracExternalFenceRejoinStatus)op->status;
	}
#ifdef WIN32
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, reason);
#else
	if (!external_fence_monotonic_now(&now_ns))
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN,
											  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, reason);
	if (now_ns >= op->deadline_mono_ns)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN,
											  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, reason);

	if (op->connect_pending) {
		poll_fd.fd = op->socket_fd;
		poll_fd.events = POLLOUT;
		poll_fd.revents = 0;
		if (poll(&poll_fd, 1, 0) < 0) {
			if (errno == EINTR) {
				*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
				return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
			}
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
												  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE,
												  reason);
		}
		if ((poll_fd.revents & POLLOUT) == 0) {
			if ((poll_fd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0)
				return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
													  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE,
													  reason);
			*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
		}
		socket_error = 0;
		socket_error_len = sizeof(socket_error);
		if (getsockopt(op->socket_fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) != 0
			|| socket_error != 0)
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
												  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE,
												  reason);
		op->connect_pending = false;
	}
	if (!op->peer_authenticated) {
		if (!external_fence_root_peer_authenticated(op->socket_fd))
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
												  PGRAC_EXTERNAL_FENCE_DENY_PEER_AUTH, reason);
		op->peer_authenticated = true;
	}

	if (op->tx_sent < sizeof(op->tx_frame)) {
		io_count = send(op->socket_fd, op->tx_frame + op->tx_sent,
						sizeof(op->tx_frame) - op->tx_sent, send_flags);
		if (io_count < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
				*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
				return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
			}
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED,
												  reason);
		}
		if (io_count == 0)
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED,
												  reason);
		op->tx_sent += (size_t)io_count;
		if (op->tx_sent < sizeof(op->tx_frame)) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
		}
	}

	io_count = recv(op->socket_fd, op->rx_frame + op->rx_used, sizeof(op->rx_frame) - op->rx_used,
					MSG_DONTWAIT);
	if (io_count < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
		}
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	}
	if (io_count == 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	op->rx_used += (size_t)io_count;
	if (op->rx_used < sizeof(op->rx_frame)) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	}
	if (op->refresh_sent)
		expected_opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_RESULT;
	else if (op->moved_clear != NULL)
		expected_opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT;
	else
		expected_opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER;
	if (!pgrac_external_fence_rejoin_v1_decode(op->rx_frame, sizeof(op->rx_frame), &response)
		|| response.opcode != expected_opcode
		|| memcmp(response.transport_nonce, op->transport_nonce, sizeof(op->transport_nonce)) != 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
											  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
	if (response.status != PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED
		&& response.status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER
		&& response.status != PGRAC_EXTERNAL_FENCE_REJOIN_READY) {
		if (response.status < PGRAC_EXTERNAL_FENCE_REJOIN_REJECTED
			|| response.status > PGRAC_EXTERNAL_FENCE_REJOIN_STALE
			|| response.deny_reason < PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT
			|| response.deny_reason > PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR)
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
												  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
		return external_fence_rejoin_terminal(op, (PgracExternalFenceRejoinStatus)response.status,
											  (PgracExternalFenceDenyReason)response.deny_reason,
											  reason);
	}
	if (expected_opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_OFFER) {
		if (!external_fence_rejoin_offer_valid(op, &response, now_ns))
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH,
												  reason);

		op->offer_frame = response;
		memcpy(op->offer.operation_id, response.operation_id, sizeof(op->offer.operation_id));
		op->offer.old_node_id = response.old_node_id;
		op->offer.old_incarnation = response.old_incarnation;
		op->offer.candidate_incarnation = response.candidate_incarnation;
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED;
	} else if (expected_opcode == PGRAC_EXTERNAL_FENCE_REJOIN_LMON_ON_RESULT) {
		if (!external_fence_rejoin_on_result_valid(op, &response, now_ns))
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH,
												  reason);
		op->on_frame = response;
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER;
	} else {
		if (!external_fence_rejoin_refresh_result_valid(op, &response, now_ns))
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH,
												  reason);
		op->ready_frame = response;
		memset(&op->binding, 0, sizeof(op->binding));
		op->binding.system_identifier = response.system_identifier;
		memcpy(op->binding.rejoin_gate_digest, response.rejoin_gate_digest,
			   sizeof(op->binding.rejoin_gate_digest));
		op->binding.old_node_id = response.old_node_id;
		op->binding.old_incarnation = response.old_incarnation;
		op->binding.candidate_incarnation = response.candidate_incarnation;
		op->binding.target_mapping_generation = response.target_mapping_generation;
		memcpy(op->binding.protected_set_digest, response.protected_set_digest,
			   sizeof(op->binding.protected_set_digest));
		op->binding.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_REJOIN_ON;
		op->binding.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
		op->root_revalidated = false;
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_READY;
	}
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return (PgracExternalFenceRejoinStatus)op->status;
#endif
}

const PgracExternalFenceRejoinOfferV1 *
cluster_external_fence_rejoin_offer(const PgracExternalFenceRejoinOpV1 *op)
{
	if (op == NULL || op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid()
		|| (op->status != PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED
			&& op->status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT))
		return NULL;
	return &op->offer;
}

static bool
external_fence_rejoin_root_complete_valid(const PgracExternalFenceRejoinOpV1 *op,
										  const ClusterControlRootIdentity *identity,
										  const ClusterControlRootSnapshot *snapshot,
										  const ClusterControlRootReadToken *token)
{
	return cluster_recovery_duty_key_valid_v1(identity)
		   && identity->system_identifier == op->offer_frame.system_identifier
		   && identity->origin_node_id == op->offer_frame.old_node_id
		   && identity->origin_thread_id == (uint16)(op->offer_frame.old_node_id + 1)
		   && identity->origin_owner_incarnation == op->offer_frame.old_incarnation
		   && memcmp(&snapshot->identity, identity, sizeof(*identity)) == 0
		   && snapshot->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		   && snapshot->root_publish_seq != 0 && snapshot->reserved96 == 0
		   && snapshot->reserved122 == 0 && snapshot->reserved124 == 0 && snapshot->reserved160 == 0
		   && snapshot->reserved208 == 0
		   && memcmp(token->authority_uuid, identity->authority_uuid, sizeof(token->authority_uuid))
				  == 0
		   && token->origin_thread_id == identity->origin_thread_id
		   && token->source == PGRAC_EXTERNAL_FENCE_ROOT_SOURCE_PRIMARY
		   && token->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		   && token->reserved20 == 0 && token->reserved32 == 0
		   && token->root_lineage_seq == identity->root_lineage_seq && token->file_txn_seq != 0
		   && token->root_publish_seq == snapshot->root_publish_seq && token->record_crc32c != 0
		   && token->root_flags == snapshot->root_flags;
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_authority_clear_build(
	const PgracExternalFenceRejoinOpV1 *offered_op,
	const ClusterReconfigRejoinFailureSnapshotV1 *failure,
	const ClusterGrdRejoinClearSnapshotV1 *grd_clear,
	PgracExternalFenceRejoinAuthorityClearV1 **out_clear, PgracExternalFenceDenyReason *reason)
{
	PgracExternalFenceRejoinAuthorityClearV1 *clear;
	ClusterReconfigRejoinFailureSnapshotV1 current_failure;
	ClusterGrdRejoinClearSnapshotV1 current_grd_clear;
	uint32 survivor_count;
	int i;
#ifndef WIN32
	struct pollfd poll_fd;
	uint64 now_ns;
#endif

	if (out_clear != NULL)
		*out_clear = NULL;
	if (reason == NULL || out_clear == NULL || offered_op == NULL || failure == NULL
		|| grd_clear == NULL)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (offered_op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| offered_op->owner_pid != external_fence_owner_pid() || offered_op->socket_fd < 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (offered_op->status != PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH, reason);

	if (failure->reconfig_kind != RECONFIG_KIND_FAIL_STOP || failure->reserved0 != 0
		|| failure->reserved68 != 0 || failure->event_id == 0 || failure->new_epoch == 0
		|| failure->cssd_dead_generation == 0 || failure->old_node_id < 0
		|| failure->old_node_id >= CLUSTER_MAX_NODES || failure->old_incarnation == 0
		|| !external_fence_bitmap_member(failure->dead_bitmap, failure->old_node_id)
		|| external_fence_bitmap_member(failure->survivor_bitmap, failure->old_node_id))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
										  reason);
	survivor_count = external_fence_bitmap_popcount(failure->survivor_bitmap);
	if (survivor_count < 1 || survivor_count >= CLUSTER_MAX_NODES
		|| grd_clear->episode_epoch != failure->new_epoch || grd_clear->dead_bitmap_hash == 0
		|| memcmp(grd_clear->survivor_bitmap, failure->survivor_bitmap,
				  sizeof(grd_clear->survivor_bitmap))
			   != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_PENDING,
										  PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR, reason);
	for (i = 0; i < 16; i++) {
		if ((failure->dead_bitmap[i] & failure->survivor_bitmap[i]) != 0)
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
											  reason);
	}
	if (offered_op->offer_frame.old_node_id != failure->old_node_id
		|| offered_op->offer_frame.old_incarnation != failure->old_incarnation
		|| memcmp(offered_op->offer.operation_id, offered_op->offer_frame.operation_id,
				  sizeof(offered_op->offer.operation_id))
			   != 0
		|| offered_op->offer.old_node_id != failure->old_node_id
		|| offered_op->offer.old_incarnation != failure->old_incarnation
		|| offered_op->offer.candidate_incarnation != offered_op->offer_frame.candidate_incarnation)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH, reason);

	memset(&current_failure, 0, sizeof(current_failure));
	if (!cluster_reconfig_rejoin_failure_snapshot(failure->old_node_id, failure->old_incarnation,
												  &current_failure)
		|| memcmp(&current_failure, failure, sizeof(current_failure)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
										  reason);
	memset(&current_grd_clear, 0, sizeof(current_grd_clear));
	if (!cluster_grd_rejoin_clear_snapshot(&current_failure, &current_grd_clear)
		|| memcmp(&current_grd_clear, grd_clear, sizeof(current_grd_clear)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_PENDING,
										  PGRAC_EXTERNAL_FENCE_DENY_GRD_NOT_CLEAR, reason);

#ifdef WIN32
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, reason);
#else
	if (!external_fence_monotonic_now(&now_ns)
		|| !external_fence_rejoin_offer_valid(offered_op, &offered_op->offer_frame, now_ns))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_EXPIRED, reason);
	poll_fd.fd = offered_op->socket_fd;
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	poll_fd.revents = 0;
	if (poll(&poll_fd, 1, 0) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);

	clear = palloc0(sizeof(*clear));
	clear->magic = PGRAC_EXTERNAL_FENCE_REJOIN_CLEAR_MAGIC;
	clear->owner_pid = external_fence_owner_pid();
	clear->operation_deadline_mono_ns = offered_op->deadline_mono_ns;
	clear->failure = *failure;
	clear->grd_clear = *grd_clear;
	clear->offer_frame = offered_op->offer_frame;
	if (!external_fence_rejoin_clear_digest(offered_op, failure, grd_clear,
											clear->authority_clear_digest)) {
		explicit_bzero(clear, sizeof(*clear));
		pfree(clear);
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
	}
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	((PgracExternalFenceRejoinOpV1 *)offered_op)->status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
	((PgracExternalFenceRejoinOpV1 *)offered_op)->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	*out_clear = clear;
	return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
#endif
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_authorize_on_async(
	PgracExternalFenceRejoinOpV1 *op, PgracExternalFenceRejoinAuthorityClearV1 **authority_clear,
	const ClusterControlRootIdentity *old_identity,
	const ClusterControlRootSnapshot *complete_snapshot,
	const ClusterControlRootReadToken *complete_token, const uint8 protected_set_digest[32],
	PgracExternalFenceDenyReason *reason)
{
	PgracExternalFenceRejoinAuthorityClearV1 *clear;
	ClusterReconfigRejoinFailureSnapshotV1 current_failure;
	ClusterGrdRejoinClearSnapshotV1 current_grd_clear;
	uint8 recalculated_clear_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
	uint8 gate_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];
#ifndef WIN32
	PgracExternalFenceProtocolRejoinFrameV1 authorize;
	struct pollfd poll_fd;
	uint64 now_ns;
	uint64 remaining_ns;
	uint64 remaining_ms;
	ssize_t io_count;
	int send_flags = MSG_DONTWAIT;
#ifdef MSG_NOSIGNAL
	send_flags |= MSG_NOSIGNAL;
#endif
#endif

	if (reason == NULL || op == NULL || authority_clear == NULL || old_identity == NULL
		|| complete_snapshot == NULL || complete_token == NULL || protected_set_digest == NULL)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid() || op->socket_fd < 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (*authority_clear == NULL)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING,
										  reason);
	clear = *authority_clear;
	if (clear->magic != PGRAC_EXTERNAL_FENCE_REJOIN_CLEAR_MAGIC
		|| clear->owner_pid != external_fence_owner_pid()
		|| memcmp(&clear->offer_frame, &op->offer_frame, sizeof(clear->offer_frame)) != 0
		|| clear->operation_deadline_mono_ns != op->deadline_mono_ns)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING,
										  reason);
	if (op->status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT
		&& op->status != PGRAC_EXTERNAL_FENCE_REJOIN_OFFERED)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING,
										  reason);
	if (complete_snapshot->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE) {
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
		op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_ROOT_NOT_COMPLETE;
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT,
										  PGRAC_EXTERNAL_FENCE_DENY_ROOT_NOT_COMPLETE, reason);
	}
	if (!external_fence_rejoin_root_complete_valid(op, old_identity, complete_snapshot,
												   complete_token))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_ROOT_STALE, reason);
	if (!bytes_nonzero(protected_set_digest, PGRAC_EXTERNAL_FENCE_DIGEST_BYTES)
		|| memcmp(protected_set_digest, op->offer_frame.protected_set_digest,
				  PGRAC_EXTERNAL_FENCE_DIGEST_BYTES)
			   != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_OFFER_MISMATCH, reason);
	memset(&current_failure, 0, sizeof(current_failure));
	memset(&current_grd_clear, 0, sizeof(current_grd_clear));
	if (!cluster_reconfig_rejoin_failure_snapshot(clear->failure.old_node_id,
												  clear->failure.old_incarnation, &current_failure)
		|| memcmp(&current_failure, &clear->failure, sizeof(current_failure)) != 0
		|| !cluster_grd_rejoin_clear_snapshot(&current_failure, &current_grd_clear)
		|| memcmp(&current_grd_clear, &clear->grd_clear, sizeof(current_grd_clear)) != 0
		|| !external_fence_rejoin_clear_digest(op, &clear->failure, &clear->grd_clear,
											   recalculated_clear_digest)
		|| memcmp(recalculated_clear_digest, clear->authority_clear_digest,
				  sizeof(recalculated_clear_digest))
			   != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_STALE, reason);

#ifdef WIN32
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, reason);
#else
	if (!external_fence_monotonic_now(&now_ns) || now_ns >= op->deadline_mono_ns
		|| now_ns < op->offer_frame.verified_mono_ns
		|| now_ns >= op->offer_frame.fresh_until_mono_ns)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_STALE,
											  reason);
	poll_fd.fd = op->socket_fd;
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	poll_fd.revents = 0;
	if (poll(&poll_fd, 1, 0) != 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);

	if (!op->authorize_enqueue_pending) {
		if (!external_fence_rejoin_root_completion_digest(old_identity, complete_token,
														  op->root_completion_digest)
			|| !external_fence_rejoin_gate_digest(op->root_completion_digest,
												  clear->authority_clear_digest, gate_digest)
			|| !pg_strong_random(op->transport_nonce, sizeof(op->transport_nonce))
			|| !bytes_nonzero(op->transport_nonce, sizeof(op->transport_nonce)))
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
											  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
		memset(&authorize, 0, sizeof(authorize));
		authorize.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_AUTHORIZE_ON;
		memcpy(authorize.transport_nonce, op->transport_nonce, sizeof(authorize.transport_nonce));
		memcpy(authorize.operation_id, op->offer_frame.operation_id,
			   sizeof(authorize.operation_id));
		authorize.system_identifier = op->offer_frame.system_identifier;
		memcpy(authorize.rejoin_gate_digest, gate_digest, sizeof(authorize.rejoin_gate_digest));
		memcpy(authorize.protected_set_digest, protected_set_digest,
			   sizeof(authorize.protected_set_digest));
		authorize.old_node_id = op->offer_frame.old_node_id;
		authorize.old_incarnation = op->offer_frame.old_incarnation;
		authorize.candidate_incarnation = op->offer_frame.candidate_incarnation;
		remaining_ns = op->deadline_mono_ns - now_ns;
		remaining_ms = (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
		if (remaining_ms < 1 || remaining_ms > 600000)
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
												  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_STALE,
												  reason);
		authorize.timeout_ms = (uint32)remaining_ms;
		authorize.status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
		authorize.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		if (!pgrac_external_fence_rejoin_v1_encode(&authorize, op->tx_frame))
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
											  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
		op->tx_sent = 0;
		op->rx_used = 0;
		op->authorize_enqueue_pending = true;
		op->authorize_clear_borrowed = clear;
		op->saved_root_identity = *old_identity;
		op->saved_root_snapshot = *complete_snapshot;
		op->saved_root_token = *complete_token;
		memset(&op->rejoin_need, 0, sizeof(op->rejoin_need));
		op->rejoin_need.system_identifier = authorize.system_identifier;
		memcpy(op->rejoin_need.rejoin_gate_digest, authorize.rejoin_gate_digest,
			   sizeof(op->rejoin_need.rejoin_gate_digest));
		op->rejoin_need.old_node_id = authorize.old_node_id;
		op->rejoin_need.old_incarnation = authorize.old_incarnation;
		op->rejoin_need.candidate_incarnation = authorize.candidate_incarnation;
		memcpy(op->rejoin_need.protected_set_digest, authorize.protected_set_digest,
			   sizeof(op->rejoin_need.protected_set_digest));
		op->rejoin_need.predicate_id = PGRAC_EXTERNAL_FENCE_PREDICATE_REJOIN_ON;
		op->rejoin_need.predicate_version = PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1;
	} else if (op->authorize_clear_borrowed != clear
			   || memcmp(&op->saved_root_identity, old_identity, sizeof(*old_identity)) != 0
			   || memcmp(&op->saved_root_snapshot, complete_snapshot, sizeof(*complete_snapshot))
					  != 0
			   || memcmp(&op->saved_root_token, complete_token, sizeof(*complete_token)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_AUTHORITY_CLEAR_MISSING,
										  reason);

	io_count = send(op->socket_fd, op->tx_frame + op->tx_sent, sizeof(op->tx_frame) - op->tx_sent,
					send_flags);
	if (io_count < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
			op->status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
			op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
		}
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	}
	if (io_count == 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	op->tx_sent += (size_t)io_count;
	if (op->tx_sent < sizeof(op->tx_frame)) {
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
		op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_ROOT;
	}
	op->moved_clear = clear;
	op->authorize_clear_borrowed = NULL;
	op->authorize_enqueue_pending = false;
	*authority_clear = NULL;
	op->status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
#endif
}

PgracExternalFenceRejoinStatus
cluster_external_fence_rejoin_refresh_on_async(
	PgracExternalFenceRejoinOpV1 *op, const ClusterReconfigRejoinPendingSnapshotV1 *pending,
	PgracExternalFenceDenyReason *reason)
{
	ClusterReconfigRejoinPendingSnapshotV1 current_pending;
	uint32 join_count;
	int i;
#ifndef WIN32
	PgracExternalFenceProtocolRejoinFrameV1 refresh;
	struct pollfd poll_fd;
	uint64 now_ns;
	uint64 remaining_ns;
	uint64 remaining_ms;
	ssize_t io_count;
	int send_flags = MSG_DONTWAIT;
#ifdef MSG_NOSIGNAL
	send_flags |= MSG_NOSIGNAL;
#endif
#endif

	if (reason == NULL || op == NULL || pending == NULL)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid() || op->socket_fd < 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										  PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
	if (op->status != PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER || op->moved_clear == NULL)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
										  reason);
	if (pending->reconfig_kind != RECONFIG_KIND_JOIN_PENDING || pending->reserved0 != 0
		|| pending->reserved76 != 0 || pending->event_id == 0 || pending->old_epoch == 0
		|| pending->old_epoch == UINT64_MAX || pending->new_epoch != pending->old_epoch + 1
		|| pending->cssd_dead_generation == 0 || pending->node_id != op->offer_frame.old_node_id
		|| pending->candidate_incarnation != op->offer_frame.candidate_incarnation
		|| pending->observed_slot_generation == 0
		|| pending->old_epoch != op->moved_clear->failure.new_epoch)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH,
										  reason);
	join_count = external_fence_bitmap_popcount(pending->join_bitmap);
	if (join_count != 1 || !external_fence_bitmap_member(pending->join_bitmap, pending->node_id))
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
										  reason);
	for (i = 0; i < 16; i++) {
		if (pending->dead_bitmap[i] != 0)
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
											  reason);
	}
	memset(&current_pending, 0, sizeof(current_pending));
	if (!cluster_reconfig_rejoin_pending_snapshot(&op->moved_clear->failure,
												  pending->candidate_incarnation, &current_pending)
		|| memcmp(&current_pending, pending, sizeof(current_pending)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH,
										  reason);
	if (!cluster_reconfig_rejoin_pending_ready(&current_pending)) {
		op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_JOIN_NOT_READY;
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER,
										  PGRAC_EXTERNAL_FENCE_DENY_JOIN_NOT_READY, reason);
	}
	memset(&current_pending, 0, sizeof(current_pending));
	if (!cluster_reconfig_rejoin_pending_snapshot(&op->moved_clear->failure,
												  pending->candidate_incarnation, &current_pending)
		|| memcmp(&current_pending, pending, sizeof(current_pending)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH,
										  reason);

#ifdef WIN32
	return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									  PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, reason);
#else
	if (!external_fence_monotonic_now(&now_ns) || now_ns >= op->deadline_mono_ns)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN,
											  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, reason);
	poll_fd.fd = op->socket_fd;
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	poll_fd.revents = 0;
	if (poll(&poll_fd, 1, 0) != 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);

	if (!op->refresh_enqueue_pending) {
		memset(&refresh, 0, sizeof(refresh));
		refresh.opcode = PGRAC_EXTERNAL_FENCE_REJOIN_LMON_REFRESH_ON;
		if (!pg_strong_random(op->transport_nonce, sizeof(op->transport_nonce))
			|| !bytes_nonzero(op->transport_nonce, sizeof(op->transport_nonce)))
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
											  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
		memcpy(refresh.transport_nonce, op->transport_nonce, sizeof(refresh.transport_nonce));
		memcpy(refresh.operation_id, op->offer_frame.operation_id, sizeof(refresh.operation_id));
		refresh.system_identifier = op->rejoin_need.system_identifier;
		memcpy(refresh.rejoin_gate_digest, op->rejoin_need.rejoin_gate_digest,
			   sizeof(refresh.rejoin_gate_digest));
		memcpy(refresh.protected_set_digest, op->rejoin_need.protected_set_digest,
			   sizeof(refresh.protected_set_digest));
		refresh.old_node_id = op->rejoin_need.old_node_id;
		refresh.old_incarnation = op->rejoin_need.old_incarnation;
		refresh.candidate_incarnation = op->rejoin_need.candidate_incarnation;
		remaining_ns = op->deadline_mono_ns - now_ns;
		remaining_ms = (remaining_ns + UINT64_C(999999)) / UINT64_C(1000000);
		if (remaining_ms < 1 || remaining_ms > 600000)
			return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_UNKNOWN,
												  PGRAC_EXTERNAL_FENCE_DENY_TIMEOUT, reason);
		refresh.timeout_ms = (uint32)remaining_ms;
		refresh.status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
		refresh.deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		if (!pgrac_external_fence_rejoin_v1_encode(&refresh, op->tx_frame))
			return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
											  PGRAC_EXTERNAL_FENCE_DENY_PROTOCOL, reason);
		op->tx_sent = 0;
		op->rx_used = 0;
		op->refresh_pending = *pending;
		op->refresh_enqueue_pending = true;
	} else if (memcmp(&op->refresh_pending, pending, sizeof(*pending)) != 0)
		return external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										  PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH,
										  reason);

	io_count = send(op->socket_fd, op->tx_frame + op->tx_sent, sizeof(op->tx_frame) - op->tx_sent,
					send_flags);
	if (io_count < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
			*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
			return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER;
		}
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	}
	if (io_count == 0)
		return external_fence_rejoin_terminal(op, PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
											  PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
	op->tx_sent += (size_t)io_count;
	if (op->tx_sent < sizeof(op->tx_frame)) {
		*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
		return PGRAC_EXTERNAL_FENCE_REJOIN_WAITING_JOINER;
	}
	op->refresh_enqueue_pending = false;
	op->refresh_sent = true;
	op->status = PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return PGRAC_EXTERNAL_FENCE_REJOIN_PENDING;
#endif
}

bool
cluster_external_fence_rejoin_revalidate_root(PgracExternalFenceRejoinOpV1 *op,
											  ClusterControlRootSnapshot *out_fresh_snapshot,
											  PgracExternalFenceDenyReason *reason)
{
	ClusterControlRootSnapshot fresh_snapshot;
	ClusterControlRootResult result;
	uint8 root_completion_digest[PGRAC_EXTERNAL_FENCE_DIGEST_BYTES];

	if (out_fresh_snapshot != NULL)
		memset(out_fresh_snapshot, 0, sizeof(*out_fresh_snapshot));
	if (reason == NULL || op == NULL || out_fresh_snapshot == NULL) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										 PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
		return false;
	}
	if (op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid()
		|| op->status != PGRAC_EXTERNAL_FENCE_REJOIN_READY || op->moved_clear == NULL
		|| op->consumed) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										 PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
		return false;
	}
	op->root_revalidated = false;
	memset(&fresh_snapshot, 0, sizeof(fresh_snapshot));
	result = cluster_control_root_revalidate(&op->saved_root_token, &op->saved_root_identity,
											 &fresh_snapshot);
	if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !external_fence_rejoin_root_complete_valid(op, &op->saved_root_identity, &fresh_snapshot,
													  &op->saved_root_token)
		|| memcmp(&fresh_snapshot, &op->saved_root_snapshot, sizeof(fresh_snapshot)) != 0
		|| !external_fence_rejoin_root_completion_digest(
			&op->saved_root_identity, &op->saved_root_token, root_completion_digest)
		|| memcmp(root_completion_digest, op->root_completion_digest,
				  sizeof(root_completion_digest))
			   != 0) {
		op->status = PGRAC_EXTERNAL_FENCE_REJOIN_STALE;
		op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_ROOT_STALE;
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_ROOT_STALE, reason);
		return false;
	}
	op->root_revalidated = true;
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	*out_fresh_snapshot = fresh_snapshot;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
}

bool
cluster_external_fence_rejoin_consume_nowait(
	PgracExternalFenceRejoinOpV1 *op, const ClusterReconfigRejoinPendingSnapshotV1 *current_pending,
	const ClusterJoinCommitMarker *committed_candidate, PgracExternalFenceDenyReason *reason)
{
	uint64 now_ns;
#ifndef WIN32
	struct pollfd poll_fd;
#endif

	if (reason == NULL || op == NULL || current_pending == NULL || committed_candidate == NULL) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										 PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
		return false;
	}
	if (op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid()) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
										 PGRAC_EXTERNAL_FENCE_DENY_BAD_ARGUMENT, reason);
		return false;
	}
	if (op->consumed) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_CONSUMED,
										 PGRAC_EXTERNAL_FENCE_DENY_REJOIN_CONSUMED, reason);
		return false;
	}
	if (op->status != PGRAC_EXTERNAL_FENCE_REJOIN_READY || op->moved_clear == NULL
		|| !op->root_revalidated || op->socket_fd < 0) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH, reason);
		return false;
	}
	if (memcmp(current_pending, &op->refresh_pending, sizeof(*current_pending)) != 0
		|| current_pending->old_epoch != op->moved_clear->failure.new_epoch
		|| current_pending->new_epoch == UINT64_MAX
		|| current_pending->node_id != op->rejoin_need.old_node_id
		|| current_pending->candidate_incarnation != op->rejoin_need.candidate_incarnation
		|| !cluster_reconfig_rejoin_pending_ready(current_pending)
		|| cluster_membership_get_state(current_pending->node_id) != CLUSTER_MEMBER_JOINING
		|| cluster_membership_get_last_admitted_incarnation(current_pending->node_id)
			   >= current_pending->candidate_incarnation
		|| cluster_epoch_get_current() != current_pending->new_epoch) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_REJOIN_LINEAGE_MISMATCH, reason);
		return false;
	}
	if (!cluster_join_marker_struct_valid(committed_candidate, current_pending->node_id)
		|| committed_candidate->phase != CLUSTER_JCMK_PHASE_COMMITTED
		|| committed_candidate->_pad[0] != 0 || committed_candidate->_pad[1] != 0
		|| committed_candidate->_pad[2] != 0
		|| committed_candidate->generation != current_pending->candidate_incarnation
		|| committed_candidate->admitted_incarnation != current_pending->candidate_incarnation
		|| committed_candidate->admitted_epoch != current_pending->new_epoch + 1
		|| committed_candidate->commit_nonce == 0) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_JOIN_CANDIDATE_MISMATCH, reason);
		return false;
	}
	if (op->binding.system_identifier != op->rejoin_need.system_identifier
		|| memcmp(op->binding.rejoin_gate_digest, op->rejoin_need.rejoin_gate_digest,
				  sizeof(op->binding.rejoin_gate_digest))
			   != 0
		|| op->binding.old_node_id != op->rejoin_need.old_node_id || op->binding.reserved44 != 0
		|| op->binding.old_incarnation != op->rejoin_need.old_incarnation
		|| op->binding.candidate_incarnation != op->rejoin_need.candidate_incarnation
		|| op->binding.target_mapping_generation != op->ready_frame.target_mapping_generation
		|| memcmp(op->binding.protected_set_digest, op->rejoin_need.protected_set_digest,
				  sizeof(op->binding.protected_set_digest))
			   != 0
		|| op->binding.predicate_id != PGRAC_EXTERNAL_FENCE_PREDICATE_REJOIN_ON
		|| op->binding.predicate_version != PGRAC_EXTERNAL_FENCE_PREDICATE_VERSION_V1) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_BINDING_MISMATCH, reason);
		return false;
	}

#ifdef WIN32
	(void)now_ns;
	(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_UNAVAILABLE,
									 PGRAC_EXTERNAL_FENCE_DENY_DAEMON_UNAVAILABLE, reason);
	return false;
#else
	if (!external_fence_monotonic_now(&now_ns) || now_ns >= op->deadline_mono_ns
		|| !external_fence_rejoin_refresh_result_valid(op, &op->ready_frame, now_ns)) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_EXPIRED, reason);
		return false;
	}
	poll_fd.fd = op->socket_fd;
	poll_fd.events = POLLIN | POLLHUP | POLLERR;
	poll_fd.revents = 0;
	if (poll(&poll_fd, 1, 0) != 0) {
		(void)external_fence_rejoin_fail(PGRAC_EXTERNAL_FENCE_REJOIN_STALE,
										 PGRAC_EXTERNAL_FENCE_DENY_CONNECTION_CLOSED, reason);
		return false;
	}

	/* The local state edge is first: no later caller can consume the same
	 * authority even if cleanup below is interrupted. */
	op->consumed = true;
	op->status = PGRAC_EXTERNAL_FENCE_REJOIN_CONSUMED;
	op->deny_reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	op->root_revalidated = false;
	explicit_bzero(op->moved_clear, sizeof(*op->moved_clear));
	pfree(op->moved_clear);
	op->moved_clear = NULL;
	*reason = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	external_fence_last_deny = PGRAC_EXTERNAL_FENCE_DENY_NONE;
	return true;
#endif
}

const PgracExternalFenceRejoinBindingV1 *
cluster_external_fence_rejoin_binding(const PgracExternalFenceRejoinOpV1 *op)
{
	if (op == NULL || op->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| op->owner_pid != external_fence_owner_pid()
		|| op->status != PGRAC_EXTERNAL_FENCE_REJOIN_READY)
		return NULL;
	return &op->binding;
}

void
cluster_external_fence_rejoin_release(PgracExternalFenceRejoinOpV1 **op)
{
	PgracExternalFenceRejoinOpV1 *value;

	if (op == NULL || *op == NULL)
		return;
	value = *op;
	if (value->magic != PGRAC_EXTERNAL_FENCE_REJOIN_OP_MAGIC
		|| value->owner_pid != external_fence_owner_pid()) {
		*op = NULL;
		return;
	}
#ifndef WIN32
	if (value->socket_fd >= 0)
		(void)close(value->socket_fd);
#endif
	if (value->moved_clear != NULL) {
		explicit_bzero(value->moved_clear, sizeof(*value->moved_clear));
		pfree(value->moved_clear);
		value->moved_clear = NULL;
	}
	memset(value, 0, sizeof(*value));
	pfree(value);
	*op = NULL;
}

void
cluster_external_fence_rejoin_authority_clear_release(
	PgracExternalFenceRejoinAuthorityClearV1 **authority_clear)
{
	PgracExternalFenceRejoinAuthorityClearV1 *value;

	if (authority_clear == NULL || *authority_clear == NULL)
		return;
	value = *authority_clear;
	if (value->magic == PGRAC_EXTERNAL_FENCE_REJOIN_CLEAR_MAGIC
		&& value->owner_pid == external_fence_owner_pid()) {
		memset(value, 0, sizeof(*value));
		pfree(value);
	}
	*authority_clear = NULL;
}

PgracExternalFenceDenyReason
cluster_external_fence_last_deny_reason(void)
{
	return external_fence_last_deny;
}
