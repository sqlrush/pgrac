/*-------------------------------------------------------------------------
 *
 * cluster_recovery_duty.c
 *	  No-generation failed-origin recovery-duty identity (RF-ROOT P2).
 *
 * The full STOP-01 root identity is the sole durable duty key.  Formation
 * freshness and execution authority remain separate STOP-02/STOP-03 proofs;
 * this module deliberately creates no scalar generation or durable object.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_semantic_activation.h" /* R4 cutover ACK proof (G3) */
#include "cluster/cluster_wal_state.h"			 /* G4 runtime census gate (bit22) */
#include "cluster/cluster_recovery_duty.h"
#include "cluster_control_root_private.h"
#include "cluster/cluster_startup_phase.h" /* serving rebind (recovery path retry) */
#include "cluster/cluster_wal_thread.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "portability/instr_time.h"
#include "storage/latch.h"
#include "miscadmin.h" /* MyLatch / CHECK_FOR_INTERRUPTS */
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CHECKPOINTER_MAIN */

/* RF-ROOT P7 recovery path: bounded THREAD_CLEAN_CLOSE retry window (never block
 * the clean shutdown beyond this; the 5.13 drain already bounds the
 * handoff side). */
#define CLUSTER_CLEAN_CLOSE_RETRY_MS 5000

typedef struct ClusterControlRootPublishAuthorityV1 {
	bool active;
	ClusterControlRootPublishReason reason;
	ClusterControlRootReadToken token;
	ClusterControlRootPatch patch;
} ClusterControlRootPublishAuthorityV1;

static ClusterControlRootPublishAuthorityV1 root_publish_authority;

static void cluster_control_root_publish_authority_clear_v1(void);

bool
cluster_control_root_create_authority_current_v1(const ClusterControlRootMigrationImage *image,
												 const ClusterControlRootMigrationRoundV1 *round)
{
	/* RF-ROOT P7 G3 (R4 cutover batch, Stage 8 contract): the
	 * coordinator's exact one-shot proof, replacing the P5 refusal.  This
	 * process must be the round's coordinator; the ACK table must be
	 * COMPLETE (every member observed == expected) bound to this exact
	 * round identity; the round must carry bit22 in its target bitmap.
	 * Fail-closed on any mismatch.  The census gate is CI-enforced
	 * (scripts/ci/check-wal-state-correctness-census.sh strict) + the
	 * unit-tested whitelist assertions; the migration image itself is
	 * validated by create_prepared before this call. */
	if (image == NULL || round == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	if ((int32)round->coordinator_node_id != cluster_node_id)
		return false;
	/* RF-ROOT P9 verification (contract, follow-up option b): the bit22 cutover
	 * round's create is EXEMPT from the SAMPLE-stage ACK precondition —
	 * create only mints the PREPARED root (no authority granted until
	 * activate), and the W6 clause-3 CLOSED-ACK binding lives in the
	 * activate proof (activate_authority_current_v1 requires the
	 * PREPARED-stage all-member COMPLETE).  The SAMPLE stage is R4
	 * capability sampling, which the cutover round bypasses (member set +
	 * capabilities come from current_authority + IC sampling in begin).
	 * R4 rounds (no bit22) keep the frozen precondition. */
	if ((round->target_feature_bitmap & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1) == 0
		&& !cluster_semantic_activation_ack_complete_matches(
			round->transition_epoch, round->prepare_generation, round->admitted_bitmap_low,
			round->admitted_bitmap_high, round->source_feature_bitmap, round->target_feature_bitmap,
			round->capability_sample_digest, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_SAMPLE))
		return false;
	if (!cluster_control_root_feature_bitmap_is_known(round->source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(round->target_feature_bitmap)
		|| (round->target_feature_bitmap & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1)
			   == 0)
		return false;
	return true;
}

bool
cluster_control_root_activate_authority_current_v1(
	const ClusterControlRootFileToken *expected_token, const uint8 expected_round_sha256[32],
	const ClusterControlRootMigrationRoundV1 *round)
{
	/* RF-ROOT P7 G3 (R4 cutover batch, Stage 8 contract + implementation /
	 * follow-up): the activate proof — the bit22 OPEN gate.  This process
	 * must be the round's coordinator; the ACK table must be COMPLETE (every
	 * member observed == expected) AND stand at (or beyond) the PREPARED
	 * stage (W6 clause 3: only the PREPARED-stage all-member ACK is the
	 * CLOSED binding that opens bit22); the round must carry bit22 in its
	 * target bitmap.  Fail-closed on any mismatch.  The runtime census gate
	 * was REMOVED here (implementation): the pre-bit22 census requirement forced the
	 * inverted root-only cutover order; the census now binds INSIDE the
	 * cutover round at the latch apply (cluster_r4_bit22_cutover_latch_apply
	 * refuses while KNOWN-DEFERRED sites remain).  The expected token/sha
	 * freshness is established by the caller against the canonical PREPARED
	 * image. */
	if (expected_token == NULL || expected_round_sha256 == NULL || round == NULL
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	if ((int32)round->coordinator_node_id != cluster_node_id)
		return false;
	if (!cluster_semantic_activation_ack_complete_matches(
			round->transition_epoch, round->prepare_generation, round->admitted_bitmap_low,
			round->admitted_bitmap_high, round->source_feature_bitmap, round->target_feature_bitmap,
			round->capability_sample_digest, CLUSTER_SEMANTIC_ACTIVATION_ACK_STAGE_PREPARED))
		return false;
	if (!cluster_control_root_feature_bitmap_is_known(round->source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(round->target_feature_bitmap)
		|| (round->target_feature_bitmap & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1)
			   == 0)
		return false;
	return true;
}

static bool
cluster_control_root_publish_authority_bind_v1(const ClusterControlRootReadToken *expected_token,
											   const ClusterControlRootPatch *patch,
											   ClusterControlRootPublishReason reason)
{
	if (root_publish_authority.active || expected_token == NULL || patch == NULL
		|| (reason != CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
			&& reason != CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
			&& reason != CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE
			&& reason != CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE
			/* RF-ROOT P7 G1a: the checkpointer's per-checkpoint canonical
			 * root advertisement (CHECKPOINT_ADVANCE, frozen 0x38 shape).
			 * Sole publisher = the checkpointer; no cross-publisher mixing
			 * with the three lifecycle reasons (verification F3
			 * re-check: the token/patch/reason triple stays exact). */
			&& reason != CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE
			/* RF-ROOT P7 G1a-2: the checkpointer's FPW-off sticky root
			 * publication (FPW_STICKY, frozen 0x40 shape).  Same sole
			 * publisher = the checkpointer. */
			&& reason != CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY))
		return false;
	memset(&root_publish_authority, 0, sizeof(root_publish_authority));
	root_publish_authority.active = true;
	root_publish_authority.reason = reason;
	root_publish_authority.token = *expected_token;
	root_publish_authority.patch = *patch;
	return true;
}

ClusterControlRootResult
cluster_control_root_recovery_complete_publish_v1(const ClusterControlRootReadToken *expected_token,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	ClusterControlRootResult result;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected_token == NULL || patch == NULL
		|| patch->mask
			   != (CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE
				   | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS)
		|| patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		|| !cluster_control_root_publish_authority_bind_v1(
			expected_token, patch, CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_compare_and_publish(
		expected_token, patch, CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE, out_snapshot,
		out_token);
	cluster_control_root_publish_authority_clear_v1();
	return result;
}

static void
cluster_control_root_publish_authority_clear_v1(void)
{
	explicit_bzero(&root_publish_authority, sizeof(root_publish_authority));
}

bool
cluster_control_root_publish_authority_current_v1(const ClusterControlRootReadToken *expected_token,
												  const ClusterControlRootPatch *patch,
												  ClusterControlRootPublishReason reason)
{
	bool exact
		= root_publish_authority.active && expected_token != NULL && patch != NULL
		  && reason == root_publish_authority.reason
		  && memcmp(expected_token, &root_publish_authority.token, sizeof(*expected_token)) == 0
		  && memcmp(patch, &root_publish_authority.patch, sizeof(*patch)) == 0;

	/* One exact compare-and-publish attempt consumes the authority before the
	 * control-root layer can acquire CF or touch storage.  A failed or nested
	 * retry must rebuild its upstream proof. */
	if (exact)
		cluster_control_root_publish_authority_clear_v1();
	return exact;
}

static void
write_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
}

static void
write_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
write_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

bool
cluster_recovery_duty_key_encode_v1(const ClusterRecoveryDutyKey *key,
									uint8 out[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES])
{
	if (out == NULL)
		return false;
	memset(out, 0, CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	if (!cluster_recovery_duty_key_valid_v1(key))
		return false;
	write_u64_le(out, key->system_identifier);
	memcpy(out + 8, key->storage_uuid, 16);
	memcpy(out + 24, key->authority_uuid, 16);
	write_u16_le(out + 40, key->origin_thread_id);
	write_u32_le(out + 42, (uint32)key->origin_node_id);
	write_u64_le(out + 46, (uint64)key->thread_claim_created_at);
	write_u32_le(out + 54, key->thread_claim_crc32c);
	write_u64_le(out + 58, key->origin_owner_incarnation);
	write_u64_le(out + 66, key->root_lineage_seq);
	return true;
}

ClusterRecoveryDutyCompare
cluster_recovery_duty_key_compare(const ClusterRecoveryDutyKey *expected,
								  const ClusterRecoveryDutyKey *observed)
{
	uint8 expected_bytes[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	uint8 observed_bytes[CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];

	if (!cluster_recovery_duty_key_encode_v1(expected, expected_bytes)
		|| !cluster_recovery_duty_key_encode_v1(observed, observed_bytes))
		return CLUSTER_RECOVERY_DUTY_COMPARE_INVALID;
	return memcmp(expected_bytes, observed_bytes, sizeof(expected_bytes)) == 0
			   ? CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
			   : CLUSTER_RECOVERY_DUTY_COMPARE_DIFFERENT;
}

bool
cluster_recovery_duty_digest_v1(const ClusterRecoveryDutyKey *key, ClusterRecoveryDutyDigest *out)
{
	static const uint8 domain[19] = "PGRAC-ROOT-DUTY-V1";
	uint8 preimage[19 + 4 + CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES];
	pg_cryptohash_ctx *ctx;
	bool success = false;

	if (out == NULL)
		return false;
	memset(out, 0, sizeof(*out));
	memcpy(preimage, domain, sizeof(domain));
	write_u32_le(preimage + sizeof(domain), CLUSTER_RECOVERY_DUTY_KEY_V1_BYTES);
	if (!cluster_recovery_duty_key_encode_v1(key, preimage + sizeof(domain) + sizeof(uint32)))
		return false;
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, preimage, sizeof(preimage)) >= 0
		&& pg_cryptohash_final(ctx, out->bytes, sizeof(out->bytes)) >= 0)
		success = true;
	pg_cryptohash_free(ctx);
	if (!success)
		memset(out, 0, sizeof(*out));
	return success;
}

static bool
owner_marker_absent(const ClusterJoinCommitMarker *marker)
{
	const uint8 *bytes = (const uint8 *)marker;
	size_t i;

	for (i = 0; i < sizeof(*marker); i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
owner_marker_committed_valid(const ClusterJoinCommitMarker *marker, int32 node_id)
{
	pg_crc32c crc;

	if (marker->magic != CLUSTER_JCMK_MAGIC || marker->version != CLUSTER_JCMK_VERSION
		|| marker->node_id != node_id || marker->phase != CLUSTER_JCMK_PHASE_COMMITTED
		|| marker->admitted_incarnation == 0)
		return false;
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, marker, offsetof(ClusterJoinCommitMarker, crc32c));
	FIN_CRC32C(crc);
	return (uint32)crc == marker->crc32c;
}

ClusterRecoveryOwnerImportResult
cluster_recovery_owner_import_select_v1(int32 node_id, const ClusterWalThreadClaim *immutable_claim,
										uint64 frozen_admitted_bitmap_low,
										uint64 frozen_admitted_bitmap_high,
										const ClusterRecoveryOwnerDiskSampleV1 *samples,
										int total_disk_count, uint64 *out_incarnation)
{
	ClusterJoinCommitMarker committed[CLUSTER_MAX_VOTING_DISKS];
	uint32 majority;
	int n_committed = 0;
	bool join_unreadable = false;
	bool join_present = false;
	bool initial_member;
	int i;

	if (out_incarnation != NULL)
		*out_incarnation = 0;
	if (out_incarnation == NULL || immutable_claim == NULL || samples == NULL || node_id < 0
		|| node_id >= CLUSTER_MAX_NODES || total_disk_count <= 0
		|| total_disk_count > CLUSTER_MAX_VOTING_DISKS)
		return CLUSTER_RECOVERY_OWNER_IMPORT_BAD_ARGUMENT;
	if (!cluster_wal_thread_claim_validate(immutable_claim, (uint16)(node_id + 1), node_id, NULL))
		return CLUSTER_RECOVERY_OWNER_IMPORT_CLAIM_MISMATCH;
	initial_member = node_id < 64
						 ? (frozen_admitted_bitmap_low & (UINT64_C(1) << node_id)) != 0
						 : (frozen_admitted_bitmap_high & (UINT64_C(1) << (node_id - 64))) != 0;
	majority = (uint32)(total_disk_count / 2) + 1;
	for (i = 0; i < total_disk_count; i++) {
		const ClusterJoinCommitMarker *marker = &samples[i].join_marker;

		if (samples[i].join_io_state != CLUSTER_VOTING_DISK_IO_OK) {
			join_unreadable = true;
			continue;
		}
		if (owner_marker_absent(marker))
			continue;
		join_present = true;
		if (owner_marker_committed_valid(marker, node_id))
			committed[n_committed++] = *marker;
	}
	if (join_present) {
		int a;

		for (a = 0; a < n_committed; a++) {
			uint32 same = 0;
			int b;

			for (b = 0; b < n_committed; b++)
				if (memcmp(&committed[a], &committed[b], sizeof(committed[a])) == 0)
					same++;
			if (same >= majority) {
				*out_incarnation = committed[a].admitted_incarnation;
				return CLUSTER_RECOVERY_OWNER_IMPORT_JCMK;
			}
		}
		return CLUSTER_RECOVERY_OWNER_IMPORT_JCMK_UNPROVEN;
	}
	if (join_unreadable)
		return CLUSTER_RECOVERY_OWNER_IMPORT_IO_FAILED;
	if (!initial_member)
		return CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN;
	for (i = 0; i < total_disk_count; i++) {
		uint32 same = 0;
		uint64 candidate;
		int j;

		if (samples[i].slot_io_state != CLUSTER_VOTING_DISK_IO_OK
			|| samples[i].slot.node_id != (uint32)node_id || samples[i].slot.incarnation == 0)
			continue;
		candidate = samples[i].slot.incarnation;
		for (j = 0; j < total_disk_count; j++)
			if (samples[j].slot_io_state == CLUSTER_VOTING_DISK_IO_OK
				&& samples[j].slot.node_id == (uint32)node_id
				&& samples[j].slot.incarnation == candidate)
				same++;
		if (same >= majority) {
			*out_incarnation = candidate;
			return CLUSTER_RECOVERY_OWNER_IMPORT_VOTING_SLOT;
		}
	}
	return CLUSTER_RECOVERY_OWNER_IMPORT_SLOT_UNPROVEN;
}

bool
cluster_recovery_owner_rejoin_v1(int32 node_id, uint64 admitted_incarnation)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;
	ClusterRecoveryOwnerImportResult owner_result;
	ClusterWalThreadClaim immutable_claim;
	uint64 proven_incarnation = 0;

	if (node_id < 0 || node_id >= CLUSTER_MAX_NODES || admitted_incarnation == 0)
		return false;
	root_result
		= cluster_control_root_lookup_owner_by_node_runtime(node_id, &identity, &snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| (snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			&& snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			/* RF-ROOT P6 (STOP-01 contract, corrected
			 * design):  a CLOSED root is the same-owner clean release
			 * (THREAD_CLEAN_CLOSE contract).  Its reopen is the frozen
			 * STOP-01 THREAD_OPEN mainline — CLOSED -> OPEN with the
			 * fresh boot incarnation and lineage+1, executed HERE (the
			 * commit-time re-vet, by the coordinator holding the full
			 * proof set:  write-once claim CRC + durable JCMK majority +
			 * monotonic newer incarnation) because the phase-3 postmaster
			 * driver has no PGPROC (S1 r=10) and the startup process is
			 * forked only after phase-3, whose barrier waits on this very
			 * commit — running the reopen later deadlocks. */
			&& snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
		/* STOP-01 contract: the coordinator-side
		 * missed-clean-close repair violated the publisher frozen contract
		 * — the OWNER (checkpointer) closes its own thread; the fix is the
		 * checkpointer's bounded THREAD_CLEAN_CLOSE retry
		 * (cluster_control_root_thread_clean_close_publish_retry).  OPEN
		 * under any owner other than admitted stays fail-closed here. */
		|| (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			&& identity.origin_owner_incarnation != admitted_incarnation)
		/* verification: split the non-OPEN reject by lifecycle.  The
		 * RECOVERY_COMPLETE branch keeps the frozen OWNER_REJOIN stale-owner
		 * reject (owner >= admitted).  The CLOSED branch must NOT reject on
		 * owner >= admitted: the frozen THREAD_OPEN mainline's monotonicity
		 * is enforced by the CAS itself (control_root.c compare_and_publish
		 * requires desired.owner > current.owner + lineage+1), so a
		 * fast-restart window whose observed slot still carries the prior
		 * incarnation (admitted == root owner) fails closed AT THE CAS and
		 * converges once the qvotec prior-incarnation slot ages out — the
		 * head gate only needs the exhausted-lineage reject. */
		|| (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			&& (identity.root_lineage_seq == UINT64_MAX
				|| identity.origin_owner_incarnation >= admitted_incarnation))
		|| (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			&& identity.root_lineage_seq == UINT64_MAX))
		return false;
	cluster_wal_thread_claim_fill(&immutable_claim, identity.origin_thread_id,
								  identity.origin_node_id, identity.thread_claim_created_at);
	if (immutable_claim.crc != identity.thread_claim_crc32c)
		return false;
	owner_result = cluster_recovery_owner_import_read_v1(node_id, &immutable_claim, 0, 0,
														 &proven_incarnation);
	if (owner_result != CLUSTER_RECOVERY_OWNER_IMPORT_JCMK
		|| proven_incarnation != admitted_incarnation)
		return false;
	/* A previous attempt may have completed the single ROOT CAS and then lost
	 * the local membership publish race.  Exact OPEN+owner plus the same direct
	 * majority JCMK is the already-satisfied gate; never advance lineage twice. */
	if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		&& identity.origin_owner_incarnation == admitted_incarnation)
		return true;

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE
				 | CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT | CLUSTER_CONTROL_ROOT_PATCH_TAIL
				 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	/* Frozen shapes (STOP-01 §17.4 + STOP-02): OWNER_REJOIN admits only the
	 * RECOVERY_COMPLETE pre-lifecycle (crash-rejoin mainline); a CLOSED root
	 * (clean release) reopens under the THREAD_OPEN reason with its frozen
	 * CLOSED -> OPEN shape.  Same owner-lineage monotonicity + proof set. */
	patch.expected_lifecycle = (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
								   ? CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
								   : CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = admitted_incarnation;
	patch.desired.identity.root_lineage_seq = identity.root_lineage_seq + 1;
	patch.desired.root_flags = snapshot.root_flags;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.tail_tli = snapshot.tail_tli;
	patch.desired.tail_validation_kind = snapshot.tail_validation_kind;
	patch.desired.validated_tail_lsn_exclusive = snapshot.validated_tail_lsn_exclusive;
	patch.desired.tail_last_record_lsn = snapshot.tail_last_record_lsn;
	patch.desired.tail_last_record_crc32c = snapshot.tail_last_record_crc32c;
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive = snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c = snapshot.recovered_last_record_crc32c;

	if (!cluster_control_root_publish_authority_bind_v1(
			&token, &patch,
			(snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
				? CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
				: CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN))
		return false;
	root_result = cluster_control_root_compare_and_publish(
		&token, &patch,
		(snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
			? CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
			: CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
		&published, &published_token);
	cluster_control_root_publish_authority_clear_v1();
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		&& published.identity.origin_owner_incarnation == admitted_incarnation
		&& published.identity.root_lineage_seq == identity.root_lineage_seq + 1) {
		if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
			ereport(LOG,
					(errmsg("cluster control root: thread %u clean-reopened by node %d "
							"(THREAD_OPEN, owner " UINT64_FORMAT ", lineage " UINT64_FORMAT ")",
							identity.origin_thread_id, node_id, admitted_incarnation,
							identity.root_lineage_seq + 1)));
		return true;
	}
	return false;
}

/*
 * cluster_control_root_thread_clean_close_publish -- RF-ROOT P6 (STOP-01
 * frozen THREAD_CLEAN_CLOSE, the Oracle clean-close mainline).
 *
 *	The OWNER of this node's thread closes its own redo thread after the
 *	shutdown checkpoint is durable:  OPEN -> CLOSED with the owner lineage
 *	UNCHANGED (the frozen 0x39 mask has no OWNER_LINEAGE bit).  Only the
 *	checkpointer's clean-shutdown path reaches this (after ShutdownXLOG
 *	and the STOPPED wal-state publish); crash / immediate-stop exits never
 *	write CLOSED, so a later failure stays on the survivor-driven
 *	failure-recovery FSM instead of the clean-reopen path.
 */
bool
cluster_control_root_thread_clean_close_publish(void)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(cluster_node_id, &identity,
																	&snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return false; /* not an open owner of this thread: nothing to close */

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT
				 | CLUSTER_CONTROL_ROOT_PATCH_TAIL | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.root_flags = snapshot.root_flags;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.tail_tli = snapshot.tail_tli;
	patch.desired.tail_validation_kind = snapshot.tail_validation_kind;
	patch.desired.validated_tail_lsn_exclusive = snapshot.validated_tail_lsn_exclusive;
	patch.desired.tail_last_record_lsn = snapshot.tail_last_record_lsn;
	patch.desired.tail_last_record_crc32c = snapshot.tail_last_record_crc32c;
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive = snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c = snapshot.recovered_last_record_crc32c;

	if (!cluster_control_root_publish_authority_bind_v1(
			&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE))
		return false;
	root_result = cluster_control_root_compare_and_publish(
		&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE, &published,
		&published_token);
	cluster_control_root_publish_authority_clear_v1();
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED) {
		ereport(LOG, (errmsg("cluster control root: thread %u clean-closed by owner node %d "
							 "(owner incarnation " UINT64_FORMAT ")",
							 identity.origin_thread_id, cluster_node_id,
							 identity.origin_owner_incarnation)));
		return true;
	}
	return false;
}

/*
 * cluster_control_root_thread_open_publish -- RF-ROOT P6 (STOP-01 frozen
 * THREAD_OPEN, the Oracle clean-reopen mainline).
 *
 *	A normally-restarted owner reopens its clean-closed redo thread with a
 *	fresh boot incarnation:  CLOSED -> OPEN with owner = boot_incarnation
 *	and lineage+1 (the frozen 0x3b mask carries OWNER_LINEAGE).  The
 *	expected-lifecycle CAS fails closed when the root is NOT CLOSED (first
 *	formation, rejoin-OPEN, or a crash path), so those flows are untouched.
 *	The admission / serving gates downstream never consult this publish
 *	directly;  the survivor's join chain re-validates the exact OPEN owner
 *	plus the majority COMMITTED JCMK before committing.
 */
bool
cluster_control_root_thread_open_publish(uint64 boot_incarnation)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES || boot_incarnation == 0)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(cluster_node_id, &identity,
																	&snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
		|| identity.root_lineage_seq == UINT64_MAX
		|| boot_incarnation <= identity.origin_owner_incarnation)
		return false; /* not a clean-closed thread of this owner, or stale */

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE | CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE
				 | CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT | CLUSTER_CONTROL_ROOT_PATCH_TAIL
				 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = boot_incarnation;
	patch.desired.identity.root_lineage_seq = identity.root_lineage_seq + 1;
	patch.desired.root_flags = snapshot.root_flags;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.tail_tli = snapshot.tail_tli;
	patch.desired.tail_validation_kind = snapshot.tail_validation_kind;
	patch.desired.validated_tail_lsn_exclusive = snapshot.validated_tail_lsn_exclusive;
	patch.desired.tail_last_record_lsn = snapshot.tail_last_record_lsn;
	patch.desired.tail_last_record_crc32c = snapshot.tail_last_record_crc32c;
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive = snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c = snapshot.recovered_last_record_crc32c;

	if (!cluster_control_root_publish_authority_bind_v1(&token, &patch,
														CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN))
		return false;
	root_result = cluster_control_root_compare_and_publish(
		&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN, &published, &published_token);
	cluster_control_root_publish_authority_clear_v1();
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		&& published.identity.origin_owner_incarnation == boot_incarnation
		&& published.identity.root_lineage_seq == identity.root_lineage_seq + 1) {
		ereport(LOG,
				(errmsg("cluster control root: thread %u reopened by owner node %d "
						"(owner incarnation %llu -> " UINT64_FORMAT ", lineage " UINT64_FORMAT ")",
						identity.origin_thread_id, cluster_node_id,
						(unsigned long long)identity.origin_owner_incarnation, boot_incarnation,
						identity.root_lineage_seq + 1)));
		return true;
	}
	return false;
}

/*
 * cluster_control_root_checkpoint_advance_publish -- RF-ROOT P7 G1a: the
 * checkpointer advertises its durable checkpoint in the canonical control
 * root (STOP-01 §17.2 reason CHECKPOINT_ADVANCE, frozen 0x38 shape =
 * CHECKPOINT | TAIL | RECOVERY_PROGRESS — owner lineage untouched).
 *
 *	The root's per-thread checkpoint_lower_lsn becomes the canonical
 *	merged-recovery start / retention bound for the correctness readers
 *	(G1b migration target); the wal-state registry keeps only telemetry.
 *	Called by CreateCheckPoint AFTER the clusterwide CF(X) is released
 *	(the frozen lock order forbids CF -> WALR and a held CF(X) deadlocks
 *	the STRONG root read's own CF(S)) and BEFORE the guarded WAL recycle
 *	(the advertised checkpoint must precede any removal of WAL it needs).
 *	Non-fatal: on any failure the checkpoint still completes and the next
 *	checkpoint retries (mirrors ClusterWalStatePublishCheckpointRedo).
 *
 *	record_crc32c = the exact CRC32C of the just-written checkpoint WAL
 *	record (read back by the caller after XLogFlush).
 */
bool
cluster_control_root_checkpoint_advance_publish(XLogRecPtr redo, TimeLineID tli,
												XLogRecPtr ckpt_record_start,
												XLogRecPtr ckpt_record_end, uint32 record_crc32c)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES || XLogRecPtrIsInvalid(redo)
		|| tli == 0 || record_crc32c == 0 || XLogRecPtrIsInvalid(ckpt_record_start)
		|| XLogRecPtrIsInvalid(ckpt_record_end) || ckpt_record_start >= ckpt_record_end)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(cluster_node_id, &identity,
																	&snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| (snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID) == 0
		|| snapshot.checkpoint_lower_lsn == 0)
		return false; /* not an OPEN owner thread with a valid checkpoint */

	/*
	 * Only advance: a checkpoint redo at or below the root's current bound
	 * (e.g. an end-of-recovery re-checkpoint) is a no-op — never move the
	 * advertised bound backwards.
	 */
	if ((uint64)redo <= snapshot.checkpoint_lower_lsn)
		return false;

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT | CLUSTER_CONTROL_ROOT_PATCH_TAIL
				 | CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	/* No LIFECYCLE bit in the 0x38 mask: the shape rule requires the
	 * unmasked desired.lifecycle to stay at the zero value (UNUSED). */
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED;
	patch.desired.root_flags = snapshot.root_flags | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
							   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	patch.desired.checkpoint_tli = tli;
	patch.desired.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	patch.desired.checkpoint_lower_lsn = (uint64)redo;
	patch.desired.checkpoint_record_crc32c = record_crc32c;
	/*
	 * The just-written checkpoint record is the WAL-extent validation point:
	 * the root's validated tail advances to the checkpoint record's end, and
	 * the tail's LAST record = the checkpoint record itself (start + CRC).
	 * This keeps the RANGE invariant validated_tail >= checkpoint_lower_lsn
	 * exact while the canonical bound moves forward.
	 */
	patch.desired.tail_tli = tli;
	patch.desired.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	patch.desired.validated_tail_lsn_exclusive = (uint64)ckpt_record_end;
	patch.desired.tail_last_record_lsn = (uint64)ckpt_record_start;
	patch.desired.tail_last_record_crc32c = record_crc32c;
	/* recovery progress preserved from the durable snapshot. */
	patch.desired.recovered_tli = snapshot.recovered_tli;
	patch.desired.recovered_through_lsn_exclusive = snapshot.recovered_through_lsn_exclusive;
	patch.desired.recovered_last_record_lsn = snapshot.recovered_last_record_lsn;
	patch.desired.recovered_last_record_crc32c = snapshot.recovered_last_record_crc32c;

	if (!cluster_control_root_publish_authority_bind_v1(
			&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE))
		return false;
	root_result = cluster_control_root_compare_and_publish(
		&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE, &published,
		&published_token);
	cluster_control_root_publish_authority_clear_v1();
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& published.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		&& published.checkpoint_lower_lsn == (uint64)redo
		&& published.checkpoint_record_crc32c == record_crc32c
		&& published.validated_tail_lsn_exclusive == (uint64)ckpt_record_end) {
		ereport(LOG, (errmsg("cluster control root: thread %u checkpoint advanced by node %d "
							 "(redo %X/%X, tli %u, tail %X/%X, root publish seq " UINT64_FORMAT ")",
							 identity.origin_thread_id, cluster_node_id, LSN_FORMAT_ARGS(redo),
							 (unsigned)tli, LSN_FORMAT_ARGS(ckpt_record_end),
							 published.root_publish_seq)));
		return true;
	}
	ereport(WARNING,
			(errmsg("cluster control root: checkpoint advance publish failed for thread %u "
					"(result %d); the next checkpoint will retry",
					identity.origin_thread_id, (int)root_result)));
	return false;
}

/*
 * cluster_control_root_thread_clean_close_publish_retry -- RF-ROOT P7
 * recovery path (recovery contract, verification).
 *
 *	The OWNER (checkpointer, clean-shutdown mainline) publishes its own
 *	THREAD_CLEAN_CLOSE.  A transient S1 serving-stale refusal (the local
 *	serving authority went stale inside the shutdown window) is retried
 *	with a BOUNDED window: re-validate / re-bind the leaver serving
 *	authority, then retry, 50ms backoff — the shutdown never blocks beyond
 *	the fixed deadline.  On expiry the root stays OPEN and the restart
 *	takes the ordinary crash-rejoin chain (fail-closed, no fake
 *	clean-leave).  This is the frozen-publisher mainline: the checkpointer
 *	(owner) closes its own thread; no coordinator-side repair (the
 *	increment-21 two-CAS rewrite was removed per the adjudication).
 */
bool
cluster_control_root_thread_clean_close_publish_retry(void)
{
	TimestampTz deadline
		= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), CLUSTER_CLEAN_CLOSE_RETRY_MS);
	bool closed_ok = false;

	for (;;) {
		closed_ok = cluster_control_root_thread_clean_close_publish();
		if (closed_ok)
			break;
		/* Re-validate / re-bind the leaver serving authority (the LMON-tick
		 * rebind may not have landed inside this shutdown window) before
		 * the next attempt. */
		(void)cluster_authority_serving_rebind_leaver();
		if (GetCurrentTimestamp() >= deadline)
			break;
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 50,
						WAIT_EVENT_CHECKPOINTER_MAIN);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}
	if (!closed_ok)
		ereport(LOG, (errmsg("cluster control root: THREAD_CLEAN_CLOSE could not be published "
							 "within the bounded shutdown window; the root stays OPEN and the "
							 "restart takes the ordinary crash-rejoin chain (fail-closed)")));
	return closed_ok;
}

/*
 * cluster_control_root_fpw_sticky_publish -- RF-ROOT P7 G1a-2: the
 * checkpointer publishes the FPW-off sticky into the canonical control root
 * (STOP-01 §17.2 reason FPW_STICKY, frozen 0x40 shape) after the W5b
 * registry sticky succeeded (same checkpoint, CF(X) released, before the
 * recycle — mirroring the CHECKPOINT_ADVANCE placement).  The root's
 * FLAG_FPW_WAS_OFF is sticky: once set it is never cleared (the apply_patch
 * guard), so the merged-recovery 53RA3 gate (G1b target) can read the
 * canonical flag instead of the registry.  Non-fatal: any failure is
 * retried by the next checkpoint.
 */
bool
cluster_control_root_fpw_sticky_publish(void)
{
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	ClusterControlRootResult root_result;

	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return false;
	root_result = cluster_control_root_lookup_owner_by_node_runtime(cluster_node_id, &identity,
																	&snapshot, &token);
	if ((root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| !cluster_recovery_duty_key_valid_v1(&identity)
		|| cluster_recovery_duty_key_compare(&identity, &snapshot.identity)
			   != CLUSTER_RECOVERY_DUTY_COMPARE_EXACT
		|| snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return false; /* not an OPEN owner thread */
	if ((snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0)
		return true; /* already sticky — no-op */

	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	/* No LIFECYCLE bit in the 0x40 mask: desired.lifecycle stays 0. */
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_UNUSED;
	patch.desired.root_flags = snapshot.root_flags | CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;

	if (!cluster_control_root_publish_authority_bind_v1(&token, &patch,
														CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY))
		return false;
	root_result = cluster_control_root_compare_and_publish(
		&token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY, &published, &published_token);
	cluster_control_root_publish_authority_clear_v1();
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (published.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0) {
		ereport(LOG,
				(errmsg("cluster control root: thread %u FPW-off sticky published by node %d "
						"(root publish seq " UINT64_FORMAT ")",
						identity.origin_thread_id, cluster_node_id, published.root_publish_seq)));
		return true;
	}
	ereport(WARNING, (errmsg("cluster control root: FPW sticky publish failed for thread %u "
							 "(result %d); the next checkpoint will retry",
							 identity.origin_thread_id, (int)root_result)));
	return false;
}

static bool
formation_bitmap_has_node(const uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES], int32 node_id)
{
	return node_id >= 0 && node_id < CLUSTER_MAX_NODES
		   && (bitmap[node_id / 8] & (uint8)(1u << (node_id % 8))) != 0;
}

static bool
formation_bitmap_nonempty(const uint8 bitmap[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES])
{
	int i;

	for (i = 0; i < CLUSTER_RECONFIG_DEAD_BITMAP_BYTES; i++)
		if (bitmap[i] != 0)
			return true;
	return false;
}

static void
formation_expected_marker(const ClusterFormationSnapshotV1 *snapshot, ClusterFenceMarker *expected)
{
	memset(expected, 0, sizeof(*expected));
	expected->magic = CLUSTER_FENCE_MARKER_MAGIC;
	expected->version = CLUSTER_FENCE_MARKER_VERSION;
	expected->fence_epoch = snapshot->applied.new_epoch;
	expected->fence_event_id = snapshot->applied.event_id;
	expected->fence_generation = snapshot->applied.cssd_dead_generation;
	expected->issuer_node_id = snapshot->applied.event_id == 0
								   ? CLUSTER_FENCE_BASELINE_INITIAL_ISSUER
								   : snapshot->applied.coordinator_node_id;
	memcpy(expected->fenced_dead_bitmap, snapshot->excluded_bitmap,
		   sizeof(expected->fenced_dead_bitmap));
	expected->marker_kind = snapshot->applied.event_id == 0 ? CLUSTER_FENCE_MARKER_KIND_BASELINE
															: CLUSTER_FENCE_MARKER_KIND_FENCE;
}

ClusterFormationWitnessResult
cluster_formation_witness_decide_v1(const ClusterFormationSnapshotV1 *f1,
									const ClusterFenceAuthorityProof *authority,
									const ClusterFormationSnapshotV1 *f2, uint16 origin_thread,
									bool opening_new_duty)
{
	ClusterFenceMarker expected;
	int32 origin_node;
	int survivor_count = 0;
	int i;

	if (f1 == NULL || authority == NULL || f2 == NULL || origin_thread == 0
		|| origin_thread > CLUSTER_MAX_NODES)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	if (memcmp(f1, f2, sizeof(*f1)) != 0)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->prebump_sync_active != 0 || !f2->self_join_admitted || f2->self_join_failed
		|| formation_bitmap_nonempty(f2->pending_join_bitmap)
		|| f2->applied.reconfig_kind == RECONFIG_KIND_JOIN_PENDING)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->local_epoch != f2->applied.new_epoch)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (authority->total_disk_count == 0
		|| authority->agree_disk_count <= authority->total_disk_count / 2
		|| !cluster_fence_marker_valid_v1(&authority->marker))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	formation_expected_marker(f2, &expected);
	if (!cluster_fence_marker_valid_v1(&expected)
		|| !cluster_fence_marker_tuple_equal(&authority->marker, &expected))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;

	origin_node = (int32)origin_thread - 1;
	if (!formation_bitmap_has_node(f2->excluded_bitmap, origin_node)
		|| (opening_new_duty && !formation_bitmap_has_node(f2->applied.dead_bitmap, origin_node)))
		return CLUSTER_FORMATION_WITNESS_ORIGIN_NOT_EXCLUDED;
	if (f2->victim_incarnation == 0
		|| f2->victim_incarnation != f2->membership.last_admitted_incarnation[origin_node]
		|| (f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_DEAD
			&& f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_REMOVED))
		return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		if (f2->membership.membership_state[i] == CLUSTER_MEMBER_MEMBER
			&& !formation_bitmap_has_node(f2->excluded_bitmap, i))
			survivor_count++;
	if (survivor_count == 0)
		return CLUSTER_FORMATION_WITNESS_FULL_OUTAGE_UNRECOVERED;
	return CLUSTER_FORMATION_WITNESS_READY;
}

/* STOP-05 E1 needs the same durable marker plus stable membership proof for
 * the checkpoint process's live redo thread.  It deliberately does not reuse
 * the failed-origin predicate above: a live recycler must be an admitted,
 * non-excluded MEMBER, while a recovery duty must be excluded DEAD/REMOVED. */
static ClusterFormationWitnessResult
formation_witness_decide_live_v1(const ClusterFormationSnapshotV1 *f1,
								 const ClusterFenceAuthorityProof *authority,
								 const ClusterFormationSnapshotV1 *f2, uint16 origin_thread)
{
	ClusterFenceMarker expected;
	int32 origin_node;
	int i;

	if (f1 == NULL || authority == NULL || f2 == NULL || origin_thread == 0
		|| origin_thread > CLUSTER_MAX_NODES)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	if (memcmp(f1, f2, sizeof(*f1)) != 0)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->prebump_sync_active != 0 || !f2->self_join_admitted || f2->self_join_failed
		|| formation_bitmap_nonempty(f2->pending_join_bitmap)
		|| f2->applied.reconfig_kind == RECONFIG_KIND_JOIN_PENDING) {
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	}
	/*
	 * RF-ROOT P6 (crash-rejoin): the settled-epoch gate.  A rejoiner NEVER
	 * advances its last_applied past the pre-crash event (AD-023 §9.2.3:
	 * the IC carries no ReconfigEvent and the JCMK has no event_id, so the
	 * JOIN_COMMITTED cannot be mirrored joiner-side).  Its formation is
	 * still settled once self-join admission has run: the durable
	 * quorum-majority COMMITTED marker + publish-proof adopted the epoch
	 * forward (local_epoch strictly above the stale applied epoch), and the
	 * expected marker logic below already treats an empty applied event as
	 * the BASELINE form.  Without this arm the witness can only turn READY
	 * when some UNRELATED later reconfig lands — phase 3 wedges for the
	 * whole phase3_timeout on every crash-rejoin.
	 */
	if (f2->local_epoch != f2->applied.new_epoch
		&& !(f2->self_join_admitted && f2->local_epoch > f2->applied.new_epoch))
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (authority->total_disk_count == 0
		|| authority->agree_disk_count <= authority->total_disk_count / 2
		|| !cluster_fence_marker_valid_v1(&authority->marker))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	formation_expected_marker(f2, &expected);
	/*
	 * RF-ROOT P6 (crash-rejoin): the expected/authority marker tuple gate is
	 * UNVERIFIABLE on a rejoiner.  The joiner's applied event is empty
	 * (AD-023 §9.2.3), so the expected marker is the initial BASELINE
	 * {epoch 0, event_id 0}, while the durable majority marker is the
	 * join-commit BASELINE {admitted epoch, JOIN_COMMITTED event_id} —
	 * an event_id the joiner can never learn (the IC carries no
	 * ReconfigEvent and the JCMK has no event_id).  The tuple inequality is
	 * therefore permanent, and the witness can only turn READY when an
	 * UNRELATED later reconfig lands (observed: the exact tick of the next
	 * fail-stop).  The durable proof this gate re-verifies is ALREADY held
	 * when self_join_admitted is set: the qvotec admission path proved a
	 * quorum-majority COMMITTED join marker plus the publish-proof against
	 * the durable fence chain before setting it (the same proof the
	 * write-fence supersede_by_admit relies on).  The majority + marker
	 * VALIDITY gate just above still runs, so a corrupt / minority durable
	 * state fails closed; only the identity comparison is waived under the
	 * admission proof, and only while the adopted epoch is strictly newer
	 * than the stale applied epoch.
	 */
	if ((!f2->self_join_admitted || f2->local_epoch <= f2->applied.new_epoch)
		&& (!cluster_fence_marker_valid_v1(&expected)
			|| !cluster_fence_marker_tuple_equal(&authority->marker, &expected))) {
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	}

	origin_node = (int32)origin_thread - 1;
	if (f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_MEMBER
		|| f2->membership.last_admitted_incarnation[origin_node] == 0
		|| formation_bitmap_has_node(f2->excluded_bitmap, origin_node)) {
		return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;
	}
	/*
	 * RF-ROOT P9 verification / cold-formation ruling (2026-08-19): phase-3
	 * readiness must ALSO wait until every founding member — the local
	 * admitted bitmap: every non-excluded MEMBER of this formation — carries
	 * a NON-ZERO exact floor, before the D13/BARRIER cutover may start.  With
	 * the B′ ABSENT-branch floor publish (cluster_reconfig.c) this holds as
	 * soon as the founding formation is admitted; while LMON is still
	 * publishing floors the witness stays OWNER_MISMATCH, the same transient
	 * the phase-3 deadline loop already retries.
	 */
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (f2->membership.membership_state[i] != CLUSTER_MEMBER_MEMBER
			|| formation_bitmap_has_node(f2->excluded_bitmap, i))
			continue;
		if (f2->membership.last_admitted_incarnation[i] == 0)
			return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;
	}
	return CLUSTER_FORMATION_WITNESS_READY;
}

/* AD-023 recovery-control/serving split.  The cold initial formation is
 * authority for recovery coordination before StartupXLOG, but it is not an
 * ordinary write/serving admission.  This is deliberately narrower than the
 * live witness: only the exact epoch-0 baseline with no reconfiguration debt
 * may omit self_join_admitted. */
static ClusterFormationWitnessResult
formation_witness_decide_recovery_control_v1(const ClusterFormationSnapshotV1 *f1,
											 const ClusterFenceAuthorityProof *authority,
											 const ClusterFormationSnapshotV1 *f2,
											 uint16 origin_thread)
{
	ClusterFenceMarker expected;
	int32 origin_node;
	int i;

	if (f1 == NULL || authority == NULL || f2 == NULL || origin_thread == 0
		|| origin_thread > CLUSTER_MAX_NODES)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	if (f2->self_join_admitted)
		return formation_witness_decide_live_v1(f1, authority, f2, origin_thread);
	if (memcmp(f1, f2, sizeof(*f1)) != 0)
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (f2->prebump_sync_active != 0 || f2->self_join_failed
		|| formation_bitmap_nonempty(f2->pending_join_bitmap)
		|| formation_bitmap_nonempty(f2->clean_departed_bitmap)
		|| formation_bitmap_nonempty(f2->removed_bitmap)
		|| formation_bitmap_nonempty(f2->excluded_bitmap)
		|| f2->local_epoch != CLUSTER_EPOCH_INITIAL || f2->applied.event_id != 0
		|| f2->applied.old_epoch != CLUSTER_EPOCH_INITIAL
		|| f2->applied.new_epoch != CLUSTER_EPOCH_INITIAL || f2->applied.event_seq != 0
		|| f2->applied.cssd_dead_generation != 0 || f2->applied.reconfig_kind != RECONFIG_KIND_NONE
		|| formation_bitmap_nonempty(f2->applied.dead_bitmap)
		|| formation_bitmap_nonempty(f2->applied.join_bitmap))
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	if (authority->total_disk_count == 0
		|| authority->agree_disk_count <= authority->total_disk_count / 2
		|| !cluster_fence_marker_valid_v1(&authority->marker))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	formation_expected_marker(f2, &expected);
	if (!cluster_fence_marker_valid_v1(&expected)
		|| !cluster_fence_marker_tuple_equal(&authority->marker, &expected))
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;

	origin_node = (int32)origin_thread - 1;
	if (f2->membership.membership_state[origin_node] != CLUSTER_MEMBER_MEMBER
		|| f2->membership.last_admitted_incarnation[origin_node] == 0)
		return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (f2->membership.membership_state[i] == CLUSTER_MEMBER_MEMBER
			&& f2->membership.last_admitted_incarnation[i] == 0)
			return CLUSTER_FORMATION_WITNESS_OWNER_MISMATCH;
	}
	return CLUSTER_FORMATION_WITNESS_READY;
}

#define CLUSTER_FORMATION_WITNESS_MAGIC UINT32_C(0x46575631) /* FWV1 */

struct ClusterFormationWitnessV1 {
	uint32 magic;
	uint16 origin_thread;
	bool opening_new_duty;
	uint8 reserved;
	ClusterFenceAuthorityProof authority;
	ClusterFormationSnapshotV1 f1;
	ClusterFormationSnapshotV1 f2;
};

typedef enum ClusterFormationWitnessMode {
	CLUSTER_FORMATION_WITNESS_MODE_DUTY = 0,
	CLUSTER_FORMATION_WITNESS_MODE_LIVE = 1,
	CLUSTER_FORMATION_WITNESS_MODE_RECOVERY_CONTROL = 2
} ClusterFormationWitnessMode;

static uint64
formation_monotonic_us(void)
{
	instr_time now;
	int64 ns;

	INSTR_TIME_SET_CURRENT(now);
	ns = INSTR_TIME_GET_NANOSEC(now);
	return ns <= 0 ? 0 : (uint64)ns / UINT64_C(1000);
}

static ClusterFormationWitnessResult
formation_authority_result(ClusterFenceAuthorityReadResult result)
{
	switch (result) {
	case CLUSTER_FENCE_AUTHORITY_OK:
		return CLUSTER_FORMATION_WITNESS_READY;
	case CLUSTER_FENCE_AUTHORITY_NO_MAJORITY:
		return CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN;
	case CLUSTER_FENCE_AUTHORITY_IO_UNAVAILABLE:
		return CLUSTER_FORMATION_WITNESS_IO_FAILED;
	case CLUSTER_FENCE_AUTHORITY_ENFORCEMENT_OFF:
	case CLUSTER_FENCE_AUTHORITY_NO_CONFIG:
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	case CLUSTER_FENCE_AUTHORITY_BAD_ARGUMENT:
	case CLUSTER_FENCE_AUTHORITY_BAD_CONFIG:
	case CLUSTER_FENCE_AUTHORITY_MIXED_VERSION:
	case CLUSTER_FENCE_AUTHORITY_CORRUPT:
	default:
		return CLUSTER_FORMATION_WITNESS_CORRUPT;
	}
}

static ClusterFormationWitnessResult
formation_witness_build_wait_internal(uint16 origin_thread, bool opening_new_duty,
									  ClusterFormationWitnessMode mode, int timeout_ms,
									  ClusterFormationWitnessV1 **out)
{
	ClusterFormationWitnessResult last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
	uint64 start_us;
	uint64 deadline_us;

	if (out == NULL || *out != NULL || origin_thread == 0 || origin_thread > CLUSTER_MAX_NODES
		|| timeout_ms < 1 || timeout_ms > 600000)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	start_us = formation_monotonic_us();
	if (start_us == 0 || start_us > UINT64_MAX - (uint64)timeout_ms * UINT64_C(1000))
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	deadline_us = start_us + (uint64)timeout_ms * UINT64_C(1000);
	for (;;) {
		ClusterFormationSnapshotV1 f1;
		ClusterFormationSnapshotV1 f2;
		ClusterFenceAuthorityProof authority;
		ClusterFenceAuthorityReadResult read_result;
		uint64 proof_sequence;
		uint64 now_us;

		proof_sequence = cluster_write_fence_authority_cache_sequence();
		if ((proof_sequence & UINT64_C(1)) != 0 || proof_sequence >= UINT64_MAX - 1) {
			last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
			goto retry;
		}
		if (!cluster_reconfig_capture_formation_snapshot_v1(origin_thread, &f1))
			return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
		read_result = cluster_write_fence_read_durable_authority(&authority);
		last = formation_authority_result(read_result);
		if (last == CLUSTER_FORMATION_WITNESS_READY) {
			if (!cluster_reconfig_capture_formation_snapshot_v1(origin_thread, &f2))
				return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
			if (mode == CLUSTER_FORMATION_WITNESS_MODE_LIVE)
				last = formation_witness_decide_live_v1(&f1, &authority, &f2, origin_thread);
			else if (mode == CLUSTER_FORMATION_WITNESS_MODE_RECOVERY_CONTROL)
				last = formation_witness_decide_recovery_control_v1(&f1, &authority, &f2,
																	origin_thread);
			else
				last = cluster_formation_witness_decide_v1(&f1, &authority, &f2, origin_thread,
														   opening_new_duty);
			if (last == CLUSTER_FORMATION_WITNESS_READY) {
				ClusterFormationWitnessV1 *witness;

				now_us = formation_monotonic_us();
				if (now_us == 0)
					return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
				if (!cluster_write_fence_authority_cache_publish_if_unchanged(
						&authority.marker, now_us, proof_sequence)) {
					last = CLUSTER_FORMATION_WITNESS_UNSTABLE;
					goto retry;
				}
				witness = (ClusterFormationWitnessV1 *)palloc(sizeof(*witness));
				memset(witness, 0, sizeof(*witness));
				witness->magic = CLUSTER_FORMATION_WITNESS_MAGIC;
				witness->origin_thread = origin_thread;
				witness->opening_new_duty = opening_new_duty;
				witness->reserved = (uint8)mode;
				witness->authority = authority;
				witness->f1 = f1;
				witness->f2 = f2;
				if (mode == CLUSTER_FORMATION_WITNESS_MODE_RECOVERY_CONTROL) {
					witness->f1.reserved[0] = CLUSTER_FORMATION_SNAPSHOT_RECOVERY_CONTROL;
					witness->f2.reserved[0] = CLUSTER_FORMATION_SNAPSHOT_RECOVERY_CONTROL;
				}
				*out = witness;
				return CLUSTER_FORMATION_WITNESS_READY;
			}
		}
		if (last != CLUSTER_FORMATION_WITNESS_UNSTABLE
			&& last != CLUSTER_FORMATION_WITNESS_MARKER_UNPROVEN
			&& last != CLUSTER_FORMATION_WITNESS_IO_FAILED)
			return last;
	retry:
		now_us = formation_monotonic_us();
		if (now_us == 0 || now_us >= deadline_us)
			return last;
		pg_usleep(1000L);
	}
}

ClusterFormationWitnessResult
cluster_formation_witness_build_wait(uint16 origin_thread, bool opening_new_duty, int timeout_ms,
									 ClusterFormationWitnessV1 **out)
{
	return formation_witness_build_wait_internal(
		origin_thread, opening_new_duty, CLUSTER_FORMATION_WITNESS_MODE_DUTY, timeout_ms, out);
}

ClusterFormationWitnessResult
cluster_formation_witness_build_live_wait(uint16 origin_thread, int timeout_ms,
										  ClusterFormationWitnessV1 **out)
{
	return formation_witness_build_wait_internal(
		origin_thread, false, CLUSTER_FORMATION_WITNESS_MODE_LIVE, timeout_ms, out);
}

ClusterFormationWitnessResult
cluster_formation_witness_build_recovery_control_wait(uint16 origin_thread, int timeout_ms,
													  ClusterFormationWitnessV1 **out)
{
	return formation_witness_build_wait_internal(
		origin_thread, false, CLUSTER_FORMATION_WITNESS_MODE_RECOVERY_CONTROL, timeout_ms, out);
}

ClusterFormationWitnessResult
cluster_formation_witness_revalidate_nowait(const ClusterFormationWitnessV1 *witness)
{
	ClusterFenceAuthorityCacheResult result;
	uint64 now_us;

	if (witness == NULL || witness->magic != CLUSTER_FORMATION_WITNESS_MAGIC)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	now_us = formation_monotonic_us();
	if (now_us == 0)
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	result = cluster_write_fence_revalidate_cached_nowait(&witness->authority.marker, now_us);
	switch (result) {
	case CLUSTER_FENCE_CACHE_MATCH:
		return CLUSTER_FORMATION_WITNESS_READY;
	case CLUSTER_FENCE_CACHE_STALE:
	case CLUSTER_FENCE_CACHE_EXPIRED:
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;
	case CLUSTER_FENCE_CACHE_INVALID:
	case CLUSTER_FENCE_CACHE_UNAVAILABLE:
	default:
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	}
}

/* Compare an immutable classification with a freshly captured runtime
 * snapshot.  Normal classifications remain byte-exact.  A recovery-control
 * classification additionally tolerates only its one monotone local edge:
 * self_join_admitted 0 -> 1 after StartupXLOG completes the stripe gate. */
bool
cluster_formation_snapshot_matches_v1(const ClusterFormationSnapshotV1 *expected,
									  const ClusterFormationSnapshotV1 *observed)
{
	ClusterFormationSnapshotV1 normalized;

	if (expected == NULL || observed == NULL)
		return false;
	if (expected->reserved[0] == 0 && expected->reserved[1] == 0)
		return memcmp(expected, observed, sizeof(*expected)) == 0;
	if (expected->reserved[0] != CLUSTER_FORMATION_SNAPSHOT_RECOVERY_CONTROL
		|| expected->reserved[1] != 0 || observed->reserved[0] != 0 || observed->reserved[1] != 0)
		return false;

	normalized = *observed;
	normalized.reserved[0] = CLUSTER_FORMATION_SNAPSHOT_RECOVERY_CONTROL;
	if (expected->self_join_admitted == 0 && normalized.self_join_admitted == 1)
		normalized.self_join_admitted = 0;
	return memcmp(expected, &normalized, sizeof(*expected)) == 0;
}

/* Revalidate a copied live-formation classification against both present
 * membership bytes and the cached durable fence marker.  This is the
 * generation-bound counterpart of copy_classification_v1(): a copied proof
 * never becomes a timeless authority token. */
ClusterFormationWitnessResult
cluster_formation_classification_revalidate_nowait(uint16 origin_thread,
												   const ClusterFenceAuthorityProof *authority,
												   const ClusterFormationSnapshotV1 *snapshot)
{
	ClusterFormationSnapshotV1 current;
	ClusterFenceAuthorityCacheResult cache_result;
	uint64 now_us;

	if (origin_thread == 0 || origin_thread > CLUSTER_MAX_NODES || authority == NULL
		|| snapshot == NULL)
		return CLUSTER_FORMATION_WITNESS_BAD_ARGUMENT;
	if (!cluster_reconfig_capture_formation_snapshot_v1(origin_thread, &current))
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	if (!cluster_formation_snapshot_matches_v1(snapshot, &current))
		return CLUSTER_FORMATION_WITNESS_UNSTABLE;

	now_us = formation_monotonic_us();
	if (now_us == 0)
		return CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
	cache_result = cluster_write_fence_revalidate_cached_nowait(&authority->marker, now_us);
	return cache_result == CLUSTER_FENCE_CACHE_MATCH ? CLUSTER_FORMATION_WITNESS_READY
		   : (cache_result == CLUSTER_FENCE_CACHE_STALE
			  || cache_result == CLUSTER_FENCE_CACHE_EXPIRED)
			   ? CLUSTER_FORMATION_WITNESS_UNSTABLE
			   : CLUSTER_FORMATION_WITNESS_CAPABILITY_UNAVAILABLE;
}

bool
cluster_formation_witness_copy_classification_v1(const ClusterFormationWitnessV1 *witness,
												 uint16 *origin_thread,
												 ClusterFenceAuthorityProof *authority,
												 ClusterFormationSnapshotV1 *snapshot)
{
	if (witness == NULL || witness->magic != CLUSTER_FORMATION_WITNESS_MAGIC
		|| origin_thread == NULL || authority == NULL || snapshot == NULL)
		return false;
	*origin_thread = witness->origin_thread;
	*authority = witness->authority;
	*snapshot = witness->f2;
	return true;
}

const ClusterFenceAuthorityProof *
cluster_formation_witness_authority(const ClusterFormationWitnessV1 *witness)
{
	if (witness == NULL || witness->magic != CLUSTER_FORMATION_WITNESS_MAGIC)
		return NULL;
	return &witness->authority;
}

void
cluster_formation_witness_destroy(ClusterFormationWitnessV1 **witness)
{
	if (witness == NULL || *witness == NULL)
		return;
	(*witness)->magic = 0;
	pfree(*witness);
	*witness = NULL;
}
