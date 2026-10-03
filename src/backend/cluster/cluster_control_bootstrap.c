/*-------------------------------------------------------------------------
 * PGRAC: exact object composition for a provisional bootstrap observation.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_wal_claim.h"
#include "common/cryptohash.h"
#include "cluster_control_bootstrap_private.h"
#include "cluster_control_root_private.h"
#include "cluster_recovery_anchor_private.h"

static bool
bootstrap_overlap(const void *left, size_t size, const void *right, size_t right_size)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;

	if (left == NULL || right == NULL || size == 0 || right_size == 0)
		return false;
	return a <= b ? b - a < size : a - b < right_size;
}

static bool
bootstrap_output_alias(const ClusterControlBootstrapInput *input,
					   const ClusterControlBootstrapSnapshot *out)
{
	if (bootstrap_overlap(input, sizeof(*input), out, sizeof(*out)))
		return true;
	if (input != NULL) {
		const ClusterControlBootstrapBytes *views[]
			= { &input->binding, &input->root_before, &input->root_after, &input->common,
				&input->config,	 &input->claim,		  &input->anchor };

		for (size_t i = 0; i < lengthof(views); ++i)
			if (bootstrap_overlap(views[i]->data, views[i]->len, out, sizeof(*out)))
				return true;
	}
	return false;
}

static bool
bootstrap_input_valid(const ClusterControlBootstrapInput *input)
{
	const ClusterControlBootstrapBytes *views[]
		= { &input->binding, &input->root_before, &input->root_after, &input->common,
			&input->config,	 &input->claim,		  &input->anchor };

	for (size_t i = 0; i < lengthof(views); ++i)
		if (views[i]->data == NULL)
			return false;
	return input->node_id < PGRAC_CONTROL_BINDING_MAX_NODES
		   && input->binding.len == PGRAC_CONTROL_BINDING_BYTES
		   && input->root_before.len == CLUSTER_CONTROL_ROOT_FILE_BYTES
		   && input->root_after.len == CLUSTER_CONTROL_ROOT_FILE_BYTES
		   && input->common.len == PG_CONTROL_FILE_SIZE && input->config.len > 0
		   && input->config.len <= CLUSTER_SHARED_CONFIG_MAX_BYTES
		   && input->claim.len == CLUSTER_WAL_CLAIM_V2_BYTES
		   && input->anchor.len == CLUSTER_RECOVERY_ANCHOR_SIZE;
}

static bool
bootstrap_hash(const uint8 *bytes, size_t len, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;

	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
		 && pg_cryptohash_final(ctx, hash, 32) >= 0;
	pg_cryptohash_free(ctx);
	return ok;
}

ClusterControlRootResult
cluster_control_bootstrap_root_decode(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
									  uint64 system_identifier, ControlRootImage *out)
{
	/* Only dispatch on the little-endian version tag; do not retry a failed
	 * decoder under a different version or reinterpret any reserved byte. */
	if (bytes != NULL && len >= 6 && bytes[4] == 3 && bytes[5] == 0)
		return cluster_control_root_v3_decode(bytes, len, storage_uuid, system_identifier, out);
	return cluster_control_root_v2_decode(bytes, len, storage_uuid, system_identifier, out);
}

ClusterControlRootResult
cluster_control_bootstrap_root_bound(const PgracControlBinding *binding,
									 const ControlRootImage *root)
{
	uint32 node;
	const ControlRootHeader *header;

	if (binding == NULL || root == NULL || binding->node_id >= PGRAC_CONTROL_BINDING_MAX_NODES)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	node = binding->node_id;
	header = &root->header;

	if (header->lineage_kind != binding->lineage_kind
		|| header->system_identifier != binding->system_identifier
		|| memcmp(header->storage_uuid, binding->storage_uuid, 16) != 0
		|| memcmp(header->authority_uuid, binding->authority_uuid, 16) != 0
		|| header->v2.database_incarnation != binding->database_incarnation
		|| memcmp(header->migration_round_sha256, binding->migration_round_sha256, 32) != 0
		|| memcmp(header->source_wal_state_sha256, binding->source_wal_state_sha256, 32) != 0
		|| header->migration_prepare_generation != binding->migration_prepare_generation
		|| header->migration_transition_epoch != binding->migration_transition_epoch)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (binding->lineage_kind == PGRAC_CONTROL_LINEAGE_CREATION_V1) {
		uint64 required = PGRAC_CONTROL_ROOT_FEATURE_SPACE_IDENTITY_V1
			| PGRAC_CONTROL_ROOT_FEATURE_SPACE_RESERVATION_V1;
		if ((header->target_feature_bitmap & required) != required)
			return CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
	}
	if (header->v2.database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| header->v2.database_state > CLUSTER_CONTROL_ROOT_DATABASE_CLOSED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if ((header->v2.configured[node / 64] & (UINT64CONST(1) << (node % 64))) == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (!root->present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->records[node].lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	/* Other states describe a snapshot. None are serving/recovery permits. */
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
bootstrap_common(const ClusterControlBootstrapBytes *bytes, const ControlRootImage *root,
				 ControlFileData *out)
{
	uint8 hash[32], canonical[PG_CONTROL_FILE_SIZE];
	ClusterCfValidity validity;
	ClusterControlRootResult result;

	if (!bootstrap_hash(bytes->data, bytes->len, hash))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, root->header.v2.control_image_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	validity = cluster_cf_classify_buffer((const char *)bytes->data, bytes->len,
										  root->header.system_identifier);
	if (validity == CLUSTER_CF_INVALID_IDENTITY)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (validity != CLUSTER_CF_VALID)
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	memcpy(out, bytes->data, sizeof(*out));
	result = cluster_cf_control_image_encode(out, canonical);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& memcmp(bytes->data, canonical, sizeof(canonical)) != 0)
		result = CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	/* PGRAC: valid bytes are not proof of supported optional recovery state. */
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (out->track_commit_timestamp
			|| TransactionIdIsValid(out->checkPointCopy.oldestCommitTsXid)
			|| TransactionIdIsValid(out->checkPointCopy.newestCommitTsXid)))
		result = CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
	return result;
}

ClusterControlRootResult
cluster_control_bootstrap_decode(const ClusterControlBootstrapInput *input,
								 ClusterControlBootstrapSnapshot *out)
{
	ClusterControlBootstrapSnapshot snapshot;
	ControlRootImage *before, *after;
	ControlFileData common;
	ClusterWalThreadClaimRefV2 claim_ref;
	ClusterWalThreadClaimV2 claim;
	ClusterRecoveryAnchorRefV2 anchor_ref;
	ClusterControlRootResult result;
	uint32 count;
	bool alias = bootstrap_output_alias(input, out);

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || input == NULL || out == NULL || !bootstrap_input_valid(input))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(&snapshot, 0, sizeof(snapshot));
	if (!pgrac_control_binding_decode(input->binding.data, input->binding.len, &snapshot.binding))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (snapshot.binding.node_id != input->node_id)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;

	before = palloc(sizeof(*before));
	after = palloc(sizeof(*after));
	result = cluster_control_bootstrap_root_decode(input->root_before.data, input->root_before.len,
												   snapshot.binding.storage_uuid,
												   snapshot.binding.system_identifier, before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = cluster_control_bootstrap_root_bound(&snapshot.binding, before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = cluster_control_bootstrap_root_decode(input->root_after.data, input->root_after.len,
												   snapshot.binding.storage_uuid,
												   snapshot.binding.system_identifier, after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	result = cluster_control_bootstrap_root_bound(&snapshot.binding, after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (memcmp(before->bytes, after->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}

	result = bootstrap_common(&input->common, before, &common);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	snapshot.thread = before->records[input->node_id].identity;
	snapshot.config.identity.system_identifier = before->header.system_identifier;
	snapshot.config.identity.database_incarnation = before->header.v2.database_incarnation;
	snapshot.config.identity.generation = before->header.v2.config_generation;
	memcpy(snapshot.config.identity.storage_uuid, before->header.storage_uuid, 16);
	memcpy(snapshot.config.identity.authority_uuid, before->header.authority_uuid, 16);
	memcpy(snapshot.config.identity.configured, before->header.v2.configured, 16);
	memcpy(snapshot.config.sha256, before->header.v2.config_sha256, 32);
	result = cluster_shared_config_validate((const char *)input->config.data, input->config.len,
											&snapshot.config, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;

	memset(&claim_ref, 0, sizeof(claim_ref));
	claim_ref.identity = snapshot.thread;
	claim_ref.database_incarnation = before->header.v2.database_incarnation;
	claim_ref.max_config_generation = before->header.v2.config_generation;
	memcpy(claim_ref.claim_sha256, before->refs[input->node_id].claim_sha256, 32);
	result = cluster_wal_claim_v2_decode(input->claim.data, input->claim.len, &claim_ref, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	memset(&anchor_ref, 0, sizeof(anchor_ref));
	anchor_ref.identity = snapshot.thread;
	anchor_ref.database_incarnation = before->header.v2.database_incarnation;
	anchor_ref.max_config_generation = before->header.v2.config_generation;
	anchor_ref.anchor_generation = before->refs[input->node_id].anchor_generation;
	memcpy(anchor_ref.anchor_sha256, before->refs[input->node_id].anchor_sha256, 32);
	memcpy(anchor_ref.claim_sha256, before->refs[input->node_id].claim_sha256, 32);
	result = cluster_recovery_anchor_v2_project(input->anchor.data, input->anchor.len, &anchor_ref,
												&common, &snapshot.control);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (snapshot.control.track_commit_timestamp
		|| TransactionIdIsValid(snapshot.control.checkPointCopy.oldestCommitTsXid)
		|| TransactionIdIsValid(snapshot.control.checkPointCopy.newestCommitTsXid)) {
		result = CLUSTER_CONTROL_ROOT_PROFILE_UNSUPPORTED;
		goto done;
	}
	result = cluster_recovery_anchor_v2_thread_state(&anchor_ref, &before->records[input->node_id],
													 &snapshot.control);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	snapshot.wal.claim = claim_ref;
	snapshot.wal.timeline = snapshot.control.checkPointCopy.ThisTimeLineID;
	if (!bootstrap_hash(before->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES, snapshot.root_sha256)) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	snapshot.root_sequence = before->header.file_txn_seq;
	snapshot.database_state = before->header.v2.database_state;
	snapshot.activation_state = before->header.activation_state;
	*out = snapshot;
done:
	pfree(after);
	pfree(before);
	return result;
}
