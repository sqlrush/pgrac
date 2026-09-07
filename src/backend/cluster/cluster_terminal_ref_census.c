/*-------------------------------------------------------------------------
 *
 * cluster_terminal_ref_census.c
 *	  Bounded canonical terminal-reference census state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_terminal_ref_census.c
 *
 * NOTES
 *	  This is a pgrac-original file.  CTRC is reference-release evidence;
 *	  it never replaces the physical transaction-table status authority.
 *	  The tables are activation-sized and never shrink or evict at runtime.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "../../common/sha2_int.h"

#include "cluster/cluster_terminal_ref_census.h"
#include "cluster/cluster_tt_slot.h"
#include "cluster/storage/cluster_undo_alloc.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/spin.h"

#ifndef CLUSTER_CTRC_UNIT_TEST
#include "access/generic_xlog.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/xlog.h"
#include "catalog/pg_class_d.h"
#include "miscadmin.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_mode.h"
#include "cluster/cluster_mxid_stripe.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_runtime_visibility.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_undo_cleaner.h"
#include "cluster/storage/cluster_undo_block0.h"
#include "cluster/storage/cluster_undo_block0_current.h"
#include "cluster/storage/cluster_undo_xlog.h"
#include "cluster/cluster_undo_segment.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "storage/backendid.h"
#include "storage/bufpage.h"
#include "storage/procarray.h"
#include "storage/smgr.h"
#include "storage/shmem.h"
#include "portability/instr_time.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#endif

#define CLUSTER_CTRC_SHMEM_MAGIC UINT32_C(0x43545243)
#define CLUSTER_CTRC_SHMEM_VERSION UINT32_C(7)
const uint8 cluster_ctrc_empty_sha256[32] = {
	0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
	0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
	0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
	0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
};

bool
cluster_ctrc_sha256_exact(const void *bytes, Size length, uint8 digest[32])
{
	pg_sha256_ctx context;

	if (bytes == NULL || digest == NULL)
		return false;
	pg_sha256_init(&context);
	pg_sha256_update(&context, (const uint8 *)bytes, length);
	pg_sha256_final(&context, digest);
	return true;
}

static bool
ctrc_bytes_zero(const void *address, Size length)
{
	const uint8 *bytes = (const uint8 *)address;
	Size		i;

	if (address == NULL)
		return false;
	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static const char *const ctrc_stat_names[CTRC_STAT_COUNT] = {
	[CTRC_STAT_GRANT_ISSUED] = "grant_issued_count",
	[CTRC_STAT_GRANT_REFUSED] = "grant_refused_count",
	[CTRC_STAT_RECEIPT_PREPARED] = "receipt_prepared_count",
	[CTRC_STAT_RECEIPT_APPLIED] = "receipt_applied_count",
	[CTRC_STAT_RECEIPT_CANCELLED] = "receipt_cancelled_count",
	[CTRC_STAT_RECEIPT_CAPACITY_REFUSED] = "receipt_capacity_refused_count",
	[CTRC_STAT_SEAL_STARTED] = "seal_started_count",
	[CTRC_STAT_SEAL_BLOCKED] = "seal_blocked_count",
	[CTRC_STAT_TARGET_ABSENT] = "target_absent_count",
	[CTRC_STAT_TARGET_REWRITTEN] = "target_rewritten_count",
	[CTRC_STAT_TARGET_RETAINED] = "target_retained_count",
	[CTRC_STAT_ACK_FROZEN] = "ack_frozen_count",
	[CTRC_STAT_ACK_RESENT] = "ack_resent_count",
	[CTRC_STAT_CERTIFICATE_APPLIED] = "certificate_applied_count",
	[CTRC_STAT_CERTIFICATE_REPLAYED] = "certificate_replayed_count",
	[CTRC_STAT_L11_RELEASE_SAMPLE] = "l11_release_sample_count",
	[CTRC_STAT_L12_RECYCLE] = "l12_recycle_count",
	[CTRC_STAT_ORDINARY_PUBLICATION_AFTER_APPLY] = "ordinary_publication_after_apply_count",
	[CTRC_STAT_CURRENT_MX_PUBLICATION_AFTER_APPLY] = "current_mx_publication_after_apply_count",
	[CTRC_STAT_PUBLICATION_ORDER_VIOLATION] = "publication_order_violation_count",
	[CTRC_STAT_CLEANER_PASS] = "cleaner_pass_count",
	[CTRC_STAT_CLEANER_ATTEMPT] = "cleaner_attempt_count",
	[CTRC_STAT_SEMANTIC_PROGRESS] = "semantic_progress_count",
	[CTRC_STAT_PENDING_DUPLICATE] = "pending_duplicate_count",
	[CTRC_STAT_DISPATCH_BACKLOG] = "dispatch_backlog",
	[CTRC_STAT_CERTIFICATE_BACKLOG] = "certificate_backlog",
	[CTRC_STAT_PENDING_OBSERVED_AGE_MS] = "pending_observed_age_ms",
	[CTRC_STAT_OBSERVED_AT_US] = "observed_at_monotonic_us",
	[CTRC_STAT_OBSERVATION_AGE_MS] = "observation_age_ms",
};

static const char *const ctrc_cleaner_reason_names[
	CTRC_CLEANER_REASON_COUNT] = {
	[CTRC_CLEANER_REASON_NONE] = "NONE",
	[CTRC_CLEANER_REASON_PREPARED_DRAIN] = "PREPARED_DRAIN",
	[CTRC_CLEANER_REASON_RESOURCE_X] = "RESOURCE_X",
	[CTRC_CLEANER_REASON_PAGE_REVALIDATE] = "PAGE_REVALIDATE",
	[CTRC_CLEANER_REASON_WAL_DURABILITY] = "WAL_DURABILITY",
	[CTRC_CLEANER_REASON_PARTICIPANT_ACK] = "PARTICIPANT_ACK",
	[CTRC_CLEANER_REASON_BLOCK0_CERTIFICATE] = "BLOCK0_CERTIFICATE",
	[CTRC_CLEANER_REASON_BLOCKED] = "BLOCKED",
};

const char *
cluster_ctrc_stat_name(ClusterCtrcStatId stat)
{
	return stat >= 0 && stat < CTRC_STAT_COUNT
		? ctrc_stat_names[stat] : NULL;
}

const char *
cluster_ctrc_cleaner_reason_name(ClusterCtrcCleanerReason reason)
{
	return reason >= 0 && reason < CTRC_CLEANER_REASON_COUNT
		? ctrc_cleaner_reason_names[reason] : NULL;
}

static bool
ctrc_txn_key_valid(const ClusterCtrcTxnKeyV1 *key)
{
	return key != NULL
		&& key->format_version == CLUSTER_CTRC_FORMAT_VERSION
		&& key->origin_node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& key->owner_instance == key->origin_node_id + 1
		&& key->slot_offset < TT_SLOTS_PER_SEGMENT
		&& TransactionIdIsValid(key->xid)
		&& key->system_identifier != 0
		&& key->origin_boot_incarnation != 0
		&& key->admission_record_generation != 0
		&& key->root_descriptor_incarnation != 0
		&& key->root_id != 0
		&& key->root_generation != 0
		&& ctrc_bytes_zero(key->reserved, sizeof(key->reserved));
}

static bool
ctrc_participant_identity_valid(const ClusterCtrcTxnKeyV1 *key,
								const ClusterCtrcParticipantIdentity *identity)
{
	return key != NULL && identity != NULL
		&& identity->node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& identity->reserved16 == 0
		&& identity->capability_record_generation != 0
		&& identity->boot_incarnation != 0
		&& identity->formation_epoch == key->formation_epoch
		&& identity->admission_record_generation
		   == key->admission_record_generation;
}

static bool
ctrc_bytes_nonzero(const void *address, Size length)
{
	return address != NULL && !ctrc_bytes_zero(address, length);
}

static void
ctrc_put_u16_le(uint8 *bytes, uint16 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
}

static void
ctrc_put_u32_le(uint8 *bytes, uint32 value)
{
	bytes[0] = (uint8)value;
	bytes[1] = (uint8)(value >> 8);
	bytes[2] = (uint8)(value >> 16);
	bytes[3] = (uint8)(value >> 24);
}

static void
ctrc_put_u64_le(uint8 *bytes, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		bytes[i] = (uint8)(value >> (i * 8));
}

static uint16
ctrc_get_u16_le(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
ctrc_get_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0]
		| ((uint32)bytes[1] << 8)
		| ((uint32)bytes[2] << 16)
		| ((uint32)bytes[3] << 24);
}

static uint64
ctrc_get_u64_le(const uint8 *bytes)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= ((uint64)bytes[i]) << (i * 8);
	return value;
}

static uint32
ctrc_crc32c_exact(const uint8 *bytes, Size length)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, length);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
ctrc_txn_key_encode(const ClusterCtrcTxnKeyV1 *key, uint8 bytes[96])
{
	if (!ctrc_txn_key_valid(key) || bytes == NULL)
		return false;
	MemSet(bytes, 0, CLUSTER_CTRC_TXN_KEY_BYTES);
	bytes[0] = key->format_version;
	bytes[1] = key->owner_instance;
	ctrc_put_u16_le(bytes + 2, key->origin_node_id);
	ctrc_put_u32_le(bytes + 4, key->segment_id);
	ctrc_put_u32_le(bytes + 8, key->segment_generation);
	ctrc_put_u16_le(bytes + 12, key->slot_offset);
	ctrc_put_u16_le(bytes + 14, key->slot_wrap);
	ctrc_put_u32_le(bytes + 16, key->xid);
	ctrc_put_u32_le(bytes + 20, key->cluster_epoch);
	ctrc_put_u64_le(bytes + 24, key->system_identifier);
	ctrc_put_u64_le(bytes + 32, key->origin_boot_incarnation);
	ctrc_put_u64_le(bytes + 40, key->formation_epoch);
	ctrc_put_u64_le(bytes + 48, key->admission_record_generation);
	ctrc_put_u64_le(bytes + 56, key->root_descriptor_incarnation);
	ctrc_put_u64_le(bytes + 64, key->root_id);
	ctrc_put_u64_le(bytes + 72, key->root_generation);
	return true;
}

static bool
ctrc_txn_key_decode(const uint8 bytes[96], ClusterCtrcTxnKeyV1 *key_out)
{
	ClusterCtrcTxnKeyV1 key;

	if (bytes == NULL || key_out == NULL)
		return false;
	MemSet(&key, 0, sizeof(key));
	key.format_version = bytes[0];
	key.owner_instance = bytes[1];
	key.origin_node_id = ctrc_get_u16_le(bytes + 2);
	key.segment_id = ctrc_get_u32_le(bytes + 4);
	key.segment_generation = ctrc_get_u32_le(bytes + 8);
	key.slot_offset = ctrc_get_u16_le(bytes + 12);
	key.slot_wrap = ctrc_get_u16_le(bytes + 14);
	key.xid = (TransactionId)ctrc_get_u32_le(bytes + 16);
	key.cluster_epoch = ctrc_get_u32_le(bytes + 20);
	key.system_identifier = ctrc_get_u64_le(bytes + 24);
	key.origin_boot_incarnation = ctrc_get_u64_le(bytes + 32);
	key.formation_epoch = ctrc_get_u64_le(bytes + 40);
	key.admission_record_generation = ctrc_get_u64_le(bytes + 48);
	key.root_descriptor_incarnation = ctrc_get_u64_le(bytes + 56);
	key.root_id = ctrc_get_u64_le(bytes + 64);
	key.root_generation = ctrc_get_u64_le(bytes + 72);
	if (!ctrc_bytes_zero(bytes + 80, 16) || !ctrc_txn_key_valid(&key))
		return false;
	*key_out = key;
	return true;
}

static bool
ctrc_seal_suboperation_valid(uint8 suboperation)
{
	return suboperation == CTRC_SEAL_CLOSE_AND_CLEAN
		|| suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED;
}

bool
cluster_ctrc_seal_request_encode(
	const ClusterCtrcTxnKeyV1 *key, uint64 request_id,
	uint32 grant_generation, uint64 seal_generation,
	uint32 participant_capability_record_generation,
	ClusterCtrcSealSuboperation suboperation,
	uint8 *bytes, Size length)
{
	uint32 tt_slot_id;

	if (bytes != NULL && length > 0)
		MemSet(bytes, 0, length);
	if (bytes == NULL || length != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| !ctrc_txn_key_valid(key) || key->segment_id == 0
		|| key->segment_id > UINT16_MAX || request_id == 0
		|| grant_generation == 0 || seal_generation == 0
		|| participant_capability_record_generation == 0
		|| !ctrc_seal_suboperation_valid((uint8)suboperation))
		return false;
	tt_slot_id = (uint32)key->slot_offset + 1;

	ctrc_put_u64_le(bytes + 0, request_id);
	ctrc_put_u64_le(bytes + 8, key->cluster_epoch);
	ctrc_put_u16_le(bytes + 16, key->origin_node_id);
	ctrc_put_u16_le(bytes + 18, (uint16)key->segment_id);
	ctrc_put_u32_le(bytes + 20, tt_slot_id);
	ctrc_put_u32_le(bytes + 24, key->cluster_epoch);
	ctrc_put_u32_le(bytes + 28, key->xid);
	ctrc_put_u32_le(bytes + 36, (uint32)(int32)key->origin_node_id);
	ctrc_put_u32_le(bytes + 40, (uint32)CLUSTER_CTRC_INTERNAL_ENDPOINT);
	ctrc_put_u32_le(bytes + 48, grant_generation);
	ctrc_put_u16_le(bytes + 52, key->slot_wrap);
	bytes[54] = key->owner_instance;
	bytes[55] = (uint8)suboperation;
	ctrc_put_u32_le(bytes + 56, key->segment_generation);
	ctrc_put_u16_le(bytes + 60, 0);
	bytes[62] = CLUSTER_CTRC_SELECTOR_VERSION;
	bytes[63] = CLUSTER_CTRC_FORWARD_KIND;
	ctrc_put_u32_le(bytes + 64, CLUSTER_CTRC_WIRE_MAGIC);
	ctrc_put_u16_le(bytes + 68, CLUSTER_CTRC_WIRE_VERSION);
	ctrc_put_u16_le(bytes + 70, CLUSTER_CTRC_SEAL_REQUEST_BYTES);
	ctrc_put_u64_le(bytes + 72, key->origin_boot_incarnation);
	ctrc_put_u64_le(bytes + 80, key->formation_epoch);
	ctrc_put_u64_le(bytes + 88, key->admission_record_generation);
	ctrc_put_u64_le(bytes + 96, key->root_descriptor_incarnation);
	ctrc_put_u64_le(bytes + 104, key->root_id);
	ctrc_put_u64_le(bytes + 112, key->root_generation);
	ctrc_put_u64_le(bytes + 120, seal_generation);
	ctrc_put_u32_le(bytes + 128,
		participant_capability_record_generation);
	ctrc_put_u32_le(bytes + 132, 0);
	return true;
}

bool
cluster_ctrc_seal_request_decode(
	const uint8 *bytes, Size length, uint64 authenticated_system_identifier,
	int32 envelope_source_node, int32 local_node, uint64 current_epoch,
	ClusterCtrcSealRequestV1 *request_out, ClusterCtrcTxnKeyV1 *key_out)
{
	ClusterCtrcSealRequestV1 request;
	ClusterCtrcTxnKeyV1 key;
	uint64 encoded_epoch;
	uint32 tt_slot_id;

	if (request_out != NULL)
		MemSet(request_out, 0, sizeof(*request_out));
	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (bytes == NULL || request_out == NULL || key_out == NULL
		|| length != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| authenticated_system_identifier == 0
		|| envelope_source_node < 0
		|| envelope_source_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| local_node < 0 || local_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| current_epoch > UINT32_MAX)
		return false;

	encoded_epoch = ctrc_get_u64_le(bytes + 8);
	tt_slot_id = ctrc_get_u32_le(bytes + 20);
	if (ctrc_get_u64_le(bytes + 0) == 0
		|| encoded_epoch > UINT32_MAX || encoded_epoch != current_epoch
		|| ctrc_get_u16_le(bytes + 16) != (uint16)envelope_source_node
		|| ctrc_get_u16_le(bytes + 18) == 0
		|| tt_slot_id == 0 || tt_slot_id > TT_SLOTS_PER_SEGMENT
		|| ctrc_get_u32_le(bytes + 24) != (uint32)encoded_epoch
		|| !TransactionIdIsValid((TransactionId)ctrc_get_u32_le(bytes + 28))
		|| !ctrc_bytes_zero(bytes + 32, 4)
		|| (int32)ctrc_get_u32_le(bytes + 36) != envelope_source_node
		|| (int32)ctrc_get_u32_le(bytes + 40)
		   != CLUSTER_CTRC_INTERNAL_ENDPOINT
		|| !ctrc_bytes_zero(bytes + 44, 4)
		|| ctrc_get_u32_le(bytes + 48) == 0
		|| bytes[54] != (uint8)(envelope_source_node + 1)
		|| !ctrc_seal_suboperation_valid(bytes[55])
		|| ctrc_get_u16_le(bytes + 60) != 0
		|| bytes[62] != CLUSTER_CTRC_SELECTOR_VERSION
		|| bytes[63] != CLUSTER_CTRC_FORWARD_KIND
		|| ctrc_get_u32_le(bytes + 64) != CLUSTER_CTRC_WIRE_MAGIC
		|| ctrc_get_u16_le(bytes + 68) != CLUSTER_CTRC_WIRE_VERSION
		|| ctrc_get_u16_le(bytes + 70) != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| ctrc_get_u64_le(bytes + 72) == 0
		|| ctrc_get_u64_le(bytes + 88) == 0
		|| ctrc_get_u64_le(bytes + 96) == 0
		|| ctrc_get_u64_le(bytes + 104) == 0
		|| ctrc_get_u64_le(bytes + 112) == 0
		|| ctrc_get_u64_le(bytes + 120) == 0
		|| ctrc_get_u32_le(bytes + 128) == 0
		|| ctrc_get_u32_le(bytes + 132) != 0)
		return false;

	MemSet(&key, 0, sizeof(key));
	key.format_version = CLUSTER_CTRC_FORMAT_VERSION;
	key.owner_instance = bytes[54];
	key.origin_node_id = ctrc_get_u16_le(bytes + 16);
	key.segment_id = ctrc_get_u16_le(bytes + 18);
	key.segment_generation = ctrc_get_u32_le(bytes + 56);
	key.slot_offset = (uint16)(tt_slot_id - 1);
	key.slot_wrap = ctrc_get_u16_le(bytes + 52);
	key.xid = (TransactionId)ctrc_get_u32_le(bytes + 28);
	key.cluster_epoch = (uint32)encoded_epoch;
	key.system_identifier = authenticated_system_identifier;
	key.origin_boot_incarnation = ctrc_get_u64_le(bytes + 72);
	key.formation_epoch = ctrc_get_u64_le(bytes + 80);
	key.admission_record_generation = ctrc_get_u64_le(bytes + 88);
	key.root_descriptor_incarnation = ctrc_get_u64_le(bytes + 96);
	key.root_id = ctrc_get_u64_le(bytes + 104);
	key.root_generation = ctrc_get_u64_le(bytes + 112);
	if (!ctrc_txn_key_valid(&key))
		return false;

	MemSet(&request, 0, sizeof(request));
	request.request_id = ctrc_get_u64_le(bytes + 0);
	request.cluster_epoch = encoded_epoch;
	memcpy(request.tt_status_key_0_19, bytes + 16, 20);
	request.original_requester_node
		= (int32)ctrc_get_u32_le(bytes + 36);
	request.requester_backend_id = (int32)ctrc_get_u32_le(bytes + 40);
	memcpy(request.tt_status_key_20_23, bytes + 44, 4);
	request.grant_generation = ctrc_get_u32_le(bytes + 48);
	request.slot_wrap = ctrc_get_u16_le(bytes + 52);
	request.owner_instance = bytes[54];
	request.suboperation = bytes[55];
	request.segment_generation = ctrc_get_u32_le(bytes + 56);
	request.request_flags = ctrc_get_u16_le(bytes + 60);
	request.selector_version = bytes[62];
	request.forward_kind = bytes[63];
	request.magic = ctrc_get_u32_le(bytes + 64);
	request.wire_version = ctrc_get_u16_le(bytes + 68);
	request.wire_length = ctrc_get_u16_le(bytes + 70);
	request.origin_boot_incarnation = ctrc_get_u64_le(bytes + 72);
	request.formation_epoch = ctrc_get_u64_le(bytes + 80);
	request.admission_record_generation = ctrc_get_u64_le(bytes + 88);
	request.root_descriptor_incarnation = ctrc_get_u64_le(bytes + 96);
	request.root_id = ctrc_get_u64_le(bytes + 104);
	request.root_generation = ctrc_get_u64_le(bytes + 112);
	request.seal_generation = ctrc_get_u64_le(bytes + 120);
	request.participant_capability_record_generation
		= ctrc_get_u32_le(bytes + 128);
	request.reserved_tail = ctrc_get_u32_le(bytes + 132);
	*request_out = request;
	*key_out = key;
	return true;
}

static bool
ctrc_local_ack_shape_valid(const ClusterCtrcLocalReleaseAckV1 *ack)
{
	bool zero_range;

	if (ack == NULL || !ctrc_txn_key_valid(&ack->transaction_key)
		|| ack->grant_generation == 0
		|| ack->result != CTRC_ACK_RELEASED
		|| ack->first_failed_predicate != 0
		|| (ack->flags & ~(CTRC_ACK_FLAG_ZERO_RANGE
							 | CTRC_ACK_FLAG_ALL_DURABLE)) != 0
		|| (ack->flags & CTRC_ACK_FLAG_ALL_DURABLE) == 0
		|| ack->seal_generation == 0
		|| ack->participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| ack->dependency_entry_count != CLUSTER_SF_DEP_MAX_ORIGINS
		|| ack->capability_record_generation == 0
		|| ack->participant_boot_incarnation == 0
		|| ack->formation_epoch != ack->transaction_key.formation_epoch
		|| ack->admission_record_generation
		   != ack->transaction_key.admission_record_generation
		|| !ctrc_bytes_zero(ack->reserved, sizeof(ack->reserved)))
		return false;

	zero_range = (ack->flags & CTRC_ACK_FLAG_ZERO_RANGE) != 0;
	if (zero_range)
		return ack->flags
				 == (CTRC_ACK_FLAG_ZERO_RANGE | CTRC_ACK_FLAG_ALL_DURABLE)
			&& ack->first_key_sequence == 0
			&& ack->last_key_sequence == 0
			&& ack->minimum_journal_sequence == 0
			&& ack->maximum_journal_sequence == 0
			&& ack->total_receipt_count == 0
			&& ack->prepared_count == 0
			&& ack->applied_count == 0
			&& ack->cancelled_count == 0
			&& ack->cleaned_count == 0
			&& ack->ack_frozen_count == 0
			&& memcmp(ack->row_digest_sha256, cluster_ctrc_empty_sha256,
					  sizeof(ack->row_digest_sha256)) == 0
			&& ack->highest_local_cleanout_lsn == InvalidXLogRecPtr
			&& ctrc_bytes_zero(ack->required_lsn_vector,
							   sizeof(ack->required_lsn_vector));

	return ack->flags == CTRC_ACK_FLAG_ALL_DURABLE
		&& ack->first_key_sequence == 1
		&& ack->total_receipt_count != 0
		&& ack->last_key_sequence == ack->total_receipt_count
		&& ack->minimum_journal_sequence != 0
		&& ack->maximum_journal_sequence
		   >= ack->minimum_journal_sequence
		&& ack->prepared_count == 0
		&& ack->applied_count == 0
		&& ack->cancelled_count <= ack->total_receipt_count
		&& ack->cleaned_count
		   == ack->total_receipt_count - ack->cancelled_count
		&& ack->ack_frozen_count == ack->total_receipt_count
		&& memcmp(ack->row_digest_sha256, cluster_ctrc_empty_sha256,
				  sizeof(ack->row_digest_sha256)) != 0;
}

bool
cluster_ctrc_local_release_ack_encode(
	const ClusterCtrcLocalReleaseAckV1 *ack,
	uint8 bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES])
{
	uint32 crc;
	int i;

	if (bytes != NULL)
		MemSet(bytes, 0, CLUSTER_CTRC_LOCAL_ACK_BYTES);
	if (bytes == NULL || !ctrc_local_ack_shape_valid(ack)
		|| !ctrc_txn_key_encode(&ack->transaction_key, bytes))
		return false;
	ctrc_put_u32_le(bytes + 96, ack->grant_generation);
	bytes[100] = ack->result;
	bytes[101] = ack->first_failed_predicate;
	ctrc_put_u16_le(bytes + 102, ack->flags);
	ctrc_put_u64_le(bytes + 104, ack->seal_generation);
	ctrc_put_u16_le(bytes + 112, ack->participant_node_id);
	ctrc_put_u16_le(bytes + 114, ack->dependency_entry_count);
	ctrc_put_u32_le(bytes + 116, ack->capability_record_generation);
	ctrc_put_u64_le(bytes + 120, ack->participant_boot_incarnation);
	ctrc_put_u64_le(bytes + 128, ack->formation_epoch);
	ctrc_put_u64_le(bytes + 136, ack->admission_record_generation);
	ctrc_put_u64_le(bytes + 144, ack->first_key_sequence);
	ctrc_put_u64_le(bytes + 152, ack->last_key_sequence);
	ctrc_put_u64_le(bytes + 160, ack->minimum_journal_sequence);
	ctrc_put_u64_le(bytes + 168, ack->maximum_journal_sequence);
	ctrc_put_u64_le(bytes + 176, ack->total_receipt_count);
	ctrc_put_u64_le(bytes + 184, ack->prepared_count);
	ctrc_put_u64_le(bytes + 192, ack->applied_count);
	ctrc_put_u64_le(bytes + 200, ack->cancelled_count);
	ctrc_put_u64_le(bytes + 208, ack->cleaned_count);
	ctrc_put_u64_le(bytes + 216, ack->ack_frozen_count);
	memcpy(bytes + 224, ack->row_digest_sha256, 32);
	ctrc_put_u64_le(bytes + 256, ack->highest_local_cleanout_lsn);
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
		ctrc_put_u64_le(bytes + 264 + i * 8, ack->required_lsn_vector[i]);
	crc = ctrc_crc32c_exact(bytes, 412);
	ctrc_put_u32_le(bytes + 412, crc);
	return true;
}

bool
cluster_ctrc_local_release_ack_decode(
	const uint8 bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES],
	ClusterCtrcLocalReleaseAckV1 *ack_out)
{
	ClusterCtrcLocalReleaseAckV1 ack;
	uint32 expected_crc;
	int i;

	if (ack_out != NULL)
		MemSet(ack_out, 0, sizeof(*ack_out));
	if (bytes == NULL || ack_out == NULL)
		return false;
	expected_crc = ctrc_get_u32_le(bytes + 412);
	if (expected_crc != ctrc_crc32c_exact(bytes, 412)
		|| !ctrc_bytes_zero(bytes + 392, 20))
		return false;
	MemSet(&ack, 0, sizeof(ack));
	if (!ctrc_txn_key_decode(bytes, &ack.transaction_key))
		return false;
	ack.grant_generation = ctrc_get_u32_le(bytes + 96);
	ack.result = bytes[100];
	ack.first_failed_predicate = bytes[101];
	ack.flags = ctrc_get_u16_le(bytes + 102);
	ack.seal_generation = ctrc_get_u64_le(bytes + 104);
	ack.participant_node_id = ctrc_get_u16_le(bytes + 112);
	ack.dependency_entry_count = ctrc_get_u16_le(bytes + 114);
	ack.capability_record_generation = ctrc_get_u32_le(bytes + 116);
	ack.participant_boot_incarnation = ctrc_get_u64_le(bytes + 120);
	ack.formation_epoch = ctrc_get_u64_le(bytes + 128);
	ack.admission_record_generation = ctrc_get_u64_le(bytes + 136);
	ack.first_key_sequence = ctrc_get_u64_le(bytes + 144);
	ack.last_key_sequence = ctrc_get_u64_le(bytes + 152);
	ack.minimum_journal_sequence = ctrc_get_u64_le(bytes + 160);
	ack.maximum_journal_sequence = ctrc_get_u64_le(bytes + 168);
	ack.total_receipt_count = ctrc_get_u64_le(bytes + 176);
	ack.prepared_count = ctrc_get_u64_le(bytes + 184);
	ack.applied_count = ctrc_get_u64_le(bytes + 192);
	ack.cancelled_count = ctrc_get_u64_le(bytes + 200);
	ack.cleaned_count = ctrc_get_u64_le(bytes + 208);
	ack.ack_frozen_count = ctrc_get_u64_le(bytes + 216);
	memcpy(ack.row_digest_sha256, bytes + 224, 32);
	ack.highest_local_cleanout_lsn = ctrc_get_u64_le(bytes + 256);
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
		ack.required_lsn_vector[i] = ctrc_get_u64_le(bytes + 264 + i * 8);
	ack.crc32c = expected_crc;
	if (!ctrc_local_ack_shape_valid(&ack))
		return false;
	*ack_out = ack;
	return true;
}

static bool
ctrc_reply_result_valid(ClusterCtrcSealReplyResult result,
						uint8 suboperation, uint16 first_reason,
						const ClusterCtrcLocalReleaseAckV1 *ack)
{
	if (!ctrc_seal_suboperation_valid(suboperation)
		|| result < CTRC_SEAL_REPLY_DENIED
		|| result > CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED)
		return false;
	if (result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
		return suboperation == CTRC_SEAL_CLOSE_AND_CLEAN
			&& first_reason == 0 && ack != NULL;
	if (result == CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED)
		return suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED
			&& first_reason == 0 && ack == NULL;
	if (result == CTRC_SEAL_REPLY_PENDING_DRAIN
		&& suboperation != CTRC_SEAL_CLOSE_AND_CLEAN)
		return false;
	return first_reason != 0 && ack == NULL;
}

static bool
ctrc_ack_matches_request(const ClusterCtrcLocalReleaseAckV1 *ack,
						 const uint8 *request_bytes,
						 int32 source_node, int32 destination_node)
{
	ClusterCtrcSealRequestV1 request;
	ClusterCtrcTxnKeyV1 key;
	uint64 current_epoch;

	if (ack == NULL || request_bytes == NULL)
		return false;
	current_epoch = ctrc_get_u64_le(request_bytes + 8);
	if (!cluster_ctrc_seal_request_decode(request_bytes,
			CLUSTER_CTRC_SEAL_REQUEST_BYTES,
			ack->transaction_key.system_identifier, destination_node,
			source_node, current_epoch, &request, &key))
		return false;
	return request.suboperation == CTRC_SEAL_CLOSE_AND_CLEAN
		&& memcmp(&key, &ack->transaction_key, sizeof(key)) == 0
		&& ack->grant_generation == request.grant_generation
		&& ack->seal_generation == request.seal_generation
		&& ack->participant_node_id == (uint16)source_node
		&& ack->capability_record_generation
		   == request.participant_capability_record_generation
		&& ack->formation_epoch == request.formation_epoch
		&& ack->admission_record_generation
		   == request.admission_record_generation;
}

bool
cluster_ctrc_seal_reply_encode(
	const uint8 *request_bytes, Size request_length,
	int32 source_node, int32 destination_node,
	ClusterCtrcSealReplyResult result, uint16 first_reason,
	const ClusterCtrcLocalReleaseAckV1 *ack,
	uint8 *page, Size page_length)
{
	ClusterCtrcSealRequestV1 request;
	ClusterCtrcTxnKeyV1 ignored_key;
	uint8 request_digest[32];
	uint32 header_crc;
	uint64 current_epoch;

	if (page != NULL && page_length > 0)
		MemSet(page, 0, page_length);
	if (request_bytes == NULL
		|| request_length != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| page == NULL || page_length != BLCKSZ
		|| source_node < 0 || source_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| destination_node < 0
		|| destination_node >= CLUSTER_CTRC_MAX_PARTICIPANTS)
		return false;
	current_epoch = ctrc_get_u64_le(request_bytes + 8);
	if (!cluster_ctrc_seal_request_decode(request_bytes, request_length,
			ack != NULL ? ack->transaction_key.system_identifier : UINT64_C(1),
			destination_node, source_node, current_epoch, &request, &ignored_key)
		|| !ctrc_reply_result_valid(result, request.suboperation,
			first_reason, ack)
		|| (ack != NULL
			&& (!ctrc_local_ack_shape_valid(ack)
				|| !ctrc_ack_matches_request(ack, request_bytes,
					source_node, destination_node))))
		return false;

	ctrc_put_u32_le(page + 0, CLUSTER_CTRC_WIRE_MAGIC);
	ctrc_put_u16_le(page + 4, CLUSTER_CTRC_WIRE_VERSION);
	ctrc_put_u16_le(page + 6, CLUSTER_CTRC_REPLY_HEADER_BYTES);
	ctrc_put_u64_le(page + 8, request.request_id);
	ctrc_put_u64_le(page + 16, request.cluster_epoch);
	ctrc_put_u64_le(page + 24, request.seal_generation);
	ctrc_put_u32_le(page + 32, (uint32)source_node);
	ctrc_put_u32_le(page + 36, (uint32)destination_node);
	page[40] = (uint8)result;
	page[41] = request.suboperation;
	ctrc_put_u16_le(page + 42, first_reason);
	ctrc_put_u16_le(page + 44, 0);
	ctrc_put_u16_le(page + 46,
		result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK
			? CLUSTER_CTRC_LOCAL_ACK_BYTES : 0);
	if (!cluster_ctrc_sha256_exact(request_bytes, request_length,
			request_digest))
		return false;
	memcpy(page + 48, request_digest, 8);
	ctrc_put_u32_le(page + 56, BLCKSZ);
	if (ack != NULL
		&& !cluster_ctrc_local_release_ack_encode(ack,
			page + CLUSTER_CTRC_REPLY_ACK_OFFSET))
		return false;
	header_crc = ctrc_crc32c_exact(page, 60);
	ctrc_put_u32_le(page + 60, header_crc);
	return true;
}

bool
cluster_ctrc_seal_reply_decode(
	const uint8 *page, Size page_length,
	const uint8 *request_bytes, Size request_length,
	int32 expected_source_node, int32 expected_destination_node,
	ClusterCtrcSealReplyHeaderV1 *header_out,
	ClusterCtrcLocalReleaseAckV1 *ack_out)
{
	ClusterCtrcSealReplyHeaderV1 header;
	ClusterCtrcSealRequestV1 request;
	ClusterCtrcTxnKeyV1 ignored_key;
	ClusterCtrcLocalReleaseAckV1 ack;
	uint8 request_digest[32];
	uint64 current_epoch;
	bool carries_ack;

	if (header_out != NULL)
		MemSet(header_out, 0, sizeof(*header_out));
	if (ack_out != NULL)
		MemSet(ack_out, 0, sizeof(*ack_out));
	if (page == NULL || request_bytes == NULL || header_out == NULL
		|| ack_out == NULL || page_length != BLCKSZ
		|| request_length != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| expected_source_node < 0
		|| expected_source_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| expected_destination_node < 0
		|| expected_destination_node >= CLUSTER_CTRC_MAX_PARTICIPANTS)
		return false;
	current_epoch = ctrc_get_u64_le(request_bytes + 8);
	if (!cluster_ctrc_seal_request_decode(request_bytes, request_length,
			UINT64_C(1), expected_destination_node, expected_source_node,
			current_epoch, &request, &ignored_key)
		|| !cluster_ctrc_sha256_exact(request_bytes, request_length,
			request_digest)
		|| ctrc_get_u32_le(page + 0) != CLUSTER_CTRC_WIRE_MAGIC
		|| ctrc_get_u16_le(page + 4) != CLUSTER_CTRC_WIRE_VERSION
		|| ctrc_get_u16_le(page + 6) != CLUSTER_CTRC_REPLY_HEADER_BYTES
		|| ctrc_get_u64_le(page + 8) != request.request_id
		|| ctrc_get_u64_le(page + 16) != request.cluster_epoch
		|| ctrc_get_u64_le(page + 24) != request.seal_generation
		|| (int32)ctrc_get_u32_le(page + 32) != expected_source_node
		|| (int32)ctrc_get_u32_le(page + 36) != expected_destination_node
		|| page[41] != request.suboperation
		|| ctrc_get_u16_le(page + 44) != 0
		|| memcmp(page + 48, request_digest, 8) != 0
		|| ctrc_get_u32_le(page + 56) != BLCKSZ
		|| ctrc_get_u32_le(page + 60) != ctrc_crc32c_exact(page, 60))
		return false;

	carries_ack = page[40] == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK;
	if (!ctrc_reply_result_valid((ClusterCtrcSealReplyResult)page[40],
			page[41], ctrc_get_u16_le(page + 42),
			carries_ack ? &ack : NULL)
		|| ctrc_get_u16_le(page + 46)
		   != (carries_ack ? CLUSTER_CTRC_LOCAL_ACK_BYTES : 0))
		return false;
	if (carries_ack)
	{
		if (!cluster_ctrc_local_release_ack_decode(
				page + CLUSTER_CTRC_REPLY_ACK_OFFSET,
				&ack)
			|| !ctrc_ack_matches_request(&ack, request_bytes,
				expected_source_node, expected_destination_node)
			|| !ctrc_bytes_zero(page + CLUSTER_CTRC_REPLY_TAIL_OFFSET,
				BLCKSZ - CLUSTER_CTRC_REPLY_TAIL_OFFSET))
			return false;
		*ack_out = ack;
	}
	else if (!ctrc_bytes_zero(page + CLUSTER_CTRC_REPLY_ACK_OFFSET,
			BLCKSZ - CLUSTER_CTRC_REPLY_ACK_OFFSET))
		return false;

	MemSet(&header, 0, sizeof(header));
	header.magic = ctrc_get_u32_le(page + 0);
	header.wire_version = ctrc_get_u16_le(page + 4);
	header.header_length = ctrc_get_u16_le(page + 6);
	header.request_id = ctrc_get_u64_le(page + 8);
	header.cluster_epoch = ctrc_get_u64_le(page + 16);
	header.seal_generation = ctrc_get_u64_le(page + 24);
	header.source_node = (int32)ctrc_get_u32_le(page + 32);
	header.destination_node = (int32)ctrc_get_u32_le(page + 36);
	header.result = page[40];
	header.suboperation = page[41];
	header.first_reason = ctrc_get_u16_le(page + 42);
	header.reply_flags = ctrc_get_u16_le(page + 44);
	header.ack_length = ctrc_get_u16_le(page + 46);
	memcpy(header.request_sha256_prefix, page + 48, 8);
	header.body_length = ctrc_get_u32_le(page + 56);
	header.header_crc32c = ctrc_get_u32_le(page + 60);
	*header_out = header;
	return true;
}

static bool
ctrc_publication_request_equal(const ClusterCtrcPublicationIdV1 *stored,
							   const ClusterCtrcPublicationIdV1 *request)
{
	ClusterCtrcPublicationIdV1 normalized;

	if (stored == NULL || request == NULL)
		return false;
	normalized = *stored;
	normalized.journal_sequence = 0;
	normalized.key_sequence = 0;
	normalized.journal_slot_generation = 0;
	return memcmp(&normalized, request, sizeof(normalized)) == 0;
}

/* The probe hash is an in-memory placement detail.  Hash the same normalized
 * identity used by duplicate comparison; assigned journal/key sequences must
 * not move a retransmitted publication to another chain. */
static uint64
ctrc_receipt_hash_bytes(uint64 hash, const void *address, Size length)
{
	const uint8 *bytes = (const uint8 *)address;
	Size i;

	for (i = 0; i < length; i++)
	{
		hash ^= bytes[i];
		hash *= UINT64_C(1099511628211);
	}
	return hash;
}

static uint64
ctrc_receipt_probe_hash(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcPublicationIdV1 *publication)
{
	ClusterCtrcPublicationIdV1 normalized;
	uint64 hash = UINT64_C(1469598103934665603);

	normalized = *publication;
	normalized.journal_sequence = 0;
	normalized.key_sequence = 0;
	normalized.journal_slot_generation = 0;
	hash = ctrc_receipt_hash_bytes(hash, key, sizeof(*key));
	return ctrc_receipt_hash_bytes(hash, &normalized, sizeof(normalized));
}

ClusterCtrcPageVersionOrder
cluster_ctrc_page_version_order(uint16 predecessor_origin,
	XLogRecPtr predecessor_lsn, SCN predecessor_scn, uint16 current_origin,
	XLogRecPtr current_lsn, SCN current_scn)
{
	/* Pages created before cluster activation can have a native PostgreSQL
	 * LSN but no cluster WAL origin or block SCN.  That pair is a one-way
	 * baseline: only a fully identified cluster version may succeed it, and
	 * the two LSN integers are never compared. */
	if (predecessor_origin == CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID
		&& !SCN_VALID(predecessor_scn)
		&& !XLogRecPtrIsInvalid(predecessor_lsn))
		return current_origin < CLUSTER_CTRC_MAX_PARTICIPANTS
			&& !XLogRecPtrIsInvalid(current_lsn) && SCN_VALID(current_scn)
			? CTRC_PAGE_VERSION_CURRENT : CTRC_PAGE_VERSION_UNKNOWN;
	if (XLogRecPtrIsInvalid(predecessor_lsn))
		return predecessor_origin == CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID
			&& !SCN_VALID(predecessor_scn)
			? CTRC_PAGE_VERSION_CURRENT : CTRC_PAGE_VERSION_UNKNOWN;
	if (predecessor_origin >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| current_origin >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| XLogRecPtrIsInvalid(current_lsn)
		|| !SCN_VALID(predecessor_scn) || !SCN_VALID(current_scn))
		return CTRC_PAGE_VERSION_UNKNOWN;
	if (scn_time_cmp(current_scn, predecessor_scn) < 0)
		return CTRC_PAGE_VERSION_REGRESSED;
	if (current_origin == predecessor_origin
		&& current_lsn < predecessor_lsn)
		return CTRC_PAGE_VERSION_REGRESSED;
	return CTRC_PAGE_VERSION_CURRENT;
}

static bool
ctrc_target_predecessor_version_valid(const ClusterCtrcTargetV1 *target)
{
	if (target == NULL || target->predecessor_page_lsn_reserved16 != 0)
		return false;
	if (target->predecessor_page_lsn_origin_node_id
		== CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID
		&& !SCN_VALID(target->predecessor_page_scn))
		return true;
	if (XLogRecPtrIsInvalid(target->predecessor_page_lsn))
		return false;
	return target->predecessor_page_lsn_origin_node_id
			< CLUSTER_CTRC_MAX_PARTICIPANTS
		&& SCN_VALID(target->predecessor_page_scn);
}

static bool
ctrc_target_pending_itl_valid(const ClusterCtrcTargetV1 *target)
{
	return target != NULL
		&& target->kind == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& ctrc_target_predecessor_version_valid(target)
		&& target->publication_own_generation != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind != 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_exact_itl_valid(const ClusterCtrcTxnKeyV1 *key,
							const ClusterCtrcTargetV1 *target)
{
	return key != NULL && target != NULL
		&& target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& ctrc_target_predecessor_version_valid(target)
		&& target->publication_own_generation != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind != 0
		&& TransactionIdIsValid(target->itl_xid)
		&& target->itl_xid == key->xid
		&& target->itl_class != 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_nonzero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_nonzero(target->planned_predecessor_sha256,
							  sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_nonzero(target->planned_successor_sha256,
							  sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_itl_finalizes_exact(const ClusterCtrcTargetV1 *pending,
							const ClusterCtrcTargetV1 *final_target)
{
	return pending != NULL && final_target != NULL
		&& pending->kind == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		&& final_target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& pending->spc_oid == final_target->spc_oid
		&& pending->db_oid == final_target->db_oid
		&& pending->rel_number == final_target->rel_number
		&& pending->fork_number == final_target->fork_number
		&& pending->block_number == final_target->block_number
		&& pending->predecessor_page_lsn_origin_node_id
		   == final_target->predecessor_page_lsn_origin_node_id
		&& pending->predecessor_page_lsn
		   == final_target->predecessor_page_lsn
		&& pending->predecessor_page_scn
		   == final_target->predecessor_page_scn
		&& pending->publication_own_generation
		   == final_target->publication_own_generation
		&& pending->publication_acquisition_epoch
		   == final_target->publication_acquisition_epoch
		&& pending->relation_persistence == final_target->relation_persistence
		&& pending->needs_wal == final_target->needs_wal
		&& pending->page_operation_kind == final_target->page_operation_kind;
}

ClusterCtrcItlCleanoutApplyResult
cluster_ctrc_itl_cleanout_slot(const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcTerminalStatus terminal_status, SCN commit_scn,
	ClusterItlSlotData *slot)
{
	uint8 active_flag;
	uint8 committed_flag;
	uint8 aborted_flag;

	if (!ctrc_target_exact_itl_valid(key, target) || slot == NULL
		|| slot->xid != target->itl_xid
		|| slot->wrap != target->itl_slot_wrap
		|| memcmp(&slot->undo_segment_head, target->uba,
			sizeof(slot->undo_segment_head)) != 0
		|| (target->itl_class != 1 && target->itl_class != 2))
		return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
	active_flag = target->itl_class == 1
		? ITL_FLAG_ACTIVE : ITL_FLAG_LOCK_ONLY_ACTIVE;
	committed_flag = target->itl_class == 1
		? ITL_FLAG_COMMITTED : ITL_FLAG_LOCK_ONLY_COMMITTED;
	aborted_flag = target->itl_class == 1
		? ITL_FLAG_ABORTED : ITL_FLAG_LOCK_ONLY_ABORTED;

	if (terminal_status == CTRC_TERMINAL_COMMITTED)
	{
		if (!SCN_VALID(commit_scn))
			return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
		if (slot->flags == committed_flag)
			return slot->commit_scn == commit_scn
				? CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL
				: CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
		if (slot->flags != active_flag
			&& !(target->itl_class == 1
				&& slot->flags == ITL_FLAG_NEEDS_CLEANOUT
				&& slot->commit_scn == commit_scn))
			return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
		if (slot->flags == active_flag && SCN_VALID(slot->commit_scn))
			return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
		slot->commit_scn = commit_scn;
		slot->flags = committed_flag;
		return CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN;
	}
	if (terminal_status != CTRC_TERMINAL_ABORTED
		|| SCN_VALID(commit_scn))
		return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
	if (slot->flags == aborted_flag)
		return slot->commit_scn == InvalidScn
			? CLUSTER_CTRC_ITL_CLEANOUT_ALREADY_TERMINAL
			: CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
	if (slot->flags != active_flag || SCN_VALID(slot->commit_scn))
		return CLUSTER_CTRC_ITL_CLEANOUT_RETAIN;
	slot->commit_scn = InvalidScn;
	slot->flags = aborted_flag;
	return CLUSTER_CTRC_ITL_CLEANOUT_REWRITTEN;
}

static bool
ctrc_target_pending_offnum_valid(const ClusterCtrcPublicationIdV1 *publication,
								 const ClusterCtrcTargetV1 *target)
{
	return publication != NULL && target != NULL
		&& target->kind == CTRC_TARGET_PAGE_PENDING_OFFNUM
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& ctrc_target_predecessor_version_valid(target)
		&& target->publication_own_generation != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind == 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number == 0
		&& target->itemid_flags == 0
		&& target->itemid_offset == 0
		&& target->itemid_length == 0
		&& ctrc_bytes_zero(target->tuple_header_sha256,
						   sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id == 0
		&& target->reserved16 == 0
		&& target->multixact_id == 0
		&& target->mx_cluster_epoch == 0
		&& target->descriptor_hash == 0
		&& ctrc_bytes_zero(target->successor_topology_sha256,
						   sizeof(target->successor_topology_sha256))
		&& target->intended_descriptor_hash != 0
		&& target->intended_descriptor_hash == publication->descriptor_hash;
}

static bool
ctrc_target_exact_tid_valid(const ClusterCtrcPublicationIdV1 *publication,
							const ClusterCtrcTargetV1 *target)
{
	bool hot_edge;

	if (publication == NULL || target == NULL)
		return false;
	hot_edge = publication->reference_kind == CTRC_REF_HOT_FOLLOW_EDGE;
	return target->kind == CTRC_TARGET_EXACT_TID
		&& target->spc_oid != InvalidOid
		&& target->db_oid != InvalidOid
		&& target->rel_number != InvalidOid
		&& target->block_number != InvalidBlockNumber
		&& ctrc_target_predecessor_version_valid(target)
		&& target->publication_own_generation != 0
		&& target->relation_persistence != 0
		&& target->page_operation_kind == 0
		&& target->itl_slot_index == 0
		&& target->itl_slot_wrap == 0
		&& !TransactionIdIsValid(target->itl_xid)
		&& target->itl_class == 0
		&& ctrc_bytes_zero(target->reserved8, sizeof(target->reserved8))
		&& ctrc_bytes_zero(target->uba, sizeof(target->uba))
		&& ctrc_bytes_zero(target->planned_predecessor_sha256,
						   sizeof(target->planned_predecessor_sha256))
		&& ctrc_bytes_zero(target->planned_successor_sha256,
						   sizeof(target->planned_successor_sha256))
		&& target->offset_number != 0
		&& target->itemid_flags != 0
		&& target->itemid_offset != 0
		&& target->itemid_length != 0
		&& ctrc_bytes_nonzero(target->tuple_header_sha256,
						  sizeof(target->tuple_header_sha256))
		&& target->mx_origin_node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& target->reserved16 == 0
		&& MultiXactIdIsValid((MultiXactId)target->multixact_id)
		&& target->descriptor_hash != 0
		&& target->descriptor_hash == publication->descriptor_hash
		&& (hot_edge
			? ctrc_bytes_nonzero(target->successor_topology_sha256,
							 sizeof(target->successor_topology_sha256))
			: ctrc_bytes_zero(target->successor_topology_sha256,
							 sizeof(target->successor_topology_sha256)))
		&& target->intended_descriptor_hash == 0;
}

static bool
ctrc_target_offnum_finalizes_exact(
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *pending,
	const ClusterCtrcTargetV1 *final_target)
{
	return ctrc_target_pending_offnum_valid(publication, pending)
		&& ctrc_target_exact_tid_valid(publication, final_target)
		&& pending->spc_oid == final_target->spc_oid
		&& pending->db_oid == final_target->db_oid
		&& pending->rel_number == final_target->rel_number
		&& pending->fork_number == final_target->fork_number
		&& pending->block_number == final_target->block_number
		&& pending->predecessor_page_lsn_origin_node_id
		   == final_target->predecessor_page_lsn_origin_node_id
		&& pending->predecessor_page_lsn
		   == final_target->predecessor_page_lsn
		&& pending->predecessor_page_scn
		   == final_target->predecessor_page_scn
		&& pending->publication_own_generation
		   == final_target->publication_own_generation
		&& pending->publication_acquisition_epoch
		   == final_target->publication_acquisition_epoch
		&& pending->relation_persistence == final_target->relation_persistence
		&& pending->needs_wal == final_target->needs_wal
		&& pending->intended_descriptor_hash
		   == final_target->descriptor_hash;
}

static bool
ctrc_publication_prepare_valid(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target)
{
	return participant != NULL && publication != NULL
		&& participant->state == CTRC_PARTICIPANT_OPEN
		&& participant->grant_generation != 0
		&& ctrc_txn_key_valid(&participant->key)
		&& ctrc_participant_identity_valid(&participant->key,
										 &participant->identity)
		&& publication->requester_node_id == participant->identity.node_id
		&& publication->requester_boot_incarnation
		   == participant->identity.boot_incarnation
		&& publication->capability_record_generation
		   == participant->identity.capability_record_generation
		&& publication->requester_backend_id > 0
		&& publication->wire_request_id != 0
		&& publication->operation_id != 0
		&& publication->attempt_generation != 0
		&& publication->reserved8 == 0
		&& publication->journal_sequence == 0
		&& publication->key_sequence == 0
		&& publication->journal_slot_generation == 0
		&& publication->grant_generation == participant->grant_generation
		&& ((publication->reference_kind == CTRC_REF_HEAP_ITL_UBA
				&& publication->descriptor_hash == 0
				&& publication->member_ordinal == UINT16_MAX
				&& publication->member_role == 0
				&& publication->target_kind
				   == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
				&& ctrc_target_pending_itl_valid(target))
			|| (publication->reference_kind >= CTRC_REF_CURRENT_MX_LOCKER
				&& publication->reference_kind <= CTRC_REF_HOT_FOLLOW_EDGE
				&& publication->descriptor_hash != 0
				&& publication->member_ordinal
				   < CLUSTER_CURRENT_MX_MAX_MEMBERS
				&& publication->member_role != 0
				&& publication->target_kind
				   == CTRC_TARGET_PAGE_PENDING_OFFNUM
				&& ctrc_target_pending_offnum_valid(publication, target)));
}

static bool
ctrc_tlv_bytes(uint8 *bytes, Size capacity, Size *offset, uint16 tag,
	const uint8 *value, uint16 value_length)
{
	if (bytes == NULL || offset == NULL || value == NULL
		|| *offset > capacity || capacity - *offset < (Size)4 + value_length)
		return false;
	ctrc_put_u16_le(bytes + *offset, tag);
	ctrc_put_u16_le(bytes + *offset + 2, value_length);
	memcpy(bytes + *offset + 4, value, value_length);
	*offset += (Size)4 + value_length;
	return true;
}

static bool
ctrc_tlv_u8(uint8 *bytes, Size capacity, Size *offset, uint16 tag,
	uint8 value)
{
	return ctrc_tlv_bytes(bytes, capacity, offset, tag, &value, 1);
}

static bool
ctrc_tlv_u16(uint8 *bytes, Size capacity, Size *offset, uint16 tag,
	uint16 value)
{
	uint8 encoded[2];

	ctrc_put_u16_le(encoded, value);
	return ctrc_tlv_bytes(bytes, capacity, offset, tag, encoded,
		sizeof(encoded));
}

static bool
ctrc_tlv_u32(uint8 *bytes, Size capacity, Size *offset, uint16 tag,
	uint32 value)
{
	uint8 encoded[4];

	ctrc_put_u32_le(encoded, value);
	return ctrc_tlv_bytes(bytes, capacity, offset, tag, encoded,
		sizeof(encoded));
}

static bool
ctrc_tlv_u64(uint8 *bytes, Size capacity, Size *offset, uint16 tag,
	uint64 value)
{
	uint8 encoded[8];

	ctrc_put_u64_le(encoded, value);
	return ctrc_tlv_bytes(bytes, capacity, offset, tag, encoded,
		sizeof(encoded));
}

static bool
ctrc_publication_id_encode(const ClusterCtrcPublicationIdV1 *publication,
	uint8 bytes[CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES])
{
	Size offset = 0;

	if (publication == NULL || bytes == NULL || publication->reserved8 != 0)
		return false;
	MemSet(bytes, 0, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES);
	return ctrc_tlv_u16(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 1, publication->requester_node_id)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 2, publication->requester_boot_incarnation)
		&& ctrc_tlv_u32(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 3, publication->capability_record_generation)
		&& ctrc_tlv_u32(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 4, (uint32)publication->requester_backend_id)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 5, publication->wire_request_id)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 6, publication->operation_id)
		&& ctrc_tlv_u32(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 7, publication->attempt_generation)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 8, publication->descriptor_hash)
		&& ctrc_tlv_u16(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 9, publication->member_ordinal)
		&& ctrc_tlv_u8(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 10, publication->member_role)
		&& ctrc_tlv_u8(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 11, publication->reference_kind)
		&& ctrc_tlv_u8(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 12, publication->target_kind)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 13, publication->journal_sequence)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 14, publication->key_sequence)
		&& ctrc_tlv_u64(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 15, publication->journal_slot_generation)
		&& ctrc_tlv_u32(bytes, CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES,
			&offset, 16, publication->grant_generation)
		&& offset == CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES;
}

static bool
ctrc_target_encode(const ClusterCtrcTargetV1 *target,
	uint8 bytes[CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES], Size *length_out)
{
	Size offset = 0;
	Size expected_length;

	if (length_out != NULL)
		*length_out = 0;
	if (target == NULL || bytes == NULL || length_out == NULL)
		return false;
	MemSet(bytes, 0, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES);
	if (!ctrc_tlv_u8(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 1, target->kind)
		|| !ctrc_tlv_u32(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 2, target->spc_oid)
		|| !ctrc_tlv_u32(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 3, target->db_oid)
		|| !ctrc_tlv_u32(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 4, target->rel_number)
		|| !ctrc_tlv_u32(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 5, (uint32)target->fork_number)
		|| !ctrc_tlv_u32(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 6, target->block_number)
		|| !ctrc_tlv_u64(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 7, target->predecessor_page_lsn)
		|| !ctrc_tlv_u64(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 8, target->predecessor_page_scn)
		|| !ctrc_tlv_u64(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 9, target->publication_own_generation)
		|| !ctrc_tlv_u64(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 10, target->publication_acquisition_epoch)
		|| !ctrc_tlv_u8(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 11, target->relation_persistence)
		|| !ctrc_tlv_u8(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 12, target->needs_wal)
		|| !ctrc_tlv_u16(bytes, CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES,
			&offset, 13, target->predecessor_page_lsn_origin_node_id))
		return false;

	switch ((ClusterCtrcTargetKind)target->kind)
	{
		case CTRC_TARGET_EXACT_ITL_SLOT:
			expected_length = 226;
			if (!ctrc_tlv_u16(bytes, sizeof(uint8) *
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 20,
					target->itl_slot_index)
				|| !ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 21,
					target->itl_slot_wrap)
				|| !ctrc_tlv_u32(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 22,
					target->itl_xid)
				|| !ctrc_tlv_u8(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 23,
					target->itl_class)
				|| !ctrc_tlv_bytes(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 24,
					target->uba, sizeof(target->uba))
				|| !ctrc_tlv_bytes(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 25,
					target->planned_predecessor_sha256,
					sizeof(target->planned_predecessor_sha256))
				|| !ctrc_tlv_bytes(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 26,
					target->planned_successor_sha256,
					sizeof(target->planned_successor_sha256)))
				return false;
			break;
		case CTRC_TARGET_PAGE_PENDING_ITL_SLOT:
			expected_length = 114;
			if (!ctrc_tlv_u8(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 27,
					target->page_operation_kind))
				return false;
			break;
		case CTRC_TARGET_EXACT_TID:
			expected_length = 239;
			if (!ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 30,
					target->offset_number)
				|| !ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 31,
					target->itemid_flags)
				|| !ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 32,
					target->itemid_offset)
				|| !ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 33,
					target->itemid_length)
				|| !ctrc_tlv_bytes(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 34,
					target->tuple_header_sha256,
					sizeof(target->tuple_header_sha256))
				|| !ctrc_tlv_u16(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 35,
					target->mx_origin_node_id)
				|| !ctrc_tlv_u32(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 36,
					target->multixact_id)
				|| !ctrc_tlv_u32(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 37,
					target->mx_cluster_epoch)
				|| !ctrc_tlv_u64(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 38,
					target->descriptor_hash)
				|| !ctrc_tlv_bytes(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 39,
					target->successor_topology_sha256,
					sizeof(target->successor_topology_sha256)))
				return false;
			break;
		case CTRC_TARGET_PAGE_PENDING_OFFNUM:
			expected_length = 121;
			if (!ctrc_tlv_u64(bytes,
					CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES, &offset, 40,
					target->intended_descriptor_hash))
				return false;
			break;
		default:
			return false;
	}
	if (offset != expected_length)
		return false;
	*length_out = offset;
	return true;
}

/* Header bytes are part of sizing, not a wire or shared ABI promise. */
typedef struct ClusterCtrcSharedHeader
{
	slock_t origin_lock;
	slock_t participant_lock;
	slock_t receipt_lock;
	uint32 magic;
	uint32 version;
	uint32 global_grant_generation;
	uint32 reserved32;
	uint64 total_bytes;
	uint64 origin_key_entries;
	uint64 participant_key_entries;
	uint64 receipt_entries;
	uint64 participant_ack_summary_entries;
	uint64 origin_ack_inbox_entries;
	uint64 origin_offset;
	uint64 participant_offset;
	uint64 receipt_offset;
	uint64 receipt_probe_offset;
	uint64 participant_ack_offset;
	uint64 origin_ack_offset;
	uint64 global_reservation_generation;
	uint64 global_journal_generation;
	uint64 global_seal_generation;
	uint64 global_request_generation;
	uint64 full_refusal_count;
	pg_atomic_uint64 stats[CTRC_STAT_COUNT];
	pg_atomic_uint64 test_barrier_hit_count;
	pg_atomic_uint32 test_barrier_phase;
	pg_atomic_uint32 cleaner_reason;
	uint8 reserved[24];
} ClusterCtrcSharedHeader;

static bool
ctrc_size_add(Size left, Size right, Size *result)
{
	if (result == NULL || left > SIZE_MAX - right)
		return false;
	*result = left + right;
	return true;
}

static bool
ctrc_size_mul(Size left, Size right, Size *result)
{
	if (result == NULL || (right != 0 && left > SIZE_MAX / right))
		return false;
	*result = left * right;
	return true;
}

static bool
ctrc_capacity_add_array(Size count, Size element_bytes, Size *total)
{
	Size bytes;

	return ctrc_size_mul(count, element_bytes, &bytes)
		&& ctrc_size_add(*total, MAXALIGN(bytes), total);
}

bool
cluster_ctrc_capacity_compute(Size n_buffers, int max_backends,
							  int declared_nodes,
							  ClusterCtrcCapacity *capacity)
{
	Size origin_entries;
	Size participant_entries;
	Size receipt_entries;
	Size backend_receipts;
	Size total;
	int nodes;

	if (capacity == NULL || max_backends < 0)
		return false;
	MemSet(capacity, 0, sizeof(*capacity));
	nodes = declared_nodes < 1 ? 1 : declared_nodes;
	if (nodes > CLUSTER_CTRC_MAX_PARTICIPANTS)
		nodes = CLUSTER_CTRC_MAX_PARTICIPANTS;
	if (!ctrc_size_mul((Size)CLUSTER_UNDO_SEGS_PER_INSTANCE,
					   (Size)TT_SLOTS_PER_SEGMENT, &origin_entries)
		|| !ctrc_size_mul(origin_entries, (Size)nodes,
						&participant_entries)
		|| !ctrc_size_mul((Size)max_backends,
					   (Size)CLUSTER_CURRENT_MX_MAX_MEMBERS,
					   &backend_receipts)
		|| !ctrc_size_add(n_buffers, backend_receipts, &receipt_entries))
		return false;

	total = MAXALIGN(sizeof(ClusterCtrcSharedHeader));
	if (!ctrc_capacity_add_array(origin_entries,
							 sizeof(ClusterCtrcOriginEntry), &total)
		|| !ctrc_capacity_add_array(participant_entries,
							 sizeof(ClusterCtrcParticipantEntry), &total)
		|| !ctrc_capacity_add_array(receipt_entries,
								 sizeof(ClusterCtrcReceipt), &total)
		|| !ctrc_capacity_add_array(receipt_entries, sizeof(uint8), &total)
		|| !ctrc_capacity_add_array(participant_entries,
							 sizeof(ClusterCtrcLocalReleaseAckV1), &total)
		|| !ctrc_capacity_add_array(participant_entries,
							 sizeof(ClusterCtrcLocalReleaseAckV1), &total))
		return false;

	capacity->origin_key_entries = origin_entries;
	capacity->participant_key_entries = participant_entries;
	capacity->receipt_entries = receipt_entries;
	capacity->participant_ack_summary_entries = participant_entries;
	capacity->origin_ack_inbox_entries = participant_entries;
	capacity->total_bytes = total;
	return true;
}

ClusterCtrcCapacityDisposition
cluster_ctrc_runtime_full_disposition(void)
{
	return CLUSTER_CTRC_CAPACITY_REFUSE_BEFORE_MUTATION;
}

bool
cluster_ctrc_participant_index_compute(
	uint64 origin_index, uint64 origin_entries, uint64 participant_entries,
	uint16 participant_node_id, uint64 *index_out)
{
	uint64 nodes;
	uint64 index;

	if (index_out == NULL || origin_entries == 0
		|| origin_index >= origin_entries
		|| participant_entries % origin_entries != 0)
		return false;
	nodes = participant_entries / origin_entries;
	if (nodes == 0 || nodes > CLUSTER_CTRC_MAX_PARTICIPANTS
		|| participant_node_id >= nodes)
		return false;
	index = origin_index * nodes + participant_node_id;
	if (index >= participant_entries)
		return false;
	*index_out = index;
	return true;
}

#ifndef CLUSTER_CTRC_UNIT_TEST

static ClusterCtrcSharedHeader *CtrcShared = NULL;

/* Process-local scheduling hints, never part of an authority snapshot. Only
 * the existing UndoCleaner runs batches. A reused slot may inherit a cursor,
 * but every operation still samples and checks the current shared identity. */
#define CTRC_CLEANER_SEAL_BATCH 8
#define CTRC_CLEANER_RECEIPT_BATCH 32
#define CTRC_CLEANER_ACK_BATCH 16
#define CTRC_CLEANER_DISPATCH_BATCH 64
#define CTRC_CLEANER_OLD_BATCH (CTRC_CLEANER_DISPATCH_BATCH / 2)
#define CTRC_CLEANER_CERTIFICATE_BATCH 16
#define CTRC_CLEANER_ORIGINS (CLUSTER_UNDO_SEGS_PER_INSTANCE * TT_SLOTS_PER_SEGMENT)

typedef enum CtrcCleanerScanKind {
	CTRC_SCAN_OPEN,
	CTRC_SCAN_RECEIPT,
	CTRC_SCAN_ACK,
	CTRC_SCAN_CERTIFICATE,
	CTRC_SCAN_COUNT
} CtrcCleanerScanKind;

typedef struct CtrcCleanerOriginHint {
	uint64 seal_generation;
	uint64 first_observed_us;
	uint16 participant_cursor;
} CtrcCleanerOriginHint;

typedef struct CtrcCleanerBatch {
	bool active;
	uint64 remaining[CTRC_SCAN_COUNT];
	uint64 dispatch_index[CTRC_CLEANER_DISPATCH_BATCH];
	unsigned dispatch_count;
	unsigned dispatch_next;
} CtrcCleanerBatch;

static CtrcCleanerBatch CtrcBatch;
static CtrcCleanerOriginHint CtrcOriginHint[CTRC_CLEANER_ORIGINS];
static uint64 CtrcDispatchCursor;

static uint64
ctrc_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static uint64
ctrc_scan_limit(CtrcCleanerScanKind kind, uint64 capacity)
{
	return CtrcBatch.active ? Min(capacity, CtrcBatch.remaining[kind]) : capacity;
}

static void
ctrc_scan_charge(CtrcCleanerScanKind kind, uint64 visited)
{
	if (CtrcBatch.active) {
		Assert(visited <= CtrcBatch.remaining[kind]);
		CtrcBatch.remaining[kind] -= visited;
	}
}

static void
ctrc_semantic_progress(bool wake)
{
	cluster_ctrc_stat_bump(CTRC_STAT_SEMANTIC_PROGRESS);
	if (wake)
		cluster_undo_cleaner_wakeup();
}
static bool ctrc_cleaner_clean_next_receipt(void);
static bool ctrc_cleaner_clean_current_mx_receipt(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcReceipt *receipt, uint64 participant_index,
	uint64 receipt_index);
static bool ctrc_participant_freeze_next_ack_shared(void);
static bool ctrc_origin_next_certificate_snapshot_shared(
	ClusterCtrcOriginCertificateSnapshot *snapshot);
static bool ctrc_origin_certificate_snapshot_matches_shared(
	const ClusterCtrcOriginCertificateSnapshot *expected);
static bool ctrc_origin_certificate_commit_shared(
	const ClusterCtrcOriginCertificateSnapshot *snapshot);
static bool ctrc_cleaner_publish_certificate(
	const ClusterCtrcOriginCertificateSnapshot *snapshot);
static void ctrc_participant_capture_durability(
	ClusterCtrcDurability *durability);

/* Hot-path counters and the deterministic test seam must not repeat the
 * capacity/layout walk performed by cluster_ctrc_shmem_ready().  The shared
 * object is fixed for the postmaster lifetime; magic+version is the bounded
 * attachment check after the full validation in cluster_ctrc_shmem_init(). */
static inline bool
ctrc_runtime_attached(void)
{
	return CtrcShared != NULL
		&& CtrcShared->magic == CLUSTER_CTRC_SHMEM_MAGIC
		&& CtrcShared->version == CLUSTER_CTRC_SHMEM_VERSION;
}

static ClusterCtrcOriginEntry *
ctrc_origin_entries(void)
{
	return (ClusterCtrcOriginEntry *)((char *)CtrcShared
		+ CtrcShared->origin_offset);
}

static ClusterCtrcParticipantEntry *
ctrc_participant_entries(void)
{
	return (ClusterCtrcParticipantEntry *)((char *)CtrcShared
		+ CtrcShared->participant_offset);
}

static ClusterCtrcReceipt *
ctrc_receipt_entries(void)
{
	return (ClusterCtrcReceipt *)((char *)CtrcShared
		+ CtrcShared->receipt_offset);
}

static uint8 *
ctrc_receipt_probe_states(void)
{
	return (uint8 *)((char *)CtrcShared
		+ CtrcShared->receipt_probe_offset);
}

static ClusterCtrcLocalReleaseAckV1 *
ctrc_participant_ack_entries(void)
{
	return (ClusterCtrcLocalReleaseAckV1 *)((char *)CtrcShared
		+ CtrcShared->participant_ack_offset);
}

static ClusterCtrcLocalReleaseAckV1 *
ctrc_origin_ack_entries(void)
{
	return (ClusterCtrcLocalReleaseAckV1 *)((char *)CtrcShared
		+ CtrcShared->origin_ack_offset);
}

static bool
ctrc_origin_index(const ClusterCtrcTxnKeyV1 *key, uint64 *index_out)
{
	uint32 first_segment;
	uint32 segment_ordinal;
	uint64 index;

	if (!ctrc_txn_key_valid(key) || index_out == NULL
		|| key->owner_instance == 0
		|| key->origin_node_id + 1 != key->owner_instance)
		return false;
	first_segment = ((uint32)key->owner_instance - 1)
		* CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
	if (key->segment_id < first_segment
		|| key->segment_id >= first_segment + CLUSTER_UNDO_SEGS_PER_INSTANCE)
		return false;
	segment_ordinal = key->segment_id - first_segment;
	index = (uint64)segment_ordinal * TT_SLOTS_PER_SEGMENT
		+ key->slot_offset;
	if (CtrcShared == NULL || index >= CtrcShared->origin_key_entries)
		return false;
	*index_out = index;
	return true;
}

static bool
ctrc_participant_index(const ClusterCtrcTxnKeyV1 *key,
					   uint16 participant_node_id, uint64 *index_out)
{
	uint64 origin_index;

	if (CtrcShared == NULL || index_out == NULL
		|| !ctrc_origin_index(key, &origin_index))
		return false;
	return cluster_ctrc_participant_index_compute(origin_index,
		CtrcShared->origin_key_entries, CtrcShared->participant_key_entries,
		participant_node_id, index_out);
}

static bool
ctrc_origin_ack_index(const ClusterCtrcTxnKeyV1 *key,
					  uint16 participant_node_id, uint64 *index_out)
{
	uint64 origin_index;
	uint64 nodes;
	uint64 index;

	if (CtrcShared == NULL || index_out == NULL
		|| !ctrc_origin_index(key, &origin_index)
		|| CtrcShared->origin_key_entries == 0
		|| CtrcShared->origin_ack_inbox_entries
		   % CtrcShared->origin_key_entries != 0)
		return false;
	nodes = CtrcShared->origin_ack_inbox_entries
		/ CtrcShared->origin_key_entries;
	if (nodes == 0 || participant_node_id >= nodes)
		return false;
	index = origin_index * nodes + participant_node_id;
	if (index >= CtrcShared->origin_ack_inbox_entries)
		return false;
	*index_out = index;
	return true;
}

/* Caller holds origin_lock followed by receipt_lock. */
static bool
ctrc_origin_certificate_snapshot_index_locked(uint64 origin_index,
	ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS];
	ClusterCtrcOriginEntry *origin;
	uint64 nodes;
	uint16 node_id;

	if (snapshot != NULL)
		MemSet(snapshot, 0, sizeof(*snapshot));
	if (snapshot == NULL || CtrcShared == NULL
		|| origin_index >= CtrcShared->origin_key_entries
		|| CtrcShared->origin_key_entries == 0
		|| CtrcShared->origin_ack_inbox_entries
		   % CtrcShared->origin_key_entries != 0)
		return false;
	nodes = CtrcShared->origin_ack_inbox_entries
		/ CtrcShared->origin_key_entries;
	if (nodes == 0 || nodes > CLUSTER_CTRC_MAX_PARTICIPANTS)
		return false;

	MemSet(ack_slots, 0, sizeof(ack_slots));
	origin = &ctrc_origin_entries()[origin_index];
	for (node_id = 0; node_id < CLUSTER_CTRC_MAX_PARTICIPANTS; node_id++)
	{
		uint32 bit = UINT32_C(1) << node_id;

		if (node_id >= nodes)
		{
			if ((origin->touched_bitmap & bit) != 0)
				return false;
			continue;
		}
		ack_slots[node_id]
			= ctrc_origin_ack_entries()[origin_index * nodes + node_id];
	}
	return cluster_ctrc_origin_certificate_snapshot_entry(origin,
		ack_slots, origin_index, snapshot);
}

/* Caller holds origin_lock followed by receipt_lock.  Release proof is
 * already canonical on block 0; this only reclaims the exact volatile inbox
 * and origin entry after every participant has acknowledged notification. */
static bool
ctrc_origin_release_reclaim_locked(uint64 origin_index)
{
	ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS];
	ClusterCtrcOriginCertificateSnapshot checked;
	ClusterCtrcOriginEntry certifying;
	ClusterCtrcOriginEntry *origin;
	uint64 nodes;
	uint16 node_id;

	if (CtrcShared == NULL
		|| origin_index >= CtrcShared->origin_key_entries
		|| CtrcShared->origin_key_entries == 0
		|| CtrcShared->origin_ack_inbox_entries
		   % CtrcShared->origin_key_entries != 0)
		return false;
	nodes = CtrcShared->origin_ack_inbox_entries
		/ CtrcShared->origin_key_entries;
	if (nodes == 0 || nodes > CLUSTER_CTRC_MAX_PARTICIPANTS)
		return false;
	origin = &ctrc_origin_entries()[origin_index];
	if (origin->state != CTRC_ORIGIN_RELEASE_PROVEN
		|| origin->close_dispatched_bitmap != origin->touched_bitmap
		|| origin->close_confirmed_bitmap != origin->touched_bitmap)
		return false;

	MemSet(ack_slots, 0, sizeof(ack_slots));
	for (node_id = 0; node_id < nodes; node_id++)
		ack_slots[node_id]
			= ctrc_origin_ack_entries()[origin_index * nodes + node_id];
	certifying = *origin;
	certifying.state = CTRC_ORIGIN_CERTIFYING;
	if (!cluster_ctrc_origin_certificate_snapshot_entry(&certifying,
		ack_slots, origin_index, &checked))
		return false;

	MemSet(&ctrc_origin_ack_entries()[origin_index * nodes], 0,
		nodes * sizeof(ClusterCtrcLocalReleaseAckV1));
	MemSet(origin, 0, sizeof(*origin));
	return true;
}

static bool
ctrc_origin_next_certificate_snapshot_shared(
	ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	static uint64 scan_cursor = 0;
	uint64 visited;
	uint64 limit;
	bool found = false;

	if (snapshot != NULL)
		MemSet(snapshot, 0, sizeof(*snapshot));
	if (snapshot == NULL || cluster_node_id < 0
		|| !cluster_ctrc_shmem_ready())
		return false;
	limit = ctrc_scan_limit(CTRC_SCAN_CERTIFICATE, CtrcShared->origin_key_entries);
	SpinLockAcquire(&CtrcShared->origin_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	for (visited = 0; visited < limit; visited++) {
		uint64 index = (scan_cursor + visited)
			% CtrcShared->origin_key_entries;
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];

		if (origin->state != CTRC_ORIGIN_CERTIFYING
			|| origin->key.origin_node_id != (uint16)cluster_node_id)
			continue;
		if (!ctrc_origin_certificate_snapshot_index_locked(index, snapshot))
		{
			origin->state = CTRC_ORIGIN_BLOCKED;
			continue;
		}
		scan_cursor = (index + 1) % CtrcShared->origin_key_entries;
		found = true;
		break;
	}
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->origin_lock);
	ctrc_scan_charge(CTRC_SCAN_CERTIFICATE, visited + (found ? 1 : 0));
	return found;
}

static bool
ctrc_origin_certificate_snapshot_matches_shared(
	const ClusterCtrcOriginCertificateSnapshot *expected)
{
	ClusterCtrcOriginCertificateSnapshot current;
	bool exact;

	if (expected == NULL || !cluster_ctrc_shmem_ready())
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	exact = ctrc_origin_certificate_snapshot_index_locked(
		expected->origin_index, &current)
		&& memcmp(&current, expected, sizeof(current)) == 0;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->origin_lock);
	return exact;
}

static bool
ctrc_origin_certificate_commit_shared(
	const ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcOriginEntry *origin;
	bool committed = false;

	if (snapshot == NULL || !cluster_ctrc_shmem_ready()
		|| snapshot->origin_index >= CtrcShared->origin_key_entries)
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	origin = &ctrc_origin_entries()[snapshot->origin_index];
	committed = cluster_ctrc_origin_certificate_commit_entry(
		origin, snapshot);
	if (committed && origin->touched_bitmap == 0)
		committed = ctrc_origin_release_reclaim_locked(
			snapshot->origin_index);
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->origin_lock);
	return committed;
}

static bool
ctrc_runtime_capacity(ClusterCtrcCapacity *capacity)
{
	return cluster_ctrc_capacity_compute((Size)NBuffers, MaxBackends,
		cluster_conf_declared_node_count_early(), capacity);
}

Size
cluster_ctrc_shmem_size(void)
{
	ClusterCtrcCapacity capacity;

	if (!ctrc_runtime_capacity(&capacity))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("CTRC shared-memory capacity overflows Size")));
	return capacity.total_bytes;
}

static void
ctrc_shared_fill_layout(ClusterCtrcSharedHeader *state,
						const ClusterCtrcCapacity *capacity)
{
	Size offset = MAXALIGN(sizeof(*state));

	state->origin_offset = offset;
	offset += MAXALIGN(capacity->origin_key_entries
					   * sizeof(ClusterCtrcOriginEntry));
	state->participant_offset = offset;
	offset += MAXALIGN(capacity->participant_key_entries
					   * sizeof(ClusterCtrcParticipantEntry));
	state->receipt_offset = offset;
	offset += MAXALIGN(capacity->receipt_entries * sizeof(ClusterCtrcReceipt));
	state->receipt_probe_offset = offset;
	offset += MAXALIGN(capacity->receipt_entries * sizeof(uint8));
	state->participant_ack_offset = offset;
	offset += MAXALIGN(capacity->participant_ack_summary_entries
				   * sizeof(ClusterCtrcLocalReleaseAckV1));
	state->origin_ack_offset = offset;
}

static bool
ctrc_shared_header_exact(const ClusterCtrcSharedHeader *state,
						 const ClusterCtrcCapacity *capacity)
{
	ClusterCtrcSharedHeader expected;

	if (state == NULL || capacity == NULL)
		return false;
	MemSet(&expected, 0, sizeof(expected));
	ctrc_shared_fill_layout(&expected, capacity);
	return state->magic == CLUSTER_CTRC_SHMEM_MAGIC
		&& state->version == CLUSTER_CTRC_SHMEM_VERSION
		&& state->total_bytes == capacity->total_bytes
		&& state->origin_key_entries == capacity->origin_key_entries
		&& state->participant_key_entries
		   == capacity->participant_key_entries
		&& state->receipt_entries == capacity->receipt_entries
		&& state->participant_ack_summary_entries
		   == capacity->participant_ack_summary_entries
		&& state->origin_ack_inbox_entries
		   == capacity->origin_ack_inbox_entries
		&& state->origin_offset == expected.origin_offset
		&& state->participant_offset == expected.participant_offset
		&& state->receipt_offset == expected.receipt_offset
		&& state->receipt_probe_offset == expected.receipt_probe_offset
		&& state->participant_ack_offset == expected.participant_ack_offset
		&& state->origin_ack_offset == expected.origin_ack_offset
		&& state->global_grant_generation != 0
		&& state->reserved32 == 0
		&& state->global_reservation_generation != 0
		&& state->global_journal_generation != 0
		&& state->global_seal_generation != 0
		&& state->global_request_generation != 0
		&& ctrc_bytes_zero(state->reserved, sizeof(state->reserved));
}

void
cluster_ctrc_shmem_init(void)
{
	ClusterCtrcCapacity capacity;
	bool found;

	if (!ctrc_runtime_capacity(&capacity))
		ereport(FATAL,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("CTRC shared-memory capacity is invalid")));
	CtrcShared = (ClusterCtrcSharedHeader *)ShmemInitStruct(
		"pgrac cluster terminal reference census", capacity.total_bytes,
		&found);
	if (!found)
	{
		MemSet(&CtrcBatch, 0, sizeof(CtrcBatch));
		MemSet(CtrcOriginHint, 0, sizeof(CtrcOriginHint));
		CtrcDispatchCursor = 0;
		MemSet(CtrcShared, 0, capacity.total_bytes);
		CtrcShared->magic = CLUSTER_CTRC_SHMEM_MAGIC;
		CtrcShared->version = CLUSTER_CTRC_SHMEM_VERSION;
		CtrcShared->total_bytes = capacity.total_bytes;
		CtrcShared->origin_key_entries = capacity.origin_key_entries;
		CtrcShared->participant_key_entries
			= capacity.participant_key_entries;
		CtrcShared->receipt_entries = capacity.receipt_entries;
		CtrcShared->participant_ack_summary_entries
			= capacity.participant_ack_summary_entries;
		CtrcShared->origin_ack_inbox_entries
			= capacity.origin_ack_inbox_entries;
		SpinLockInit(&CtrcShared->origin_lock);
		SpinLockInit(&CtrcShared->participant_lock);
		SpinLockInit(&CtrcShared->receipt_lock);
		CtrcShared->global_grant_generation = 1;
		CtrcShared->global_reservation_generation = 1;
		CtrcShared->global_journal_generation = 1;
		CtrcShared->global_seal_generation = 1;
		CtrcShared->global_request_generation = 1;
		for (int i = 0; i < CTRC_STAT_COUNT; i++)
			pg_atomic_init_u64(&CtrcShared->stats[i], 0);
		pg_atomic_init_u64(&CtrcShared->test_barrier_hit_count, 0);
		pg_atomic_init_u32(&CtrcShared->test_barrier_phase,
			CTRC_TEST_BARRIER_NONE);
		pg_atomic_init_u32(&CtrcShared->cleaner_reason,
			CTRC_CLEANER_REASON_NONE);
		ctrc_shared_fill_layout(CtrcShared, &capacity);
	}
	if (!ctrc_shared_header_exact(CtrcShared, &capacity))
		ereport(FATAL,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("CTRC shared-memory header is invalid")));
}

bool
cluster_ctrc_shmem_ready(void)
{
	ClusterCtrcCapacity capacity;

	return CtrcShared != NULL && ctrc_runtime_capacity(&capacity)
		&& ctrc_shared_header_exact(CtrcShared, &capacity);
}

uint64
cluster_ctrc_stat_get(ClusterCtrcStatId stat)
{
	if (stat < 0 || stat >= CTRC_STAT_COUNT
		|| !ctrc_runtime_attached())
		return 0;
	if (stat == CTRC_STAT_OBSERVATION_AGE_MS) {
		uint64 sampled = pg_atomic_read_u64(&CtrcShared->stats[CTRC_STAT_OBSERVED_AT_US]);
		uint64 now = ctrc_monotonic_us();

		return sampled != 0 && now >= sampled ? (now - sampled) / 1000 : 0;
	}
	return pg_atomic_read_u64(&CtrcShared->stats[stat]);
}

void
cluster_ctrc_stat_bump(ClusterCtrcStatId stat)
{
	if (stat >= 0 && stat < CTRC_STAT_COUNT
		&& ctrc_runtime_attached())
		pg_atomic_fetch_add_u64(&CtrcShared->stats[stat], 1);
}

ClusterCtrcCleanerReason
cluster_ctrc_cleaner_reason_get(void)
{
	uint32 reason;

	if (!ctrc_runtime_attached())
		return CTRC_CLEANER_REASON_NONE;
	reason = pg_atomic_read_u32(&CtrcShared->cleaner_reason);
	return reason < CTRC_CLEANER_REASON_COUNT
		? (ClusterCtrcCleanerReason)reason : CTRC_CLEANER_REASON_BLOCKED;
}

void
cluster_ctrc_cleaner_reason_set(ClusterCtrcCleanerReason reason)
{
	if (reason >= 0 && reason < CTRC_CLEANER_REASON_COUNT
		&& ctrc_runtime_attached())
		pg_atomic_write_u32(&CtrcShared->cleaner_reason, (uint32)reason);
}

bool
cluster_ctrc_debug_snapshot(ClusterCtrcDebugSnapshot *snapshot)
{
	uint64 i;

	if (snapshot == NULL || !cluster_ctrc_shmem_ready())
		return false;
	MemSet(snapshot, 0, sizeof(*snapshot));
	SpinLockAcquire(&CtrcShared->origin_lock);
	for (i = 0; i < CtrcShared->origin_key_entries; i++)
	{
		switch ((ClusterCtrcOriginState)ctrc_origin_entries()[i].state)
		{
			case CTRC_ORIGIN_OPEN:
				snapshot->origin_open++;
				break;
			case CTRC_ORIGIN_SEALING:
			case CTRC_ORIGIN_SEALED:
			case CTRC_ORIGIN_CLEANING:
			case CTRC_ORIGIN_CERTIFYING:
				snapshot->origin_sealing++;
				break;
			case CTRC_ORIGIN_RELEASE_PROVEN:
				snapshot->origin_release_proven++;
				break;
			case CTRC_ORIGIN_BLOCKED:
				snapshot->origin_blocked++;
				break;
			default:
				break;
		}
	}
	snapshot->full_refusal_count = CtrcShared->full_refusal_count;
	SpinLockRelease(&CtrcShared->origin_lock);

	SpinLockAcquire(&CtrcShared->participant_lock);
	for (i = 0; i < CtrcShared->participant_key_entries; i++)
	{
		switch ((ClusterCtrcParticipantState)
			ctrc_participant_entries()[i].state)
		{
			case CTRC_PARTICIPANT_OPEN:
				snapshot->participant_open++;
				break;
			case CTRC_PARTICIPANT_CLOSED_DRAINING:
				snapshot->participant_draining++;
				break;
			case CTRC_PARTICIPANT_ACK_READY:
				snapshot->participant_ack_ready++;
				break;
			case CTRC_PARTICIPANT_ACK_FROZEN:
				snapshot->participant_ack_frozen++;
				break;
			case CTRC_PARTICIPANT_BLOCKED:
				snapshot->participant_blocked++;
				break;
			default:
				break;
		}
	}
	SpinLockRelease(&CtrcShared->participant_lock);

	SpinLockAcquire(&CtrcShared->receipt_lock);
	for (i = 0; i < CtrcShared->receipt_entries; i++)
	{
		uint32 state = pg_atomic_read_u32((pg_atomic_uint32 *)
			&ctrc_receipt_entries()[i].state);

		switch ((ClusterCtrcReceiptState)state)
		{
			case CTRC_RECEIPT_PREPARED:
				snapshot->receipt_prepared++;
				break;
			case CTRC_RECEIPT_APPLIED:
			case CTRC_RECEIPT_RETARGETING:
				snapshot->receipt_applied++;
				break;
			case CTRC_RECEIPT_CLEANED:
				snapshot->receipt_cleaned++;
				break;
			case CTRC_RECEIPT_CANCELLED:
				snapshot->receipt_cancelled++;
				break;
			case CTRC_RECEIPT_ACK_FROZEN:
				snapshot->receipt_ack_frozen++;
				break;
			case CTRC_RECEIPT_BLOCKED:
				snapshot->receipt_blocked++;
				break;
			default:
				break;
		}
	}
	SpinLockRelease(&CtrcShared->receipt_lock);
	snapshot->test_barrier_hit_count = pg_atomic_read_u64(
		&CtrcShared->test_barrier_hit_count);
	snapshot->test_barrier_phase = pg_atomic_read_u32(
		&CtrcShared->test_barrier_phase);
	snapshot->cleaner_reason = pg_atomic_read_u32(
		&CtrcShared->cleaner_reason);
	return snapshot->test_barrier_phase < CTRC_TEST_BARRIER_COUNT
		&& snapshot->cleaner_reason < CTRC_CLEANER_REASON_COUNT;
}

bool
cluster_ctrc_test_barrier_control(ClusterCtrcTestBarrierPhase phase,
	bool armed)
{
	if (!ctrc_runtime_attached())
		return false;
	if (armed)
	{
		if (phase <= CTRC_TEST_BARRIER_NONE
			|| phase >= CTRC_TEST_BARRIER_COUNT)
			return false;
		pg_atomic_write_u32(&CtrcShared->test_barrier_phase,
			(uint32)phase);
	}
	else
		pg_atomic_write_u32(&CtrcShared->test_barrier_phase,
			CTRC_TEST_BARRIER_NONE);
	return true;
}

void
cluster_ctrc_test_barrier_wait(ClusterCtrcTestBarrierPhase phase)
{
	if (phase <= CTRC_TEST_BARRIER_NONE
		|| phase >= CTRC_TEST_BARRIER_COUNT
		|| !ctrc_runtime_attached()
		|| pg_atomic_read_u32(&CtrcShared->test_barrier_phase)
		   != (uint32)phase)
		return;
	pg_atomic_fetch_add_u64(&CtrcShared->test_barrier_hit_count, 1);
	while (pg_atomic_read_u32(&CtrcShared->test_barrier_phase)
		   == (uint32)phase)
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L);
	}
}

void
cluster_ctrc_note_publication_after_apply(
	const ClusterCtrcReceiptHandle *handle, bool current_mx)
{
	bool exact = false;

	if (handle != NULL && handle->valid && ctrc_runtime_attached()
		&& handle->participant_index < CtrcShared->participant_key_entries
		&& handle->receipt_index < CtrcShared->receipt_entries
		&& handle->participant
		   == &ctrc_participant_entries()[handle->participant_index]
		&& handle->receipt == &ctrc_receipt_entries()[handle->receipt_index]
		&& handle->journal_slot_generation != 0
		&& handle->receipt->publication.journal_slot_generation
		   == handle->journal_slot_generation
		&& memcmp(&handle->key, &handle->receipt->key,
			   sizeof(handle->key)) == 0
		&& pg_atomic_read_u32((pg_atomic_uint32 *)&handle->receipt->state)
		   == CTRC_RECEIPT_APPLIED)
		exact = true;
	cluster_ctrc_stat_bump(exact
		? (current_mx
			? CTRC_STAT_CURRENT_MX_PUBLICATION_AFTER_APPLY
			: CTRC_STAT_ORDINARY_PUBLICATION_AFTER_APPLY)
		: CTRC_STAT_PUBLICATION_ORDER_VIOLATION);
}

static const ClusterShmemRegion cluster_ctrc_region = {
	.name = "pgrac cluster terminal reference census",
	.size_fn = cluster_ctrc_shmem_size,
	.init_fn = cluster_ctrc_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "cluster_ctrc",
	.reserved_flags = 0,
};

void
cluster_ctrc_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ctrc_region);
}

#else

Size
cluster_ctrc_shmem_size(void)
{
	return 0;
}

void
cluster_ctrc_shmem_init(void)
{
}

void
cluster_ctrc_shmem_register(void)
{
}

bool
cluster_ctrc_shmem_ready(void)
{
	return false;
}

uint64
cluster_ctrc_stat_get(ClusterCtrcStatId stat pg_attribute_unused())
{
	return 0;
}

void
cluster_ctrc_stat_bump(ClusterCtrcStatId stat pg_attribute_unused())
{
}

ClusterCtrcCleanerReason
cluster_ctrc_cleaner_reason_get(void)
{
	return CTRC_CLEANER_REASON_NONE;
}

void
cluster_ctrc_cleaner_reason_set(
	ClusterCtrcCleanerReason reason pg_attribute_unused())
{
}

bool
cluster_ctrc_debug_snapshot(ClusterCtrcDebugSnapshot *snapshot)
{
	if (snapshot != NULL)
		MemSet(snapshot, 0, sizeof(*snapshot));
	return false;
}

bool
cluster_ctrc_test_barrier_control(
	ClusterCtrcTestBarrierPhase phase pg_attribute_unused(),
	bool armed pg_attribute_unused())
{
	return false;
}

void
cluster_ctrc_test_barrier_wait(
	ClusterCtrcTestBarrierPhase phase pg_attribute_unused())
{
}

void
cluster_ctrc_note_publication_after_apply(
	const ClusterCtrcReceiptHandle *handle pg_attribute_unused(),
	bool current_mx pg_attribute_unused())
{
}

#endif

static bool
ctrc_allocate_journal_sequence(uint64 *sequence_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	bool allocated = false;

	if (sequence_out == NULL || !cluster_ctrc_shmem_ready())
		return false;
	*sequence_out = 0;
	SpinLockAcquire(&CtrcShared->origin_lock);
	if (CtrcShared->global_journal_generation != 0
		&& CtrcShared->global_journal_generation != UINT64_MAX)
	{
		*sequence_out = CtrcShared->global_journal_generation++;
		allocated = true;
	}
	else
		CtrcShared->full_refusal_count++;
	SpinLockRelease(&CtrcShared->origin_lock);
	return allocated;
#else
	static uint64 unit_journal_sequence = 1;

	if (sequence_out == NULL || unit_journal_sequence == UINT64_MAX)
		return false;
	*sequence_out = unit_journal_sequence++;
	return true;
#endif
}

ClusterCtrcOriginReserveResult
cluster_ctrc_origin_reserve_entry(
	ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	uint64 origin_index, uint64 reservation_generation,
	ClusterCtrcOriginReservation *reservation)
{
	ClusterCtrcOriginReserveResult result
		= CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;

	if (reservation != NULL)
		MemSet(reservation, 0, sizeof(*reservation));
	if (origin == NULL || reservation == NULL || !ctrc_txn_key_valid(key)
		|| reservation_generation == 0
		|| reservation_generation == UINT64_MAX)
		return result;

	if (origin->state == CTRC_ORIGIN_OPEN
		&& origin->reservation_generation != 0
		&& origin->grant_generation != 0
		&& memcmp(&origin->key, key, sizeof(*key)) == 0)
	{
		reservation_generation = origin->reservation_generation;
		result = CLUSTER_CTRC_ORIGIN_RESERVED_EXISTING_OPEN;
		reservation->kind = CTRC_ORIGIN_RESERVATION_EXISTING_OPEN;
	}
	else if (ctrc_bytes_zero(origin, sizeof(*origin)))
	{
		origin->key = *key;
		origin->reservation_generation = reservation_generation;
		result = CLUSTER_CTRC_ORIGIN_RESERVED_PENDING;
		reservation->kind = CTRC_ORIGIN_RESERVATION_PENDING_OWNED;
	}
	else if (origin->state == CTRC_ORIGIN_RELEASE_PROVEN
			 && memcmp(&origin->key, key, sizeof(*key)) != 0)
	{
		/* The canonical release certificate already permits the TT slot's
		 * next incarnation, but the old origin row may still be carrying only
		 * participant-summary notification state.  That continuation must
		 * remain byte-exact until its reply reclaims it. */
		return CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED;
	}
	else
	{
		/* A second owner for the same physical origin slot is an invariant
		 * conflict.  Retain it rather than allowing either claimant to reuse
		 * the entry. */
		origin->state = CTRC_ORIGIN_BLOCKED;
		return result;
	}

	reservation->key = *key;
	reservation->origin_index = origin_index;
	reservation->reservation_generation = reservation_generation;
	reservation->valid = true;
	return result;
}

static bool
ctrc_origin_reservation_exact(
	const ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	return origin != NULL && reservation != NULL && reservation->valid
		&& reservation->kind == CTRC_ORIGIN_RESERVATION_PENDING_OWNED
		&& reservation->origin_index == origin_index
		&& reservation->reservation_generation != 0
		&& reservation->reservation_generation != UINT64_MAX
		&& ctrc_bytes_zero(reservation->reserved8,
							  sizeof(reservation->reserved8))
		&& origin->reservation_generation
		   == reservation->reservation_generation
		&& memcmp(&origin->key, &reservation->key,
				  sizeof(reservation->key)) == 0;
}

bool
cluster_ctrc_origin_cancel_pre_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	if (!ctrc_origin_reservation_exact(origin, origin_index, reservation)
		|| origin->state != CTRC_ORIGIN_EMPTY
		|| origin->grant_generation != 0
		|| origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0
		|| origin->close_dispatched_bitmap != 0
		|| origin->close_confirmed_bitmap != 0 || origin->ack_bitmap != 0
		|| !ctrc_bytes_zero(origin->reserved8, sizeof(origin->reserved8))
		|| !ctrc_bytes_zero(origin->touched, sizeof(origin->touched))
		|| !ctrc_bytes_zero(origin->close_request_id,
						   sizeof(origin->close_request_id)))
		return false;
	MemSet(origin, 0, sizeof(*origin));
	return true;
}

bool
cluster_ctrc_origin_block_post_bind_entry(
	ClusterCtrcOriginEntry *origin, uint64 origin_index,
	const ClusterCtrcOriginReservation *reservation)
{
	if (!ctrc_origin_reservation_exact(origin, origin_index, reservation))
		return false;
	if (origin->state == CTRC_ORIGIN_BLOCKED)
		return true;
	if ((origin->state != CTRC_ORIGIN_EMPTY
		 && origin->state != CTRC_ORIGIN_OPEN)
		 || origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0
		|| origin->close_dispatched_bitmap != 0
		|| origin->close_confirmed_bitmap != 0 || origin->ack_bitmap != 0
		|| !ctrc_bytes_zero(origin->close_request_id,
						   sizeof(origin->close_request_id)))
		return false;
	origin->state = CTRC_ORIGIN_BLOCKED;
	return true;
}

ClusterCtrcOriginReserveResult
cluster_ctrc_origin_reserve_active(const ClusterCtrcTxnKeyV1 *key,
									   ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 index;
	uint64 reservation_generation;
	ClusterCtrcOriginReserveResult result;

	if (reservation == NULL)
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
	MemSet(reservation, 0, sizeof(*reservation));
	if (!cluster_ctrc_shmem_ready() || !ctrc_origin_index(key, &index))
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (origin->state == CTRC_ORIGIN_RELEASE_PROVEN
		&& memcmp(&origin->key, key, sizeof(*key)) != 0)
	{
		SpinLockRelease(&CtrcShared->origin_lock);
		return CLUSTER_CTRC_ORIGIN_RESERVE_RETRY_RELEASED;
	}
	if (origin->state == CTRC_ORIGIN_OPEN)
		reservation_generation = origin->reservation_generation;
	else if (CtrcShared->global_reservation_generation != 0
			 && CtrcShared->global_reservation_generation != UINT64_MAX)
		reservation_generation
			= CtrcShared->global_reservation_generation++;
	else
	{
		CtrcShared->full_refusal_count++;
		SpinLockRelease(&CtrcShared->origin_lock);
		return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
	}
	result = cluster_ctrc_origin_reserve_entry(origin, key, index,
		reservation_generation, reservation);
	if (result == CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED)
		CtrcShared->full_refusal_count++;
	SpinLockRelease(&CtrcShared->origin_lock);
	return result;
#else
	(void)key;
	if (reservation != NULL)
		MemSet(reservation, 0, sizeof(*reservation));
	return CLUSTER_CTRC_ORIGIN_RESERVE_REFUSED;
#endif
}

/* A released predecessor can retain the one row for this physical TT slot
 * only while its participant-summary notification is in flight.  ACTIVE
 * publication uses this read-only probe after dropping block-0 authority, so
 * waiting for that continuation never becomes a Resource-X acquire/release
 * loop.  Any state other than the exact released overlap is revalidated by
 * the normal reservation path under a freshly rebuilt canonical snapshot. */
bool
cluster_ctrc_origin_release_overlap_pending(const ClusterCtrcTxnKeyV1 *key)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 index;
	bool pending;

	if (!cluster_ctrc_shmem_ready() || !ctrc_origin_index(key, &index))
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	pending = origin->state == CTRC_ORIGIN_RELEASE_PROVEN
		&& memcmp(&origin->key, key, sizeof(*key)) != 0;
	SpinLockRelease(&CtrcShared->origin_lock);
	return pending;
#else
	(void)key;
	return false;
#endif
}

bool
cluster_ctrc_origin_cancel_pre_bind(
	const ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	uint64 index;
	bool cancelled;

	if (reservation == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	cancelled = cluster_ctrc_origin_cancel_pre_bind_entry(
		&ctrc_origin_entries()[index], index, reservation);
	SpinLockRelease(&CtrcShared->origin_lock);
	return cancelled;
#else
	(void)reservation;
	return false;
#endif
}

bool
cluster_ctrc_origin_block_post_bind(
	const ClusterCtrcOriginReservation *reservation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	uint64 index;
	bool blocked;

	if (reservation == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	blocked = cluster_ctrc_origin_block_post_bind_entry(
		&ctrc_origin_entries()[index], index, reservation);
	SpinLockRelease(&CtrcShared->origin_lock);
	return blocked;
#else
	(void)reservation;
	return false;
#endif
}

bool
cluster_ctrc_origin_open_reserved(
	const ClusterCtrcOriginReservation *reservation,
	uint32 *grant_generation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	ClusterCtrcOriginOpenResult result;
	uint64 index;
	uint32 grant;
	bool opened = false;
	bool issued = false;

	if (grant_generation != NULL)
		*grant_generation = 0;
	if (reservation == NULL || grant_generation == NULL
		|| !reservation->valid
		|| (reservation->kind != CTRC_ORIGIN_RESERVATION_PENDING_OWNED
			&& reservation->kind != CTRC_ORIGIN_RESERVATION_EXISTING_OPEN)
		|| reservation->reservation_generation == 0
		|| !ctrc_bytes_zero(reservation->reserved8,
						   sizeof(reservation->reserved8))
		|| !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&reservation->key, &index)
		|| index != reservation->origin_index)
		return false;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (origin->state == CTRC_ORIGIN_OPEN
		&& origin->reservation_generation
		   == reservation->reservation_generation
		&& origin->grant_generation != 0
		&& memcmp(&origin->key, &reservation->key,
				  sizeof(reservation->key)) == 0)
	{
		grant = origin->grant_generation;
		opened = true;
	}
	else if (origin->state == CTRC_ORIGIN_EMPTY
			 && reservation->kind
				== CTRC_ORIGIN_RESERVATION_PENDING_OWNED
			 && origin->reservation_generation
				== reservation->reservation_generation
			 && origin->grant_generation == 0
			 && memcmp(&origin->key, &reservation->key,
					   sizeof(reservation->key)) == 0
			 && CtrcShared->global_grant_generation != UINT32_MAX)
	{
		grant = CtrcShared->global_grant_generation++;
		result = cluster_ctrc_origin_open_active(
			origin, &reservation->key, grant);
		opened = result == CLUSTER_CTRC_ORIGIN_OPENED
			|| result == CLUSTER_CTRC_ORIGIN_DUPLICATE;
		issued = result == CLUSTER_CTRC_ORIGIN_OPENED;
	}
	else
		CtrcShared->full_refusal_count++;
	if (opened)
		*grant_generation = origin->grant_generation;
	SpinLockRelease(&CtrcShared->origin_lock);
	if (issued)
		cluster_ctrc_stat_bump(CTRC_STAT_GRANT_ISSUED);
	else if (!opened)
		cluster_ctrc_stat_bump(CTRC_STAT_GRANT_REFUSED);
	return opened;
#else
	(void)reservation;
	if (grant_generation != NULL)
		*grant_generation = 0;
	return false;
#endif
}

ClusterCtrcTouchResult
cluster_ctrc_origin_touch_exact(const ClusterCtrcTxnKeyV1 *key,
								const ClusterCtrcParticipantIdentity *participant,
								ClusterCtrcProofClass proof_class,
								uint32 *grant_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcTouchResult result;
	ClusterCtrcOriginEntry *origin;
	uint64 index;

	if (grant_out != NULL)
		*grant_out = 0;
	if (proof_class == CTRC_PROOF_UNKNOWN
		|| proof_class == CTRC_PROOF_COMMITTED
		|| proof_class == CTRC_PROOF_ABORTED)
		return CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT;
	if (grant_out == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(key, &index))
		return CLUSTER_CTRC_TOUCH_REFUSED;

	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (memcmp(&origin->key, key, sizeof(*key)) != 0)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		CtrcShared->full_refusal_count++;
		result = CLUSTER_CTRC_TOUCH_REFUSED;
	}
	else
		result = cluster_ctrc_origin_record_touched(
			origin, participant, proof_class, grant_out);
	SpinLockRelease(&CtrcShared->origin_lock);
	if (result == CLUSTER_CTRC_TOUCH_REFUSED)
		cluster_ctrc_stat_bump(CTRC_STAT_GRANT_REFUSED);
	return result;
#else
	(void)key;
	(void)participant;
	(void)proof_class;
	if (grant_out != NULL)
		*grant_out = 0;
	return CLUSTER_CTRC_TOUCH_REFUSED;
#endif
}

/* Tasks 3-8 replace these fail-closed transition bodies in protocol order.
 * Keeping every entry point closed now prevents partial Task-2 storage from
 * becoming a second transaction authority. */
ClusterCtrcOriginOpenResult
cluster_ctrc_origin_open_active(ClusterCtrcOriginEntry *origin,
								const ClusterCtrcTxnKeyV1 *key,
								uint32 grant_generation)
{
	if (origin == NULL || !ctrc_txn_key_valid(key) || grant_generation == 0)
		return CLUSTER_CTRC_ORIGIN_REFUSED;
	if (origin->state == CTRC_ORIGIN_OPEN
		&& memcmp(&origin->key, key, sizeof(*key)) == 0
		&& origin->grant_generation == grant_generation)
		return CLUSTER_CTRC_ORIGIN_DUPLICATE;
	if (origin->state != CTRC_ORIGIN_EMPTY
		|| (!ctrc_bytes_zero(&origin->key, sizeof(origin->key))
			&& memcmp(&origin->key, key, sizeof(*key)) != 0)
		|| origin->grant_generation != 0
		|| origin->touched_bitmap != 0 || origin->touched_count != 0
		|| origin->seal_generation != 0
		|| origin->close_dispatched_bitmap != 0
		|| origin->close_confirmed_bitmap != 0 || origin->ack_bitmap != 0
		|| !ctrc_bytes_zero(origin->reserved8, sizeof(origin->reserved8))
		|| !ctrc_bytes_zero(origin->touched, sizeof(origin->touched))
		|| !ctrc_bytes_zero(origin->close_request_id,
						   sizeof(origin->close_request_id)))
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_ORIGIN_REFUSED;
	}

	origin->key = *key;
	origin->grant_generation = grant_generation;
	origin->state = CTRC_ORIGIN_OPEN;
	return CLUSTER_CTRC_ORIGIN_OPENED;
}

ClusterCtrcTouchResult
cluster_ctrc_origin_record_touched(ClusterCtrcOriginEntry *origin,
								   const ClusterCtrcParticipantIdentity *participant,
								   ClusterCtrcProofClass proof_class,
								   uint32 *grant_out)
{
	if (grant_out != NULL)
		*grant_out = 0;
	if (proof_class == CTRC_PROOF_UNKNOWN
		|| proof_class == CTRC_PROOF_COMMITTED
		|| proof_class == CTRC_PROOF_ABORTED)
		return CLUSTER_CTRC_TOUCH_TERMINAL_NO_GRANT;
	if (origin == NULL || grant_out == NULL
		|| (proof_class != CTRC_PROOF_SELF
			&& proof_class != CTRC_PROOF_ACTIVE)
		|| origin->state != CTRC_ORIGIN_OPEN
		|| origin->grant_generation == 0
		|| !ctrc_txn_key_valid(&origin->key)
		|| !ctrc_participant_identity_valid(&origin->key, participant))
		return CLUSTER_CTRC_TOUCH_REFUSED;

	if ((origin->touched_bitmap & (UINT32_C(1) << participant->node_id)) != 0)
	{
		if (memcmp(&origin->touched[participant->node_id], participant,
				   sizeof(*participant)) == 0)
		{
			*grant_out = origin->grant_generation;
			return CLUSTER_CTRC_TOUCH_DUPLICATE;
		}
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_TOUCH_REFUSED;
	}
	if (origin->touched_count >= CLUSTER_CTRC_MAX_PARTICIPANTS)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return CLUSTER_CTRC_TOUCH_REFUSED;
	}

	origin->touched[participant->node_id] = *participant;
	origin->touched_bitmap |= UINT32_C(1) << participant->node_id;
	origin->touched_count++;
	*grant_out = origin->grant_generation;
	return CLUSTER_CTRC_TOUCH_RECORDED;
}

bool
cluster_ctrc_origin_has_exact_touch(const ClusterCtrcOriginEntry *origin,
									const ClusterCtrcParticipantIdentity *participant)
{
	return origin != NULL && participant != NULL
		&& participant->node_id < CLUSTER_CTRC_MAX_PARTICIPANTS
		&& (origin->touched_bitmap
			& (UINT32_C(1) << participant->node_id)) != 0
		&& memcmp(&origin->touched[participant->node_id], participant,
				  sizeof(*participant)) == 0;
}

/* A positive ACTIVE/SELF proof is publishable only while the same origin row
 * is still OPEN under the exact grant and participant identity that created
 * it.  This pure predicate is also the deterministic delayed-send oracle. */
bool
cluster_ctrc_origin_grant_publishable_entry(
	const ClusterCtrcOriginEntry *origin, const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	uint32 grant_generation)
{
	return origin != NULL && key != NULL && participant != NULL
		&& grant_generation != 0
		&& origin->state == CTRC_ORIGIN_OPEN
		&& origin->grant_generation == grant_generation
		&& memcmp(&origin->key, key, sizeof(*key)) == 0
		&& cluster_ctrc_origin_has_exact_touch(origin, participant);
}

bool
cluster_ctrc_origin_grant_publishable(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *participant,
	uint32 grant_generation)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 index;
	bool publishable;

	if (key == NULL || participant == NULL || grant_generation == 0
		|| !cluster_ctrc_shmem_ready() || !ctrc_origin_index(key, &index))
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	publishable = cluster_ctrc_origin_grant_publishable_entry(
		origin, key, participant, grant_generation);
	SpinLockRelease(&CtrcShared->origin_lock);
	return publishable;
#else
	(void)key;
	(void)participant;
	(void)grant_generation;
	return false;
#endif
}

static bool
ctrc_origin_frozen_touch_set_valid(const ClusterCtrcOriginEntry *origin)
{
	uint32 allowed = (UINT32_C(1) << CLUSTER_CTRC_MAX_PARTICIPANTS) - 1;
	uint8 count = 0;
	uint16 node_id;

	if (origin == NULL || (origin->touched_bitmap & ~allowed) != 0)
		return false;
	for (node_id = 0; node_id < CLUSTER_CTRC_MAX_PARTICIPANTS; node_id++)
	{
		uint32 bit = UINT32_C(1) << node_id;

		if ((origin->touched_bitmap & bit) != 0)
		{
			if (!ctrc_participant_identity_valid(&origin->key,
					&origin->touched[node_id])
				|| origin->touched[node_id].node_id != node_id)
				return false;
			count++;
		}
		else if (!ctrc_bytes_zero(&origin->touched[node_id],
				sizeof(origin->touched[node_id])))
			return false;
	}
	return count == origin->touched_count;
}

bool
cluster_ctrc_origin_begin_seal_entry(ClusterCtrcOriginEntry *origin,
									 uint64 seal_generation)
{
	if (origin == NULL || seal_generation == 0
		|| seal_generation == UINT64_MAX)
		return false;
	if (origin->state != CTRC_ORIGIN_OPEN)
		return origin->state >= CTRC_ORIGIN_SEALING
			&& origin->state <= CTRC_ORIGIN_RELEASE_PROVEN
			&& origin->seal_generation == seal_generation;
	if (!ctrc_txn_key_valid(&origin->key)
		|| origin->grant_generation == 0
		|| origin->seal_generation != 0
		|| origin->close_dispatched_bitmap != 0
		|| origin->close_confirmed_bitmap != 0
		|| origin->ack_bitmap != 0
		|| !ctrc_bytes_zero(origin->close_request_id,
							   sizeof(origin->close_request_id))
		|| !ctrc_origin_frozen_touch_set_valid(origin))
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	origin->seal_generation = seal_generation;
	origin->state = CTRC_ORIGIN_SEALING;
	return true;
}

bool
cluster_ctrc_origin_arm_close_entry(ClusterCtrcOriginEntry *origin,
									uint16 participant_node_id,
									uint64 request_id)
{
	uint32 bit;

	if (origin == NULL || request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| origin->state < CTRC_ORIGIN_SEALING
		|| origin->state > CTRC_ORIGIN_CLEANING
		|| origin->seal_generation == 0)
		return false;
	bit = UINT32_C(1) << participant_node_id;
	if ((origin->touched_bitmap & bit) == 0)
		return false;
	if ((origin->close_dispatched_bitmap & bit) != 0)
	{
		if (origin->close_request_id[participant_node_id] == request_id)
			return true;
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	if (origin->close_request_id[participant_node_id] != 0)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	origin->close_request_id[participant_node_id] = request_id;
	pg_write_barrier();
	origin->close_dispatched_bitmap |= bit;
	return true;
}

bool
cluster_ctrc_origin_note_close_reply_entry(ClusterCtrcOriginEntry *origin,
									   uint16 participant_node_id,
									   uint64 request_id,
									   ClusterCtrcSealReplyResult result)
{
	uint32 bit;

	if (origin == NULL || request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| origin->state < CTRC_ORIGIN_SEALING
		|| origin->state > CTRC_ORIGIN_CLEANING)
		return false;
	bit = UINT32_C(1) << participant_node_id;
	if ((origin->touched_bitmap & bit) == 0
		|| (origin->close_dispatched_bitmap & bit) == 0
		|| origin->close_request_id[participant_node_id] != request_id)
		return false;
	if (result != CTRC_SEAL_REPLY_PENDING_DRAIN
		&& result != CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	origin->close_confirmed_bitmap |= bit;
	if (origin->state == CTRC_ORIGIN_SEALING
		&& origin->close_confirmed_bitmap == origin->touched_bitmap)
		origin->state = CTRC_ORIGIN_SEALED;
	return true;
}

bool
cluster_ctrc_origin_arm_certificate_entry(ClusterCtrcOriginEntry *origin,
									  uint16 participant_node_id,
									  uint64 request_id)
{
	uint32 bit;

	if (origin == NULL || request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| origin->state != CTRC_ORIGIN_RELEASE_PROVEN
		|| !ctrc_txn_key_valid(&origin->key)
		|| origin->grant_generation == 0
		|| origin->seal_generation == 0
		|| origin->ack_bitmap != origin->touched_bitmap
		|| !ctrc_origin_frozen_touch_set_valid(origin)
		|| (origin->close_dispatched_bitmap & ~origin->touched_bitmap) != 0
		|| (origin->close_confirmed_bitmap
			& ~origin->close_dispatched_bitmap) != 0)
		return false;
	bit = UINT32_C(1) << participant_node_id;
	if ((origin->touched_bitmap & bit) == 0)
		return false;
	if ((origin->close_dispatched_bitmap & bit) != 0)
	{
		if (origin->close_request_id[participant_node_id] == request_id)
			return true;
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	if (origin->close_request_id[participant_node_id] != 0)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	origin->close_request_id[participant_node_id] = request_id;
	pg_write_barrier();
	origin->close_dispatched_bitmap |= bit;
	return true;
}

bool
cluster_ctrc_origin_note_certificate_reply_entry(
	ClusterCtrcOriginEntry *origin, uint16 participant_node_id,
	uint64 request_id, ClusterCtrcSealReplyResult result)
{
	uint32 bit;

	if (origin == NULL || request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| origin->state != CTRC_ORIGIN_RELEASE_PROVEN
		|| result != CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED)
		return false;
	bit = UINT32_C(1) << participant_node_id;
	if ((origin->touched_bitmap & bit) == 0
		|| (origin->close_dispatched_bitmap & bit) == 0
		|| origin->close_request_id[participant_node_id] != request_id)
		return false;
	origin->close_confirmed_bitmap |= bit;
	return true;
}

bool
cluster_ctrc_origin_begin_cleaning_entry(ClusterCtrcOriginEntry *origin)
{
	if (origin == NULL || origin->seal_generation == 0)
		return false;
	if (origin->state == CTRC_ORIGIN_SEALING
		&& origin->touched_bitmap == 0
		&& origin->close_dispatched_bitmap == 0
		&& origin->close_confirmed_bitmap == 0)
		origin->state = CTRC_ORIGIN_SEALED;
	if (origin->state == CTRC_ORIGIN_CLEANING
		|| origin->state == CTRC_ORIGIN_CERTIFYING)
		return true;
	if (origin->state != CTRC_ORIGIN_SEALED
		|| origin->close_dispatched_bitmap != origin->touched_bitmap
		|| origin->close_confirmed_bitmap != origin->touched_bitmap)
		return false;
	origin->state = CTRC_ORIGIN_CLEANING;
	return true;
}

bool
cluster_ctrc_origin_request_snapshot_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcTxnKeyV1 *key_out,
	ClusterCtrcParticipantIdentity *identity_out,
	uint32 *grant_generation_out, uint64 *seal_generation_out,
	ClusterCtrcSealSuboperation *suboperation_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *matched = NULL;
	ClusterCtrcSealSuboperation suboperation = 0;
	uint64 i;
	uint32 bit;

	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (identity_out != NULL)
		MemSet(identity_out, 0, sizeof(*identity_out));
	if (grant_generation_out != NULL)
		*grant_generation_out = 0;
	if (seal_generation_out != NULL)
		*seal_generation_out = 0;
	if (suboperation_out != NULL)
		*suboperation_out = 0;
	if (request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| key_out == NULL || identity_out == NULL
		|| grant_generation_out == NULL || seal_generation_out == NULL
		|| suboperation_out == NULL
		|| !cluster_ctrc_shmem_ready())
		return false;
	bit = UINT32_C(1) << participant_node_id;

	SpinLockAcquire(&CtrcShared->origin_lock);
	for (i = 0; i < CtrcShared->origin_key_entries; i++)
	{
		ClusterCtrcOriginEntry *candidate = &ctrc_origin_entries()[i];

		if ((candidate->close_dispatched_bitmap & bit) == 0
			|| candidate->close_request_id[participant_node_id]
			   != request_id)
			continue;
		if (matched != NULL)
		{
			matched = NULL;
			break;
		}
		matched = candidate;
	}
	if (matched != NULL
		&& matched->seal_generation != 0
		&& (matched->touched_bitmap & bit) != 0
		&& ctrc_origin_frozen_touch_set_valid(matched))
	{
		if (matched->state >= CTRC_ORIGIN_SEALING
			&& matched->state <= CTRC_ORIGIN_CLEANING)
			suboperation = CTRC_SEAL_CLOSE_AND_CLEAN;
		else if (matched->state == CTRC_ORIGIN_RELEASE_PROVEN)
			suboperation = CTRC_SEAL_CERTIFICATE_COMMITTED;
		if (suboperation != 0)
		{
			*key_out = matched->key;
			*identity_out = matched->touched[participant_node_id];
			*grant_generation_out = matched->grant_generation;
			*seal_generation_out = matched->seal_generation;
			*suboperation_out = suboperation;
		}
	}
	SpinLockRelease(&CtrcShared->origin_lock);
	return matched != NULL && *grant_generation_out != 0
		&& *seal_generation_out != 0 && *suboperation_out != 0;
#else
	(void)request_id;
	(void)participant_node_id;
	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (identity_out != NULL)
		MemSet(identity_out, 0, sizeof(*identity_out));
	if (grant_generation_out != NULL)
		*grant_generation_out = 0;
	if (seal_generation_out != NULL)
		*seal_generation_out = 0;
	if (suboperation_out != NULL)
		*suboperation_out = 0;
	return false;
#endif
}

bool
cluster_ctrc_origin_close_request_snapshot_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcTxnKeyV1 *key_out,
	ClusterCtrcParticipantIdentity *identity_out,
	uint32 *grant_generation_out, uint64 *seal_generation_out)
{
	ClusterCtrcSealSuboperation suboperation = 0;
	bool found;

	found = cluster_ctrc_origin_request_snapshot_shared(request_id,
		participant_node_id, key_out, identity_out, grant_generation_out,
		seal_generation_out, &suboperation);
	if (found && suboperation == CTRC_SEAL_CLOSE_AND_CLEAN)
		return true;
	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (identity_out != NULL)
		MemSet(identity_out, 0, sizeof(*identity_out));
	if (grant_generation_out != NULL)
		*grant_generation_out = 0;
	if (seal_generation_out != NULL)
		*seal_generation_out = 0;
	return false;
}

bool
cluster_ctrc_origin_note_close_reply_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcSealReplyResult result)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *matched = NULL;
	bool noted = false;
	bool progressed = false;
	uint64 i;
	uint32 bit;

	if (request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| !cluster_ctrc_shmem_ready())
		return false;
	bit = UINT32_C(1) << participant_node_id;
	SpinLockAcquire(&CtrcShared->origin_lock);
	for (i = 0; i < CtrcShared->origin_key_entries; i++)
	{
		ClusterCtrcOriginEntry *candidate = &ctrc_origin_entries()[i];

		if ((candidate->close_dispatched_bitmap & bit) == 0
			|| candidate->close_request_id[participant_node_id]
			   != request_id)
			continue;
		if (matched != NULL)
		{
			matched->state = CTRC_ORIGIN_BLOCKED;
			candidate->state = CTRC_ORIGIN_BLOCKED;
			matched = NULL;
			break;
		}
		matched = candidate;
	}
	if (matched != NULL) {
		uint32 confirmed_before = matched->close_confirmed_bitmap;
		uint8 state_before = matched->state;

		noted = cluster_ctrc_origin_note_close_reply_entry(
			matched, participant_node_id, request_id, result);
		progressed = noted
					 && (confirmed_before != matched->close_confirmed_bitmap
						 || state_before != matched->state);
	}
	SpinLockRelease(&CtrcShared->origin_lock);
	if (progressed)
		ctrc_semantic_progress(true);
	else if (noted && result == CTRC_SEAL_REPLY_PENDING_DRAIN)
		cluster_ctrc_stat_bump(CTRC_STAT_PENDING_DUPLICATE);
	return noted;
#else
	(void)request_id;
	(void)participant_node_id;
	(void)result;
	return false;
#endif
}

bool
cluster_ctrc_origin_note_certificate_reply_shared(
	uint64 request_id, uint16 participant_node_id,
	ClusterCtrcSealReplyResult result)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *matched = NULL;
	uint64 matched_index = UINT64_MAX;
	bool noted = false;
	bool progressed = false;
	uint64 i;
	uint32 bit;

	if (request_id == 0
		|| participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| !cluster_ctrc_shmem_ready())
		return false;
	bit = UINT32_C(1) << participant_node_id;
	SpinLockAcquire(&CtrcShared->origin_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	for (i = 0; i < CtrcShared->origin_key_entries; i++)
	{
		ClusterCtrcOriginEntry *candidate = &ctrc_origin_entries()[i];

		if ((candidate->close_dispatched_bitmap & bit) == 0
			|| candidate->close_request_id[participant_node_id]
			   != request_id)
			continue;
		if (matched != NULL)
		{
			matched->state = CTRC_ORIGIN_BLOCKED;
			candidate->state = CTRC_ORIGIN_BLOCKED;
			matched = NULL;
			matched_index = UINT64_MAX;
			break;
		}
		matched = candidate;
		matched_index = i;
	}
	if (matched != NULL)
	{
		uint32 confirmed_before = matched->close_confirmed_bitmap;

		noted = cluster_ctrc_origin_note_certificate_reply_entry(
			matched, participant_node_id, request_id, result);
		progressed = noted && confirmed_before != matched->close_confirmed_bitmap;
		if (noted
			&& matched->close_confirmed_bitmap == matched->touched_bitmap)
		{
			noted = ctrc_origin_release_reclaim_locked(matched_index);
			if (!noted && matched->state == CTRC_ORIGIN_RELEASE_PROVEN)
				matched->state = CTRC_ORIGIN_BLOCKED;
		}
	}
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->origin_lock);
	if (noted && progressed)
		ctrc_semantic_progress(true);
	return noted;
#else
	(void)request_id;
	(void)participant_node_id;
	(void)result;
	return false;
#endif
}

bool
cluster_ctrc_origin_next_open_shared(ClusterCtrcTxnKeyV1 *key_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	static uint64 scan_cursor = 0;
	uint64 i;
	uint64 limit;
	bool found = false;

	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	if (key_out == NULL || cluster_node_id < 0
		|| !cluster_ctrc_shmem_ready())
		return false;
	limit = ctrc_scan_limit(CTRC_SCAN_OPEN, CtrcShared->origin_key_entries);
	SpinLockAcquire(&CtrcShared->origin_lock);
	for (i = 0; i < limit; i++) {
		uint64 index = (scan_cursor + i) % CtrcShared->origin_key_entries;
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];

		if (origin->state != CTRC_ORIGIN_OPEN
			|| origin->key.origin_node_id != (uint16)cluster_node_id)
			continue;
		if (!ctrc_txn_key_valid(&origin->key)
			|| origin->grant_generation == 0
			|| !ctrc_origin_frozen_touch_set_valid(origin))
		{
			origin->state = CTRC_ORIGIN_BLOCKED;
			continue;
		}
		*key_out = origin->key;
		scan_cursor = (index + 1) % CtrcShared->origin_key_entries;
		found = true;
		break;
	}
	SpinLockRelease(&CtrcShared->origin_lock);
	ctrc_scan_charge(CTRC_SCAN_OPEN, i + (found ? 1 : 0));
	return found;
#else
	if (key_out != NULL)
		MemSet(key_out, 0, sizeof(*key_out));
	return false;
#endif
}

bool
cluster_ctrc_origin_begin_seal_shared(const ClusterCtrcTxnKeyV1 *key)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 index;
	uint64 seal_generation;
	bool sealed;
	bool started = false;

	if (key == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(key, &index))
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	origin = &ctrc_origin_entries()[index];
	if (memcmp(&origin->key, key, sizeof(*key)) != 0)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		SpinLockRelease(&CtrcShared->origin_lock);
		return false;
	}
	if (origin->state != CTRC_ORIGIN_OPEN)
	{
		sealed = origin->state >= CTRC_ORIGIN_SEALING
			&& origin->state <= CTRC_ORIGIN_RELEASE_PROVEN
			&& origin->seal_generation != 0;
		SpinLockRelease(&CtrcShared->origin_lock);
		return sealed;
	}
	if (CtrcShared->global_seal_generation == 0
		|| CtrcShared->global_seal_generation == UINT64_MAX)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		CtrcShared->full_refusal_count++;
		SpinLockRelease(&CtrcShared->origin_lock);
		return false;
	}
	seal_generation = CtrcShared->global_seal_generation++;
	sealed = cluster_ctrc_origin_begin_seal_entry(origin, seal_generation);
	started = sealed;
	SpinLockRelease(&CtrcShared->origin_lock);
	if (started)
		cluster_ctrc_stat_bump(CTRC_STAT_SEAL_STARTED);
	else
		cluster_ctrc_stat_bump(CTRC_STAT_SEAL_BLOCKED);
	return sealed;
#else
	(void)key;
	return false;
#endif
}

#ifndef CLUSTER_CTRC_UNIT_TEST
/* Advance only the existing local FSM edges. Selection is not permission to
 * clean a page, publish a certificate, or recycle a slot. */
static bool
ctrc_origin_dispatchable_locked(ClusterCtrcOriginEntry *origin)
{
	uint8 before = origin->state;

	if (cluster_node_id < 0 || origin->key.origin_node_id != (uint16)cluster_node_id)
		return false;
	if (origin->state == CTRC_ORIGIN_SEALED
		|| (origin->state == CTRC_ORIGIN_SEALING && origin->touched_bitmap == 0))
		(void)cluster_ctrc_origin_begin_cleaning_entry(origin);
	if (origin->state == CTRC_ORIGIN_CLEANING && origin->ack_bitmap == origin->touched_bitmap)
		origin->state = CTRC_ORIGIN_CERTIFYING;
	if (before != origin->state)
		ctrc_semantic_progress(false);
	if (origin->state == CTRC_ORIGIN_RELEASE_PROVEN)
		return (origin->touched_bitmap & ~origin->close_confirmed_bitmap) != 0;
	return (origin->state == CTRC_ORIGIN_SEALING || origin->state == CTRC_ORIGIN_CLEANING)
		   && (origin->touched_bitmap & ~origin->ack_bitmap) != 0;
}

static bool
ctrc_dispatch_batch_contains(uint64 index)
{
	unsigned i;

	for (i = 0; i < CtrcBatch.dispatch_count; i++)
		if (CtrcBatch.dispatch_index[i] == index)
			return true;
	return false;
}

/* One census, at most 32 oldest origins plus the remaining round-robin
 * share. Only indices are retained; arm re-reads the real identity under its
 * original lock. No shared state, receipt or request is pre-reserved here. */
static void
ctrc_dispatch_batch_collect(void)
{
	uint64 old_index[CTRC_CLEANER_OLD_BATCH];
	uint64 old_seal[CTRC_CLEANER_OLD_BATCH];
	unsigned old_count = 0;
	uint64 pending = 0;
	uint64 certifying = 0;
	uint64 oldest_age_us = 0;
	uint64 now = ctrc_monotonic_us();
	uint64 i;
	uint64 start;

	CtrcBatch.dispatch_count = 0;
	CtrcBatch.dispatch_next = 0;
	if (!cluster_ctrc_shmem_ready())
		return;
	Assert(CtrcShared->origin_key_entries == lengthof(CtrcOriginHint));
	SpinLockAcquire(&CtrcShared->origin_lock);
	for (i = 0; i < CtrcShared->origin_key_entries; i++) {
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[i];
		CtrcCleanerOriginHint *hint = &CtrcOriginHint[i];
		bool eligible = ctrc_origin_dispatchable_locked(origin);
		bool certificate = origin->state == CTRC_ORIGIN_CERTIFYING;
		unsigned pos;

		if (certificate)
			certifying++;
		if (eligible || certificate) {
			if (hint->seal_generation != origin->seal_generation || hint->first_observed_us == 0
				|| now < hint->first_observed_us) {
				hint->seal_generation = origin->seal_generation;
				hint->first_observed_us = now;
			}
			oldest_age_us = Max(oldest_age_us, now - hint->first_observed_us);
		} else {
			hint->seal_generation = 0;
			hint->first_observed_us = 0;
		}
		if (!eligible)
			continue;
		pending++;
		for (pos = 0; pos < old_count; pos++)
			if (origin->seal_generation < old_seal[pos])
				break;
		if (pos >= CTRC_CLEANER_OLD_BATCH)
			continue;
		if (old_count < CTRC_CLEANER_OLD_BATCH)
			old_count++;
		for (unsigned move = old_count - 1; move > pos; move--) {
			old_index[move] = old_index[move - 1];
			old_seal[move] = old_seal[move - 1];
		}
		old_index[pos] = i;
		old_seal[pos] = origin->seal_generation;
	}
	for (unsigned pos = 0; pos < old_count; pos++)
		CtrcBatch.dispatch_index[CtrcBatch.dispatch_count++] = old_index[pos];
	start = CtrcDispatchCursor % CtrcShared->origin_key_entries;
	for (i = 0; i < CtrcShared->origin_key_entries
				&& CtrcBatch.dispatch_count < CTRC_CLEANER_DISPATCH_BATCH;
		 i++) {
		uint64 index = (start + i) % CtrcShared->origin_key_entries;

		if (ctrc_dispatch_batch_contains(index)
			|| !ctrc_origin_dispatchable_locked(&ctrc_origin_entries()[index]))
			continue;
		CtrcBatch.dispatch_index[CtrcBatch.dispatch_count++] = index;
		CtrcDispatchCursor = (index + 1) % CtrcShared->origin_key_entries;
	}
	SpinLockRelease(&CtrcShared->origin_lock);

	/* Non-atomic observation across tables/stages. Age is a lower bound
	 * since this process first saw the seal, never an expiry decision. */
	pg_atomic_write_u64(&CtrcShared->stats[CTRC_STAT_DISPATCH_BACKLOG], pending);
	pg_atomic_write_u64(&CtrcShared->stats[CTRC_STAT_CERTIFICATE_BACKLOG], certifying);
	pg_atomic_write_u64(&CtrcShared->stats[CTRC_STAT_PENDING_OBSERVED_AGE_MS],
						oldest_age_us / 1000);
	pg_atomic_write_u64(&CtrcShared->stats[CTRC_STAT_OBSERVED_AT_US], now);
}
#endif

bool
cluster_ctrc_origin_next_close_dispatch_shared(
	ClusterCtrcCloseDispatch *dispatch_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	bool found = false;

	if (dispatch_out != NULL)
		MemSet(dispatch_out, 0, sizeof(*dispatch_out));
	if (dispatch_out == NULL || !cluster_ctrc_shmem_ready())
		return false;
	if (!CtrcBatch.active && CtrcBatch.dispatch_next >= CtrcBatch.dispatch_count)
		ctrc_dispatch_batch_collect();
	SpinLockAcquire(&CtrcShared->origin_lock);
	while (CtrcBatch.dispatch_next < CtrcBatch.dispatch_count) {
		uint64 index = CtrcBatch.dispatch_index[CtrcBatch.dispatch_next++];
		ClusterCtrcOriginEntry *origin = &ctrc_origin_entries()[index];
		CtrcCleanerOriginHint *hint = &CtrcOriginHint[index];
		bool certificate_notification;
		unsigned visited;

		if (!ctrc_origin_dispatchable_locked(origin))
			continue;
		certificate_notification = origin->state == CTRC_ORIGIN_RELEASE_PROVEN;
		for (visited = 0; visited < CLUSTER_CTRC_MAX_PARTICIPANTS; visited++) {
			uint16 node_id = (hint->participant_cursor + visited) % CLUSTER_CTRC_MAX_PARTICIPANTS;
			uint32 bit = UINT32_C(1) << node_id;
			uint64 request_id;

			if ((origin->touched_bitmap & bit) == 0
				|| (certificate_notification
					? (origin->close_confirmed_bitmap & bit) != 0
					: (origin->ack_bitmap & bit) != 0))
				continue;
			request_id = origin->close_request_id[node_id];
			if (request_id == 0)
			{
				if (CtrcShared->global_request_generation == 0
					|| CtrcShared->global_request_generation == UINT64_MAX)
				{
					origin->state = CTRC_ORIGIN_BLOCKED;
					CtrcShared->full_refusal_count++;
					break;
				}
				request_id = CtrcShared->global_request_generation++;
			}
			if (!(certificate_notification
					  ? cluster_ctrc_origin_arm_certificate_entry(origin, node_id, request_id)
					  : cluster_ctrc_origin_arm_close_entry(origin, node_id, request_id)))
				break;
			dispatch_out->key = origin->key;
			dispatch_out->participant = origin->touched[node_id];
			dispatch_out->request_id = request_id;
			dispatch_out->seal_generation = origin->seal_generation;
			dispatch_out->grant_generation = origin->grant_generation;
			dispatch_out->suboperation = certificate_notification ? CTRC_SEAL_CERTIFICATE_COMMITTED
																  : CTRC_SEAL_CLOSE_AND_CLEAN;
			hint->participant_cursor = (node_id + 1) % CLUSTER_CTRC_MAX_PARTICIPANTS;
			found = true;
			break;
		}
		if (found)
			break;
	}
	SpinLockRelease(&CtrcShared->origin_lock);
	return found;
#else
	if (dispatch_out != NULL)
		MemSet(dispatch_out, 0, sizeof(*dispatch_out));
	return false;
#endif
}

#ifndef CLUSTER_CTRC_UNIT_TEST
static bool
ctrc_cleaner_terminal_sample_exact(const ClusterCtrcTxnKeyV1 *key,
	ClusterCtrcTerminalStatus *status_out, SCN *commit_scn_out)
{
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = {false, 0};
	ClusterUndoBlock0Generation final_generation = {false, 0};
	ClusterUndoBlock0CurrentGuard guard = {0};
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	PGAlignedBlock page;
	const UndoSegmentHeaderData *header;
	TTSlot slot;
	bool native_commit_before;
	bool native_abort_before;
	bool native_progress_before;
	bool native_commit_after;
	bool native_abort_after;
	bool native_progress_after;
	bool admission_entered = false;
	bool current_active = false;
	bool terminal = false;

	if (status_out != NULL)
		*status_out = CTRC_TERMINAL_UNKNOWN;
	if (commit_scn_out != NULL)
		*commit_scn_out = InvalidScn;

	MemSet(&admission, 0, sizeof(admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	MemSet(&final_root, 0, sizeof(final_root));
	MemSet(&page, 0, sizeof(page));
	MemSet(&slot, 0, sizeof(slot));
	if (!ctrc_txn_key_valid(key) || cluster_node_id < 0
		|| key->origin_node_id != (uint16)cluster_node_id
		|| key->owner_instance != (uint8)(cluster_node_id + 1)
		|| key->cluster_epoch != cluster_epoch_get_current()
		|| key->system_identifier != GetSystemIdentifier()
		|| key->origin_boot_incarnation
		   != cluster_qvotec_get_self_incarnation())
		return false;
	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	admission_entered = true;
	logical.owner_instance = key->owner_instance;
	logical.segment_id = key->segment_id;

	PG_TRY();
	{
		if (admission.formation_epoch != key->formation_epoch
			|| admission.record_generation
			   != key->admission_record_generation
			|| !cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root)
			|| root.intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
			|| root.root_id != key->root_id
			|| root.root_generation != key->root_generation
			|| key->root_descriptor_incarnation != root.root_generation)
			goto sample_done;

		step = cluster_undo_block0_current_acquire_begin_admitted(
			&logical, CLUSTER_UNDO_BLOCK0_SCUR,
			cluster_ges_request_timeout_ms, &admission, &guard, &failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED)
			goto sample_done;
		current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
		{
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_CLEANER_SAMPLE_ACQUIRE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto sample_done;
		if (cluster_undo_block0_current_sample_generation(
				&guard, &root, &generation) != CLUSTER_UNDO_BLOCK0_OK
			|| !generation.known
			|| generation.value != key->segment_generation
			|| cluster_undo_block0_current_copy_resident(
				&guard, &root, &generation, page.data)
			   != CLUSTER_UNDO_BLOCK0_OK)
			goto sample_done;

		native_commit_before = TransactionIdDidCommit(key->xid);
		native_abort_before = TransactionIdDidAbort(key->xid);
		native_progress_before = TransactionIdIsInProgress(key->xid);
		header = (const UndoSegmentHeaderData *)page.data;
		if (header->segment_id != key->segment_id
			|| header->owner_instance != key->owner_instance
			|| header->wrap_count != key->segment_generation
			|| header->tt_slots_count != TT_SLOTS_PER_SEGMENT)
			goto sample_done;
		slot = header->tt_slots[key->slot_offset];
		native_commit_after = TransactionIdDidCommit(key->xid);
		native_abort_after = TransactionIdDidAbort(key->xid);
		native_progress_after = TransactionIdIsInProgress(key->xid);
		if (native_commit_before != native_commit_after
			|| native_abort_before != native_abort_after
			|| native_progress_before != native_progress_after
			|| native_progress_after
			|| slot.xid != key->xid || slot.wrap != key->slot_wrap
			|| slot.flags != TT_FLAGS_RESERVED)
			goto sample_done;

		MemSet(&final_root, 0, sizeof(final_root));
		if (!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &final_root)
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_recheck_r4_terminal_census(
				&admission)
			|| cluster_undo_block0_current_sample_generation(
				&guard, &root, &final_generation)
			   != CLUSTER_UNDO_BLOCK0_OK
			|| !final_generation.known
			|| final_generation.value != generation.value)
			goto sample_done;

		terminal = (slot.status == TT_SLOT_COMMITTED
				&& SCN_VALID(slot.commit_scn)
				&& native_commit_after && !native_abort_after)
			|| (slot.status == TT_SLOT_ABORTED
				&& slot.commit_scn == InvalidScn
				&& native_abort_after && !native_commit_after);

sample_done:
		if (current_active)
		{
			step = cluster_undo_block0_current_release_begin(
				&guard, &failure);
			while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
			{
				CHECK_FOR_INTERRUPTS();
				step = cluster_undo_block0_current_release_poll(
					&guard, &failure);
				if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
					&& !cluster_undo_block0_current_wait_reply(
						&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_CLEANER_SAMPLE_RELEASE))
					pg_usleep(1000L);
			}
			current_active = false;
			if (step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
				terminal = false;
		}
	}
	PG_CATCH();
	{
		if (current_active)
			cluster_undo_block0_current_cancel(&guard);
		if (admission_entered)
			cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	if (terminal && status_out != NULL)
		*status_out = slot.status == TT_SLOT_COMMITTED
			? CTRC_TERMINAL_COMMITTED : CTRC_TERMINAL_ABORTED;
	if (terminal && commit_scn_out != NULL)
		*commit_scn_out = slot.status == TT_SLOT_COMMITTED
			? slot.commit_scn : InvalidScn;
	return terminal;
}
#endif

/*
 * cluster_ctrc_terminal_release_sample_exact -- 8D-12 L11/L12.
 *
 * The allocator deliberately carries only a compact shmem owner snapshot, so
 * it cannot read block 0 while holding its own lock.  Reconstruct the local
 * logical block identity here, enter the frozen R4 terminal-census admission,
 * and sample one exact terminal slot under SCUR.  The durable release bit is
 * reference-release authority only; the stable native bracket remains the
 * independent terminal-status authority.
 */
bool
cluster_ctrc_terminal_release_sample_exact(uint32 segment_id,
	uint16 slot_offset, TransactionId xid, uint16 slot_wrap,
	uint8 terminal_status, SCN terminal_scn, uint64 expected_epoch)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = {false, 0};
	ClusterUndoBlock0Generation final_generation = {false, 0};
	ClusterUndoBlock0CurrentGuard guard = {0};
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	PGAlignedBlock page;
	const UndoSegmentHeaderData *header;
	TTSlot slot;
	uint8 owner_instance;
	bool native_commit_before;
	bool native_abort_before;
	bool native_progress_before;
	bool native_commit_after;
	bool native_abort_after;
	bool native_progress_after;
	bool admission_entered = false;
	bool current_active = false;
	bool released = false;

	MemSet(&admission, 0, sizeof(admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	MemSet(&final_root, 0, sizeof(final_root));
	MemSet(&page, 0, sizeof(page));
	MemSet(&slot, 0, sizeof(slot));
	if (cluster_node_id < 0 || segment_id == 0
		|| slot_offset >= TT_SLOTS_PER_SEGMENT
		|| !TransactionIdIsNormal(xid) || slot_wrap == TT_WRAP_INVALID
		|| (expected_epoch == 0 && cluster_conf_node_count() != 4)
		|| cluster_epoch_get_current() != expected_epoch
		|| (terminal_status != TT_SLOT_COMMITTED
			&& terminal_status != TT_SLOT_ABORTED)
		|| ((terminal_status == TT_SLOT_COMMITTED)
			!= SCN_VALID(terminal_scn)))
		return false;
	owner_instance = (uint8)(((segment_id - 1)
		/ CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1);
	if (owner_instance != (uint8)(cluster_node_id + 1))
		return false;
	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	admission_entered = true;
	logical.owner_instance = owner_instance;
	logical.segment_id = segment_id;

	PG_TRY();
	{
		if (admission.formation_epoch != expected_epoch
			|| !cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root))
			goto release_sample_done;

		step = cluster_undo_block0_current_acquire_begin_admitted(
			&logical, CLUSTER_UNDO_BLOCK0_SCUR,
			cluster_ges_request_timeout_ms, &admission, &guard, &failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED)
			goto release_sample_done;
		current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
		{
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_RELEASE_SAMPLE_ACQUIRE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto release_sample_done;
		if (cluster_undo_block0_current_sample_generation(
				&guard, &root, &generation) != CLUSTER_UNDO_BLOCK0_OK
			|| !generation.known || generation.value == UINT32_MAX
			|| cluster_undo_block0_current_copy_resident(
				&guard, &root, &generation, page.data)
			   != CLUSTER_UNDO_BLOCK0_OK)
			goto release_sample_done;

		native_commit_before = TransactionIdDidCommit(xid);
		native_abort_before = TransactionIdDidAbort(xid);
		native_progress_before = TransactionIdIsInProgress(xid);
		header = (const UndoSegmentHeaderData *)page.data;
		if (header->segment_id != segment_id
			|| header->owner_instance != owner_instance
			|| header->wrap_count != generation.value
			|| header->tt_slots_count != TT_SLOTS_PER_SEGMENT)
			goto release_sample_done;
		slot = header->tt_slots[slot_offset];
		native_commit_after = TransactionIdDidCommit(xid);
		native_abort_after = TransactionIdDidAbort(xid);
		native_progress_after = TransactionIdIsInProgress(xid);
		if (native_commit_before != native_commit_after
			|| native_abort_before != native_abort_after
			|| native_progress_before != native_progress_after
			|| native_progress_after || slot.xid != xid
			|| slot.wrap != slot_wrap || slot.status != terminal_status
			|| slot.commit_scn != terminal_scn
			|| slot.flags != TT_SLOT_FLAG_CTRC_RELEASE_PROVEN
			|| !UBA_is_invalid(slot.first_undo_block)
			|| (terminal_status == TT_SLOT_COMMITTED
				? (!native_commit_after || native_abort_after)
				: (!native_abort_after || native_commit_after)))
			goto release_sample_done;

		MemSet(&final_root, 0, sizeof(final_root));
		if (!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &final_root)
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_recheck_r4_terminal_census(
				&admission)
			|| cluster_epoch_get_current() != expected_epoch
			|| cluster_undo_block0_current_sample_generation(
				&guard, &root, &final_generation)
			   != CLUSTER_UNDO_BLOCK0_OK
			|| !final_generation.known
			|| final_generation.value != generation.value)
			goto release_sample_done;
		released = true;

release_sample_done:
		if (current_active)
		{
			step = cluster_undo_block0_current_release_begin(&guard, &failure);
			while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
			{
				CHECK_FOR_INTERRUPTS();
				step = cluster_undo_block0_current_release_poll(
					&guard, &failure);
				if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
					&& !cluster_undo_block0_current_wait_reply(
						&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_RELEASE_SAMPLE_RELEASE))
					pg_usleep(1000L);
			}
			current_active = false;
			if (step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
				released = false;
		}
	}
	PG_CATCH();
	{
		if (current_active)
			cluster_undo_block0_current_cancel(&guard);
		if (admission_entered)
			cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	if (released)
		cluster_ctrc_stat_bump(CTRC_STAT_L11_RELEASE_SAMPLE);
	return released;
#else
	(void)segment_id;
	(void)slot_offset;
	(void)xid;
	(void)slot_wrap;
	(void)terminal_status;
	(void)terminal_scn;
	(void)expected_epoch;
	return false;
#endif
}

#ifndef CLUSTER_CTRC_UNIT_TEST
static bool
ctrc_cleaner_publish_certificate(
	const ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcCertificateInput input;
	ClusterSemanticAdmissionToken admission;
	ClusterUndoBlock0LogicalKey logical;
	ClusterUndoBlock0ResolvedRoot root;
	ClusterUndoBlock0ResolvedRoot final_root;
	ClusterUndoBlock0Generation generation = {false, 0};
	ClusterUndoBlock0CurrentGuard guard = {0};
	ClusterUndoBlock0Pin pin;
	ClusterUndoBlock0CurrentStep step;
	ClusterUndoBlock0Result failure = CLUSTER_UNDO_BLOCK0_AUTHORITY_DENIED;
	PGAlignedBlock disk_block;
	PGAlignedBlock successor_block;
	PGAlignedBlock final_disk_block;
	const UndoSegmentHeaderData *disk_header;
	UndoSegmentHeaderData *successor_header;
	const UndoSegmentHeaderData *final_disk_header;
	const TTSlot *disk_slot;
	const TTSlot *final_disk_slot;
	xl_undo_tt_slot_ctrc_release_v1 record;
	ClusterUndoTtCtrcReleaseRedoDecision decision
		= CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_CONFLICT;
	const ClusterCtrcTxnKeyV1 *key;
	XLogRecPtr certificate_lsn = InvalidXLogRecPtr;
	uint8 digest[32];
	char *resident_page = NULL;
	bool admission_entered = false;
	bool current_active = false;
	bool pin_held = false;
	bool durable = false;
	bool committed = false;

	MemSet(&input, 0, sizeof(input));
	MemSet(&admission, 0, sizeof(admission));
	MemSet(&logical, 0, sizeof(logical));
	MemSet(&root, 0, sizeof(root));
	MemSet(&final_root, 0, sizeof(final_root));
	MemSet(&pin, 0, sizeof(pin));
	pin.slot = -1;
	MemSet(&disk_block, 0, sizeof(disk_block));
	MemSet(&successor_block, 0, sizeof(successor_block));
	MemSet(&final_disk_block, 0, sizeof(final_disk_block));
	MemSet(&record, 0, sizeof(record));
	MemSet(digest, 0, sizeof(digest));
	if (snapshot == NULL)
		return false;
	key = &snapshot->origin.key;
	input.acks = snapshot->ack_count == 0 ? NULL : snapshot->acks;
	input.ack_count = snapshot->ack_count;
	input.frozen_touched_bitmap = snapshot->origin.touched_bitmap;
	input.seal_generation = snapshot->origin.seal_generation;
	input.block0_terminal_exact = true;
	input.all_dependencies_durable = true;
	if (cluster_ctrc_origin_certificate_validate(&input)
		!= CLUSTER_CTRC_CERTIFICATE_READY
		|| !cluster_ctrc_origin_certificate_digest(&input, digest)
		|| !ctrc_txn_key_valid(key)
		|| snapshot->origin.state != CTRC_ORIGIN_CERTIFYING
		|| cluster_node_id < 0
		|| key->origin_node_id != (uint16)cluster_node_id
		|| key->owner_instance != (uint8)(cluster_node_id + 1)
		|| key->cluster_epoch != cluster_epoch_get_current()
		|| key->system_identifier != GetSystemIdentifier()
		|| key->origin_boot_incarnation
		   != cluster_qvotec_get_self_incarnation()
		/* Independently reproduce the exact shared image after digesting it,
		 * before entering any block-0 wait. */
		|| !ctrc_origin_certificate_snapshot_matches_shared(snapshot))
		return false;
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_BLOCK0_CERTIFICATE);
	cluster_ctrc_test_barrier_wait(CTRC_TEST_BARRIER_CERTIFICATE_READY);

	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	admission_entered = true;
	logical.owner_instance = key->owner_instance;
	logical.segment_id = key->segment_id;

	PG_TRY();
	{
		if (admission.formation_epoch != key->formation_epoch
			|| admission.record_generation
			   != key->admission_record_generation
			|| !cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &root)
			|| root.intent != CLUSTER_UNDO_PATH_RUNTIME_SHARED
			|| root.root_id != key->root_id
			|| root.root_generation != key->root_generation
			|| key->root_descriptor_incarnation != root.root_generation)
			goto certificate_done;

		step = cluster_undo_block0_current_acquire_begin_ctrc_release(
			&logical, cluster_ges_request_timeout_ms, &admission,
			&guard, &failure);
		if (step == CLUSTER_UNDO_BLOCK0_CURRENT_FAILED)
			goto certificate_done;
		current_active = true;
		while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
		{
			CHECK_FOR_INTERRUPTS();
			step = cluster_undo_block0_current_acquire_poll(&guard, &failure);
			if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
				&& !cluster_undo_block0_current_wait_reply(
					&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_CERTIFICATE_ACQUIRE))
				pg_usleep(1000L);
		}
		if (step != CLUSTER_UNDO_BLOCK0_CURRENT_HELD)
			goto certificate_done;
		if (cluster_undo_block0_current_sample_generation_exclusive(
				&guard, &root, &generation) != CLUSTER_UNDO_BLOCK0_OK
			|| !generation.known
			|| generation.value != key->segment_generation
			|| cluster_undo_block0_current_pin_exclusive(
				&guard, &root, &generation, &pin, &resident_page)
			   != CLUSTER_UNDO_BLOCK0_OK
			|| resident_page == NULL)
			goto certificate_done;
		pin_held = true;

		if (!cluster_undo_smgr_read_block(root.intent, key->segment_id,
				key->owner_instance, 0, disk_block.data)
			|| memcmp(disk_block.data, resident_page, BLCKSZ) != 0)
			goto certificate_done;
		disk_header = (const UndoSegmentHeaderData *)disk_block.data;
		if (disk_header->segment_id != key->segment_id
			|| disk_header->owner_instance != key->owner_instance
			|| disk_header->wrap_count != key->segment_generation
			|| disk_header->tt_slots_count != TT_SLOTS_PER_SEGMENT)
			goto certificate_done;
		disk_slot = &disk_header->tt_slots[key->slot_offset];
		if (disk_slot->xid != key->xid
			|| disk_slot->wrap != key->slot_wrap
			|| (disk_slot->status != TT_SLOT_COMMITTED
				&& disk_slot->status != TT_SLOT_ABORTED)
			|| (disk_slot->status == TT_SLOT_COMMITTED)
			   != SCN_VALID(disk_slot->commit_scn)
			|| (disk_slot->flags & ~TT_SLOT_FLAGS_KNOWN) != 0)
			goto certificate_done;

		record.segment_id = key->segment_id;
		record.segment_generation = key->segment_generation;
		record.xid = key->xid;
		record.cluster_epoch = key->cluster_epoch;
		record.root_id = key->root_id;
		record.root_generation = key->root_generation;
		record.formation_epoch = key->formation_epoch;
		record.admission_record_generation
			= key->admission_record_generation;
		record.seal_generation = snapshot->origin.seal_generation;
		record.touched_nodes_low = snapshot->origin.touched_bitmap;
		record.touched_nodes_high = 0;
		memcpy(record.ack_set_digest, digest,
			sizeof(record.ack_set_digest));
		record.slot_offset = key->slot_offset;
		record.slot_wrap = key->slot_wrap;
		record.owner_instance = key->owner_instance;
		record.terminal_status = disk_slot->status;
		record.format_version = CLUSTER_UNDO_TT_CTRC_RELEASE_VERSION;
		record.flags = CLUSTER_UNDO_TT_CTRC_RELEASE_ALL_TOUCHED_ACKED;
		decision = cluster_undo_tt_ctrc_release_redo_decide(
			disk_header->wrap_count, disk_slot, &record);
		if (decision != CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY
			&& decision != CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_IDEMPOTENT)
			goto certificate_done;

		MemSet(&final_root, 0, sizeof(final_root));
		if (!cluster_semantic_activation_resolve_shared_undo_root_r4_terminal_census(
				&admission, CLUSTER_UNDO_PATH_RUNTIME_SHARED,
				logical.owner_instance, logical.segment_id, &final_root)
			|| !cluster_undo_block0_root_matches(&root, &final_root)
			|| !cluster_semantic_activation_recheck_r4_terminal_census(
				&admission)
			|| cluster_undo_block0_current_recheck_exclusive(&guard)
			   != CLUSTER_UNDO_BLOCK0_OK
			/* CERTIFYING ACK bytes are immutable, but the exact shared image
			 * is still sampled once more under XCUR before WAL insertion. */
			|| !ctrc_origin_certificate_snapshot_matches_shared(snapshot))
			goto certificate_done;

		if (decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY)
		{
			memcpy(successor_block.data, disk_block.data, BLCKSZ);
			successor_header
				= (UndoSegmentHeaderData *)successor_block.data;
			successor_header->tt_slots[key->slot_offset].flags
				= TT_SLOT_FLAG_CTRC_RELEASE_PROVEN;
			certificate_lsn
				= cluster_undo_xlog_insert_tt_ctrc_release(&record);
			if (XLogRecPtrIsInvalid(certificate_lsn))
				goto certificate_done;
			cluster_undo_block0_flush_sync(&pin, successor_block.data,
				certificate_lsn, false);
		}

		if (!cluster_undo_smgr_read_block(root.intent, key->segment_id,
				key->owner_instance, 0, final_disk_block.data)
			|| memcmp(final_disk_block.data, resident_page, BLCKSZ) != 0)
			goto certificate_done;
		final_disk_header
			= (const UndoSegmentHeaderData *)final_disk_block.data;
		final_disk_slot = &final_disk_header->tt_slots[key->slot_offset];
		if (final_disk_header->segment_id != key->segment_id
			|| final_disk_header->owner_instance != key->owner_instance
			|| final_disk_header->wrap_count != key->segment_generation
			|| final_disk_header->tt_slots_count != TT_SLOTS_PER_SEGMENT
			|| cluster_undo_tt_ctrc_release_redo_decide(
				final_disk_header->wrap_count, final_disk_slot, &record)
			   != CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_IDEMPOTENT
			/* The EXCLUSIVE resident pin is still held, so its sampled
			 * generation cannot change.  Calling the sampling API here would
			 * recursively acquire the same non-reentrant content lock. */
			|| !cluster_undo_block0_generation_matches(
				&pin.observed_generation, &generation)
			|| !cluster_semantic_activation_recheck_r4_terminal_census(
				&admission)
			|| cluster_undo_block0_current_recheck_exclusive(&guard)
			   != CLUSTER_UNDO_BLOCK0_OK)
			goto certificate_done;
		durable = true;

certificate_done:
		if (pin_held)
		{
			cluster_undo_block0_unpin(&pin);
			pin_held = false;
		}
		if (current_active)
		{
			step = cluster_undo_block0_current_release_begin(
				&guard, &failure);
			while (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING)
			{
				CHECK_FOR_INTERRUPTS();
				step = cluster_undo_block0_current_release_poll(
					&guard, &failure);
				if (step == CLUSTER_UNDO_BLOCK0_CURRENT_PENDING
					&& !cluster_undo_block0_current_wait_reply(
						&guard, CLUSTER_UNDO_BLOCK0_WAIT_CTRC_CERTIFICATE_RELEASE))
					pg_usleep(1000L);
			}
			current_active = false;
			if (step != CLUSTER_UNDO_BLOCK0_CURRENT_RELEASED)
				durable = false;
		}
	}
	PG_CATCH();
	{
		if (pin_held)
			cluster_undo_block0_unpin(&pin);
		if (current_active)
			cluster_undo_block0_current_cancel(&guard);
		if (admission_entered)
			cluster_semantic_activation_leave(&admission);
		PG_RE_THROW();
	}
	PG_END_TRY();
	cluster_semantic_activation_leave(&admission);
	if (durable)
		committed = ctrc_origin_certificate_commit_shared(snapshot);
	if (committed)
	{
		cluster_ctrc_stat_bump(
			decision == CLUSTER_UNDO_TT_CTRC_RELEASE_REDO_APPLY
				? CTRC_STAT_CERTIFICATE_APPLIED
				: CTRC_STAT_CERTIFICATE_REPLAYED);
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);
		/* RELEASE_PROVEN still needs the participant-summary notification
		 * continuation before this physical-slot origin row is reusable. */
		cluster_undo_cleaner_wakeup();
	}
	else
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_BLOCKED);
	return committed;
}
#endif

bool
cluster_ctrc_cleaner_run_pass(void)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	uint64 progress_before;

	if (!cluster_ctrc_shmem_ready())
		return false;
	Assert(!CtrcBatch.active);
	if (CtrcBatch.active)
		return false;
	cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_PASS);
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);
	progress_before = cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS);
	CtrcBatch.active = true;
	CtrcBatch.remaining[CTRC_SCAN_OPEN] = CtrcShared->origin_key_entries;
	CtrcBatch.remaining[CTRC_SCAN_RECEIPT] = CtrcShared->receipt_entries;
	CtrcBatch.remaining[CTRC_SCAN_ACK] = CtrcShared->participant_key_entries;
	CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE] = CtrcShared->origin_key_entries;
	PG_TRY();
	{
		/* Service the terminal pipeline before admitting new seals. Each
		 * selector walks at most one table circumference per pass; a
		 * retained item consumes its attempt and cannot block its peers. */
		for (unsigned i = 0;
			 i < CTRC_CLEANER_RECEIPT_BATCH && CtrcBatch.remaining[CTRC_SCAN_RECEIPT] != 0; i++) {
			CHECK_FOR_INTERRUPTS();
			cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_ATTEMPT);
			if (ctrc_cleaner_clean_next_receipt())
				ctrc_semantic_progress(false);
		}
		for (unsigned i = 0; i < CTRC_CLEANER_ACK_BATCH && CtrcBatch.remaining[CTRC_SCAN_ACK] != 0;
			 i++) {
			CHECK_FOR_INTERRUPTS();
			cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_ATTEMPT);
			if (ctrc_participant_freeze_next_ack_shared())
				ctrc_semantic_progress(false);
		}
		ctrc_dispatch_batch_collect();
		for (unsigned i = 0; i < CTRC_CLEANER_DISPATCH_BATCH; i++) {
			ClusterCtrcCloseDispatch dispatch;

			CHECK_FOR_INTERRUPTS();
			if (!cluster_ctrc_origin_next_close_dispatch_shared(&dispatch))
				break;
			cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_ATTEMPT);
			/* An accepted duplicate and a remote enqueue are not progress.
			 * Only the actual shared reply/participant edges record it. */
			(void)cluster_gcs_ctrc_dispatch_close(&dispatch);
		}
		for (unsigned i = 0;
			 i < CTRC_CLEANER_CERTIFICATE_BATCH && CtrcBatch.remaining[CTRC_SCAN_CERTIFICATE] != 0;
			 i++) {
			ClusterCtrcOriginCertificateSnapshot certificate;

			CHECK_FOR_INTERRUPTS();
			cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_ATTEMPT);
			if (ctrc_origin_next_certificate_snapshot_shared(&certificate)
				&& ctrc_cleaner_publish_certificate(&certificate))
				ctrc_semantic_progress(false);
		}
		for (unsigned i = 0;
			 i < CTRC_CLEANER_SEAL_BATCH && CtrcBatch.remaining[CTRC_SCAN_OPEN] != 0; i++) {
			ClusterCtrcTxnKeyV1 key;

			CHECK_FOR_INTERRUPTS();
			cluster_ctrc_stat_bump(CTRC_STAT_CLEANER_ATTEMPT);
			if (cluster_ctrc_origin_next_open_shared(&key)
				&& ctrc_cleaner_terminal_sample_exact(&key, NULL, NULL)
				&& cluster_ctrc_origin_begin_seal_shared(&key))
				ctrc_semantic_progress(false);
		}
	}
	PG_FINALLY();
	{
		/* No batch hint or scan limit survives an ERROR into a later pass. */
		MemSet(&CtrcBatch, 0, sizeof(CtrcBatch));
	}
	PG_END_TRY();
	return cluster_ctrc_stat_get(CTRC_STAT_SEMANTIC_PROGRESS) != progress_before;
#else
	return false;
#endif
}

ClusterCtrcParticipantOpenResult
cluster_ctrc_participant_open(ClusterCtrcParticipantEntry *participant,
							  const ClusterCtrcTxnKeyV1 *key,
							  uint32 grant_generation,
							  const ClusterCtrcParticipantIdentity *identity)
{
	if (participant == NULL || !ctrc_txn_key_valid(key)
		|| grant_generation == 0
		|| !ctrc_participant_identity_valid(key, identity))
		return CLUSTER_CTRC_PARTICIPANT_REFUSED;
	if (participant->state == CTRC_PARTICIPANT_OPEN
		&& participant->grant_generation == grant_generation
		&& memcmp(&participant->key, key, sizeof(*key)) == 0
		&& memcmp(&participant->identity, identity, sizeof(*identity)) == 0)
		return CLUSTER_CTRC_PARTICIPANT_DUPLICATE;
	if (!ctrc_bytes_zero(participant, sizeof(*participant)))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PARTICIPANT_REFUSED;
	}

	participant->key = *key;
	participant->identity = *identity;
	participant->grant_generation = grant_generation;
	participant->state = CTRC_PARTICIPANT_OPEN;
	participant->next_key_sequence = 1;
	return CLUSTER_CTRC_PARTICIPANT_OPENED;
}

static ClusterCtrcPrepareResult
ctrc_receipt_prepare_with_sequence(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target, ClusterCtrcReceipt *receipt,
	uint64 supplied_journal_sequence)
{
	ClusterCtrcPublicationIdV1 stored_publication;
	uint64 journal_sequence;
	uint64 key_sequence;

	if (receipt == NULL)
		return CLUSTER_CTRC_PREPARE_CAPACITY;
	if (!ctrc_publication_prepare_valid(participant, publication, target))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	if (pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state)
		!= CTRC_RECEIPT_FREE)
	{
		stored_publication = receipt->publication;
		if (memcmp(&receipt->key, &participant->key,
				   sizeof(receipt->key)) == 0
			&& ctrc_publication_request_equal(&stored_publication, publication)
			&& memcmp(&receipt->target, target, sizeof(*target)) == 0
			&& stored_publication.journal_sequence != 0
			&& stored_publication.key_sequence != 0
			&& stored_publication.journal_slot_generation != 0)
			return CLUSTER_CTRC_PREPARE_DUPLICATE;
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PREPARE_REFUSED;
	}
	if (!ctrc_bytes_zero(receipt, sizeof(*receipt))
		|| participant->next_key_sequence == 0
		|| participant->next_key_sequence == UINT64_MAX
		|| participant->last_key_sequence == UINT64_MAX
		|| participant->next_key_sequence
		   != participant->last_key_sequence + 1
		|| (supplied_journal_sequence == 0
			? !ctrc_allocate_journal_sequence(&journal_sequence)
			: (journal_sequence = supplied_journal_sequence) == 0))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_PREPARE_REFUSED;
	}

	key_sequence = participant->next_key_sequence;
	MemSet(receipt, 0, sizeof(*receipt));
	receipt->key = participant->key;
	receipt->publication = *publication;
	receipt->publication.journal_sequence = journal_sequence;
	receipt->publication.key_sequence = key_sequence;
	/* Until Task 8 reclaims slots, each allocation has a unique generation. */
	receipt->publication.journal_slot_generation = journal_sequence;
	receipt->target = *target;
	pg_atomic_init_u32((pg_atomic_uint32 *)&receipt->state,
					   CTRC_RECEIPT_PREPARED);
	participant->next_key_sequence++;
	participant->last_key_sequence = key_sequence;
	participant->receipt_count++;
	participant->prepared_count++;
	return CLUSTER_CTRC_PREPARE_READY;
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare(ClusterCtrcParticipantEntry *participant,
							 const ClusterCtrcPublicationIdV1 *publication,
							 const ClusterCtrcTargetV1 *target,
							 ClusterCtrcReceipt *receipt)
{
	return ctrc_receipt_prepare_with_sequence(
		participant, publication, target, receipt, 0);
}

static bool
ctrc_receipt_publication_matches(const ClusterCtrcReceipt *receipt,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcPublicationIdV1 *publication)
{
	return receipt != NULL
		&& memcmp(&receipt->key, key, sizeof(*key)) == 0
		&& ctrc_publication_request_equal(&receipt->publication, publication);
}

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_table_locked(
	ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceipt *receipts, uint8 *probe_states, Size receipt_count,
	uint64 journal_sequence, uint64 *receipt_index_out,
	Size *probe_count_out)
{
	uint64 first_tombstone = UINT64_MAX;
	uint64 hash;
	uint64 selected_index = UINT64_MAX;
	Size probe;

	if (receipt_index_out != NULL)
		*receipt_index_out = UINT64_MAX;
	if (probe_count_out != NULL)
		*probe_count_out = 0;
	if (participant == NULL || key == NULL || identity == NULL
		|| publication == NULL || target == NULL || receipts == NULL
		|| probe_states == NULL
		|| receipt_count == 0 || journal_sequence == 0
		|| receipt_index_out == NULL
		|| cluster_ctrc_participant_open(participant, key,
			grant_generation, identity) == CLUSTER_CTRC_PARTICIPANT_REFUSED
		|| !ctrc_publication_prepare_valid(participant, publication, target))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	hash = ctrc_receipt_probe_hash(key, publication);
	for (probe = 0; probe < receipt_count; probe++)
	{
		uint64 index = (hash + probe) % receipt_count;
		ClusterCtrcReceipt *receipt = &receipts[index];
		uint8 probe_state = probe_states[index];
		uint32 state = pg_atomic_read_u32(
			(pg_atomic_uint32 *)&receipt->state);

		if (probe_count_out != NULL)
			*probe_count_out = probe + 1;
		if (probe_state == CTRC_RECEIPT_PROBE_EMPTY)
		{
			if (!ctrc_bytes_zero(receipt, sizeof(*receipt)))
			{
				participant->state = CTRC_PARTICIPANT_BLOCKED;
				return CLUSTER_CTRC_PREPARE_REFUSED;
			}
			selected_index = first_tombstone != UINT64_MAX
				? first_tombstone : index;
			break;
		}
		if (probe_state == CTRC_RECEIPT_PROBE_TOMBSTONE)
		{
			if (!ctrc_bytes_zero(receipt, sizeof(*receipt)))
			{
				participant->state = CTRC_PARTICIPANT_BLOCKED;
				return CLUSTER_CTRC_PREPARE_REFUSED;
			}
			if (first_tombstone == UINT64_MAX)
				first_tombstone = index;
			continue;
		}
		if (probe_state != CTRC_RECEIPT_PROBE_OCCUPIED
			|| state == CTRC_RECEIPT_FREE
			|| ctrc_bytes_zero(receipt, sizeof(*receipt)))
		{
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			return CLUSTER_CTRC_PREPARE_REFUSED;
		}

		if (ctrc_receipt_publication_matches(receipt, key,
				publication))
		{
			if (state != CTRC_RECEIPT_FREE
				&& memcmp(&receipt->target, target,
					sizeof(*target)) == 0
				&& receipt->publication.journal_sequence != 0
				&& receipt->publication.key_sequence != 0
				&& receipt->publication.journal_slot_generation != 0)
			{
				*receipt_index_out = index;
				return CLUSTER_CTRC_PREPARE_DUPLICATE;
			}
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			return CLUSTER_CTRC_PREPARE_REFUSED;
		}
	}
	if (selected_index == UINT64_MAX)
		selected_index = first_tombstone;
	if (selected_index == UINT64_MAX)
		return CLUSTER_CTRC_PREPARE_CAPACITY;
	if (ctrc_receipt_prepare_with_sequence(participant, publication, target,
			&receipts[selected_index], journal_sequence)
		!= CLUSTER_CTRC_PREPARE_READY)
		return CLUSTER_CTRC_PREPARE_REFUSED;
	probe_states[selected_index] = CTRC_RECEIPT_PROBE_OCCUPIED;
	*receipt_index_out = selected_index;
	return CLUSTER_CTRC_PREPARE_READY;
}

bool
cluster_ctrc_receipt_reclaim_frozen_table_locked(
	const ClusterCtrcTxnKeyV1 *key, uint64 expected_receipt_count,
	ClusterCtrcReceipt *receipts, uint8 *probe_states, Size receipt_count,
	Size *reclaimed_count_out)
{
	Size found = 0;
	Size occupied = 0;
	Size i;

	if (reclaimed_count_out != NULL)
		*reclaimed_count_out = 0;
	if (!ctrc_txn_key_valid(key) || receipts == NULL || probe_states == NULL
		|| receipt_count == 0 || expected_receipt_count > receipt_count)
		return false;

	/* Validation is deliberately complete before the first destructive byte.
	 * A corrupt index or non-frozen member retains the entire set. */
	for (i = 0; i < receipt_count; i++)
	{
		ClusterCtrcReceipt *receipt = &receipts[i];
		uint8 probe_state = probe_states[i];
		bool bytes_zero = ctrc_bytes_zero(receipt, sizeof(*receipt));

		if (probe_state == CTRC_RECEIPT_PROBE_EMPTY
			|| probe_state == CTRC_RECEIPT_PROBE_TOMBSTONE)
		{
			if (!bytes_zero)
				return false;
			continue;
		}
		if (probe_state != CTRC_RECEIPT_PROBE_OCCUPIED
			|| bytes_zero || receipt->state == CTRC_RECEIPT_FREE)
			return false;
		occupied++;
		if (memcmp(&receipt->key, key, sizeof(*key)) != 0)
			continue;
		if (receipt->state != CTRC_RECEIPT_ACK_FROZEN
			|| found == expected_receipt_count)
			return false;
		found++;
	}
	if (found != expected_receipt_count)
		return false;

	for (i = 0; i < receipt_count; i++)
	{
		if (probe_states[i] != CTRC_RECEIPT_PROBE_OCCUPIED
			|| memcmp(&receipts[i].key, key, sizeof(*key)) != 0)
			continue;
		MemSet(&receipts[i], 0, sizeof(receipts[i]));
		probe_states[i] = CTRC_RECEIPT_PROBE_TOMBSTONE;
	}
	if (occupied == found)
		MemSet(probe_states, CTRC_RECEIPT_PROBE_EMPTY, receipt_count);
	else
	{
		/* A trailing tombstone cannot be part of any surviving linear-probe
		 * chain.  Collapse every such run so churn cannot consume the finite
		 * table; never move a live row because handles retain its slot. */
		for (i = 0; i < receipt_count; i++)
		{
			Size cursor;
			Size visited = 0;

			if (probe_states[i] != CTRC_RECEIPT_PROBE_EMPTY)
				continue;
			cursor = i;
			while (visited++ < receipt_count)
			{
				cursor = cursor == 0 ? receipt_count - 1 : cursor - 1;
				if (probe_states[cursor] != CTRC_RECEIPT_PROBE_TOMBSTONE)
					break;
				probe_states[cursor] = CTRC_RECEIPT_PROBE_EMPTY;
			}
		}
	}
	if (reclaimed_count_out != NULL)
		*reclaimed_count_out = found;
	return true;
}

#ifndef CLUSTER_CTRC_UNIT_TEST
static void
ctrc_receipt_handle_fill(ClusterCtrcReceiptHandle *handle,
	ClusterCtrcParticipantEntry *participant, uint64 participant_index,
	ClusterCtrcReceipt *receipt, uint64 receipt_index)
{
	MemSet(handle, 0, sizeof(*handle));
	handle->participant = participant;
	handle->receipt = receipt;
	handle->key = receipt->key;
	handle->participant_index = participant_index;
	handle->receipt_index = receipt_index;
	handle->journal_slot_generation
		= receipt->publication.journal_slot_generation;
	handle->valid = true;
}

static bool
ctrc_receipt_handle_exact(const ClusterCtrcReceiptHandle *handle)
{
	return handle != NULL && handle->valid
		&& cluster_ctrc_shmem_ready()
		&& ctrc_bytes_zero(handle->reserved8, sizeof(handle->reserved8))
		&& handle->participant_index < CtrcShared->participant_key_entries
		&& handle->receipt_index < CtrcShared->receipt_entries
		&& handle->participant
		   == &ctrc_participant_entries()[handle->participant_index]
		&& handle->receipt == &ctrc_receipt_entries()[handle->receipt_index]
		&& handle->journal_slot_generation != 0
		&& handle->receipt->publication.journal_slot_generation
		   == handle->journal_slot_generation
		&& memcmp(&handle->participant->key, &handle->key,
				  sizeof(handle->key)) == 0
		&& memcmp(&handle->receipt->key, &handle->key,
				  sizeof(handle->key)) == 0;
}
#endif

ClusterCtrcPrepareResult
cluster_ctrc_receipt_prepare_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity,
	uint32 grant_generation,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCtrcTargetV1 *target,
	ClusterCtrcReceiptHandle *handle)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcReceipt *receipts;
	ClusterCtrcPrepareResult result;
	uint64 participant_index;
	uint64 receipt_index = UINT64_MAX;
	uint64 journal_sequence;

	if (handle != NULL)
		MemSet(handle, 0, sizeof(*handle));
	if (handle == NULL || identity == NULL || publication == NULL
		|| target == NULL || cluster_node_id < 0
		|| identity->node_id != (uint16)cluster_node_id
		|| !cluster_ctrc_shmem_ready()
		|| !ctrc_participant_index(key, identity->node_id,
			&participant_index)
		|| !ctrc_allocate_journal_sequence(&journal_sequence))
		return CLUSTER_CTRC_PREPARE_REFUSED;

	participant = &ctrc_participant_entries()[participant_index];
	receipts = ctrc_receipt_entries();
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	result = cluster_ctrc_receipt_prepare_table_locked(participant, key,
		identity, grant_generation, publication, target, receipts,
		ctrc_receipt_probe_states(), CtrcShared->receipt_entries,
		journal_sequence, &receipt_index, NULL);
	if (result == CLUSTER_CTRC_PREPARE_CAPACITY)
	{
		(void)pg_atomic_fetch_add_u64(
			(pg_atomic_uint64 *)&CtrcShared->full_refusal_count, 1);
	}
	if (result == CLUSTER_CTRC_PREPARE_READY
		|| result == CLUSTER_CTRC_PREPARE_DUPLICATE)
		ctrc_receipt_handle_fill(handle, participant, participant_index,
			&receipts[receipt_index], receipt_index);

	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	if (result == CLUSTER_CTRC_PREPARE_READY)
		cluster_ctrc_stat_bump(CTRC_STAT_RECEIPT_PREPARED);
	else if (result == CLUSTER_CTRC_PREPARE_CAPACITY)
		cluster_ctrc_stat_bump(CTRC_STAT_RECEIPT_CAPACITY_REFUSED);
	return result;
#else
	(void)key;
	(void)identity;
	(void)grant_generation;
	(void)publication;
	(void)target;
	if (handle != NULL)
		MemSet(handle, 0, sizeof(*handle));
	return CLUSTER_CTRC_PREPARE_REFUSED;
#endif
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_prepared(ClusterCtrcParticipantEntry *participant,
								ClusterCtrcReceipt *receipt,
								const ClusterCtrcTargetV1 *final_target,
								ClusterCtrcApplyToken *token)
{
	uint32 expected;
	bool target_valid;
	bool target_finalizes;

	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	if (participant == NULL || receipt == NULL || token == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| receipt->publication.journal_sequence == 0
		|| receipt->publication.key_sequence == 0
		|| receipt->publication.journal_slot_generation == 0)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	target_valid = receipt->publication.reference_kind == CTRC_REF_HEAP_ITL_UBA
		? ctrc_target_exact_itl_valid(&participant->key, final_target)
		: ctrc_target_exact_tid_valid(&receipt->publication, final_target);
	if (!target_valid)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;

	expected = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (expected == CTRC_RECEIPT_APPLIED)
	{
		if (memcmp(&receipt->target, final_target,
				   sizeof(*final_target)) != 0)
			return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
		token->valid = true;
		token->journal_sequence = receipt->publication.journal_sequence;
		token->key_sequence = receipt->publication.key_sequence;
		token->journal_slot_generation
			= receipt->publication.journal_slot_generation;
		return CLUSTER_CTRC_APPLY_APPLIED;
	}
	if (expected != CTRC_RECEIPT_PREPARED)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	target_finalizes
		= receipt->publication.reference_kind == CTRC_REF_HEAP_ITL_UBA
		? ctrc_target_itl_finalizes_exact(&receipt->target, final_target)
		: ctrc_target_offnum_finalizes_exact(
			&receipt->publication, &receipt->target, final_target);
	if (participant->state != CTRC_PARTICIPANT_OPEN || !target_finalizes)
		return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;

	receipt->target = *final_target;
	pg_write_barrier();
	expected = CTRC_RECEIPT_PREPARED;
	if (!pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&receipt->state,
									&expected, CTRC_RECEIPT_APPLIED))
		return expected == CTRC_RECEIPT_PREPARED
			? CLUSTER_CTRC_APPLY_RETRY_REQUIRED
			: CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->prepared_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->applied_count, 1);
	token->valid = true;
	token->journal_sequence = receipt->publication.journal_sequence;
	token->key_sequence = receipt->publication.key_sequence;
	token->journal_slot_generation
		= receipt->publication.journal_slot_generation;
	return CLUSTER_CTRC_APPLY_APPLIED;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_apply_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcApplyResult result;
	uint32 state_before;

	if (!ctrc_receipt_handle_exact(handle))
	{
		if (token != NULL)
			MemSet(token, 0, sizeof(*token));
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	}
	state_before = pg_atomic_read_u32(
		(pg_atomic_uint32 *)&handle->receipt->state);
	result = cluster_ctrc_receipt_apply_prepared(
		handle->participant, handle->receipt, final_target, token);
	if (result == CLUSTER_CTRC_APPLY_APPLIED
		&& state_before == CTRC_RECEIPT_PREPARED)
		cluster_ctrc_stat_bump(CTRC_STAT_RECEIPT_APPLIED);
	return result;
#else
	(void)handle;
	(void)final_target;
	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
#endif
}

static bool
ctrc_receipt_retarget_itl_exact(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcTargetV1 *final_target)
{
	const ClusterCtrcTargetV1 *current_target;
	const ClusterCtrcPublicationIdV1 *publication;

	if (participant == NULL || receipt == NULL || pending_target == NULL
		|| final_target == NULL
		|| participant->state != CTRC_PARTICIPANT_OPEN
		|| memcmp(&receipt->key, &participant->key,
			sizeof(receipt->key)) != 0)
		return false;
	publication = &receipt->publication;
	current_target = &receipt->target;
	if (publication->grant_generation != participant->grant_generation
		|| publication->reference_kind != CTRC_REF_HEAP_ITL_UBA
		|| publication->target_kind != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		|| publication->descriptor_hash != 0
		|| publication->member_ordinal != UINT16_MAX
		|| publication->member_role != 0
		|| publication->journal_sequence == 0
		|| publication->key_sequence == 0
		|| publication->journal_slot_generation == 0
		|| receipt->disposition != CTRC_RELEASE_NONE
		|| !XLogRecPtrIsInvalid(receipt->highest_local_wal_lsn)
		|| !ctrc_bytes_zero(receipt->required_lsn,
			sizeof(receipt->required_lsn))
		|| !ctrc_target_exact_itl_valid(&participant->key, current_target)
		|| !ctrc_target_pending_itl_valid(pending_target)
		|| !ctrc_target_exact_itl_valid(&participant->key, final_target)
		|| !ctrc_target_itl_finalizes_exact(pending_target, final_target))
		return false;

	return current_target->spc_oid == final_target->spc_oid
		&& current_target->db_oid == final_target->db_oid
		&& current_target->rel_number == final_target->rel_number
		&& current_target->fork_number == final_target->fork_number
		&& current_target->block_number == final_target->block_number
		&& current_target->relation_persistence
		   == final_target->relation_persistence
		&& current_target->needs_wal == final_target->needs_wal
		&& current_target->publication_own_generation
		   == pending_target->publication_own_generation
		&& current_target->publication_acquisition_epoch
		   == pending_target->publication_acquisition_epoch
		&& current_target->itl_slot_index == final_target->itl_slot_index
		&& current_target->itl_slot_wrap == final_target->itl_slot_wrap
		&& current_target->itl_xid == final_target->itl_xid
		&& current_target->itl_class == final_target->itl_class
		&& memcmp(current_target->planned_successor_sha256,
			final_target->planned_predecessor_sha256,
			sizeof(current_target->planned_successor_sha256)) == 0;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_retarget_itl(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token)
{
	uint32 expected;
	bool identity_exact;

	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	if (participant == NULL || receipt == NULL || pending_target == NULL
		|| final_target == NULL || token == NULL
		|| participant->state != CTRC_PARTICIPANT_OPEN
		|| memcmp(&receipt->key, &participant->key,
			sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation)
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;

	expected = CTRC_RECEIPT_APPLIED;
	if (!pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_RETARGETING))
		return expected == CTRC_RECEIPT_CLEANED
			|| expected == CTRC_RECEIPT_CANCELLED
			|| expected == CTRC_RECEIPT_ACK_FROZEN
			|| expected == CTRC_RECEIPT_FREE
				? CLUSTER_CTRC_APPLY_RETRY_REQUIRED
				: CLUSTER_CTRC_APPLY_FAIL_CLOSED;

	identity_exact = ctrc_receipt_retarget_itl_exact(participant, receipt,
		pending_target, final_target);
	if (!identity_exact)
	{
		expected = CTRC_RECEIPT_RETARGETING;
		if (!pg_atomic_compare_exchange_u32(
				(pg_atomic_uint32 *)&receipt->state, &expected,
				CTRC_RECEIPT_APPLIED))
			return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
		return participant->state == CTRC_PARTICIPANT_OPEN
			? CLUSTER_CTRC_APPLY_RETRY_REQUIRED
			: CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	}

	receipt->target = *final_target;
	pg_write_barrier();
	expected = CTRC_RECEIPT_RETARGETING;
	if (!pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_APPLIED))
		return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
	token->valid = true;
	token->journal_sequence = receipt->publication.journal_sequence;
	token->key_sequence = receipt->publication.key_sequence;
	token->journal_slot_generation
		= receipt->publication.journal_slot_generation;
	return CLUSTER_CTRC_APPLY_APPLIED;
}

ClusterCtrcApplyResult
cluster_ctrc_receipt_retarget_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcTargetV1 *final_target, ClusterCtrcApplyToken *token)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	if (!ctrc_receipt_handle_exact(handle))
	{
		if (token != NULL)
			MemSet(token, 0, sizeof(*token));
		return CLUSTER_CTRC_APPLY_RETRY_REQUIRED;
	}
	return cluster_ctrc_receipt_retarget_itl(handle->participant,
		handle->receipt, pending_target, final_target, token);
#else
	(void)handle;
	(void)pending_target;
	(void)final_target;
	if (token != NULL)
		MemSet(token, 0, sizeof(*token));
	return CLUSTER_CTRC_APPLY_FAIL_CLOSED;
#endif
}

bool
cluster_ctrc_receipt_itl_reuse_candidate_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *pending_target,
	const ClusterCtrcItlTargetIdentity *current_target,
	const uint8 current_slot_sha256[32])
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	const ClusterCtrcReceipt *receipt;
	const ClusterCtrcTargetV1 *target;

	if (!ctrc_receipt_handle_exact(handle) || pending_target == NULL
		|| current_target == NULL || current_slot_sha256 == NULL)
		return false;
	receipt = handle->receipt;
	target = &receipt->target;
	return pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state)
		   == CTRC_RECEIPT_APPLIED
		&& handle->participant->state == CTRC_PARTICIPANT_OPEN
		&& receipt->publication.reference_kind == CTRC_REF_HEAP_ITL_UBA
		&& receipt->publication.target_kind
		   == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		&& receipt->publication.grant_generation
		   == handle->participant->grant_generation
		&& receipt->disposition == CTRC_RELEASE_NONE
		&& XLogRecPtrIsInvalid(receipt->highest_local_wal_lsn)
		&& ctrc_bytes_zero(receipt->required_lsn,
			sizeof(receipt->required_lsn))
		&& ctrc_target_exact_itl_valid(&handle->participant->key, target)
		&& ctrc_target_pending_itl_valid(pending_target)
		&& target->spc_oid == pending_target->spc_oid
		&& target->db_oid == pending_target->db_oid
		&& target->rel_number == pending_target->rel_number
		&& target->fork_number == pending_target->fork_number
		&& target->block_number == pending_target->block_number
		&& target->relation_persistence
		   == pending_target->relation_persistence
		&& target->needs_wal == pending_target->needs_wal
		&& target->publication_own_generation
		   == pending_target->publication_own_generation
		&& target->publication_acquisition_epoch
		   == pending_target->publication_acquisition_epoch
		&& cluster_ctrc_itl_target_identity_matches(target, current_target)
		&& memcmp(target->planned_successor_sha256, current_slot_sha256,
			sizeof(target->planned_successor_sha256)) == 0;
#else
	(void)handle;
	(void)pending_target;
	(void)current_target;
	(void)current_slot_sha256;
	return false;
#endif
}

bool
cluster_ctrc_receipt_cancel_prepared(ClusterCtrcParticipantEntry *participant,
								 ClusterCtrcReceipt *receipt)
{
	uint32 expected = CTRC_RECEIPT_PREPARED;

	if (participant == NULL || receipt == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| participant->state != CTRC_PARTICIPANT_OPEN
		|| !pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_CANCELLED))
		return false;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->prepared_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->cancelled_count, 1);
	receipt->disposition = CTRC_RELEASE_CANCELLED_PREMUTATION;
	return true;
}

bool
cluster_ctrc_receipt_cancel_shared(const ClusterCtrcReceiptHandle *handle)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	bool cancelled = ctrc_receipt_handle_exact(handle)
		&& cluster_ctrc_receipt_cancel_prepared(
			handle->participant, handle->receipt);

	if (cancelled)
	{
		cluster_ctrc_stat_bump(CTRC_STAT_RECEIPT_CANCELLED);
		cluster_undo_cleaner_wakeup();
	}
	return cancelled;
#else
	(void)handle;
	return false;
#endif
}

static bool
ctrc_reference_durability_covers(
	const ClusterCtrcTargetV1 *target,
	XLogRecPtr highest_local_lsn,
	const XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS],
	const ClusterCtrcDurability *durability)
{
	int i;

	if (target == NULL || required_lsn == NULL || durability == NULL)
		return false;
	if (target->needs_wal)
	{
		if (XLogRecPtrIsInvalid(highest_local_lsn)
			|| XLogRecPtrIsInvalid(durability->local_flush_lsn)
			|| durability->local_flush_lsn < highest_local_lsn)
			return false;
	}
	else if (!XLogRecPtrIsInvalid(highest_local_lsn)
			 && (XLogRecPtrIsInvalid(durability->local_flush_lsn)
				 || durability->local_flush_lsn < highest_local_lsn))
		return false;
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
	{
		if (!XLogRecPtrIsInvalid(required_lsn[i])
			&& (XLogRecPtrIsInvalid(durability->durable_lsn[i])
				|| durability->durable_lsn[i] < required_lsn[i]))
			return false;
	}
	return true;
}

bool
cluster_ctrc_itl_target_identity_matches(
	const ClusterCtrcTargetV1 *target,
	const ClusterCtrcItlTargetIdentity *expected)
{
	return target != NULL && expected != NULL
		&& target->kind == CTRC_TARGET_EXACT_ITL_SLOT
		&& target->spc_oid == expected->spc_oid
		&& target->db_oid == expected->db_oid
		&& target->rel_number == expected->rel_number
		&& target->fork_number == expected->fork_number
		&& target->block_number == expected->block_number
		&& target->itl_slot_index == expected->itl_slot_index
		&& target->itl_slot_wrap == expected->itl_slot_wrap
		&& target->itl_xid == expected->itl_xid
		&& target->itl_class == expected->itl_class
		&& target->needs_wal == expected->needs_wal
		&& ctrc_bytes_zero(expected->reserved8, sizeof(expected->reserved8))
		&& memcmp(target->uba, expected->uba, sizeof(target->uba)) == 0;
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_itl(ClusterCtrcParticipantEntry *participant,
								   ClusterCtrcReceipt *receipt,
								   ClusterCtrcItlProjection projection,
								   const ClusterCtrcDurability *durability)
{
	ClusterCtrcReleaseDisposition disposition;
	uint32 expected;

	if (participant == NULL || receipt == NULL || durability == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| (participant->state != CTRC_PARTICIPANT_OPEN
			&& participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING)
		|| receipt->publication.reference_kind != CTRC_REF_HEAP_ITL_UBA
		|| receipt->publication.target_kind
		   != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		|| !ctrc_target_exact_itl_valid(&participant->key, &receipt->target))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;
	if (projection == CTRC_ITL_TERMINAL_INDEPENDENT)
		disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	else if (projection == CTRC_ITL_TARGET_ABSENT)
		disposition = CTRC_RELEASE_CLEANED_ABSENT;
	else
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	expected = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (expected == CTRC_RECEIPT_CLEANED)
	{
		return receipt->disposition == (uint8)disposition
			&& ctrc_reference_durability_covers(
				&receipt->target, receipt->highest_local_wal_lsn,
				receipt->required_lsn, durability)
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	}
	if (expected != CTRC_RECEIPT_APPLIED
		|| !ctrc_reference_durability_covers(
			&receipt->target, durability->highest_local_lsn,
			durability->required_lsn, durability))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	receipt->highest_local_wal_lsn = durability->highest_local_lsn;
	memcpy(receipt->required_lsn, durability->required_lsn,
		   sizeof(receipt->required_lsn));
	receipt->disposition = (uint8)disposition;
	pg_write_barrier();
	expected = CTRC_RECEIPT_APPLIED;
	if (!pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_CLEANED))
		return expected == CTRC_RECEIPT_CLEANED
			&& receipt->disposition == (uint8)disposition
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->applied_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->cleaned_count, 1);
	return CLUSTER_CTRC_DISCHARGE_CLEANED;
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_itl_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcItlTargetIdentity *expected_target,
	ClusterCtrcItlProjection projection,
	const ClusterCtrcDurability *durability)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcDischargeResult result;
	uint32 state_before;

	if (!ctrc_receipt_handle_exact(handle))
	{
		cluster_ctrc_stat_bump(CTRC_STAT_TARGET_RETAINED);
		return CLUSTER_CTRC_DISCHARGE_RETAIN;
	}
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	state_before = pg_atomic_read_u32(
		(pg_atomic_uint32 *)&handle->receipt->state);
	result = ctrc_receipt_handle_exact(handle)
		&& cluster_ctrc_itl_target_identity_matches(
			&handle->receipt->target, expected_target)
		? cluster_ctrc_receipt_discharge_itl(
			handle->participant, handle->receipt, projection, durability)
		: CLUSTER_CTRC_DISCHARGE_RETAIN;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	if (result == CLUSTER_CTRC_DISCHARGE_CLEANED)
	{
		if (state_before == CTRC_RECEIPT_APPLIED)
			cluster_ctrc_stat_bump(projection == CTRC_ITL_TARGET_ABSENT
				? CTRC_STAT_TARGET_ABSENT : CTRC_STAT_TARGET_REWRITTEN);
		cluster_undo_cleaner_wakeup();
	}
	else
		cluster_ctrc_stat_bump(CTRC_STAT_TARGET_RETAINED);
	return result;
#else
	(void)handle;
	(void)expected_target;
	(void)projection;
	(void)durability;
	return CLUSTER_CTRC_DISCHARGE_RETAIN;
#endif
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_current_mx(
	ClusterCtrcParticipantEntry *participant, ClusterCtrcReceipt *receipt,
	const ClusterCtrcTargetV1 *expected_target,
	ClusterCtrcCleanResult clean_result,
	const ClusterCtrcDurability *durability)
{
	ClusterCtrcReleaseDisposition disposition;
	uint32 expected;

	if (clean_result == CTRC_CLEANED_ABSENT)
		disposition = CTRC_RELEASE_CLEANED_ABSENT;
	else if (clean_result == CTRC_CLEANED_TERMINAL_REWRITE)
		disposition = CTRC_RELEASE_CLEANED_TERMINAL_REWRITE;
	else if (clean_result == CTRC_CLEANED_SUCCESSOR_REPLACED)
		disposition = CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED;
	else
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	if (participant == NULL || receipt == NULL || expected_target == NULL
		|| durability == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation
		|| (participant->state != CTRC_PARTICIPANT_OPEN
			&& participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING)
		|| receipt->publication.reference_kind
		   < CTRC_REF_CURRENT_MX_LOCKER
		|| receipt->publication.reference_kind > CTRC_REF_HOT_FOLLOW_EDGE
		|| receipt->publication.target_kind
		   != CTRC_TARGET_PAGE_PENDING_OFFNUM
		|| !ctrc_target_exact_tid_valid(
			&receipt->publication, &receipt->target)
		|| memcmp(&receipt->target, expected_target,
				  sizeof(receipt->target)) != 0)
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	expected = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (expected == CTRC_RECEIPT_CLEANED)
	{
		return receipt->disposition == (uint8)disposition
			&& ctrc_reference_durability_covers(
				&receipt->target, receipt->highest_local_wal_lsn,
				receipt->required_lsn, durability)
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	}
	if (expected != CTRC_RECEIPT_APPLIED
		|| !ctrc_reference_durability_covers(
			&receipt->target, durability->highest_local_lsn,
			durability->required_lsn, durability))
		return CLUSTER_CTRC_DISCHARGE_RETAIN;

	receipt->highest_local_wal_lsn = durability->highest_local_lsn;
	memcpy(receipt->required_lsn, durability->required_lsn,
		   sizeof(receipt->required_lsn));
	receipt->disposition = (uint8)disposition;
	pg_write_barrier();
	expected = CTRC_RECEIPT_APPLIED;
	if (!pg_atomic_compare_exchange_u32(
			(pg_atomic_uint32 *)&receipt->state, &expected,
			CTRC_RECEIPT_CLEANED))
		return expected == CTRC_RECEIPT_CLEANED
			&& receipt->disposition == (uint8)disposition
				? CLUSTER_CTRC_DISCHARGE_CLEANED
				: CLUSTER_CTRC_DISCHARGE_RETAIN;
	(void)pg_atomic_fetch_sub_u64(
		(pg_atomic_uint64 *)&participant->applied_count, 1);
	(void)pg_atomic_fetch_add_u64(
		(pg_atomic_uint64 *)&participant->cleaned_count, 1);
	return CLUSTER_CTRC_DISCHARGE_CLEANED;
}

ClusterCtrcDischargeResult
cluster_ctrc_receipt_discharge_current_mx_shared(
	const ClusterCtrcReceiptHandle *handle,
	const ClusterCtrcTargetV1 *expected_target,
	ClusterCtrcCleanResult clean_result,
	const ClusterCtrcDurability *durability)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcDischargeResult result;
	uint32 state_before;

	if (!ctrc_receipt_handle_exact(handle))
	{
		cluster_ctrc_stat_bump(CTRC_STAT_TARGET_RETAINED);
		return CLUSTER_CTRC_DISCHARGE_RETAIN;
	}
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	state_before = pg_atomic_read_u32(
		(pg_atomic_uint32 *)&handle->receipt->state);
	result = ctrc_receipt_handle_exact(handle)
		? cluster_ctrc_receipt_discharge_current_mx(
			handle->participant, handle->receipt, expected_target,
			clean_result, durability)
		: CLUSTER_CTRC_DISCHARGE_RETAIN;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	if (result == CLUSTER_CTRC_DISCHARGE_CLEANED)
	{
		if (state_before == CTRC_RECEIPT_APPLIED)
			cluster_ctrc_stat_bump(clean_result == CTRC_CLEANED_ABSENT
				? CTRC_STAT_TARGET_ABSENT : CTRC_STAT_TARGET_REWRITTEN);
		cluster_undo_cleaner_wakeup();
	}
	else
		cluster_ctrc_stat_bump(CTRC_STAT_TARGET_RETAINED);
	return result;
#else
	(void)handle;
	(void)expected_target;
	(void)clean_result;
	(void)durability;
	return CLUSTER_CTRC_DISCHARGE_RETAIN;
#endif
}

ClusterCtrcCloseResult
cluster_ctrc_participant_close_or_tombstone(
	ClusterCtrcParticipantEntry *participant, const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation)
{
	ClusterCtrcParticipantOpenResult open_result;

	if (participant == NULL || !ctrc_txn_key_valid(key)
		|| !ctrc_participant_identity_valid(key, identity)
		|| grant_generation == 0 || seal_generation == 0)
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;

	if (ctrc_bytes_zero(participant, sizeof(*participant)))
	{
		open_result = cluster_ctrc_participant_open(participant, key,
			grant_generation, identity);
		if (open_result != CLUSTER_CTRC_PARTICIPANT_OPENED)
			return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}
	else if (memcmp(&participant->key, key, sizeof(*key)) != 0
			 || memcmp(&participant->identity, identity,
					   sizeof(*identity)) != 0
			 || participant->grant_generation != grant_generation)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}

	return cluster_ctrc_participant_close(participant, identity,
		grant_generation, seal_generation);
}

static bool
ctrc_ack_bytes_exact(const ClusterCtrcLocalReleaseAckV1 *ack)
{
	uint8 encoded[CLUSTER_CTRC_LOCAL_ACK_BYTES];

	return ack != NULL
		&& cluster_ctrc_local_release_ack_encode(ack, encoded)
		&& ack->crc32c == ctrc_get_u32_le(encoded + 412);
}

static bool
ctrc_frozen_ack_summary_exact(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcLocalReleaseAckV1 *summary)
{
	return participant != NULL && summary != NULL
		&& participant->state == CTRC_PARTICIPANT_ACK_FROZEN
		&& ctrc_ack_bytes_exact(summary)
		&& memcmp(&summary->transaction_key, &participant->key,
				  sizeof(participant->key)) == 0
		&& summary->grant_generation == participant->grant_generation
		&& summary->seal_generation == participant->seal_generation
		&& summary->participant_node_id == participant->identity.node_id
		&& summary->capability_record_generation
		   == participant->identity.capability_record_generation
		&& summary->participant_boot_incarnation
		   == participant->identity.boot_incarnation
		&& summary->formation_epoch == participant->identity.formation_epoch
		&& summary->admission_record_generation
		   == participant->identity.admission_record_generation;
}

bool
cluster_ctrc_origin_ack_land_entry(
	ClusterCtrcOriginEntry *origin, uint64 request_id,
	const ClusterCtrcLocalReleaseAckV1 *ack,
	ClusterCtrcLocalReleaseAckV1 *ack_slot)
{
	uint16 node_id;
	uint32 node_bit;

	if (origin == NULL || ack == NULL || ack_slot == NULL
		|| request_id == 0
		|| (origin->state != CTRC_ORIGIN_SEALING
			&& origin->state != CTRC_ORIGIN_SEALED
			&& origin->state != CTRC_ORIGIN_CLEANING
			&& origin->state != CTRC_ORIGIN_CERTIFYING)
		|| origin->seal_generation == 0)
		return false;
	node_id = ack->participant_node_id;
	if (node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS)
		return false;
	node_bit = UINT32_C(1) << node_id;
	if ((origin->touched_bitmap & node_bit) == 0
		|| (origin->close_dispatched_bitmap & node_bit) == 0
		|| origin->close_request_id[node_id] != request_id)
		return false;

	if (!ctrc_ack_bytes_exact(ack)
		|| memcmp(&origin->key, &ack->transaction_key,
				  sizeof(origin->key)) != 0
		|| origin->grant_generation != ack->grant_generation
		|| origin->seal_generation != ack->seal_generation
		|| ack->participant_node_id != origin->touched[node_id].node_id
		|| ack->capability_record_generation
		   != origin->touched[node_id].capability_record_generation
		|| ack->participant_boot_incarnation
		   != origin->touched[node_id].boot_incarnation
		|| ack->formation_epoch
		   != origin->touched[node_id].formation_epoch
		|| ack->admission_record_generation
		   != origin->touched[node_id].admission_record_generation)
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}

	if ((origin->ack_bitmap & node_bit) != 0)
	{
		if (memcmp(ack_slot, ack, sizeof(*ack)) == 0)
			return true;
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	if (!ctrc_bytes_zero(ack_slot, sizeof(*ack_slot)))
	{
		origin->state = CTRC_ORIGIN_BLOCKED;
		return false;
	}
	*ack_slot = *ack;
	origin->ack_bitmap |= node_bit;
	origin->close_confirmed_bitmap |= node_bit;
	if (origin->state == CTRC_ORIGIN_SEALING
		&& origin->close_confirmed_bitmap == origin->touched_bitmap)
		origin->state = CTRC_ORIGIN_SEALED;
	else if (origin->state == CTRC_ORIGIN_CLEANING
			 && origin->ack_bitmap == origin->touched_bitmap)
		origin->state = CTRC_ORIGIN_CERTIFYING;
	return true;
}

bool
cluster_ctrc_origin_ack_land_shared(
	uint64 request_id, const ClusterCtrcLocalReleaseAckV1 *ack)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcOriginEntry *origin;
	uint64 origin_index;
	uint64 ack_index;
	bool landed;
	bool progressed;
	uint32 ack_before;

	if (ack == NULL || !cluster_ctrc_shmem_ready()
		|| !ctrc_origin_index(&ack->transaction_key, &origin_index)
		|| !ctrc_origin_ack_index(&ack->transaction_key,
			ack->participant_node_id, &ack_index))
		return false;
	SpinLockAcquire(&CtrcShared->origin_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	origin = &ctrc_origin_entries()[origin_index];
	ack_before = origin->ack_bitmap;
	landed = cluster_ctrc_origin_ack_land_entry(origin, request_id, ack,
		&ctrc_origin_ack_entries()[ack_index]);
	progressed = landed && ack_before != origin->ack_bitmap;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->origin_lock);
	if (progressed)
		ctrc_semantic_progress(true);
	return landed;
#else
	(void)request_id;
	(void)ack;
	return false;
#endif
}

ClusterCtrcSealReplyResult
cluster_ctrc_participant_request_apply(
	ClusterCtrcParticipantEntry *participant,
	ClusterCtrcLocalReleaseAckV1 *ack_summary,
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation, ClusterCtrcSealSuboperation suboperation,
	uint16 *first_reason, ClusterCtrcLocalReleaseAckV1 *ack_out)
{
	ClusterCtrcCloseResult close_result;
	ClusterCtrcDurability durability;

	if (first_reason != NULL)
		*first_reason = CTRC_SEAL_REASON_MALFORMED;
	if (ack_out != NULL)
		MemSet(ack_out, 0, sizeof(*ack_out));
	if (participant == NULL || ack_summary == NULL || key == NULL
		|| identity == NULL || first_reason == NULL || ack_out == NULL
		|| !ctrc_seal_suboperation_valid((uint8)suboperation))
		return CTRC_SEAL_REPLY_DENIED;

	if (suboperation == CTRC_SEAL_CERTIFICATE_COMMITTED)
	{
		if (participant->state != CTRC_PARTICIPANT_ACK_FROZEN
			|| memcmp(&participant->key, key, sizeof(*key)) != 0
			|| memcmp(&participant->identity, identity,
					  sizeof(*identity)) != 0
			|| participant->grant_generation != grant_generation
			|| participant->seal_generation != seal_generation
			|| !ctrc_frozen_ack_summary_exact(participant, ack_summary))
		{
			*first_reason = CTRC_SEAL_REASON_IDENTITY;
			return CTRC_SEAL_REPLY_BLOCKED_RETAIN;
		}
		MemSet(ack_summary, 0, sizeof(*ack_summary));
		MemSet(participant, 0, sizeof(*participant));
		*first_reason = 0;
		return CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED;
	}

	close_result = cluster_ctrc_participant_close_or_tombstone(
		participant, key, identity, grant_generation, seal_generation);
	if (close_result == CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN)
	{
		*first_reason = CTRC_SEAL_REASON_IDENTITY;
		return CTRC_SEAL_REPLY_BLOCKED_RETAIN;
	}
	if (close_result == CLUSTER_CTRC_CLOSE_PENDING_DRAIN)
	{
		*first_reason = participant->prepared_count != 0
			? CTRC_SEAL_REASON_PREPARED : CTRC_SEAL_REASON_CLEANOUT;
		return CTRC_SEAL_REPLY_PENDING_DRAIN;
	}

	if (participant->state == CTRC_PARTICIPANT_ACK_FROZEN)
	{
		if (!ctrc_frozen_ack_summary_exact(participant, ack_summary))
		{
			*first_reason = CTRC_SEAL_REASON_ACK_UNAVAILABLE;
			return CTRC_SEAL_REPLY_BLOCKED_RETAIN;
		}
		*ack_out = *ack_summary;
		*first_reason = 0;
		return CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK;
	}
	if (participant->receipt_count != 0)
	{
		/* The existing undo cleaner hashes and double-samples nonempty rows
		 * outside the short participant/receipt locks.  Until that immutable
		 * summary is installed, CLOSE is pending rather than a permanent
		 * retain failure. */
		*first_reason = CTRC_SEAL_REASON_CLEANOUT;
		return CTRC_SEAL_REPLY_PENDING_DRAIN;
	}

	MemSet(&durability, 0, sizeof(durability));
	if (cluster_ctrc_participant_build_ack(participant, &durability, ack_out)
		!= CLUSTER_CTRC_ACK_RELEASED)
	{
		*first_reason = CTRC_SEAL_REASON_ACK_UNAVAILABLE;
		return CTRC_SEAL_REPLY_BLOCKED_RETAIN;
	}
	*ack_summary = *ack_out;
	*first_reason = 0;
	return CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK;
}

ClusterCtrcSealReplyResult
cluster_ctrc_participant_request_shared(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcParticipantIdentity *identity, uint32 grant_generation,
	uint64 seal_generation, ClusterCtrcSealSuboperation suboperation,
	uint16 *first_reason, ClusterCtrcLocalReleaseAckV1 *ack_out)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcLocalReleaseAckV1 *ack_summary;
	ClusterCtrcParticipantEntry participant_before;
	ClusterCtrcLocalReleaseAckV1 ack_summary_before;
	ClusterCtrcSealReplyResult result;
	ClusterCtrcParticipantState state_before;
	uint64 participant_index;
	bool progressed;

	if (first_reason != NULL)
		*first_reason = CTRC_SEAL_REASON_MALFORMED;
	if (ack_out != NULL)
		MemSet(ack_out, 0, sizeof(*ack_out));
	if (key == NULL || identity == NULL || first_reason == NULL
		|| ack_out == NULL || cluster_node_id < 0
		|| identity->node_id != (uint16)cluster_node_id
		|| !cluster_ctrc_shmem_ready()
		|| !ctrc_participant_index(key, identity->node_id,
			&participant_index))
		return CTRC_SEAL_REPLY_DENIED;

	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	participant = &ctrc_participant_entries()[participant_index];
	ack_summary = &ctrc_participant_ack_entries()[participant_index];
	participant_before = *participant;
	ack_summary_before = *ack_summary;
	state_before = (ClusterCtrcParticipantState)participant->state;
	result = cluster_ctrc_participant_request_apply(
		participant, ack_summary, key, identity,
		grant_generation, seal_generation, suboperation,
		first_reason, ack_out);
	if (result == CTRC_SEAL_REPLY_CERTIFICATE_RECLAIMED
		&& (participant_before.receipt_count
				!= ack_summary_before.total_receipt_count
			|| participant_before.ack_frozen_count
				   != ack_summary_before.ack_frozen_count
			|| !cluster_ctrc_receipt_reclaim_frozen_table_locked(
				&participant_before.key,
				ack_summary_before.total_receipt_count,
				ctrc_receipt_entries(), ctrc_receipt_probe_states(),
				CtrcShared->receipt_entries, NULL)))
	{
		/* The pure FSM has already cleared the participant and summary.
		 * Restore their exact frozen bytes before retaining this identity;
		 * receipt reclamation itself is two-pass and cannot be partial. */
		*participant = participant_before;
		*ack_summary = ack_summary_before;
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		*first_reason = CTRC_SEAL_REASON_ACK_UNAVAILABLE;
		result = CTRC_SEAL_REPLY_BLOCKED_RETAIN;
	}
	progressed = result != CTRC_SEAL_REPLY_BLOCKED_RETAIN && result != CTRC_SEAL_REPLY_DENIED
				 && memcmp(participant, &participant_before, sizeof(participant_before)) != 0;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	if (progressed)
		ctrc_semantic_progress(true);
	if (result == CTRC_SEAL_REPLY_PENDING_DRAIN)
	{
		cluster_ctrc_cleaner_reason_set(
			*first_reason == CTRC_SEAL_REASON_PREPARED
				? CTRC_CLEANER_REASON_PREPARED_DRAIN
				: CTRC_CLEANER_REASON_PAGE_REVALIDATE);
		if (!progressed)
			cluster_ctrc_stat_bump(CTRC_STAT_PENDING_DUPLICATE);
	}
	else if (result == CTRC_SEAL_REPLY_LOCAL_RELEASE_ACK)
	{
		cluster_ctrc_stat_bump(
			state_before == CTRC_PARTICIPANT_ACK_FROZEN
				? CTRC_STAT_ACK_RESENT : CTRC_STAT_ACK_FROZEN);
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);
	}
	else if (result == CTRC_SEAL_REPLY_BLOCKED_RETAIN
			 || result == CTRC_SEAL_REPLY_DENIED)
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_BLOCKED);
	return result;
#else
	(void)key;
	(void)identity;
	(void)grant_generation;
	(void)seal_generation;
	(void)suboperation;
	if (first_reason != NULL)
		*first_reason = CTRC_SEAL_REASON_MALFORMED;
	if (ack_out != NULL)
		MemSet(ack_out, 0, sizeof(*ack_out));
	return CTRC_SEAL_REPLY_DENIED;
#endif
}

ClusterCtrcCloseResult
cluster_ctrc_participant_close(ClusterCtrcParticipantEntry *participant,
								   const ClusterCtrcParticipantIdentity *identity,
								   uint32 grant_generation,
								   uint64 seal_generation)
{
	uint64 terminal_count;

	if (participant == NULL)
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	if (participant->state == CTRC_PARTICIPANT_BLOCKED)
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	if (identity == NULL || grant_generation == 0 || seal_generation == 0
		|| !ctrc_txn_key_valid(&participant->key)
		|| !ctrc_participant_identity_valid(&participant->key, identity)
		|| memcmp(&participant->identity, identity, sizeof(*identity)) != 0
		|| participant->grant_generation != grant_generation
		|| !ctrc_bytes_zero(participant->reserved8,
								sizeof(participant->reserved8)))
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}

	if (participant->state == CTRC_PARTICIPANT_OPEN)
	{
		if (participant->seal_generation != 0)
		{
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
		}
		participant->seal_generation = seal_generation;
		participant->state = CTRC_PARTICIPANT_CLOSED_DRAINING;
	}
	else if ((participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING
			  && participant->state != CTRC_PARTICIPANT_ACK_READY
			  && participant->state != CTRC_PARTICIPANT_ACK_FROZEN)
			 || participant->seal_generation != seal_generation)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}

	if (participant->next_key_sequence == 0
		|| participant->last_key_sequence == UINT64_MAX
		|| participant->next_key_sequence
		   != participant->last_key_sequence + 1
		|| participant->receipt_count != participant->last_key_sequence
		|| participant->prepared_count > participant->receipt_count
		|| participant->applied_count > participant->receipt_count
		|| participant->cancelled_count > participant->receipt_count
		|| participant->cleaned_count > participant->receipt_count)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}
	if (participant->prepared_count != 0 || participant->applied_count != 0)
		return CLUSTER_CTRC_CLOSE_PENDING_DRAIN;

	terminal_count = participant->cancelled_count + participant->cleaned_count;
	if (terminal_count < participant->cancelled_count
		|| terminal_count != participant->receipt_count)
	{
		participant->state = CTRC_PARTICIPANT_BLOCKED;
		return CLUSTER_CTRC_CLOSE_BLOCKED_RETAIN;
	}
	if (participant->state == CTRC_PARTICIPANT_CLOSED_DRAINING)
		participant->state = CTRC_PARTICIPANT_ACK_READY;
	return CLUSTER_CTRC_CLOSE_ACK_READY;
}

ClusterCtrcLossResult
cluster_ctrc_participant_note_owner_loss(ClusterCtrcParticipantEntry *participant,
									 ClusterCtrcReceipt *receipt)
{
	uint32 state;
	uint32 expected;

	if (participant == NULL)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	participant->state = CTRC_PARTICIPANT_BLOCKED;
	if (receipt == NULL
		|| memcmp(&receipt->key, &participant->key,
				  sizeof(receipt->key)) != 0
		|| receipt->publication.grant_generation
		   != participant->grant_generation)
		return CLUSTER_CTRC_LOSS_BLOCKED;

	state = pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state);
	if (state == CTRC_RECEIPT_BLOCKED)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	if (state != CTRC_RECEIPT_PREPARED && state != CTRC_RECEIPT_APPLIED
		&& state != CTRC_RECEIPT_RETARGETING)
		return CLUSTER_CTRC_LOSS_BLOCKED;
	expected = state;
	if (!pg_atomic_compare_exchange_u32((pg_atomic_uint32 *)&receipt->state,
									&expected, CTRC_RECEIPT_BLOCKED))
		return CLUSTER_CTRC_LOSS_BLOCKED;
	if (state == CTRC_RECEIPT_PREPARED)
		(void)pg_atomic_fetch_sub_u64(
			(pg_atomic_uint64 *)&participant->prepared_count, 1);
	else
		(void)pg_atomic_fetch_sub_u64(
			(pg_atomic_uint64 *)&participant->applied_count, 1);
	return CLUSTER_CTRC_LOSS_BLOCKED;
}

ClusterCtrcCleanResult
cluster_ctrc_clean_reference(const ClusterCtrcCleanReferenceInput *input)
{
	if (input == NULL || !input->page_authority_exact)
		return CTRC_CLEAN_RETAIN;

	switch ((ClusterCtrcTargetState)input->target_state)
	{
		case CTRC_TARGET_ABSENT:
			return input->source_transition_censused
				? CTRC_CLEANED_ABSENT : CTRC_CLEAN_RETAIN;
		case CTRC_TARGET_TERMINAL_LOCK_ONLY:
		case CTRC_TARGET_ABORTED_UPDATER:
			if (input->unknown_companions != 0)
				return CTRC_CLEAN_RETAIN;
			if (input->active_companions == 0)
				return CTRC_CLEANED_TERMINAL_REWRITE;
			return input->successor_topology_exact
				? CTRC_CLEANED_SUCCESSOR_REPLACED
				: CTRC_CLEAN_RETAIN;
		case CTRC_TARGET_COMMITTED_UPDATER:
			if (input->active_companions != 0
				|| input->unknown_companions != 0)
				return CTRC_CLEAN_RETAIN;
			return input->successor_topology_exact
				? CTRC_CLEANED_TERMINAL_REWRITE
				: CTRC_CLEAN_RETAIN;
		case CTRC_TARGET_ACTIVE_SURVIVOR:
			return input->unknown_companions == 0
				&& input->successor_topology_exact
				? CTRC_CLEANED_SUCCESSOR_REPLACED
				: CTRC_CLEAN_RETAIN;
		case CTRC_TARGET_AMBIGUOUS:
		default:
			return CTRC_CLEAN_RETAIN;
	}
}

/*
 * MXA-I01/I02/I17: a current-MX receipt may be held by a participant other
 * than the transaction origin.  cluster_multixact_current_members_resolve()
 * has already asked each member origin to sample its unique physical TT slot
 * and has rejected a partial, wrong-source or wrong-ordinal batch.  Bind that
 * terminal decision back to the closed receipt's xid/epoch/member role here;
 * CTRC remains reference-release evidence and does not create the verdict.
 */
bool
cluster_ctrc_current_mx_terminal_proof_exact(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCurrentMxKey *descriptor_key,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	ClusterCtrcTerminalStatus *terminal_status_out, SCN *commit_scn_out)
{
	const ClusterCurrentMxMemberDesc *member;
	const ClusterCurrentMemberProof *proof;
	uint16 ordinal;

	if (terminal_status_out != NULL)
		*terminal_status_out = CTRC_TERMINAL_UNKNOWN;
	if (commit_scn_out != NULL)
		*commit_scn_out = InvalidScn;
	if (!ctrc_txn_key_valid(key) || descriptor_key == NULL
		|| publication == NULL || members == NULL || proofs == NULL
		|| nmembers == 0 || nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| descriptor_key->origin_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| descriptor_key->reserved16 != 0
		|| !MultiXactIdIsValid(descriptor_key->multixact_id)
		|| descriptor_key->cluster_epoch != key->cluster_epoch
		|| descriptor_key->reserved32 != 0
		|| publication->descriptor_hash == 0
		|| publication->reference_kind < CTRC_REF_CURRENT_MX_LOCKER
		|| publication->reference_kind > CTRC_REF_HOT_FOLLOW_EDGE
		|| publication->member_ordinal >= nmembers)
		return false;

	ordinal = publication->member_ordinal;
	member = &members[ordinal];
	proof = &proofs[ordinal];
	if (!TransactionIdIsNormal(member->xid)
		|| member->member_status > MaxMultiXactStatus
		|| !ctrc_bytes_zero(member->reserved8, sizeof(member->reserved8))
		|| key->xid != member->xid
		|| publication->member_role != member->member_status + 1
		|| proof->member_xid != member->xid
		|| proof->member_ordinal != ordinal
		|| proof->member_status != member->member_status
		|| ClusterCurrentMemberProofGetCtrcGrant(proof) != 0
		|| !ctrc_bytes_zero(&proof->key, sizeof(proof->key)))
		return false;

	if (proof->state == CCM_COMMITTED && SCN_VALID(proof->commit_scn))
	{
		if (terminal_status_out != NULL)
			*terminal_status_out = CTRC_TERMINAL_COMMITTED;
		if (commit_scn_out != NULL)
			*commit_scn_out = proof->commit_scn;
		return true;
	}
	if (proof->state == CCM_ABORTED
		&& proof->commit_scn == InvalidScn)
	{
		if (terminal_status_out != NULL)
			*terminal_status_out = CTRC_TERMINAL_ABORTED;
		return true;
	}
	return false;
}

bool
cluster_ctrc_current_mx_rewrite_plan(
	const ClusterCtrcTxnKeyV1 *key,
	const ClusterCtrcPublicationIdV1 *publication,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs, uint16 nmembers,
	ClusterCtrcCurrentMxRewritePlan *plan)
{
	TransactionId committed_updater_xid = InvalidTransactionId;
	SCN committed_updater_scn = InvalidScn;
	bool descriptor_has_updater = false;
	uint16 i;

	if (plan != NULL)
		MemSet(plan, 0, sizeof(*plan));
	if (key == NULL || publication == NULL || members == NULL
		|| proofs == NULL || plan == NULL || !ctrc_txn_key_valid(key)
		|| nmembers == 0 || nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| publication->member_ordinal >= nmembers
		|| publication->member_role == 0
		|| publication->reference_kind < CTRC_REF_CURRENT_MX_LOCKER
		|| publication->reference_kind > CTRC_REF_HOT_FOLLOW_EDGE)
		return false;

	for (i = 0; i < nmembers; i++)
	{
		bool updater;

		if (!TransactionIdIsNormal(members[i].xid)
			|| members[i].member_status > MaxMultiXactStatus
			|| proofs[i].member_xid != members[i].xid
			|| proofs[i].member_ordinal != i
			|| proofs[i].member_status != members[i].member_status
			|| proofs[i].state > CCM_UNKNOWN)
			return false;
		updater = ISUPDATE_from_mxstatus(members[i].member_status);
		if (updater)
		{
			if (descriptor_has_updater)
				return false;
			descriptor_has_updater = true;
		}

		switch ((ClusterCurrentMemberState)proofs[i].state)
		{
			case CCM_SELF:
			case CCM_ACTIVE:
				if (ClusterCurrentMemberProofGetCtrcGrant(&proofs[i]) == 0
					|| SCN_VALID(proofs[i].commit_scn))
					return false;
				plan->survivors[plan->survivor_count].xid
					= members[i].xid;
				plan->survivors[plan->survivor_count].status
					= (MultiXactStatus)members[i].member_status;
				plan->survivor_count++;
				break;
			case CCM_COMMITTED:
				if (ClusterCurrentMemberProofGetCtrcGrant(&proofs[i]) != 0
					|| !SCN_VALID(proofs[i].commit_scn))
					return false;
				if (updater)
				{
					committed_updater_xid = members[i].xid;
					committed_updater_scn = proofs[i].commit_scn;
				}
				break;
			case CCM_ABORTED:
				if (ClusterCurrentMemberProofGetCtrcGrant(&proofs[i]) != 0
					|| SCN_VALID(proofs[i].commit_scn))
					return false;
				break;
			case CCM_UNKNOWN:
			default:
				return false;
		}
	}

	i = publication->member_ordinal;
	if (!TransactionIdEquals(key->xid, members[i].xid)
		|| publication->member_role != members[i].member_status + 1
		|| (publication->reference_kind == CTRC_REF_CURRENT_MX_LOCKER
			&& ISUPDATE_from_mxstatus(members[i].member_status))
		|| (publication->reference_kind == CTRC_REF_CURRENT_MX_UPDATER
			&& !ISUPDATE_from_mxstatus(members[i].member_status))
		|| (publication->reference_kind == CTRC_REF_HOT_FOLLOW_EDGE
			&& !ISUPDATE_from_mxstatus(members[i].member_status))
		|| proofs[i].state == CCM_ACTIVE || proofs[i].state == CCM_SELF)
		return false;

	/* A terminal committed updater is the semantic consequence.  It cannot
	 * be represented beside an ACTIVE survivor in a replacement descriptor. */
	if (TransactionIdIsValid(committed_updater_xid)
		&& plan->survivor_count != 0)
	{
		MemSet(plan, 0, sizeof(*plan));
		return false;
	}
	if (plan->survivor_count != 0)
	{
		plan->kind = CTRC_CURRENT_MX_REWRITE_SUCCESSOR;
		plan->clean_result = CTRC_CLEANED_SUCCESSOR_REPLACED;
		return true;
	}
	if (TransactionIdIsValid(committed_updater_xid))
	{
		plan->kind = CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER;
		plan->clean_result = CTRC_CLEANED_TERMINAL_REWRITE;
		plan->committed_updater_xid = committed_updater_xid;
		plan->committed_updater_scn = committed_updater_scn;
		return true;
	}
	plan->kind = CTRC_CURRENT_MX_REWRITE_INVALIDATE;
	plan->clean_result = CTRC_CLEANED_TERMINAL_REWRITE;
	return true;
}

/* Native prune/freeze has no place to PREPARE/APPLY a retained-member
 * successor receipt.  In peer mode a derivable striped MXID is therefore
 * owned exclusively by CTRC cleanout; native code may continue only for the
 * pre-activation/local MultiXact domain. */
bool
cluster_ctrc_native_current_mx_mutation_allowed(bool peer_mode,
											 int mx_origin_slot)
{
	return !peer_mode || mx_origin_slot < 0;
}

/*
 * Pure bounded journal predicate used by DROP/TRUNCATE and both KO sides.
 * The object-reuse wire intentionally names a whole relfilenode, so this
 * over-drains every fork and block for the locator.  A matching row is safe
 * only after its pre-mutation cancellation or exact terminal cleanout has
 * become immutable; every live, blocked, malformed or unknown state retains.
 */
bool
cluster_ctrc_relation_removal_ready_from_snapshot(
	const ClusterCtrcReceipt *receipts, Size receipt_count,
	uint32 spc_oid, uint32 db_oid, uint32 rel_number)
{
	Size i;

	if ((receipt_count != 0 && receipts == NULL)
		|| spc_oid == 0 || db_oid == 0 || rel_number == 0)
		return false;
	for (i = 0; i < receipt_count; i++)
	{
		const ClusterCtrcReceipt *receipt = &receipts[i];
		uint32 state = receipt->state;
		uint8 disposition = receipt->disposition;
		uint8 target_kind = receipt->target.kind;

		if (state == CTRC_RECEIPT_FREE)
			continue;
		if (receipt->target.spc_oid != spc_oid
			|| receipt->target.db_oid != db_oid
			|| receipt->target.rel_number != rel_number)
			continue;

		if (state == CTRC_RECEIPT_CANCELLED)
		{
			if (disposition != CTRC_RELEASE_CANCELLED_PREMUTATION
				|| (target_kind != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
					&& target_kind != CTRC_TARGET_PAGE_PENDING_OFFNUM
					&& target_kind != CTRC_TARGET_EXACT_ITL_SLOT
					&& target_kind != CTRC_TARGET_EXACT_TID))
				return false;
			continue;
		}
		if (state == CTRC_RECEIPT_CLEANED)
		{
			if (disposition < CTRC_RELEASE_CLEANED_ABSENT
				|| disposition > CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED
				|| (target_kind != CTRC_TARGET_EXACT_ITL_SLOT
					&& target_kind != CTRC_TARGET_EXACT_TID))
				return false;
			continue;
		}
		if (state == CTRC_RECEIPT_ACK_FROZEN)
		{
			if (disposition == CTRC_RELEASE_CANCELLED_PREMUTATION)
			{
				if (target_kind != CTRC_TARGET_PAGE_PENDING_ITL_SLOT
					&& target_kind != CTRC_TARGET_PAGE_PENDING_OFFNUM
					&& target_kind != CTRC_TARGET_EXACT_ITL_SLOT
					&& target_kind != CTRC_TARGET_EXACT_TID)
					return false;
			}
			else if (disposition < CTRC_RELEASE_CLEANED_ABSENT
					 || disposition > CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED
					 || (target_kind != CTRC_TARGET_EXACT_ITL_SLOT
						 && target_kind != CTRC_TARGET_EXACT_TID))
				return false;
			continue;
		}
		return false;
	}
	return true;
}

bool
cluster_ctrc_relation_removal_ready_shared(
	uint32 spc_oid, uint32 db_oid, uint32 rel_number)
{
#ifndef CLUSTER_CTRC_UNIT_TEST
	bool ready;

	if (!cluster_ctrc_shmem_ready())
		return false;
	SpinLockAcquire(&CtrcShared->receipt_lock);
	ready = cluster_ctrc_relation_removal_ready_from_snapshot(
		ctrc_receipt_entries(), CtrcShared->receipt_entries,
		spc_oid, db_oid, rel_number);
	SpinLockRelease(&CtrcShared->receipt_lock);
	return ready;
#else
	(void)spc_oid;
	(void)db_oid;
	(void)rel_number;
	return false;
#endif
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_note_successor_receipt(ClusterCtrcTransferState *transfer)
{
	if (transfer == NULL || transfer->predecessor_removed
		|| transfer->descriptor_durable
		|| transfer->active_survivor_count == 0
		|| transfer->successor_receipt_count
		   >= transfer->active_survivor_count)
		return CLUSTER_CTRC_TRANSFER_REFUSED;
	transfer->successor_receipt_count++;
	return CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR;
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_note_descriptor_durable(ClusterCtrcTransferState *transfer)
{
	if (transfer == NULL || transfer->predecessor_removed
		|| transfer->active_survivor_count == 0
		|| transfer->successor_receipt_count
		   != transfer->active_survivor_count)
		return CLUSTER_CTRC_TRANSFER_REFUSED;
	transfer->descriptor_durable = true;
	return CLUSTER_CTRC_TRANSFER_READY;
}

ClusterCtrcTransferResult
cluster_ctrc_transfer_remove_predecessor(ClusterCtrcTransferState *transfer)
{
	if (transfer == NULL)
		return CLUSTER_CTRC_TRANSFER_REFUSED;
	if (transfer->predecessor_removed)
		return CLUSTER_CTRC_TRANSFER_REMOVED;
	if (transfer->active_survivor_count != 0
		&& (transfer->successor_receipt_count
			!= transfer->active_survivor_count
			|| !transfer->descriptor_durable))
		return CLUSTER_CTRC_TRANSFER_REFUSED;
	transfer->predecessor_removed = true;
	return CLUSTER_CTRC_TRANSFER_REMOVED;
}

static bool
ctrc_receipt_snapshot_valid(const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcReceipt *receipt)
{
	const ClusterCtrcPublicationIdV1 *publication;
	bool target_valid;

	if (participant == NULL || receipt == NULL
		|| memcmp(&receipt->key, &participant->key,
			sizeof(receipt->key)) != 0
		|| !ctrc_bytes_zero(receipt->reserved8,
			sizeof(receipt->reserved8)))
		return false;
	publication = &receipt->publication;
	if (publication->requester_node_id != participant->identity.node_id
		|| publication->requester_boot_incarnation
		   != participant->identity.boot_incarnation
		|| publication->capability_record_generation
		   != participant->identity.capability_record_generation
		|| publication->requester_backend_id <= 0
		|| publication->wire_request_id == 0
		|| publication->operation_id == 0
		|| publication->attempt_generation == 0
		|| publication->reserved8 != 0
		|| publication->journal_sequence == 0
		|| publication->key_sequence == 0
		|| publication->journal_slot_generation == 0
		|| publication->grant_generation != participant->grant_generation)
		return false;

	if (publication->reference_kind == CTRC_REF_HEAP_ITL_UBA)
	{
		if (publication->descriptor_hash != 0
			|| publication->member_ordinal != UINT16_MAX
			|| publication->member_role != 0
			|| publication->target_kind
			   != CTRC_TARGET_PAGE_PENDING_ITL_SLOT)
			return false;
		target_valid = receipt->target.kind == CTRC_TARGET_EXACT_ITL_SLOT
			? ctrc_target_exact_itl_valid(&participant->key,
				&receipt->target)
			: ctrc_target_pending_itl_valid(&receipt->target);
	}
	else if (publication->reference_kind >= CTRC_REF_CURRENT_MX_LOCKER
			 && publication->reference_kind <= CTRC_REF_HOT_FOLLOW_EDGE)
	{
		if (publication->descriptor_hash == 0
			|| publication->member_ordinal >= CLUSTER_CURRENT_MX_MAX_MEMBERS
			|| publication->member_role == 0
			|| publication->target_kind
			   != CTRC_TARGET_PAGE_PENDING_OFFNUM)
			return false;
		target_valid = receipt->target.kind == CTRC_TARGET_EXACT_TID
			? ctrc_target_exact_tid_valid(publication, &receipt->target)
			: ctrc_target_pending_offnum_valid(publication,
				&receipt->target);
	}
	else
		return false;
	if (!target_valid)
		return false;

	if (receipt->state == CTRC_RECEIPT_CANCELLED)
		return receipt->disposition
			== CTRC_RELEASE_CANCELLED_PREMUTATION;
	if (receipt->state != CTRC_RECEIPT_CLEANED
		|| receipt->target.kind == CTRC_TARGET_PAGE_PENDING_ITL_SLOT
		|| receipt->target.kind == CTRC_TARGET_PAGE_PENDING_OFFNUM)
		return false;
	return receipt->disposition >= CTRC_RELEASE_CLEANED_ABSENT
		&& receipt->disposition <= CTRC_RELEASE_CLEANED_SUCCESSOR_REPLACED;
}

static bool
ctrc_receipt_row_encode(const ClusterCtrcReceipt *receipt,
	uint8 bytes[CLUSTER_CTRC_ROW_ENCODING_MAX_BYTES], Size *length_out)
{
	uint8 key_bytes[CLUSTER_CTRC_TXN_KEY_BYTES];
	uint8 publication_bytes[CLUSTER_CTRC_PUBLICATION_ENCODING_BYTES];
	uint8 target_bytes[CLUSTER_CTRC_TARGET_ENCODING_MAX_BYTES];
	Size target_length;
	Size offset = 0;
	int i;

	if (length_out != NULL)
		*length_out = 0;
	if (receipt == NULL || bytes == NULL || length_out == NULL
		|| !ctrc_txn_key_encode(&receipt->key, key_bytes)
		|| !ctrc_publication_id_encode(&receipt->publication,
			publication_bytes)
		|| !ctrc_target_encode(&receipt->target, target_bytes,
			&target_length))
		return false;
	MemSet(bytes, 0, CLUSTER_CTRC_ROW_ENCODING_MAX_BYTES);
	memcpy(bytes + offset, key_bytes, sizeof(key_bytes));
	offset += sizeof(key_bytes);
	ctrc_put_u32_le(bytes + offset, sizeof(publication_bytes));
	offset += sizeof(uint32);
	memcpy(bytes + offset, publication_bytes, sizeof(publication_bytes));
	offset += sizeof(publication_bytes);
	ctrc_put_u32_le(bytes + offset, (uint32)target_length);
	offset += sizeof(uint32);
	memcpy(bytes + offset, target_bytes, target_length);
	offset += target_length;
	bytes[offset++] = receipt->disposition;
	ctrc_put_u64_le(bytes + offset, receipt->highest_local_wal_lsn);
	offset += sizeof(uint64);
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
	{
		ctrc_put_u64_le(bytes + offset, receipt->required_lsn[i]);
		offset += sizeof(uint64);
	}
	if (offset > CLUSTER_CTRC_ROW_ENCODING_MAX_BYTES)
		return false;
	*length_out = offset;
	return true;
}

static int
ctrc_receipt_key_sequence_cmp(const void *left, const void *right)
{
	const ClusterCtrcReceipt *a = (const ClusterCtrcReceipt *)left;
	const ClusterCtrcReceipt *b = (const ClusterCtrcReceipt *)right;

	if (a->publication.key_sequence < b->publication.key_sequence)
		return -1;
	if (a->publication.key_sequence > b->publication.key_sequence)
		return 1;
	return 0;
}

static bool
ctrc_receipt_durability_covered(const ClusterCtrcReceipt *receipt,
	const ClusterCtrcDurability *durability,
	XLogRecPtr *highest_local_lsn,
	XLogRecPtr required_lsn[CLUSTER_SF_DEP_MAX_ORIGINS])
{
	int i;

	if (receipt == NULL || durability == NULL || highest_local_lsn == NULL
		|| required_lsn == NULL)
		return false;
	if (receipt->state == CTRC_RECEIPT_CLEANED && receipt->target.needs_wal
		&& XLogRecPtrIsInvalid(receipt->highest_local_wal_lsn))
		return false;
	if (!XLogRecPtrIsInvalid(receipt->highest_local_wal_lsn))
	{
		if (XLogRecPtrIsInvalid(durability->local_flush_lsn)
			|| durability->local_flush_lsn
			   < receipt->highest_local_wal_lsn)
			return false;
		if (XLogRecPtrIsInvalid(*highest_local_lsn)
			|| receipt->highest_local_wal_lsn > *highest_local_lsn)
			*highest_local_lsn = receipt->highest_local_wal_lsn;
	}
	for (i = 0; i < CLUSTER_SF_DEP_MAX_ORIGINS; i++)
	{
		if (XLogRecPtrIsInvalid(receipt->required_lsn[i]))
			continue;
		if (XLogRecPtrIsInvalid(durability->durable_lsn[i])
			|| durability->durable_lsn[i] < receipt->required_lsn[i])
			return false;
		if (XLogRecPtrIsInvalid(required_lsn[i])
			|| receipt->required_lsn[i] > required_lsn[i])
			required_lsn[i] = receipt->required_lsn[i];
	}
	return true;
}

ClusterCtrcAckResult
cluster_ctrc_participant_ack_from_snapshot(
	const ClusterCtrcParticipantEntry *participant,
	ClusterCtrcReceipt *receipts, Size receipt_count,
	const ClusterCtrcDurability *durability,
	ClusterCtrcLocalReleaseAckV1 *ack)
{
	static const uint8 row_domain[] = "PGRAC-CTRC-ROW-V1";
	pg_sha256_ctx context;
	uint8 count_bytes[8];
	uint8 length_bytes[4];
	uint8 row_bytes[CLUSTER_CTRC_ROW_ENCODING_MAX_BYTES];
	uint8 encoded[CLUSTER_CTRC_LOCAL_ACK_BYTES];
	uint64 cancelled_count = 0;
	uint64 cleaned_count = 0;
	Size row_length;
	Size i;
	Size j;

	if (ack != NULL)
		MemSet(ack, 0, sizeof(*ack));
	if (participant == NULL || durability == NULL || ack == NULL
		|| participant->state != CTRC_PARTICIPANT_ACK_READY
		|| !ctrc_txn_key_valid(&participant->key)
		|| !ctrc_participant_identity_valid(&participant->key,
			&participant->identity)
		|| participant->grant_generation == 0
		|| participant->seal_generation == 0
		|| !ctrc_bytes_zero(participant->reserved8,
			sizeof(participant->reserved8))
		|| participant->receipt_count != receipt_count
		|| participant->prepared_count != 0
		|| participant->applied_count != 0
		|| participant->ack_frozen_count != 0
		|| (receipt_count == 0 ? receipts != NULL : receipts == NULL)
		|| receipt_count > UINT64_MAX)
		return CLUSTER_CTRC_ACK_DENIED;

	ack->transaction_key = participant->key;
	ack->grant_generation = participant->grant_generation;
	ack->result = CTRC_ACK_RELEASED;
	ack->seal_generation = participant->seal_generation;
	ack->participant_node_id = participant->identity.node_id;
	ack->dependency_entry_count = CLUSTER_SF_DEP_MAX_ORIGINS;
	ack->capability_record_generation
		= participant->identity.capability_record_generation;
	ack->participant_boot_incarnation
		= participant->identity.boot_incarnation;
	ack->formation_epoch = participant->identity.formation_epoch;
	ack->admission_record_generation
		= participant->identity.admission_record_generation;

	if (receipt_count == 0)
	{
		if (participant->next_key_sequence != 1
			|| participant->last_key_sequence != 0
			|| participant->cancelled_count != 0
			|| participant->cleaned_count != 0)
			return CLUSTER_CTRC_ACK_DENIED;
		ack->flags = CTRC_ACK_FLAG_ZERO_RANGE | CTRC_ACK_FLAG_ALL_DURABLE;
		memcpy(ack->row_digest_sha256, cluster_ctrc_empty_sha256,
			sizeof(ack->row_digest_sha256));
	}
	else
	{
		if (receipt_count == UINT64_MAX
			|| participant->next_key_sequence != receipt_count + 1
			|| participant->last_key_sequence != receipt_count
			|| participant->cancelled_count > receipt_count
			|| participant->cleaned_count
			   != receipt_count - participant->cancelled_count)
			return CLUSTER_CTRC_ACK_DENIED;
		qsort(receipts, receipt_count, sizeof(*receipts),
			ctrc_receipt_key_sequence_cmp);
		pg_sha256_init(&context);
		pg_sha256_update(&context, row_domain, sizeof(row_domain));
		ctrc_put_u64_le(count_bytes, (uint64)receipt_count);
		pg_sha256_update(&context, count_bytes, sizeof(count_bytes));
		for (i = 0; i < receipt_count; i++)
		{
			ClusterCtrcReceipt *receipt = &receipts[i];

			if (receipt->publication.key_sequence != i + 1
				|| !ctrc_receipt_snapshot_valid(participant, receipt)
				|| !ctrc_receipt_durability_covered(receipt, durability,
					&ack->highest_local_cleanout_lsn,
					ack->required_lsn_vector)
				|| !ctrc_receipt_row_encode(receipt, row_bytes,
					&row_length))
				return CLUSTER_CTRC_ACK_DENIED;
			for (j = 0; j < i; j++)
			{
				if (ctrc_publication_request_equal(
						&receipts[j].publication,
						&receipt->publication)
					|| receipts[j].publication.journal_sequence
					   == receipt->publication.journal_sequence
					|| receipts[j].publication.journal_slot_generation
					   == receipt->publication.journal_slot_generation)
					return CLUSTER_CTRC_ACK_DENIED;
			}
			if (receipt->state == CTRC_RECEIPT_CANCELLED)
				cancelled_count++;
			else
				cleaned_count++;
			ctrc_put_u32_le(length_bytes, (uint32)row_length);
			pg_sha256_update(&context, length_bytes,
				sizeof(length_bytes));
			pg_sha256_update(&context, row_bytes, row_length);
		}
		if (cancelled_count != participant->cancelled_count
			|| cleaned_count != participant->cleaned_count)
			return CLUSTER_CTRC_ACK_DENIED;
		pg_sha256_final(&context, ack->row_digest_sha256);
		ack->flags = CTRC_ACK_FLAG_ALL_DURABLE;
		ack->first_key_sequence = 1;
		ack->last_key_sequence = receipt_count;
		ack->minimum_journal_sequence
			= receipts[0].publication.journal_sequence;
		ack->maximum_journal_sequence
			= receipts[0].publication.journal_sequence;
		for (i = 1; i < receipt_count; i++)
		{
			if (receipts[i].publication.journal_sequence
				< ack->minimum_journal_sequence)
				ack->minimum_journal_sequence
					= receipts[i].publication.journal_sequence;
			if (receipts[i].publication.journal_sequence
				> ack->maximum_journal_sequence)
				ack->maximum_journal_sequence
					= receipts[i].publication.journal_sequence;
		}
		ack->total_receipt_count = receipt_count;
		ack->cancelled_count = cancelled_count;
		ack->cleaned_count = cleaned_count;
		ack->ack_frozen_count = receipt_count;
	}

	if (!cluster_ctrc_local_release_ack_encode(ack, encoded))
	{
		MemSet(ack, 0, sizeof(*ack));
		return CLUSTER_CTRC_ACK_DENIED;
	}
	ack->crc32c = ctrc_get_u32_le(encoded + 412);
	return CLUSTER_CTRC_ACK_RELEASED;
}

#ifndef CLUSTER_CTRC_UNIT_TEST
static bool
ctrc_page_version_capture(Page page, uint16 *origin_out,
	XLogRecPtr *lsn_out, SCN *scn_out)
{
	XLogRecPtr page_lsn;
	SCN page_scn;
	int page_lsn_origin;

	if (page == NULL || origin_out == NULL || lsn_out == NULL
		|| scn_out == NULL)
		return false;
	page_lsn = PageGetLSN(page);
	page_scn = ((PageHeader)page)->pd_block_scn;
	*lsn_out = page_lsn;
	*scn_out = page_scn;
	if (XLogRecPtrIsInvalid(page_lsn))
	{
		*origin_out = CLUSTER_CTRC_PAGE_LSN_ORIGIN_INVALID;
		return !SCN_VALID(page_scn);
	}
	if (!PageGetLSNOrigin(page, &page_lsn_origin)
		|| page_lsn_origin < 0
		|| page_lsn_origin >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| !SCN_VALID(page_scn))
		return false;
	*origin_out = (uint16)page_lsn_origin;
	return true;
}

static bool
ctrc_cleaner_next_applied_receipt(ClusterCtrcParticipantEntry *participant_out,
	ClusterCtrcReceipt *receipt_out, uint64 *participant_index_out,
	uint64 *receipt_index_out)
{
	static uint64 scan_cursor = 0;
	uint64 visited;
	uint64 limit;
	bool found = false;

	if (participant_out == NULL || receipt_out == NULL
		|| participant_index_out == NULL || receipt_index_out == NULL
		|| cluster_node_id < 0 || !cluster_ctrc_shmem_ready())
		return false;
	MemSet(participant_out, 0, sizeof(*participant_out));
	MemSet(receipt_out, 0, sizeof(*receipt_out));
	*participant_index_out = UINT64_MAX;
	*receipt_index_out = UINT64_MAX;
	limit = ctrc_scan_limit(CTRC_SCAN_RECEIPT, CtrcShared->receipt_entries);
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	for (visited = 0; visited < limit; visited++) {
		uint64 receipt_index = (scan_cursor + visited)
			% CtrcShared->receipt_entries;
		ClusterCtrcReceipt *receipt
			= &ctrc_receipt_entries()[receipt_index];
		ClusterCtrcParticipantEntry *participant;
		uint64 participant_index;

		if (pg_atomic_read_u32((pg_atomic_uint32 *)&receipt->state)
			!= CTRC_RECEIPT_APPLIED
			|| !ctrc_participant_index(&receipt->key,
				(uint16)cluster_node_id, &participant_index))
			continue;
		participant = &ctrc_participant_entries()[participant_index];
		if (participant->state != CTRC_PARTICIPANT_CLOSED_DRAINING
			|| participant->identity.node_id != (uint16)cluster_node_id
			|| participant->applied_count == 0
			|| memcmp(&participant->key, &receipt->key,
				sizeof(receipt->key)) != 0
			|| participant->grant_generation
			   != receipt->publication.grant_generation)
			continue;
		*participant_out = *participant;
		*receipt_out = *receipt;
		*participant_index_out = participant_index;
		*receipt_index_out = receipt_index;
		scan_cursor = (receipt_index + 1) % CtrcShared->receipt_entries;
		found = true;
		break;
	}
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	ctrc_scan_charge(CTRC_SCAN_RECEIPT, visited + (found ? 1 : 0));
	return found;
}

static bool
ctrc_cleaner_itl_page_exact(Buffer buffer,
	const ClusterCtrcTargetV1 *target, Page *page_out)
{
	RelFileLocator locator;
	ForkNumber fork_number;
	BlockNumber block_number;
	Page page;
	uint16 page_lsn_origin;
	XLogRecPtr page_lsn;
	SCN page_scn;

	if (!BufferIsValid(buffer) || target == NULL || page_out == NULL)
		return false;
	BufferGetTag(buffer, &locator, &fork_number, &block_number);
	if (locator.spcOid != target->spc_oid
		|| locator.dbOid != target->db_oid
		|| locator.relNumber != target->rel_number
		|| fork_number != target->fork_number
		|| block_number != target->block_number)
		return false;
	page = BufferGetPage(buffer);
	if (PageIsNew(page) || !PageHasItl(page)
		|| target->itl_slot_index >= CLUSTER_ITL_INITRANS_DEFAULT
		|| !ctrc_page_version_capture(page, &page_lsn_origin,
			&page_lsn, &page_scn)
		|| cluster_ctrc_page_version_order(
			target->predecessor_page_lsn_origin_node_id,
			target->predecessor_page_lsn,
			target->predecessor_page_scn, page_lsn_origin,
			page_lsn, page_scn) != CTRC_PAGE_VERSION_CURRENT)
		return false;
	*page_out = page;
	return true;
}

static bool
ctrc_cleaner_dependencies_durable(const ClusterSfDepVec *dependencies,
	ClusterCtrcDurability *durability)
{
	int origin;

	if (dependencies == NULL || durability == NULL)
		return false;
	ctrc_participant_capture_durability(durability);
	for (origin = 0; origin < CLUSTER_SF_DEP_MAX_ORIGINS; origin++)
	{
		durability->required_lsn[origin]
			= dependencies->required[origin];
		if (!XLogRecPtrIsInvalid(dependencies->required[origin])
			&& (XLogRecPtrIsInvalid(durability->durable_lsn[origin])
				|| durability->durable_lsn[origin]
				   < dependencies->required[origin]))
			return false;
	}
	return true;
}

typedef enum CtrcCleanerCurrentMxPresence
{
	CTRC_CLEANER_CURRENT_MX_RETAIN = 0,
	CTRC_CLEANER_CURRENT_MX_ABSENT,
	CTRC_CLEANER_CURRENT_MX_PRESENT
} CtrcCleanerCurrentMxPresence;

typedef enum CtrcCleanerCurrentMxCaptureFailure
{
	CTRC_CLEANER_CURRENT_MX_CAPTURE_OK = 0,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_INPUT,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_TAG,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_RESOURCE_X,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM_FLAGS,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM_TAG,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_SHAPE,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_LSN,
	CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_SCN
} CtrcCleanerCurrentMxCaptureFailure;

typedef struct CtrcCleanerCurrentMxCapture
{
	ClusterPcmOwnSnapshot pcm;
	ClusterSfDepVec dependencies;
	ItemIdData line_pointer;
	uint8 tuple_header[SizeofHeapTupleHeader];
	XLogRecPtr page_lsn;
	SCN page_scn;
	uint16 page_lsn_origin_node_id;
	uint8 presence;
	uint8 failure;
} CtrcCleanerCurrentMxCapture;

static CtrcCleanerCurrentMxPresence
ctrc_cleaner_current_mx_capture(Buffer buffer,
	const ClusterCtrcTargetV1 *target,
	CtrcCleanerCurrentMxCapture *capture)
{
	RelFileLocator locator;
	ForkNumber fork_number;
	BlockNumber block_number;
	OffsetNumber offnum;
	BufferDesc *descriptor;
	HeapTupleHeader tuple;
	ItemId item;
	Page page;
	uint16 page_lsn_origin;
	XLogRecPtr page_lsn;
	SCN page_scn;
	ClusterCtrcPageVersionOrder page_version_order;

	if (capture == NULL)
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	MemSet(capture, 0, sizeof(*capture));
	if (!BufferIsValid(buffer) || target == NULL)
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_INPUT;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	BufferGetTag(buffer, &locator, &fork_number, &block_number);
	descriptor = GetBufferDescriptor(buffer - 1);
	if (locator.spcOid != target->spc_oid
		|| locator.dbOid != target->db_oid
		|| locator.relNumber != target->rel_number
		|| fork_number != target->fork_number
		|| block_number != target->block_number)
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_TAG;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	if (!cluster_bufmgr_pcm_x_ordinary_content_write_permitted(descriptor))
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_RESOURCE_X;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	if (cluster_bufmgr_pcm_own_snapshot(descriptor, &capture->pcm)
		!= CLUSTER_PCM_OWN_OK)
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	if (capture->pcm.flags != 0)
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM_FLAGS;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	if (!BufferTagsEqual(&capture->pcm.tag, &descriptor->tag))
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_PCM_TAG;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	page = BufferGetPage(buffer);
	if (PageIsNew(page) || !PageHasItl(page)
		|| PageGetPageSize(page) != BLCKSZ)
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_SHAPE;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	if (!ctrc_page_version_capture(page, &page_lsn_origin,
			&page_lsn, &page_scn))
	{
		capture->failure = CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_LSN;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	page_version_order = cluster_ctrc_page_version_order(
		target->predecessor_page_lsn_origin_node_id,
		target->predecessor_page_lsn, target->predecessor_page_scn,
		page_lsn_origin, page_lsn, page_scn);
	if (page_version_order != CTRC_PAGE_VERSION_CURRENT)
	{
		capture->failure = SCN_VALID(target->predecessor_page_scn)
			&& (!SCN_VALID(page_scn)
				|| scn_time_cmp(page_scn,
					target->predecessor_page_scn) < 0)
			? CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_SCN
			: CTRC_CLEANER_CURRENT_MX_CAPTURE_PAGE_LSN;
		return CTRC_CLEANER_CURRENT_MX_RETAIN;
	}
	capture->page_lsn = page_lsn;
	capture->page_scn = page_scn;
	capture->page_lsn_origin_node_id = page_lsn_origin;
	cluster_sf_dep_vec_reset(&capture->dependencies);
	(void)cluster_sf_dep_vec_for_ship(buffer, &capture->dependencies);

	offnum = (OffsetNumber)target->offset_number;
	if (offnum < FirstOffsetNumber || offnum > PageGetMaxOffsetNumber(page))
	{
		capture->presence = CTRC_CLEANER_CURRENT_MX_ABSENT;
		return CTRC_CLEANER_CURRENT_MX_ABSENT;
	}
	item = PageGetItemId(page, offnum);
	if (!ItemIdIsNormal(item) || ItemIdGetLength(item) < SizeofHeapTupleHeader)
	{
		capture->presence = CTRC_CLEANER_CURRENT_MX_ABSENT;
		return CTRC_CLEANER_CURRENT_MX_ABSENT;
	}
	tuple = (HeapTupleHeader)PageGetItem(page, item);
	if ((tuple->t_infomask & HEAP_XMAX_INVALID) != 0
		|| (tuple->t_infomask & HEAP_XMAX_IS_MULTI) == 0
		|| !TransactionIdEquals(HeapTupleHeaderGetRawXmax(tuple),
			(TransactionId)target->multixact_id))
	{
		capture->presence = CTRC_CLEANER_CURRENT_MX_ABSENT;
		return CTRC_CLEANER_CURRENT_MX_ABSENT;
	}
	capture->line_pointer = *item;
	memcpy(capture->tuple_header, tuple, SizeofHeapTupleHeader);
	capture->presence = CTRC_CLEANER_CURRENT_MX_PRESENT;
	return CTRC_CLEANER_CURRENT_MX_PRESENT;
}

static bool
ctrc_cleaner_current_mx_capture_recheck(Buffer buffer,
	const ClusterCtrcTargetV1 *target,
	const CtrcCleanerCurrentMxCapture *expected)
{
	CtrcCleanerCurrentMxCapture live;
	CtrcCleanerCurrentMxPresence presence;

	if (expected == NULL)
		return false;
	presence = ctrc_cleaner_current_mx_capture(buffer, target, &live);
	if (presence != (CtrcCleanerCurrentMxPresence)expected->presence
		|| presence == CTRC_CLEANER_CURRENT_MX_RETAIN
		|| live.page_lsn != expected->page_lsn
		|| live.page_scn != expected->page_scn
		|| memcmp(&live.pcm, &expected->pcm, sizeof(live.pcm)) != 0
		|| memcmp(&live.dependencies, &expected->dependencies,
				  sizeof(live.dependencies)) != 0)
		return false;
	if (presence == CTRC_CLEANER_CURRENT_MX_ABSENT)
		return true;
	return memcmp(&live.line_pointer, &expected->line_pointer,
				  sizeof(live.line_pointer)) == 0
		&& memcmp(live.tuple_header, expected->tuple_header,
				  sizeof(live.tuple_header)) == 0;
}

static bool
ctrc_cleaner_current_mx_committed_projection_exact(Page page,
	HeapTupleHeader tuple, TransactionId updater_xid, SCN commit_scn)
{
	const ClusterItlSlotData *slot;
	uint8 slot_index;

	if (page == NULL || tuple == NULL || !TransactionIdIsNormal(updater_xid)
		|| !SCN_VALID(commit_scn) || !PageHasItl(page))
		return false;
	slot_index = tuple->t_itl_slot_idx;
	if (slot_index == CLUSTER_ITL_SLOT_UNALLOCATED
		|| slot_index >= CLUSTER_ITL_INITRANS_DEFAULT)
		return false;
	slot = &ClusterPageGetItlSlots(page)[slot_index];
	return slot->xid == updater_xid && slot->flags == ITL_FLAG_COMMITTED
		&& slot->commit_scn == commit_scn;
}

static void
ctrc_cleaner_current_mx_hint_bits(
	const ClusterCtrcCurrentMxRewritePlan *plan,
	uint16 *infomask, uint16 *infomask2)
{
	LockTupleMode strongest = LockTupleKeyShare;
	bool has_update = false;
	uint16 i;

	Assert(plan != NULL && plan->survivor_count > 0);
	*infomask = HEAP_XMAX_IS_MULTI;
	*infomask2 = 0;
	for (i = 0; i < plan->survivor_count; i++)
	{
		LockTupleMode member_mode;

		switch (plan->survivors[i].status)
		{
			case MultiXactStatusForKeyShare:
				member_mode = LockTupleKeyShare;
				break;
			case MultiXactStatusForShare:
				member_mode = LockTupleShare;
				break;
			case MultiXactStatusForNoKeyUpdate:
			case MultiXactStatusNoKeyUpdate:
				member_mode = LockTupleNoKeyExclusive;
				break;
			case MultiXactStatusForUpdate:
			case MultiXactStatusUpdate:
				member_mode = LockTupleExclusive;
				break;
			default:
				Assert(false);
				member_mode = LockTupleExclusive;
				break;
		}

		if (member_mode > strongest)
			strongest = member_mode;
		if (plan->survivors[i].status == MultiXactStatusForUpdate
			|| plan->survivors[i].status == MultiXactStatusUpdate)
			*infomask2 |= HEAP_KEYS_UPDATED;
		if (ISUPDATE_from_mxstatus(plan->survivors[i].status))
			has_update = true;
	}
	if (strongest == LockTupleExclusive
		|| strongest == LockTupleNoKeyExclusive)
		*infomask |= HEAP_XMAX_EXCL_LOCK;
	else if (strongest == LockTupleShare)
		*infomask |= HEAP_XMAX_SHR_LOCK;
	else
		*infomask |= HEAP_XMAX_KEYSHR_LOCK;
	if (!has_update)
		*infomask |= HEAP_XMAX_LOCK_ONLY;
}

static void
ctrc_cleaner_current_mx_plan_header(
	const CtrcCleanerCurrentMxCapture *capture,
	const ClusterCtrcCurrentMxRewritePlan *plan, MultiXactId successor,
	uint8 planned_header[SizeofHeapTupleHeader])
{
	HeapTupleHeader tuple = (HeapTupleHeader)planned_header;
	uint16 new_infomask = 0;
	uint16 new_infomask2 = 0;

	memcpy(planned_header, capture->tuple_header, SizeofHeapTupleHeader);
	tuple->t_infomask &= ~HEAP_XMAX_BITS;
	if (plan->kind == CTRC_CURRENT_MX_REWRITE_INVALIDATE)
	{
		HeapTupleHeaderSetXmax(tuple, InvalidTransactionId);
		tuple->t_infomask |= HEAP_XMAX_INVALID;
		tuple->t_infomask2 &= ~(HEAP_KEYS_UPDATED | HEAP_HOT_UPDATED);
	}
	else if (plan->kind == CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER)
	{
		HeapTupleHeaderSetXmax(tuple, plan->committed_updater_xid);
		tuple->t_infomask |= HEAP_XMAX_COMMITTED;
	}
	else
	{
		Assert(plan->kind == CTRC_CURRENT_MX_REWRITE_SUCCESSOR);
		HeapTupleHeaderSetXmax(tuple, (TransactionId)successor);
		tuple->t_infomask2 &= ~HEAP_KEYS_UPDATED;
		ctrc_cleaner_current_mx_hint_bits(
			plan, &new_infomask, &new_infomask2);
		tuple->t_infomask |= new_infomask;
		tuple->t_infomask2 |= new_infomask2;
	}
}

static void
ctrc_cleaner_cancel_successor_receipts(
	ClusterCtrcReceiptHandle *handles, const bool *new_receipts,
	uint16 receipt_count)
{
	uint16 i;

	for (i = 0; i < receipt_count; i++)
		if (new_receipts[i])
			(void)cluster_ctrc_receipt_cancel_shared(&handles[i]);
}

static bool
ctrc_cleaner_prepare_current_mx_successor(
	const ClusterCtrcReceipt *predecessor,
	const CtrcCleanerCurrentMxCapture *capture,
	const ClusterCurrentMxMemberDesc *members,
	const ClusterCurrentMemberProof *proofs,
	const uint32 *proof_capability_generations, uint16 nmembers,
	const ClusterCtrcCurrentMxRewritePlan *plan,
	uint8 planned_header[SizeofHeapTupleHeader],
	ClusterCtrcTargetV1 *successor_target,
	ClusterCtrcTransferState *transfer)
{
	ClusterCtrcReceiptHandle handles[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	bool new_receipts[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMxMemberDesc successor_members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMxKey successor_key;
	ClusterCtrcTargetV1 pending_target;
	MultiXactId successor;
	XLogRecPtr descriptor_lsn;
	uint64 successor_operation_id;
	uint64 descriptor_hash;
	uint16 prepared_count = 0;
	uint16 i;
	int successor_origin;

	if (predecessor == NULL || capture == NULL || members == NULL
		|| proofs == NULL || proof_capability_generations == NULL
		|| plan == NULL || planned_header == NULL
		|| successor_target == NULL || transfer == NULL
		|| plan->kind != CTRC_CURRENT_MX_REWRITE_SUCCESSOR
		|| plan->survivor_count == 0 || plan->survivor_count > nmembers
		|| !ctrc_allocate_journal_sequence(&successor_operation_id))
		return false;
	MemSet(handles, 0, sizeof(handles));
	MemSet(new_receipts, 0, sizeof(new_receipts));
	MemSet(successor_members, 0, sizeof(successor_members));
	MemSet(&successor_key, 0, sizeof(successor_key));
	MemSet(&pending_target, 0, sizeof(pending_target));
	MemSet(successor_target, 0, sizeof(*successor_target));
	MemSet(transfer, 0, sizeof(*transfer));

	successor = MultiXactIdCreateLocalCurrentMembers(
		plan->survivor_count, (MultiXactMember *)plan->survivors);
	descriptor_lsn = GetXLogInsertRecPtr();
	successor_origin = cluster_mxid_origin_slot(successor);
	if (!MultiXactIdIsValid(successor) || successor_origin < 0
		|| successor_origin >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| successor_origin != cluster_node_id)
		return false;
	successor_key.origin_node_id = (uint16)successor_origin;
	successor_key.multixact_id = successor;
	successor_key.cluster_epoch = predecessor->target.mx_cluster_epoch;
	for (i = 0; i < plan->survivor_count; i++)
	{
		successor_members[i].xid = plan->survivors[i].xid;
		successor_members[i].member_status
			= (uint8)plan->survivors[i].status;
	}
	descriptor_hash = cluster_multixact_current_descriptor_hash(
		&successor_key, successor_members, plan->survivor_count);
	if (descriptor_hash == 0)
		return false;
	ctrc_cleaner_current_mx_plan_header(
		capture, plan, successor, planned_header);

	pending_target.kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
	pending_target.relation_persistence
		= predecessor->target.relation_persistence;
	pending_target.needs_wal = predecessor->target.needs_wal;
	pending_target.spc_oid = predecessor->target.spc_oid;
	pending_target.db_oid = predecessor->target.db_oid;
	pending_target.rel_number = predecessor->target.rel_number;
	pending_target.fork_number = predecessor->target.fork_number;
	pending_target.block_number = predecessor->target.block_number;
	pending_target.predecessor_page_lsn_origin_node_id
		= capture->page_lsn_origin_node_id;
	pending_target.predecessor_page_lsn = capture->page_lsn;
	pending_target.predecessor_page_scn = capture->page_scn;
	pending_target.publication_own_generation = capture->pcm.generation;
	pending_target.publication_acquisition_epoch
		= predecessor->target.mx_cluster_epoch;
	pending_target.intended_descriptor_hash = descriptor_hash;

	*successor_target = pending_target;
	successor_target->kind = CTRC_TARGET_EXACT_TID;
	successor_target->offset_number = predecessor->target.offset_number;
	successor_target->itemid_flags
		= ItemIdGetFlags(&capture->line_pointer);
	successor_target->itemid_offset
		= ItemIdGetOffset(&capture->line_pointer);
	successor_target->itemid_length
		= ItemIdGetLength(&capture->line_pointer);
	if (!cluster_ctrc_sha256_exact(planned_header, SizeofHeapTupleHeader,
			successor_target->tuple_header_sha256))
		return false;
	successor_target->mx_origin_node_id = (uint16)successor_origin;
	successor_target->multixact_id = (uint32)successor;
	successor_target->mx_cluster_epoch = predecessor->target.mx_cluster_epoch;
	successor_target->descriptor_hash = descriptor_hash;
	successor_target->intended_descriptor_hash = 0;

	for (i = 0; i < plan->survivor_count; i++)
	{
		ClusterCtrcTxnKeyV1 key;
		ClusterCtrcParticipantIdentity participant;
		ClusterCtrcPublicationIdV1 publication;
		ClusterCtrcPrepareResult prepare_result;
		uint32 grant = 0;
		uint16 source_ordinal;
		bool found = false;

		for (source_ordinal = 0; source_ordinal < nmembers;
			 source_ordinal++)
			if (proofs[source_ordinal].member_xid
					== plan->survivors[i].xid
				&& proofs[source_ordinal].member_status
					== (uint8)plan->survivors[i].status
				&& (proofs[source_ordinal].state == CCM_ACTIVE
					|| proofs[source_ordinal].state == CCM_SELF))
			{
				found = true;
				break;
			}
		if (!found)
			goto successor_failed;
		grant = ClusterCurrentMemberProofGetCtrcGrant(
			&proofs[source_ordinal]);
		MemSet(&key, 0, sizeof(key));
		MemSet(&participant, 0, sizeof(participant));
		if (grant == 0
			|| proof_capability_generations[source_ordinal] == 0
			|| !cluster_runtime_visibility_active_proof_ctrc_identity_exact(
				&proofs[source_ordinal].key, grant,
				proof_capability_generations[source_ordinal],
				&key, &participant))
			goto successor_failed;

		MemSet(&publication, 0, sizeof(publication));
		publication.requester_node_id = participant.node_id;
		publication.requester_boot_incarnation
			= participant.boot_incarnation;
		publication.capability_record_generation
			= participant.capability_record_generation;
		publication.requester_backend_id
			= predecessor->publication.requester_backend_id;
		publication.wire_request_id = successor_operation_id;
		publication.operation_id = successor_operation_id;
		publication.attempt_generation = 1;
		publication.descriptor_hash = descriptor_hash;
		publication.member_ordinal = i;
		publication.member_role = (uint8)plan->survivors[i].status + 1;
		publication.reference_kind = CTRC_REF_RECOMPOSED_SURVIVOR;
		publication.target_kind = CTRC_TARGET_PAGE_PENDING_OFFNUM;
		publication.grant_generation = grant;
		prepare_result = cluster_ctrc_receipt_prepare_shared(
			&key, &participant, grant, &publication,
			&pending_target, &handles[prepared_count]);
		if (prepare_result != CLUSTER_CTRC_PREPARE_READY
			&& prepare_result != CLUSTER_CTRC_PREPARE_DUPLICATE)
			goto successor_failed;
		new_receipts[prepared_count]
			= prepare_result == CLUSTER_CTRC_PREPARE_READY;
		prepared_count++;
	}

	transfer->active_survivor_count = plan->survivor_count;
	for (i = 0; i < prepared_count; i++)
	{
		ClusterCtrcApplyToken token;

		MemSet(&token, 0, sizeof(token));
		if (cluster_ctrc_receipt_apply_shared(&handles[i],
				successor_target, &token) != CLUSTER_CTRC_APPLY_APPLIED
			|| !token.valid
			|| cluster_ctrc_transfer_note_successor_receipt(transfer)
			   != CLUSTER_CTRC_TRANSFER_PENDING_DESCRIPTOR)
			goto successor_failed;
	}
	if (XLogRecPtrIsInvalid(descriptor_lsn))
		goto successor_failed;
	XLogFlush(descriptor_lsn);
	if (cluster_ctrc_transfer_note_descriptor_durable(transfer)
		!= CLUSTER_CTRC_TRANSFER_READY)
		goto successor_failed;
	return true;

successor_failed:
	ctrc_cleaner_cancel_successor_receipts(
		handles, new_receipts, prepared_count);
	return false;
}

static bool
ctrc_cleaner_clean_current_mx_receipt(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcReceipt *receipt, uint64 participant_index,
	uint64 receipt_index)
{
	ClusterSemanticAdmissionToken admission;
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentMemberProof proofs[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint32 proof_capability_generations[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	ClusterCurrentUpdaterProof updater_proof;
	ClusterCtrcCurrentMxRewritePlan plan;
	ClusterCtrcCleanReferenceInput clean_input;
	ClusterCtrcTransferState transfer;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcDurability durability;
	ClusterCurrentMxKey descriptor_key;
	CtrcCleanerCurrentMxCapture capture;
	ClusterCtrcTargetV1 successor_target;
	ClusterCtrcCleanResult clean_result;
	ClusterMxDescribeResult describe_result = CMX_DESC_UNKNOWN;
	ClusterMxResolveResult resolve_result = CMX_RESOLVE_UNKNOWN;
	RelFileLocator locator;
	SMgrRelation smgr;
	GenericXLogState *xlog_state;
	XLogRecPtr cleanout_lsn = InvalidXLogRecPtr;
	Page page;
	Page image;
	HeapTupleHeader image_tuple;
	ItemId image_item;
	Buffer buffer;
	uint8 planned_header[SizeofHeapTupleHeader]
		pg_attribute_aligned(MAXIMUM_ALIGNOF);
	uint64 descriptor_hash = 0;
	uint32 reported_total = 0;
	uint16 nmembers = 0;
	uint16 target_ordinal;
	bool permanent;
	const char *retain_stage = "input";
	CtrcCleanerCurrentMxPresence capture_presence;

	if (participant == NULL || receipt == NULL
		|| participant_index >= CtrcShared->participant_key_entries
		|| receipt_index >= CtrcShared->receipt_entries
		|| receipt->publication.reference_kind < CTRC_REF_CURRENT_MX_LOCKER
		|| receipt->publication.reference_kind > CTRC_REF_HOT_FOLLOW_EDGE
		|| receipt->state != CTRC_RECEIPT_APPLIED
		|| !ctrc_target_exact_tid_valid(
			&receipt->publication, &receipt->target)
		|| (receipt->target.relation_persistence
			!= RELPERSISTENCE_PERMANENT
			&& receipt->target.relation_persistence
			   != RELPERSISTENCE_UNLOGGED)
		|| receipt->target.fork_number < 0
		|| receipt->target.fork_number > MAX_FORKNUM)
		return false;

	MemSet(&admission, 0, sizeof(admission));
	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	if (admission.formation_epoch != receipt->key.formation_epoch
		|| admission.record_generation
		   != receipt->key.admission_record_generation)
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	locator.spcOid = receipt->target.spc_oid;
	locator.dbOid = receipt->target.db_oid;
	locator.relNumber = receipt->target.rel_number;
	smgr = smgropen(locator, InvalidBackendId);
	if (!smgrexists(smgr, (ForkNumber)receipt->target.fork_number)
		|| smgrnblocks(smgr, (ForkNumber)receipt->target.fork_number)
		   <= receipt->target.block_number)
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	permanent = receipt->target.relation_persistence
		== RELPERSISTENCE_PERMANENT;
	buffer = ReadBufferWithoutRelcache(locator,
		(ForkNumber)receipt->target.fork_number,
		receipt->target.block_number, RBM_NORMAL, NULL, permanent);
	if (!BufferIsValid(buffer))
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
	if (!ClusterLockBufferExclusiveRetryAware(buffer))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PAGE_REVALIDATE);
	capture_presence = ctrc_cleaner_current_mx_capture(
		buffer, &receipt->target, &capture);
	if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
		|| capture_presence == CTRC_CLEANER_CURRENT_MX_RETAIN)
	{
		static uint64 last_capture_journal_sequence = 0;

		if (last_capture_journal_sequence
			!= receipt->publication.journal_sequence)
		{
			last_capture_journal_sequence
				= receipt->publication.journal_sequence;
			ereport(LOG,
				(errmsg_internal(
					"PGRAC CTRC current-MX cleanout retained: stage=capture "
					"journal=" UINT64_FORMAT " key_origin=%u key_xid=%u "
					"mx_origin=%u mxid=%u mx_epoch=%u ordinal=%u "
					"capture=%d failure=%u admission_current=%d",
					receipt->publication.journal_sequence,
					receipt->key.origin_node_id, receipt->key.xid,
					receipt->target.mx_origin_node_id,
					receipt->target.multixact_id,
					receipt->target.mx_cluster_epoch,
					receipt->publication.member_ordinal,
					(int)capture_presence,
					capture.failure,
					cluster_semantic_activation_recheck_r4_terminal_census(
						&admission))));
		}
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_WAL_DURABILITY);
	if (!ctrc_cleaner_dependencies_durable(
			&capture.dependencies, &durability))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	if (capture.presence == CTRC_CLEANER_CURRENT_MX_ABSENT)
	{
		MemSet(&clean_input, 0, sizeof(clean_input));
		clean_input.target_state = CTRC_TARGET_ABSENT;
		clean_input.source_transition_censused = true;
		clean_input.page_authority_exact = true;
		clean_result = cluster_ctrc_clean_reference(&clean_input);
		if (clean_result != CTRC_CLEANED_ABSENT)
		{
			retain_stage = "absent-policy";
			goto current_mx_retain_unlocked;
		}
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
		if (!ClusterLockBufferExclusiveRetryAware(buffer))
		{
			ReleaseBuffer(buffer);
			cluster_semantic_activation_leave(&admission);
			return false;
		}
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PAGE_REVALIDATE);
		if (!cluster_semantic_activation_recheck_r4_terminal_census(
				&admission)
			|| !ctrc_cleaner_current_mx_capture_recheck(
				buffer, &receipt->target, &capture))
		{
			UnlockReleaseBuffer(buffer);
			cluster_semantic_activation_leave(&admission);
			return false;
		}
		cleanout_lsn = PageGetLSN(BufferGetPage(buffer));
		UnlockReleaseBuffer(buffer);
		if (receipt->target.needs_wal)
		{
			if (XLogRecPtrIsInvalid(cleanout_lsn))
			{
				cluster_semantic_activation_leave(&admission);
				return false;
			}
			XLogFlush(cleanout_lsn);
		}
		ctrc_participant_capture_durability(&durability);
		durability.highest_local_lsn = cleanout_lsn;
		memcpy(durability.required_lsn, capture.dependencies.required,
			sizeof(durability.required_lsn));
		goto current_mx_discharge;
	}

	MemSet(members, 0, sizeof(members));
	MemSet(proofs, 0, sizeof(proofs));
	MemSet(proof_capability_generations, 0,
		sizeof(proof_capability_generations));
	MemSet(&updater_proof, 0, sizeof(updater_proof));
	MemSet(&descriptor_key, 0, sizeof(descriptor_key));
	descriptor_key.origin_node_id = receipt->target.mx_origin_node_id;
	descriptor_key.multixact_id
		= (MultiXactId)receipt->target.multixact_id;
	descriptor_key.cluster_epoch = receipt->target.mx_cluster_epoch;
	describe_result = cluster_multixact_current_describe(
		&descriptor_key, members, lengthof(members), &nmembers,
		&reported_total);
	if (describe_result != CMX_DESC_OK || nmembers == 0
		|| reported_total != nmembers)
	{
		retain_stage = "describe";
		goto current_mx_retain_unlocked;
	}
	descriptor_hash = cluster_multixact_current_descriptor_hash(
		&descriptor_key, members, nmembers);
	if (descriptor_hash == 0
		|| descriptor_hash != receipt->target.descriptor_hash)
	{
		retain_stage = "descriptor-hash";
		goto current_mx_retain_unlocked;
	}
	resolve_result = cluster_multixact_current_members_resolve(
		&descriptor_key, members, nmembers, descriptor_hash, NULL,
		proofs, &updater_proof, proof_capability_generations);
	if (resolve_result != CMX_RESOLVE_OK)
	{
		retain_stage = "resolve";
		goto current_mx_retain_unlocked;
	}
	target_ordinal = receipt->publication.member_ordinal;
	if (target_ordinal >= nmembers
		|| !cluster_ctrc_current_mx_terminal_proof_exact(
			&receipt->key, &descriptor_key, &receipt->publication,
			members, proofs, nmembers, NULL, NULL))
	{
		retain_stage = "terminal-proof";
		goto current_mx_retain_unlocked;
	}
	if (!cluster_ctrc_current_mx_rewrite_plan(
			&receipt->key, &receipt->publication, members, proofs,
			nmembers, &plan))
	{
		retain_stage = "rewrite-plan";
		goto current_mx_retain_unlocked;
	}

	MemSet(&transfer, 0, sizeof(transfer));
	MemSet(&successor_target, 0, sizeof(successor_target));
	if (plan.kind == CTRC_CURRENT_MX_REWRITE_SUCCESSOR)
	{
		if (!ctrc_cleaner_prepare_current_mx_successor(
				receipt, &capture, members, proofs,
				proof_capability_generations, nmembers, &plan,
				planned_header, &successor_target, &transfer))
		{
			retain_stage = "successor";
			goto current_mx_retain_unlocked;
		}
	}
	else
	{
		ctrc_cleaner_current_mx_plan_header(
			&capture, &plan, InvalidMultiXactId, planned_header);
	}

	MemSet(&clean_input, 0, sizeof(clean_input));
	clean_input.page_authority_exact = true;
	clean_input.active_companions = plan.survivor_count;
	clean_input.successor_topology_exact
		= plan.kind == CTRC_CURRENT_MX_REWRITE_SUCCESSOR
		  || plan.kind == CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER;
	if (ISUPDATE_from_mxstatus(members[target_ordinal].member_status))
		clean_input.target_state
			= proofs[target_ordinal].state == CCM_COMMITTED
			  ? CTRC_TARGET_COMMITTED_UPDATER
			  : CTRC_TARGET_ABORTED_UPDATER;
	else
		clean_input.target_state = CTRC_TARGET_TERMINAL_LOCK_ONLY;
	clean_result = cluster_ctrc_clean_reference(&clean_input);
	if (clean_result != (ClusterCtrcCleanResult)plan.clean_result)
	{
		retain_stage = "clean-policy";
		goto current_mx_retain_unlocked;
	}

	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
	if (!ClusterLockBufferExclusiveRetryAware(buffer))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PAGE_REVALIDATE);
	if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
		|| !ctrc_cleaner_current_mx_capture_recheck(
			buffer, &receipt->target, &capture))
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	page = BufferGetPage(buffer);
	image_item = PageGetItemId(page,
		(OffsetNumber)receipt->target.offset_number);
	image_tuple = (HeapTupleHeader)PageGetItem(page, image_item);
	if (plan.kind == CTRC_CURRENT_MX_REWRITE_COMMITTED_UPDATER
		&& !ctrc_cleaner_current_mx_committed_projection_exact(
			page, image_tuple, plan.committed_updater_xid,
			plan.committed_updater_scn))
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	if (cluster_ctrc_transfer_remove_predecessor(&transfer)
		!= CLUSTER_CTRC_TRANSFER_REMOVED)
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	xlog_state = GenericXLogStartLogged(receipt->target.needs_wal);
	image = GenericXLogRegisterBuffer(xlog_state, buffer, 0);
	image_item = PageGetItemId(image,
		(OffsetNumber)receipt->target.offset_number);
	if (!ItemIdIsNormal(image_item)
		|| ItemIdGetLength(image_item) < SizeofHeapTupleHeader)
	{
		GenericXLogAbort(xlog_state);
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	image_tuple = (HeapTupleHeader)PageGetItem(image, image_item);
	if (memcmp(image_tuple, capture.tuple_header,
			SizeofHeapTupleHeader) != 0)
	{
		GenericXLogAbort(xlog_state);
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	memcpy(image_tuple, planned_header, SizeofHeapTupleHeader);
	cleanout_lsn = GenericXLogFinish(xlog_state);
	UnlockReleaseBuffer(buffer);
	if (receipt->target.needs_wal)
	{
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_WAL_DURABILITY);
		if (XLogRecPtrIsInvalid(cleanout_lsn))
		{
			cluster_semantic_activation_leave(&admission);
			return false;
		}
		XLogFlush(cleanout_lsn);
	}
	ctrc_participant_capture_durability(&durability);
	durability.highest_local_lsn = cleanout_lsn;
	memcpy(durability.required_lsn, capture.dependencies.required,
		sizeof(durability.required_lsn));

current_mx_discharge:
	MemSet(&handle, 0, sizeof(handle));
	handle.participant = &ctrc_participant_entries()[participant_index];
	handle.receipt = &ctrc_receipt_entries()[receipt_index];
	handle.key = receipt->key;
	handle.participant_index = participant_index;
	handle.receipt_index = receipt_index;
	handle.journal_slot_generation
		= receipt->publication.journal_slot_generation;
	handle.valid = true;
	cluster_semantic_activation_leave(&admission);
	return cluster_ctrc_receipt_discharge_current_mx_shared(
		&handle, &receipt->target, clean_result, &durability)
		== CLUSTER_CTRC_DISCHARGE_CLEANED;

current_mx_retain_unlocked:
	{
		static uint64 last_journal_sequence = 0;
		static const char *last_stage = NULL;
		const ClusterCurrentMemberProof *target_proof
			= receipt->publication.member_ordinal < nmembers
			? &proofs[receipt->publication.member_ordinal] : NULL;

		if (last_journal_sequence
				!= receipt->publication.journal_sequence
			|| last_stage != retain_stage)
		{
			last_journal_sequence = receipt->publication.journal_sequence;
			last_stage = retain_stage;
			ereport(LOG,
				(errmsg_internal(
					"PGRAC CTRC current-MX cleanout retained: stage=%s "
					"journal=" UINT64_FORMAT " key_origin=%u key_xid=%u "
					"mx_origin=%u mxid=%u mx_epoch=%u ordinal=%u "
					"describe=%d resolve=%d members=%u total=%u "
					"target_hash=" UINT64_FORMAT " actual_hash=" UINT64_FORMAT
					" proof_state=%d proof_xid=%u proof_ordinal=%u "
					"proof_status=%u proof_grant=%u proof_scn=" UINT64_FORMAT,
					retain_stage, receipt->publication.journal_sequence,
					receipt->key.origin_node_id, receipt->key.xid,
					receipt->target.mx_origin_node_id,
					receipt->target.multixact_id,
					receipt->target.mx_cluster_epoch,
					receipt->publication.member_ordinal,
					(int)describe_result, (int)resolve_result, nmembers,
					reported_total, receipt->target.descriptor_hash,
					descriptor_hash,
					target_proof != NULL ? (int)target_proof->state : -1,
					target_proof != NULL ? target_proof->member_xid : 0,
					target_proof != NULL ? target_proof->member_ordinal : 0,
					target_proof != NULL ? target_proof->member_status : 0,
					target_proof != NULL
						? ClusterCurrentMemberProofGetCtrcGrant(target_proof) : 0,
					target_proof != NULL ? target_proof->commit_scn : InvalidScn)));
		}
	}
	ReleaseBuffer(buffer);
	cluster_semantic_activation_leave(&admission);
	return false;
}

static bool
ctrc_cleaner_clean_itl_receipt(
	const ClusterCtrcParticipantEntry *participant,
	const ClusterCtrcReceipt *receipt, uint64 participant_index,
	uint64 receipt_index)
{
	ClusterSemanticAdmissionToken admission;
	ClusterCtrcTerminalStatus terminal_status;
	ClusterCtrcItlCleanoutApplyResult apply_result;
	ClusterCtrcItlTargetIdentity expected_target;
	ClusterCtrcReceiptHandle handle;
	ClusterCtrcDurability durability;
	ClusterSfDepVec first_dependencies;
	ClusterSfDepVec final_dependencies;
	RelFileLocator locator;
	SMgrRelation smgr;
	GenericXLogState *xlog_state;
	XLogRecPtr cleanout_lsn;
	SCN commit_scn;
	Buffer buffer;
	Page page;
	Page image;
	bool permanent;

	if (participant == NULL || receipt == NULL
		|| participant_index >= CtrcShared->participant_key_entries
		|| receipt_index >= CtrcShared->receipt_entries
		|| receipt->publication.reference_kind != CTRC_REF_HEAP_ITL_UBA
		|| receipt->state != CTRC_RECEIPT_APPLIED
		|| receipt->key.origin_node_id != (uint16)cluster_node_id
		|| !ctrc_target_exact_itl_valid(&receipt->key, &receipt->target)
		|| (receipt->target.relation_persistence
			!= RELPERSISTENCE_PERMANENT
			&& receipt->target.relation_persistence
			   != RELPERSISTENCE_UNLOGGED)
		|| receipt->target.fork_number < 0
		|| receipt->target.fork_number > MAX_FORKNUM)
		return false;
	if (!ctrc_cleaner_terminal_sample_exact(&receipt->key,
			&terminal_status, &commit_scn))
		return false;

	MemSet(&admission, 0, sizeof(admission));
	if (cluster_semantic_activation_enter_r4_terminal_census(&admission)
		!= CLUSTER_SEMANTIC_ADMISSION_OK)
		return false;
	if (admission.formation_epoch != receipt->key.formation_epoch
		|| admission.record_generation
		   != receipt->key.admission_record_generation)
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	locator.spcOid = receipt->target.spc_oid;
	locator.dbOid = receipt->target.db_oid;
	locator.relNumber = receipt->target.rel_number;
	smgr = smgropen(locator, InvalidBackendId);
	if (!smgrexists(smgr, (ForkNumber)receipt->target.fork_number)
		|| smgrnblocks(smgr, (ForkNumber)receipt->target.fork_number)
		   <= receipt->target.block_number)
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	permanent = receipt->target.relation_persistence
		== RELPERSISTENCE_PERMANENT;
	buffer = ReadBufferWithoutRelcache(locator,
		(ForkNumber)receipt->target.fork_number,
		receipt->target.block_number, RBM_NORMAL, NULL, permanent);
	if (!BufferIsValid(buffer))
	{
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	/* First Resource-X round captures page dependencies, then releases every
	 * page lock before sampling foreign durability. */
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
	if (!ClusterLockBufferExclusiveRetryAware(buffer))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PAGE_REVALIDATE);
	if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
		|| !ctrc_cleaner_itl_page_exact(buffer, &receipt->target, &page))
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_sf_dep_vec_reset(&first_dependencies);
	(void)cluster_sf_dep_vec_for_ship(buffer, &first_dependencies);
	LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_WAL_DURABILITY);
	if (!ctrc_cleaner_dependencies_durable(&first_dependencies,
			&durability))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}

	/* Reacquire a fresh exact Resource-X/current page and require the
	 * dependency vector to be unchanged before the WAL-protected rewrite. */
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_RESOURCE_X);
	if (!ClusterLockBufferExclusiveRetryAware(buffer))
	{
		ReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PAGE_REVALIDATE);
	if (!cluster_semantic_activation_recheck_r4_terminal_census(&admission)
		|| !ctrc_cleaner_itl_page_exact(buffer, &receipt->target, &page))
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cluster_sf_dep_vec_reset(&final_dependencies);
	(void)cluster_sf_dep_vec_for_ship(buffer, &final_dependencies);
	if (memcmp(&first_dependencies, &final_dependencies,
			sizeof(first_dependencies)) != 0)
	{
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	xlog_state = GenericXLogStartLogged(receipt->target.needs_wal);
	image = GenericXLogRegisterBuffer(xlog_state, buffer, 0);
	apply_result = cluster_ctrc_itl_cleanout_slot(&receipt->key,
		&receipt->target, terminal_status, commit_scn,
		&ClusterPageGetItlSlots(image)[receipt->target.itl_slot_index]);
	if (apply_result == CLUSTER_CTRC_ITL_CLEANOUT_RETAIN)
	{
		GenericXLogAbort(xlog_state);
		UnlockReleaseBuffer(buffer);
		cluster_semantic_activation_leave(&admission);
		return false;
	}
	cleanout_lsn = GenericXLogFinish(xlog_state);
	UnlockReleaseBuffer(buffer);
	cluster_semantic_activation_leave(&admission);

	if (receipt->target.needs_wal)
	{
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_WAL_DURABILITY);
		if (XLogRecPtrIsInvalid(cleanout_lsn))
			return false;
		XLogFlush(cleanout_lsn);
	}
	ctrc_participant_capture_durability(&durability);
	durability.highest_local_lsn = cleanout_lsn;
	memcpy(durability.required_lsn, final_dependencies.required,
		sizeof(durability.required_lsn));

	MemSet(&handle, 0, sizeof(handle));
	handle.participant = &ctrc_participant_entries()[participant_index];
	handle.receipt = &ctrc_receipt_entries()[receipt_index];
	handle.key = receipt->key;
	handle.participant_index = participant_index;
	handle.receipt_index = receipt_index;
	handle.journal_slot_generation
		= receipt->publication.journal_slot_generation;
	handle.valid = true;
	MemSet(&expected_target, 0, sizeof(expected_target));
	expected_target.spc_oid = receipt->target.spc_oid;
	expected_target.db_oid = receipt->target.db_oid;
	expected_target.rel_number = receipt->target.rel_number;
	expected_target.fork_number = receipt->target.fork_number;
	expected_target.block_number = receipt->target.block_number;
	expected_target.itl_xid = receipt->target.itl_xid;
	expected_target.itl_slot_index = receipt->target.itl_slot_index;
	expected_target.itl_slot_wrap = receipt->target.itl_slot_wrap;
	expected_target.itl_class = receipt->target.itl_class;
	expected_target.needs_wal = receipt->target.needs_wal;
	memcpy(expected_target.uba, receipt->target.uba,
		sizeof(expected_target.uba));
	return cluster_ctrc_receipt_discharge_itl_shared(&handle,
		&expected_target, CTRC_ITL_TERMINAL_INDEPENDENT, &durability)
		== CLUSTER_CTRC_DISCHARGE_CLEANED;
}

static bool
ctrc_cleaner_clean_next_receipt(void)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt receipt;
	uint64 participant_index;
	uint64 receipt_index;
	bool cleaned;

	if (!ctrc_cleaner_next_applied_receipt(&participant, &receipt,
			&participant_index, &receipt_index))
		return false;
	if (receipt.publication.reference_kind == CTRC_REF_HEAP_ITL_UBA)
		cleaned = ctrc_cleaner_clean_itl_receipt(&participant, &receipt,
			participant_index, receipt_index);
	else
		cleaned = ctrc_cleaner_clean_current_mx_receipt(
			&participant, &receipt, participant_index, receipt_index);
	if (cleaned)
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);
	return cleaned;
}

static bool
ctrc_participant_find_ack_ready(ClusterCtrcParticipantEntry *snapshot,
	uint64 *participant_index_out)
{
	static uint64 scan_cursor = 0;
	uint64 visited;
	uint64 limit;
	bool found = false;

	if (snapshot == NULL || participant_index_out == NULL
		|| cluster_node_id < 0 || !cluster_ctrc_shmem_ready())
		return false;
	MemSet(snapshot, 0, sizeof(*snapshot));
	*participant_index_out = UINT64_MAX;
	limit = ctrc_scan_limit(CTRC_SCAN_ACK, CtrcShared->participant_key_entries);
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	for (visited = 0; visited < limit; visited++) {
		uint64 index = (scan_cursor + visited)
			% CtrcShared->participant_key_entries;
		ClusterCtrcParticipantEntry *participant
			= &ctrc_participant_entries()[index];

		if (participant->state == CTRC_PARTICIPANT_CLOSED_DRAINING
			&& participant->prepared_count == 0
			&& participant->applied_count == 0)
			(void)cluster_ctrc_participant_close(participant,
				&participant->identity, participant->grant_generation,
				participant->seal_generation);
		if (participant->state != CTRC_PARTICIPANT_ACK_READY
			|| participant->identity.node_id != (uint16)cluster_node_id)
			continue;
		if (!ctrc_bytes_zero(&ctrc_participant_ack_entries()[index],
				sizeof(ClusterCtrcLocalReleaseAckV1)))
		{
			participant->state = CTRC_PARTICIPANT_BLOCKED;
			continue;
		}
		*snapshot = *participant;
		*participant_index_out = index;
		scan_cursor = (index + 1) % CtrcShared->participant_key_entries;
		found = true;
		break;
	}
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	ctrc_scan_charge(CTRC_SCAN_ACK, visited + (found ? 1 : 0));
	return found;
}

static bool
ctrc_participant_copy_receipts(uint64 participant_index,
	const ClusterCtrcParticipantEntry *expected,
	ClusterCtrcReceipt *receipts, Size receipt_count)
{
	ClusterCtrcParticipantEntry *participant;
	Size found = 0;
	uint64 i;
	bool exact = true;

	if (expected == NULL || (receipt_count != 0 && receipts == NULL)
		|| participant_index >= CtrcShared->participant_key_entries)
		return false;
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	participant = &ctrc_participant_entries()[participant_index];
	if (memcmp(participant, expected, sizeof(*expected)) != 0
		|| participant->state != CTRC_PARTICIPANT_ACK_READY
		|| participant->receipt_count != receipt_count)
		exact = false;
	for (i = 0; exact && i < CtrcShared->receipt_entries; i++)
	{
		ClusterCtrcReceipt *receipt = &ctrc_receipt_entries()[i];

		if (memcmp(&receipt->key, &expected->key,
				sizeof(receipt->key)) != 0)
			continue;
		if (found >= receipt_count)
		{
			exact = false;
			break;
		}
		receipts[found++] = *receipt;
	}
	if (exact && found != receipt_count)
		exact = false;
	if (!exact && memcmp(&participant->key, &expected->key,
			sizeof(expected->key)) == 0
		&& participant->grant_generation == expected->grant_generation
		&& participant->seal_generation == expected->seal_generation
		&& participant->state == CTRC_PARTICIPANT_ACK_READY)
		participant->state = CTRC_PARTICIPANT_BLOCKED;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	return exact;
}

static void
ctrc_participant_capture_durability(ClusterCtrcDurability *durability)
{
	int origin;

	MemSet(durability, 0, sizeof(*durability));
	durability->local_flush_lsn = GetFlushRecPtr(NULL);
	for (origin = 0; origin < CLUSTER_SF_DEP_MAX_ORIGINS; origin++)
		durability->durable_lsn[origin] = origin == cluster_node_id
			? durability->local_flush_lsn
			: cluster_sf_observed_origin_durable_lsn(origin);
}

static bool
ctrc_participant_freeze_ack_exact(uint64 participant_index,
	const ClusterCtrcParticipantEntry *expected,
	const ClusterCtrcReceipt *sorted_receipts, Size receipt_count,
	const ClusterCtrcLocalReleaseAckV1 *ack)
{
	ClusterCtrcParticipantEntry *participant;
	ClusterCtrcLocalReleaseAckV1 *summary;
	Size found = 0;
	uint64 i;
	bool exact = true;

	if (expected == NULL || ack == NULL
		|| (receipt_count != 0 && sorted_receipts == NULL)
		|| participant_index >= CtrcShared->participant_key_entries)
		return false;
	SpinLockAcquire(&CtrcShared->participant_lock);
	SpinLockAcquire(&CtrcShared->receipt_lock);
	participant = &ctrc_participant_entries()[participant_index];
	summary = &ctrc_participant_ack_entries()[participant_index];
	if (memcmp(participant, expected, sizeof(*expected)) != 0
		|| participant->state != CTRC_PARTICIPANT_ACK_READY
		|| participant->receipt_count != receipt_count
		|| !ctrc_bytes_zero(summary, sizeof(*summary)))
		exact = false;
	for (i = 0; exact && i < CtrcShared->receipt_entries; i++)
	{
		ClusterCtrcReceipt *receipt = &ctrc_receipt_entries()[i];
		uint64 key_sequence;

		if (memcmp(&receipt->key, &expected->key,
				sizeof(receipt->key)) != 0)
			continue;
		key_sequence = receipt->publication.key_sequence;
		if (key_sequence == 0 || key_sequence > receipt_count
			|| memcmp(receipt, &sorted_receipts[key_sequence - 1],
				sizeof(*receipt)) != 0)
		{
			exact = false;
			break;
		}
		found++;
	}
	if (exact && found != receipt_count)
		exact = false;
	if (exact)
	{
		*summary = *ack;
		for (i = 0; i < CtrcShared->receipt_entries; i++)
		{
			ClusterCtrcReceipt *receipt = &ctrc_receipt_entries()[i];

			if (memcmp(&receipt->key, &expected->key,
					sizeof(receipt->key)) == 0)
				pg_atomic_write_u32((pg_atomic_uint32 *)&receipt->state,
					CTRC_RECEIPT_ACK_FROZEN);
		}
		participant->ack_frozen_count = receipt_count;
		pg_write_barrier();
		participant->state = CTRC_PARTICIPANT_ACK_FROZEN;
	}
	else if (memcmp(&participant->key, &expected->key,
			sizeof(expected->key)) == 0
			 && participant->grant_generation == expected->grant_generation
			 && participant->seal_generation == expected->seal_generation
			 && participant->state == CTRC_PARTICIPANT_ACK_READY)
		participant->state = CTRC_PARTICIPANT_BLOCKED;
	SpinLockRelease(&CtrcShared->receipt_lock);
	SpinLockRelease(&CtrcShared->participant_lock);
	return exact;
}

static bool
ctrc_participant_freeze_next_ack_shared(void)
{
	ClusterCtrcParticipantEntry participant;
	ClusterCtrcReceipt *receipts = NULL;
	ClusterCtrcLocalReleaseAckV1 first_ack;
	ClusterCtrcLocalReleaseAckV1 second_ack;
	ClusterCtrcDurability durability;
	XLogRecPtr highest_local_lsn = InvalidXLogRecPtr;
	uint64 participant_index;
	Size receipt_count;
	Size allocation_bytes;
	Size i;
	bool frozen = false;

	if (!ctrc_participant_find_ack_ready(&participant,
			&participant_index))
		return false;
	if (participant.receipt_count > (uint64)(MaxAllocSize
			/ sizeof(ClusterCtrcReceipt)))
		return false;
	receipt_count = (Size)participant.receipt_count;
	allocation_bytes = receipt_count * sizeof(ClusterCtrcReceipt);
	if (receipt_count != 0)
	{
		receipts = (ClusterCtrcReceipt *)palloc_extended(allocation_bytes,
			MCXT_ALLOC_NO_OOM);
		if (receipts == NULL)
			return false;
	}
	if (!ctrc_participant_copy_receipts(participant_index, &participant,
			receipts, receipt_count))
		goto freeze_done;
	for (i = 0; i < receipt_count; i++)
		if (receipts[i].highest_local_wal_lsn > highest_local_lsn)
			highest_local_lsn = receipts[i].highest_local_wal_lsn;
	if (!XLogRecPtrIsInvalid(highest_local_lsn))
	{
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_WAL_DURABILITY);
		XLogFlush(highest_local_lsn);
	}
	ctrc_participant_capture_durability(&durability);
	if (cluster_ctrc_participant_ack_from_snapshot(&participant, receipts,
			receipt_count, &durability, &first_ack)
		!= CLUSTER_CTRC_ACK_RELEASED)
		goto freeze_done;

	/* A second independently captured range must reproduce the exact ACK
	 * before the final lock-held equality check publishes ACK_FROZEN. */
	if (!ctrc_participant_copy_receipts(participant_index, &participant,
			receipts, receipt_count))
		goto freeze_done;
	ctrc_participant_capture_durability(&durability);
	if (cluster_ctrc_participant_ack_from_snapshot(&participant, receipts,
			receipt_count, &durability, &second_ack)
		!= CLUSTER_CTRC_ACK_RELEASED
		|| memcmp(&first_ack, &second_ack, sizeof(first_ack)) != 0)
		goto freeze_done;
	cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_PARTICIPANT_ACK);
	cluster_ctrc_test_barrier_wait(CTRC_TEST_BARRIER_ACK_DURABLE);
	frozen = ctrc_participant_freeze_ack_exact(participant_index,
		&participant, receipts, receipt_count, &second_ack);
	if (frozen)
	{
		cluster_ctrc_stat_bump(CTRC_STAT_ACK_FROZEN);
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_NONE);
		cluster_undo_cleaner_wakeup();
	}
	else
		cluster_ctrc_cleaner_reason_set(CTRC_CLEANER_REASON_BLOCKED);

freeze_done:
	if (receipts != NULL)
		pfree(receipts);
	return frozen;
}
#endif

ClusterCtrcAckResult
cluster_ctrc_participant_build_ack(ClusterCtrcParticipantEntry *participant,
								   const ClusterCtrcDurability *durability,
								   ClusterCtrcLocalReleaseAckV1 *ack)
{
	if (cluster_ctrc_participant_ack_from_snapshot(participant, NULL, 0,
			durability, ack) != CLUSTER_CTRC_ACK_RELEASED)
		return CLUSTER_CTRC_ACK_DENIED;
	participant->state = CTRC_PARTICIPANT_ACK_FROZEN;
	return CLUSTER_CTRC_ACK_RELEASED;
}

ClusterCtrcCertificateResult
cluster_ctrc_origin_certificate_validate(const ClusterCtrcCertificateInput *input)
{
	const ClusterCtrcLocalReleaseAckV1 *first = NULL;
	uint64 allowed_bitmap
		= (UINT64_C(1) << CLUSTER_CTRC_MAX_PARTICIPANTS) - 1;
	uint64 seen_bitmap = 0;
	uint16 i;

	if (input == NULL || input->reserved16 != 0
		|| input->seal_generation == 0
		|| input->seal_generation == UINT64_MAX
		|| !input->block0_terminal_exact
		|| !input->all_dependencies_durable
		|| (input->frozen_touched_bitmap & ~allowed_bitmap) != 0
		|| input->ack_count > CLUSTER_CTRC_MAX_PARTICIPANTS
		|| (input->ack_count != 0 && input->acks == NULL))
		return CLUSTER_CTRC_CERTIFICATE_RETAIN;

	for (i = 0; i < input->ack_count; i++)
	{
		const ClusterCtrcLocalReleaseAckV1 *ack = &input->acks[i];
		uint64 node_bit;

		if (!ctrc_ack_bytes_exact(ack)
			|| ack->seal_generation != input->seal_generation
			|| ack->participant_node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS)
			return CLUSTER_CTRC_CERTIFICATE_RETAIN;
		node_bit = UINT64_C(1) << ack->participant_node_id;
		if ((input->frozen_touched_bitmap & node_bit) == 0
			|| (seen_bitmap & node_bit) != 0)
			return CLUSTER_CTRC_CERTIFICATE_RETAIN;
		if (first == NULL)
			first = ack;
		else if (memcmp(&ack->transaction_key, &first->transaction_key,
				sizeof(ack->transaction_key)) != 0
				 || ack->grant_generation != first->grant_generation)
			return CLUSTER_CTRC_CERTIFICATE_RETAIN;
		seen_bitmap |= node_bit;
	}
	if (seen_bitmap != input->frozen_touched_bitmap)
		return CLUSTER_CTRC_CERTIFICATE_RETAIN;
	return CLUSTER_CTRC_CERTIFICATE_READY;
}

bool
cluster_ctrc_origin_certificate_digest(
	const ClusterCtrcCertificateInput *input, uint8 digest[32])
{
	static const uint8 ack_set_domain[] = "PGRAC-CTRC-ACKSET-V1";
	const ClusterCtrcLocalReleaseAckV1 *ordered[CLUSTER_CTRC_MAX_PARTICIPANTS];
	pg_sha256_ctx context;
	uint8 ack_bytes[CLUSTER_CTRC_LOCAL_ACK_BYTES];
	uint8 count_bytes[2];
	uint8 length_bytes[4];
	uint16 node_id;
	uint16 i;

	if (digest != NULL)
		MemSet(digest, 0, 32);
	if (digest == NULL
		|| cluster_ctrc_origin_certificate_validate(input)
		   != CLUSTER_CTRC_CERTIFICATE_READY)
		return false;
	MemSet(ordered, 0, sizeof(ordered));
	for (i = 0; i < input->ack_count; i++)
		ordered[input->acks[i].participant_node_id] = &input->acks[i];

	pg_sha256_init(&context);
	pg_sha256_update(&context, ack_set_domain, sizeof(ack_set_domain));
	ctrc_put_u16_le(count_bytes, input->ack_count);
	pg_sha256_update(&context, count_bytes, sizeof(count_bytes));
	ctrc_put_u32_le(length_bytes, CLUSTER_CTRC_LOCAL_ACK_BYTES);
	for (node_id = 0; node_id < CLUSTER_CTRC_MAX_PARTICIPANTS; node_id++)
	{
		if (ordered[node_id] == NULL)
			continue;
		if (!cluster_ctrc_local_release_ack_encode(
				ordered[node_id], ack_bytes))
		{
			MemSet(digest, 0, 32);
			return false;
		}
		pg_sha256_update(&context, length_bytes, sizeof(length_bytes));
		pg_sha256_update(&context, ack_bytes, sizeof(ack_bytes));
	}
	pg_sha256_final(&context, digest);
	return true;
}

bool
cluster_ctrc_origin_certificate_snapshot_entry(
	const ClusterCtrcOriginEntry *origin,
	const ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS],
	uint64 origin_index, ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcCertificateInput input;
	uint16 node_id;

	if (snapshot != NULL)
		MemSet(snapshot, 0, sizeof(*snapshot));
	if (origin == NULL || ack_slots == NULL || snapshot == NULL
		|| origin->state != CTRC_ORIGIN_CERTIFYING
		|| !ctrc_txn_key_valid(&origin->key)
		|| origin->grant_generation == 0
		|| origin->seal_generation == 0
		|| origin->seal_generation == UINT64_MAX
		|| !ctrc_bytes_zero(origin->reserved8, sizeof(origin->reserved8))
		|| !ctrc_origin_frozen_touch_set_valid(origin)
		|| origin->close_dispatched_bitmap != origin->touched_bitmap
		|| origin->close_confirmed_bitmap != origin->touched_bitmap
		|| origin->ack_bitmap != origin->touched_bitmap)
		return false;

	snapshot->origin_index = origin_index;
	snapshot->origin = *origin;
	for (node_id = 0; node_id < CLUSTER_CTRC_MAX_PARTICIPANTS; node_id++)
	{
		const ClusterCtrcLocalReleaseAckV1 *ack = &ack_slots[node_id];
		uint32 bit = UINT32_C(1) << node_id;
		const ClusterCtrcParticipantIdentity *identity;

		if ((origin->touched_bitmap & bit) == 0)
		{
			if (!ctrc_bytes_zero(ack, sizeof(*ack)))
				goto invalid;
			continue;
		}
		identity = &origin->touched[node_id];
		if (!ctrc_ack_bytes_exact(ack)
			|| memcmp(&ack->transaction_key, &origin->key,
					  sizeof(origin->key)) != 0
			|| ack->grant_generation != origin->grant_generation
			|| ack->seal_generation != origin->seal_generation
			|| ack->participant_node_id != node_id
			|| ack->capability_record_generation
			   != identity->capability_record_generation
			|| ack->participant_boot_incarnation != identity->boot_incarnation
			|| ack->formation_epoch != identity->formation_epoch
			|| ack->admission_record_generation
			   != identity->admission_record_generation)
			goto invalid;
		snapshot->acks[snapshot->ack_count++] = *ack;
	}
	if (snapshot->ack_count != origin->touched_count)
		goto invalid;

	MemSet(&input, 0, sizeof(input));
	input.acks = snapshot->ack_count == 0 ? NULL : snapshot->acks;
	input.ack_count = snapshot->ack_count;
	input.frozen_touched_bitmap = origin->touched_bitmap;
	input.seal_generation = origin->seal_generation;
	input.block0_terminal_exact = true;
	input.all_dependencies_durable = true;
	if (cluster_ctrc_origin_certificate_validate(&input)
		!= CLUSTER_CTRC_CERTIFICATE_READY)
		goto invalid;
	return true;

invalid:
	MemSet(snapshot, 0, sizeof(*snapshot));
	return false;
}

bool
cluster_ctrc_origin_certificate_commit_entry(
	ClusterCtrcOriginEntry *origin,
	const ClusterCtrcOriginCertificateSnapshot *snapshot)
{
	ClusterCtrcOriginCertificateSnapshot rebuilt;
	ClusterCtrcLocalReleaseAckV1 ack_slots[CLUSTER_CTRC_MAX_PARTICIPANTS];
	uint16 i;

	if (origin == NULL || snapshot == NULL
		|| !ctrc_bytes_zero(snapshot->reserved, sizeof(snapshot->reserved))
		|| snapshot->ack_count > CLUSTER_CTRC_MAX_PARTICIPANTS
		|| !ctrc_bytes_zero(&snapshot->acks[snapshot->ack_count],
			(CLUSTER_CTRC_MAX_PARTICIPANTS - snapshot->ack_count)
			* sizeof(snapshot->acks[0]))
		|| memcmp(origin, &snapshot->origin, sizeof(*origin)) != 0)
		return false;

	MemSet(ack_slots, 0, sizeof(ack_slots));
	for (i = 0; i < snapshot->ack_count; i++)
	{
		uint16 node_id = snapshot->acks[i].participant_node_id;

		if (node_id >= CLUSTER_CTRC_MAX_PARTICIPANTS
			|| !ctrc_bytes_zero(&ack_slots[node_id],
								 sizeof(ack_slots[node_id])))
			return false;
		ack_slots[node_id] = snapshot->acks[i];
	}
	if (!cluster_ctrc_origin_certificate_snapshot_entry(&snapshot->origin,
		ack_slots, snapshot->origin_index, &rebuilt)
		|| memcmp(&rebuilt, snapshot, sizeof(rebuilt)) != 0)
		return false;

	origin->state = CTRC_ORIGIN_RELEASE_PROVEN;
	origin->close_dispatched_bitmap = 0;
	origin->close_confirmed_bitmap = 0;
	MemSet(origin->close_request_id, 0, sizeof(origin->close_request_id));
	return true;
}

bool
cluster_ctrc_terminal_recyclable(const ClusterCtrcRecycleInput *input)
{
	if (input == NULL || !input->release_proven)
		return false;
	if (input->status == CTRC_TERMINAL_COMMITTED)
		return !input->durable_aborted
			&& SCN_VALID(input->commit_scn)
			&& input->horizon_valid
			&& SCN_VALID(input->horizon_scn)
			&& scn_time_cmp(input->commit_scn, input->horizon_scn) <= 0;
	if (input->status == CTRC_TERMINAL_ABORTED)
		return input->durable_aborted && !SCN_VALID(input->commit_scn);
	return false;
}

ClusterCtrcCrashDisposition
cluster_ctrc_crash_cut_disposition(ClusterCtrcCrashCut cut)
{
	return cut == CTRC_CRASH_DURABLE_CERTIFICATE_BEFORE_NOTIFICATION
		? CLUSTER_CTRC_CRASH_RELEASE_PROVEN
		: CLUSTER_CTRC_CRASH_RETAIN;
}
